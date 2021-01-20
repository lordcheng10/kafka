/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.common

import java.nio.charset.StandardCharsets.UTF_8
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicBoolean

import kafka.utils.{Logging, ShutdownableThread}
import kafka.zk.{KafkaZkClient, StateChangeHandlers}
import kafka.zookeeper.{StateChangeHandler, ZNodeChildChangeHandler}
import org.apache.kafka.common.utils.Time

import scala.collection.Seq
import scala.util.{Failure, Try}

/**
 * Handle the notificationMessage.
 */
trait NotificationHandler {
  def processNotification(notificationMessage: Array[Byte]): Unit
}


/**
 * ZkNodeChangeNotificationListener的调用逻辑：
 * ①init()注册监听器：
 * zkClient.registerStateChangeHandler(ZkStateChangeHandler)
 * zkClient.registerZNodeChildChangeHandler(ChangeNotificationHandler)
 * ②触发监听器，调用ZkStateChangeHandler.afterInitializingSession()和ChangeNotificationHandler.handleChildChange();
 * ③afterInitializingSession和handleChildChange方法都调用addChangeNotification();
 * ④addChangeNotification()方法中会new ChangeNotification放入queue队列中；
 * ⑤ChangeEventProcessThread线程会从队列中取出ChangeNotification对象，然后调用ChangeNotification.process
 * ⑥ChangeNotification.process() 方法会调用processNotifications()方法;
 * ⑦processNotifications()方法会调用processNotification() ，注意这里不是同一个方法，后面的那个没有s；
 * ⑧processNotification()会调用ConfigChangedNotificationHandler.processNotification()方法;
 * ⑨ConfigChangedNotificationHandler.processNotification()方法中会调用processEntityConfigChangeVersion1或者processEntityConfigChangeVersion2方法（根据版本号）；
 * 10.processEntityConfigChangeVersion1或processEntityConfigChangeVersion2会调用对应ConfigHandler（对应实现类有TopicConfigHandler、QuotaConfigHandler等）的processConfigChanges方法来处理：
 * configHandlers(entityType).processConfigChanges(entity, entityConfig)
 * */
/**
 * A listener that subscribes to seqNodeRoot for any child changes where all children are assumed to be sequence node
 * with seqNodePrefix. When a child is added under seqNodeRoot this class gets notified, it looks at lastExecutedChange
 * number to avoid duplicate processing and if it finds an unprocessed child, it reads its data and calls supplied
 * notificationHandler's processNotification() method with the child's data as argument. As part of processing these changes it also
 * purges any children with currentTime - createTime > changeExpirationMs.
 *
 * @param zkClient
 * @param seqNodeRoot
 * @param seqNodePrefix
 * @param notificationHandler
 * @param changeExpirationMs
 * @param time
 */
class ZkNodeChangeNotificationListener(private val zkClient: KafkaZkClient,
                                       private val seqNodeRoot: String,
                                       private val seqNodePrefix: String,
                                       private val notificationHandler: NotificationHandler,
                                       private val changeExpirationMs: Long = 15 * 60 * 1000,
                                       private val time: Time = Time.SYSTEM) extends Logging {
  private var lastExecutedChange = -1L
  private val queue = new LinkedBlockingQueue[ChangeNotification]
  private val thread = new ChangeEventProcessThread(s"$seqNodeRoot-event-process-thread")
  private val isClosed = new AtomicBoolean(false)

  def init(): Unit = {
    /**
     * 这里注册对应的监听方法，这两个方法ZkStateChangeHandler和ChangeNotificationHandler实际都是在触发后，调用addChangeNotification方法，
     * 而addChangeNotification方法实际是构建一个事件（实际就是ChangeNotification类对象）放入queue队列中，然后由ChangeEventProcessThread线程来处理，
     * 这个线程会从队列中取出事件，然后调用事件的process方法（实际就是调用ChangeNotification.process），然后在process中会调用processNotifications()方法,
     *
     * */
    zkClient.registerStateChangeHandler(ZkStateChangeHandler)
    zkClient.registerZNodeChildChangeHandler(ChangeNotificationHandler)
    addChangeNotification()
    thread.start()
  }

  def close() = {
    isClosed.set(true)
    zkClient.unregisterStateChangeHandler(ZkStateChangeHandler.name)
    zkClient.unregisterZNodeChildChangeHandler(ChangeNotificationHandler.path)
    queue.clear()
    thread.shutdown()
  }

  /**
   * Process notifications
   */
  private def processNotifications(): Unit = {
    try {
      val notifications = zkClient.getChildren(seqNodeRoot).sorted
      if (notifications.nonEmpty) {
        info(s"Processing notification(s) to $seqNodeRoot")
        val now = time.milliseconds
        for (notification <- notifications) {
          val changeId = changeNumber(notification)
          if (changeId > lastExecutedChange) {
            processNotification(notification)
            lastExecutedChange = changeId
          }
        }
        purgeObsoleteNotifications(now, notifications)
      }
    } catch {
      case e: InterruptedException => if (!isClosed.get) error(s"Error while processing notification change for path = $seqNodeRoot", e)
      case e: Exception => error(s"Error while processing notification change for path = $seqNodeRoot", e)
    }
  }

  private def processNotification(notification: String): Unit = {
    val changeZnode = seqNodeRoot + "/" + notification
    val (data, _) = zkClient.getDataAndStat(changeZnode)
    data match {
      case Some(d) => Try(notificationHandler.processNotification(d)) match {
        case Failure(e) => error(s"error processing change notification ${new String(d, UTF_8)} from $changeZnode", e)
        case _ =>
      }
      case None => warn(s"read null data from $changeZnode")
    }
  }

  private def addChangeNotification(): Unit = {
    if (!isClosed.get && queue.peek() == null)
      queue.put(new ChangeNotification)
  }

  class ChangeNotification {
    def process(): Unit = processNotifications()
  }

  /**
   * Purges expired notifications.
   *
   * @param now
   * @param notifications
   */
  private def purgeObsoleteNotifications(now: Long, notifications: Seq[String]): Unit = {
    for (notification <- notifications.sorted) {
      val notificationNode = seqNodeRoot + "/" + notification
      val (data, stat) = zkClient.getDataAndStat(notificationNode)
      if (data.isDefined) {
        if (now - stat.getCtime > changeExpirationMs) {
          debug(s"Purging change notification $notificationNode")
          zkClient.deletePath(notificationNode)
        }
      }
    }
  }

  /* get the change number from a change notification znode */
  private def changeNumber(name: String): Long = name.substring(seqNodePrefix.length).toLong

  class ChangeEventProcessThread(name: String) extends ShutdownableThread(name = name) {
    override def doWork(): Unit = queue.take().process()
  }

  object ChangeNotificationHandler extends ZNodeChildChangeHandler {
    override val path: String = seqNodeRoot
    override def handleChildChange(): Unit = addChangeNotification()
  }

  object ZkStateChangeHandler extends  StateChangeHandler {
    override val name: String = StateChangeHandlers.zkNodeChangeListenerHandler(seqNodeRoot)
    override def afterInitializingSession(): Unit = addChangeNotification()
  }
}

