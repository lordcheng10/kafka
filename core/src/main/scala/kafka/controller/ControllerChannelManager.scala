/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package kafka.controller

import java.net.SocketTimeoutException
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import com.yammer.metrics.core.{Gauge, Timer}
import kafka.api._
import kafka.cluster.Broker
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils._
import kafka.utils.Implicits._
import org.apache.kafka.clients._
import org.apache.kafka.common.message.LeaderAndIsrRequestData.LeaderAndIsrPartitionState
import org.apache.kafka.common.message.StopReplicaRequestData.{StopReplicaPartitionState, StopReplicaTopicState}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.message.UpdateMetadataRequestData.{UpdateMetadataBroker, UpdateMetadataEndpoint, UpdateMetadataPartitionState}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.common.{KafkaException, Node, Reconfigurable, TopicPartition}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.HashMap
import scala.collection.{Seq, Set, mutable}

/**
 * 相对0.11.0，这里增加了变量：RequestRateAndQueueTimeMetricName,
 * 这里的两个变量作用是作为metric的name。
 * */
object ControllerChannelManager {
  val QueueSizeMetricName = "QueueSize"
  val RequestRateAndQueueTimeMetricName = "RequestRateAndQueueTimeMs"
}

/**
 * 相对0.11.0，这里增加了变量：stateChangeLogger: StateChangeLogger
 *
 * */
class ControllerChannelManager(controllerContext: ControllerContext,
                               config: KafkaConfig,
                               time: Time,
                               metrics: Metrics,
                               stateChangeLogger: StateChangeLogger,
                               threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {
  import ControllerChannelManager._
  /**
   * 这里保存的是用于和每个broker通信用的的一切信息，比如：发送队列、发送线程等。key是brokerId.
   * controller到每一个broker之间都有一个单独的队列和发送线程。
   * */
  protected val brokerStateInfo = new HashMap[Int, ControllerBrokerStateInfo]
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "

  /**
   * 统计controller到broker总的队列大小，这里应该拆开，总和指标没有意义.
   * */
  newGauge("TotalQueueSize",
    () => brokerLock synchronized {
      brokerStateInfo.values.iterator.map(_.messageQueue.size).sum
    }
  )

  /**
   *  这里感觉有bug,这里为啥要shutdown的broker也执行addNewBroker呢？
   *
   *  没有bug，只是在调用方法的时候，注意下，别多个线程同时调用addBroker和startup方法。
   * */
  def startup() = {
    controllerContext.liveOrShuttingDownBrokers.foreach(addNewBroker)

    /**
     * 有个问题，为啥上面哪个锁没包括进来，反倒是下面的线程启动要包括进来，brokerStateInfo的访问会有线程安全问题吗，
     * 或者说会有同时两个线程去访问这个吗,我们的事件管理不是只有一个线程吗
     *
     * 这个brokerLock锁是12年写的，但我们事件管理逻辑是后面加的，所以我猜测，
     * 是之前有多线程同时操作问题，后来就引入事件管理就没有了，
     * 但是任然保留了这个锁.当时还没有上面哪行代码，上面哪行是19年加的。
     * */
    brokerLock synchronized {
      brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
    }
  }

  def shutdown() = {
    brokerLock synchronized {
      brokerStateInfo.values.toList.foreach(removeExistingBroker)
    }
  }

  def sendRequest(brokerId: Int, request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit = {
    brokerLock synchronized {
      val stateInfoOpt = brokerStateInfo.get(brokerId)
      stateInfoOpt match {
        case Some(stateInfo) =>
          stateInfo.messageQueue.put(QueueItem(request.apiKey, request, callback, time.milliseconds()))
        case None =>
          warn(s"Not sending request $request to broker $brokerId, since it is offline.")
      }
    }
  }

  /**
   * 这个方法在BrokerChange变化事件里面被调用了两次，但是在0.11.0版本里面，只被调用了一次。
   * 因为在trunk版本里面，broker的epoch版本号变化的也要调用这个方法，而不只是新增的。
   *
   * 那broker版本号变化的broker，什么情况下出现呢？应该就是那种瞬间停启那种broker把，此时虽然它没在新增的里面，但是也应该重新添加下通信结构：先移除，再添加。
   * 之前0.11.0版本，只管新增的（也就是从zk拿到全量list然后减去内存维护的），但有些瞬间变动的broker，可能在两个列表里都有，对于这种broker
   * ，应该移除后，重新建立下链接。也就是先removeBroker再addBroker
   * */
  def addBroker(broker: Broker): Unit = {
    /**
     * 这里它叫我们小心，说startup（）方法中可能已经启动了send发送线程了。
     * 所以这里才进行了判断，要不包含该broker.id的时候，才启动？ 还是说，即使判断了，也可能存在多启动的情况？
     *
     * startup()也在调用addNewBroker，这两个同时加，有没可能，没有可能。因为startup()是在conroller选举成为leader的时候，初始化
     * 调用的，虽然是先注册了broker变化的监听器，再调用startUp方法，但是controller有一个事件管理类ControllerEventManager,所有的事件都是放到该类的一个队列中，
     * 然后下游有一个单线程取出事件进行处理，所以，即便是先注册监听器，即便此时刚好有broker变动触发回调，也会按照顺序来处理，先处理Startup事件完成（此时就会把startUp调用走完），
     * 再处理后面的事件，这样肯定不会出现同时处理多个事件，也就是同时调用Startup和addBroker方法。
     *
     * 另外为什么，这里作者会让我们小心，重复启动发送线程，并不是说现在存在这个问题，而是提醒我们，别两个线程同时调用startUp和addBroker方法，这样会出现这个问题。
     * */
    // be careful here. Maybe the startup() API has already started the request send thread
    brokerLock synchronized {
      if (!brokerStateInfo.contains(broker.id)) {
        /**
         * 有个问题：为什么在addNewBroker中，构建requestThread的时候 不启动requestThread，要单独在startRequestSendThread中去启动。
         * */
        addNewBroker(broker)
        /**
         * 为啥要单独在这启动呢？直接在addNewBroker方法末尾启动不行吗？
         * 感觉是为了逻辑更加清晰一点，所以拆开了，上面是添加，下面是启动方法。
         * */
        startRequestSendThread(broker.id)
      }
    }
  }

  def removeBroker(brokerId: Int): Unit = {
    brokerLock synchronized {
      removeExistingBroker(brokerStateInfo(brokerId))
    }
  }

  private def addNewBroker(broker: Broker): Unit = {
    /**
     * 首先创建一个message queue，这个queue是用来controller和broker之间异步发送请求的.
     * */
    val messageQueue = new LinkedBlockingQueue[QueueItem]
    debug(s"Controller ${config.brokerId} trying to connect to broker ${broker.id}")
    /**
     * 如果配置了control.plane.listener.name ，就从中获取，否则就从inter.broker.listener.name获取。
     *
     * 有个问题 ：control.plane.listener.name和inter.broker.listener.name的区别是啥，为啥要分开配置?
     *
     * listerName我们分为：内部和外部listerName两类,而内部listerName又分为了数据和控制流两类。
     * 所以如果配置了控制流listerName，那么意思是内部listerName我们又分了控制流和数据流，内部的数据流还是走内部listerName。
     * */
    val controllerToBrokerListenerName = config.controlPlaneListenerName.getOrElse(config.interBrokerListenerName)
    /**
     * controller和broker之间的安全协议,如果配置了control.plane.listener.name，就从其中获取，
     * 否则从security.inter.broker.protocol配置中获取的
     * */
    val controllerToBrokerSecurityProtocol = config.controlPlaneSecurityProtocol.getOrElse(config.interBrokerSecurityProtocol)
    /**
     * 根据controller和broker之间的listerName，从建立连接时候，构建的listerName和endpoint的映射map，来构建对应的brokerNode
     * */
    val brokerNode = broker.node(controllerToBrokerListenerName)
    /**
     * 这个玩意，似乎是用来加一些公共内容的，但具体这玩意是什么作用呢
     *
     * 我理解是添加一些公共的日志内容，然后后面我们通过这个来创建logger，打的日志都会有这个公共内容
     * */
    val logContext = new LogContext(s"[Controller id=${config.brokerId}, targetBrokerId=${brokerNode.idString}] ")

    val (networkClient, reconfigurableChannelBuilder) = {
      /**
       * 构建一个builder
       * */
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        controllerToBrokerSecurityProtocol,
        JaasContext.Type.SERVER,
        config,
        controllerToBrokerListenerName,
        config.saslMechanismInterBrokerProtocol,
        time,
        config.saslInterBrokerHandshakeRequestEnable,
        logContext
      )
      /**
       * 上面是channel，怎么和config匹配上了，啥玩意
       * ChannelBuilder是继承自Configurable的。
       * 所以上面是有可能创建Reconfigurable的
       *
       * 有点不太明白为啥Reconfigurable、Configurable、ChannelBuilder三个之间有啥关系
       * 配置和channelBuilder为啥要设计在一起?
       *
       * Reconfigurable是一个动态更新配置的接口，继承它，可以提供动态更新配置的功能。
       * Reconfigurable又是属于配置这一类的，所以Reconfigurable又继承了Configurable，从而这三个就串起来了：
       * ChannelBuilder类有一些配置信息，这些信息我们希望可以动态配置，所以继承了Reconfigurable。
       * 类似的，还有JmxReporter等继承了该类，从而可以支持动态配置。
       *
       *
       * 这里主要是加到动态配置里,方便后面可以动态修改channel的配置
       * */
      val reconfigurableChannelBuilder = channelBuilder match {
        case reconfigurable: Reconfigurable =>
          config.addReconfigurable(reconfigurable)
          Some(reconfigurable)
        case _ => None
      }

      /**
       * 构建和broker之间通信用的selector
       * */
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        "controller-channel",
        Map("broker-id" -> brokerNode.idString).asJava,
        false,
        channelBuilder,
        logContext
      )

      /**
       * 利用上面构建的selector来构建networkClient，
       * controller主要就是通过networkClient来和broker之间通信的
       * */
      val networkClient = new NetworkClient(
        selector,
        new ManualMetadataUpdater(Seq(brokerNode).asJava),
        config.brokerId.toString,
        1,
        0,
        0,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        config.requestTimeoutMs,
        config.connectionSetupTimeoutMs,
        config.connectionSetupTimeoutMaxMs,
        ClientDnsLookup.USE_ALL_DNS_IPS,
        time,
        false,
        new ApiVersions,
        logContext
      )

      (networkClient, reconfigurableChannelBuilder)
    }

   /**
    * controller和brokerId之间的线程名
    * threadNamePrefix前缀是传进来的，应该是留的一个口，但实际没有用到，追踪了下，传入的全部是None，也不支持配置
    * */
    val threadName = threadNamePrefix match {
      case None => s"Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
      case Some(name) => s"$name:Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
    }

    /**
     * 这个metric在统计啥玩意.
     * requestRateAndQueueTimeMetrics.update(time.milliseconds() - enqueueTimeMs,
     * TimeUnit.MILLISECONDS)从这里看，似乎是统计的队列挤压时间.
     * 用当前时间-入队时间.
     * 但从requestRateAndQueueTimeMetrics命名，可以看出，似乎Timer这个metric在统计排队耗时的时候，
     * 也可以统计controller发送的请求速率，这个是真的开始发送的速率，而不是之前放入broker消息队列的速率.
     * */
    val requestRateAndQueueTimeMetrics = newTimer(
      RequestRateAndQueueTimeMetricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS, brokerMetricTags(broker.id)
    )

    /**
     * 构建controller和broker通信用的发送线程;
     * setDaemon(true):表示设置为守护线程，进程退出不用等这个线程；
     * setDaemon(false):表示设置为非守护线程，进程退出必须要等这个线程；
     * */
    val requestThread = new RequestSendThread(config.brokerId, controllerContext, messageQueue, networkClient,
      brokerNode, config, time, requestRateAndQueueTimeMetrics, stateChangeLogger, threadName)
    requestThread.setDaemon(false)

    /**
     * trunk这个队列统计的metric也有变化。
     * newGauge的第二个参数，应该是函数接口类吧 ？在trunk版本中直接传入了一个函数，方法要求的参数类型都是 metric: Gauge[T]
     * 另外第三个参数，传入的变量名也改了：brokerMetricTags，实际是一样的，就只改了名。
     *
     * 这里传入函数方法，看起来是新版本scala的一个新特性。换个scala低版本就会报错。
     * 似乎高版本，当函数参数是类时，这个时候如果传入方法，它会自动匹配类中合适的方法。
     * 这个类必须是抽象类，或接口,然后这个参数类，必须包含一个抽象方法或接口方法，而且只能有一个。
     *
     *
     * 其实这里用到了java8 的新特性：函数式接口，只不过规范的函数式接口需要带@FunctionalInterface 修饰，这里Gauge没有带，而且是抽象类,
     * 如果在java函数式编程中，不能是抽象类，只能是接口，似乎高版本的scala可以解决这个问题，可以是抽象类，
     * 但里面的抽象方法和接口类一样，只能有一个，否则无法匹配上。
     *
     * 也就是高版本的scala，可以对抽象类也匹配上。
     * */
    val queueSizeGauge = newGauge(QueueSizeMetricName, () => messageQueue.size, brokerMetricTags(broker.id))

    /**
     * 这个是统计controller到每个broker的状态信息，包括：metric监控、网络客户端、node节点等，
     * 也就是说controller到这个节点通信相关的都放到这个map中了.
     *
     * 也就是说这个方法把和broker通信要用的一些东西构建好后，封装到ControllerBrokerStateInfo中，并全部放到了brokerStateInfo中。
     * */
    brokerStateInfo.put(broker.id, ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue,
      requestThread, queueSizeGauge, requestRateAndQueueTimeMetrics, reconfigurableChannelBuilder))
  }

  private def brokerMetricTags(brokerId: Int) = Map("broker-id" -> brokerId.toString)

  private def removeExistingBroker(brokerState: ControllerBrokerStateInfo): Unit = {
    try {
      // Shutdown the RequestSendThread before closing the NetworkClient to avoid the concurrent use of the
      // non-threadsafe classes as described in KAFKA-4959.
      // The call to shutdownLatch.await() in ShutdownableThread.shutdown() serves as a synchronization barrier that
      // hands off the NetworkClient from the RequestSendThread to the ZkEventThread.
      brokerState.reconfigurableChannelBuilder.foreach(config.removeReconfigurable)
      brokerState.requestSendThread.shutdown()
      brokerState.networkClient.close()
      brokerState.messageQueue.clear()
      removeMetric(QueueSizeMetricName, brokerMetricTags(brokerState.brokerNode.id))
      removeMetric(RequestRateAndQueueTimeMetricName, brokerMetricTags(brokerState.brokerNode.id))
      brokerStateInfo.remove(brokerState.brokerNode.id)
    } catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }
  }

  protected def startRequestSendThread(brokerId: Int): Unit = {
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    /**
     * 只启动线程状态是NEW的线程，保证了不会重复启动。
     * */
    if (requestThread.getState == Thread.State.NEW)
      requestThread.start()
  }
}

case class QueueItem(apiKey: ApiKeys, request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                     callback: AbstractResponse => Unit, enqueueTimeMs: Long)

class RequestSendThread(val controllerId: Int,
                        val controllerContext: ControllerContext,
                        val queue: BlockingQueue[QueueItem],
                        val networkClient: NetworkClient,
                        val brokerNode: Node,
                        val config: KafkaConfig,
                        val time: Time,
                        val requestRateAndQueueTimeMetrics: Timer,
                        val stateChangeLogger: StateChangeLogger,
                        name: String)
  extends ShutdownableThread(name = name) {

  logIdent = s"[RequestSendThread controllerId=$controllerId] "

  private val socketTimeoutMs = config.controllerSocketTimeoutMs

  override def doWork(): Unit = {

    def backoff(): Unit = pause(100, TimeUnit.MILLISECONDS)

    val QueueItem(apiKey, requestBuilder, callback, enqueueTimeMs) = queue.take()
    requestRateAndQueueTimeMetrics.update(time.milliseconds() - enqueueTimeMs, TimeUnit.MILLISECONDS)

    var clientResponse: ClientResponse = null
    try {
      var isSendSuccessful = false
      while (isRunning && !isSendSuccessful) {
        // if a broker goes down for a long time, then at some point the controller's zookeeper listener will trigger a
        // removeBroker which will invoke shutdown() on this thread. At that point, we will stop retrying.
        try {
          if (!brokerReady()) {
            isSendSuccessful = false
            backoff()
          }
          else {
            val clientRequest = networkClient.newClientRequest(brokerNode.idString, requestBuilder,
              time.milliseconds(), true)
            clientResponse = NetworkClientUtils.sendAndReceive(networkClient, clientRequest, time)
            isSendSuccessful = true
          }
        } catch {
          case e: Throwable => // if the send was not successful, reconnect to broker and resend the message
            warn(s"Controller $controllerId epoch ${controllerContext.epoch} fails to send request $requestBuilder " +
              s"to broker $brokerNode. Reconnecting to broker.", e)
            networkClient.close(brokerNode.idString)
            isSendSuccessful = false
            backoff()
        }
      }
      if (clientResponse != null) {
        val requestHeader = clientResponse.requestHeader
        val api = requestHeader.apiKey
        if (api != ApiKeys.LEADER_AND_ISR && api != ApiKeys.STOP_REPLICA && api != ApiKeys.UPDATE_METADATA)
          throw new KafkaException(s"Unexpected apiKey received: $apiKey")

        val response = clientResponse.responseBody

        stateChangeLogger.withControllerEpoch(controllerContext.epoch).trace(s"Received response " +
          s"${response.toString(requestHeader.apiVersion)} for request $api with correlation id " +
          s"${requestHeader.correlationId} sent to broker $brokerNode")

        if (callback != null) {
          callback(response)
        }
      }
    } catch {
      case e: Throwable =>
        error(s"Controller $controllerId fails to send a request to broker $brokerNode", e)
        // If there is any socket error (eg, socket timeout), the connection is no longer usable and needs to be recreated.
        networkClient.close(brokerNode.idString)
    }
  }

  private def brokerReady(): Boolean = {
    try {
      if (!NetworkClientUtils.isReady(networkClient, brokerNode, time.milliseconds())) {
        if (!NetworkClientUtils.awaitReady(networkClient, brokerNode, time, socketTimeoutMs))
          throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

        info(s"Controller $controllerId connected to $brokerNode for sending state change requests")
      }

      true
    } catch {
      case e: Throwable =>
        warn(s"Controller $controllerId's connection to broker $brokerNode was unsuccessful", e)
        networkClient.close(brokerNode.idString)
        false
    }
  }

  override def initiateShutdown(): Boolean = {
    if (super.initiateShutdown()) {
      networkClient.initiateClose()
      true
    } else
      false
  }
}

class ControllerBrokerRequestBatch(config: KafkaConfig,
                                   controllerChannelManager: ControllerChannelManager,
                                   controllerEventManager: ControllerEventManager,
                                   controllerContext: ControllerContext,
                                   stateChangeLogger: StateChangeLogger)
  extends AbstractControllerBrokerRequestBatch(config, controllerContext, stateChangeLogger) {

  def sendEvent(event: ControllerEvent): Unit = {
    controllerEventManager.put(event)
  }

  def sendRequest(brokerId: Int,
                  request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit = {
    controllerChannelManager.sendRequest(brokerId, request, callback)
  }

}

abstract class AbstractControllerBrokerRequestBatch(config: KafkaConfig,
                                                    controllerContext: ControllerContext,
                                                    stateChangeLogger: StateChangeLogger) extends Logging {
  val controllerId: Int = config.brokerId
  /**
   * leaderAndIsrRequestMap用来收集需要发送leaderAndIsr请求的partition
   * */
  val leaderAndIsrRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, LeaderAndIsrPartitionState]]
  /**
   * stopReplicaRequestMap用来收集需要发送stopReplica请求的partition
   * */
  val stopReplicaRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, StopReplicaPartitionState]]
  /**
   * updateMetadataRequestBrokerSet用来收集需要发送metadata更新请求的broker集合；
   * updateMetadataRequestPartitionInfoMap用来收集需要发送metadata更新请求的partition集合;
   * */
  val updateMetadataRequestBrokerSet = mutable.Set.empty[Int]
  val updateMetadataRequestPartitionInfoMap = mutable.Map.empty[TopicPartition, UpdateMetadataPartitionState]

  def sendEvent(event: ControllerEvent): Unit

  def sendRequest(brokerId: Int,
                  request: AbstractControlRequest.Builder[_ <: AbstractControlRequest],
                  callback: AbstractResponse => Unit = null): Unit

  /**
   * 在每次发送metadata请求等前，需要看下之前的metadata请求是否完成，完成后才能发送下次请求.
   * 判断之前的发送请求是否发送完成：
   * ①leader and isr;②stop replica;③更新metadata请求;
   *
   * 这三个结构在要发之前添加，在发送后，clear掉。不会等收到response才clear掉.
   * */
  def newBatch(): Unit = {
    // raise error if the previous batch is not empty
    if (leaderAndIsrRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        s"a new one. Some LeaderAndIsr state changes $leaderAndIsrRequestMap might be lost ")
    if (stopReplicaRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        s"new one. Some StopReplica state changes $stopReplicaRequestMap might be lost ")
    if (updateMetadataRequestBrokerSet.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        s"new one. Some UpdateMetadata state changes to brokers $updateMetadataRequestBrokerSet with partition info " +
        s"$updateMetadataRequestPartitionInfoMap might be lost ")
  }

  def clear(): Unit = {
    leaderAndIsrRequestMap.clear()
    stopReplicaRequestMap.clear()
    updateMetadataRequestBrokerSet.clear()
    updateMetadataRequestPartitionInfoMap.clear()
  }

  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int],
                                       topicPartition: TopicPartition,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicaAssignment: ReplicaAssignment,
                                       isNew: Boolean): Unit = {

    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val result = leaderAndIsrRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      val alreadyNew = result.get(topicPartition).exists(_.isNew)
      val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
      result.put(topicPartition, new LeaderAndIsrPartitionState()
        .setTopicName(topicPartition.topic)
        .setPartitionIndex(topicPartition.partition)
        .setControllerEpoch(leaderIsrAndControllerEpoch.controllerEpoch)
        .setLeader(leaderAndIsr.leader)
        .setLeaderEpoch(leaderAndIsr.leaderEpoch)
        .setIsr(leaderAndIsr.isr.map(Integer.valueOf).asJava)
        .setZkVersion(leaderAndIsr.zkVersion)
        .setReplicas(replicaAssignment.replicas.map(Integer.valueOf).asJava)
        .setAddingReplicas(replicaAssignment.addingReplicas.map(Integer.valueOf).asJava)
        .setRemovingReplicas(replicaAssignment.removingReplicas.map(Integer.valueOf).asJava)
        .setIsNew(isNew || alreadyNew))
    }

    addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
  }

  def addStopReplicaRequestForBrokers(brokerIds: Seq[Int],
                                      topicPartition: TopicPartition,
                                      deletePartition: Boolean): Unit = {
    // A sentinel (-2) is used as an epoch if the topic is queued for deletion. It overrides
    // any existing epoch.
    val leaderEpoch = if (controllerContext.isTopicQueuedUpForDeletion(topicPartition.topic)) {
      LeaderAndIsr.EpochDuringDelete
    } else {
      controllerContext.partitionLeadershipInfo(topicPartition)
        .map(_.leaderAndIsr.leaderEpoch)
        .getOrElse(LeaderAndIsr.NoEpoch)
    }

    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val result = stopReplicaRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      val alreadyDelete = result.get(topicPartition).exists(_.deletePartition)
      result.put(topicPartition, new StopReplicaPartitionState()
          .setPartitionIndex(topicPartition.partition())
          .setLeaderEpoch(leaderEpoch)
          .setDeletePartition(alreadyDelete || deletePartition))
    }
  }

  /** Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted */
  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: collection.Set[TopicPartition]): Unit = {

    def updateMetadataRequestPartitionInfo(partition: TopicPartition, beingDeleted: Boolean): Unit = {
      /**
       * 在0.11.0版本controllerContext是定义的成员变量.
       * */
      controllerContext.partitionLeadershipInfo(partition) match {
          /**
           * 这里相当于是强制类型检查，0.11.0是用@符号来进行检查的：
           * LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)
           * */
        case Some(LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)) =>
          /**
           * 取出该partiton的AR
           * */
          val replicas = controllerContext.partitionReplicaAssignment(partition)
          /**
           * 过滤出offline partition
           * */
          val offlineReplicas = replicas.filter(!controllerContext.isReplicaOnline(_, partition))
          /**
           * 如果是正准备删除的partiton，那么构建的leaderAndIsr对象中，leader broker为-2.
           * */
          val updatedLeaderAndIsr =
            if (beingDeleted) LeaderAndIsr.duringDelete(leaderAndIsr.isr)
            else leaderAndIsr


          /**
           * 在0.11.0中使用的是PartitionStateInfo来承载该信息，
           * 但在这个版本中用了一个新的类UpdateMetadataPartitionState来承载。
           * 这个是自动生成的类，这样在发送的时候，就不用再转化一次了。
           *
           * UpdateMetadataPartitionState是属于自动生成协议类里面的.
           *
           * Jackson类库包含了很多注解，
           * 可以让我们快速建立Java类与JSON之间的关系。详细文档可以参考Jackson-Annotations。
           * */
          val partitionStateInfo = new UpdateMetadataPartitionState()
            .setTopicName(partition.topic)
            .setPartitionIndex(partition.partition)
            .setControllerEpoch(controllerEpoch)
            .setLeader(updatedLeaderAndIsr.leader)
            .setLeaderEpoch(updatedLeaderAndIsr.leaderEpoch)
            .setIsr(updatedLeaderAndIsr.isr.map(Integer.valueOf).asJava)
            .setZkVersion(updatedLeaderAndIsr.zkVersion)
            .setReplicas(replicas.map(Integer.valueOf).asJava)
            .setOfflineReplicas(offlineReplicas.map(Integer.valueOf).asJava)
          updateMetadataRequestPartitionInfoMap.put(partition, partitionStateInfo)

        case None =>
          info(s"Leader not yet assigned for partition $partition. Skip sending UpdateMetadataRequest.")
      }
    }


    updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0)
    partitions.foreach(partition => updateMetadataRequestPartitionInfo(partition,
      beingDeleted = controllerContext.topicsToBeDeleted.contains(partition.topic)))
  }

  private def sendLeaderAndIsrRequest(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    /**
     * 根据kafka版本获取对应的leaderAndIsr版本号
     * */
    val leaderAndIsrRequestVersion: Short =
      if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 4
      else if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV0) 3
      else if (config.interBrokerProtocolVersion >= KAFKA_2_2_IV0) 2
      else if (config.interBrokerProtocolVersion >= KAFKA_1_0_IV0) 1
      else 0

    leaderAndIsrRequestMap.forKeyValue { (broker, leaderAndIsrPartitionStates) =>
      /**
       * 这里挂了的broker也要发吗
       * 能发出去吗
       * */
      if (controllerContext.liveOrShuttingDownBrokerIds.contains(broker)) {
        /**
         * leaderIds存了本次leaderAndIsr所携带的leader
         * */
        val leaderIds = mutable.Set.empty[Int]
        /**
         * 本次请求中要切为leader的个数。
         * 应该不一定是从follower变为leader。
         * LeaderAndIsr请求也可能是从leader切leader吧？
         * */
        var numBecomeLeaders = 0

        /**
         * 这里主要是填充上面两个变量
         * */
        leaderAndIsrPartitionStates.forKeyValue { (topicPartition, state) =>
          leaderIds += state.leader
          val typeOfRequest = if (broker == state.leader) {
            numBecomeLeaders += 1
            "become-leader"
          } else {
            "become-follower"
          }
          if (stateChangeLog.isTraceEnabled)
            stateChangeLog.trace(s"Sending $typeOfRequest LeaderAndIsr request $state to broker $broker for partition $topicPartition")
        }
        stateChangeLog.info(s"Sending LeaderAndIsr request to broker $broker with $numBecomeLeaders become-leader " +
          s"and ${leaderAndIsrPartitionStates.size - numBecomeLeaders} become-follower partitions")

        /**
         *  将leader按照端口号来构建node对象。
         *  首先过滤出leader的broker，然后根据填的内部broker之间的通信协议来构建Node对象.
         * */
          val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id))
            .map {
          _.node(config.interBrokerListenerName)
        }

        /**
         * broker其实是brokerId，从brokerId和epoch的映射中拿到对应的epoch.
         * */
        val brokerEpoch = controllerContext.liveBrokerIdAndEpochs(broker)

        val leaderAndIsrRequestBuilder = new LeaderAndIsrRequest.Builder(leaderAndIsrRequestVersion, controllerId,
          controllerEpoch, brokerEpoch, leaderAndIsrPartitionStates.values.toBuffer.asJava, leaders.asJava)

        /**
         * 这里往broker对应的队列放入一个leaderAndIsr请求并且构建一个回调(该回调方法里，会构建LeaderAndIsr处理事件，并放入controller的事件处理队列)，
         * 然后下游会有发送线程从中取出，来进行发送,发送完后出发回调。
         *
         * trunk版本会构建对应的事件来处理对应的response，leaderAndIsr、updateMetadata、stopReplica这三类请求都会有对应事件来处理：
         * LeaderAndIsrResponseReceived、UpdateMetadataResponseReceived、TopicDeletionStopReplicaResponseReceived
         * */
        sendRequest(broker, leaderAndIsrRequestBuilder, (r: AbstractResponse) => {
          val leaderAndIsrResponse = r.asInstanceOf[LeaderAndIsrResponse]
          sendEvent(LeaderAndIsrResponseReceived(leaderAndIsrResponse, broker))
        })
      }
    }

    /**
     * 发送完成后，清理掉，以便下次继续发送
     * */
    leaderAndIsrRequestMap.clear()
  }

  private def sendUpdateMetadataRequests(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    stateChangeLog.info(s"Sending UpdateMetadata request to brokers $updateMetadataRequestBrokerSet " +
      s"for ${updateMetadataRequestPartitionInfoMap.size} partitions")

    /**
     * 将updateMetadataRequestPartitionInfoMap的values转换为ArrayBuffer:
     * ArrayBuffer(UpdateMetadataPartitionState,UpdateMetadataPartitionState,UpdateMetadataPartitionState,....)
     * */
    val partitionStates = updateMetadataRequestPartitionInfoMap.values.toBuffer
    /**
     * 获取updateMetadataRequest rpc的版本号,根据不同版本号，会做不同处理.
     * */
    val updateMetadataRequestVersion: Short =
      if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 6
      else if (config.interBrokerProtocolVersion >= KAFKA_2_2_IV0) 5
      else if (config.interBrokerProtocolVersion >= KAFKA_1_0_IV0) 4
      else if (config.interBrokerProtocolVersion >= KAFKA_0_10_2_IV0) 3
      else if (config.interBrokerProtocolVersion >= KAFKA_0_10_0_IV1) 2
      else if (config.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
      else 0


    /**
     *
     * */
    val liveBrokers = controllerContext.liveOrShuttingDownBrokers.iterator.map { broker =>
      val endpoints = if (updateMetadataRequestVersion == 0) {
        /**
         * 0.9.0之前的版本只能支持PLAINTEXT
         * */
        // Version 0 of UpdateMetadataRequest only supports PLAINTEXT
        val securityProtocol = SecurityProtocol.PLAINTEXT
        /**
         * 通过安全协议转换为listenerName
         * */
        val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
        val node = broker.node(listenerName)
        Seq(new UpdateMetadataEndpoint()
          .setHost(node.host)
          .setPort(node.port)
          .setSecurityProtocol(securityProtocol.id)
          .setListener(listenerName.value))
      } else {
        broker.endPoints.map { endpoint =>
          new UpdateMetadataEndpoint()
            .setHost(endpoint.host)
            .setPort(endpoint.port)
            .setSecurityProtocol(endpoint.securityProtocol.id)
            .setListener(endpoint.listenerName.value)
        }
      }
      new UpdateMetadataBroker()
        .setId(broker.id)
        .setEndpoints(endpoints.asJava)
        .setRack(broker.rack.orNull)
    }.toBuffer

    /**
     *  Set.intersect求两个集合的交集,因为如果不是我们broker列表里面的节点，是不需要发送metadata的。
     *  然后遍历每个交集中的节点，构建metadata请求，这里有个问题，
     *  构建metadata请求的brokerEpoch是从liveBroker中获取的,那么如果交集中的某个节点是downbroker呢，已经是挂掉的broker，
     *  那么controllerContext.liveBrokerIdAndEpochs(broker)获取的就是null，这样是不是有问题？
     *  controllerContext.liveBrokerIdAndEpochs是一个map，它的定义是：
     *  private val liveBrokerEpochs = mutable.Map.empty[Int, Long]
     *  从中获取元素可以有两种方式：liveBrokerEpochs.get(3) 或 liveBrokerEpochs(3)
     *  liveBrokerEpochs(3)获取会抛异常。
     *  所以这里如果broker在liveBrokerIdAndEpochs中不存在，那么这里会抛异常:
     *      Exception in thread "main" java.util.NoSuchElementException: key not found: 3
     *      at scala.collection.MapLike$class.default(MapLike.scala:228)
     *      at scala.collection.AbstractMap.default(Map.scala:59)
     *      at scala.collection.mutable.HashMap.apply(HashMap.scala:65)
     *      at Test1$.main(Test1.scala:16)
     *      at Test1.main(Test1.scala)
     *
     *   有没有可能存在一个broker，在liveOrShuttingDownBrokerIds和updateMetadataRequestBrokerSet中，
     *   但是不在liveBrokerIdAndEpochs中。
     *
     *
     * */
    updateMetadataRequestBrokerSet.intersect(controllerContext.liveOrShuttingDownBrokerIds).foreach { broker =>
      val brokerEpoch = controllerContext.liveBrokerIdAndEpochs(broker)

      val updateMetadataRequestBuilder = new UpdateMetadataRequest.Builder(updateMetadataRequestVersion,
        controllerId, controllerEpoch, brokerEpoch, partitionStates.asJava, liveBrokers.asJava)


      sendRequest(broker, updateMetadataRequestBuilder, (r: AbstractResponse) => {
        val updateMetadataResponse = r.asInstanceOf[UpdateMetadataResponse]
        sendEvent(UpdateMetadataResponseReceived(updateMetadataResponse, broker))
      })

    }


    updateMetadataRequestBrokerSet.clear()
    updateMetadataRequestPartitionInfoMap.clear()
  }

  private def sendStopReplicaRequests(controllerEpoch: Int, stateChangeLog: StateChangeLogger): Unit = {
    val traceEnabled = stateChangeLog.isTraceEnabled
    val stopReplicaRequestVersion: Short =
      if (config.interBrokerProtocolVersion >= KAFKA_2_6_IV0) 3
      else if (config.interBrokerProtocolVersion >= KAFKA_2_4_IV1) 2
      else if (config.interBrokerProtocolVersion >= KAFKA_2_2_IV0) 1
      else 0

    def responseCallback(brokerId: Int, isPartitionDeleted: TopicPartition => Boolean)
                        (response: AbstractResponse): Unit = {
      val stopReplicaResponse = response.asInstanceOf[StopReplicaResponse]
      val partitionErrorsForDeletingTopics = mutable.Map.empty[TopicPartition, Errors]
      stopReplicaResponse.partitionErrors.forEach { pe =>
        val tp = new TopicPartition(pe.topicName, pe.partitionIndex)
        if (controllerContext.isTopicDeletionInProgress(pe.topicName) &&
            isPartitionDeleted(tp)) {
          partitionErrorsForDeletingTopics += tp -> Errors.forCode(pe.errorCode)
        }
      }
      if (partitionErrorsForDeletingTopics.nonEmpty)
        sendEvent(TopicDeletionStopReplicaResponseReceived(brokerId, stopReplicaResponse.error,
          partitionErrorsForDeletingTopics))
    }

    stopReplicaRequestMap.forKeyValue { (brokerId, partitionStates) =>
      if (controllerContext.liveOrShuttingDownBrokerIds.contains(brokerId)) {
        if (traceEnabled)
          partitionStates.forKeyValue { (topicPartition, partitionState) =>
            stateChangeLog.trace(s"Sending StopReplica request $partitionState to " +
              s"broker $brokerId for partition $topicPartition")
          }

        val brokerEpoch = controllerContext.liveBrokerIdAndEpochs(brokerId)
        if (stopReplicaRequestVersion >= 3) {
          val stopReplicaTopicState = mutable.Map.empty[String, StopReplicaTopicState]
          partitionStates.forKeyValue { (topicPartition, partitionState) =>
            val topicState = stopReplicaTopicState.getOrElseUpdate(topicPartition.topic,
              new StopReplicaTopicState().setTopicName(topicPartition.topic))
            topicState.partitionStates().add(partitionState)
          }

          stateChangeLog.info(s"Sending StopReplica request for ${partitionStates.size} " +
            s"replicas to broker $brokerId")
          val stopReplicaRequestBuilder = new StopReplicaRequest.Builder(
            stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
            false, stopReplicaTopicState.values.toBuffer.asJava)
          sendRequest(brokerId, stopReplicaRequestBuilder,
            responseCallback(brokerId, tp => partitionStates.get(tp).exists(_.deletePartition)))
        } else {
          var numPartitionStateWithDelete = 0
          var numPartitionStateWithoutDelete = 0
          val topicStatesWithDelete = mutable.Map.empty[String, StopReplicaTopicState]
          val topicStatesWithoutDelete = mutable.Map.empty[String, StopReplicaTopicState]

          partitionStates.forKeyValue { (topicPartition, partitionState) =>
            val topicStates = if (partitionState.deletePartition()) {
              numPartitionStateWithDelete += 1
              topicStatesWithDelete
            } else {
              numPartitionStateWithoutDelete += 1
              topicStatesWithoutDelete
            }
            val topicState = topicStates.getOrElseUpdate(topicPartition.topic,
              new StopReplicaTopicState().setTopicName(topicPartition.topic))
            topicState.partitionStates().add(partitionState)
          }

          if (topicStatesWithDelete.nonEmpty) {
            stateChangeLog.info(s"Sending StopReplica request (delete = true) for " +
              s"$numPartitionStateWithDelete replicas to broker $brokerId")
            val stopReplicaRequestBuilder = new StopReplicaRequest.Builder(
              stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
              true, topicStatesWithDelete.values.toBuffer.asJava)
            sendRequest(brokerId, stopReplicaRequestBuilder, responseCallback(brokerId, _ => true))
          }

          if (topicStatesWithoutDelete.nonEmpty) {
            stateChangeLog.info(s"Sending StopReplica request (delete = false) for " +
              s"$numPartitionStateWithoutDelete replicas to broker $brokerId")
            val stopReplicaRequestBuilder = new StopReplicaRequest.Builder(
              stopReplicaRequestVersion, controllerId, controllerEpoch, brokerEpoch,
              false, topicStatesWithoutDelete.values.toBuffer.asJava)
            sendRequest(brokerId, stopReplicaRequestBuilder)
          }
        }
      }
    }

    stopReplicaRequestMap.clear()
  }

  /**
   * 发送请求给broker，都是按照这三个流程来发送的：
   * 先发leaderAndIsr(内部先协商好，调班)，
   * 然后发metadata(公布给外面，别人就会找新的工作人员来处理问题),
   * 停止废弃replica(下掉退休人员)
   *
   * kafka controller和broker之间的通信，都是按照这三步来进行的。
   * */
  def sendRequestsToBrokers(controllerEpoch: Int): Unit = {
    try {
      /**
       * 相对于0.11.0，这里增加了stateChangeLogger,这个类是用来打日志的，目的是为了打日志的时候，加上一个controller epoch号的前缀;
       *
       * 但这里每次发送都会new一个新的StateChangeLogger对象 ，感觉有问题。
       *
       * 终于明白了，首先用这个类的目的是为了，打日志的时候，可以自动加上controller epoch号，而不用像之前那样进行日志拼接(很麻烦)。
       * 但这里为了线程安全(有可能很多线程都会用这个打日志，如果每次打日志前set下logIdent变量值，那么可能其他线程下一秒就给修改了，这样就不能做到日志和epoch号的一致性)，
       * 所以没办法只能在用的时候new下(因为这个日志类很轻，应该还好。)
       * */
      val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerEpoch)
      /**
       * 发送leaderAndIsr请求：放入发送队列=> 发送线程取出发送
       * => 收到response，触发callback方法，方法里构建event事件，放入事件处理队列=>
       * 事件处理线程取出事件，进行处理.
       *
       * leaderAndIsr的作用：
       * ①切换leader和follower;
       * ②
       * */
      sendLeaderAndIsrRequest(controllerEpoch, stateChangeLog)





      /**
       *
       * */
      sendUpdateMetadataRequests(controllerEpoch, stateChangeLog)



      sendStopReplicaRequests(controllerEpoch, stateChangeLog)
    } catch {
      case e: Throwable =>
        if (leaderAndIsrRequestMap.nonEmpty) {
          error("Haven't been able to send leader and isr requests, current state of " +
            s"the map is $leaderAndIsrRequestMap. Exception message: $e")
        }
        if (updateMetadataRequestBrokerSet.nonEmpty) {
          error(s"Haven't been able to send metadata update requests to brokers $updateMetadataRequestBrokerSet, " +
            s"current state of the partition info is $updateMetadataRequestPartitionInfoMap. Exception message: $e")
        }
        if (stopReplicaRequestMap.nonEmpty) {
          error("Haven't been able to send stop replica requests, current state of " +
            s"the map is $stopReplicaRequestMap. Exception message: $e")
        }
        throw new IllegalStateException(e)
    }
  }
}

/**
 * 相对于0.11.0，这里增加了requestRateAndTimeMetrics: Timer和reconfigurableChannelBuilder: Option[Reconfigurable]
 * */
case class ControllerBrokerStateInfo(networkClient: NetworkClient,
                                     brokerNode: Node,
                                     messageQueue: BlockingQueue[QueueItem],
                                     requestSendThread: RequestSendThread,
                                     queueSizeGauge: Gauge[Int],
                                     requestRateAndTimeMetrics: Timer,
                                     reconfigurableChannelBuilder: Option[Reconfigurable])

