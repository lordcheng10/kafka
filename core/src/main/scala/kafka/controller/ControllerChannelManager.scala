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
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import com.yammer.metrics.core.Gauge
import kafka.api._
import kafka.cluster.Broker
import kafka.common.{KafkaException, TopicAndPartition}
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.clients._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.protocol.{ApiKeys, SecurityProtocol}
import org.apache.kafka.common.requests.UpdateMetadataRequest.EndPoint
import org.apache.kafka.common.requests.{UpdateMetadataRequest, _}
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.{Node, TopicPartition, requests}

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.collection.{Set, mutable}

/**
 * 在trunk版本中，这里增加了一个定义：
 * val RequestRateAndQueueTimeMetricName = "RequestRateAndQueueTimeMs"
 * 这里的变量作用是作为metric的name。
 * */
object ControllerChannelManager {
  val QueueSizeMetricName = "QueueSize"
}

/**
 * 这个类的作用是用于管理controller到broker之间的通信channel。
 *
 * */
class ControllerChannelManager(controllerContext: ControllerContext,
                               config: KafkaConfig,
                               time: Time,
                               metrics: Metrics,
                               threadNamePrefix: Option[String] = None) extends Logging with KafkaMetricsGroup {
  import ControllerChannelManager._

  /**
   * 这里保存的是用于和每个broker通信的信息，比如：发送队列、发送线程等。key是brokerId.
   * controller到每一个broker之间都有一个单独的队列和发送线程。
   * */
  protected val brokerStateInfo = new HashMap[Int, ControllerBrokerStateInfo]
  private val brokerLock = new Object
  this.logIdent = "[Channel manager on controller " + config.brokerId + "]: "

  /**
   * 统计controller到broker总的队列大小，这里应该拆开，总和指标没有意义.
   * */
  newGauge(
    "TotalQueueSize",
    new Gauge[Int] {
      def value: Int = brokerLock synchronized {
        brokerStateInfo.values.iterator.map(_.messageQueue.size).sum
      }
    }
  )

  /**
   * 在trunk版本中，这里是放到startup中的，
   * 并且添加的是全量broker节点:liveOrShuttingDownBrokers
   *
   * 这里感觉有bug
   *
   * trunk版本 不会出现一个发送线程被启动两次的情况，因为成为controller事件也是放到事件队列中的，然后下游都是单线程处理的。当然也不会出现同一个broker被添加两次的情况。
   *
   * 但这里总感觉会加两次。。。,不会加两次，因为，0.11.0版本也是基于事件处理的，下游一样是单线程处理，大家都是放到队列中排队处理，不会出现同时有两个或者多个线程同时添加或启动线程的情况。
   *
   *
   * 这行代码要执行，只会在两个事件中：①Reelect（重新选举成为controller事件）；②startUp事件;这两个事件依然是放到事件队列中的，所以这里不会有bug。
   *
   * */
  controllerContext.liveBrokers.foreach(addNewBroker)

  def startup() = {
    brokerLock synchronized {
      brokerStateInfo.foreach(brokerState => startRequestSendThread(brokerState._1))
    }
  }

  def shutdown() = {
    brokerLock synchronized {
      brokerStateInfo.values.foreach(removeExistingBroker)
    }
  }

  def sendRequest(brokerId: Int, apiKey: ApiKeys, request: AbstractRequest.Builder[_ <: AbstractRequest],
                  callback: AbstractResponse => Unit = null) {
    brokerLock synchronized {
      val stateInfoOpt = brokerStateInfo.get(brokerId)
      stateInfoOpt match {
        case Some(stateInfo) =>
          stateInfo.messageQueue.put(QueueItem(apiKey, request, callback))
        case None =>
          warn("Not sending request %s to broker %d, since it is offline.".format(request, brokerId))
      }
    }
  }

  /**
   * trunk版本中，这个方法在BrokerChange变化事件里面被调用了两次，但是在0.11.0版本里面，只被调用了一次。
   * 因为在trunk版本里面，broker的epoch版本号变化的也要调用这个方法，而不只是新增的。
   *
   * 那broker版本号变化的broker，什么情况下出现呢？不管什么情况下出现，只要变化了，
   * 就该重新构建下链接，先删除，再重新add进来，也就是重新关闭后再建立链接，也就是先removeBroker再addBroker
   * */
  def addBroker(broker: Broker) {
    // be careful here. Maybe the startup() API has already started the request send thread
    brokerLock synchronized {
      if(!brokerStateInfo.contains(broker.id)) {
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

  def removeBroker(brokerId: Int) {
    brokerLock synchronized {
      removeExistingBroker(brokerStateInfo(brokerId))
    }
  }

  /**
   * 这里也是这样的思路，把和broker通信要用的东西构建好后，
   * 放到ControllerBrokerStateInfo对象，并放入brokerStateInfo中.
   * 和trunk版本一样的思路.
   * */
  private def addNewBroker(broker: Broker) {
    /**
     * trunk版本也是这么定义messageQueue的。
     * */
    val messageQueue = new LinkedBlockingQueue[QueueItem]
    debug("Controller %d trying to connect to broker %d".format(config.brokerId, broker.id))
    /**
     * 因为0.11.0版本没有从内部listerName中区分cotnroller和broker通信、broker与broker通信的listerName，所以这里没和trunk那样麻烦，
     * 直接用了配置的内部listerName来构建node.
     *
     * trunk版本多了controllerToBrokerSecurityProtocol和controllerToBrokerListenerName的构建
     * */
    val brokerNode = broker.getNode(config.interBrokerListenerName)

    /**
     * trunk版本多了LogContext对象构建，LogContext相当于一个logger工厂，
     * 从这个工厂里构建的logger，打的日志有相同前缀，
     * 这个前缀是我们在构架LogContext对象的时候指定的。
     *
     *
     * 另外还多了一个reconfigurableChannelBuilder，这个实际就是channelBuilder，只不过是支持动态修改配置的channelBuilder。
     * */
    val networkClient = {
      /**
       * trunk版本 在这里就多了对channelBuilder进行动态配置的操作
       * */
      val channelBuilder = ChannelBuilders.clientChannelBuilder(
        config.interBrokerSecurityProtocol,
        JaasContext.Type.SERVER,
        config,
        config.interBrokerListenerName,
        config.saslMechanismInterBrokerProtocol,
        config.saslInterBrokerHandshakeRequestEnable
      )
      val selector = new Selector(
        NetworkReceive.UNLIMITED,
        Selector.NO_IDLE_TIMEOUT_MS,
        metrics,
        time,
        "controller-channel",
        Map("broker-id" -> broker.id.toString).asJava,
        false,
        channelBuilder
      )
      new NetworkClient(
        selector,
        new ManualMetadataUpdater(Seq(brokerNode).asJava),
        config.brokerId.toString,
        1,
        0,
        0,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        Selectable.USE_DEFAULT_BUFFER_SIZE,
        config.requestTimeoutMs,
        time,
        false,
        new ApiVersions
      )
    }

    /**
     * 这里的字符串格式化，trunk版本也改了，不再用format了，而是用：
     * case None => s"Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
     * case Some(name) => s"$name:Controller-${config.brokerId}-to-broker-${broker.id}-send-thread"
     *
     * 内容倒没变。这个前缀threadNamePrefix是传进来的。
     * threadNamePrefix前缀是传进来的，应该是留的一个口，但实际没有用到，追踪了下，传入的全部是None，也不支持配置
     * */
    val threadName = threadNamePrefix match {
      case None => "Controller-%d-to-broker-%d-send-thread".format(config.brokerId, broker.id)
      case Some(name) => "%s:Controller-%d-to-broker-%d-send-thread".format(name, config.brokerId, broker.id)
    }

    /**
     * trunk这里还增加了一个requestRateAndQueueTimeMetrics的metric来统计请求发送e率以及请求的队列挤压耗时统计
     *
     *  构建controller和broker通信用的发送线程;
     *  setDaemon(true):表示设置为守护线程，进程退出不用等这个线程；
     *  setDaemon(false):表示设置为非守护线程，进程退出必须要等这个线程；
     *
     *  RequestSendThread的传入参数也有变化，trunk版本多传入了requestRateAndQueueTimeMetrics和stateChangeLogger，
     *  stateChangeLogger是一个状态变化日志类，打日志用的。0.11版本没传进来，那RequestSendThread这类怎么打日志呢？
     *  RequestSendThread继承了ShutdownableThread类，ShutdownableThread继承了Logging接口，自身可以打日志。
     *  只不过trunk版本认为里面涉及到一些应该打到状态日志里的，所以才传入了状态日志类：
     * stateChangeLogger.withControllerEpoch(controllerContext.epoch).trace(s"Received response "
     * s"${response.toString(requestHeader.apiVersion)} for request $api with correlation id " +
     * s"${requestHeader.correlationId} sent to broker $brokerNode")
     *
     * 其实也就是把每次发送请求给broker收到responose这个过程信息打出来。
     *
     * */
    val requestThread = new RequestSendThread(config.brokerId, controllerContext, messageQueue, networkClient,
      brokerNode, config, time, threadName)
    requestThread.setDaemon(false)

    /**
     * trunk这个队列统计的metric也有变化。
     * newGauge的第二个参数，应该是函数接口类吧 ？在trunk版本中直接传入了一个函数，方法要求的参数类型都是 metric: Gauge[T]
     * 另外第三个参数，传入的变量名也改了：brokerMetricTags，实际是一样的，就只改了名。
     *
     * trunk这里传入函数方法，看起来是新版本scala的一个新特性。换个scala低版本就会报错.
     * 似乎高版本scala可以匹配到Gauge中适合的方法value()
     * */
    val queueSizeGauge = newGauge(
      QueueSizeMetricName,
      new Gauge[Int] {
        def value: Int = messageQueue.size
      },
      queueSizeTags(broker.id)
    )

    /**
     *
     *
     * */
    brokerStateInfo.put(broker.id, new ControllerBrokerStateInfo(networkClient, brokerNode, messageQueue,
      requestThread, queueSizeGauge))
  }

  private def queueSizeTags(brokerId: Int) = Map("broker-id" -> brokerId.toString)

  private def removeExistingBroker(brokerState: ControllerBrokerStateInfo) {
    try {
      // Shutdown the RequestSendThread before closing the NetworkClient to avoid the concurrent use of the
      // non-threadsafe classes as described in KAFKA-4959.
      // The call to shutdownLatch.await() in ShutdownableThread.shutdown() serves as a synchronization barrier that
      // hands off the NetworkClient from the RequestSendThread to the ZkEventThread.
      brokerState.requestSendThread.shutdown()
      brokerState.networkClient.close()
      brokerState.messageQueue.clear()
      removeMetric(QueueSizeMetricName, queueSizeTags(brokerState.brokerNode.id))
      brokerStateInfo.remove(brokerState.brokerNode.id)
    } catch {
      case e: Throwable => error("Error while removing broker by the controller", e)
    }
  }

  protected def startRequestSendThread(brokerId: Int) {
    val requestThread = brokerStateInfo(brokerId).requestSendThread
    if(requestThread.getState == Thread.State.NEW)
      requestThread.start()
  }
}

case class QueueItem(apiKey: ApiKeys, request: AbstractRequest.Builder[_ <: AbstractRequest],
                     callback: AbstractResponse => Unit)

class RequestSendThread(val controllerId: Int,
                        val controllerContext: ControllerContext,
                        val queue: BlockingQueue[QueueItem],
                        val networkClient: NetworkClient,
                        val brokerNode: Node,
                        val config: KafkaConfig,
                        val time: Time,
                        name: String)
  extends ShutdownableThread(name = name) {

  private val stateChangeLogger = KafkaController.stateChangeLogger
  private val socketTimeoutMs = config.controllerSocketTimeoutMs

  override def doWork(): Unit = {

    def backoff(): Unit = CoreUtils.swallowTrace(Thread.sleep(100))

    val QueueItem(apiKey, requestBuilder, callback) = queue.take()
    var clientResponse: ClientResponse = null
    try {
      var isSendSuccessful = false
      /**
       * 0.11.0版本，controller给broker发送请求，如果发送失败，会一直reconnect和retry.
       * trunk版本也是会一致retry，trunk版本会传callback方法，但这里的callback有些是null，比如发送leaderAndIsr请求，
       * 也就是说trunk版本会对response进行处理，0.11.0版本不会
       * */
      while (isRunning.get() && !isSendSuccessful) {
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
            warn(("Controller %d epoch %d fails to send request %s to broker %s. " +
              "Reconnecting to broker.").format(controllerId, controllerContext.epoch,
                requestBuilder.toString, brokerNode.toString), e)
            networkClient.close(brokerNode.idString)
            isSendSuccessful = false
            backoff()
        }
      }
      if (clientResponse != null) {
        val requestHeader = clientResponse.requestHeader
        val api = ApiKeys.forId(requestHeader.apiKey)
        if (api != ApiKeys.LEADER_AND_ISR && api != ApiKeys.STOP_REPLICA && api != ApiKeys.UPDATE_METADATA_KEY)
          throw new KafkaException(s"Unexpected apiKey received: $apiKey")

        val response = clientResponse.responseBody

        stateChangeLogger.trace("Controller %d epoch %d received response %s for a request sent to broker %s"
          .format(controllerId, controllerContext.epoch, response.toString(requestHeader.apiVersion), brokerNode.toString))

        if (callback != null) {
          callback(response)
        }
      }
    } catch {
      case e: Throwable =>
        error("Controller %d fails to send a request to broker %s".format(controllerId, brokerNode.toString), e)
        // If there is any socket error (eg, socket timeout), the connection is no longer usable and needs to be recreated.
        networkClient.close(brokerNode.idString)
    }
  }

  private def brokerReady(): Boolean = {
    try {
      if (!NetworkClientUtils.isReady(networkClient, brokerNode, time.milliseconds())) {
        if (!NetworkClientUtils.awaitReady(networkClient, brokerNode, time, socketTimeoutMs))
          throw new SocketTimeoutException(s"Failed to connect within $socketTimeoutMs ms")

        info("Controller %d connected to %s for sending state change requests".format(controllerId, brokerNode.toString))
      }

      true
    } catch {
      case e: Throwable =>
        warn("Controller %d's connection to broker %s was unsuccessful".format(controllerId, brokerNode.toString), e)
        networkClient.close(brokerNode.idString)
        false
    }
  }

}

class ControllerBrokerRequestBatch(controller: KafkaController) extends  Logging {
  val controllerContext = controller.controllerContext
  val controllerId: Int = controller.config.brokerId
  val leaderAndIsrRequestMap = mutable.Map.empty[Int, mutable.Map[TopicPartition, PartitionStateInfo]]
  val stopReplicaRequestMap = mutable.Map.empty[Int, Seq[StopReplicaRequestInfo]]
  val updateMetadataRequestBrokerSet = mutable.Set.empty[Int]
  val updateMetadataRequestPartitionInfoMap = mutable.Map.empty[TopicPartition, PartitionStateInfo]
  private val stateChangeLogger = KafkaController.stateChangeLogger

  /**
   * 判断之前的发送请求是否发送完成：
   * ①leader and isr;②stop replica;③更新metadata请求;
   *
   * 这三个结构在要发之前添加，在发送后，clear掉。不会等收到response才clear掉.
   * */
  def newBatch() {
    // raise error if the previous batch is not empty
    if (leaderAndIsrRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating " +
        "a new one. Some LeaderAndIsr state changes %s might be lost ".format(leaderAndIsrRequestMap.toString()))
    if (stopReplicaRequestMap.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some StopReplica state changes %s might be lost ".format(stopReplicaRequestMap.toString()))
    if (updateMetadataRequestBrokerSet.nonEmpty)
      throw new IllegalStateException("Controller to broker state change requests batch is not empty while creating a " +
        "new one. Some UpdateMetadata state changes to brokers %s with partition info %s might be lost ".format(
          updateMetadataRequestBrokerSet.toString(), updateMetadataRequestPartitionInfoMap.toString()))
  }

  def clear() {
    leaderAndIsrRequestMap.clear()
    stopReplicaRequestMap.clear()
    updateMetadataRequestBrokerSet.clear()
    updateMetadataRequestPartitionInfoMap.clear()
  }

  def addLeaderAndIsrRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int,
                                       leaderIsrAndControllerEpoch: LeaderIsrAndControllerEpoch,
                                       replicas: Seq[Int], callback: AbstractResponse => Unit = null) {
    val topicPartition = new TopicPartition(topic, partition)

    brokerIds.filter(_ >= 0).foreach { brokerId =>
      val result = leaderAndIsrRequestMap.getOrElseUpdate(brokerId, mutable.Map.empty)
      result.put(topicPartition, PartitionStateInfo(leaderIsrAndControllerEpoch, replicas))
    }

    addUpdateMetadataRequestForBrokers(controllerContext.liveOrShuttingDownBrokerIds.toSeq,
                                       Set(TopicAndPartition(topic, partition)))
  }

  def addStopReplicaRequestForBrokers(brokerIds: Seq[Int], topic: String, partition: Int, deletePartition: Boolean,
                                      callback: (AbstractResponse, Int) => Unit = null) {
    brokerIds.filter(b => b >= 0).foreach { brokerId =>
      stopReplicaRequestMap.getOrElseUpdate(brokerId, Seq.empty[StopReplicaRequestInfo])
      val v = stopReplicaRequestMap(brokerId)
      if(callback != null)
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition, (r: AbstractResponse) => callback(r, brokerId))
      else
        stopReplicaRequestMap(brokerId) = v :+ StopReplicaRequestInfo(PartitionAndReplica(topic, partition, brokerId),
          deletePartition)
    }
  }

  /**
   * 一般partitions参数是不传的，只有在删除topic的时候，才会传入partition
   * */
  /** Send UpdateMetadataRequest to the given brokers for the given partitions and partitions that are being deleted */
  def addUpdateMetadataRequestForBrokers(brokerIds: Seq[Int],
                                         partitions: collection.Set[TopicAndPartition] = Set.empty[TopicAndPartition]) {

    def updateMetadataRequestPartitionInfo(partition: TopicAndPartition, beingDeleted: Boolean) {
      /**
       * controllerContext在trunk版本是变成了AbstractControllerBrokerRequestBatch(controllerContext)定义，
       * 这个版本是按照变量定义的。
       * */
      val leaderIsrAndControllerEpochOpt = controllerContext.partitionLeadershipInfo.get(partition)
      leaderIsrAndControllerEpochOpt match {
          /**
           * trunk版本是case Some(LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)) 这样写的，
           * 没有用@符号，scala里面@表示什么意思？
           * l @ LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch) ：
           * 这样写表示leaderIsrAndControllerEpochOpt匹配出的l必须是LeaderIsrAndControllerEpoch类型的值，
           * 并且还必须传入leaderAndIsr和 controllerEpoch两个参数。否则就会报错。
           *
           * 换句话说： case Some(l @ LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))要匹配成功，
           * 那么进行匹配的变量结构必须是这样的结构：外围由some包住，里面的值必须是LeaderIsrAndControllerEpoch类型，
           * 而且LeaderIsrAndControllerEpoch类型的传入参数还需要是两个。
           *
           * 等同于：case l @ Some(LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))
           *
           * 但如果这样写，那么就不管Some里面的类型是啥了，都能匹配上：case l @ Some(_) 或 case Some(l)
           *
           * 总的一句话说，这里的@作用主要是进行强制类型检查，如果不是匹配的类型，编译就会报错.
           * 即便前面的一个case能匹配上，编译也会报错，eg:
           * val op = Option("bb")
           * op match {
           * case  Some( l)=> println(l)
           * case  l @ Some( LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))=> println(l)
           * case None=> println("xx")
           * }
           *
           * constructor cannot be instantiated to expected type;
           * found   : LeaderIsrAndControllerEpoch
           * required: String
           * case  l @ Some( LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))=> println(l)
           *
           * 这里加@进行强制类型检查，能够防止代码乌龙，还是有益处的，那么为什么trunk会干掉这个@符号呢？
           *
           * 因为这种写法就相当于类型检查了：
           * case  Some(LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch))
           * */
        case Some(l @ LeaderIsrAndControllerEpoch(leaderAndIsr, controllerEpoch)) =>
          val replicas = controllerContext.partitionReplicaAssignment(partition)
          /**
           * 如果是要删除的partition，那么构建的leaderIsr对象中的leader brokerId就为-2。
           * */
          val leaderIsrAndControllerEpoch = if (beingDeleted) {
            val leaderDuringDelete = LeaderAndIsr.duringDelete(leaderAndIsr.isr)
            LeaderIsrAndControllerEpoch(leaderDuringDelete, controllerEpoch)
          } else {
            l
          }

          /**
           * trunk版本中，PartitionStateInfo类似这种承载类，
           * 都是用Jackson的注解映射自动生成的类
           * */
          val partitionStateInfo = PartitionStateInfo(leaderIsrAndControllerEpoch, replicas)
          updateMetadataRequestPartitionInfoMap.put(new TopicPartition(partition.topic, partition.partition), partitionStateInfo)

        case None =>
          info("Leader not yet assigned for partition %s. Skip sending UpdateMetadataRequest.".format(partition))
      }
    }

    /**
     * 这里主要是需要把要删除的topic partition，从partitionLeadershipInfo集合中移除，
     * 然后把过滤后的partition leader信息发送给给定的broker节点进行更新.
     * trunk版本中没有这个过滤操作,为什么 trunk可以不需要这个操作?因为它借助一次metadata更新，就做了两件事：
     * ①对于不用删除的topic，更新metadata；②对于要删除的topic，通过下发metadata ，进行删除。
     *
     * 而在当前版本是分开做的，先把不用删除的topic metadata下发到所有broker，然后再专门把要删除的topic进行下发metadata。
     *
     * trunk版本的方案是通过beingDeleted赋值来实现的。
     * */
    val filteredPartitions = {
      val givenPartitions = if (partitions.isEmpty)
        controllerContext.partitionLeadershipInfo.keySet
      else
        partitions
      if (controller.topicDeletionManager.partitionsToBeDeleted.isEmpty)
        givenPartitions
      else
        givenPartitions -- controller.topicDeletionManager.partitionsToBeDeleted
    }

    /**
     * 过滤出，要更新metadata的broker列表。
     * brokerId小于0的都是非法broker，应该都不存在.
     * */
    updateMetadataRequestBrokerSet ++= brokerIds.filter(_ >= 0)

    filteredPartitions.foreach(partition => updateMetadataRequestPartitionInfo(partition, beingDeleted = false))
    controller.topicDeletionManager.partitionsToBeDeleted.foreach(partition => updateMetadataRequestPartitionInfo(partition, beingDeleted = true))
  }

  /**
   * 发送请求给broker，都是按照这三个流程来发送的：
   * 先发leaderAndIsr(内部先协商好，调班)，
   * 然后发metadata(公布给外面，别人就会找新的工作人员来处理问题),
   * 停止废弃replica(下掉退休人员)
   *
   * kafka controller和broker之间的通信，都是按照这三步来进行的。
   *
   * 有个问题，为什么controller给broker发送请求需要先放到leaderAndIsrRequestMap，然后再统一发送呢？
   * 初步想的是，可能是因为为了统一三个请求的发送顺序，防止漏发或乱序发送了这三个请求。
   *
   * 这是统一操作到sendRequestsToBrokers接口的原因吧，哪又为什么需要这三个结构呢？
   * leaderAndIsrRequestMap、updateMetadataRequestBrokerSet、stopReplicaRequestMap
   *
   * 这里想到了之前的newBatch()接口，在每次调用sendRequestsToBrokers前，
   * 都会调用newBatch接口来检查下，这三个结构是否清空：leaderAndIsrRequestMap、updateMetadataRequestBrokerSet、stopReplicaRequestMap，
   * 也就是每次调用sendRequestsToBrokers这个前，之前需要发送的都需要发送完成后，才能继续调用。
   *
   * 那么这三个结构的目的，应该就是为了方便判断之前的这三类请求是否发送完成
   * （这里的发送完成是指放到对应broker的发送队列中，就算完成）
   *
   *
   * 这里的队列差点和event队列搞混了，controller模块有两大块：
   * ①事件处理模块：ControllerEventManager
   * ②和broker之前的通信模块(channel管理模块)：ControllerChannelManager
   * */
  def sendRequestsToBrokers(controllerEpoch: Int) {
    try {
      leaderAndIsrRequestMap.foreach { case (broker, partitionStateInfos) =>
        /**
         * 这里只是为了打日志:那些变为leader了，那些变成follower了
         * */
        partitionStateInfos.foreach { case (topicPartition, state) =>
          val typeOfRequest =
            if (broker == state.leaderIsrAndControllerEpoch.leaderAndIsr.leader) "become-leader"
            else "become-follower"
          stateChangeLogger.trace(("Controller %d epoch %d sending %s LeaderAndIsr request %s to broker %d " +
                                   "for partition [%s,%d]").format(controllerId, controllerEpoch, typeOfRequest,
                                                                   state.leaderIsrAndControllerEpoch, broker,
                                                                   topicPartition.topic, topicPartition.partition))
        }

        /**
         * 这里过滤出，本次成为leader的brokerId。
         * */
        val leaderIds = partitionStateInfos.map(_._2.leaderIsrAndControllerEpoch.leaderAndIsr.leader).toSet
        /**
         * 根据leaderId和内部listenerName构建出Node对象集合
         * */
        val leaders = controllerContext.liveOrShuttingDownBrokers.filter(b => leaderIds.contains(b.id)).map {
          _.getNode(controller.config.interBrokerListenerName)
        }

        /**
         * 构建每个partition和partitionState的映射关系map.
         * PartitionStateInfo类里保存的信息和PartitionState是一样的，因此两者可以相互转换
         * */
        val partitionStates = partitionStateInfos.map { case (topicPartition, partitionStateInfo) =>
          val LeaderIsrAndControllerEpoch(leaderIsr, controllerEpoch) = partitionStateInfo.leaderIsrAndControllerEpoch
          val partitionState = new requests.PartitionState(controllerEpoch, leaderIsr.leader,
            leaderIsr.leaderEpoch, leaderIsr.isr.map(Integer.valueOf).asJava, leaderIsr.zkVersion,
            partitionStateInfo.allReplicas.map(Integer.valueOf).asJava)
          topicPartition -> partitionState
        }

        /**
         *  根据partition的状态信息、controller的brokerId、controllerEpoch号、本次涉及的leader node集合
         *  来构建LeaderAndIsrRequest的Builder对象
         * */
        val leaderAndIsrRequest = new LeaderAndIsrRequest.Builder(controllerId, controllerEpoch, partitionStates.asJava,
          leaders.asJava)

        controller.sendRequest(broker, ApiKeys.LEADER_AND_ISR, leaderAndIsrRequest)
      }

      /**
       * leaderAndIsr发送完成后，clear掉.
       * */
      leaderAndIsrRequestMap.clear()


      /***
       * 发送metadata
       * */
      updateMetadataRequestPartitionInfoMap.foreach(p => stateChangeLogger.trace(("Controller %d epoch %d sending UpdateMetadata request %s " +
        "to brokers %s for partition %s").format(controllerId, controllerEpoch, p._2.leaderIsrAndControllerEpoch,
        updateMetadataRequestBrokerSet.toString(), p._1)))
      val partitionStates = updateMetadataRequestPartitionInfoMap.map { case (topicPartition, partitionStateInfo) =>
        val LeaderIsrAndControllerEpoch(leaderIsr, controllerEpoch) = partitionStateInfo.leaderIsrAndControllerEpoch
        val partitionState = new requests.PartitionState(controllerEpoch, leaderIsr.leader,
          leaderIsr.leaderEpoch, leaderIsr.isr.map(Integer.valueOf).asJava, leaderIsr.zkVersion,
          partitionStateInfo.allReplicas.map(Integer.valueOf).asJava)
        topicPartition -> partitionState
      }

      val version: Short =
        if (controller.config.interBrokerProtocolVersion >= KAFKA_0_10_2_IV0) 3
        else if (controller.config.interBrokerProtocolVersion >= KAFKA_0_10_0_IV1) 2
        else if (controller.config.interBrokerProtocolVersion >= KAFKA_0_9_0) 1
        else 0

      val updateMetadataRequest = {
        val liveBrokers = if (version == 0) {
          // Version 0 of UpdateMetadataRequest only supports PLAINTEXT.
          controllerContext.liveOrShuttingDownBrokers.map { broker =>
            val securityProtocol = SecurityProtocol.PLAINTEXT
            val listenerName = ListenerName.forSecurityProtocol(securityProtocol)
            val node = broker.getNode(listenerName)
            val endPoints = Seq(new EndPoint(node.host, node.port, securityProtocol, listenerName))
            new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull)
          }
        } else {
          controllerContext.liveOrShuttingDownBrokers.map { broker =>
            val endPoints = broker.endPoints.map { endPoint =>
              new UpdateMetadataRequest.EndPoint(endPoint.host, endPoint.port, endPoint.securityProtocol, endPoint.listenerName)
            }
            new UpdateMetadataRequest.Broker(broker.id, endPoints.asJava, broker.rack.orNull)
          }
        }
        new UpdateMetadataRequest.Builder(version, controllerId, controllerEpoch, partitionStates.asJava,
          liveBrokers.asJava)
      }

      updateMetadataRequestBrokerSet.foreach { broker =>
        controller.sendRequest(broker, ApiKeys.UPDATE_METADATA_KEY, updateMetadataRequest, null)
      }
      updateMetadataRequestBrokerSet.clear()
      updateMetadataRequestPartitionInfoMap.clear()

      /**
       * 发送stopReplica
       * */
      stopReplicaRequestMap.foreach { case (broker, replicaInfoList) =>
        val stopReplicaWithDelete = replicaInfoList.filter(_.deletePartition).map(_.replica).toSet
        val stopReplicaWithoutDelete = replicaInfoList.filterNot(_.deletePartition).map(_.replica).toSet
        debug("The stop replica request (delete = true) sent to broker %d is %s"
          .format(broker, stopReplicaWithDelete.mkString(",")))
        debug("The stop replica request (delete = false) sent to broker %d is %s"
          .format(broker, stopReplicaWithoutDelete.mkString(",")))

        /**
         * replicasToNotGroup：是r.deletePartition为true或者r.callback != null的replica副本；
         * replicasToGroup：是r.deletePartition为false且r.callback == null的副本;
         * */
        val (replicasToGroup, replicasToNotGroup) = replicaInfoList.partition(r => !r.deletePartition && r.callback == null)

        // Send one StopReplicaRequest for all partitions that require neither delete nor callback. This potentially
        // changes the order in which the requests are sent for the same partitions, but that's OK.
        val stopReplicaRequest = new StopReplicaRequest.Builder(controllerId, controllerEpoch, false,
          replicasToGroup.map(r => new TopicPartition(r.replica.topic, r.replica.partition)).toSet.asJava)
        controller.sendRequest(broker, ApiKeys.STOP_REPLICA, stopReplicaRequest)


        replicasToNotGroup.foreach { r =>
          val stopReplicaRequest = new StopReplicaRequest.Builder(
              controllerId, controllerEpoch, r.deletePartition,
              Set(new TopicPartition(r.replica.topic, r.replica.partition)).asJava)
          controller.sendRequest(broker, ApiKeys.STOP_REPLICA, stopReplicaRequest, r.callback)
        }
      }
      stopReplicaRequestMap.clear()
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

case class ControllerBrokerStateInfo(networkClient: NetworkClient,
                                     brokerNode: Node,
                                     messageQueue: BlockingQueue[QueueItem],
                                     requestSendThread: RequestSendThread,
                                     queueSizeGauge: Gauge[Int])

case class StopReplicaRequestInfo(replica: PartitionAndReplica, deletePartition: Boolean, callback: AbstractResponse => Unit = null)

class Callbacks private (var leaderAndIsrResponseCallback: AbstractResponse => Unit = null,
                         var updateMetadataResponseCallback: AbstractResponse => Unit = null,
                         var stopReplicaResponseCallback: (AbstractResponse, Int) => Unit = null)

object Callbacks {
  class CallbackBuilder {
    var leaderAndIsrResponseCbk: AbstractResponse => Unit = null
    var updateMetadataResponseCbk: AbstractResponse => Unit = null
    var stopReplicaResponseCbk: (AbstractResponse, Int) => Unit = null

    def leaderAndIsrCallback(cbk: AbstractResponse => Unit): CallbackBuilder = {
      leaderAndIsrResponseCbk = cbk
      this
    }

    def updateMetadataCallback(cbk: AbstractResponse => Unit): CallbackBuilder = {
      updateMetadataResponseCbk = cbk
      this
    }

    def stopReplicaCallback(cbk: (AbstractResponse, Int) => Unit): CallbackBuilder = {
      stopReplicaResponseCbk = cbk
      this
    }

    def build: Callbacks = {
      new Callbacks(leaderAndIsrResponseCbk, updateMetadataResponseCbk, stopReplicaResponseCbk)
    }
  }
}
