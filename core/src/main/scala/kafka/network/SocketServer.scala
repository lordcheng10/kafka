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

package kafka.network

import java.io.IOException
import java.net._
import java.nio.channels._
import java.nio.channels.{Selector => NSelector}
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.security.CredentialProvider
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.errors.InvalidRequestException
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{ChannelBuilders, KafkaChannel, ListenerName, Selectable, Send, Selector => KSelector}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection._
import JavaConverters._
import scala.util.control.{ControlThrowable, NonFatal}

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time, val credentialProvider: CredentialProvider) extends Logging with KafkaMetricsGroup {

  private val endpoints = config.listeners.map(l => l.listenerName -> l).toMap
  private val numProcessorThreads = config.numNetworkThreads
  private val maxQueuedRequests = config.queuedMaxRequests
  private val totalProcessorThreads = numProcessorThreads * endpoints.size

  private val maxConnectionsPerIp = config.maxConnectionsPerIp
  private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

  this.logIdent = "[Socket Server on Broker " + config.brokerId + "], "
  /**
   * trunk版本引入了数据流队列和控制流队列分离的方案，从process开始就分离了
   * */
  val requestChannel = new RequestChannel(totalProcessorThreads, maxQueuedRequests)
  private val processors = new Array[Processor](totalProcessorThreads)

  private[network] val acceptors = mutable.Map[EndPoint, Acceptor]()
  private var connectionQuotas: ConnectionQuotas = _

  /**
   * Start the socket server
   */
  def startup() {
    this.synchronized {
      /**
       * 链接配额,本版本是把总的最大链接数
       * 和单ip的最大链接数传入进去了。
       * trunk版本，是传入的配置config和metric
       * */
      connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)

      val sendBufferSize = config.socketSendBufferBytes
      val recvBufferSize = config.socketReceiveBufferBytes
      val brokerId = config.brokerId

      var processorBeginIndex = 0
      /**
       * 一个EndPoint(host: String, port: Int, listenerName: ListenerName, securityProtocol: SecurityProtocol)对应一个acceptor，
       * 一个acceptor对应多个processor；
       *
       *
       * 那么有个问题，如果我配置多个不同的listenerName，port相同，那么会不会有问题？
       * 同一个端口有两套acceptor和processor，来了一个包，怎么接收呢？
       *
       * 添加配置的时候进行了校验，这种配置会报错 启动不起来。
       * 同一个listenerName对应多个端口，一个端口对应多个listenerName都是非法配置，会校验，启动报错
       * */
      config.listeners.foreach { endpoint =>
        val listenerName = endpoint.listenerName
        val securityProtocol = endpoint.securityProtocol
        /**
         * 在开始位置+上processor线程数，得到end位置。
         * */
        val processorEndIndex = processorBeginIndex + numProcessorThreads

        /**
         *  processorBeginIndex主要是为了让所有listener的processor线程编号依次递增.
         *
         *  所有listener对应的processor都放到了一个数组中：
         * private val processors = new Array[Processor](totalProcessorThreads)
         * */
        for (i <- processorBeginIndex until processorEndIndex)
          processors(i) = newProcessor(i, connectionQuotas, listenerName, securityProtocol)

        /**
         * 通过processors.slice将这个acceptor对应的processor传入给它
         * */
        val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId,
          processors.slice(processorBeginIndex, processorEndIndex), connectionQuotas)


        /**
         * 构建端口和acceptor的映射
         * */
        acceptors.put(endpoint, acceptor)

        /**
         * 这里创建一个线程，但是这个线程创建后run起来就不管了，并且daemon设置为false代表，shutdown的时候，需要阻塞shutdown流程.
         * 也就是说守护线程，不会阻塞主线程shutdown.
         * 虽然没有持有这个线程，但是我们持有这个线程执行的任务，通过shutdown这个任务就可以shutdown这个线程
         * */
        Utils.newThread(s"kafka-socket-acceptor-$listenerName-$securityProtocol-${endpoint.port}", acceptor, false).start()

        /**
         * 这里等待acceptor启动完成，才能启动下一个，为什么呢? 我理解他是想等这个listener端口绑定注册到select后才能继续下一个accptor的创建。
         * 似乎是为了防止同一个端口被同时绑定到多个acceptor。(如果端口配置错误)
         * */
        acceptor.awaitStartup()

        /**
         * 更新下，下个端口的processor的开始编号
         * */
        processorBeginIndex = processorEndIndex
      }
    }


    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {
        /**
         * ioWaitRatioMetricNames记录每个processor的select key的等待时间(如果没有ready的key会等待)
         * */
        private val ioWaitRatioMetricNames = processors.map { p =>
          metrics.metricName("io-wait-ratio", "socket-server-metrics", p.metricTags)
        }

        /**
         * 这里是计算平均每个processor线程的等待时间.
         * 包括所有listener对应的processor
         * */
        def value = ioWaitRatioMetricNames.map { metricName =>
          Option(metrics.metric(metricName)).fold(0.0)(_.value)
        }.sum / totalProcessorThreads
      }
    )

    info("Started " + acceptors.size + " acceptor threads")
  }

  // register the processor threads for notification of responses
  requestChannel.addResponseListener(id => processors(id).wakeup())

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down")
    this.synchronized {
      acceptors.values.foreach(_.shutdown)
      processors.foreach(_.shutdown)
    }
    info("Shutdown completed")
  }

  def boundPort(listenerName: ListenerName): Int = {
    try {
      acceptors(endpoints(listenerName)).serverChannel.socket.getLocalPort
    } catch {
      case e: Exception => throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  /* `protected` for test usage */
  protected[network] def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, listenerName: ListenerName,
                                      securityProtocol: SecurityProtocol): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      listenerName,
      securityProtocol,
      config,
      metrics,
      credentialProvider
    )
  }

  /* For test usage */
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  /* For test usage */
  private[network] def processor(index: Int): Processor = processors(index)

}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  private val startupLatch = new CountDownLatch(1)

  // `shutdown()` is invoked before `startupComplete` and `shutdownComplete` if an exception is thrown in the constructor
  // (e.g. if the address is already in use). We want `shutdown` to proceed in such cases, so we first assign an open
  // latch and then replace it in `startupComplete()`.
  @volatile private var shutdownLatch = new CountDownLatch(0)

  private val alive = new AtomicBoolean(true)

  def wakeup(): Unit

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    alive.set(false)
    wakeup()
    shutdownLatch.await()
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete(): Unit = {
    // Replace the open latch with a closed one
    shutdownLatch = new CountDownLatch(1)
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete(): Unit = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning: Boolean = alive.get

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   */
  def close(selector: KSelector, connectionId: String): Unit = {
    val channel = selector.channel(connectionId)
    if (channel != null) {
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(address)
      selector.close(connectionId)
    }
  }

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(channel: SocketChannel): Unit = {
    if (channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      swallowError(channel.socket().close())
      swallowError(channel.close())
    }
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 */
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              processors: Array[Processor],
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private val nioSelector = NSelector.open()
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)

  /**
   * 在new acceptor的时候，就会start processor。
   * 启动顺序socketServer.startup() {
   *   new acceptor(){
   *      processor.start()
   *   }
   *
   *   acceptor.start()
   *
   *   但是在trunk版本中，acceptor专门提供了一个startProcessors方法来启动processor，
   *   触发还是在socketServer.startup里面触发的，启动顺序还是先启动processor，再启动acceptor.
   * }
   * */
  this.synchronized {
    processors.foreach { processor =>
      Utils.newThread(s"kafka-network-thread-$brokerId-${endPoint.listenerName}-${endPoint.securityProtocol}-${processor.id}",
        processor, false).start()
    }
  }

  /**
   * Accept loop that checks for new connection attempts
   */
  def run() {
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    startupComplete()
    try {
      var currentProcessor = 0
      while (isRunning) {
        try {
          val ready = nioSelector.select(500)
          if (ready > 0) {
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                iter.remove()
                if (key.isAcceptable)
                  accept(key, processors(currentProcessor))
                else
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")

                // round robin to the next processor thread
                currentProcessor = (currentProcessor + 1) % processors.length
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket and selector.")
      swallowError(serverChannel.close())
      swallowError(nioSelector.close())
      shutdownComplete()
    }
  }

  /*
   * Create a server socket to listen for connections on.
   */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if(host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    if (recvBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
      serverChannel.socket().setReceiveBufferSize(recvBufferSize)

    try {
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))
    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString, port, e.getMessage), e)
    }
    serverChannel
  }

  /*
   * Accept a new connection
   */
  def accept(key: SelectionKey, processor: Processor) {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()
    /**
     * socketChannel = serverSocketChannel.accept()这里就已经和客户端建立链接了，
     * 下面再判断是否超过connection quota，如果超过就死等有可用的slot的时候，
     * 就不会说出现链接超时了，因为是先建立链接，再判断是否超过，超过就死等资源释放。
     * 当然超过死等，是trunk版本的逻辑，在0.11.0版本，还是通过抛异常来解决的，抛异常然后关闭链接.
     * */
    try {
      connectionQuotas.inc(socketChannel.socket().getInetAddress)
      /**
       * socketChannel.configureBlocking(false)是设置nio的非阻塞模式，非阻塞模式下，read() write() connect() 调用这些方法时，
       * 可能等待到结果就返回了，也就是说这些接口不是阻塞的。
       *
       *
       * socketChannel.socket().setTcpNoDelay(true)关闭tcp缓冲，
       * 目的是确保数据及时发送给客户端，主要是为了减少延迟，不然会影响业务或者内部通信延迟.
       *
       *
       * socketChannel.socket().setKeepAlive(true) 这个设置为true是为了解决客户端异常宕机等情况下，释放连接用的.
       * 当设置为true时，如果客户端长时间不发送数据，那么服务端会发送一些ack探测包来确认客户的是否挂掉了(可能是断网等)，
       * 如果探测是挂掉了，会释放掉这个连接。所以如果不设置为true的话，这些失效的连接就会一直保持。
       * 另外，客户端也可以设置这个参数为true，来探测服务端是否挂了.参考：https://blog.csdn.net/huang_xw/article/details/7338663
       * */
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)

      /**
       * sendBufferSize如果为-1的话，那么久表示使用默认的send buffer大小
       * */
      if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
        socketChannel.socket().setSendBufferSize(sendBufferSize)


      debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))

      /**
       * 将新的链接添加到processor线程中（其实就是放到该线程的newConnections集合中），
       * 并且此时唤醒processor，如果此时它正处于select(ms)阻塞状态的话.
       * */
      processor.accept(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        /**
         * 如果      connectionQuotas.inc(socketChannel.socket().getInetAddress)判断出超过最大连接数了，
         * 那么会抛出一个TooManyConnectionsException异常，然后在这里关闭链接处理。
         * 但这种处理方式，关闭后，客户端可能源源不断的继续发链接请求过来，不是特别好。最好的是像trunk版本那样，
         * acceptor线程阻塞等待，等待有可用的链接slot释放出来，而不是抛异常，这样就更优雅点。
         *
         * 这里在关闭链接的时候，会释放一个slot,因为在之前判断时候，已经加进去了(因为真实的链接就已经加上了).
         * 注意这里的操作是单线程的，所以不存在多个线程同时加减的问题.
         * */
        close(socketChannel)
    }
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = nioSelector.wakeup()

}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 */
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               listenerName: ListenerName,
                               securityProtocol: SecurityProtocol,
                               config: KafkaConfig,
                               metrics: Metrics,
                               credentialProvider: CredentialProvider) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort)
        }
      }
      case _ => None
    }
  }

  private case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort"
  }

  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
  private[kafka] val metricTags = mutable.LinkedHashMap(
    "listener" -> listenerName.value,
    "networkProcessor" -> id.toString
  ).asJava

  newGauge("IdlePercent",
    new Gauge[Double] {
      def value = {
        Option(metrics.metric(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags))).fold(0.0)(_.value)
      }
    },
    // for compatibility, only add a networkProcessor tag to the Yammer Metrics alias (the equivalent Selector metric
    // also includes the listener name)
    Map("networkProcessor" -> id.toString)
  )

  private val selector = new KSelector(
    maxRequestSize,
    connectionsMaxIdleMs,
    metrics,
    time,
    "socket-server",
    metricTags,
    false,
    true,
    ChannelBuilders.serverChannelBuilder(listenerName, securityProtocol, config, credentialProvider.credentialCache))

  override def run() {
    startupComplete()
    while (isRunning) {
      try {
        // setup any new connections that have been queued up
        configureNewConnections()
        // register any new responses for writing
        processNewResponses()
        poll()
        processCompletedReceives()
        processCompletedSends()
        processDisconnected()
      } catch {
        // We catch all the throwables here to prevent the processor thread from exiting. We do this because
        // letting a processor exit might cause a bigger impact on the broker. Usually the exceptions thrown would
        // be either associated with a specific socket channel or a bad request. We just ignore the bad socket channel
        // or request. This behavior might need to be reviewed if we see an exception that need the entire broker to stop.
        case e: ControlThrowable => throw e
        case e: Throwable =>
          error("Processor got uncaught exception.", e)
      }
    }

    debug("Closing selector - processor " + id)
    swallowError(closeAll())
    shutdownComplete()
  }

  private def processNewResponses() {
    var curr = requestChannel.receiveResponse(id)
    while (curr != null) {
      try {
        curr.responseAction match {
          case RequestChannel.NoOpAction =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            updateRequestMetrics(curr.request)
            trace("Socket server received empty response to send, registering for read: " + curr)
            val channelId = curr.request.connectionId
            if (selector.channel(channelId) != null || selector.closingChannel(channelId) != null)
                selector.unmute(channelId)
          case RequestChannel.SendAction =>
            val responseSend = curr.responseSend.getOrElse(
              throw new IllegalStateException(s"responseSend must be defined for SendAction, response: $curr"))
            sendResponse(curr, responseSend)
          case RequestChannel.CloseConnectionAction =>
            updateRequestMetrics(curr.request)
            trace("Closing socket connection actively according to the response code.")
            close(selector, curr.request.connectionId)
        }
      } finally {
        curr = requestChannel.receiveResponse(id)
      }
    }
  }

  /* `protected` for test usage */
  protected[network] def sendResponse(response: RequestChannel.Response, responseSend: Send) {
    val connectionId = response.request.connectionId
    trace(s"Socket server received response to send to $connectionId, registering for write and sending data: $response")
    val channel = selector.channel(connectionId)
    // `channel` can be null if the selector closed the connection because it was idle for too long
    if (channel == null) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $connectionId")
      response.request.updateRequestMetrics(0L)
    }
    else {
      selector.send(responseSend)
      inflightResponses += (connectionId -> response)
    }
  }

  private def poll() {
    try selector.poll(300)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        error(s"Closing processor $id due to illegal state or IO exception")
        swallow(closeAll())
        shutdownComplete()
        throw e
    }
  }

  private def processCompletedReceives() {
    selector.completedReceives.asScala.foreach { receive =>
      try {
        val openChannel = selector.channel(receive.source)
        // Only methods that are safe to call on a disconnected channel should be invoked on 'openOrClosingChannel'.
        val openOrClosingChannel = if (openChannel != null) openChannel else selector.closingChannel(receive.source)
        val session = RequestChannel.Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, openOrClosingChannel.principal.getName), openOrClosingChannel.socketAddress)

        val req = RequestChannel.Request(processor = id, connectionId = receive.source, session = session,
          buffer = receive.payload, startTimeNanos = time.nanoseconds,
          listenerName = listenerName, securityProtocol = securityProtocol)
        requestChannel.sendRequest(req)
        selector.mute(receive.source)
      } catch {
        case e @ (_: InvalidRequestException | _: SchemaException) =>
          // note that even though we got an exception, we can assume that receive.source is valid. Issues with constructing a valid receive object were handled earlier
          error(s"Closing socket for ${receive.source} because of error", e)
          close(selector, receive.source)
      }
    }
  }

  private def processCompletedSends() {
    selector.completedSends.asScala.foreach { send =>
      val resp = inflightResponses.remove(send.destination).getOrElse {
        throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
      }
      updateRequestMetrics(resp.request)
      selector.unmute(send.destination)
    }
  }

  private def updateRequestMetrics(request: RequestChannel.Request) {
    val channel = selector.channel(request.connectionId)
    val openOrClosingChannel = if (channel != null) channel else selector.closingChannel(request.connectionId)
    val networkThreadTimeNanos = if (openOrClosingChannel != null) openOrClosingChannel.getAndResetNetworkThreadTimeNanos() else 0L
    request.updateRequestMetrics(networkThreadTimeNanos)
  }

  private def processDisconnected() {
    selector.disconnected.keySet.asScala.foreach { connectionId =>
      val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
        throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
      }.remoteHost
      inflightResponses.remove(connectionId).foreach(response => updateRequestMetrics(response.request))
      // the channel has been closed by the selector but the quotas still need to be updated
      connectionQuotas.dec(InetAddress.getByName(remoteHost))
    }
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel) {
    newConnections.add(socketChannel)
    /**
     * 这里是为了唤醒processor线程，因为processor线程有可能此时在
     * return this.nioSelector.select(ms)的时候暂时阻塞住了(processor目前只有这里会阻塞)
     * 这里的wakeup实际里面调用的是this.nioSelector.wakeup()。
     * 虽然this.nioSelector.select(ms)并不会阻塞太久,代码写死的是300ms，
     * 但是为了极大的提高吞吐，这里还是及时去唤醒下，因为此时processor有事情处理了，它此时需要处理链接请求.
     * */
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
   */
  private def configureNewConnections() {
    while (!newConnections.isEmpty) {
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        val localHost = channel.socket().getLocalAddress.getHostAddress
        val localPort = channel.socket().getLocalPort
        val remoteHost = channel.socket().getInetAddress.getHostAddress
        val remotePort = channel.socket().getPort
        val connectionId = ConnectionId(localHost, localPort, remoteHost, remotePort).toString
        selector.register(connectionId, channel)
      } catch {
        // We explicitly catch all non fatal exceptions and close the socket to avoid a socket leak. The other
        // throwables will be caught in processor and logged as uncaught exceptions.
        case NonFatal(e) =>
          val remoteAddress = channel.getRemoteAddress
          // need to close the channel here to avoid a socket leak.
          close(channel)
          error(s"Processor $id closed connection from $remoteAddress", e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll() {
    selector.channels.asScala.foreach { channel =>
      close(selector, channel.id)
    }
    selector.close()
  }

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  /* For test usage */
  private[network] def openOrClosingChannel(connectionId: String): Option[KafkaChannel] =
    channel(connectionId).orElse(Option(selector.closingChannel(connectionId)))

  // Visible for testing
  private[network] def numStagedReceives(connectionId: String): Int =
    openOrClosingChannel(connectionId).map(c => selector.numStagedReceives(c)).getOrElse(0)

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = selector.wakeup()

}

class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {
  /**
   * 转化成<ip,maxCount>的映射，这个是记录了每个ip最多可以有的链接数
   * */
  private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }

  /**
   * 每个ip目前实际的连接数
   * */
  private val counts = mutable.Map[InetAddress, Int]()


  /**
   * 对给定访问的ip进行叠加计数.
   * 在trunk版本中当连接数超过阈值时，不是单纯的抛异常，
   * 而是会wait等待，防止客户端频繁发送链接请求.
   * */
  def inc(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      val max = overrides.getOrElse(address, defaultMax)
      /**
       * 先加，后判断，因为在此之前，这个链接已经加进来了。
       * 超过后，抛异常出去，在外边，关闭的时候，又会再减一次，太多余了.
       * */
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  /**
   * 对给定访问的ip进行计数
   * */
  def dec(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)
    }
  }


  /**
   * 获取当前计数
   * */
  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
