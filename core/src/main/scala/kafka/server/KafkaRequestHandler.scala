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

package kafka.server

import kafka.network._
import kafka.utils._
import kafka.metrics.KafkaMetricsGroup
import java.util.concurrent.{CountDownLatch, TimeUnit}

import com.yammer.metrics.core.Meter
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.utils.{Time, Utils}

/**
 * A thread that answers kafka requests.
 */
class KafkaRequestHandler(id: Int,
                          brokerId: Int,
                          val aggregateIdleMeter: Meter,
                          val totalHandlerThreads: Int,
                          val requestChannel: RequestChannel,
                          apis: KafkaApis,
                          time: Time) extends Runnable with Logging {
  this.logIdent = "[Kafka Request Handler " + id + " on Broker " + brokerId + "], "
  private val latch = new CountDownLatch(1)

  def run() {
    while (true) {
      try {
        var req : RequestChannel.Request = null
        while (req == null) {
          // We use a single meter for aggregate idle percentage for the thread pool.
          // Since meter is calculated as total_recorded_value / time_window and
          // time_window is independent of the number of threads, each recorded idle
          // time should be discounted by # threads.
          val startSelectTime = time.nanoseconds
          req = requestChannel.receiveRequest(300)
          val endTime = time.nanoseconds
          if (req != null)
            req.requestDequeueTimeNanos = endTime
          val idleTime = endTime - startSelectTime
          aggregateIdleMeter.mark(idleTime / totalHandlerThreads)
        }

        if (req eq RequestChannel.AllDone) {
          debug("Kafka request handler %d on broker %d received shut down command".format(id, brokerId))
          latch.countDown()
          return
        }
        trace("Kafka request handler %d on broker %d handling request %s".format(id, brokerId, req))
        apis.handle(req)
      } catch {
        case e: FatalExitError =>
          latch.countDown()
          Exit.exit(e.statusCode)
        case e: Throwable => error("Exception when handling request", e)
      }
    }
  }

  def initiateShutdown(): Unit = requestChannel.sendRequest(RequestChannel.AllDone)

  def awaitShutdown(): Unit = latch.await()

}

class KafkaRequestHandlerPool(val brokerId: Int,
                              val requestChannel: RequestChannel,
                              val apis: KafkaApis,
                              time: Time,
                              numThreads: Int) extends Logging with KafkaMetricsGroup {

  /* a meter to track the average free capacity of the request handlers */
  private val aggregateIdleMeter = newMeter("RequestHandlerAvgIdlePercent", "percent", TimeUnit.NANOSECONDS)

  this.logIdent = "[Kafka Request Handler on Broker " + brokerId + "], "
  val runnables = new Array[KafkaRequestHandler](numThreads)
  for(i <- 0 until numThreads) {
    runnables(i) = new KafkaRequestHandler(i, brokerId, aggregateIdleMeter, numThreads, requestChannel, apis, time)
    Utils.daemonThread("kafka-request-handler-" + i, runnables(i)).start()
  }

  def shutdown() {
    info("shutting down")
    for (handler <- runnables)
      handler.initiateShutdown()
    for (handler <- runnables)
      handler.awaitShutdown()
    info("shut down completely")
  }
}

class BrokerTopicMetrics(name: Option[String]) extends KafkaMetricsGroup {
  val tags: scala.collection.Map[String, String] = name match {
    case None => Map.empty
    case Some(topic) => Map("topic" -> topic)
  }
  /**
   * messagesInRate是解压后的消息数，用的end_offset-start_offset计算出来的真实消息条目数
   * */
  val messagesInRate = newMeter(BrokerTopicStats.MessagesInPerSec, "messages", TimeUnit.SECONDS, tags)
  val bytesInRate = newMeter(BrokerTopicStats.BytesInPerSec, "bytes", TimeUnit.SECONDS, tags)
  /**
   * bytesOutRate和bytesRejectedRate都是统计的解压前的数据大小.
   * 在trunk版本中，提供了接口来获取meter对象，然后再打点：
   * def bytesRejectedRate: Meter = metricTypeMap.get(BrokerTopicStats.BytesRejectedPerSec).meter()
   *
   * brokerTopicStats.topicStats(topicPartition.topic).bytesRejectedRate.mark(records.sizeInBytes)
   * */
  val bytesOutRate = newMeter(BrokerTopicStats.BytesOutPerSec, "bytes", TimeUnit.SECONDS, tags)
  val bytesRejectedRate = newMeter(BrokerTopicStats.BytesRejectedPerSec, "bytes", TimeUnit.SECONDS, tags)
  /**
   * follower append时候，统计的副本入口流量
   * */
  private[server] val replicationBytesInRate =
    if (name.isEmpty) Some(newMeter(BrokerTopicStats.ReplicationBytesInPerSec, "bytes", TimeUnit.SECONDS, tags))
    else None

  /**
   * leader统计的副本出口流量
   * */
  private[server] val replicationBytesOutRate =
    if (name.isEmpty) Some(newMeter(BrokerTopicStats.ReplicationBytesOutPerSec, "bytes", TimeUnit.SECONDS, tags))
    else None
  val failedProduceRequestRate = newMeter(BrokerTopicStats.FailedProduceRequestsPerSec, "requests", TimeUnit.SECONDS, tags)
  val failedFetchRequestRate = newMeter(BrokerTopicStats.FailedFetchRequestsPerSec, "requests", TimeUnit.SECONDS, tags)
  /**
   * producer qps ,但这个qps是按照partition维度的累计的，eg：一个producerRequest有10个partition，那么这个值会统计10次
   * 可能它想表达的是：单位时间内，这个topic或broker，有10次partiton写入
   *
   * 包括follower的producer request qps
   * */
  val totalProduceRequestRate = newMeter(BrokerTopicStats.TotalProduceRequestsPerSec, "requests", TimeUnit.SECONDS, tags)
  /**
   * fetch qps,和上面一样，都是partiton粒度的累加，包括follower的fetch 请求
   * TODO-PATCH:这里也可以提下，可以直接增加一个broker维度的qps统计，是真实的request请求数
   * */
  val totalFetchRequestRate = newMeter(BrokerTopicStats.TotalFetchRequestsPerSec, "requests", TimeUnit.SECONDS, tags)

  def close() {
    removeMetric(BrokerTopicStats.MessagesInPerSec, tags)
    removeMetric(BrokerTopicStats.BytesInPerSec, tags)
    removeMetric(BrokerTopicStats.BytesOutPerSec, tags)
    removeMetric(BrokerTopicStats.BytesRejectedPerSec, tags)
    if (replicationBytesInRate.isDefined)
      removeMetric(BrokerTopicStats.ReplicationBytesInPerSec, tags)
    if (replicationBytesOutRate.isDefined)
      removeMetric(BrokerTopicStats.ReplicationBytesOutPerSec, tags)
    removeMetric(BrokerTopicStats.FailedProduceRequestsPerSec, tags)
    removeMetric(BrokerTopicStats.FailedFetchRequestsPerSec, tags)
    removeMetric(BrokerTopicStats.TotalProduceRequestsPerSec, tags)
    removeMetric(BrokerTopicStats.TotalFetchRequestsPerSec, tags)
  }
}

object BrokerTopicStats {
  val MessagesInPerSec = "MessagesInPerSec"
  val BytesInPerSec = "BytesInPerSec"
  val BytesOutPerSec = "BytesOutPerSec"
  val BytesRejectedPerSec = "BytesRejectedPerSec"
  val ReplicationBytesInPerSec = "ReplicationBytesInPerSec"
  val ReplicationBytesOutPerSec = "ReplicationBytesOutPerSec"
  val FailedProduceRequestsPerSec = "FailedProduceRequestsPerSec"
  val FailedFetchRequestsPerSec = "FailedFetchRequestsPerSec"
  val TotalProduceRequestsPerSec = "TotalProduceRequestsPerSec"
  val TotalFetchRequestsPerSec = "TotalFetchRequestsPerSec"
  private val valueFactory = (k: String) => new BrokerTopicMetrics(Some(k))
}

class BrokerTopicStats {
  import BrokerTopicStats._

  private val stats = new Pool[String, BrokerTopicMetrics](Some(valueFactory))
  val allTopicsStats = new BrokerTopicMetrics(None)

  def topicStats(topic: String): BrokerTopicMetrics =
    stats.getAndMaybePut(topic)

  def updateReplicationBytesIn(value: Long) {
    allTopicsStats.replicationBytesInRate.foreach { metric =>
      metric.mark(value)
    }
  }

  private def updateReplicationBytesOut(value: Long) {
    allTopicsStats.replicationBytesOutRate.foreach { metric =>
      metric.mark(value)
    }
  }

  def removeMetrics(topic: String) {
    val metrics = stats.remove(topic)
    if (metrics != null)
      metrics.close()
  }

  def updateBytesOut(topic: String, isFollower: Boolean, value: Long) {
    if (isFollower) {
      updateReplicationBytesOut(value)
    } else {
      topicStats(topic).bytesOutRate.mark(value)
      allTopicsStats.bytesOutRate.mark(value)
    }
  }


  def close(): Unit = {
    allTopicsStats.close()
    stats.values.foreach(_.close())
  }

}
