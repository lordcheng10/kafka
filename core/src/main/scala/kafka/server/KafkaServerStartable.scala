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

import java.util.Properties

import kafka.metrics.KafkaMetricsReporter
import kafka.utils.{Exit, Logging, VerifiableProperties}

import scala.collection.Seq

/**
 * 这静态的KafkaServerStartable类提供了两个接口，实际都是一个接口，区别在与是否要给定主线程名。
 * 然后主要功能就是根据传入的配置项，构建KafkaServerStartable对象。
 *
 * */
object KafkaServerStartable {
  def fromProps(serverProps: Properties): KafkaServerStartable = {
    fromProps(serverProps, None)
  }

  /**
   * 写了这个方法为什么不用这个方法，这样可以设置主线程名
   * */
  def fromProps(serverProps: Properties, threadNamePrefix: Option[String]): KafkaServerStartable = {
    /**
     * KafkaMetricsReporter这个类到底是干啥的
     *
     * */
    val reporters = KafkaMetricsReporter.startReporters(new VerifiableProperties(serverProps))
    new KafkaServerStartable(KafkaConfig.fromProps(serverProps, false), reporters, threadNamePrefix)
  }
}

class KafkaServerStartable(val staticServerConfig: KafkaConfig, reporters: Seq[KafkaMetricsReporter], threadNamePrefix: Option[String] = None) extends Logging {
  private val server = new KafkaServer(staticServerConfig, kafkaMetricsReporters = reporters, threadNamePrefix = threadNamePrefix)

  def this(serverConfig: KafkaConfig) = this(serverConfig, Seq.empty)

  def startup(): Unit = {
    try server.startup()
    catch {
      case _: Throwable =>
        // KafkaServer.startup() calls shutdown() in case of exceptions, so we invoke `exit` to set the status code
        fatal("Exiting Kafka.")
        Exit.exit(1)
    }
  }

  def shutdown(): Unit = {
    try server.shutdown()
    catch {
      case _: Throwable =>
        fatal("Halting Kafka.")
        // Calling exit() can lead to deadlock as exit() can be called multiple times. Force exit.
        Exit.halt(1)
    }
  }

  /**
   * Allow setting broker state from the startable.
   * This is needed when a custom kafka server startable want to emit new states that it introduces.
   */
  def setServerState(newState: Byte): Unit = {
    server.brokerState.newState(newState)
  }

  def awaitShutdown(): Unit = server.awaitShutdown()

}


