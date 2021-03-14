/**
 *
 *
 *
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

package kafka.metrics

import kafka.utils.{CoreUtils, VerifiableProperties}
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.Seq
import scala.collection.mutable.ArrayBuffer


/**
 * KafkaMetricsReporterMBean 这个类是干啥的？
 * 1.首先从注释看：报告MBean的基本特质类。如果客户端想要公开这些JMX操作，
 *   可以通过一个自定义的reporter(这个实现了kafka.metrics.KafkaMetricsReporter接口)；这个自定义的reporter需要额外实现一个MBean特质类，这样注册的MBean符合标准的MBean约定。
 *
 * 喔，我明白了 ，类似KafkaCSVMetricsReporter这个实现类，当调用startReporter后，
 * 会在底层有个schedule线程池，定期把metric刷到本地csv文件中，然后我们再通过采集csv文件来进行打点。
 *
 *
 *
 * Base trait for reporter MBeans. If a client wants to expose these JMX
 * operations on a custom reporter (that implements
 * [[kafka.metrics.KafkaMetricsReporter]]), the custom reporter needs to
 * additionally implement an MBean trait that extends this trait so that the
 * registered MBean is compliant with the standard MBean convention.
 */
trait KafkaMetricsReporterMBean {
  def startReporter(pollingPeriodInSeconds: Long): Unit
  def stopReporter(): Unit
  /**
   *
   * @return The name with which the MBean will be registered.
   */
  def getMBeanName: String
}

/**
  * Implement {@link org.apache.kafka.common.ClusterResourceListener} to receive cluster metadata once it's available. Please see the class documentation for ClusterResourceListener for more information.
  */
trait KafkaMetricsReporter {
  def init(props: VerifiableProperties): Unit
}

object KafkaMetricsReporter {
  val ReporterStarted: AtomicBoolean = new AtomicBoolean(false)
  private var reporters: ArrayBuffer[KafkaMetricsReporter] = null

  def startReporters (verifiableProps: VerifiableProperties): Seq[KafkaMetricsReporter] = {
    ReporterStarted synchronized {
      if (!ReporterStarted.get()) {
        reporters = ArrayBuffer[KafkaMetricsReporter]()
        val metricsConfig = new KafkaMetricsConfig(verifiableProps)
        if(metricsConfig.reporters.nonEmpty) {
          metricsConfig.reporters.foreach(reporterType => {
            val reporter = CoreUtils.createObject[KafkaMetricsReporter](reporterType)
            reporter.init(verifiableProps)
            reporters += reporter
            reporter match {
              case bean: KafkaMetricsReporterMBean => CoreUtils.registerMBean(reporter, bean.getMBeanName)
              case _ =>
            }
          })
          ReporterStarted.set(true)
        }
      }
    }
    reporters
  }
}

