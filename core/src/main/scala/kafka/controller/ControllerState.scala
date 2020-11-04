/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import scala.collection.Seq

sealed abstract class ControllerState {

  def value: Byte

  def rateAndTimeMetricName: Option[String] =
    if (hasRateAndTimeMetric) Some(s"${toString}RateAndTimeMs") else None

  protected def hasRateAndTimeMetric: Boolean = true
}

/**
 *  这里的状态都代表一个事件（除了Idle初始状态外），
 *  而controller是通过监听zk来发现这些事件是否发生的，
 *  这里表示的是这些事件所对应的状态标签，或者直接理解为这些事件的分类.
 *
 *  controller设计了一个事件管理器，通过监听zk，触发回调，构建出事件，并放入队列中，然后异步进行处理.
 * */
object ControllerState {

  // Note: `rateAndTimeMetricName` is based on the case object name by default. Changing a name is a breaking change
  // unless `rateAndTimeMetricName` is overridden.

  /**
   *  Idle是一个初始状态，没有具体含义
   * */
  case object Idle extends ControllerState {
    def value = 0
    override protected def hasRateAndTimeMetric: Boolean = false
  }

  /**
   *  表示Controller变化状态
   * */
  case object ControllerChange extends ControllerState {
    def value = 1
  }

  /**
   *  表示broker变化的状态(比如，broker宕机，或新启broker)
   * */
  case object BrokerChange extends ControllerState {
    def value = 2
    // The LeaderElectionRateAndTimeMs metric existed before `ControllerState` was introduced and we keep the name
    // for backwards compatibility. The alternative would be to have the same metric under two different names.
    override def rateAndTimeMetricName = Some("LeaderElectionRateAndTimeMs")
  }

  /**
   * 表示Topic发生改变的状态(比如，topic扩partition)
   * */
  case object TopicChange extends ControllerState {
    def value = 3
  }

  /**
   *  表示Topic删除事件状态
   * */
  case object TopicDeletion extends ControllerState {
    def value = 4
  }

  /**
   *   代表ressign事件(数据迁移)
   * */
  case object PartitionReassignment extends ControllerState {
    def value = 5
  }

  /**
   *  自动prefer事件
   * */
  case object AutoLeaderBalance extends ControllerState {
    def value = 6
  }

  /**
   *  主动prefer事件
   * */
  case object ManualLeaderBalance extends ControllerState {
    def value = 7
  }

  /**
   *  表示controller宕机事件
   * */
  case object ControlledShutdown extends ControllerState {
    def value = 8
  }

  /**
   *  表示isr改变事件
   * */
  case object IsrChange extends ControllerState {
    def value = 9
  }

  val values: Seq[ControllerState] = Seq(Idle, ControllerChange, BrokerChange, TopicChange, TopicDeletion,
    PartitionReassignment, AutoLeaderBalance, ManualLeaderBalance, ControlledShutdown, IsrChange)
}
