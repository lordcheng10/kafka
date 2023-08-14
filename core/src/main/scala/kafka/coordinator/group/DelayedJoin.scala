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

package kafka.coordinator.group

import kafka.server.{DelayedOperation, DelayedOperationPurgatory, GroupKey}

import scala.math.{max, min}

/**
 * Delayed rebalance operations that are added to the purgatory when group is preparing for rebalance
 *
 * Whenever a join-group request is received, check if all known group members have requested
 * to re-join the group; if yes, complete this operation to proceed rebalance.
 *
 * When the operation has expired, any known members that have not requested to re-join
 * the group are marked as failed, and complete this operation to proceed rebalance with
 * the rest of the group.
 */
private[group] class DelayedJoin(coordinator: GroupCoordinator,
                                 group: GroupMetadata,
                                 rebalanceTimeout: Long) extends DelayedOperation(rebalanceTimeout, Some(group.lock)) {
  // 到时间后，会调用forceComplete方法，该方法会调用onComplete，如果forceComplete返回false，那么才会调用onExpiration
  override def tryComplete(): Boolean = coordinator.tryCompleteJoin(group, forceComplete _)
  override def onExpiration() = coordinator.onExpireJoin()

  // 到时间后，会调用forceComplete，forceComplete返回false才会调用onExpiration，而在forceComplete中，会调用onComplete
  override def onComplete() = coordinator.onCompleteJoin(group)
}

/**
  * Delayed rebalance operation that is added to the purgatory when a group is transitioning from
  * Empty to PreparingRebalance
  *
  * When onComplete is triggered we check if any new members have been added and if there is still time remaining
  * before the rebalance timeout. If both are true we then schedule a further delay. Otherwise we complete the
  * rebalance.
  */
private[group] class InitialDelayedJoin(coordinator: GroupCoordinator,
                                        purgatory: DelayedOperationPurgatory[DelayedJoin],
                                        group: GroupMetadata,
                                        configuredRebalanceDelay: Int,
                                        delayMs: Int,
                                        remainingMs: Int) extends DelayedJoin(coordinator, group, delayMs) {

  override def tryComplete(): Boolean = false

  override def onComplete(): Unit = {
    group.inLock {
      // 这里最终要做的效果是，当达到rebalancetimeout后，会触发完成join；或者在一轮configuredRebalanceDelay中，没有任何新memebr加入，那么也会完成；
      // 也就是第一轮join耗时可能会比rebalance timeout短，后面几轮就必须等rebalance timeout到达才行，为啥呢?

      // newMemberAdded为true的话，代表是该group的第一轮加入，如果是第一轮，并且remainingMs不为0
      // 其中remainingMs初始值是rebalance timeout减去一次delay间隔时间
      // 也就是只有第一轮加入的时候，才会一直等rebalance timeout结束后，才会正在完成join
      // 如果是InitialDelayedJoin，那么是不是就是第一轮join，也就是说只要是InitialDelayedJoin，那么必然满足group.newMemberAdded && remainingMs != 0
      if (group.newMemberAdded && remainingMs != 0) {
        group.newMemberAdded = false
        val delay = min(configuredRebalanceDelay, remainingMs)
        val remaining = max(remainingMs - delayMs, 0)
        purgatory.tryCompleteElseWatch(new InitialDelayedJoin(coordinator,
          purgatory,
          group,
          configuredRebalanceDelay,
          delay,
          remaining
        ), Seq(GroupKey(group.groupId)))
      } else {
        // 到期了或在一定时间内，没有新的member加入，那么就标记该group完成
        // 默认是3秒，也就是说，如果三秒没有新的member加入的话，那么就将该group标记完成
        // 即便有新member加入，一旦到达5分钟(对应的是rebalanceTimeout，默认是5分钟)，那么也会强制将该group标记完成(老版本默认是用的sessionTimeOut ,10秒)
        // 通常来说，5分钟内是肯定能加入的，除非consumer特别多，那么这个时候，需要调大rebalanceTimeOut，
        // 但大部分情况是consumer启动之间有间隔，所以通常当第一次触发rebalance失败后，如果客户端重试的话，在第二次rebalance时，是能成功的
        super.onComplete()
      }
    }
  }

}
