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

import kafka.server.DelayedOperation

/**
 * Delayed heartbeat operations that are added to the purgatory for session timeout checking.
 * Heartbeats are paused during rebalance.
 */
private[group] class DelayedHeartbeat(coordinator: GroupCoordinator,
                                      group: GroupMetadata,
                                      memberId: String,
                                      isPending: Boolean,
                                      timeoutMs: Long)
  extends DelayedOperation(timeoutMs, Some(group.lock)) {
  // 这里的forceComplete只是取消定时任务，onComplete中没有逻辑，如果取消成功，那么会调用onExpiration，进行过期处理，如果没取消成功，那么下一次还会执行到
  // 这里的tryComplete只会在tryCompleteElseWatch中调用,每次watch前，都会先尝试完成
  // 我们可以通过调用checkAndComplete来检查是否可以在到期前完成
  override def tryComplete(): Boolean = coordinator.tryCompleteHeartbeat(group, memberId, isPending, forceComplete _)
  // 到期后，才会调用
  override def onExpiration() = coordinator.onExpireHeartbeat(group, memberId, isPending)

  // 到时间后，会调用forceComplete，在forceComplete中会调用onComplete
  // 或者在调用checkAndComplete来手动尝试完成时，会调用forceComplete，从而调用onComplete
  override def onComplete() = coordinator.onCompleteHeartbeat()
}
