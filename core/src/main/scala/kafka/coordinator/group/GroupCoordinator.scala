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

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.OffsetAndMetadata
import kafka.log.LogConfig
import kafka.message.ProducerCompressionCodec
import kafka.server._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record.RecordBatch.{NO_PRODUCER_EPOCH, NO_PRODUCER_ID}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, Seq, immutable}
import scala.math.max

/**
 * GroupCoordinator handles general group membership and offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * groups. Groups are assigned to coordinators based on their group names.
 * <p>
 * <b>Delayed operation locking notes:</b>
 * Delayed operations in GroupCoordinator use `group` as the delayed operation
 * lock. ReplicaManager.appendRecords may be invoked while holding the group lock
 * used by its callback.  The delayed callback may acquire the group lock
 * since the delayed operation is completed only if the group lock can be acquired.
 */
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupConfig,
                       val offsetConfig: OffsetConfig,
                       val groupManager: GroupMetadataManager,
                       val heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
                       val joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
                       time: Time) extends Logging {
  import GroupCoordinator._

  type JoinCallback = JoinGroupResult => Unit
  type SyncCallback = (Array[Byte], Errors) => Unit

  this.logIdent = "[GroupCoordinator " + brokerId + "]: "

  private val isActive = new AtomicBoolean(false)

  def offsetsTopicConfigs: Properties = {
    val props = new Properties
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CompressionTypeProp, ProducerCompressionCodec.name)
    props
  }

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup(enableMetadataExpiration: Boolean = true) {
    info("Starting up.")
    groupManager.startup(enableMetadataExpiration)
    isActive.set(true)
    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown() {
    info("Shutting down.")
    isActive.set(false)
    groupManager.shutdown()
    heartbeatPurgatory.shutdown()
    joinPurgatory.shutdown()
    info("Shutdown complete.")
  }

  def handleJoinGroup(groupId: String,
                      memberId: String,
                      requireKnownMemberId: Boolean,
                      clientId: String,
                      clientHost: String,
                      rebalanceTimeoutMs: Int,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(String, Array[Byte])],
                      responseCallback: JoinCallback): Unit = {
    // 检查group的状态
    validateGroupStatus(groupId, ApiKeys.JOIN_GROUP).foreach { error =>
      // 如果有错误的话，那么就回复带有对应错误码的response
      responseCallback(joinError(memberId, error))
      return
    }

    // 检查客户端配置的session超时时间是否小于服务端配置的最小超时时间,默认最小超时时间是6s
    if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
      sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) {// 检查是否超过服务端最大的超时时间，服务端最大的超时时间是5分钟

      // 如果客户端配置的参数不合理，那么就回复错误超时的错误码给客户端
      responseCallback(joinError(memberId, Errors.INVALID_SESSION_TIMEOUT))
    } else {
      // 如果memberId为空字符串，那么就是未知的member
      val isUnknownMember = memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID
      // 从groupmanager中获取该groupId的metadata信息
      groupManager.getGroup(groupId) match {
        case None =>// 如果为none，没有该group
          // only try to create the group if the group is UNKNOWN AND
          // the member id is UNKNOWN, if member is specified but group does not
          // exist we should reject the request.
          // 仅当组未知并且成员ID未知时才尝试创建组，如果指定了成员但组不存在，我们应该拒绝该请求。
          if (isUnknownMember) {
            //创建一个group
            val group = groupManager.addGroup(new GroupMetadata(groupId, Empty, time))
            // 处理未知join
            doUnknownJoinGroup(group, requireKnownMemberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
          } else {
            // 回复error response
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
          }

        case Some(group) =>
          group.inLock {
            // 如果group存在，那么就看该group的成员数是否超过group的最大成员数限制，如果已经超过了单group的最大member数了，
            // 那么就得看该group中是否存在该member，如果存在，并且该member还没处于等待join的状态，那么就将该member移除：规模过大的团队，需要裁减尚未加入的成员
            if ((groupIsOverCapacity(group)
                  && group.has(memberId) && !group.get(memberId).isAwaitingJoin) // oversized group, need to shed members that haven't joined yet
              // 如果是未知的member，并且该group的成员数超过了最大限制，那么也要移除该memberId
              || (isUnknownMember && group.size >= groupConfig.groupMaxSize)) {
              group.remove(memberId)
              responseCallback(joinError(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.GROUP_MAX_SIZE_REACHED))
            } else if (isUnknownMember) {//如果是未知的member,那么就调用doUnknownJoinGroup方法
              doUnknownJoinGroup(group, requireKnownMemberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
            } else {
              // 如果没有超过单group的member限制，那么就就处理group加入
              doJoinGroup(group, memberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
            }

            // attempt to complete JoinGroup
            if (group.is(PreparingRebalance)) {//如果该group当前处于prepare，就检查下该group是否完成了join阶段
              joinPurgatory.checkAndComplete(GroupKey(group.groupId))
            }
          }
      }
    }
  }

  /**
   * 1.group: group的metadata
   * 2.requireKnownMemberId: 如果协议版本号大于4，那么就必须要求memberId必须提供
   * 3.clientId: 客户端Id，这个是客户端配置的，如果没配置就是用conumser-前缀后面加自增数字；
   * 4.clientHost客户端地址
   * 5.rebalanceTimeoutMs: rebalance超时时间
   * 6.sessionTimeoutMs: session超时时间
   * 7.protocolType:协议类型
   * 8.protocols:有哪些协议
   * 9.responseCallback: 回复response的callback
   * */
  private def doUnknownJoinGroup(group: GroupMetadata,
                                 requireKnownMemberId: Boolean,
                                 clientId: String,
                                 clientHost: String,
                                 rebalanceTimeoutMs: Int,
                                 sessionTimeoutMs: Int,
                                 protocolType: String,
                                 protocols: List[(String, Array[Byte])],
                                 responseCallback: JoinCallback): Unit = {
    group.inLock {// 对该group进行加锁
      if (group.is(Dead)) {// 如果该group处于dead状态,那么直接返回unknown member id
        // 如果该组被标记为死亡，则意味着其他某个线程刚刚从协调器元数据中删除了该组；
        // 该组可能已迁移到其他协调器，或者该组处于暂时不稳定阶段。 让成员在没有指定成员 ID 的情况下重试加入。
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; it is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // joining without the specified member id.
        responseCallback(joinError(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.UNKNOWN_MEMBER_ID))
      } else if (!group.supportsProtocols(protocolType, MemberMetadata.plainProtocolSet(protocols))) {// 检查是否支持该协议
        // 如果不支持的话，也会返回INCONSISTENT_GROUP_PROTOCOL异常,该异常不是重试类型的异常
        responseCallback(joinError(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else {
        // 新成员的id，在clientId后面加一个随机的uuid
        val newMemberId = clientId + "-" + group.generateMemberIdSuffix

        if (requireKnownMemberId) {// 如果是高版本，那么就必须要知道memberId，所以还是得报错
          // 如果需要成员 ID，则在待处理成员列表中注册该成员，并发送回响应以调用具有分配的成员 ID 的另一个加入组请求。
          // If member id required, register the member in the pending member list
          // and send back a response to call for another join group request with allocated member id.
          group.addPendingMember(newMemberId)//都回复error response了，为啥要加入到pending member里? 加入到pendding里，是为了让检查过的逻辑不用再检查?
          addPendingMemberExpiration(group, newMemberId, sessionTimeoutMs)// 如果超过sessionTimeoutMs，那么就会从pendding中移除该member
          //回复一个MEMBER_ID_REQUIRED异常的错误码,客户端再收到该错误后，会用服务端传过来的memberId，然后重新发送join请求
          responseCallback(joinError(newMemberId, Errors.MEMBER_ID_REQUIRED))
        } else {
          // 如果是低版本，那么就直接添加成员，并触发rebalance
          addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, newMemberId, clientId, clientHost, protocolType,
            protocols, group, responseCallback)
        }
      }
    }
  }

  /**
   * group: group的元数据信息
   * memberId: 成员id
   * clientId：客户端id
   * clientHost:客户端ip
   * rebalanceTimeoutMs: rebalance超时时间
   * sessionTimeoutMs: 会话超时时间
   * protocolType: 协议类型
   * protocols: 该成员携带的协议，key是对应的分区分配策略名，value是对应的负责的分区
   * responseCallback: 最后回复response的处理callback函数
   * */
  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          clientId: String,
                          clientHost: String,
                          rebalanceTimeoutMs: Int,
                          sessionTimeoutMs: Int,
                          protocolType: String,
                          protocols: List[(String, Array[Byte])],
                          responseCallback: JoinCallback) {
    group.inLock {
      if (group.is(Dead)) {
        //如果该组被标记为dead，则表示其他线程刚刚删除了该组
        //从协调器元数据；这很可能是该组已迁移到其他组
        //协调器OR组处于瞬态不稳定阶段。让成员重试
        //在没有指定成员id的情况下加入。
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // joining without the specified member id.
        responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
      } else if (!group.supportsProtocols(protocolType, MemberMetadata.plainProtocolSet(protocols))) {// 如果协议不匹配，那么就返回不一致的group协议错误response
        responseCallback(joinError(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else if (group.isPendingMember(memberId)) {// 如果是之前被挂起的member，说明是重新join，那么此时就加入就好了
        // A rejoining pending member will be accepted.
        addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, memberId, clientId, clientHost, protocolType,
          protocols, group, responseCallback)
      } else if (!group.has(memberId)) {// 如果该group没有这个member，那么就回复未知response的错误码
        // if the member trying to register with a un-recognized id, send the response to let
        // it reset its member id and retry.
        responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
      } else {
        // 如果该member之前已经join过(因为group里面有改member)
        group.currentState match {
          case PreparingRebalance =>// 如果是准备rebalance阶段
            val member = group.get(memberId)// 获取member
            // 更新member并rebalance
            updateMemberAndRebalance(group, member, protocols, responseCallback)

          case CompletingRebalance =>
            // 首先从group中获取该member
            val member = group.get(memberId)
            // 检查该member请求过来的协议和group中存放的协议是否匹配
            if (member.matches(protocols)) {
              // 成员使用相同的元数据加入（可能是因为它未能接收到初始JoinGroup响应），所以只返回当前生成的当前组信息。
              // 这种情况就直接回复response就好了
              // member is joining with the same metadata (which could be because it failed to
              // receive the initial JoinGroup response), so just return current group information
              // for the current generation.
              responseCallback(JoinGroupResult(
                // 如果是leader，那么会把当前的成员的信息以及每个成员订阅的topic和对应负责的分区回复给leader
                members = if (group.isLeader(memberId)) {
                  group.currentMemberMetadata
                } else {
                  Map.empty
                },
                memberId = memberId,
                generationId = group.generationId,
                subProtocol = group.protocolOrNull,
                // 并且告诉他，你是leader
                leaderId = group.leaderOrNull,
                error = Errors.NONE))
            } else {
              // 如果没有匹配上，那么就说明成员改变了metadata信息，这个时候，需要更新member的信息，然后rebalance
              // member has changed metadata, so force a rebalance
              updateMemberAndRebalance(group, member, protocols, responseCallback)
            }

          case Stable =>
            // 从group中获取到该member
            val member = group.get(memberId)
            // 如果此时是stable状态，并且该member是leader，或不匹配对应的协议
            if (group.isLeader(memberId) || !member.matches(protocols)) {
              // 如果成员更改了元数据，或者领导者发送了JoinGroup，则强制重新平衡。
              // 后者允许领导者为影响分配但不影响成员元数据的更改触发重新平衡（例如使用者的主题元数据更改）
              // force a rebalance if a member has changed metadata or if the leader sends JoinGroup.
              // The latter allows the leader to trigger rebalances for changes affecting assignment
              // which do not affect the member metadata (such as topic metadata changes for the consumer)
              updateMemberAndRebalance(group, member, protocols, responseCallback)
            } else {//如果该join请求的member不是leader并且对应的元数据是匹配的，那么就直接回复response
              // 对于元数据没有实际更改的关注者，只需返回当前代的组信息，即可发布SyncGroup
              // 用于当前一代，这将允许他们发布SyncGroup
              // 什么情况下，之前注册过的成员，元数据没变的情况，又会再发一次join请求呢？
              // for followers with no actual change to their metadata, just return group information
              // for the current generation which will allow them to issue SyncGroup
              responseCallback(JoinGroupResult(
                members = Map.empty,
                memberId = memberId,
                generationId = group.generationId,
                subProtocol = group.protocolOrNull,
                leaderId = group.leaderOrNull,
                error = Errors.NONE))
            }

          case Empty | Dead =>
            // 如果该group的状态是empty或dead,那么group此时的状态是不符合预期的，需要直接回复response
            // Group reaches unexpected state. Let the joining member reset their generation and rejoin.
            warn(s"Attempt to add rejoining member $memberId of group ${group.groupId} in " +
              s"unexpected group state ${group.currentState}")
            responseCallback(joinError(memberId, Errors.UNKNOWN_MEMBER_ID))
        }
      }
    }
  }

  def handleSyncGroup(groupId: String,
                      generation: Int,
                      memberId: String,
                      groupAssignment: Map[String, Array[Byte]],
                      responseCallback: SyncCallback): Unit = {
    validateGroupStatus(groupId, ApiKeys.SYNC_GROUP) match {
      case Some(error) if error == Errors.COORDINATOR_LOAD_IN_PROGRESS =>
        // The coordinator is loading, which means we've lost the state of the active rebalance and the
        // group will need to start over at JoinGroup. By returning rebalance in progress, the consumer
        // will attempt to rejoin without needing to rediscover the coordinator. Note that we cannot
        // return COORDINATOR_LOAD_IN_PROGRESS since older clients do not expect the error.
        responseCallback(Array.empty, Errors.REBALANCE_IN_PROGRESS)

      case Some(error) => responseCallback(Array.empty, error)

      case None =>
        groupManager.getGroup(groupId) match {
          case None => responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)
          case Some(group) => doSyncGroup(group, generation, memberId, groupAssignment, responseCallback)
        }
    }
  }

  private def doSyncGroup(group: GroupMetadata,
                          generationId: Int,
                          memberId: String,
                          groupAssignment: Map[String, Array[Byte]],
                          responseCallback: SyncCallback) {
    group.inLock {
      if (!group.has(memberId)) {
        responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)
      } else if (generationId != group.generationId) {
        responseCallback(Array.empty, Errors.ILLEGAL_GENERATION)
      } else {
        group.currentState match {
          case Empty | Dead =>
            responseCallback(Array.empty, Errors.UNKNOWN_MEMBER_ID)

          case PreparingRebalance =>
            responseCallback(Array.empty, Errors.REBALANCE_IN_PROGRESS)

          case CompletingRebalance =>
            group.get(memberId).awaitingSyncCallback = responseCallback

            // if this is the leader, then we can attempt to persist state and transition to stable
            if (group.isLeader(memberId)) {
              info(s"Assignment received from leader for group ${group.groupId} for generation ${group.generationId}")

              // fill any missing members with an empty assignment
              val missing = group.allMembers -- groupAssignment.keySet
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

              groupManager.storeGroup(group, assignment, (error: Errors) => {
                group.inLock {
                  // another member may have joined the group while we were awaiting this callback,
                  // so we must ensure we are still in the CompletingRebalance state and the same generation
                  // when it gets invoked. if we have transitioned to another state, then do nothing
                  if (group.is(CompletingRebalance) && generationId == group.generationId) {
                    if (error != Errors.NONE) {
                      resetAndPropagateAssignmentError(group, error)
                      maybePrepareRebalance(group, s"error when storing group assignment during SyncGroup (member: $memberId)")
                    } else {
                      setAndPropagateAssignment(group, assignment)
                      group.transitionTo(Stable)
                    }
                  }
                }
              })
            }

          case Stable =>
            // if the group is stable, we just return the current assignment
            val memberMetadata = group.get(memberId)
            responseCallback(memberMetadata.assignment, Errors.NONE)
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))
        }
      }
    }
  }

  def handleLeaveGroup(groupId: String, memberId: String, responseCallback: Errors => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.LEAVE_GROUP).foreach { error =>
      responseCallback(error)
      return
    }

    groupManager.getGroup(groupId) match {
      case None =>
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; it is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the consumer to retry
        // joining without specified consumer id,
        responseCallback(Errors.UNKNOWN_MEMBER_ID)

      case Some(group) =>
        group.inLock {
          if (group.is(Dead)) {
            responseCallback(Errors.UNKNOWN_MEMBER_ID)
          } else if (group.isPendingMember(memberId)) {
            // if a pending member is leaving, it needs to be removed from the pending list, heartbeat cancelled
            // and if necessary, prompt a JoinGroup completion.
            info(s"Pending member $memberId is leaving group ${group.groupId}.")
            removePendingMemberAndUpdateGroup(group, memberId)
            heartbeatPurgatory.checkAndComplete(MemberKey(group.groupId, memberId))
            responseCallback(Errors.NONE)
          } else if (!group.has(memberId)) {
            responseCallback(Errors.UNKNOWN_MEMBER_ID)
          } else {
            val member = group.get(memberId)
            removeHeartbeatForLeavingMember(group, member)
            info(s"Member ${member.memberId} in group ${group.groupId} has left, removing it from the group")
            removeMemberAndUpdateGroup(group, member, s"removing member $memberId on LeaveGroup")
            responseCallback(Errors.NONE)
          }
        }
    }
  }

  def handleDeleteGroups(groupIds: Set[String]): Map[String, Errors] = {
    var groupErrors: Map[String, Errors] = Map()
    var groupsEligibleForDeletion: Seq[GroupMetadata] = Seq()

    groupIds.foreach { groupId =>
      validateGroupStatus(groupId, ApiKeys.DELETE_GROUPS) match {
        case Some(error) =>
          groupErrors += groupId -> error

        case None =>
          groupManager.getGroup(groupId) match {
            case None =>
              groupErrors += groupId ->
                (if (groupManager.groupNotExists(groupId)) Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR)
            case Some(group) =>
              group.inLock {
                group.currentState match {
                  case Dead =>
                    groupErrors += groupId ->
                      (if (groupManager.groupNotExists(groupId)) Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR)
                  case Empty =>
                    group.transitionTo(Dead)
                    groupsEligibleForDeletion :+= group
                  case _ =>
                    groupErrors += groupId -> Errors.NON_EMPTY_GROUP
                }
              }
          }
      }
    }

    if (groupsEligibleForDeletion.nonEmpty) {
      val offsetsRemoved = groupManager.cleanupGroupMetadata(groupsEligibleForDeletion, _.removeAllOffsets())
      groupErrors ++= groupsEligibleForDeletion.map(_.groupId -> Errors.NONE).toMap
      info(s"The following groups were deleted: ${groupsEligibleForDeletion.map(_.groupId).mkString(", ")}. " +
        s"A total of $offsetsRemoved offsets were removed.")
    }

    groupErrors
  }

  def handleHeartbeat(groupId: String,
                      memberId: String,
                      generationId: Int,
                      responseCallback: Errors => Unit) {
    validateGroupStatus(groupId, ApiKeys.HEARTBEAT).foreach { error =>
      if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS)
        // the group is still loading, so respond just blindly
        responseCallback(Errors.NONE)
      else
        responseCallback(error)
      return
    }

    groupManager.getGroup(groupId) match {
      case None =>
        responseCallback(Errors.UNKNOWN_MEMBER_ID)

      case Some(group) => group.inLock {
        group.currentState match {
          case Dead =>
            // if the group is marked as dead, it means some other thread has just removed the group
            // from the coordinator metadata; it is likely that the group has migrated to some other
            // coordinator OR the group is in a transient unstable phase. Let the member retry
            // joining without the specified member id.
            responseCallback(Errors.UNKNOWN_MEMBER_ID)

          case Empty =>
            responseCallback(Errors.UNKNOWN_MEMBER_ID)

          case CompletingRebalance =>
            if (!group.has(memberId))
              responseCallback(Errors.UNKNOWN_MEMBER_ID)
            else
              responseCallback(Errors.REBALANCE_IN_PROGRESS)

          case PreparingRebalance =>
            if (!group.has(memberId)) {
              responseCallback(Errors.UNKNOWN_MEMBER_ID)
            } else if (generationId != group.generationId) {
              responseCallback(Errors.ILLEGAL_GENERATION)
            } else {
              val member = group.get(memberId)
              completeAndScheduleNextHeartbeatExpiration(group, member)
              responseCallback(Errors.REBALANCE_IN_PROGRESS)
            }

          case Stable =>
            if (!group.has(memberId)) {
              responseCallback(Errors.UNKNOWN_MEMBER_ID)
            } else if (generationId != group.generationId) {
              responseCallback(Errors.ILLEGAL_GENERATION)
            } else {
              val member = group.get(memberId)
              completeAndScheduleNextHeartbeatExpiration(group, member)
              responseCallback(Errors.NONE)
            }
        }
      }
    }
  }

  def handleTxnCommitOffsets(groupId: String,
                             producerId: Long,
                             producerEpoch: Short,
                             offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                             responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.TXN_OFFSET_COMMIT) match {
      case Some(error) => responseCallback(offsetMetadata.mapValues(_ => error))
      case None =>
        val group = groupManager.getGroup(groupId).getOrElse {
          groupManager.addGroup(new GroupMetadata(groupId, Empty, time))
        }
        doCommitOffsets(group, NoMemberId, NoGeneration, producerId, producerEpoch, offsetMetadata, responseCallback)
    }
  }

  def handleCommitOffsets(groupId: String,
                          memberId: String,
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Errors] => Unit) {
    validateGroupStatus(groupId, ApiKeys.OFFSET_COMMIT) match {
      case Some(error) => responseCallback(offsetMetadata.mapValues(_ => error))
      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            if (generationId < 0) {
              // the group is not relying on Kafka for group management, so allow the commit
              val group = groupManager.addGroup(new GroupMetadata(groupId, Empty, time))
              doCommitOffsets(group, memberId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
                offsetMetadata, responseCallback)
            } else {
              // or this is a request coming from an older generation. either way, reject the commit
              responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION))
            }

          case Some(group) =>
            doCommitOffsets(group, memberId, generationId, NO_PRODUCER_ID, NO_PRODUCER_EPOCH,
              offsetMetadata, responseCallback)
        }
    }
  }

  def scheduleHandleTxnCompletion(producerId: Long,
                                  offsetsPartitions: Iterable[TopicPartition],
                                  transactionResult: TransactionResult) {
    require(offsetsPartitions.forall(_.topic == Topic.GROUP_METADATA_TOPIC_NAME))
    val isCommit = transactionResult == TransactionResult.COMMIT
    groupManager.scheduleHandleTxnCompletion(producerId, offsetsPartitions.map(_.partition).toSet, isCommit)
  }

  private def doCommitOffsets(group: GroupMetadata,
                              memberId: String,
                              generationId: Int,
                              producerId: Long,
                              producerEpoch: Short,
                              offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                              responseCallback: immutable.Map[TopicPartition, Errors] => Unit) {
    group.inLock {
      if (group.is(Dead)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID))
      } else if ((generationId < 0 && group.is(Empty)) || (producerId != NO_PRODUCER_ID)) {
        // The group is only using Kafka to store offsets.
        // Also, for transactional offset commits we don't need to validate group membership and the generation.
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback, producerId, producerEpoch)
      } else if (group.is(CompletingRebalance)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.REBALANCE_IN_PROGRESS))
      } else if (!group.has(memberId)) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.UNKNOWN_MEMBER_ID))
      } else if (generationId != group.generationId) {
        responseCallback(offsetMetadata.mapValues(_ => Errors.ILLEGAL_GENERATION))
      } else {
        val member = group.get(memberId)
        completeAndScheduleNextHeartbeatExpiration(group, member)
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback)
      }
    }
  }

  def handleFetchOffsets(groupId: String, partitions: Option[Seq[TopicPartition]] = None):
  (Errors, Map[TopicPartition, OffsetFetchResponse.PartitionData]) = {

    validateGroupStatus(groupId, ApiKeys.OFFSET_FETCH) match {
      case Some(error) => error -> Map.empty
      case None =>
        // return offsets blindly regardless the current group state since the group may be using
        // Kafka commit storage without automatic group management
        (Errors.NONE, groupManager.getOffsets(groupId, partitions))
    }
  }

  def handleListGroups(): (Errors, List[GroupOverview]) = {
    if (!isActive.get) {
      (Errors.COORDINATOR_NOT_AVAILABLE, List[GroupOverview]())
    } else {
      val errorCode = if (groupManager.isLoading) Errors.COORDINATOR_LOAD_IN_PROGRESS else Errors.NONE
      (errorCode, groupManager.currentGroups.map(_.overview).toList)
    }
  }

  def handleDescribeGroup(groupId: String): (Errors, GroupSummary) = {
    validateGroupStatus(groupId, ApiKeys.DESCRIBE_GROUPS) match {
      case Some(error) => (error, GroupCoordinator.EmptyGroup)
      case None =>
        groupManager.getGroup(groupId) match {
          case None => (Errors.NONE, GroupCoordinator.DeadGroup)
          case Some(group) =>
            group.inLock {
              (Errors.NONE, group.summary)
            }
        }
    }
  }

  def handleDeletedPartitions(topicPartitions: Seq[TopicPartition]) {
    val offsetsRemoved = groupManager.cleanupGroupMetadata(groupManager.currentGroups, group => {
      group.removeOffsets(topicPartitions)
    })
    info(s"Removed $offsetsRemoved offsets associated with deleted partitions: ${topicPartitions.mkString(", ")}.")
  }

  private def isValidGroupId(groupId: String, api: ApiKeys): Boolean = {
    api match {
      case ApiKeys.OFFSET_COMMIT | ApiKeys.OFFSET_FETCH | ApiKeys.DESCRIBE_GROUPS | ApiKeys.DELETE_GROUPS =>
        // 为了向后兼容，对于OFFSET_COMMIT、 OFFSET_FETCH、DESCRIBE_GROUPS、DELETE_GROUPS几种api，groupId可以为空串
        // For backwards compatibility, we support the offset commit APIs for the empty groupId, and also
        // in DescribeGroups and DeleteGroups so that users can view and delete state of all groups.
        groupId != null
      case _ =>
        // 其余API是使用Kafka进行组协调的组，并且必须有一个非空的groupId
        // The remaining APIs are groups using Kafka for group coordination and must have a non-empty groupId
        groupId != null && !groupId.isEmpty
    }
  }

  /**
   * Check that the groupId is valid, assigned to this coordinator and that the group has been loaded.
   */
  private def validateGroupStatus(groupId: String, api: ApiKeys): Option[Errors] = {
    if (!isValidGroupId(groupId, api))// 检查groupId是否合法，find coordinator的时候，没有对groupId进行检查，这个没有影响，反正最后会在这里检查出来
      Some(Errors.INVALID_GROUP_ID)
    else if (!isActive.get) // 检查是否是活跃状态，该coordinator启动完成后，状态就是活跃的，在shutdown的时候，就是false
      Some(Errors.COORDINATOR_NOT_AVAILABLE)
    else if (isCoordinatorLoadInProgress(groupId))//检查是否处于load offset状态
      Some(Errors.COORDINATOR_LOAD_IN_PROGRESS)
    else if (!isCoordinatorForGroup(groupId))// 检查是否是这个coordinator负责的group
      Some(Errors.NOT_COORDINATOR)
    else // 如果都没问题
      None
  }

  private def onGroupUnloaded(group: GroupMetadata) {
    group.inLock {
      info(s"Unloading group metadata for ${group.groupId} with generation ${group.generationId}")
      val previousState = group.currentState
      group.transitionTo(Dead)

      previousState match {
        case Empty | Dead =>
        case PreparingRebalance =>
          for (member <- group.allMemberMetadata) {
            group.maybeInvokeJoinCallback(member, joinError(member.memberId, Errors.NOT_COORDINATOR))
          }

          joinPurgatory.checkAndComplete(GroupKey(group.groupId))

        case Stable | CompletingRebalance =>
          for (member <- group.allMemberMetadata) {
            if (member.awaitingSyncCallback != null) {
              member.awaitingSyncCallback(Array.empty[Byte], Errors.NOT_COORDINATOR)
              member.awaitingSyncCallback = null
            }
            heartbeatPurgatory.checkAndComplete(MemberKey(member.groupId, member.memberId))
          }
      }
    }
  }

  private def onGroupLoaded(group: GroupMetadata) {
    group.inLock {
      info(s"Loading group metadata for ${group.groupId} with generation ${group.generationId}")
      assert(group.is(Stable) || group.is(Empty))
      if (groupIsOverCapacity(group)) {
        prepareRebalance(group, s"Freshly-loaded group is over capacity ($groupConfig.groupMaxSize). Rebalacing in order to give a chance for consumers to commit offsets")
      }

      group.allMemberMetadata.foreach(completeAndScheduleNextHeartbeatExpiration(group, _))
    }
  }

  def handleGroupImmigration(offsetTopicPartitionId: Int) {
    groupManager.scheduleLoadGroupAndOffsets(offsetTopicPartitionId, onGroupLoaded)
  }

  def handleGroupEmigration(offsetTopicPartitionId: Int) {
    groupManager.removeGroupsForPartition(offsetTopicPartitionId, onGroupUnloaded)
  }

  private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]) {
    assert(group.is(CompletingRebalance))
    group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))
    propagateAssignment(group, Errors.NONE)
  }

  private def resetAndPropagateAssignmentError(group: GroupMetadata, error: Errors) {
    assert(group.is(CompletingRebalance))// 检查是否是出于正在完成rebalance阶段
    group.allMemberMetadata.foreach(_.assignment = Array.empty[Byte])// 遍历所有member的分配，将分配方案赋值为空byte数组
    propagateAssignment(group, error)//然后传播分配
  }

  private def propagateAssignment(group: GroupMetadata, error: Errors) {
    for (member <- group.allMemberMetadata) {
      if (member.awaitingSyncCallback != null) {
        member.awaitingSyncCallback(member.assignment, error)
        member.awaitingSyncCallback = null

        // reset the session timeout for members after propagating the member's assignment.
        // This is because if any member's session expired while we were still awaiting either
        // the leader sync group or the storage callback, its expiration will be ignored and no
        // future heartbeat expectations will not be scheduled.
        completeAndScheduleNextHeartbeatExpiration(group, member)
      }
    }
  }

  private def joinError(memberId: String, error: Errors): JoinGroupResult = {
    JoinGroupResult(
      members = Map.empty,
      memberId = memberId,
      generationId = GroupCoordinator.NoGeneration,
      subProtocol = GroupCoordinator.NoProtocol,
      leaderId = GroupCoordinator.NoLeader,
      error = error)
  }

  /**
   * Complete existing DelayedHeartbeats for the given member and schedule the next one
   */
  private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, member: MemberMetadata) {
    completeAndScheduleNextExpiration(group, member, member.sessionTimeoutMs)
  }

  private def completeAndScheduleNextExpiration(group: GroupMetadata, member: MemberMetadata, timeoutMs: Long): Unit = {
    // 成员key
    val memberKey = MemberKey(member.groupId, member.memberId)

    // 完成当前心跳预期
    // complete current heartbeat expectation
    member.heartbeatSatisfied = true
    // 检查是否心跳完成，如果还有没完成的，会尝试完成
    heartbeatPurgatory.checkAndComplete(memberKey)

    // 将心跳变量设置为false，标识继续等待下一次心跳完成
    // reschedule the next heartbeat expiration deadline
    member.heartbeatSatisfied = false
    // 构建一个心跳delay task，放入heartbeatPurgatory中
    val delayedHeartbeat = new DelayedHeartbeat(this, group, member.memberId, isPending = false, timeoutMs)
    // 放入心跳监视器中,heartbeatPurgatory背后的原理还是不太清楚
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
  }

  /**
    * Add pending member expiration to heartbeat purgatory
    */
  private def addPendingMemberExpiration(group: GroupMetadata, pendingMemberId: String, timeoutMs: Long) {
    val pendingMemberKey = MemberKey(group.groupId, pendingMemberId)
    val delayedHeartbeat = new DelayedHeartbeat(this, group, pendingMemberId, isPending = true, timeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(pendingMemberKey))
  }

  private def removeHeartbeatForLeavingMember(group: GroupMetadata, member: MemberMetadata) {
    member.isLeaving = true
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)
  }

  /**
   * rebalanceTimeoutMs: 传入rebalance超时;
   * sessionTimeoutMs: 传入sessionTimeoutMs;
   * memberId: 成员id
   * clientId: 客户端id
   * clientHost: 客户端地址
   * protocolType: 协议类型
   * protocols: 客户端带过来的协议元数据： 包括订阅的topic以及配置的分区分配策略
   * group: group metadata,记录了该group的groupId和group状态以及时间
   * callback: join后的回调方法
   * */
  private def addMemberAndRebalance(rebalanceTimeoutMs: Int,
                                    sessionTimeoutMs: Int,
                                    memberId: String,
                                    clientId: String,
                                    clientHost: String,
                                    protocolType: String,
                                    protocols: List[(String, Array[Byte])],
                                    group: GroupMetadata,
                                    callback: JoinCallback) {
    // 首先构建member的元数据
    val member = new MemberMetadata(memberId, group.groupId, clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, protocols)

    // 该member是否是新加入的，这个拿来干嘛呀? 如果是新加入的member，那么就要检查心跳是否过期，当然只有等待加入的时候，
    // 才可以忽略心跳,所谓等待加入，实际就是该join请求已经到服务端了，并且在处理了，此时即便检查到该member心跳过期，那也得让他加入；
    member.isNew = true

    // 更新 newMemberAdded 标志以指示加入组可以进一步延迟
    // PreparingRebalance: 开始等待join加入，rebalance阶段；
    // generationId这个变量是干嘛的？ 每完成一轮group的join，该变量就会加1
    // update the newMemberAdded flag to indicate that the join group can be further delayed
    if (group.is(PreparingRebalance) && group.generationId == 0)
      group.newMemberAdded = true

    // 将该member放入到该group中，同时记录上对应的callback，这个callback就是该member完成后回复response的
    group.add(member, callback)

    // 会话超时不会影响新成员，因为他们没有memberId，也无法发送检测信号。
    // 此外，我们无法检测到断开连接，因为当JoinGroup处于炼狱中时，套接字被静音。
    // 如果客户端确实断开了连接（例如，由于长时间重新平衡期间的请求超时），他们可能会简单地重试，这将导致重新平衡中有许多不起作用的成员
    // 为了防止这种情况无限期地发生，我们暂停JoinGroup对新成员的请求。如果新成员仍然存在，我们希望它重试。
    // The session timeout does not affect new members since they do not have their memberId and
    // cannot send heartbeats. Furthermore, we cannot detect disconnects because sockets are muted
    // while the JoinGroup is in purgatory. If the client does disconnect (e.g. because of a request
    // timeout during a long rebalance), they may simply retry which will lead to a lot of defunct
    // members in the rebalance. To prevent this going on indefinitely, we timeout JoinGroup requests
    // for new members. If the new member is still there, we expect it to retry.
    // 完成并安排下一次到期
    completeAndScheduleNextExpiration(group, member, NewMemberJoinTimeoutMs)// 新成员加入超时，写死的5min

    // 从pending的member中移除该member
    group.removePendingMember(memberId)
    // 也许准备重新平衡
    maybePrepareRebalance(group, s"Adding new member $memberId")
  }

  private def updateMemberAndRebalance(group: GroupMetadata,
                                       member: MemberMetadata,
                                       protocols: List[(String, Array[Byte])],
                                       callback: JoinCallback) {
    // 更新该group的member
    group.updateMember(member, protocols, callback)
    // 更新该group的状态
    maybePrepareRebalance(group, s"Updating metadata for member ${member.memberId}")
  }

  // 尝试重新rebalance
  private def maybePrepareRebalance(group: GroupMetadata, reason: String) {
    group.inLock {
      if (group.canRebalance) {// 首先检查是否可以rebalance:就是检查当前状态的前一个状态是否合理
        // 可以的话就准备rebalance
        prepareRebalance(group, reason)
      }
    }
  }

  private def prepareRebalance(group: GroupMetadata, reason: String) {
    // if any members are awaiting sync, cancel their request and have them rejoin
    if (group.is(CompletingRebalance))// 如果是出于正在完成rebalance阶段，那么就取消该join请求，让其重新发送join
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)//给每一个member发送一个正在rebalance的错误码，让其重新发送join请求

    // 当group状态为empty的时候，只能通过一段时间内，有没有member加入来判断是否完成一轮join阶段；
    // 如果group状态不是empty，那么就知道该group有哪些member，那么在每次加入member的时候，都可以尝试tryComplete，在该方法中会去判断是否所有member都加入进来了，如果都加入了，那么就会complete；
    // 为啥group 为empty时，不能用DelayedJoin，因为如果用DelayedJoin的话，每次tryComplete都会返回true，这样如果加入十个consumer成员，就会触发十次rebalance
    val delayedRebalance = if (group.is(Empty)) {// 如果empty为空，那么就构建一个初始delayjoin请求，否则就构建一个delayjoin请求
      // 只有第一次join的时候，每次delay唤醒时间是groupInitialRebalanceDelayMs,每隔groupInitialRebalanceDelayMs都会检查一下是否完成
      new InitialDelayedJoin(this,
        joinPurgatory,
        group,
        groupConfig.groupInitialRebalanceDelayMs,
        groupConfig.groupInitialRebalanceDelayMs,
        max(group.rebalanceTimeoutMs - groupConfig.groupInitialRebalanceDelayMs, 0))
    } else {
      // 后面再join的话，都会等rebalanceTimeoutMs才会检查一次，只检查一次
      new DelayedJoin(this, group, group.rebalanceTimeoutMs)
    }

    // 将该group的状态转成准备rebalance状态
    group.transitionTo(PreparingRebalance)

    info(s"Preparing to rebalance group ${group.groupId} in state ${group.currentState} with old generation " +
      s"${group.generationId} (${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)}) (reason: $reason)")

    val groupKey = GroupKey(group.groupId)
    // 尝试完成该delay join请求，并会watch
    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }

  private def removeMemberAndUpdateGroup(group: GroupMetadata, member: MemberMetadata, reason: String) {
    // New members may timeout with a pending JoinGroup while the group is still rebalancing, so we have
    // to invoke the callback before removing the member. We return UNKNOWN_MEMBER_ID so that the consumer
    // will retry the JoinGroup request if is still active.
    group.maybeInvokeJoinCallback(member, joinError(NoMemberId, Errors.UNKNOWN_MEMBER_ID))

    group.remove(member.memberId)

    group.currentState match {
      case Dead | Empty =>
      case Stable | CompletingRebalance => maybePrepareRebalance(group, reason)
      case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  private def removePendingMemberAndUpdateGroup(group: GroupMetadata, memberId: String) {
    group.removePendingMember(memberId)

    if (group.is(PreparingRebalance)) {
      joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean) = {
    group.inLock {
      // 如果所有member都加入了,并且没有挂起的member，那么就强制complete完成
      if (group.hasAllMembersJoined)
        forceComplete()
      else false
    }
  }

  // 过期不会做什么
  def onExpireJoin() {
    // TODO: add metrics for restabilize timeouts
  }

  def onCompleteJoin(group: GroupMetadata) {
    group.inLock {
      // 删除所有尚未加入group的member(正在等待加入的member)
      // remove any members who haven't joined the group yet
      group.notYetRejoinedMembers.foreach { failedMember =>
        // 首先将这些要移除的member，从心跳中移除
        removeHeartbeatForLeavingMember(group, failedMember)
        group.remove(failedMember.memberId)
        // TODO: cut the socket connection to the client
      }

      if (!group.is(Dead)) {// 如果group不是dead状态
        // 选出该group使用的分区策略，并且将该group的状态切换到CompletingRebalance
        group.initNextGeneration()
        if (group.is(Empty)) {// 如果此时group的状态切换到了empty，那么将该group的GroupMetadata信息持久化
          info(s"Group ${group.groupId} with generation ${group.generationId} is now empty " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

          groupManager.storeGroup(group, Map.empty, error => {
            if (error != Errors.NONE) {
              // we failed to write the empty group metadata. If the broker fails before another rebalance,
              // the previous generation written to the log will become active again (and most likely timeout).
              // This should be safe since there are no active members in an empty generation, so we just warn.
              warn(s"Failed to write empty metadata for group ${group.groupId}: ${error.message}")
            }
          })
        } else {
          info(s"Stabilized group ${group.groupId} generation ${group.generationId} " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")
          // 遍历每个member,将每个member的call back函数进行回调
          // trigger the awaiting join group response callback for all the members after rebalancing
          for (member <- group.allMemberMetadata) {
            assert(member.awaitingJoinCallback != null)
            val joinResult = JoinGroupResult(
              // 如果是leader的话，要把所有成员信息以及成员的分配信息返回
              members = if (group.isLeader(member.memberId)) {
                group.currentMemberMetadata
              } else {
                Map.empty
              },
              memberId = member.memberId,
              generationId = group.generationId,
              subProtocol = group.protocolOrNull,
              leaderId = group.leaderOrNull,
              error = Errors.NONE)

            group.maybeInvokeJoinCallback(member, joinResult)
            completeAndScheduleNextHeartbeatExpiration(group, member)
            member.isNew = false
          }
        }
      }
    }
  }

  def tryCompleteHeartbeat(group: GroupMetadata,
                           memberId: String,
                           isPending: Boolean,
                           forceComplete: () => Boolean): Boolean = {
    group.inLock {
      // The group has been unloaded and invalid, we should complete the heartbeat.
      if (group.is(Dead)) {
        forceComplete()
      } else if (isPending) {
        // complete the heartbeat if the member has joined the group
        if (group.has(memberId)) {
          forceComplete()
        } else false
      } else if (shouldCompleteNonPendingHeartbeat(group, memberId)) {
        forceComplete()
      } else false
    }
  }

  def shouldCompleteNonPendingHeartbeat(group: GroupMetadata, memberId: String): Boolean = {
    if (group.has(memberId)) {
      val member = group.get(memberId)
      member.hasSatisfiedHeartbeat || member.isLeaving
    } else {
      info(s"Member id $memberId was not found in ${group.groupId} during heartbeat completion check")
      true
    }
  }

  def onExpireHeartbeat(group: GroupMetadata, memberId: String, isPending: Boolean): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        info(s"Received notification of heartbeat expiration for member $memberId after group ${group.groupId} had already been unloaded or deleted.")
      } else if (isPending) {
        info(s"Pending member $memberId in group ${group.groupId} has been removed after session timeout expiration.")
        removePendingMemberAndUpdateGroup(group, memberId)
      } else if (!group.has(memberId)) {
        debug(s"Member $memberId has already been removed from the group.")
      } else {
        val member = group.get(memberId)
        if (!member.hasSatisfiedHeartbeat) {
          info(s"Member ${member.memberId} in group ${group.groupId} has failed, removing it from the group")
          removeMemberAndUpdateGroup(group, member, s"removing member ${member.memberId} on heartbeat expiration")
        }
      }
    }
  }

  def onCompleteHeartbeat() {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = groupManager.partitionFor(group)

  private def groupIsOverCapacity(group: GroupMetadata): Boolean = {
    group.size > groupConfig.groupMaxSize
  }

  private def isCoordinatorForGroup(groupId: String) = groupManager.isGroupLocal(groupId)

  private def isCoordinatorLoadInProgress(groupId: String) = groupManager.isGroupLoading(groupId)
}

object GroupCoordinator {

  val NoState = ""
  val NoProtocolType = ""
  val NoProtocol = ""
  val NoLeader = ""
  val NoGeneration = -1
  val NoMemberId = ""
  val NoMembers = List[MemberSummary]()
  val EmptyGroup = GroupSummary(NoState, NoProtocolType, NoProtocol, NoMembers)
  val DeadGroup = GroupSummary(Dead.toString, NoProtocolType, NoProtocol, NoMembers)
  val NewMemberJoinTimeoutMs: Int = 5 * 60 * 1000

  def apply(config: KafkaConfig,
            zkClient: KafkaZkClient,
            replicaManager: ReplicaManager,
            time: Time): GroupCoordinator = {
    val heartbeatPurgatory = DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId)
    val joinPurgatory = DelayedOperationPurgatory[DelayedJoin]("Rebalance", config.brokerId)
    apply(config, zkClient, replicaManager, heartbeatPurgatory, joinPurgatory, time)
  }

  private[group] def offsetConfig(config: KafkaConfig) = OffsetConfig(
    maxMetadataSize = config.offsetMetadataMaxSize,
    loadBufferSize = config.offsetsLoadBufferSize,
    offsetsRetentionMs = config.offsetsRetentionMinutes * 60L * 1000L,
    offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
    offsetsTopicNumPartitions = config.offsetsTopicPartitions,
    offsetsTopicSegmentBytes = config.offsetsTopicSegmentBytes,
    offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
    offsetsTopicCompressionCodec = config.offsetsTopicCompressionCodec,
    offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
    offsetCommitRequiredAcks = config.offsetCommitRequiredAcks
  )

  def apply(config: KafkaConfig,
            zkClient: KafkaZkClient,
            replicaManager: ReplicaManager,
            heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
            joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
            time: Time): GroupCoordinator = {
    val offsetConfig = this.offsetConfig(config)
    val groupConfig = GroupConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
      groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs,
      groupMaxSize = config.groupMaxSize,
      groupInitialRebalanceDelayMs = config.groupInitialRebalanceDelay)

    val groupMetadataManager = new GroupMetadataManager(config.brokerId, config.interBrokerProtocolVersion,
      offsetConfig, replicaManager, zkClient, time)
    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, groupMetadataManager, heartbeatPurgatory, joinPurgatory, time)
  }

}

case class GroupConfig(groupMinSessionTimeoutMs: Int,
                       groupMaxSessionTimeoutMs: Int,
                       groupMaxSize: Int,
                       groupInitialRebalanceDelayMs: Int)

case class JoinGroupResult(members: Map[String, Array[Byte]],
                           memberId: String,
                           generationId: Int,
                           subProtocol: String,
                           leaderId: String,
                           error: Errors)
