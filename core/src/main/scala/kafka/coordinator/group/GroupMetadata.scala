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
package kafka.coordinator.group

import java.util.UUID
import java.util.concurrent.locks.ReentrantLock

import kafka.common.OffsetAndMetadata
import kafka.utils.{CoreUtils, Logging, nonthreadsafe}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Time

import scala.collection.{Seq, immutable, mutable}

private[group] sealed trait GroupState

/**
 * Group is preparing to rebalance
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to sync group with REBALANCE_IN_PROGRESS
 *         remove member on leave group request
 *         park join group requests from new or existing members until all expected members have joined
 *         allow offset commits from previous generation
 *         allow offset fetch requests
 * transition: some members have joined by the timeout => CompletingRebalance
 *             all members have left the group => Empty
 *             group is removed by partition emigration => Dead
 */
private[group] case object PreparingRebalance extends GroupState

/**
 * Group is awaiting state assignment from the leader
 *
 * action: respond to heartbeats with REBALANCE_IN_PROGRESS
 *         respond to offset commits with REBALANCE_IN_PROGRESS
 *         park sync group requests from followers until transition to Stable
 *         allow offset fetch requests
 * transition: sync group with state assignment received from leader => Stable
 *             join group from new member or existing member with updated metadata => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             member failure detected => PreparingRebalance
 *             group is removed by partition emigration => Dead
 */
private[group] case object CompletingRebalance extends GroupState

/**
 * Group is stable
 *
 * action: respond to member heartbeats normally
 *         respond to sync group from any member with current assignment
 *         respond to join group from followers with matching metadata with current group metadata
 *         allow offset commits from member of current generation
 *         allow offset fetch requests
 * transition: member failure detected via heartbeat => PreparingRebalance
 *             leave group from existing member => PreparingRebalance
 *             leader join-group received => PreparingRebalance
 *             follower join-group with new metadata => PreparingRebalance
 *             group is removed by partition emigration => Dead
 */
private[group] case object Stable extends GroupState

/**
 * Group has no more members and its metadata is being removed
 *
 * action: respond to join group with UNKNOWN_MEMBER_ID
 *         respond to sync group with UNKNOWN_MEMBER_ID
 *         respond to heartbeat with UNKNOWN_MEMBER_ID
 *         respond to leave group with UNKNOWN_MEMBER_ID
 *         respond to offset commit with UNKNOWN_MEMBER_ID
 *         allow offset fetch requests
 * transition: Dead is a final state before group metadata is cleaned up, so there are no transitions
 */
private[group] case object Dead extends GroupState

/**
  * Group has no more members, but lingers until all offsets have expired. This state
  * also represents groups which use Kafka only for offset commits and have no members.
  *
  * action: respond normally to join group from new members
  *         respond to sync group with UNKNOWN_MEMBER_ID
  *         respond to heartbeat with UNKNOWN_MEMBER_ID
  *         respond to leave group with UNKNOWN_MEMBER_ID
  *         respond to offset commit with UNKNOWN_MEMBER_ID
  *         allow offset fetch requests
  * transition: last offsets removed in periodic expiration task => Dead
  *             join group from a new member => PreparingRebalance
  *             group is removed by partition emigration => Dead
  *             group is removed by expiration => Dead
  */
private[group] case object Empty extends GroupState


private object GroupMetadata {
  private val validPreviousStates: Map[GroupState, Set[GroupState]] =
    Map(Dead -> Set(Stable, PreparingRebalance, CompletingRebalance, Empty, Dead),
      CompletingRebalance -> Set(PreparingRebalance),
      Stable -> Set(CompletingRebalance),
      PreparingRebalance -> Set(Stable, CompletingRebalance, Empty),
      Empty -> Set(PreparingRebalance))

  def loadGroup(groupId: String,
                initialState: GroupState,
                generationId: Int,
                protocolType: String,
                protocol: String,
                leaderId: String,
                currentStateTimestamp: Option[Long],
                members: Iterable[MemberMetadata],
                time: Time): GroupMetadata = {
    val group = new GroupMetadata(groupId, initialState, time)
    group.generationId = generationId
    group.protocolType = if (protocolType == null || protocolType.isEmpty) None else Some(protocolType)
    group.protocol = Option(protocol)
    group.leaderId = Option(leaderId)
    group.currentStateTimestamp = currentStateTimestamp
    members.foreach(group.add(_, null))
    group
  }
}

/**
 * Case class used to represent group metadata for the ListGroups API
 */
case class GroupOverview(groupId: String,
                         protocolType: String)

/**
 * Case class used to represent group metadata for the DescribeGroup API
 */
case class GroupSummary(state: String,
                        protocolType: String,
                        protocol: String,
                        members: List[MemberSummary])

/**
  * We cache offset commits along with their commit record offset. This enables us to ensure that the latest offset
  * commit is always materialized when we have a mix of transactional and regular offset commits. Without preserving
  * information of the commit record offset, compaction of the offsets topic it self may result in the wrong offset commit
  * being materialized.
  */
case class CommitRecordMetadataAndOffset(appendedBatchOffset: Option[Long], offsetAndMetadata: OffsetAndMetadata) {
  def olderThan(that: CommitRecordMetadataAndOffset) : Boolean = appendedBatchOffset.get < that.appendedBatchOffset.get
}

/**
 * Group contains the following metadata:
 *
 *  Membership metadata:
 *  1. Members registered in this group
 *  2. Current protocol assigned to the group (e.g. partition assignment strategy for consumers)
 *  3. Protocol metadata associated with group members
 *
 *  State metadata:
 *  1. group state
 *  2. generation id
 *  3. leader id
 */
@nonthreadsafe
private[group] class GroupMetadata(val groupId: String, initialState: GroupState, time: Time) extends Logging {
  type JoinCallback = JoinGroupResult => Unit

  private[group] val lock = new ReentrantLock

  private var state: GroupState = initialState
  var currentStateTimestamp: Option[Long] = Some(time.milliseconds())
  var protocolType: Option[String] = None
  var generationId = 0
  private var leaderId: Option[String] = None
  private var protocol: Option[String] = None

  private val members = new mutable.HashMap[String, MemberMetadata]
  private val pendingMembers = new mutable.HashSet[String]
  private var numMembersAwaitingJoin = 0
  private val supportedProtocols = new mutable.HashMap[String, Integer]().withDefaultValue(0)
  private val offsets = new mutable.HashMap[TopicPartition, CommitRecordMetadataAndOffset]
  private val pendingOffsetCommits = new mutable.HashMap[TopicPartition, OffsetAndMetadata]
  private val pendingTransactionalOffsetCommits = new mutable.HashMap[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]()
  private var receivedTransactionalOffsetCommits = false
  private var receivedConsumerOffsetCommits = false

  var newMemberAdded: Boolean = false

  def inLock[T](fun: => T): T = CoreUtils.inLock(lock)(fun)

  def is(groupState: GroupState) = state == groupState
  def not(groupState: GroupState) = state != groupState
  def has(memberId: String) = members.contains(memberId)
  def get(memberId: String) = members(memberId)
  def size = members.size

  def isLeader(memberId: String): Boolean = leaderId.contains(memberId)
  def leaderOrNull: String = leaderId.orNull
  def protocolOrNull: String = protocol.orNull
  def currentStateTimestampOrDefault: Long = currentStateTimestamp.getOrElse(-1)

  def add(member: MemberMetadata, callback: JoinCallback = null) {
    // 如果没有成员，那么将该成员的协议类型赋值给group的协议变量，这里的协议类型有很多吗
    if (members.isEmpty)
      this.protocolType = Some(member.protocolType)

    // 首先检查groupId是否等于当前member的groupId
    assert(groupId == member.groupId)
    // 检查protocolType为否等于member的type
    assert(this.protocolType.orNull == member.protocolType)
    //检查支持的协议，这里检查的目的是啥？协议类型的作用是啥？检查服务端记录的协议和member数是否一致
    // 这里是为了检查，该member携带的分区策略中，是否有一个是所有member都有的，必须要保证至少有一个分区策略是所有member都有的，然后在投票选择
    assert(supportsProtocols(member.protocolType, MemberMetadata.plainProtocolSet(member.supportedProtocols)))

    // leaderId是否为空，如果为空，那么就说明当前加入的这个member是第一个，那么就选它做leader
    if (leaderId.isEmpty)
      leaderId = Some(member.memberId)
    // 记录memberId和对应的member，将该成员加入到members集合列表中
    members.put(member.memberId, member)
    // 将该成员增加的协议对应内存中维护的，全部加1，内存中记录每个协议有多少个成员的作用是啥
    // supportedProtocols的key是使用的分区策略名，value是该策略对应的member数，回复response的时候会回一个分区策略，
    // 服务端在选择使用哪个分区策略的时候，会首先过滤到不是所有member都支持的分区策略，然后再投票，选出一个分区策略给客户端
    // 投票的规则就是按照supportedProtocols中的顺序来，如果第3个策略，所有member都满足，那么所有的member都会投它一票，从而就都会选它作为分区策略
    // 客户端在配置分区策略时，可以填多个分区策略
    member.supportedProtocols.foreach{ case (protocol, _) => supportedProtocols(protocol) += 1 }
    // 当join操作完成后(不管是正常还是异常完成，或中途被移除)，那么就会调用该callback
    member.awaitingJoinCallback = callback
    if (member.isAwaitingJoin) {
    //如果该member处于awaiting join状态，那么就累计加1,只要awaitingJoinCallback不为null，那么就是处于等待join状态
      // 从将该member加入内存开始，到该memberjoin完成后，调回调方法，都是awaitingJoin状态
      numMembersAwaitingJoin += 1
    }
  }

  def remove(memberId: String) {
    members.remove(memberId).foreach { member =>
      member.supportedProtocols.foreach{ case (protocol, _) => supportedProtocols(protocol) -= 1 }
      if (member.isAwaitingJoin)
        numMembersAwaitingJoin -= 1
    }

    if (isLeader(memberId)) {
      leaderId = if (members.isEmpty) {
        None
      } else {
        Some(members.keys.head)
      }
    }
  }

  def isPendingMember(memberId: String): Boolean = pendingMembers.contains(memberId) && !has(memberId)

  def addPendingMember(memberId: String) = pendingMembers.add(memberId)

  def removePendingMember(memberId: String) = pendingMembers.remove(memberId)

  def currentState = state

  def notYetRejoinedMembers = members.values.filter(!_.isAwaitingJoin).toList

  def hasAllMembersJoined = members.size == numMembersAwaitingJoin && pendingMembers.isEmpty

  def allMembers = members.keySet

  def numPending = pendingMembers.size

  def allMemberMetadata = members.values.toList

  def rebalanceTimeoutMs = members.values.foldLeft(0) { (timeout, member) =>
    timeout.max(member.rebalanceTimeoutMs)
  }

  def generateMemberIdSuffix = UUID.randomUUID().toString

  def canRebalance = GroupMetadata.validPreviousStates(PreparingRebalance).contains(state)

  def transitionTo(groupState: GroupState) {
    assertValidTransition(groupState)
    state = groupState
    currentStateTimestamp = Some(time.milliseconds())
  }

  def selectProtocol: String = {
    if (members.isEmpty)
      throw new IllegalStateException("Cannot select protocol for empty group")

    // select the protocol for this group which is supported by all members
    val candidates = candidateProtocols

    // let each member vote for one of the protocols and choose the one with the most votes
    val votes: List[(String, Int)] = allMemberMetadata
      .map(_.vote(candidates))
      .groupBy(identity)
      .mapValues(_.size)
      .toList

    votes.maxBy(_._2)._1
  }

  private def candidateProtocols = {
    // get the set of protocols that are commonly supported by all members
    val numMembers = members.size
    supportedProtocols.filter(_._2 == numMembers).map(_._1).toSet
  }

  def supportsProtocols(memberProtocolType: String, memberProtocols: Set[String]) = {
    if (is(Empty))// 如果当前group的状态是empty，那么就检查协议类型和协议是否都不为空，有一个为空，那么就返回false
      !memberProtocolType.isEmpty && memberProtocols.nonEmpty
    else {
      // 看看是不是所有的member都使用了同一种分区策略：比如都使用轮训 或stik 分区策略，如果不同的member使用了不同的策略，那么就报错
      // 只要有一个协议是所有成员都有的就行
      protocolType.contains(memberProtocolType) && memberProtocols.exists(supportedProtocols(_) == members.size)
    }
  }

  def updateMember(member: MemberMetadata,
                   protocols: List[(String, Array[Byte])],
                   callback: JoinCallback) = {
    // 首先每个member对应协议里的成员计数减1
    member.supportedProtocols.foreach{ case (protocol, _) => supportedProtocols(protocol) -= 1 }
    // 为啥上面减了，这里又要加呢
    protocols.foreach{ case (protocol, _) => supportedProtocols(protocol) += 1 }
    // 更新该member支持的协议
    member.supportedProtocols = protocols

    // 如果该member的callback回调不为null，那么等待join计数加1，否则减1
    if (callback != null && !member.isAwaitingJoin) {
      numMembersAwaitingJoin += 1
    } else if (callback == null && member.isAwaitingJoin) {
      numMembersAwaitingJoin -= 1
    }
    // 将callback赋值给awaitingJoinCallback
    member.awaitingJoinCallback = callback
  }

  def maybeInvokeJoinCallback(member: MemberMetadata,
                              joinGroupResult: JoinGroupResult) : Unit = {
    if (member.isAwaitingJoin) {
      member.awaitingJoinCallback(joinGroupResult)
      member.awaitingJoinCallback = null
      numMembersAwaitingJoin -= 1
    }
  }

  def initNextGeneration() = {
    assert(notYetRejoinedMembers == List.empty[MemberMetadata])
    if (members.nonEmpty) {
      generationId += 1
      protocol = Some(selectProtocol)
      transitionTo(CompletingRebalance)
    } else {
      generationId += 1
      protocol = None
      transitionTo(Empty)
    }
    receivedConsumerOffsetCommits = false
    receivedTransactionalOffsetCommits = false
  }

  def currentMemberMetadata: Map[String, Array[Byte]] = {
    if (is(Dead) || is(PreparingRebalance))
      throw new IllegalStateException("Cannot obtain member metadata for group in state %s".format(state))
    members.map{ case (memberId, memberMetadata) => (memberId, memberMetadata.metadata(protocol.get))}.toMap
  }

  def summary: GroupSummary = {
    if (is(Stable)) {
      val protocol = protocolOrNull
      if (protocol == null)
        throw new IllegalStateException("Invalid null group protocol for stable group")

      val members = this.members.values.map { member => member.summary(protocol) }
      GroupSummary(state.toString, protocolType.getOrElse(""), protocol, members.toList)
    } else {
      val members = this.members.values.map{ member => member.summaryNoMetadata() }
      GroupSummary(state.toString, protocolType.getOrElse(""), GroupCoordinator.NoProtocol, members.toList)
    }
  }

  def overview: GroupOverview = {
    GroupOverview(groupId, protocolType.getOrElse(""))
  }

  def initializeOffsets(offsets: collection.Map[TopicPartition, CommitRecordMetadataAndOffset],
                        pendingTxnOffsets: Map[Long, mutable.Map[TopicPartition, CommitRecordMetadataAndOffset]]) {
    this.offsets ++= offsets
    this.pendingTransactionalOffsetCommits ++= pendingTxnOffsets
  }

  def onOffsetCommitAppend(topicPartition: TopicPartition, offsetWithCommitRecordMetadata: CommitRecordMetadataAndOffset) {
    if (pendingOffsetCommits.contains(topicPartition)) {
      if (offsetWithCommitRecordMetadata.appendedBatchOffset.isEmpty)
        throw new IllegalStateException("Cannot complete offset commit write without providing the metadata of the record " +
          "in the log.")
      if (!offsets.contains(topicPartition) || offsets(topicPartition).olderThan(offsetWithCommitRecordMetadata))
        offsets.put(topicPartition, offsetWithCommitRecordMetadata)
    }

    pendingOffsetCommits.get(topicPartition) match {
      case Some(stagedOffset) if offsetWithCommitRecordMetadata.offsetAndMetadata == stagedOffset =>
        pendingOffsetCommits.remove(topicPartition)
      case _ =>
        // The pendingOffsetCommits for this partition could be empty if the topic was deleted, in which case
        // its entries would be removed from the cache by the `removeOffsets` method.
    }
  }

  def failPendingOffsetWrite(topicPartition: TopicPartition, offset: OffsetAndMetadata): Unit = {
    pendingOffsetCommits.get(topicPartition) match {
      case Some(pendingOffset) if offset == pendingOffset => pendingOffsetCommits.remove(topicPartition)
      case _ =>
    }
  }

  def prepareOffsetCommit(offsets: Map[TopicPartition, OffsetAndMetadata]) {
    receivedConsumerOffsetCommits = true
    pendingOffsetCommits ++= offsets
  }

  def prepareTxnOffsetCommit(producerId: Long, offsets: Map[TopicPartition, OffsetAndMetadata]) {
    trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $offsets is pending")
    receivedTransactionalOffsetCommits = true
    val producerOffsets = pendingTransactionalOffsetCommits.getOrElseUpdate(producerId,
      mutable.Map.empty[TopicPartition, CommitRecordMetadataAndOffset])

    offsets.foreach { case (topicPartition, offsetAndMetadata) =>
      producerOffsets.put(topicPartition, CommitRecordMetadataAndOffset(None, offsetAndMetadata))
    }
  }

  def hasReceivedConsistentOffsetCommits : Boolean = {
    !receivedConsumerOffsetCommits || !receivedTransactionalOffsetCommits
  }

  /* Remove a pending transactional offset commit if the actual offset commit record was not written to the log.
   * We will return an error and the client will retry the request, potentially to a different coordinator.
   */
  def failPendingTxnOffsetCommit(producerId: Long, topicPartition: TopicPartition): Unit = {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffsets) =>
        val pendingOffsetCommit = pendingOffsets.remove(topicPartition)
        trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetCommit failed " +
          s"to be appended to the log")
        if (pendingOffsets.isEmpty)
          pendingTransactionalOffsetCommits.remove(producerId)
      case _ =>
        // We may hit this case if the partition in question has emigrated already.
    }
  }

  def onTxnOffsetCommitAppend(producerId: Long, topicPartition: TopicPartition,
                              commitRecordMetadataAndOffset: CommitRecordMetadataAndOffset) {
    pendingTransactionalOffsetCommits.get(producerId) match {
      case Some(pendingOffset) =>
        if (pendingOffset.contains(topicPartition)
          && pendingOffset(topicPartition).offsetAndMetadata == commitRecordMetadataAndOffset.offsetAndMetadata)
          pendingOffset.update(topicPartition, commitRecordMetadataAndOffset)
      case _ =>
        // We may hit this case if the partition in question has emigrated.
    }
  }

  /* Complete a pending transactional offset commit. This is called after a commit or abort marker is fully written
   * to the log.
   */
  def completePendingTxnOffsetCommit(producerId: Long, isCommit: Boolean): Unit = {
    val pendingOffsetsOpt = pendingTransactionalOffsetCommits.remove(producerId)
    if (isCommit) {
      pendingOffsetsOpt.foreach { pendingOffsets =>
        pendingOffsets.foreach { case (topicPartition, commitRecordMetadataAndOffset) =>
          if (commitRecordMetadataAndOffset.appendedBatchOffset.isEmpty)
            throw new IllegalStateException(s"Trying to complete a transactional offset commit for producerId $producerId " +
              s"and groupId $groupId even though the offset commit record itself hasn't been appended to the log.")

          val currentOffsetOpt = offsets.get(topicPartition)
          if (currentOffsetOpt.forall(_.olderThan(commitRecordMetadataAndOffset))) {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              "committed and loaded into the cache.")
            offsets.put(topicPartition, commitRecordMetadataAndOffset)
          } else {
            trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offset $commitRecordMetadataAndOffset " +
              s"committed, but not loaded since its offset is older than current offset $currentOffsetOpt.")
          }
        }
      }
    } else {
      trace(s"TxnOffsetCommit for producer $producerId and group $groupId with offsets $pendingOffsetsOpt aborted")
    }
  }

  def activeProducers = pendingTransactionalOffsetCommits.keySet

  def hasPendingOffsetCommitsFromProducer(producerId: Long) =
    pendingTransactionalOffsetCommits.contains(producerId)

  def removeAllOffsets(): immutable.Map[TopicPartition, OffsetAndMetadata] = removeOffsets(offsets.keySet.toSeq)

  def removeOffsets(topicPartitions: Seq[TopicPartition]): immutable.Map[TopicPartition, OffsetAndMetadata] = {
    topicPartitions.flatMap { topicPartition =>
      pendingOffsetCommits.remove(topicPartition)
      pendingTransactionalOffsetCommits.foreach { case (_, pendingOffsets) =>
        pendingOffsets.remove(topicPartition)
      }
      val removedOffset = offsets.remove(topicPartition)
      removedOffset.map(topicPartition -> _.offsetAndMetadata)
    }.toMap
  }

  def removeExpiredOffsets(currentTimestamp: Long, offsetRetentionMs: Long) : Map[TopicPartition, OffsetAndMetadata] = {

    def getExpiredOffsets(baseTimestamp: CommitRecordMetadataAndOffset => Long): Map[TopicPartition, OffsetAndMetadata] = {
      offsets.filter {
        case (topicPartition, commitRecordMetadataAndOffset) =>
          !pendingOffsetCommits.contains(topicPartition) && {
            commitRecordMetadataAndOffset.offsetAndMetadata.expireTimestamp match {
              case None =>
                // current version with no per partition retention
                currentTimestamp - baseTimestamp(commitRecordMetadataAndOffset) >= offsetRetentionMs
              case Some(expireTimestamp) =>
                // older versions with explicit expire_timestamp field => old expiration semantics is used
                currentTimestamp >= expireTimestamp
            }
          }
      }.map {
        case (topicPartition, commitRecordOffsetAndMetadata) =>
          (topicPartition, commitRecordOffsetAndMetadata.offsetAndMetadata)
      }.toMap
    }

    val expiredOffsets: Map[TopicPartition, OffsetAndMetadata] = protocolType match {
      case Some(_) if is(Empty) =>
        // no consumer exists in the group =>
        // - if current state timestamp exists and retention period has passed since group became Empty,
        //   expire all offsets with no pending offset commit;
        // - if there is no current state timestamp (old group metadata schema) and retention period has passed
        //   since the last commit timestamp, expire the offset
        getExpiredOffsets(commitRecordMetadataAndOffset =>
          currentStateTimestamp.getOrElse(commitRecordMetadataAndOffset.offsetAndMetadata.commitTimestamp))

      case None =>
        // protocolType is None => standalone (simple) consumer, that uses Kafka for offset storage only
        // expire offsets with no pending offset commit that retention period has passed since their last commit
        getExpiredOffsets(_.offsetAndMetadata.commitTimestamp)

      case _ =>
        Map()
    }

    if (expiredOffsets.nonEmpty)
      debug(s"Expired offsets from group '$groupId': ${expiredOffsets.keySet}")

    offsets --= expiredOffsets.keySet
    expiredOffsets
  }

  def allOffsets = offsets.map { case (topicPartition, commitRecordMetadataAndOffset) =>
    (topicPartition, commitRecordMetadataAndOffset.offsetAndMetadata)
  }.toMap

  def offset(topicPartition: TopicPartition): Option[OffsetAndMetadata] = offsets.get(topicPartition).map(_.offsetAndMetadata)

  // visible for testing
  private[group] def offsetWithRecordMetadata(topicPartition: TopicPartition): Option[CommitRecordMetadataAndOffset] = offsets.get(topicPartition)

  def numOffsets = offsets.size

  def hasOffsets = offsets.nonEmpty || pendingOffsetCommits.nonEmpty || pendingTransactionalOffsetCommits.nonEmpty

  private def assertValidTransition(targetState: GroupState) {
    if (!GroupMetadata.validPreviousStates(targetState).contains(state))
      throw new IllegalStateException("Group %s should be in the %s states before moving to %s state. Instead it is in %s state"
        .format(groupId, GroupMetadata.validPreviousStates(targetState).mkString(","), targetState, state))
  }

  override def toString: String = {
    "GroupMetadata(" +
      s"groupId=$groupId, " +
      s"generation=$generationId, " +
      s"protocolType=$protocolType, " +
      s"currentState=$currentState, " +
      s"members=$members)"
  }

}

