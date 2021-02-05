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
package kafka.admin

import joptsimple.OptionParser
import kafka.server.{ConfigType, DynamicConfig}
import kafka.utils._

import scala.collection._
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import kafka.common.{AdminCommandFailedException, TopicAndPartition}
import kafka.log.LogConfig
import LogConfig._
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.security.JaasUtils

object ReassignPartitionsCommand extends Logging {

  case class Throttle(value: Long, postUpdateAction: () => Unit = () => ())

  private[admin] val NoThrottle = Throttle(-1)

  def main(args: Array[String]): Unit = {

    val opts = validateAndParseArgs(args)
    val zkConnect = opts.options.valueOf(opts.zkConnectOpt)
    val zkUtils = ZkUtils(zkConnect,
                          30000,
                          30000,
                          JaasUtils.isZkSecurityEnabled())
    try {
      if(opts.options.has(opts.verifyOpt))
        verifyAssignment(zkUtils, opts)
      else if(opts.options.has(opts.generateOpt))
        generateAssignment(zkUtils, opts)
      else if (opts.options.has(opts.executeOpt))
        executeAssignment(zkUtils, opts)
    } catch {
      case e: Throwable =>
        println("Partitions reassignment failed due to " + e.getMessage)
        println(Utils.stackTrace(e))
    } finally zkUtils.close()
  }

  def verifyAssignment(zkUtils: ZkUtils, opts: ReassignPartitionsCommandOptions) {
    val jsonFile = opts.options.valueOf(opts.reassignmentJsonFileOpt)
    val jsonString = Utils.readFileAsString(jsonFile)
    verifyAssignment(zkUtils, jsonString)
  }

  def verifyAssignment(zkUtils: ZkUtils, jsonString: String): Unit = {
    println("Status of partition reassignment: ")
    val partitionsToBeReassigned = ZkUtils.parsePartitionReassignmentData(jsonString)
    val reassignedPartitionsStatus = checkIfReassignmentSucceeded(zkUtils, partitionsToBeReassigned)
    reassignedPartitionsStatus.foreach { case (topicPartition, status) =>
      status match {
        case ReassignmentCompleted =>
          println("Reassignment of partition %s completed successfully".format(topicPartition))
        case ReassignmentFailed =>
          println("Reassignment of partition %s failed".format(topicPartition))
        case ReassignmentInProgress =>
          println("Reassignment of partition %s is still in progress".format(topicPartition))
      }
    }
    removeThrottle(zkUtils, partitionsToBeReassigned, reassignedPartitionsStatus)
  }

  private[admin] def removeThrottle(zkUtils: ZkUtils, partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]], reassignedPartitionsStatus: Map[TopicAndPartition, ReassignmentStatus], admin: AdminUtilities = AdminUtils): Unit = {
    var changed = false

    //If all partitions have completed remove the throttle
    if (reassignedPartitionsStatus.forall { case (_, status) => status == ReassignmentCompleted }) {
      //Remove the throttle limit from all brokers in the cluster
      //(as we no longer know which specific brokers were involved in the move)
      for (brokerId <- zkUtils.getAllBrokersInCluster().map(_.id)) {
        val configs = admin.fetchEntityConfig(zkUtils, ConfigType.Broker, brokerId.toString)
        // bitwise OR as we don't want to short-circuit
        if (configs.remove(DynamicConfig.Broker.LeaderReplicationThrottledRateProp) != null
          | configs.remove(DynamicConfig.Broker.FollowerReplicationThrottledRateProp) != null){
          admin.changeBrokerConfig(zkUtils, Seq(brokerId), configs)
          changed = true
        }
      }

      //Remove the list of throttled replicas from all topics with partitions being moved
      val topics = partitionsToBeReassigned.keySet.map(tp => tp.topic).toSeq.distinct
      for (topic <- topics) {
        val configs = admin.fetchEntityConfig(zkUtils, ConfigType.Topic, topic)
        // bitwise OR as we don't want to short-circuit
        if (configs.remove(LogConfig.LeaderReplicationThrottledReplicasProp) != null
          | configs.remove(LogConfig.FollowerReplicationThrottledReplicasProp) != null){
          admin.changeTopicConfig(zkUtils, topic, configs)
          changed = true
        }
      }
      if (changed)
        println("Throttle was removed.")
    }
  }

  def generateAssignment(zkUtils: ZkUtils, opts: ReassignPartitionsCommandOptions) {
    val topicsToMoveJsonFile = opts.options.valueOf(opts.topicsToMoveJsonFileOpt)
    val brokerListToReassign = opts.options.valueOf(opts.brokerListOpt).split(',').map(_.toInt)
    val duplicateReassignments = CoreUtils.duplicates(brokerListToReassign)
    if (duplicateReassignments.nonEmpty)
      throw new AdminCommandFailedException("Broker list contains duplicate entries: %s".format(duplicateReassignments.mkString(",")))
    val topicsToMoveJsonString = Utils.readFileAsString(topicsToMoveJsonFile)
    val disableRackAware = opts.options.has(opts.disableRackAware)
    val (proposedAssignments, currentAssignments) = generateAssignment(zkUtils, brokerListToReassign, topicsToMoveJsonString, disableRackAware)
    println("Current partition replica assignment\n%s\n".format(ZkUtils.formatAsReassignmentJson(currentAssignments)))
    println("Proposed partition reassignment configuration\n%s".format(ZkUtils.formatAsReassignmentJson(proposedAssignments)))
  }

  def generateAssignment(zkUtils: ZkUtils, brokerListToReassign: Seq[Int], topicsToMoveJsonString: String, disableRackAware: Boolean): (Map[TopicAndPartition, Seq[Int]], Map[TopicAndPartition, Seq[Int]]) = {
    val topicsToReassign = ZkUtils.parseTopicsData(topicsToMoveJsonString)
    val duplicateTopicsToReassign = CoreUtils.duplicates(topicsToReassign)
    if (duplicateTopicsToReassign.nonEmpty)
      throw new AdminCommandFailedException("List of topics to reassign contains duplicate entries: %s".format(duplicateTopicsToReassign.mkString(",")))
    val currentAssignment = zkUtils.getReplicaAssignmentForTopics(topicsToReassign)

    val groupedByTopic = currentAssignment.groupBy { case (tp, _) => tp.topic }
    val rackAwareMode = if (disableRackAware) RackAwareMode.Disabled else RackAwareMode.Enforced
    val brokerMetadatas = AdminUtils.getBrokerMetadatas(zkUtils, rackAwareMode, Some(brokerListToReassign))

    val partitionsToBeReassigned = mutable.Map[TopicAndPartition, Seq[Int]]()
    groupedByTopic.foreach { case (topic, assignment) =>
      val (_, replicas) = assignment.head
      val assignedReplicas = AdminUtils.assignReplicasToBrokers(brokerMetadatas, assignment.size, replicas.size)
      partitionsToBeReassigned ++= assignedReplicas.map { case (partition, replicas) =>
        TopicAndPartition(topic, partition) -> replicas
      }
    }
    (partitionsToBeReassigned, currentAssignment)
  }

  def executeAssignment(zkUtils: ZkUtils, opts: ReassignPartitionsCommandOptions) {
    val reassignmentJsonFile =  opts.options.valueOf(opts.reassignmentJsonFileOpt)
    val reassignmentJsonString = Utils.readFileAsString(reassignmentJsonFile)
    /**
     * 这里会获取throttle选项值,如果为-1，那么就是没有传，也就不限速，否则就是要限速。
     * */
    val throttle = if (opts.options.has(opts.throttleOpt)) opts.options.valueOf(opts.throttleOpt) else -1
    executeAssignment(zkUtils, reassignmentJsonString, Throttle(throttle))
  }

  def executeAssignment(zkUtils: ZkUtils, reassignmentJsonString: String, throttle: Throttle) {
    /**
     * 这里是解析json字符串
     * */
    val partitionsToBeReassigned = parseAndValidate(zkUtils, reassignmentJsonString)
    /**
     *
     * */
    val reassignPartitionsCommand = new ReassignPartitionsCommand(zkUtils, partitionsToBeReassigned.toMap)

    /**
     * 如果当前存在某个rebalance任务，那么尝试去修改它的throttle。
     * 也就是说如果想更新某个reassign ，那么可以多次提交reassign，只是改变下throttle就可以修改限速阈值。
     * */
    // If there is an existing rebalance running, attempt to change its throttle
    if (zkUtils.pathExists(ZkUtils.ReassignPartitionsPath)) {
      /**
       * 如果存在真正reassign的任务，那么久重置限速阈值
       * */
      println("There is an existing assignment running.")
      reassignPartitionsCommand.maybeLimit(throttle)
    }
    else {
      /**
       * 否则，就真正限速.
       *
       * */
      printCurrentAssignment(zkUtils, partitionsToBeReassigned)
      if (throttle.value >= 0)
        println(String.format("Warning: You must run Verify periodically, until the reassignment completes, to ensure the throttle is removed. You can also alter the throttle by rerunning the Execute command passing a new value."))
      /**
       * 然后reassignPartitionsCommand.reassignPartitions这个方法才是真正提交json串的
       * */
      if (reassignPartitionsCommand.reassignPartitions(throttle)) {
        println("Successfully started reassignment of partitions.")
      } else
        println("Failed to reassign partitions %s".format(partitionsToBeReassigned))
    }
  }

  def printCurrentAssignment(zkUtils: ZkUtils, partitionsToBeReassigned: Seq[(TopicAndPartition, Seq[Int])]): Unit = {
    /**
     * 这里把当前的副本分配打印出来了，看日志的意思是加上--reassignment-json-file的话，可以保存现在的分配计划。
     * 但是--reassignment-json-file不是指定提交的josn串的文件路径吗，怎么是用来保存当前的分配计划了？看到有个during rollback，难道是说会轮转一份来保存当前的分配计划？
     * */
    // before starting assignment, output the current replica assignment to facilitate rollback
    val currentPartitionReplicaAssignment = zkUtils.getReplicaAssignmentForTopics(partitionsToBeReassigned.map(_._1.topic))
    println("Current partition replica assignment\n\n%s\n\nSave this to use as the --reassignment-json-file option during rollback"
      .format(ZkUtils.formatAsReassignmentJson(currentPartitionReplicaAssignment)))
  }

  def parseAndValidate(zkUtils: ZkUtils, reassignmentJsonString: String): Seq[(TopicAndPartition, Seq[Int])] = {
    /**
     * 这里是没有去重的解析json字符串，然后生成Seq[(TopicAndPartition, Seq[Int])]
     * */
    val partitionsToBeReassigned = ZkUtils.parsePartitionReassignmentDataWithoutDedup(reassignmentJsonString)

    /**
     * 这里是检查reassign json串是否合理：①json串不能为空；①每个tp的AR不能为空。
     * */
    if (partitionsToBeReassigned.isEmpty)
      throw new AdminCommandFailedException("Partition reassignment data file is empty")
    if (partitionsToBeReassigned.exists(_._2.isEmpty)) {
      throw new AdminCommandFailedException("Partition replica list cannot be empty")
    }

    /**
     * 因为上面从json串中获取的partitionsToBeReassigned没有去重，所以下面这里就进行了去重。
     * 返回的是一个去重partition集合。
     * */
    val duplicateReassignedPartitions = CoreUtils.duplicates(partitionsToBeReassigned.map { case (tp, _) => tp })


    /**
     * 如果存在重复的 就抛异常
     * */
    if (duplicateReassignedPartitions.nonEmpty)
      throw new AdminCommandFailedException("Partition reassignment contains duplicate topic partitions: %s".format(duplicateReassignedPartitions.mkString(",")))

    /**
     * 这里检查是否有重复的副本。
     * */
    val duplicateEntries = partitionsToBeReassigned
      .map { case (tp, replicas) => (tp, CoreUtils.duplicates(replicas))}
      .filter { case (_, duplicatedReplicas) => duplicatedReplicas.nonEmpty }

    /**
     * 如果存在有某个partition的AR有重复的副本，那么抛异常
     * */
    if (duplicateEntries.nonEmpty) {
      val duplicatesMsg = duplicateEntries
        .map { case (tp, duplicateReplicas) => "%s contains multiple entries for %s".format(tp, duplicateReplicas.mkString(",")) }
        .mkString(". ")
      throw new AdminCommandFailedException("Partition replica lists may not contain duplicate entries: %s".format(duplicatesMsg))
    }

    /**
     * 首先把涉及到的topic去重整理出来。
     *
     * 下面的注释实际是建议检查是否所有的tp都存在于该集群中，但似乎没看到有检查tp是否存在于该cluster的代码。不，下面有做这样的检查。
     * */
    // check that all partitions in the proposed assignment exist in the cluster
    val proposedTopics = partitionsToBeReassigned.map { case (tp, _) => tp.topic }.distinct

    /**
     * 获取本次reassign涉及到的topic当前的分配信息：mutable.Map[TopicAndPartition, Seq[Int]]
     * */
    val existingAssignment = zkUtils.getReplicaAssignmentForTopics(proposedTopics)
    /**
     * 然后检查下提交的分配计划设计的tp是否存在于当前集群。所以实际上，上面建议检查tp是否存在是做了的
     * */
    val nonExistentPartitions = partitionsToBeReassigned.map { case (tp, _) => tp }.filterNot(existingAssignment.contains)
    /**
     * 如果确实存在提交的分配计划中某个tp不在该机器，那么抛异常。
     * */
    if (nonExistentPartitions.nonEmpty)
      throw new AdminCommandFailedException("The proposed assignment contains non-existent partitions: " +
        nonExistentPartitions)

    /**
     * 这里注释的意思是，检查分配计划涉及的broker是否在该集群存在。
     * 首先是获取当前活着的brokerId集合。
     * */
    // check that all brokers in the proposed assignment exist in the cluster
    val existingBrokerIDs = zkUtils.getSortedBrokerList()

    /**
     * 这里过滤出不存在的brokerId。
     * 注意它是拿的活着的broker
     * */
    val nonExistingBrokerIDs = partitionsToBeReassigned.toMap.values.flatten.filterNot(existingBrokerIDs.contains).toSet
    /**
     * 如果分配的计划有某个brokerId实际是不存在的，那么抛异常。
     * */
    if (nonExistingBrokerIDs.nonEmpty)
      throw new AdminCommandFailedException("The proposed assignment contains non-existent brokerIDs: " + nonExistingBrokerIDs.mkString(","))

    /**
     * 如果上面的检查都没问题，那么久把分配计划返回出去
     * */
    partitionsToBeReassigned
  }

  private def checkIfReassignmentSucceeded(zkUtils: ZkUtils, partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]])
  :Map[TopicAndPartition, ReassignmentStatus] = {
    val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned().mapValues(_.newReplicas)
    partitionsToBeReassigned.keys.map { topicAndPartition =>
      (topicAndPartition, checkIfPartitionReassignmentSucceeded(zkUtils, topicAndPartition, partitionsToBeReassigned,
        partitionsBeingReassigned))
    }.toMap
  }

  def checkIfPartitionReassignmentSucceeded(zkUtils: ZkUtils, topicAndPartition: TopicAndPartition,
                                            partitionsToBeReassigned: Map[TopicAndPartition, Seq[Int]],
                                            partitionsBeingReassigned: Map[TopicAndPartition, Seq[Int]]): ReassignmentStatus = {
    val newReplicas = partitionsToBeReassigned(topicAndPartition)
    partitionsBeingReassigned.get(topicAndPartition) match {
      case Some(_) => ReassignmentInProgress
      case None =>
        // check if the current replica assignment matches the expected one after reassignment
        val assignedReplicas = zkUtils.getReplicasForPartition(topicAndPartition.topic, topicAndPartition.partition)
        if(assignedReplicas == newReplicas)
          ReassignmentCompleted
        else {
          println(("ERROR: Assigned replicas (%s) don't match the list of replicas for reassignment (%s)" +
            " for partition %s").format(assignedReplicas.mkString(","), newReplicas.mkString(","), topicAndPartition))
          ReassignmentFailed
        }
    }
  }

  def validateAndParseArgs(args: Array[String]): ReassignPartitionsCommandOptions = {
    val opts = new ReassignPartitionsCommandOptions(args)

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(opts.parser, "This command moves topic partitions between replicas.")

    // Should have exactly one action
    val actions = Seq(opts.generateOpt, opts.executeOpt, opts.verifyOpt).count(opts.options.has _)
    if(actions != 1)
      CommandLineUtils.printUsageAndDie(opts.parser, "Command must include exactly one action: --generate, --execute or --verify")

    CommandLineUtils.checkRequiredArgs(opts.parser, opts.options, opts.zkConnectOpt)

    //Validate arguments for each action
    if(opts.options.has(opts.verifyOpt)) {
      if(!opts.options.has(opts.reassignmentJsonFileOpt))
        CommandLineUtils.printUsageAndDie(opts.parser, "If --verify option is used, command must include --reassignment-json-file that was used during the --execute option")
      CommandLineUtils.checkInvalidArgs(opts.parser, opts.options, opts.verifyOpt, Set(opts.throttleOpt, opts.topicsToMoveJsonFileOpt, opts.disableRackAware, opts.brokerListOpt))
    }
    else if(opts.options.has(opts.generateOpt)) {
      if(!(opts.options.has(opts.topicsToMoveJsonFileOpt) && opts.options.has(opts.brokerListOpt)))
        CommandLineUtils.printUsageAndDie(opts.parser, "If --generate option is used, command must include both --topics-to-move-json-file and --broker-list options")
      CommandLineUtils.checkInvalidArgs(opts.parser, opts.options, opts.generateOpt, Set(opts.throttleOpt, opts.reassignmentJsonFileOpt))
    }
    else if (opts.options.has(opts.executeOpt)){
      if(!opts.options.has(opts.reassignmentJsonFileOpt))
        CommandLineUtils.printUsageAndDie(opts.parser, "If --execute option is used, command must include --reassignment-json-file that was output " + "during the --generate option")
      CommandLineUtils.checkInvalidArgs(opts.parser, opts.options, opts.executeOpt, Set(opts.topicsToMoveJsonFileOpt, opts.disableRackAware, opts.brokerListOpt))
    }
    opts
  }

  class ReassignPartitionsCommandOptions(args: Array[String]) {
    val parser = new OptionParser(false)

    val zkConnectOpt = parser.accepts("zookeeper", "REQUIRED: The connection string for the zookeeper connection in the " +
                      "form host:port. Multiple URLS can be given to allow fail-over.")
                      .withRequiredArg
                      .describedAs("urls")
                      .ofType(classOf[String])
    val generateOpt = parser.accepts("generate", "Generate a candidate partition reassignment configuration." +
      " Note that this only generates a candidate assignment, it does not execute it.")
    val executeOpt = parser.accepts("execute", "Kick off the reassignment as specified by the --reassignment-json-file option.")
    val verifyOpt = parser.accepts("verify", "Verify if the reassignment completed as specified by the --reassignment-json-file option. If there is a throttle engaged for the replicas specified, and the rebalance has completed, the throttle will be removed")
    val reassignmentJsonFileOpt = parser.accepts("reassignment-json-file", "The JSON file with the partition reassignment configuration" +
                      "The format to use is - \n" +
                      "{\"partitions\":\n\t[{\"topic\": \"foo\",\n\t  \"partition\": 1,\n\t  \"replicas\": [1,2,3] }],\n\"version\":1\n}")
                      .withRequiredArg
                      .describedAs("manual assignment json file path")
                      .ofType(classOf[String])
    val topicsToMoveJsonFileOpt = parser.accepts("topics-to-move-json-file", "Generate a reassignment configuration to move the partitions" +
                      " of the specified topics to the list of brokers specified by the --broker-list option. The format to use is - \n" +
                      "{\"topics\":\n\t[{\"topic\": \"foo\"},{\"topic\": \"foo1\"}],\n\"version\":1\n}")
                      .withRequiredArg
                      .describedAs("topics to reassign json file path")
                      .ofType(classOf[String])
    val brokerListOpt = parser.accepts("broker-list", "The list of brokers to which the partitions need to be reassigned" +
                      " in the form \"0,1,2\". This is required if --topics-to-move-json-file is used to generate reassignment configuration")
                      .withRequiredArg
                      .describedAs("brokerlist")
                      .ofType(classOf[String])
    val disableRackAware = parser.accepts("disable-rack-aware", "Disable rack aware replica assignment")
    val throttleOpt = parser.accepts("throttle", "The movement of partitions will be throttled to this value (bytes/sec). Rerunning with this option, whilst a rebalance is in progress, will alter the throttle value. The throttle rate should be at least 1 KB/s.")
                      .withRequiredArg()
                      .describedAs("throttle")
                      .defaultsTo("-1")
                      .ofType(classOf[Long])
    val options = parser.parse(args : _*)
  }
}

class ReassignPartitionsCommand(zkUtils: ZkUtils, proposedAssignment: Map[TopicAndPartition, Seq[Int]], admin: AdminUtilities = AdminUtils)
  extends Logging {

  import ReassignPartitionsCommand._

  def existingAssignment(): Map[TopicAndPartition, Seq[Int]] = {
    val proposedTopics = proposedAssignment.keySet.map(_.topic).toSeq
    zkUtils.getReplicaAssignmentForTopics(proposedTopics)
  }

  private def maybeThrottle(throttle: Throttle): Unit = {
    /**
     * 当throttle.value>=0才是真正给定了限速选项的。否则是-1
     * */
    if (throttle.value >= 0) {
      /**
       * 首先这里把限速的replica 提交上去，确认要限速哪些replica
       * */
      assignThrottledReplicas(existingAssignment(), proposedAssignment)
      /**
       * 然后这里是设置broker限速的quota，确认哪些broker要限速
       * */
      maybeLimit(throttle)
      /**
       * 然后这里调用限速的回调,默认回调是一个空方法
       * */
      throttle.postUpdateAction()
      println(s"The throttle limit was set to ${throttle.value} B/s")
    }
  }

  /**
   * 如果throttle的value不是-1，那么就是要限速。
   *
    * Limit the throttle on currently moving replicas. Note that this command can use used to alter the throttle, but
    * it may not alter all limits originally set, if some of the brokers have completed their rebalance.
    */
  def maybeLimit(throttle: Throttle) {
    if (throttle.value >= 0) {
      /**
       * 如果要限速，那么计算出当前存在的broker和提交的分配任务涉及到的broker求并集，然后去重。
       * 因为在此之前就检查过提交的分配任务设计的broker是存在的，如果不存在，那么在之前就抛异常了。
       * 这里求并集是为了防止当前有新增broker，可以充分利用
       * */
      val existingBrokers = existingAssignment().values.flatten.toSeq
      val proposedBrokers = proposedAssignment.values.flatten.toSeq
      val brokers = (existingBrokers ++ proposedBrokers).distinct

      /**
       * 这里遍历每个broker，然后更新它的config
       * */
      for (id <- brokers) {
        val configs = admin.fetchEntityConfig(zkUtils, ConfigType.Broker, id.toString)
        configs.put(DynamicConfig.Broker.LeaderReplicationThrottledRateProp, throttle.value.toString)
        configs.put(DynamicConfig.Broker.FollowerReplicationThrottledRateProp, throttle.value.toString)
        admin.changeBrokerConfig(zkUtils, Seq(id), configs)
      }
    }
  }

  /**
   * 这个方法是设置要限速的replica。然后它这个注释的意思是：该方法只是被用来作为reassign 初始化的时候
   * */
  /** Set throttles to replicas that are moving. Note: this method should only be used when the assignment is initiated. */
  private[admin] def assignThrottledReplicas(allExisting: Map[TopicAndPartition, Seq[Int]], allProposed: Map[TopicAndPartition, Seq[Int]], admin: AdminUtilities = AdminUtils): Unit = {
    /**
     * 本次reassign涉及到的topic.
     * allProposed.keySet.map(_.topic).toSeq 这个方法是提取本次reassign涉及到的topic
     * */
    for (topic <- allProposed.keySet.map(_.topic).toSeq) {
      /**
       * filterBy这个方法又是啥
       * */
      val (existing, proposed) = filterBy(topic, allExisting, allProposed)

      /**
       * preRebalanceReplicaForMovingPartitions这个方法是干啥的？
       *
       * 我知道了，这里的leader并不是当前某个tp的leader，而是当前某个tp的所有副本都是leader，因为我在数据迁移过程中可能有leader切换，这样拖的broker就有可能变成了该tp的所有副本所在broker之一，
       * 因此这里的leader字符串，包括了所有需要更改tp副本所在broker的tp对应的所有副本，format格式是：
       * tpId:replicaId,tpId2:replicaId2
       *
       * 其实这里有个问题，比如：当前tp0 AR(1,2,3) 我想变成AR(4,5,6) ，那么正常情况下，都是4 5 6 去拖1 2 3 ，因此这里配置的leader replica应该涉及到1，2，3三个replicaId，但是
       * 当4进队后，IS变成（1，2，3，4） ，而此时当前leader宕机了，leader切换为4了，那么5和6岂不是要去拖4，但4这个副本并未限速？那不被拖爆吗
       *
       * 或者这里涉及到leader的选举策略，其实当中间AR变成1，2，3，4，5，6时，如果4进队后，2和3在isr中，那么应该是选前面的2或者3，但是如果2和3也宕机了呢 ？这个就比较极端了
       * 那么此时就会出现我说那个情况。却是会有问题，可以提个patch TODO-chenlin fix reassing throttle
       * 应该是本次涉及到的副本都要加入leader限速配置
       * */
      //Apply the leader throttle to all replicas that exist before the re-balance.
      val leader = format(preRebalanceReplicaForMovingPartitions(existing, proposed))


      /**
       * 这个方法是把新增的副本过滤出来，并以Map[TopicAndPartition, Seq[Int]]这样的形式返回,eg:
       * tp1  AR(1,2,3) 变为AR(2,3,4,5)
       * 那么这里返回<tp1 ,(4,5) >
       * */
      //Apply a follower throttle to all "move destinations".
      val follower = format(postRebalanceReplicasThatMoved(existing, proposed))

      /**
       * 然后这里把对应的配置进行更新。这里是更新的replica限速配置，表明要限速哪些replica
       * */
      val configs = admin.fetchEntityConfig(zkUtils, ConfigType.Topic, topic)
      configs.put(LeaderReplicationThrottledReplicasProp, leader)
      configs.put(FollowerReplicationThrottledReplicasProp, follower)
      admin.changeTopicConfig(zkUtils, topic, configs)

      debug(s"Updated leader-throttled replicas for topic $topic with: $leader")
      debug(s"Updated follower-throttled replicas for topic $topic with: $follower")
    }
  }

  /**
   * 这个方法是把新增的副本过滤出来，并以Map[TopicAndPartition, Seq[Int]]这样的形式返回,eg:
   * tp1  AR(1,2,3) 变为AR(2,3,4,5)
   * 那么这里返回<tp1 ,(4,5) >
   * */
  private def postRebalanceReplicasThatMoved(existing: Map[TopicAndPartition, Seq[Int]], proposed: Map[TopicAndPartition, Seq[Int]]): Map[TopicAndPartition, Seq[Int]] = {
    //For each partition in the proposed list, filter out any replicas that exist now, and hence aren't being moved.
    proposed.map { case (tp, proposedReplicas) =>
      tp -> (proposedReplicas.toSet -- existing(tp)).toSeq
    }
  }

  /**
   * 这里似乎是把涉及副本搬迁的tp的当前分配图返回出来（可能存在reassign前后的AR副本一样的tp，或者可能只是换了顺序或者就是写错了，写成原来的AR了）：
   * */
  private def preRebalanceReplicaForMovingPartitions(existing: Map[TopicAndPartition, Seq[Int]], proposed: Map[TopicAndPartition, Seq[Int]]): Map[TopicAndPartition, Seq[Int]] = {
    def moving(before: Seq[Int], after: Seq[Int]) = (after.toSet -- before.toSet).nonEmpty
    //For any moving partition, throttle all the original (pre move) replicas (as any one might be a leader)
    existing.filter { case (tp, preMoveReplicas) =>
      proposed.contains(tp) && moving(preMoveReplicas, proposed(tp))
    }
  }

  /**
   * 这里的格式应该是：tpId:replicaId,tpId2:replicaId2,....
   * */
  def format(moves: Map[TopicAndPartition, Seq[Int]]): String =
    moves.flatMap { case (tp, moves) =>
      moves.map(replicaId => s"${tp.partition}:${replicaId}")
    }.mkString(",")

  /**
   * 这个filterBy主要是用来获取给定topic当前的副本分布情况和计划的分布情况，并以(tp,AR)的形式返回：
   * (Map[TopicAndPartition, Seq[Int]], Map[TopicAndPartition, Seq[Int]])
   * */
  def filterBy(topic: String, allExisting: Map[TopicAndPartition, Seq[Int]], allProposed: Map[TopicAndPartition, Seq[Int]]): (Map[TopicAndPartition, Seq[Int]], Map[TopicAndPartition, Seq[Int]]) = {
    (allExisting.filter { case (tp, _) => tp.topic == topic },
      allProposed.filter { case (tp, _) => tp.topic == topic })
  }

  def reassignPartitions(throttle: Throttle = NoThrottle): Boolean = {
    /**
     * 首先设置限速配置
     * */
    maybeThrottle(throttle)

    try {
      /**
       * 这里检查下tp是否存在
       * */
      val validPartitions = proposedAssignment.filter { case (p, _) => validatePartition(zkUtils, p.topic, p.partition) }

      /**
       * 然后这里提交reassign json串到zk上
       * */
      if (validPartitions.isEmpty) false
      else {
        val jsonReassignmentData = ZkUtils.formatAsReassignmentJson(validPartitions)
        zkUtils.createPersistentPath(ZkUtils.ReassignPartitionsPath, jsonReassignmentData)
        true
      }
    } catch {
      case _: ZkNodeExistsException =>
        val partitionsBeingReassigned = zkUtils.getPartitionsBeingReassigned()
        throw new AdminCommandFailedException("Partition reassignment currently in " +
        "progress for %s. Aborting operation".format(partitionsBeingReassigned))
      case e: Throwable => error("Admin command failed", e); false
    }
  }

  def validatePartition(zkUtils: ZkUtils, topic: String, partition: Int): Boolean = {
    // check if partition exists
    val partitionsOpt = zkUtils.getPartitionsForTopics(List(topic)).get(topic)
    partitionsOpt match {
      case Some(partitions) =>
        if(partitions.contains(partition)) {
          true
        } else {
          error("Skipping reassignment of partition [%s,%d] ".format(topic, partition) +
            "since it doesn't exist")
          false
        }
      case None => error("Skipping reassignment of partition " +
        "[%s,%d] since topic %s doesn't exist".format(topic, partition, topic))
        false
    }
  }
}


sealed trait ReassignmentStatus { def status: Int }
case object ReassignmentCompleted extends ReassignmentStatus { val status = 1 }
case object ReassignmentInProgress extends ReassignmentStatus { val status = 0 }
case object ReassignmentFailed extends ReassignmentStatus { val status = -1 }
