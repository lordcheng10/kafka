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

import DynamicConfig.Broker._
import kafka.api.ApiVersion
import kafka.log.{LogConfig, LogManager}
import kafka.security.CredentialProvider
import kafka.server.Constants._
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.common.config.ConfigDef.Validator
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.metrics.Quota
import org.apache.kafka.common.metrics.Quota._

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * The ConfigHandler is used to process config change notifications received by the DynamicConfigManager
  */
trait ConfigHandler {
  def processConfigChanges(entityName: String, value: Properties)
}

/**
 * topic config包括限速配置
 *
  * The TopicConfigHandler will process topic config changes in ZK.
  * The callback provides the topic name and the full properties set read from ZK
  */
class TopicConfigHandler(private val logManager: LogManager, kafkaConfig: KafkaConfig, val quotas: QuotaManagers) extends ConfigHandler with Logging  {

  def processConfigChanges(topic: String, topicConfig: Properties) {
    // Validate the configurations.
    val configNamesToExclude = excludedConfigs(topic, topicConfig)

    val logs = logManager.logsByTopicPartition.filterKeys(_.topic == topic).values.toBuffer
    if (logs.nonEmpty) {
      /* combine the default properties with the overrides in zk to create the new LogConfig */
      val props = new Properties()
      props.putAll(logManager.defaultConfig.originals)
      topicConfig.asScala.foreach { case (key, value) =>
        if (!configNamesToExclude.contains(key)) props.put(key, value)
      }
      val logConfig = LogConfig(props)
      if ((topicConfig.containsKey(LogConfig.RetentionMsProp) 
        || topicConfig.containsKey(LogConfig.MessageTimestampDifferenceMaxMsProp))
        && logConfig.retentionMs < logConfig.messageTimestampDifferenceMaxMs)
        warn(s"${LogConfig.RetentionMsProp} for topic $topic is set to ${logConfig.retentionMs}. It is smaller than " + 
          s"${LogConfig.MessageTimestampDifferenceMaxMsProp}'s value ${logConfig.messageTimestampDifferenceMaxMs}. " +
          s"This may result in frequent log rolling.")
      logs.foreach(_.config = logConfig)
    }

    /***
     * 这里传入的prop只有两种类型：
     * val LeaderReplicationThrottledReplicasProp = "leader.replication.throttled.replicas"
     * val FollowerReplicationThrottledReplicasProp = "follower.replication.throttled.replicas"
     *
     * 这里的topicConfig是外部函数processConfigChanges方法传入的，
     * */
    def updateThrottledList(prop: String, quotaManager: ReplicationQuotaManager) = {
      if (topicConfig.containsKey(prop) && topicConfig.getProperty(prop).length > 0) {
        /**
         * 限速replica配置就是在这里解析成partitions集合，然后通过quotaManager.markThrottled接口放到throttledPartitions：ConcurrentHashMap[String, Seq[Int]]中，
         *
         * */
        val partitions = parseThrottledPartitions(topicConfig, kafkaConfig.brokerId, prop)
        /**
         * 这里把partitions放到ConcurrentHashMap[String, Seq[Int]]中
         * */
        quotaManager.markThrottled(topic, partitions)
        logger.debug(s"Setting $prop on broker ${kafkaConfig.brokerId} for topic: $topic and partitions $partitions")
      } else {
        quotaManager.removeThrottle(topic)
        logger.debug(s"Removing $prop from broker ${kafkaConfig.brokerId} for topic $topic")
      }
    }
    updateThrottledList(LogConfig.LeaderReplicationThrottledReplicasProp, quotas.leader)
    updateThrottledList(LogConfig.FollowerReplicationThrottledReplicasProp, quotas.follower)
  }

  /**
   * parseThrottledPartitions : 这个方法字面意思理解就是parse(解析) Throttled（限速的）  Partitions（partition集合）
   * */
  def parseThrottledPartitions(topicConfig: Properties, brokerId: Int, prop: String): Seq[Int] = {
    /**
     * 这里configValue拿到的就是类似这种字符串：tpId1:replicaId1,tpId2:replicaId2,....
     * */
    val configValue = topicConfig.get(prop).toString.trim
    ThrottledReplicaListValidator.ensureValidString(prop, configValue)
    configValue match {
      case "" => Seq() //如果configValue为空，那么久没有要限速的replica
      case "*" => AllReplicas //如果是*就是全部replica
      case _ => configValue.trim //否则就是直接该值，上面不是刚trim过吗 怎么这里又trim，多余了
        .split(",")
        .map(_.split(":"))//这里split后就变成：<Array(tpId1,replicaId1),Array(tpId2,replicaId2),....>
        .filter(_ (1).toInt == brokerId) //Filter this replica 这里的_ (1)就是获取数组的第二个元素，也就是replicaId，把等于当前本机brokerId的过滤出来
        .map(_ (0).toInt).toSeq //convert to list of partition ids 对于是本机上的元素，这里把tpId获取出来返回，也就是说这里返回的是要限速的partitionId
    }
  }
  
  def excludedConfigs(topic: String, topicConfig: Properties): Set[String] = {
    // Verify message format version
    Option(topicConfig.getProperty(LogConfig.MessageFormatVersionProp)).flatMap { versionString =>
      if (kafkaConfig.interBrokerProtocolVersion < ApiVersion(versionString)) {
        warn(s"Log configuration ${LogConfig.MessageFormatVersionProp} is ignored for `$topic` because `$versionString` " +
          s"is not compatible with Kafka inter-broker protocol version `${kafkaConfig.interBrokerProtocolVersionString}`")
        Some(LogConfig.MessageFormatVersionProp)
      } else
        None
    }.toSet
  }
}


/**
 * Handles <client-id>, <user> or <user, client-id> quota config updates in ZK.
 * This implementation reports the overrides to the respective ClientQuotaManager objects
 */
class QuotaConfigHandler(private val quotaManagers: QuotaManagers) {

  def updateQuotaConfig(sanitizedUser: Option[String], clientId: Option[String], config: Properties) {
    val producerQuota =
      if (config.containsKey(DynamicConfig.Client.ProducerByteRateOverrideProp))
        Some(new Quota(config.getProperty(DynamicConfig.Client.ProducerByteRateOverrideProp).toLong, true))
      else
        None
    quotaManagers.produce.updateQuota(sanitizedUser, clientId, producerQuota)
    val consumerQuota =
      if (config.containsKey(DynamicConfig.Client.ConsumerByteRateOverrideProp))
        Some(new Quota(config.getProperty(DynamicConfig.Client.ConsumerByteRateOverrideProp).toLong, true))
      else
        None
    quotaManagers.fetch.updateQuota(sanitizedUser, clientId, consumerQuota)
    val requestQuota =
      if (config.containsKey(DynamicConfig.Client.RequestPercentageOverrideProp))
        Some(new Quota(config.getProperty(DynamicConfig.Client.RequestPercentageOverrideProp).toDouble, true))
      else
        None
    quotaManagers.request.updateQuota(sanitizedUser, clientId, requestQuota)
  }
}

/**
 * The ClientIdConfigHandler will process clientId config changes in ZK.
 * The callback provides the clientId and the full properties set read from ZK.
 */
class ClientIdConfigHandler(private val quotaManagers: QuotaManagers) extends QuotaConfigHandler(quotaManagers) with ConfigHandler {

  def processConfigChanges(clientId: String, clientConfig: Properties) {
    updateQuotaConfig(None, Some(clientId), clientConfig)
  }
}

/**
 * The UserConfigHandler will process <user> and <user, client-id> quota changes in ZK.
 * The callback provides the node name containing sanitized user principal, client-id if this is
 * a <user, client-id> update and the full properties set read from ZK.
 */
class UserConfigHandler(private val quotaManagers: QuotaManagers, val credentialProvider: CredentialProvider) extends QuotaConfigHandler(quotaManagers) with ConfigHandler {

  def processConfigChanges(quotaEntityPath: String, config: Properties) {
    // Entity path is <user> or <user>/clients/<client>
    val entities = quotaEntityPath.split("/")
    if (entities.length != 1 && entities.length != 3)
      throw new IllegalArgumentException("Invalid quota entity path: " + quotaEntityPath)
    val sanitizedUser = entities(0)
    val clientId = if (entities.length == 3) Some(entities(2)) else None
    updateQuotaConfig(Some(sanitizedUser), clientId, config)
    if (!clientId.isDefined && sanitizedUser != ConfigEntityName.Default)
      credentialProvider.updateCredentials(QuotaId.desanitize(sanitizedUser), config)
  }
}

/**
  * The BrokerConfigHandler will process individual broker config changes in ZK.
  * The callback provides the brokerId and the full properties set read from ZK.
  * This implementation reports the overrides to the respective ReplicationQuotaManager objects
  */
class BrokerConfigHandler(private val brokerConfig: KafkaConfig, private val quotaManagers: QuotaManagers) extends ConfigHandler with Logging {

  def processConfigChanges(brokerId: String, properties: Properties) {
    def getOrDefault(prop: String): Long = {
      if (properties.containsKey(prop))
        properties.getProperty(prop).toLong
      else
        DefaultReplicationThrottledRate
    }

    /**
     * 这里更新leader和follower的数据传输速度。
     * */
    if (brokerConfig.brokerId == brokerId.trim.toInt) {
      quotaManagers.leader.updateQuota(upperBound(getOrDefault(LeaderReplicationThrottledRateProp)))
      quotaManagers.follower.updateQuota(upperBound(getOrDefault(FollowerReplicationThrottledRateProp)))
    }
  }
}

object ThrottledReplicaListValidator extends Validator {
  def ensureValidString(name: String, value: String): Unit =
    ensureValid(name, value.split(",").map(_.trim).toSeq)

  override def ensureValid(name: String, value: Any): Unit = {
    def check(proposed: Seq[Any]): Unit = {
      if (!(proposed.forall(_.toString.trim.matches("([0-9]+:[0-9]+)?"))
        || proposed.headOption.exists(_.toString.trim.equals("*"))))
        throw new ConfigException(name, value,
          s"$name must be the literal '*' or a list of replicas in the following format: [partitionId],[brokerId]:[partitionId],[brokerId]:...")
    }
    value match {
      case scalaSeq: Seq[_] => check(scalaSeq)
      case javaList: java.util.List[_] => check(javaList.asScala)
      case _ => throw new ConfigException(name, value, s"$name must be a List but was ${value.getClass.getName}")
    }
  }

  override def toString: String = "[partitionId],[brokerId]:[partitionId],[brokerId]:..."

}
