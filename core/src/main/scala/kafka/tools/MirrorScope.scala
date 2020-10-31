package kafka.tools

import java.util
import java.util.{Collections, Properties}

import joptsimple.OptionParser
import kafka.server.{KafkaServer, KafkaServerStartable}
import kafka.utils.{CommandLineUtils, Exit, Logging}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.clients.consumer.ConsumerRecord
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition

object MirrorScope {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser(false)
    val scTopic = parser.accepts("scTopic", "The topic  to consume ")
      .withRequiredArg
      .describedAs("scTopic")
      .ofType(classOf[String])
    val srcBoostServer = parser.accepts("srcBoostServer", "srcBoostServer")
      .withRequiredArg
      .describedAs("srcBoostServer")
      .ofType(classOf[String])
    val groupId = parser.accepts("groupId", "groupId")
      .withRequiredArg
      .describedAs("groupId")
      .ofType(classOf[String])
    val startTimestamp = parser.accepts("startTimestamp", "startTimestamp")
      .withRequiredArg
      .describedAs("startTimestamp")
      .ofType(classOf[String])
    val endTimestamp = parser.accepts("endTimestamp", "endTimestamp")
      .withRequiredArg
      .describedAs("endTimestamp")
      .ofType(classOf[String])
    val dstTopicOpt = parser.accepts("dstTopic", "dstTopic")
      .withRequiredArg
      .describedAs("dstTopic")
      .ofType(classOf[String])
    val dstBoostServer = parser.accepts("dstBoostServer", "dstBoostServer")
      .withRequiredArg
      .describedAs("dstBoostServer")
      .ofType(classOf[String])
    val options = parser.parse(args: _*)
    val srcTopic = options.valueOf(scTopic)
    val startTime = options.valueOf(startTimestamp).toLong
    val endTime = options.valueOf(endTimestamp).toLong
    val dstTopic = options.valueOf(dstTopicOpt)

    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(srcBoostServer));
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, options.valueOf(groupId))
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", options.valueOf(dstBoostServer))
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)

    val timestampsToSearch = new util.HashMap[TopicPartition, java.lang.Long]()
    val tps = new util.ArrayList[TopicPartition]()
    consumer.partitionsFor(srcTopic).asScala.map { partition =>
      timestampsToSearch.put(new TopicPartition(partition.topic(), partition.partition()), startTime)
      tps.add(new TopicPartition(partition.topic(), partition.partition()))
    }

    val leos = consumer.endOffsets(tps)

    consumer.assign(tps)
    consumer.offsetsForTimes(timestampsToSearch).asScala.map { case (tp, timestamp) =>
      consumer.seek(tp, timestamp.offset())
    }

    var timestamp = 0l
    while (timestamp < endTime) {
      val records = consumer.poll(100)
      for (record <- records) {
        timestamp = record.timestamp()
        if (timestamp < endTime) {
          producer.send(new ProducerRecord[Array[Byte], Array[Byte]](dstTopic, record.key(), record.value()))
        }
      }
    }


    producer.close()
  }

}
