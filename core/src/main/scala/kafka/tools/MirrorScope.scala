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

import scala.util.control.Breaks.{break, breakable}

object MirrorScope {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser(false)
    val srcTopicOpt = parser.accepts("srcTopic", "source topic")
      .withRequiredArg
      .describedAs("srcTopic")
      .ofType(classOf[String])
    val srcBoostServerOpt = parser.accepts("srcBoostServer", "srcBoostServer")
      .withRequiredArg
      .describedAs("srcBoostServer")
      .ofType(classOf[String])
    val groupIdOpt = parser.accepts("groupId", "groupId")
      .withRequiredArg
      .describedAs("groupId")
      .ofType(classOf[String])
    val startTimestampOpt = parser.accepts("startTimestamp", "startTimestamp")
      .withRequiredArg
      .describedAs("startTimestamp")
      .ofType(classOf[String])
    val endTimestampOpt = parser.accepts("endTimestamp", "endTimestamp")
      .withRequiredArg
      .describedAs("endTimestamp")
      .ofType(classOf[String])
    val dstTopicOpt = parser.accepts("dstTopic", "dstTopic")
      .withRequiredArg
      .describedAs("dstTopic")
      .ofType(classOf[String])
    val dstBoostServerOpt = parser.accepts("dstBoostServer", "dstBoostServer")
      .withRequiredArg
      .describedAs("dstBoostServer")
      .ofType(classOf[String])

    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Read data from standard input and publish it to Kafka.")

    val options = parser.parse(args: _*)
    val srcTopic = options.valueOf(srcTopicOpt)
    val startTime = options.valueOf(startTimestampOpt).toLong
    val endTime = options.valueOf(endTimestampOpt).toLong
    val dstTopic = options.valueOf(dstTopicOpt)

    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, options.valueOf(srcBoostServerOpt))
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, options.valueOf(groupIdOpt))
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.BytesDeserializer");
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.BytesDeserializer");
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", options.valueOf(dstBoostServerOpt))
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.BytesSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.BytesSerializer")
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)

    val timestampsToSearch = new util.HashMap[TopicPartition, java.lang.Long]()
    val tps = new util.ArrayList[TopicPartition]()
    consumer.partitionsFor(srcTopic).asScala.map { partition =>
      timestampsToSearch.put(new TopicPartition(partition.topic(), partition.partition()), startTime)
      tps.add(new TopicPartition(partition.topic(), partition.partition()))
    }

    consumer.assign(tps)
    consumer.offsetsForTimes(timestampsToSearch).asScala.map { case (tp, timestamp) =>
      consumer.seek(tp, timestamp.offset())
    }


    breakable {
      while (true) {
        for (record <- consumer.poll(100)) {
          if (record.timestamp() <= endTime && record.timestamp() >= startTime) {
            //            println(s"timestamp=${record.timestamp()} record=${record} ")
            producer.send(new ProducerRecord[Array[Byte], Array[Byte]](dstTopic, record.key(), record.value()))
          } else {
            tps.remove(new TopicPartition(record.topic(), record.partition()))
          }
        }
        if (tps.isEmpty) break()
      }
    }

    producer.close()
    consumer.close()
  }

}
