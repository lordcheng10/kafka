package kafka.tools

import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util
import java.util.Properties

import joptsimple.OptionParser
import kafka.message.{DefaultCompressionCodec, NoCompressionCodec}
import kafka.utils.{CommandLineUtils, ToolsUtils}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.KafkaThread

import scala.collection.JavaConverters._
import scala.util.control.Breaks.{break, breakable}


object DataVerificationTool {
  val timeUtil = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  def main(args: Array[String]): Unit = {
    val checkConfig = new CheckConfig(args)
    val consumerProps = getConsumerProps(checkConfig)
    val producerProps = getProducerProps(checkConfig)

    for (_ <- 0 to checkConfig.checkNum - 1) {
      check(producerProps, consumerProps, checkConfig)
    }
  }

  def check(producerProps: Properties,
            consumerProps: Properties,
            checkConfig: CheckConfig): Unit = {
    val dataMap = new util.HashMap[String, Integer]()
    val reportResult = ReportResult(0, 0, 0)
    val consumedOffsets = new util.HashMap[TopicPartition, Long]()

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)
    val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerProps)
    val tps = consumer.partitionsFor(checkConfig.dstTopic).asScala.map(partition => new TopicPartition(partition.topic(), partition.partition())).toList.asJavaCollection
    consumer.assign(tps)
    consumer.endOffsets(tps).asScala.map { case (tp, offset) =>
      consumer.seek(tp, offset)
    }


    val consumerThread = new KafkaThread("consumerThread", new Runnable {
      override def run(): Unit = {
        breakable {
          while (true) {
            val records = consumer.poll(checkConfig.timeoutMs)
            if (records == null || records.isEmpty) break()
            val itr = records.iterator()
            while (itr.hasNext) {
              val record = itr.next()
              val data = new String(record.value(), "UTF-8")
              if (dataMap.containsKey(data)) {
                dataMap.put(data, dataMap.get(data) - 1)
              } else {
                reportResult.dirtyNum = reportResult.dirtyNum + 1
              }

              consumedOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset())
            }
          }
        }
      }
    }, false)

    val producerThread = new KafkaThread("producerThread", new Runnable {
      override def run(): Unit = {
        for (i <- 0 to checkConfig.messageNum - 1) {
          val data = String.valueOf(System.nanoTime())
          producer.send(new ProducerRecord(checkConfig.topic, data.getBytes(StandardCharsets.UTF_8)), new CountCallback(data, dataMap))
          Thread.sleep(1)
        }

        producer.flush()
      }
    }, false)
    producerThread.start()
    producerThread.join()
    producer.flush()
    Thread.sleep(1000 * 60 * 2)

    consumerThread.start()
    consumerThread.join()

    dataMap.asScala.map { case (_, count) =>
      if (count > 0) reportResult.throwNum = reportResult.throwNum + 1
      else if (count < 0) reportResult.repeatNum = reportResult.repeatNum + 1
    }
    println(reportResult)

    if (reportResult.throwNum > 0) {
      consumer.endOffsets(tps).asScala.map { case (tp, leo) =>
        val lag = leo - consumedOffsets.get(tp)
        if (lag > 0) println(s"LagPartition-${tp} ${lag}")
      }
    }
    producer.close()
    consumer.close()
  }

  def getProducerProps(config: CheckConfig): Properties = {
    val props = new Properties
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.brokerList)
    props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.compressionCodec)
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "DataVerificationToolProducer")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    props
  }

  def getConsumerProps(config: CheckConfig): Properties = {
    val props = new Properties
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.dstBrokerList)

    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArrayDeserializer")
    props
  }

  case class ReportResult(var throwNum: Long, var repeatNum: Long, var dirtyNum: Long) {
    override def toString: String = s"${timeUtil.format(System.currentTimeMillis())}: throwNum=${throwNum} repeatNum=${repeatNum} dirtyNum=${dirtyNum}"
  }

}

class CheckConfig(args: Array[String]) {
  val parser = new OptionParser(false)
  val checkNumOpt = parser.accepts("checkNum", "check num in DataVerificationTool.")
    .withRequiredArg
    .describedAs("checkNum")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(1)

  val messageNumOpt = parser.accepts("messageNum", "send to topic message number.")
    .withRequiredArg
    .describedAs("messageNum")
    .ofType(classOf[java.lang.Integer])
    .defaultsTo(100000)

  val topicOpt = parser.accepts("topic", "REQUIRED: The topic id to produce messages to.")
    .withRequiredArg
    .describedAs("topic")
    .ofType(classOf[String])
  val dstTopicOpt = parser.accepts("dstTopic", "REQUIRED: The consumed topic.")
    .withRequiredArg
    .describedAs("dstTopic")
    .ofType(classOf[String])
  val brokerListOpt = parser.accepts("broker-list", "REQUIRED: The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
    .withRequiredArg
    .describedAs("broker-list")
    .ofType(classOf[String])
  val dstBrokerListOpt = parser.accepts("dst-broker-list", "REQUIRED: The broker list string in the form HOST1:PORT1,HOST2:PORT2.")
    .withRequiredArg
    .describedAs("dst-broker-list")
    .ofType(classOf[String])
  val compressionCodecOpt = parser.accepts("compression-codec", "The compression codec: either 'none', 'gzip', 'snappy', or 'lz4'." +
    "If specified without value, then it defaults to 'gzip'")
    .withOptionalArg()
    .describedAs("compression-codec")
    .ofType(classOf[String])

  val timeoutMsOpt = parser.accepts("timeout-ms", "If specified, exit if no message is available for consumption for the specified interval.")
    .withRequiredArg
    .describedAs("timeout_ms")
    .ofType(classOf[java.lang.Integer])


  val options = parser.parse(args: _*)
  if (args.length == 0)
    CommandLineUtils.printUsageAndDie(parser, "Read data from standard input and publish it to Kafka.")
  CommandLineUtils.checkRequiredArgs(parser, options, messageNumOpt, checkNumOpt, topicOpt, brokerListOpt, timeoutMsOpt, dstTopicOpt, dstBrokerListOpt)

  val messageNum = options.valueOf(messageNumOpt)
  val checkNum = options.valueOf(checkNumOpt)
  val topic = options.valueOf(topicOpt)
  val dstTopic = options.valueOf(dstTopicOpt)
  val brokerList = options.valueOf(brokerListOpt)
  val dstBrokerList = options.valueOf(dstBrokerListOpt)
  val compressionCodecOptionValue = options.valueOf(compressionCodecOpt)
  val compressionCodec = if (options.has(compressionCodecOpt))
    if (compressionCodecOptionValue == null || compressionCodecOptionValue.isEmpty)
      DefaultCompressionCodec.name
    else compressionCodecOptionValue
  else NoCompressionCodec.name
  val timeoutMs = if (options.has(timeoutMsOpt)) options.valueOf(timeoutMsOpt).intValue else -1
}


class CountCallback(data: String, dataMap: util.HashMap[String, Integer]) extends Callback {
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    Option(exception) match {
      case None =>
        dataMap.put(data, 1)
      case Some(e) =>
        println("producer send failed", e)
    }
  }
}

