package org.apache.kafka;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListConsumerGroupsResult;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class KafkaConsumeTest implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private ConsumerRecords<String, String> msgList;
    private final String topic;
    private static final String GROUPID = "netflare-test-cn-ningbo-sdv2";
        private KafkaAdminClient adminClient;
    private String consumeName;

    public KafkaConsumeTest(String topicName, String consumeName) {
        // consumeName
        this.consumeName = consumeName;
        // projectid
//        String userName = "483c1905-1032-494e-8371-d1cc89ecc23a";
        String userName = "4ccb4cb9-da21-4f8d-8ebd-9ee5e0409d26";
        // 火山引擎账号的密钥，或具备对应权限的子账号密钥。不支持STS临时安全令牌。
        String passWord = "AKLTMzFiZjRjYjA4N2NhNDEyYWJlMWE0NmY1YTFhMWViODc#T1ROak0yUm1aREJrTmpreE5HUXlPVGczWTJZM1pEazNNbU5tTnpCak56UQ==";
        Properties props = new Properties();
        props.put("bootstrap.servers", "tls-cn-guilin-boe-inner.ivolces.com:9093"); //消费的地址，具体见文档
        props.put("group.id", GROUPID);
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "100");
        props.put("session.timeout.ms", "30000");
//        props.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, "org.apache.kafka.clients.consumer.StickyAssignor");

        props.put("auto.offset.reset", "earliest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");
        props.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
        props.put(SaslConfigs.SASL_JAAS_CONFIG,
                "org.apache.kafka.common.security.plain.PlainLoginModule " +
                        "required username=\"" + userName + "\" password=\"" + passWord + "\";");

        this.consumer = new KafkaConsumer<String, String>(props);
        this.topic = topicName;
        this.consumer.subscribe(Arrays.asList(topic));
        this.adminClient = (KafkaAdminClient) KafkaAdminClient.create(props);
    }

    public void run() {
        int messageNo = 1;
        System.out.println("---------开始消费---------");
        try {
            while(true) {
//                final Map<TopicPartition, OffsetAndMetadata> committableOffsets = new HashMap<>();
//                committableOffsets.put(new TopicPartition("d537c776-2b26-4167-b0e9-ed70308d9cd5",0), new OffsetAndMetadata(10));
//                committableOffsets.put(new TopicPartition("d537c776-2b26-4167-b0e9-ed70308d9cd5",1), new OffsetAndMetadata(100));
//
//                consumer.commitAsync(committableOffsets,null);
//                Thread.sleep(100);

                msgList = consumer.poll(Duration.ofSeconds(Integer.MAX_VALUE/2));
                if(null!=msgList&&msgList.count()>0){
                    for (ConsumerRecord<String, String> record : msgList) {
                        System.out.println(this.consumeName+"==="+messageNo+" offset==="+record.offset()+"=======receive: key = " + record.key() + ", value = " +
                                record.value());
                        messageNo++;
                    }
                } else{
                    Thread.sleep(10);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            consumer.close();
        }
    }

    public static void main(String args[]) throws InterruptedException, ExecutionException, TimeoutException {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        //${out-TopicID}为Kafka协议消费主题ID，格式为out+日志主题ID，例如"out-0fdaa6b6-3c9f-424c-8664-fc0d222c****"。
//        KafkaConsumeTest test1 = new KafkaConsumeTest("62009f93-c376-4f12-b66d-6cb5d43754d4","Consume1");
        KafkaConsumeTest test1 = new KafkaConsumeTest("out-f8b0f4c4-4a46-4a85-b861-4bdc11a8fa1f","Consume9993763776443");
        Thread thread1 = new Thread(test1);
        thread1.start();
//        ListConsumerGroupsResult result = test1.adminClient.listConsumerGroups();


//        System.out.println("list groups="+test1.adminClient.listConsumerGroups().all().get());
//        System.out.println("describe groups="+test1.adminClient.describeConsumerGroups(Collections.singletonList(GROUPID)).all().get());
//        System.out.println("listConsumerGroupOffsets groups="+test1.adminClient.listConsumerGroupOffsets(GROUPID).partitionsToOffsetAndMetadata().get());


        //KafkaConsumeTest test2 = new KafkaConsumeTest("out-eb79e781-e0df-4344-a49b-c8bf3a120206","Consume4");
//        Thread thread2 = new Thread(test2);
//        thread2.start();
        countDownLatch.await();
    }
}