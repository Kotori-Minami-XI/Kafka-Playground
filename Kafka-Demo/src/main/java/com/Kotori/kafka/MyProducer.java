package com.Kotori.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.Test;

public class MyProducer {
    @Test
    public void testSyncSend() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.111.10:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer(props);

        for (int i = 0; i < 5; i++) {
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<String, String>("Lovelive", 0, Integer.toString(i), Integer.toString(i));
            sendMsgSync(producerRecord, producer);
        }
        producer.close();
    }

    private void sendMsgSync(ProducerRecord<String, String> producerRecord, Producer<String, String> producer) throws ExecutionException, InterruptedException {
        //同步方式发送消息
        Future<RecordMetadata> result = producer.send(producerRecord);
        //等待消息发送成功的同步阻塞方法
        RecordMetadata metadata = result.get();
        System.out.println("同步方式发送消息结果：" + "topic-" + metadata.topic() + "|partition-"
                + metadata.partition() + "|offset-" + metadata.offset());
    }

    @Test
    public void testAsyncSend() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.111.10:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer(props);
        try {
            for (int i = 0; i < 5; i++) {
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<String, String>("Lovelive", 0, Integer.toString(i), Integer.toString(i));
                sendMsgAsync(producerRecord, producer);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private void sendMsgAsync(ProducerRecord<String, String> producerRecord, Producer<String, String> producer) {
        //异步方式发送消息
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    System.err.println("发送消息失败：" + exception.getStackTrace());

                }
                if (metadata != null) {
                    System.out.println("异步方式发送消息结果：" + "topic-" + metadata.topic() + "|partition-"
                            + metadata.partition() + "|offset-" + metadata.offset());
                }
            }
        });
    }
}
