package com.atguigu.kafka.ch01_producer;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class CallbackProducer {
    public static void main(String[] args) {
        // 创建配置对象
        Properties props = new Properties();
        // Kafka集群信息
        props.setProperty("bootstrap.servers", ":32788,:32789,:32787");
        // K,V序列化对象
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // 应答机制
        props.setProperty("acks", "1");

        // 创建生产者
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        // 准备数据
        String topic = "first";
        String value = "Hello Kafka from Java Callback";
        ProducerRecord record = new ProducerRecord(topic, value);

        // 异步生产（发送）数据，增加回调方法
        producer.send(record, new Callback() {
            // 回调函数
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                // 发送数据的分区
                System.out.println("metadata partitions: " + recordMetadata.partition());
                // 发送数据的偏移量
                System.out.println("metadata offset: " + recordMetadata.offset());
            }
        });

        // 关闭生产者
        producer.close();
    }
}
