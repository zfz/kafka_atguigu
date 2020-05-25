package com.atguigu.kafka.ch03_consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

public class HighLevelConsumer {
    public static void main(String[] args) {
        // 创建配置对象
        Properties props = new Properties();
        // Kafka集群信息
        props.setProperty("bootstrap.servers", ":32788,:32789,:32787");
        // 制定consumer group
        props.setProperty("group.id", "high-level-consumer");
        // 是否自动确认offset
        props.put("enable.auto.commit", "true");
        // K,V序列化对象
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // 订阅主题, 可同时订阅多个
        consumer.subscribe(Arrays.asList("first"));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(500);
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(record);
            }
        }
    }
}
