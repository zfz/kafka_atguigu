package com.atguigu.kafka.ch01_producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class HelloProducer {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
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
        String value = "Hello Kafka from Java";
        ProducerRecord record = new ProducerRecord(topic, value);

        // 异步生产（发送）数据
        producer.send(record);

        // 同步发送，发送成功之后继续发送下一个消息
        // future接口的get方法，会阻塞线程，等待返回
        // producer.send(record).get();

        // 关闭生产者
        producer.close();
    }
}
