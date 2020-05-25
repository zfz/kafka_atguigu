package com.atguigu.kafka.ch04_interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

public class InterceptorProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", ":32788,:32789,:32787");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("acks", "1");
        // 构建拦截链
        List<String> interceptors = new ArrayList<String>();
        interceptors.add("com.atguigu.kafka.ch04_interceptor.TimeInterceptor");
        interceptors.add("com.atguigu.kafka.ch04_interceptor.CounterInterceptor");
        // 不能用setProperty，setProperty第二个参数是字符串
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        Producer<String, String> producer = new KafkaProducer(props);
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord("first", "message" + i));
        }

        // 一定要关闭producer，这样才会调用interceptor的close方法
        producer.close();

    }
}
