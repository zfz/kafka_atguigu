package com.atguigu.kafka.ch04_interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

public class CounterInterceptor implements ProducerInterceptor<String, String> {
    private int errorCounter = 0;
    private int successCounter = 0;

    public ProducerRecord onSend(ProducerRecord record) {
        // 不要返回默认的null，返回原始数据
        return record;
    }

    public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {
        // 统计成功失败次数
        if (e == null) {
            successCounter += 1;
        } else {
            errorCounter += 1;
        }
    }

    public void close() {
        System.out.println("successful count: " + successCounter);
        System.out.println("failed count: " + errorCounter);
    }

    public void configure(Map<String, ?> map) {

    }
}
