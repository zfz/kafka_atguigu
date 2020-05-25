package com.atguigu.kafka.ch02_partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 分区算法类
 */
public class MyPartitioner implements Partitioner {

    public int partition(String s, Object o, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        // hash算法(& or %)
        return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> map) {

    }
}
