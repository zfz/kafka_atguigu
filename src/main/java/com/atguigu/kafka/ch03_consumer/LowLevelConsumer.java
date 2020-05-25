package com.atguigu.kafka.ch03_consumer;

import kafka.api.FetchRequestBuilder;
import kafka.api.FetchRequest;

import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

/**
 * 读取指定topic，指定partition,指定offset的数据
 * 因为一个topic中存在多个partition，而且每一个partition中会有多个副本，所以想要获取指定的数据
 * 必须从指定分区的主副本中获取数据，那么就必须拿到主副本的元数据信息。
 * 1）发送主题元数据请求对象
 * 2）得到主题元数据响应对象，其中包含主题元数据信息
 * 3）通过主题元数据信息，获取主题的分区元数据信息
 * 4）获取指定的分区元数据信息
 * 5）获取分区的主副本信息
 *
 * 获取主副本信息后，消费者要连接对应的主副本机器，然后抓取数据
 * 1）构建抓取数据请求对象
 * 2）发送抓取数据请求
 * 3）获取抓取数据响应，其中包含了获取的数据
 * 4）由于获取的数据为字节码，还需要转换为字符串，用于业务的操作。
 * 5）将获取的多条数据封装为集合。
 */
public class LowLevelConsumer {
    @SuppressWarnings("all")
    public static void main(String[] args) throws UnsupportedEncodingException {

        // 创建简单消费者
        BrokerEndPoint leader = null;
        // Docker不要使用"0.0.0.0"
        String host = "";
        // Docker转发的某个端口号
        int port = 32787;

        // 获取分区Leader
        SimpleConsumer metaConsumer = new SimpleConsumer(host, port, 500,
                1024 * 4, "metadata");

        // 获取元数据信息
        TopicMetadataRequest metaReq = new TopicMetadataRequest(Arrays.asList("first"));
        TopicMetadataResponse metaRes = metaConsumer.send(metaReq);
        System.out.println(metaConsumer);

        leaderLable:
        for (TopicMetadata topicMetadatum : metaRes.topicsMetadata()) {
            if ("first".equals(topicMetadatum.topic())) {
                // 关心的主题元数据信息
                List<PartitionMetadata> partitionMetadata = topicMetadatum.partitionsMetadata();
                for (PartitionMetadata partitionMetadatum : partitionMetadata) {
                    // 关心的分区元数据信息
                    if (partitionMetadatum.partitionId() == 1) {
                        leader = partitionMetadatum.leader();
                        // 跳出两层循环
                        break leaderLable;
                    }
                }
            }
        }
        if (leader == null) {
            System.out.println("Wrong partition");
            return;
        }

        // 指定分区Leader
        SimpleConsumer consumer = new SimpleConsumer(leader.host(), leader.port(), 500,
                1024 * 4, "accessLeader");

        // 抓取数据
        FetchRequest req = new FetchRequestBuilder().addFetch("first", 1,
                0, 1024).build();
        FetchResponse res = consumer.fetch(req);
        ByteBufferMessageSet msgSet = res.messageSet("first", 1);
        for (MessageAndOffset messageAndOffset : msgSet) {
            ByteBuffer buffer = messageAndOffset.message().payload();
            byte[] bs = new byte[buffer.limit()];
            buffer.get(bs);
            String value = new String(bs, "UTF-8");
            System.out.println(value);
        }
    }
}
