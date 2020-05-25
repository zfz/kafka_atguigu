package com.atguigu.kafka.ch05_streams;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.io.UnsupportedEncodingException;
import java.util.Properties;

/**
 * 需要2个topic，一个是输入，一个是输出
 */
public class MyStream {
    public static void main(String[] args) {
        // 配置对象
        Properties prop = new Properties();
        prop.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ":32771,:32772,:32773");
        // 定义本次Stream应用的id
        prop.put(StreamsConfig.APPLICATION_ID_CONFIG, "log-filter");

        // 创建拓扑对象
        TopologyBuilder builder = new TopologyBuilder();
        // Source
        builder.addSource("S1", "first");
        // Process
        builder.addProcessor("P1", new ProcessorSupplier<byte[],byte[]>() {
            public Processor<byte[], byte[]> get() {
                return new MyProcessor();
            }
        },"S1");
        // Sink
        builder.addSink("T1", "second", "P1");

        // 创建流对象
        KafkaStreams kafkaStreams = new KafkaStreams(builder, prop);

        // 启动流对象
        kafkaStreams.start();
    }
}

class MyProcessor implements Processor<byte [], byte[]> {
    private ProcessorContext context;

    public void init(ProcessorContext processorContext) {
        this.context = processorContext;
    }

    /**
     * 数据转换
     */
    public void process(byte[] key, byte[] value) {
        // 获取字符串
        try {
            String val = new String(value, "utf-8");
            // val.replace doesn't change val
            // val.replaceAll("#", "");
            String newVal = val.replaceAll("#", "");
            // 输出到另一个Topic
            context.forward(key, newVal.getBytes("utf-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }

    public void punctuate(long l) {

    }

    public void close() {

    }
}