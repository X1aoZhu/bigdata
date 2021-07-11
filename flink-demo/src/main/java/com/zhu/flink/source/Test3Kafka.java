package com.zhu.flink.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * 从kafka读取数据
 *
 * @Author ZhuHaiBo
 * @Create 2021/7/11 11:06
 */
public class Test3Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.enableCheckpointing(1000);
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "hadoop4:9092");
        /*properties.put("auto.offset.reset", "earliest");
        properties.put("group.id", "1");*/


        FlinkKafkaConsumer011<String> consumer = new FlinkKafkaConsumer011<>("flinkword", new SimpleStringSchema(), properties);
        consumer.setStartFromEarliest();
        consumer.setCommitOffsetsOnCheckpoints(true);
        environment.addSource(consumer).print();

        environment.execute();
    }
}
