package com.zhu.flink.sink;

import com.alibaba.fastjson.JSON;
import com.zhu.flink.entity.SensorRead;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.Kafka011TableSink;

import java.util.Properties;

/**
 * @author: ZhuHaiBo
 * @date: 2021/7/13  14:13
 */
public class Sink2Redis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.execute();
    }
}
