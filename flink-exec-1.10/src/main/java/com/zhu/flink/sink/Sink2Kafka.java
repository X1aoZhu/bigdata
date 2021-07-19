package com.zhu.flink.sink;

import com.alibaba.fastjson.JSON;
import com.zhu.flink.entity.SensorRead;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

/**
 * @author: ZhuHaiBo
 * @date: 2021/7/13  14:17
 */
public class Sink2Kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStream<SensorRead> source = environment.readTextFile("flink-demo/src/main/resources/SensorRead.txt")
                .map((MapFunction<String, SensorRead>) line -> {
                    String[] split = line.split(",");
                    return SensorRead.builder().id(split[0]).timestamp(Long.valueOf(split[1])).temperature(Double.valueOf(split[2])).build();
                });

        //Properties properties = new Properties();
        //properties.put("bootstrap-server", "192.168.221.161:9092");
        //properties.setProperty("group.id", "consumer-group");

        source.map((MapFunction<SensorRead, String>) JSON::toJSONString)
                .addSink(new FlinkKafkaProducer011<>("192.168.221.161:9092", "sensor-result", new SimpleStringSchema()));

        environment.execute();
    }
}
