package com.zhu.flink.source;

import com.zhu.flink.entity.SensorRead;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author ZhuHaiBo
 * @Create 2021/7/11 10:44
 */
public class Test2FileInput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从项目目录下读取
        environment.readTextFile("flink-demo/src/main/resources/wordcount.txt")
                .print("FileRead_Word");
        environment.readTextFile("flink-demo/src/main/resources/SensorRead.txt")
                .flatMap(new FlatMapFunction<String, SensorRead>() {
                    @Override
                    public void flatMap(String line, Collector<SensorRead> collector) throws Exception {
                        String[] split = line.split(",");
                        collector.collect(SensorRead.builder().id(split[0])
                                .timestamp(Long.parseLong(split[1]))
                                .temperature(Double.parseDouble(split[2])).build());
                    }
                })
                .print("SensorRead");
        // 从HDFS读取，配置的是nameNode的通信地址，参照core-site.xml
        environment.readTextFile("hdfs://192.168.240.152:8020/FlinkWord.txt")
                .print("HDFS");

        environment.execute();
    }
}
