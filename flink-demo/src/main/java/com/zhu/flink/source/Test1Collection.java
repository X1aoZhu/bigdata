package com.zhu.flink.source;

import com.zhu.flink.entity.SensorRead;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.Arrays;
import java.util.List;

/**
 * 从集合读取数据
 *
 * @Author ZhuHaiBo
 * @Create 2021/7/11 10:18
 */
public class Test1Collection {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        List<SensorRead> source = Arrays.asList(
                SensorRead.builder().id(String.valueOf(1)).temperature(35.1).timestamp(20210711L).build(),
                SensorRead.builder().id(String.valueOf(2)).temperature(36.1).timestamp(20210712L).build(),
                SensorRead.builder().id(String.valueOf(3)).temperature(37.1).timestamp(20210713L).build()
        );

        environment.fromElements("Hello", "World", "Hello", "Flink")
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String str, Collector<Tuple2<String, Integer>> collector) throws Exception {
                        collector.collect(new Tuple2<>(str, 1));
                    }
                })
                .keyBy(0).sum(1).print("wordCount");

        environment.fromCollection(source).print("SensorRead");
        environment.execute();
    }
}
