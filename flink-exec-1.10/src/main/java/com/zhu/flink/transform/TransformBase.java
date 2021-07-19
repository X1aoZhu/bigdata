package com.zhu.flink.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * 基础算子
 *
 * @Author ZhuHaiBo
 * @Create 2021/7/11 15:45
 */
public class TransformBase {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<Integer> streamSource1 =
                environment.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));
        DataStreamSource<String> streamSource2 =
                environment.readTextFile("flink-demo/src/main/resources/SensorRead.txt");

        // 输出大于4的数，其他置为-1
        streamSource1.map((MapFunction<Integer, Integer>) value -> value > 4 ? value : -1)
                .print("streamSource1");
        // 输入对象数据转为string长度
        streamSource2.map((MapFunction<String, Integer>) String::length)
                .print("streamSource2");
        // 输入数据拆分
        streamSource2.map((MapFunction<String, Tuple3<String, String, String>>) line -> {
            String[] lineArr = line.split(",");
            return new Tuple3<>(lineArr[0], lineArr[1], lineArr[2]);
        }).returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING))
                .print("map");
        // flatMap
        streamSource2.flatMap((FlatMapFunction<String, String>) (value, out) -> {
            String[] lineArr = value.split(",");
            for (String str : lineArr) {
                out.collect(str);
            }
        }).returns(Types.STRING).print("flatMap");
        // filter
        streamSource2.filter((FilterFunction<String>) value -> value.startsWith("sensor_1")).print("filter");

        environment.execute();
    }
}
