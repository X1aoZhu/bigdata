package com.zhu.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: ZhuHaiBo
 * @date: 2021/7/14  17:36
 */
public class TimeWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //路灯编号，通过数量
        //9,3

        DataStream<Tuple2<Integer, Integer>> source = environment.socketTextStream("192.168.221.161", 7000)
                .map((MapFunction<String, Tuple2<Integer, Integer>>) value -> {
                    String[] lineArr = value.split(",");
                    return Tuple2.of(Integer.parseInt(lineArr[0]), Integer.parseInt(lineArr[1]));
                }).returns(Types.TUPLE(Types.INT, Types.INT));
        // 过去5s内，通过个数
        source.keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1)
                .print();

        environment.execute();
    }
}
