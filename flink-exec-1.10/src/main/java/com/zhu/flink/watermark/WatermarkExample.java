package com.zhu.flink.watermark;

import com.zhu.flink.entity.SensorRead;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author: ZhuHaiBo
 * @date: 2021/7/15  16:21
 */
public class WatermarkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        // 设置实践实践
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 读取数据
        SingleOutputStreamOperator<SensorRead> source = environment.socketTextStream("192.168.221.161", 7000)
                .map((MapFunction<String, SensorRead>) value -> {
                    String[] split = value.split(",");
                    return SensorRead.builder().id(split[0])
                            .timestamp(Long.parseLong(split[1]))
                            .temperature(Double.parseDouble(split[2])).build();
                });

        // 设置watermark，周期性watermark
        // 参照org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows.assignWindows
        // 随机性Watermark，开窗时间为15s，延迟为2s，首次输入'sensor_1,1626341600,35'，1626341600：为秒的时间戳，所以要*1000
        // 计算窗口为：1626341600000 - （1626341600000 - offset + 1500）% 1500 = 1626341595000，
        // 则第一个窗口为：[1626341595000,1626341610000)，左闭右开
        // 由于watermark设置了等待时间，则输入'sensor_1,1626341612,23'，
        // 第一个窗口才会返回结果，输出[1626341595000,1626341610000)最小值，即：[1626341595000，1626341609000] 时间内的最小值
        SingleOutputStreamOperator<SensorRead> watermarks = source
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorRead>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorRead element) {
                        return element.getTimestamp() * 1000;
                    }
                });

        // 基于事件时间的开窗聚合
        watermarks.keyBy(SensorRead::getId)
                .timeWindow(Time.seconds(15))
                .minBy("temperature").print();

        environment.execute();
    }
}
