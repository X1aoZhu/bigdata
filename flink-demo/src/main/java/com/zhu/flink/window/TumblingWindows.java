package com.zhu.flink.window;

import com.zhu.flink.entity.SensorRead;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;


/**
 * 滑动窗口
 *
 * @author: ZhuHaiBo
 * @date: 2021/7/13  16:54
 */
public class TumblingWindows {
    public static void main(String[] args) throws Throwable {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<SensorRead> source = environment.socketTextStream("192.168.221.161", 7000)
                .map((MapFunction<String, SensorRead>) line -> {
                    String[] split = line.split(",");
                    return SensorRead.builder()
                            .id(split[0])
                            .timestamp(Long.valueOf(split[1]))
                            .temperature(Double.valueOf(split[2]))
                            .build();
                });

        source.keyBy(SensorRead::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .aggregate(new AggregateFunction<SensorRead, Integer, Integer>() {

                    @Override
                    public Integer createAccumulator() {
                        return 0;
                    }

                    @Override
                    public Integer add(SensorRead value, Integer accumulator) {
                        return accumulator + 1;
                    }

                    @Override
                    public Integer getResult(Integer accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return a + b;
                    }
                })
                .print("TumblingProcessingTimeWindows");

        environment.execute();
    }
}
