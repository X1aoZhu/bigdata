package com.zhu.flink.window;

import com.zhu.flink.entity.SensorRead;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 计数窗口
 *
 * @author: ZhuHaiBo
 * @date: 2021/7/14  16:12
 */
public class CountWindowTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        KeyedStream<SensorRead, String> keyedStream = environment.socketTextStream("192.168.221.161", 7000)
                .map((MapFunction<String, SensorRead>) line -> {
                    String[] split = line.split(",");
                    return SensorRead.builder()
                            .id(split[0])
                            .timestamp(Long.valueOf(split[1]))
                            .temperature(Double.valueOf(split[2]))
                            .build();
                })
                .keyBy(SensorRead::getId);

        // Count Window
        keyedStream.countWindow(10, 2)
                .aggregate(new AggregateFunction<SensorRead, Tuple2<Double, Double>, Double>() {
                    @Override
                    public Tuple2<Double, Double> createAccumulator() {
                        return Tuple2.of(0.0, 0.0);
                    }

                    @Override
                    public Tuple2<Double, Double> add(SensorRead value, Tuple2<Double, Double> accumulator) {
                        return Tuple2.of(accumulator.f0 + 1, accumulator.f1 + value.getTemperature());
                    }

                    @Override
                    public Double getResult(Tuple2<Double, Double> accumulator) {
                        return accumulator.f1 / accumulator.f0;
                    }

                    @Override
                    public Tuple2<Double, Double> merge(Tuple2<Double, Double> a, Tuple2<Double, Double> b) {
                        return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
                    }
                }).print();


        environment.execute();
    }
}
