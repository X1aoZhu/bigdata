package com.zhu.flink.window;

import com.zhu.flink.entity.SensorRead;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Int;

import java.util.Arrays;
import java.util.Collections;


/**
 * 时间窗口函数
 *
 * @author: ZhuHaiBo
 * @date: 2021/7/14  11:28
 */
public class TimeWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        WindowedStream<SensorRead, String, TimeWindow> window = environment
                .socketTextStream("192.168.221.161", 7000)
                .map((MapFunction<String, SensorRead>) line -> {
                    String[] split = line.split(",");
                    return SensorRead.builder()
                            .id(split[0])
                            .timestamp(Long.valueOf(split[1]))
                            .temperature(Double.valueOf(split[2]))
                            .build();
                })
                .keyBy(SensorRead::getId)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(1));

        // aggregate：增量聚合函数
        DataStream<Integer> result1 = window.aggregate(new AggregateFunction<SensorRead, Integer, Integer>() {

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
        });

        // 全窗口函数
        window.apply(new WindowFunction<SensorRead, Tuple3<String, Long, Integer>, String, TimeWindow>() {
            @Override
            public void apply(String key, TimeWindow window, Iterable<SensorRead> input, Collector<Tuple3<String, Long, Integer>> out) throws Exception {
                int size = IteratorUtils.toList(input.iterator()).size();
                out.collect(Tuple3.of(key, window.getEnd(), size));
            }
        }).print();


        //window.apply((WindowFunction<SensorRead, Tuple3<String, Long, Integer>, String, TimeWindow>) (key, window1, input, out) -> {
        //    int size = IteratorUtils.toList(input.iterator()).size();
        //    out.collect(Tuple3.of(key, window1.getEnd(), size));
        //
        //}).returns(Types.TUPLE(Types.STRING, Types.LONG, Types.INT)).print("All Window Stream");

        //Types.TUPLE(Types.STRING, Types.LONG, Types.INT)
        //((TypeInformation) TupleTypeInfo.getBasicTupleTypeInfo(String.class, Integer.class, String.class))

        //result1.print("Aggregate Stream");


        environment.execute();
    }
}
