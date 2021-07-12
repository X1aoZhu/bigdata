package com.zhu.flink.transform;

import com.zhu.flink.entity.SensorRead;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

import java.util.Collections;

/**
 * 流处理，分流操作
 *
 * @author: ZhuHaiBo
 * @date: 2021/7/12  18:01
 */
public class TransformTestMultipleStreams {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<SensorRead> source = environment.readTextFile("flink-demo/src/main/resources/SensorRead.txt")
                .map((MapFunction<String, SensorRead>) value -> {
                    String[] lineArr = value.split(",");
                    return SensorRead.builder().id(lineArr[0]).timestamp(Long.valueOf(lineArr[1])).temperature(Double.parseDouble(lineArr[2])).build();
                }).returns(SensorRead.class);

        // split,union,connect

        // split 分流，按照温度进行分流
        SplitStream<SensorRead> splitStream = source.split((OutputSelector<SensorRead>) value -> (value.getTemperature() > 37 ? Collections.singletonList("High") : Collections.singletonList("Low")));
        DataStream<SensorRead> highStream = splitStream.select("High");
        DataStream<SensorRead> lowStream = splitStream.select("Low");

        //highStream.print();
        //lowStream.print();

        ConnectedStreams<Tuple2<String, Double>, SensorRead> connectedStreams = highStream.map((MapFunction<SensorRead, Tuple2<String, Double>>) value -> Tuple2.of(value.getId(), value.getTemperature()))
                .returns(Types.TUPLE(Types.STRING, Types.DOUBLE)).connect(lowStream);

        connectedStreams.map(new CoMapFunction<Tuple2<String, Double>, SensorRead, Object>() {
            @Override
            public Object map1(Tuple2<String, Double> value) throws Exception {
                return Tuple3.of(value.f0, value.f1, "high temp warning");
            }

            @Override
            public Tuple2<String, String> map2(SensorRead value) throws Exception {
                return Tuple2.of(value.getId(), "Low");
            }
        }).print();

        environment.execute();
    }
}
