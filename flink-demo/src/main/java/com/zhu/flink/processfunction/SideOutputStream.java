package com.zhu.flink.processfunction;

import com.zhu.flink.entity.SensorRead;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * SideOutPutStream 测输出流
 *
 * @Author ZhuHaiBo
 * @Create 2021/7/13 23:23
 */
public class SideOutputStream {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<SensorRead> source = environment.readTextFile("flink-demo/src/main/resources/SensorRead.txt")
                .map((MapFunction<String, SensorRead>) line -> {
                    String[] lineArr = line.split(",");
                    return SensorRead.builder()
                            .id(lineArr[0])
                            .timestamp(Long.valueOf(lineArr[1]))
                            .temperature(Double.valueOf(lineArr[2]))
                            .build();
                });

        // 基于ProcessFunction实现测输出流功能，实现高低温分别输出功能
        // 测输出流，输出小于30度的数据
        OutputTag<Tuple3<String, String, Double>> outputTag = new OutputTag<Tuple3<String, String, Double>>("low-temperature") {
        };

        SingleOutputStreamOperator<Tuple3<String, String, Double>> streamOperator = source.process(new ProcessFunction<SensorRead, Tuple3<String, String, Double>>() {
            @Override
            public void processElement(SensorRead value, Context ctx, Collector<Tuple3<String, String, Double>> out) throws Exception {
                if (value.getTemperature() > 38) {
                    out.collect(Tuple3.of("high-temperature", value.getId(), value.getTemperature()));
                } else {
                    ctx.output(outputTag, Tuple3.of("low-temperature", value.getId(), value.getTemperature()));
                }
            }
        });

        streamOperator.print("HIGH");
        streamOperator.getSideOutput(outputTag).print("LOW");

        environment.execute();
    }
}
