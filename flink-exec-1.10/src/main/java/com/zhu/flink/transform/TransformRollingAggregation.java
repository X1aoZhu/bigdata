package com.zhu.flink.transform;


import com.zhu.flink.entity.SensorRead;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 滚动算子
 *
 * @Author ZhuHaiBo
 * @Create 2021/7/11 16:18
 */
public class TransformRollingAggregation {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        SingleOutputStreamOperator<SensorRead> source = environment.readTextFile("flink-demo/src/main/resources/SensorRead.txt")
                .map((MapFunction<String, SensorRead>) value -> {
                    String[] lineArr = value.split(",");
                    return SensorRead.builder().id(lineArr[0]).timestamp(Long.valueOf(lineArr[1])).temperature(Double.parseDouble(lineArr[2])).build();
                }).returns(SensorRead.class);

        //keyBy，max，maxBy，滚动聚合
        source.keyBy(SensorRead::getId).maxBy("temperature").print("maxBy");
        source.keyBy(SensorRead::getId).max("temperature").print("max");

        environment.execute();
    }
}
