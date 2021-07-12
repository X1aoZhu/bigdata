package com.zhu.flink.transform;


import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Int;

import java.util.Arrays;
import java.util.List;

/**
 * @author: ZhuHaiBo
 * @date: 2021/7/12  17:31
 */
public class TransformReduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);


        List<Tuple2<String, Integer>> source = Arrays.asList(Tuple2.of("Math", 60), Tuple2.of("Math", 80), Tuple2.of("Math", 70),
                Tuple2.of("English", 70), Tuple2.of("English", 80), Tuple2.of("English", 90));
        DataStreamSource<Tuple2<String, Integer>> streamSource = environment.fromCollection(source);


        // reduce:把2个类型相同的值合并成1个，对组内的所有值连续使用reduce，直到留下最后一个值
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = streamSource
                .keyBy(0)
                .reduce((ReduceFunction<Tuple2<String, Integer>>) (value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1));
        streamOperator.print();


        environment.execute();
    }
}
