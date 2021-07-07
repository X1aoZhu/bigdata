package com.zhu.flink.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author ZhuHaiBo
 * @Create 2021/7/7 23:45
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        String textFilePath = "D:\\IdeaWorkSpace\\hadoop-demo\\flink-demo\\src\\main\\resources\\wordcount.txt";
        DataSource<String> dataSource = executionEnvironment.readTextFile(textFilePath);

        AggregateOperator<Tuple2<String, Integer>> operator = dataSource.flatMap(new MyFlatMapFunction()).groupBy(0).sum(1);

        operator.print();
    }

    private static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] wordList = line.split(" ");
            for (String word : wordList) {
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
