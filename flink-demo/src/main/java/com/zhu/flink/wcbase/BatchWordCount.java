package com.zhu.flink.wcbase;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * Flink 批处理
 *
 * @Author ZhuHaiBo
 * @Create 2021/7/7 23:45
 */
public class BatchWordCount {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        // 读文件
        String textFilePath = "flink-demo/src/main/resources/wordcount.txt";
        DataSource<String> dataSource = executionEnvironment.readTextFile(textFilePath);
        // 读取数据
        //DataSource<String> dataSource = executionEnvironment.fromElements("hello zhu hello world hello CN");

        AggregateOperator<Tuple2<String, Integer>> operator = dataSource.flatMap(new MyFlatMapFunction()).groupBy(0).sum(1);

        operator.print();
    }

    private static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] wordList = line.split(" ");
            for (String word : wordList) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
