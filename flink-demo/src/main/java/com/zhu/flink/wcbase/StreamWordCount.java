package com.zhu.flink.wcbase;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


/**
 * Flink stream
 *
 * @Author ZhuHaiBo
 * @Create 2021/7/7 23:46
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        //baseWordCount();

        streamCurrentWordCount(args);
    }

    /**
     * @param args main启动参数
     * @throws Exception
     */
    private static void streamCurrentWordCount(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取启动参数 --host hadoop1 --port 7000 nc -lk 7000
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int post = Integer.parseInt(parameterTool.get("port"));

        DataStreamSource<String> dataStreamSource = environment.socketTextStream(host, post);

        dataStreamSource.flatMap(new MyFlatMap()).keyBy(0).sum(1).print();

        environment.execute();
    }

    private static void baseWordCount() throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度
        environment.setParallelism(2);

        String textFilePath = "flink-demo/src/main/resources/wordcount.txt";
        DataStreamSource<String> dataStreamSource = environment.readTextFile(textFilePath);
        dataStreamSource.flatMap(new MyFlatMap()).keyBy(0).sum(1).print();
        environment.execute();
    }


    private static class MyFlatMap implements FlatMapFunction<String, Tuple2<String, Integer>> {

        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
            String[] wordList = line.split(" ");
            for (String word : wordList) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
