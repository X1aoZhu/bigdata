package com.zhu.flink.transform;

import com.zhu.flink.entity.SensorRead;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 富函数
 *
 * @author: ZhuHaiBo
 * @date: 2021/7/12  18:41
 */
public class TransformRichFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        DataStream<SensorRead> source = environment.readTextFile("flink-demo/src/main/resources/SensorRead.txt")
                .map((MapFunction<String, SensorRead>) line -> {
                    String[] split = line.split(",");
                    return SensorRead.builder().id(split[0]).timestamp(Long.valueOf(split[1])).temperature(Double.valueOf(split[2])).build();
                });

        //自定义富函数
        source.map(new MyRichFunction()).print("MyRichFunction");

        environment.execute();
    }

    /**
     * 自定义富含鼠
     */
    static class MyRichFunction extends RichMapFunction<SensorRead, Tuple2<String, Long>> {

        @Override
        public void open(Configuration parameters) {
            // 该方法调用次数取决于并行度
            // 初始化工作，一般是定义状态，比如建立数据库连接
            System.out.println("rich function open");
        }

        @Override
        public void close() {
            // 收尾方法，比如断开数据库连接
            System.out.println("rich function close");
        }

        @Override
        public Tuple2<String, Long> map(SensorRead value) {
            return Tuple2.of(value.getId(),value.getTimestamp());
        }
    }
}
