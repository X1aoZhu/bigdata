package com.zhu.flink.source;

import com.zhu.flink.entity.SensorRead;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * 自定义数据源
 *
 * @Author ZhuHaiBo
 * @Create 2021/7/11 15:10
 */
public class Test1Udf {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.addSource(new MySourceFunction()).print();

        environment.execute();
    }

    /**
     * 自定义数据源
     */
    static class MySourceFunction implements SourceFunction<SensorRead> {
        // 停止标志位
        boolean flag = true;

        int id = 1001;
        long timestamp = 20210711;

        @Override
        public void run(SourceContext<SensorRead> ctx) throws Exception {
            Random random = new Random();
            Map<String, Double> map = new HashMap<>(16);
            for (int i = 1; i < 11; i++) {
                map.put("sensor_" + i, random.nextGaussian());
            }
            while (flag) {
                for (Map.Entry<String, Double> entry : map.entrySet()) {
                    Double source = map.get(entry.getKey());
                    ctx.collect(SensorRead.builder().id(String.valueOf(id++)).timestamp(timestamp++).temperature(source).build());
                }
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

}
