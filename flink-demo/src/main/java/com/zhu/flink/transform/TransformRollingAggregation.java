package com.zhu.flink.transform;

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

        environment.execute();
    }
}
