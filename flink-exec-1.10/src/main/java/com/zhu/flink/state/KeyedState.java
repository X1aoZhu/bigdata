package com.zhu.flink.state;

import com.zhu.flink.entity.SensorRead;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;


/**
 * @author: ZhuHaiBo
 * @date: 2021/7/19  17:27
 */
public class KeyedState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1).socketTextStream("192.168.221.161", 7000)
                .map(new MyMapFunction())
                .print();
        environment.execute();
    }

    static class MyMapFunction extends RichMapFunction<String, SensorRead> implements CheckpointedFunction {

        private transient ValueState<Long> valueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 初始化状态
            valueState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("value-state", Long.class));
        }

        @Override
        public SensorRead map(String value) throws IOException {
            Long state = valueState.value();
            valueState.update(++state);

            String[] lineArr = value.split(",");
            return SensorRead.builder()
                    .id(lineArr[0])
                    .timestamp(Long.valueOf(lineArr[1]))
                    .temperature(Double.valueOf(lineArr[2]))
                    .build();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {

        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            //valueState
        }
    }

}
