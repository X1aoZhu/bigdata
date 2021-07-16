package com.zhu.flink.tableapi;

import com.zhu.flink.entity.SensorRead;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * Flink TableApi && DataStream
 *
 * @author: ZhuHaiBo
 * @date: 2021/7/15  11:18
 */
public class TableApiExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用DataStream作为数据源，并以TableApi输出
        DataStream<SensorRead> source = environment.readTextFile("flink-demo/src/main/resources/SensorRead.txt")
                .flatMap((FlatMapFunction<String, SensorRead>) (line, collector) -> {
                    String[] split = line.split(",");
                    collector.collect(SensorRead.builder().id(split[0])
                            .timestamp(Long.parseLong(split[1]))
                            .temperature(Double.parseDouble(split[2])).build());
                }).returns(SensorRead.class);

        // 创建表环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 基于流创建Table
        Table table = tableEnvironment.fromDataStream(source);
        // 执行TableApi，执行SQL
        Table tableResult = table.select("id,timestamp,temperature").where("id='sensor_1'");
        // 将结果转化为流输出
        tableEnvironment.toAppendStream(tableResult, SensorRead.class)
                .print("TableApi Result");

        // 直接执行SQL
        tableEnvironment.createTemporaryView("temp_sensor", source);
        Table sqlQuery = tableEnvironment.sqlQuery("select * from temp_sensor where id = 'sensor_1'");
        tableEnvironment.toAppendStream(sqlQuery, SensorRead.class).print("SQL Result");

        environment.execute();
    }
}
