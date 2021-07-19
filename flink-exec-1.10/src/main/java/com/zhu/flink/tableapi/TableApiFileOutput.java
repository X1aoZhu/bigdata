package com.zhu.flink.tableapi;

import com.alibaba.fastjson.JSON;
import com.zhu.flink.entity.SensorRead;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.Properties;

/**
 * @Author ZhuHaiBo
 * @Create 2021/7/18 17:38
 */
public class TableApiFileOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // table env
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, environmentSettings);

        // kafka properties
        Properties properties = new Properties();
        properties.put("zookeeper.server", "hadoop3:2181");
        properties.put("bootstrap.servers", "hadoop4:9092");

        // kafka consumer conn
        tableEnvironment.connect(new Kafka().version("0.11").properties(properties).topic("sensor_source"))
                .withSchema(new Schema().field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .withFormat(new Csv())
                .createTemporaryTable("kafka_sensor_source");

        // 原始数据
        Table table = tableEnvironment.sqlQuery("select * from kafka_sensor_source");
        DataStream<Row> source = tableEnvironment.toAppendStream(table, Row.class);
        source.print("kafka_source");

        // 将查询数据转换为JSON
        //DataStream<String> transSource = source.map(line -> {
        //    SensorRead sensorRead = SensorRead.builder()
        //            .id(String.valueOf(line.getField(0)))
        //            .timestamp((Long) line.getField(1))
        //            .temperature((Double) line.getField(2))
        //            .build();
        //    return JSON.toJSONString(sensorRead);
        //});

        String outputPath = "flink-demo/src/main/resources/Output.txt";
        tableEnvironment.connect(new FileSystem().path(outputPath))
                .withSchema(new Schema().field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .withFormat(new Csv())
                .createTemporaryTable("output");
        //tableEnvironment.fromDataStream(transSource).insertInto("output");
        table.insertInto("output");
        environment.execute();
    }
}
