package com.zhu.flink.tableapi;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;


/**
 * Flink TableApi
 *
 * @author: ZhuHaiBo
 * @date: 2021/7/16  17:41
 */
public class TableApiKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1).setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // kafka config
        //Properties properties = new Properties();
        //properties.put("bootstrap.servers", "192.168.240.161:9092");
        //properties.put("zookeeper.connect", "192.168.240.161:2181");

        // kafka connect consumer
        tableEnvironment.connect(new Kafka()
                .topic("sensor_source")
                .property("bootstrap.servers", "192.168.221.161:9092")
                .property("zookeeper.connect", "192.168.221.161:2181")
                )
                .withFormat(new Csv())
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("timestamp", DataTypes.BIGINT())
                                .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("sensor_source_kafka");

        //Table resultTable = tableEnvironment.from("sensor_source_kafka").select("*");
        Table resultTable = tableEnvironment.sqlQuery("select * from sensor_source_kafka");
        tableEnvironment.toAppendStream(resultTable, Row.class).print();


        // kafka connect producer
        tableEnvironment.connect(new Kafka().topic("sensor_result")
                .property("bootstrap.servers", "192.168.221.161:9092")
                .property("zookeeper.connect", "192.168.221.161:2181"))
                .withFormat(new Csv())
                .withSchema(
                        new Schema()
                                .field("id", DataTypes.STRING())
                                .field("timestamp", DataTypes.BIGINT())
                                .field("temperature", DataTypes.DOUBLE()))
                .createTemporaryTable("sensor_source_result");
        resultTable.insertInto("sensor_source_result");
        environment.execute();
    }
}
