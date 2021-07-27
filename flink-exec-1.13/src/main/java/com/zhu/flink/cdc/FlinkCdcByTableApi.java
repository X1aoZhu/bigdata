package com.zhu.flink.cdc;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * FlinK cdc TableApi
 *
 * @author: ZhuHaiBo
 * @date: 2021/7/23  11:45
 */
public class FlinkCdcByTableApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, environmentSettings);

        // MySQL SourceFunction
        SourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("hadoop1")
                .port(3306)
                .username("root")
                .password("root")
                .databaseList("flink_cdc")
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();

        DataStreamSource<String> dataStreamSource = environment.addSource(sourceFunction);

        tableEnvironment.createTemporaryView("mysql_binlog", dataStreamSource);
        Table tempTable = tableEnvironment.sqlQuery("SELECT * FROM mysql_binlog");
        tableEnvironment.toAppendStream(tempTable, Row.class).print();

        environment.execute();
    }
}
