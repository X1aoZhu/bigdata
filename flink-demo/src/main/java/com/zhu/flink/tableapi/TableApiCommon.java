package com.zhu.flink.tableapi;


import com.zhu.flink.entity.SensorRead;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;


/**
 * TableApi
 *
 * @author: ZhuHaiBo
 * @date: 2021/7/16  11:00
 */
public class TableApiCommon {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();

        // 创建TableEnvironment
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        // 创建数据源连接
        // 数据源，创建连接器，创建临时表
        String filePath = "flink-demo/src/main/resources/SensorRead.txt";
        tableEnvironment.connect(new FileSystem().path(filePath))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .withFormat(new Csv().fieldDelimiter(','))
                .createTemporaryTable("input_sensor");
        tableEnvironment.from("input_sensor");

        Table sqlQuery = tableEnvironment.sqlQuery("select * from input_sensor");

        tableEnvironment.toAppendStream(sqlQuery, Row.class).print("rows");


        environment.execute();
    }
}
