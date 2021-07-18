package com.zhu.flink.tableapi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;


/**
 * TableApi Common
 *
 * @author: ZhuHaiBo
 * @date: 2021/7/16  11:00
 */
public class TableApiCommon {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);

        // 1.0 创建TableEnvironment，默认streamEnv
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);

        // 源数据，创建连接
        String filePath = "flink-demo/src/main/resources/SensorRead.txt";
        tableEnvironment.connect(new FileSystem().path(filePath))
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temperature", DataTypes.DOUBLE()))
                .withFormat(new Csv().fieldDelimiter(','))
                .createTemporaryTable("input_sensor");
        tableEnvironment.from("input_sensor");

        // 1.1 基于老版本的planner的流处理
        EnvironmentSettings oldPlannerStreamSit = EnvironmentSettings.newInstance().inStreamingMode().useOldPlanner().build();
        StreamTableEnvironment oldPlannerStreamEnv = StreamTableEnvironment.create(environment, oldPlannerStreamSit);

        // 1.2 基于老版本的planner批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);

        // 1.3 基于Blink的流处理
        EnvironmentSettings blinkStreamSit = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment blinkStreamEnv = StreamTableEnvironment.create(environment, blinkStreamSit);

        // 1.4 基于Blink的批处理
        EnvironmentSettings blinkBatchSit = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment blinkBatchEnv = TableEnvironment.create(blinkBatchSit);

        // 2.1 直接执行SQL 输出结果，直接追加 AppendStream SQL
        Table table = tableEnvironment.sqlQuery("select * from input_sensor where id = 'sensor_1'");
        tableEnvironment.toAppendStream(table, Row.class)
                .print("AppendStream SQL");

        // 2.2 AppendStream TableApi，完成查询，直接追加
        Table tempTable = tableEnvironment.from("input_sensor").select("id,timestamp").filter("id = 'sensor_1'");
        tableEnvironment.toAppendStream(tempTable, Row.class)
                .print("AppendStream TableApi");

        // 2.3 直接执行SQL RetractStream SQL
        Table table1 = tableEnvironment.sqlQuery("select id,count(id) as cnt from input_sensor group by id");
        tableEnvironment.toRetractStream(table1, Row.class).print("RetractStream SQL");

        environment.execute();
    }
}
