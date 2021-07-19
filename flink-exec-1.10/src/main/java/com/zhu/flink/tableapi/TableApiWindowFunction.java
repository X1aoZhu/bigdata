package com.zhu.flink.tableapi;

import com.zhu.flink.entity.SensorRead;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;




/**
 * TableApi WindowFunction
 *
 * @author: ZhuHaiBo
 * @date: 2021/7/19  10:03
 */
public class TableApiWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment, environmentSettings);

        // 源数据准备，配置watermark
        String filePath = "flink-demo/src/main/resources/SensorRead.txt";
        DataStream<SensorRead> source = environment.readTextFile(filePath)
                .map(line -> {
                    String[] split = line.split(",");
                    return SensorRead.builder().id(split[0])
                            .timestamp(Long.parseLong(split[1]))
                            .temperature(Double.parseDouble(split[2])).build();
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorRead>(Time.seconds(2L)) {
                    @Override
                    public long extractTimestamp(SensorRead element) {
                        return element.getTimestamp() * 1000;
                    }
                });

        // EventTime: rowtime
        Table eventTimeTable = tableEnvironment.fromDataStream(source,"id,timestamp,temperature, timestamp_rt.rowtime");
        tableEnvironment.createTemporaryView("sensor", eventTimeTable);

        /**
         * 按照时间为维度的滚动时间窗口，窗口大小为 10s
         */
        // 1.1 Group Window, TableApi
        Table eventTimeApiResult = eventTimeTable
                .window(Tumble.over("10.second").on("timestamp_rt").as("tw"))
                .groupBy("id, tw")
                .select("id, id.count, temperature.avg, tw.end, tw.start");
        //tableEnvironment.toAppendStream(eventTimeApiResult, Row.class).print("Group Window TableApi");

        // 1.2 Group Window, SQL
        Table eventTimeSqlResult = tableEnvironment.sqlQuery("select id, count(id) as cnt, avg(temperature) as avgTemp, tumble_end(timestamp_rt, interval '10' second)" +
                "from sensor group by id, tumble(timestamp_rt, interval '10' second)");
        //tableEnvironment.toAppendStream(eventTimeSqlResult, Row.class).print("Group Window SQL");


        /**
         * 按照计数为维度的计数窗口
         */
        // 2.1 Over Window, TableApi
        Table eventTimeOverResult = eventTimeTable.window(Over.partitionBy("id").orderBy("timestamp_rt").preceding("2.rows").as("ow"))
                .select("id, id.count over ow ,temperature.avg over ow");
        //tableEnvironment.toAppendStream(eventTimeOverResult, Row.class).print("result");

        // 2.2 Over Window, SQL
        Table overSqlResult = tableEnvironment.sqlQuery("select id, rt, count(id) over ow, avg(temperature) over ow " +
                " from sensor " +
                " window ow as (partition by id order by rt rows between 2 preceding and current row)");
        tableEnvironment.toRetractStream(overSqlResult, Row.class).print("SQL");

        environment.execute();
    }
}
