package com.zhu.flink.sink;

import com.alibaba.fastjson.JSON;
import com.zhu.flink.entity.SensorRead;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * @author: ZhuHaiBo
 * @date: 2021/7/13  14:18
 */
public class Sink2Jdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "sensor-result";
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.221.161:9092");

        FlinkKafkaConsumer011<String> kafkaConsumer = new FlinkKafkaConsumer011<>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer.setStartFromEarliest();

        environment.addSource(kafkaConsumer)
                .map((MapFunction<String, SensorRead>) line -> JSON.parseObject(line, SensorRead.class))
        .addSink(new MyJdbcSink());


        environment.execute();
    }

    static class MyJdbcSink extends RichSinkFunction<SensorRead> {
        Connection connection = null;
        PreparedStatement insertStmt = null;
        PreparedStatement updateStmt = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink_data?characterEncoding=utf8&useSSL=true", "root", "root");
            insertStmt = connection.prepareStatement("insert into sensor (id, timestamp, temperature) values (?, ?, ?)");
            //updateStmt = connection.prepareStatement("update sensor set temperature = ? where id = ?");
        }

        @Override
        public void invoke(SensorRead value, Context context) throws Exception {
            //每来一条数据，调用连接，执行sql
            insertStmt.setString(1, value.getId());
            insertStmt.setLong(2, value.getTimestamp());
            insertStmt.setDouble(3, value.getTemperature());
            insertStmt.execute();
        }


        @Override
        public void close() throws Exception {
            connection.close();
            insertStmt.close();
        }
    }
}
