package com.zhu.kafka.consumer;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Headers;
import org.slf4j.LoggerFactory;


import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/**
 * @Author ZhuHaiBo
 * @Create 2021/8/25 0:12
 */
@Slf4j
public class KafkaConsumerExec {

    private final static String TOPIC = "test";

    static {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger root = loggerContext.getLogger("root");
        root.setLevel(Level.INFO);
    }

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.240.152:9092");
        properties.put("group.id", "a");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton(TOPIC));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(5L));
            for (ConsumerRecord<String, String> record : consumerRecords) {
                Headers headers = record.headers();
                log.info("key:[{}],value:[{}],partition：[{}],header:[{}]", record.key(), record.value(), record.partition(), headers.toArray());
            }
        }

    }
}
