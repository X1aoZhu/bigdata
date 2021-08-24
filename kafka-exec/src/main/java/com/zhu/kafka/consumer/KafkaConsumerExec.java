package com.zhu.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @Author ZhuHaiBo
 * @Create 2021/8/25 0:12
 */
@Slf4j
public class KafkaConsumerExec {

    private final static String TOPIC = "test";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap-servers", "192.168.240.152:9092");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Collections.singleton(TOPIC));

        while (true) {
            //kafkaConsumer.poll()
        }

    }
}
