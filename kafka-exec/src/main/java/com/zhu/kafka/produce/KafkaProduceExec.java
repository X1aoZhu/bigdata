package com.zhu.kafka.produce;

import cn.hutool.core.util.ObjectUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;

/**
 * @Author ZhuHaiBo
 * @Create 2021/8/23 22:46
 */
@Slf4j
public class KafkaProduceExec {

    private final static String TOPIC = "test";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "192.168.240.152:9092");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("partitioner.class", "org.apache.kafka.clients.producer.RoundRobinPartitioner");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        try {
            for (int i = 0; i < 1000; i++) {
                kafkaProducer.send(new ProducerRecord<>(TOPIC, null, null, String.valueOf(i)),
                        (metadata, exception) -> {
                            if (ObjectUtil.isEmpty(exception)) {
                                log.info("topic:[{}],partition:[{}],offset:[{}]", metadata.topic(),
                                        metadata.partition(), metadata.offset());
                            }
                        });
                Thread.sleep(2000L);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.close();
        }
    }

}
