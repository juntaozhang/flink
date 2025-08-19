package org.apache.flink.streaming.examples.my.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class KafkaRecordConsumer {
    private final String topic = "test-output1";
    private final String brokers = "localhost:29092,localhost:39092,localhost:49092";
    private volatile boolean running = true;

    public static void main(String[] args) {
        new KafkaRecordConsumer().run();
    }

    private void run() {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("group.id", "test-consumer-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(
                "value.deserializer",
                "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("auto.offset.reset", "latest");

        try (KafkaConsumer<String, Integer> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            while (running) {
                ConsumerRecords<String, Integer> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, Integer> record : records) {
                    log.info("kafka record {} => {}", record.key(), record.value());
                }
            }
        }
    }
}
