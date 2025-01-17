package org.apache.flink.streaming.examples.my.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.apache.flink.streaming.examples.my.kafka.KafkaExample.getNumberOfPartitions;

@Slf4j
public class KafkaRecordProducer {
    private final String topic = "test-input3";
    private final String brokers = "localhost:29092,localhost:39092,localhost:49092";
    private volatile boolean running = true;

    public static void main(String[] args) throws Exception {
        KafkaRecordProducer producer = new KafkaRecordProducer();
        producer.run();
    }

    public void run() throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put(
                "value.serializer",
                "org.apache.flink.streaming.examples.my.kafka.KryoSerializer");
        props.put("linger.ms", 1);

        int partitions = getNumberOfPartitions(topic, props);

        try (Producer<String, UserEvent> producer = new KafkaProducer<>(props)) {
            int i = 0;
            while (running) {
                Future<RecordMetadata> metadataFuture = producer.send(new ProducerRecord<>(
                        topic, i % partitions, System.currentTimeMillis(),
                        null, UserEvent.click(i)));
                try {
                    log.info("sent record => {}", metadataFuture.get().toString());
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }

                if (i % 5 == 0) {
                    metadataFuture = producer.send(new ProducerRecord<>(topic, UserEvent.watermark(i)));
                    try {
                        log.info("sent watermark record => {}", metadataFuture.get().toString());
                    } catch (Exception e) {
                        log.error(e.getMessage(), e);
                    }
                }

                i += 1;
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException ignore) {
                    running = false;
                }
            }
        }

    }
}
