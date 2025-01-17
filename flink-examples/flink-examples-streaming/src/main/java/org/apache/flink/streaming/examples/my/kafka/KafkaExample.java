/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.my.kafka;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * A simple application used as smoke test example to forward messages from one topic to another
 * topic in batch mode.
 *
 * <p>Example usage: --input-topic test-input --output-topic test-output --bootstrap.servers
 * localhost:9092 --group.id myconsumer
 */
public class KafkaExample extends KafkaExampleUtil {

    public static void main(String[] args) throws Exception {
        // parse input arguments
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = KafkaExampleUtil.prepareExecutionEnv(parameterTool);
        Properties kafkaProperties = new Properties();
//        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, parameterTool.getRequired("bootstrap.servers"));
//        kafkaProperties.setProperty("security.protocol", "SASL_PLAINTEXT");
//        kafkaProperties.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-256");
//        kafkaProperties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user1\" password=\"ZSC3tkNj1x\";");

        kafkaProperties.setProperty("acks", "all");
        kafkaProperties.setProperty("enable.idempotence", "true");
        kafkaProperties.put(
                "key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaProperties.put(
                "value.deserializer",
                "org.apache.flink.streaming.examples.my.kafka.KryoDeserializer");
        kafkaProperties.setProperty(
                "bootstrap.servers",
                parameterTool.getRequired(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));
        int numPartitions = getNumberOfPartitions(
                parameterTool.getRequired("input-topic"),
                kafkaProperties);
        env.setParallelism(numPartitions);
        DataStream<UserEvent> input =
                env.fromSource(
                        KafkaSource.<UserEvent>builder()
                                .setBootstrapServers(
                                        parameterTool
                                                .getProperties()
                                                .getProperty(
                                                        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
                                .setStartingOffsets(OffsetsInitializer.latest())
                                .setDeserializer(
                                        KafkaRecordDeserializationSchema.valueOnly(KryoDeserializer.class)
                                )
                                .setTopics(parameterTool.getRequired("input-topic"))
                                .setProperties(kafkaProperties)
                                .build(),
                        WatermarkStrategy.noWatermarks(),
                        "kafka-source").disableChaining();

        input.sinkTo(
                KafkaSink.<UserEvent>builder()
                        .setBootstrapServers(
                                parameterTool
                                        .getProperties()
                                        .getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG))
                        .setRecordSerializer(
                                KafkaRecordSerializationSchema.builder()
                                        .setTopic(parameterTool.getRequired("output-topic"))
                                        .setKafkaValueSerializer(KryoSerializer.class)
                                        .build())
                        .setKafkaProducerConfig(kafkaProperties)
                        .build())
        ;
        env.execute("Smoke Kafka Example");
    }

    public static int getNumberOfPartitions(
            String topic,
            Properties properties) throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(properties)) {
            TopicDescription topicDescription = adminClient
                    .describeTopics(java.util.Collections.singletonList(topic))
                    .allTopicNames()
                    .get()
                    .get(topic);
            return topicDescription.partitions().size();
        }
    }
}
