package org.apache.flink.streaming.examples.my;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * <a href="https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/table/formats/json/">json doc</a>
 */
public class KafkaExample {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql("""
                CREATE TABLE t (
                    name STRING,
                    score INT
                )
                WITH (
                 'connector' = 'kafka',
                 'topic' = 'test-input1',
                 'properties.bootstrap.servers' = 'localhost:29092,localhost:39092,localhost:49092',
                 'properties.group.id' = 'myconsumer2',
                 'format' = 'csv',
                 'scan.startup.mode' = 'latest-offset'
                )
                """);

        tEnv.executeSql("""
                  CREATE TABLE MySink (
                     name STRING
                    ,total_score INT
                    ,PRIMARY KEY (name) NOT ENFORCED
                  )
                  WITH (
                   'connector' = 'kafka',
                   'topic' = 'test-output1',
                   'properties.bootstrap.servers' = 'localhost:29092,localhost:39092,localhost:49092',
                   'format' = 'maxwell-json'
                   -- 'format' = 'canal-json'
                   -- 'format' = 'debezium-json'
                   -- 'key.format' = 'json',
                   -- 'key.fields' = 'name',
                   -- 'key.json.ignore-parse-errors' = 'true',
                   -- 'value.format' = 'debezium-json',
                   -- 'value.debezium-json.ignore-parse-errors' = 'true',
                   -- 'value.fields-include' = 'ALL'
                  )
                """);

//        tEnv.executeSql("""
//                  CREATE TABLE MySink (
//                     name STRING
//                    ,total_score INT
//                    ,PRIMARY KEY (name) NOT ENFORCED
//                  )
//                  WITH (
//                   'connector' = 'upsert-kafka',
//                   'topic' = 'test-output1',
//                   'properties.bootstrap.servers' = 'localhost:29092,localhost:39092,localhost:49092',
//                   'key.format' = 'csv',
//                   'value.format' = 'csv'
//                  )
//                """);

        tEnv.executeSql(
                """
                        INSERT INTO MySink
                        SELECT name, sum(score) AS total_score
                        FROM t
                        group by name
                        """);
    }
}
