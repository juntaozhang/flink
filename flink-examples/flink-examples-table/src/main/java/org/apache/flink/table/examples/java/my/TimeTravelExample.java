package org.apache.flink.table.examples.java.my;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TimeTravelExample {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.configure(new Configuration().set(RestOptions.PORT, RestOptions.PORT.defaultValue()));
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

//        tEnv.executeSql("""
//                 SELECT *, NOW() as n FROM (VALUES 1, 2, 3)
//                """).print();

        tEnv.executeSql("""
                        CREATE TABLE data_seed (
                            seed INT,
                            ts TIMESTAMP(3),
                            proctime AS PROCTIME(),
                            WATERMARK FOR ts AS ts - INTERVAL '2' SECOND
                        )
                        WITH (
                          'connector' = 'datagen'
                          ,'rows-per-second' = '1'
                          ,'scan.parallelism' = '1'
                          ,'fields.seed.kind' = 'random'
                          ,'fields.seed.min' = '1'
                          ,'fields.seed.max' = '20'
                        )
                        """);

        tEnv.executeSql("SELECT *,NOW() FROM data_seed FOR SYSTEM_TIME AS OF NOW()").print();
    }
}
