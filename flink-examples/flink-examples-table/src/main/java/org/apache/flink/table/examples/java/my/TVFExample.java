package org.apache.flink.table.examples.java.my;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * nc -l 9999
 *
 * <p>n1,90,2021-09-01 00:00:01 n3,75,2021-09-01 00:00:03 n1,68,2021-09-01 00:00:04 n2,83,2021-09-01
 * 00:00:02 n2,61,2021-09-01 00:00:05 n3,54,2021-09-01 00:00:06 n1,47,2021-09-01 00:00:07
 * n3,99,2021-09-01 00:00:09 n1,98,2021-09-01 00:00:08 n2,61,2021-09-01 00:00:11 n2,68,2021-09-01
 * 00:00:10 n1,54,2021-09-01 00:00:12 n3,47,2021-09-01 00:00:13 n2,29,2021-09-01 00:00:15
 * n1,38,2021-09-01 00:00:14 n3,20,2021-09-01 00:00:16
 */
public class TVFExample {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.configure(new Configuration().set(RestOptions.PORT, RestOptions.PORT.defaultValue()));
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                """
                CREATE TABLE t (
                    name STRING,
                    score INT,
                    event_time TIMESTAMP(3),
                    WATERMARK FOR event_time AS event_time - INTERVAL '2' SECOND -- 解决乱序问题
                )
                WITH (
                  'connector' = 'socket',
                  'hostname' = '%s',
                  'port' = '%d',
                  'format' = 'csv'
                )
                """
                        .formatted("localhost", 9999));

        tEnv.executeSql(
                """
                  CREATE TABLE MySink (
                    window_start TIMESTAMP(3),
                    window_end TIMESTAMP(3),
                    total_score INT
                  )
                  WITH ('connector' = 'print')
                """);

        tEnv.executeSql(
                """
                        INSERT INTO MySink
                        SELECT window_start, window_end, SUM(score) AS total_score  --- global window
                        FROM TUMBLE(
                            DATA => TABLE t,
                            TIMECOL => DESCRIPTOR(event_time),
                            SIZE => INTERVAL '5' SECONDS -- 窗口大小
                        )
                        GROUP BY window_start, window_end
                        """);

        //        tEnv.executeSql(
        //                """
        //                        INSERT INTO MySink
        //                        SELECT window_start, window_end, SUM(score) AS total_score
        //                        FROM HOP(
        //                            DATA => TABLE t,
        //                            TIMECOL => DESCRIPTOR(event_time),
        //                            SIZE => INTERVAL '4' SECONDS, -- 窗口大小
        //                            SLIDE => INTERVAL '2' SECONDS  -- size must be an integral
        // multiple of slide
        //                        )
        //                        GROUP BY window_start, window_end
        //                        """);
    }
}
