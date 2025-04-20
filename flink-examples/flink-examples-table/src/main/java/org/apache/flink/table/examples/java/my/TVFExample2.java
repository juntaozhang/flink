package org.apache.flink.table.examples.java.my;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TVFExample2 {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
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

        //        tEnv.executeSql(
        //                """
        //                  CREATE TABLE MySink (
        //                    name STRING,
        //                    window_start TIMESTAMP(3),
        //                    window_end TIMESTAMP(3),
        //                    total_score INT
        //                  )
        //                  WITH ('connector' = 'print')
        //                """);
        //
        //        tEnv.executeSql(
        //                """
        //                        INSERT INTO MySink
        //                        SELECT name, window_start, window_end, SUM(score) AS total_score
        // --- global window
        //                        FROM TUMBLE(
        //                            DATA => TABLE t,
        //                            TIMECOL => DESCRIPTOR(event_time),
        //                            SIZE => INTERVAL '5' SECONDS -- 窗口大小
        //                        )
        //                        GROUP BY name, window_start, window_end
        //                        """);

        tEnv.executeSql(
                """
                        create view MySink as
                        SELECT name, window_start, window_end, SUM(score) AS total_score  --- global window
                        FROM TUMBLE(
                            DATA => TABLE t,
                            TIMECOL => DESCRIPTOR(event_time),
                            SIZE => INTERVAL '5' SECONDS -- 窗口大小
                        )
                        GROUP BY name, window_start, window_end
                        """);
        tEnv.executeSql("select * from MySink").print(); // 直接打印输出
    }
}
