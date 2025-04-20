package org.apache.flink.table.examples.java.my;

import org.apache.flink.changelog.fs.FsStateChangelogOptions;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateChangelogOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.nio.file.Paths;
import java.time.Duration;

public class DstlExample {
    public static void main(String[] args) {
        String baseDir =
                Paths.get("checkpoints/" + DstlExample.class.getSimpleName()).toUri().toString();

        Configuration config = new Configuration();
        config.set(RestOptions.PORT, RestOptions.PORT.defaultValue());
        config.set(CoreOptions.DEFAULT_PARALLELISM, 3);
        // config.set(StateRecoveryOptions.SAVEPOINT_PATH, baseDir +
        // "/checkpointing/2516c31f505bbd0d6b9f54abd3bd5d2a/chk-21");

        // enableCheckpointing
        config.set(CheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
        config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
        config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, baseDir + "/checkpointing/");

        // local
        // config.set(CheckpointingOptions.LOCAL_BACKUP_ENABLED, true);

        // config.set(StateRecoveryOptions.LOCAL_RECOVERY, true);

        /*
         * enableChangelogStateBackend
         * dstl:Durable Short-term Log
         */
        config.set(StateChangelogOptions.ENABLE_STATE_CHANGE_LOG, true);
        config.set(StateChangelogOptions.PERIODIC_MATERIALIZATION_INTERVAL, Duration.ofMinutes(1));
        config.set(StateChangelogOptions.STATE_CHANGE_LOG_STORAGE, "filesystem");
        config.set(FsStateChangelogOptions.BASE_PATH, baseDir + "/dstl/");

        config.set(StateBackendOptions.STATE_BACKEND, "hashmap");
        //        config.set(StateBackendOptions.STATE_BACKEND, "forst");
        //        config.set(ForStOptions.LOCAL_DIRECTORIES,
        // "/Users/juntzhang/src/juntzhang/flink/checkpoints/DstlExample/forst_local/");
        // config.set(ForStOptions.PRIMARY_DIRECTORY,
        // "/Users/juntzhang/src/juntzhang/flink/checkpoints/DstlExample/forst_pri/");
        //        config.set(ForStOptions.CACHE_SIZE_BASE_LIMIT, MemorySize.parse("1K"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.executeSql(
                """
                CREATE TABLE t (
                    name STRING,
                    score INT
                )
                WITH (
                  'connector' = 'datagen'
                  ,'rows-per-second' = '1'
                  ,'scan.parallelism' = '2'
                  ,'fields.name.length' = '1'
                  ,'fields.score.kind' = 'random'
                  ,'fields.score.min' = '10'
                  ,'fields.score.max' = '20'
                  --,'fields.score.null-rate' = '0.5'
                )
                """);

        //        tEnv.executeSql("""
        //                CREATE TABLE t (
        //                    name STRING,
        //                    score INT
        //                )
        //                WITH (
        //                  'connector' = 'socket',
        //                  'hostname' = '%s',
        //                  'port' = '%d',
        //                  'format' = 'csv'
        //                )
        //                """.formatted("localhost", 9999));

        tEnv.executeSql(
                """
                  CREATE TABLE MySink (
                    name STRING,
                    total_score BIGINT
                  )
                  WITH (
                  'connector' = 'print'
                  )
                """);

        //        tEnv.executeSql(
        //                """
        //                        INSERT INTO MySink
        //                        select t1.name, t1.total_score, t2.cnt from (
        //                            SELECT name, SUM(score) AS total_score
        //                            FROM t
        //                            GROUP BY name
        //                        ) t1 join (
        //                            SELECT name, count(1) AS cnt
        //                            FROM t
        //                            GROUP BY name
        //                        ) t2 on t1.name = t2.name
        //                        """);

        tEnv.executeSql(
                """
                        INSERT INTO MySink
                        SELECT name, SUM(score) AS total_score
                        FROM t
                        GROUP BY name
                        """);
    }
}
