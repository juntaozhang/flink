package org.apache.flink.table.examples.java.my;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.Row;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.apache.flink.table.annotation.ArgumentTrait.TABLE_AS_SET;

public class PTFExample_onTimer {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.configure(new Configuration().set(RestOptions.PORT, RestOptions.PORT.defaultValue()));
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.createTable(
                "t",
                TableDescriptor.forConnector("datagen")
                        .schema(
                                Schema.newBuilder()
                                        .column("ts", "TIMESTAMP_LTZ(3)")
                                        .column("score", "INT")
                                        .columnByExpression(
                                                "name", "IF(score % 2 = 0, 'Bob', 'Alice')")
                                        .watermark("ts", "ts - INTERVAL '1' SECOND")
                                        .build())
                        .option("scan.parallelism", "1")
                        .option("rows-per-second", "1")
                        .option("fields.score.kind", "random")
                        .option("fields.score.min", "1")
                        .option("fields.score.max", "10")
                        .build());

        //        tEnv.executeSql("""
        //                CREATE TABLE t (
        //                    name STRING,
        //                    score INT,
        //                    ts TIMESTAMP(3),
        //                    WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
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
                  CREATE TABLE MySink1 (
                    name STRING,
                    score INT
                  )
                  WITH ('connector' = 'print')
                """);

        tEnv.executeSql(
                """
                  CREATE TABLE MySink2 (
                    name STRING,
                    score INT
                  )
                  WITH ('connector' = 'print')
                """);

        tEnv.createFunction("f", Function.class);
        tEnv.executeSql(
                """
                        EXECUTE STATEMENT SET
                        BEGIN
                            insert into MySink1
                            SELECT name,score
                            FROM f (
                                r => TABLE t PARTITION BY name,
                                on_time => DESCRIPTOR(ts),
                                s => DESCRIPTOR(score),
                                threshold => 2,
                                uid => 'ptf1'
                            ) where name = 'Bob';

                            insert into MySink2
                            SELECT name,score
                            FROM f (
                                r => TABLE t PARTITION BY name,
                                on_time => DESCRIPTOR(ts),
                                s => DESCRIPTOR(score),
                                threshold => 2,
                                uid => 'ptf2'
                            ) where name = 'Alice';
                        END;
                        """);
        //        tEnv.executeSql(
        //                """
        //                        SELECT name,score,*
        //                        FROM f (
        //                            r => TABLE t PARTITION BY name,
        //                            on_time => DESCRIPTOR(ts),
        //                            s => DESCRIPTOR(score),
        //                            threshold => 2,
        //                            uid => 'same'
        //                        )
        //                        """).print();
    }

    @FunctionHint(output = @DataTypeHint("ROW<name STRING, score INT>"))
    public static class Function extends ProcessTableFunction<Row> {
        public static class ScoreState {
            public String name;
            public int score = 0;
        }

        public void eval(
                Context ctx,
                @StateHint ScoreState state,
                @ArgumentHint(TABLE_AS_SET) Row r,
                ColumnList s,
                Integer threshold) {
            Integer score = r.<Integer>getFieldAs(s.getNames().get(0));
            if (score > threshold) {
                state.name = r.getFieldAs("name");
                state.score += score;
            }

            TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
            Instant currentTime = timeCtx.time();
            Instant roundedUpTime =
                    currentTime.truncatedTo(ChronoUnit.MINUTES).plusSeconds(60).minusMillis(1);
            timeCtx.registerOnTime(roundedUpTime);
        }

        public void onTimer(ScoreState state) {
            collect(Row.of(state.name, state.score));
        }
    }
}
