package org.apache.flink.table.examples.java.my;

import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.ArgumentTrait;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.Instant;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.descriptor;

public class PTFExample_onTimer2 {
    public static void main(String[] args) {
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inStreamingMode());

        env.executeSql(
                "CREATE TABLE Events (ts TIMESTAMP_LTZ(3), id STRING, val INT, WATERMARK FOR ts AS ts - INTERVAL '2' SECONDS) "
                        + "WITH ('connector' = 'datagen', 'rows-per-second'='1')");
        // For SQL register the function and pass the DESCRIPTOR argument
        env.createFunction("PingLaterFunction", PingLaterFunction.class);
        //        env
        //                .executeSql(
        //                        "SELECT * FROM PingLaterFunction(input => TABLE Events PARTITION
        // BY id, on_time => DESCRIPTOR(`ts`))")
        //                .print();

        env.from("Events")
                .partitionBy($("id"))
                .process(PingLaterFunction.class, descriptor("ts").asArgument("on_time"))
                .execute()
                .print();
    }

    public static class PingLaterFunction extends ProcessTableFunction<String> {
        public void eval(
                Context ctx,
                @ArgumentHint({ArgumentTrait.TABLE_AS_SET, ArgumentTrait.REQUIRE_ON_TIME})
                        Row input) {
            TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
            // Replaces an existing timer and thus potentially resets the minute if necessary
            timeCtx.registerOnTime("ping", timeCtx.time().plus(Duration.ofSeconds(10)));
        }

        public void onTimer(OnTimerContext onTimerCtx) {
            collect("pong");
        }
    }
}
