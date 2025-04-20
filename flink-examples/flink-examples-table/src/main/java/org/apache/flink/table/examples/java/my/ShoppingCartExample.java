package org.apache.flink.table.examples.java.my;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.StateHint;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.Row;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.annotation.ArgumentTrait.REQUIRE_ON_TIME;
import static org.apache.flink.table.annotation.ArgumentTrait.TABLE_AS_SET;

public class ShoppingCartExample {
    @DataTypeHint("ROW<checkout_type STRING, items MAP<BIGINT, INT>>")
    public static class CheckoutProcessor extends ProcessTableFunction<Row> {

        // Object that is stored in state.
        public static class ShoppingCart {

            // The system needs to be able to access all fields for persistence and restore.

            // A map for product IDs to number of items.
            public Map<Long, Integer> content = new HashMap<>();

            // Arbitrary helper methods can be added for structuring the code.

            public void addItem(long productId) {
                content.compute(productId, (k, v) -> (v == null) ? 1 : v + 1);
            }

            public void removeItem(long productId) {
                content.compute(productId, (k, v) -> (v == null || v == 1) ? null : v - 1);
            }

            public boolean hasContent() {
                return !content.isEmpty();
            }
        }

        // Main processing logic
        public void eval(
                Context ctx,
                @StateHint ShoppingCart cart,
                @ArgumentHint({TABLE_AS_SET, REQUIRE_ON_TIME}) Row events,
                @DataTypeHint Duration reminderInterval,
                @DataTypeHint Duration timeoutInterval) {
            String eventType = events.getFieldAs("eventType");
            Long productId = events.getFieldAs("productId");
            switch (eventType) {
                // ADD item
                case "ADD":
                    cart.addItem(productId);
                    updateTimers(ctx, reminderInterval, timeoutInterval);
                    break;

                // REMOVE item
                case "REMOVE":
                    cart.removeItem(productId);
                    if (cart.hasContent()) {
                        updateTimers(ctx, reminderInterval, timeoutInterval);
                    } else {
                        ctx.clearAll();
                    }
                    break;

                // CHECKOUT process
                case "CHECKOUT":
                    if (cart.hasContent()) {
                        collect(Row.of("CHECKOUT", cart.content));
                    }
                    ctx.clearAll();
                    break;
            }
        }

        // Executes REMINDER and TIMEOUT events
        public void onTimer(OnTimerContext ctx, ShoppingCart cart) {
            switch (ctx.currentTimer()) {
                // Send reminder event
                case "REMINDER":
                    collect(Row.of("REMINDER", cart.content));
                    break;

                // Cancel transaction
                case "TIMEOUT":
                    System.out.println("Transaction timed out, clear " + cart.content);
                    ctx.clearAll();
                    break;
            }
        }

        // Helper method that sets or replaces timers for REMINDER and TIMEOUT
        private void updateTimers(
                Context ctx, Duration reminderInterval, Duration timeoutInterval) {
            TimeContext<Instant> timeCtx = ctx.timeContext(Instant.class);
            timeCtx.registerOnTime("REMINDER", timeCtx.time().plus(reminderInterval));
            timeCtx.registerOnTime("TIMEOUT", timeCtx.time().plus(timeoutInterval));
        }
    }

    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.configure(new Configuration().set(RestOptions.PORT, RestOptions.PORT.defaultValue()));
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createFunction("CheckoutProcessor", CheckoutProcessor.class);
//        tEnv.createTable(
//                "Events",
//                TableDescriptor.forConnector("datagen")
//                        .schema(
//                                Schema.newBuilder()
//                                        .column("ts", "TIMESTAMP_LTZ(3)")
//                                        .column("seed", "INT")
//                                        .columnByExpression(
//                                                "eventType",
//                                                """
//                                                CASE
//                                                    WHEN seed % 3 = 0 THEN 'ADD'
//                                                    WHEN seed % 3 = 1 THEN 'CHECKOUT'
//                                                    ELSE 'REMOVE'
//                                                END
//                                                """)
//                                        .columnByExpression(
//                                                "productId", "cast(seed % 3 + 1 as BIGINT)")
//                                        .columnByExpression(
//                                                "user", // can't be `user`
//                                                "IF(seed > 10, 'Bob', 'Alice')")
//                                        .watermark("ts", "ts - INTERVAL '1' SECOND")
//                                        .build())
//                        .option("scan.parallelism", "1")
//                        .option("rows-per-second", "1")
//                        .option("fields.seed.kind", "random")
//                        .option("fields.seed.min", "1")
//                        .option("fields.seed.max", "20")
//                        .build());
                tEnv.executeSql("""
                        CREATE TABLE data_seed (
                            seed INT,
                            ts TIMESTAMP(3),
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
                tEnv.executeSql("""
                        CREATE VIEW Events AS
                        SELECT
                            IF(seed > 10, 'Bob', 'Alice') AS `user`,
                            CASE
                                WHEN seed % 3 = 0 THEN 'ADD'
                                WHEN seed % 3 = 1 THEN 'CHECKOUT'
                                ELSE 'REMOVE'
                            END AS eventType,
                            CAST(seed % 3 + 1 AS BIGINT) AS productId,
                            ts
                        FROM data_seed
                        """);
                tEnv.executeSql("select * from Events").print();
        /*
        2025-03-27 12:00:11.000,Bob,ADD,1
        2025-03-27 12:00:21.000,Alice,ADD,1
        2025-03-27 12:00:51.000,Bob,REMOVE,1
        2025-03-27 12:00:55.000,Bob,ADD,2
        2025-03-27 12:00:56.000,Bob,ADD,5
        2025-03-27 12:01:50.000,Bob,CHECKOUT,-1
                 */
        //        tEnv.executeSql("""
        //                CREATE TABLE Events (
        //                    ts TIMESTAMP(3),
        //                    `user` STRING,
        //                    eventType STRING,
        //                    productId BIGINT,
        //                    WATERMARK FOR ts AS ts - INTERVAL '1' SECOND
        //                )
        //                WITH (
        //                  'connector' = 'socket',
        //                  'hostname' = '%s',
        //                  'port' = '%d',
        //                  'format' = 'csv'
        //                )
        //                """.formatted("localhost", 9999));
        //        tEnv.executeSql("DESCRIBE Events").print();
        //        tEnv.executeSql("select * from Events").print();

//        tEnv.executeSql(
//                """
//                  CREATE TABLE EmailNotifications (
//                    `user` STRING,
//                    checkout_type STRING,
//                    items Map<BIGINT, INT>,
//                    rowtime TIMESTAMP(3)
//                  )
//                  WITH ('connector' = 'print')
//                """);
//
//        tEnv.executeSql(
//                """
//                  CREATE TABLE CheckoutEvents (
//                    `user` STRING,
//                    checkout_type STRING,
//                    items Map<BIGINT, INT>,
//                    rowtime TIMESTAMP(3)
//                  )
//                  WITH ('connector' = 'print')
//                """);
//
//        tEnv.executeSql(
//                """
//                CREATE VIEW Checkouts AS SELECT `user`,checkout_type,items,rowtime FROM
//                CheckoutProcessor(
//                  events => TABLE Events PARTITION BY `user`,
//                  on_time => DESCRIPTOR(ts),
//                  reminderInterval => INTERVAL '1' MINUTES,
//                  timeoutInterval => INTERVAL '2' MINUTES,
//                  uid => 'cart-processor'
//                )
//                """);
//
//        tEnv.executeSql(
//                """
//                EXECUTE STATEMENT SET
//                BEGIN
//                INSERT INTO EmailNotifications
//                SELECT `user`,checkout_type,items,rowtime FROM Checkouts WHERE `checkout_type` = 'REMINDER';
//                INSERT INTO CheckoutEvents
//                SELECT `user`,checkout_type,items,rowtime FROM Checkouts WHERE `checkout_type` = 'CHECKOUT';
//                END;
//                """);
    }
}
