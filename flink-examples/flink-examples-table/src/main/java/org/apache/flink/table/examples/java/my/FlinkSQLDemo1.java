package org.apache.flink.table.examples.java.my;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQLDemo1 {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Define the Orders table
        String createOrdersTable = "CREATE TEMPORARY TABLE Orders ("
                + "order_number BIGINT, "
                + "price DECIMAL(32, 2), "
                + "ts TIMESTAMP(3), WATERMARK FOR ts AS ts - INTERVAL '5' SECOND"
                + ") WITH ("
                + "'connector' = 'datagen', "
                + "'rows-per-second' = '10', "
                + "'fields.order_number.kind' = 'sequence', "
                + "'fields.order_number.start' = '1', "
                + "'fields.order_number.end' = '1000000', "
                + "'fields.price.min' = '1', "
                + "'fields.price.max' = '100' "
                + ")";


        // Execute the queries
        tableEnv.executeSql(createOrdersTable);
        String query =
                "SELECT\n"
                        + "  CAST(TUMBLE_START(ts, INTERVAL '10' SECOND) AS STRING) window_start,\n"
                        + "  COUNT(*) order_num,\n"
                        + "  SUM(price) total_amount"
                        + " FROM Orders\n"
                        + " GROUP BY TUMBLE(ts, INTERVAL '10' SECOND) ";
        tableEnv.executeSql(query).print();


    }
}
