package org.apache.flink.table.examples.java;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class FlinkSQLDemo {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Define the Orders table
        String createOrdersTable = "CREATE TEMPORARY TABLE Orders ("
                + "order_number BIGINT, "
                + "price DECIMAL(32, 2), "
                + "order_time BIGINT"
                + ") WITH ("
                + "'connector' = 'datagen', "
                + "'rows-per-second' = '10', "
                + "'fields.order_number.kind' = 'sequence', "
                + "'fields.order_number.start' = '1', "
                + "'fields.order_number.end' = '1000000', "
                + "'fields.price.min' = '1', "
                + "'fields.price.max' = '100', "
                + "'fields.order_time.kind' = 'sequence', "
                + "'fields.order_time.start' = '1717084800', "
                + "'fields.order_time.end' = '1717171200' "
                + ")";

        // Define the print_stat table
        String createPrintStatTable = "CREATE TEMPORARY TABLE print_stat ("
                + "window_end STRING, "
                + "avg_price DECIMAL(32, 2) "
                + ") WITH ("
                + "'connector' = 'print' "
                + ")";
        // Define the insert query
        String insertQuery = "INSERT INTO print_stat "
                + "SELECT "
                + "  CAST(TUMBLE_END(TO_TIMESTAMP(order_time), INTERVAL '10' SECOND) AS STRING) AS window_end, "
                + "  SUM(price) / COUNT(1) AS avg_price "
                + "FROM Orders "
                + "GROUP BY TUMBLE(TO_TIMESTAMP(order_time), INTERVAL '10' SECOND)";

        // Execute the queries
        tableEnv.executeSql(createOrdersTable);
        tableEnv.executeSql(createPrintStatTable);
//        tableEnv.executeSql("select ts,price from (select FROM_UNIXTIME(order_time) as ts, price from Orders ) o").print();
//        tableEnv.executeSql("select ts,price from (select TO_TIMESTAMP_LTZ(order_time, 0) as ts, price from Orders ) o").print();
//        tableEnv.executeSql(insertQuery);
        tableEnv.executeSql("SELECT "
                + "  SUM(price) / COUNT(1) AS avg_price "
                + " FROM ( SELECT FROM_UNIXTIME(order_time) AS ts, price FROM Orders ) o"
                + " GROUP BY TUMBLE(ts, INTERVAL '10' SECOND)").print();


    }
}
