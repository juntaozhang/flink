package org.apache.flink.table.examples.java.my;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/*
nc –lk 9998
1,100,2021-05-08 11:00:10
1,110,2021-05-08 11:00:11
1,120,2021-05-08 11:00:12
1,130,2021-05-08 11:00:13
1,140,2021-05-08 11:00:14
1,150,2021-05-08 11:00:15

nc –lk 9999
2,2021-05-08 11:00:10
2,2021-05-08 11:00:11
2,2021-05-08 11:00:12
2,2021-05-08 11:00:13
2,2021-05-08 11:00:14
2,2021-05-08 11:00:15

 */
public class IntervalJoinExample {
    public static void main(String[] args) {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .withConfiguration(configuration)
                        .build();
        final TableEnvironment env = TableEnvironment.create(settings);

        env.executeSql(
                """
                CREATE TABLE Orders (
                  id STRING,
                  order_amount DOUBLE,
                  order_time TIMESTAMP(0),
                  WATERMARK FOR order_time AS order_time - INTERVAL '1' SECOND
                )
                WITH (
                  'connector' = 'socket',
                  'hostname' = 'localhost',
                  'port' = '9998',
                  'format' = 'csv'
                )
                """);

        env.executeSql(
                """
                CREATE TABLE Shipments (
                  order_id STRING,
                  ship_time TIMESTAMP(0),
                  WATERMARK FOR ship_time AS ship_time - INTERVAL '1' SECOND
                )
                WITH (
                  'connector' = 'socket',
                  'hostname' = 'localhost',
                  'port' = '9999',
                  'format' = 'csv'
                )
                """);

        env.executeSql(
                """
                  CREATE TABLE MyPrint (
                    id STRING,
                    order_time TIMESTAMP(0),
                    ship_time TIMESTAMP(0),
                    order_amount DOUBLE
                  )
                  WITH (
                    'connector' = 'print'
                  )
                """);

        env.executeSql(
                """
                insert into MyPrint
                SELECT
                      o.id
                     ,CAST(o.order_time AS TIMESTAMP) order_time
                     ,CAST(s.ship_time AS TIMESTAMP) ship_time
                     ,o.order_amount
                FROM Orders o
                LEFT JOIN Shipments s
                ON o.id = s.order_id AND (o.order_time BETWEEN s.ship_time - INTERVAL '2' SECOND AND s.ship_time + INTERVAL '1' SECOND)
                """);
    }
}
