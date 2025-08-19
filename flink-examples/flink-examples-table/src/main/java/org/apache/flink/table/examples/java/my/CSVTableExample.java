// CHECKSTYLE.OFF:
package org.apache.flink.table.examples.java.my;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

public class CSVTableExample {
    public static void main(String[] args) {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.executeSql("""
                CREATE TABLE customers (
                  id INT,
                  fcolors ARRAY<STRING>,
                  PRIMARY KEY (id) NOT ENFORCED
                ) WITH (
                  'connector' = 'filesystem',
                  'path' = 'file:///Users/juntzhang/src/github/juntaozhang/flink/flink-2.1-SNAPSHOT-study/flink-examples/flink-examples-table/src/main/resources/customers.csv',
                  'format' = 'csv'
                )
                """);
//        tEnv.executeSql("""
//                  CREATE TABLE customers (
//                    id INT,
//                    fcolors ARRAY<STRING>,
//                    PRIMARY KEY (id) NOT ENFORCED
//                  ) WITH (
//                    'connector' = 'jdbc',
//                    'url' = 'jdbc:postgresql://localhost:5432/postgres',
//                    'table-name' = 'customers',
//                    'username' = 'postgres',
//                    'password' = 'root'
//                  )
//                """);
        tEnv.executeSql("DESCRIBE EXTENDED customers").print();
        tEnv.executeSql("select id, fcolors from customers").print();

        tEnv.executeSql("""
                CREATE TABLE orders (
                  id INT,
                  fcolors ARRAY<STRING>,
                  PRIMARY KEY (id) NOT ENFORCED
                ) WITH (
                  'connector' = 'filesystem',
                  'path' = 'file:///Users/juntzhang/src/github/juntaozhang/flink/flink-2.1-SNAPSHOT-study/flink-examples/flink-examples-table/src/main/resources/customers.csv',
                  'format' = 'csv'
                )
                """);
        tEnv.executeSql(
                """
                  CREATE TEMPORARY VIEW orders_view AS
                   SELECT
                    *,
                    PROCTIME() AS `proc_time`
                   FROM orders;
                  """);
//        tEnv.executeSql(
//                """
//                  create table orders (
//                   id int
//                   ) with (
//                    'connector' = 'datagen',
//                    'number-of-rows' = '10',
//                    'rows-per-second' = '1' -- slow producing speed to make sure that
//                                            -- source is not finished when job is cancelled
//                  )
//                  """);
//        tEnv.executeSql(
//                """
//                  CREATE TEMPORARY VIEW orders_view AS
//                   SELECT
//                    *,
//                    CAST(ARRAY['red','green'] AS ARRAY<STRING>) AS `fcolors`,
//                    PROCTIME() AS `proc_time`
//                   FROM orders;
//                  """);
        tEnv.executeSql("DESCRIBE EXTENDED orders_view").print();

        tEnv.executeSql(
                """
                  SELECT *
                  FROM orders_view AS o
                  JOIN customers FOR SYSTEM_TIME AS OF o.proc_time as a
                   ON o.id = a.id;
                  """).print();
    }
}
