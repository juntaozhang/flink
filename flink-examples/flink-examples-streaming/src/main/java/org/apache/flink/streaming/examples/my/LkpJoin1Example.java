package org.apache.flink.streaming.examples.my;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 *  CREATE TABLE customers (
 *  id INT PRIMARY KEY,
 *  fcolors TEXT[]
 *  );
 *  INSERT INTO customers (id, fcolors) VALUES
 *  (1, string_to_array('red;blue;green', ';')),
 *  (2, string_to_array('yellow', ';')),
 *  (3, string_to_array('black;white', ';'));
 */
public class LkpJoin1Example {
    public static void main(String[] args) {
        TableEnvironment tEnv = TableEnvironment.create(EnvironmentSettings.inStreamingMode());
        tEnv.executeSql("""
                  CREATE TABLE customers (
                    id INT,
                    fcolors ARRAY<STRING>,
                    PRIMARY KEY (id) NOT ENFORCED
                  ) WITH (
                    'connector' = 'jdbc',
                    'url' = 'jdbc:postgresql://localhost:5432/postgres',
                    'table-name' = 'customers',
                    'username' = 'postgres',
                    'password' = 'root'
                  )
                """);
        tEnv.executeSql("DESCRIBE EXTENDED customers").print();
        tEnv.executeSql("select id, fcolors from customers").print();

        tEnv.executeSql("""
                CREATE TABLE orders (
                  id INT,
                  fcolors ARRAY<STRING>,
                  PRIMARY KEY (id) NOT ENFORCED
                ) WITH (
                  'connector' = 'filesystem',
                  'path' = 'file:///Users/juntzhang/src/github/juntaozhang/flink/flink-1.20/flink-examples/flink-examples-streaming/src/main/resources/orders.csv',
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
        tEnv.executeSql("DESCRIBE EXTENDED orders_view").print();

        tEnv.executeSql(
                """
                  SELECT *
                  FROM orders_view AS o
                  JOIN customers FOR SYSTEM_TIME AS OF o.proc_time as a
                   ON o.id = a.id;
                  """).print();

//        tEnv.executeSql(
//                """
//                  SELECT *
//                  FROM orders_view AS o
//                  JOIN customers FOR SYSTEM_TIME AS OF o.proc_time as a
//                  ON o.fcolors = a.fcolors;
//                  """).print();
    }
}
