package org.apache.flink.table.examples.java.my;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;

/**
 *
 *
 * <pre>
 *  nc -lk 9998
 *  1,alice
 *  2,bob
 *  ----------------
 *  2,bob2
 *
 *  nc -lk 9999
 *  1,2021-09-01 10:00:00,1,100.0
 *  2,2021-09-01 10:00:00,2,200.0
 *  3,2021-09-01 10:00:00,1,50.0
 *
 * </pre>
 */
public class JoinExample {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .withConfiguration(configuration)
                        .build();
        final TableEnvironment env = TableEnvironment.create(settings);
        createSrcTable(env);
        createSinkTable(env);
        env.executeSql(
                """
                 INSERT INTO `enrich_order`
                 SELECT o.*, u.name AS customer_name
                 FROM `order` o
                 LEFT JOIN `user` u ON o.customer_id = u.id
                """);
    }

    private static void createSinkTable(TableEnvironment tEnv) {
        tEnv.executeSql(
                """
                 CREATE TABLE enrich_order (
                    id STRING,
                    order_date TIMESTAMP(3),
                    customer_id STRING,
                    amount DOUBLE,
                    customer_name STRING,
                    PRIMARY KEY (id) NOT ENFORCED
                 )
                 WITH ('connector' = 'print')
                """);
    }

    private static void createSrcTable(TableEnvironment tEnv) {
        tEnv.executeSql(
                """
                CREATE TABLE `order` (
                    id STRING,
                    order_date TIMESTAMP(3),
                    customer_id STRING,
                    amount DOUBLE,
                    PRIMARY KEY (id) NOT ENFORCED
                )
                WITH (
                    'connector' = 'socket',
                    'hostname' = 'localhost',
                    'port' = '9999',
                    'format' = 'csv'
                )
                """);

        tEnv.executeSql(
                """
                CREATE TABLE `user` (
                    id STRING,
                    name STRING,
                    PRIMARY KEY (id) NOT ENFORCED
                )
                WITH (
                    'connector' = 'socket',
                    'hostname' = 'localhost',
                    'port' = '9998',
                    'format' = 'csv'
                )
                """);
    }
}
