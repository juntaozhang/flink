package org.apache.flink.table.examples.java.my;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.AsyncScalarFunction;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.table.examples.java.my.TableUtils.createSocketTable;

public class UDFExample {
    public static void main(String[] args) {
        final Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        // configuration.set(ExecutionConfigOptions.TABLE_EXEC_ASYNC_SCALAR_BUFFER_CAPACITY, 1);
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .withConfiguration(configuration)
                        .build();
        final TableEnvironment tEnv = TableEnvironment.create(settings);
        // testSync(tEnv);
        testAsync(tEnv);
    }

    public static void testAsync(final TableEnvironment tEnv) {
        tEnv.createTemporarySystemFunction("MyLength", MyLengthAsyncFunction.class);

        tEnv.executeSql(
                """
                  CREATE TABLE MySink (
                    a STRING,
                    b INT
                  )
                  WITH ('connector' = 'print')
                """);
        //        tEnv.executeSql("""
        //                INSERT INTO MySink
        //                SELECT a, MyLength(a)
        //                from (
        //                    VALUES
        //                      ('Hello world'),
        //                      ('Flink SQL'),
        //                      ('Apache Flink')
        //                ) AS tt(a)
        //                """);
        createSocketTable(tEnv, 9999);
        //        createKafkaTable(tEnv, "test-input");
        tEnv.executeSql(
                """
                INSERT INTO MySink
                SELECT myField AS a, MyLength(myField) AS b
                FROM MyTable
                """);
    }

    public static void testSync(final TableEnvironment tEnv) {
        tEnv.createTemporarySystemFunction("MyLength", MyLengthFunction.class);
        tEnv.executeSql(
                """
                  CREATE TABLE MySink (
                    a STRING,
                    b INT
                  )
                  WITH ('connector' = 'print')
                """);

        // tEnv.executeSql("""
        //         INSERT INTO MySink VALUES
        //             ('Hello world',1),
        //             ('Flink SQL',2),
        //             ('Apache Flink',3)
        //         """);

        tEnv.executeSql(
                """
                INSERT INTO MySink
                SELECT a, MyLength(a)
                from (
                    VALUES
                      ('Hello world'),
                      ('Flink SQL'),
                      ('Apache Flink')
                ) AS tt(a) -- table alias, add column name
                """);
    }

    public static class MyLengthFunction extends ScalarFunction {
        public int eval(String s) {
            return s.length();
        }
    }

    public static class MyLengthAsyncFunction extends AsyncScalarFunction {
        public void eval(CompletableFuture<Integer> future, String s) {
            future.complete(s.length());
        }
    }
}
