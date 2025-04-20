package org.apache.flink.table.examples.java.my;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class UDTFExample2 {
    public static void main(String[] args) throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(CoreOptions.DEFAULT_PARALLELISM, 1);
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance()
                        .inStreamingMode()
                        .withConfiguration(configuration)
                        .build();
        final TableEnvironment env = TableEnvironment.create(settings);
        // env.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        env.executeSql("create  function SplitFunction as '" + SplitFunction.class.getName() + "'");
        createSrcTable(env);
        createSinkTable(env);

        env.executeSql(
                """
                INSERT INTO MySink
                SELECT newWord, SUM(newLength) AS newLength
                FROM (
                    SELECT newWord, newLength
                    FROM MyTable
                    LEFT JOIN LATERAL TABLE(SplitFunction(myField)) AS T(newWord, newLength) ON TRUE
                ) t
                group by newWord
                """);
    }

    private static void createSinkTable(TableEnvironment tEnv) {
        tEnv.executeSql(
                """
                  CREATE TABLE MySink (
                    newWord STRING,
                    newLength INT
                  )
                  WITH ('connector' = 'print')
                """);
    }

    private static void createSrcTable(TableEnvironment tEnv) {
        String hostname = "localhost";
        int port = 9999;
        tEnv.executeSql(
                """
                CREATE TABLE MyTable (myField STRING)
                WITH (
                  'connector' = 'socket',
                  'hostname' = '%s',
                  'port' = '%d',
                  'format' = 'csv'
                )
                """
                        .formatted(hostname, port));
    }

    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class SplitFunction extends TableFunction<Row> {

        public void eval(String str) {
            for (String s : str.split(" ")) {
                collect(Row.of(s, s.length()));
            }
        }
    }
}
