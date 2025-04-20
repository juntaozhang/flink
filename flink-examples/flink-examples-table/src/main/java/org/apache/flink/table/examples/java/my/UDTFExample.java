package org.apache.flink.table.examples.java.my;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 *
 *
 * <pre>
 *  nc -lk 9999
 *  Hello world
 *  Apache Flink
 *  Flink SQL
 * </pre>
 */
public class UDTFExample {
    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class SplitFunction extends TableFunction<Row> {

        public void eval(String str) {
            for (String s : str.split(" ")) {
                // use collect(...) to emit a row
                collect(Row.of(s, s.length()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);

        createTable(tEnv);

        final Table result =
                tEnv.sqlQuery(
                        """
                 SELECT myField, newWord, newLength
                 FROM MyTable
                 LEFT JOIN LATERAL TABLE(SplitFunction(myField)) AS T(newWord, newLength) ON TRUE
                """);

        tEnv.toChangelogStream(result).print();

        env.execute();
    }

    private static void createTable(StreamTableEnvironment tEnv) {
        String hostname = "localhost";
        int port = 9999;
        tEnv.executeSql(
                "CREATE TABLE MyTable (myField STRING) "
                        + "WITH ("
                        + "'connector' = 'socket', "
                        + "'hostname' = '"
                        + hostname
                        + "', "
                        + "'port' = '"
                        + port
                        + "', "
                        + "'format' = 'csv'"
                        + ")");
    }
}
