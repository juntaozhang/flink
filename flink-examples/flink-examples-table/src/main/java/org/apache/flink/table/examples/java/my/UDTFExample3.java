package org.apache.flink.table.examples.java.my;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// fixme: discussion: FLIP-498 FLIP-313
public class UDTFExample3 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        tEnv.createTemporarySystemFunction("SplitFunction", SplitFunction.class);
        createSrcTable(tEnv);
        createSinkTable(tEnv);
        tEnv.explainSql(
                """
                 INSERT INTO MySink
                 SELECT word, length
                 FROM MyTable
                 LEFT JOIN LATERAL TABLE(SplitFunction(myField)) AS T(word, length) ON TRUE
                """);
    }

    private static void createSinkTable(TableEnvironment tEnv) {
        tEnv.executeSql(
                """
                 CREATE TABLE MySink (
                     word STRING,
                     length INT
                 )
                 WITH (
                    'connector' = 'print'
                 )
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

    //    @FunctionHint(output = @DataTypeHint("ROW<word STRING, length INT>"))
    public static class SplitFunction extends AsyncTableFunction<Row> {
        private ExecutorService executor;

        @Override
        public void open(FunctionContext context) throws Exception {
            super.open(context);
            this.executor = Executors.newSingleThreadExecutor();
        }

        public void eval(CompletableFuture<Collection<Row>> resultFuture, Object... input) {
            CompletableFuture.supplyAsync(
                            () -> {
                                List<Row> res = new ArrayList<>();
                                for (String s : input[0].toString().split(" ")) {
                                    res.add(Row.of(s, s.length()));
                                }
                                return res;
                            },
                            executor)
                    .thenAccept(resultFuture::complete);
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (executor != null) {
                executor.shutdown();
            }
        }
    }
}
