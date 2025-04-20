package org.apache.flink.table.examples.java.my;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.ArgumentHint;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ProcessTableFunction;
import org.apache.flink.types.ColumnList;
import org.apache.flink.types.Row;

import static org.apache.flink.table.annotation.ArgumentTrait.TABLE_AS_SET;

/** Bob,12 Alice,42 Hans,38 */
public class PTFExample4 {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        tEnv.executeSql(
                """
                CREATE TABLE t (
                    name STRING,
                    score INT
                )
                WITH (
                  'connector' = 'socket',
                  'hostname' = '%s',
                  'port' = '%d',
                  'format' = 'csv'
                )
                """
                        .formatted("localhost", 9999));

        tEnv.executeSql(
                """
                  CREATE TABLE MySink (
                    name STRING
                  )
                  WITH ('connector' = 'print')
                """);
        tEnv.createTemporarySystemFunction("f", Function.class);
        tEnv.executeSql(
                """
                        insert into MySink
                        SELECT name FROM f(
                            r => TABLE t PARTITION BY name,
                            s => DESCRIPTOR(score),
                            threshold => 20
                        )
                        """);
    }

    @FunctionHint(output = @DataTypeHint("ROW<name STRING, score INT>"))
    public static class Function extends ProcessTableFunction<Row> {
        public void eval(@ArgumentHint(TABLE_AS_SET) Row r, ColumnList s, Integer threshold) {
            Integer score = r.<Integer>getFieldAs(s.getNames().get(0));
            if (score > threshold) {
                collect(r);
            }
        }
    }
}
