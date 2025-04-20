package org.apache.flink.table.examples.java.my;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Arrays;

public class TableUtils {
    public static void createTable(StreamExecutionEnvironment env, StreamTableEnvironment tEnv) {
        // Create a sample data stream
        final DataStream<Row> myDataStream =
                env.fromData(
                        Arrays.asList(
                                Row.of("Hello world"),
                                Row.of("Apache Flink"),
                                Row.of("Flink SQL")));

        // Define the schema for the data stream
        final Schema schema = Schema.newBuilder().column("myField", DataTypes.STRING()).build();

        // Register the data stream as a table
        tEnv.createTemporaryView("MyTable", myDataStream, schema);
    }

    // only support in TEST testLookupJoinTableWithColumnarStorage
    //    public static void createValuesTable(TableEnvironment tEnv) {
    //        tEnv.executeSql(
    //                "CREATE TABLE MyTable (myField STRING) WITH ('connector' = 'values', 'data-id'
    // = 'data')");
    //        tEnv.executeSql("""
    //                INSERT INTO MyTable VALUES
    //                ('Hello world'),
    //                ('Apache Flink'),
    //                ('Flink SQL')
    //                """);
    //    }

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
    public static void createSocketTable(TableEnvironment tEnv, int port) {
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
                        .formatted("localhost", port));
    }

    public static void createKafkaTable(TableEnvironment tEnv, String topic) {
        tEnv.executeSql(
                """
            CREATE TABLE MyTable (
                myField STRING
            ) WITH (
                'connector' = 'kafka',
                'topic' = '%s',
                'properties.bootstrap.servers' = 'localhost:29092,localhost:39092,localhost:49092',
                'format' = 'csv',
                'scan.startup.mode' = 'latest-offset'
            )
            """
                        .formatted(topic));
    }
}
