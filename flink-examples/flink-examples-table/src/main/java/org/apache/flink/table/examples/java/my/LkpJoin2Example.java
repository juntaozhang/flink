package org.apache.flink.table.examples.java.my;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/*
nc -lk 9998
101,USD,100,5
102,USD,200,3
103,USD,300,12
104,USD,400,20

CREATE TABLE IF NOT EXISTS rates (
  currency TEXT PRIMARY KEY,
  rate DOUBLE PRECISION
);

INSERT INTO rates(currency, rate) VALUES
    ('USD', 6.5),
    ('UK', 8.4);

UPDATE rates SET rate = 6.6 WHERE currency = 'USD';
UPDATE rates SET rate = 6.7 WHERE currency = 'USD';
*/
public class LkpJoin2Example {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<OrderEvent> wmOrders = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<OrderEvent>) (e, ts) -> e.ts);

        // ----------- Socket Sources -----------
        DataStream<String> ordersLines = env.socketTextStream("localhost", 9998);

        DataStream<OrderEvent> orders = ordersLines
                .filter(s -> s != null && !s.trim().isEmpty())
                .map(line -> {
                    // 格式: orderId,currency,price,timestampSeconds
                    String[] f = line.trim().split("\\s*,\\s*");
                    String orderId = f[0];
                    String currency = f[1];
                    double price = Double.parseDouble(f[2]);
                    long tsMillis = Long.parseLong(f[3]) * 1000;
                    return new OrderEvent(orderId, currency, price, tsMillis);
                })
                .assignTimestampsAndWatermarks(wmOrders);

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
        Table ordersT = tEnv.fromDataStream(
                orders,
                Schema.newBuilder()
                        .column("orderId", DataTypes.STRING())
                        .column("currency", DataTypes.STRING().notNull())
                        .column("price", DataTypes.DOUBLE())
                        .column("ts", DataTypes.BIGINT())
                        // 由 ts(毫秒) 派生 RowTime 字段 rt
                        .columnByExpression("rt", "TO_TIMESTAMP_LTZ(ts, 3)")
                        .watermark("rt", "rt - INTERVAL '2' SECOND")
                        .columnByExpression("pt", "PROCTIME()")
                        .build()
        );
        tEnv.createTemporaryView("Orders", ordersT);

        tEnv.executeSql("CREATE TABLE Rates ( "
                + "currency STRING, "
                + "rate DOUBLE, "
                + "PRIMARY KEY (currency) NOT ENFORCED ) "
                + "WITH ( "
                + "'connector' = 'jdbc', "
                + "'url' = 'jdbc:postgresql://localhost:5432/postgres', "
                + "'table-name' = 'rates', "
                + "'username' = 'postgres', "
                + "'password' = 'root' )");

        // ----------- Lookup Join -----------
        String sql =
                "SELECT " +
                        "  o.orderId, o.currency, o.price, " +
                        "  r.rate, " +
                        "  o.rt AS order_time, " +
                        "  CAST(o.price * r.rate AS DOUBLE) AS cny " +
                        "FROM Orders o " +
                        "LEFT JOIN Rates FOR SYSTEM_TIME AS OF o.pt AS r " +
                        "ON o.currency = r.currency";

        Table result = tEnv.sqlQuery(sql);

        tEnv.toChangelogStream(result).print("RESULT");

        env.execute("Lookup Join");
    }

    public static class OrderEvent {
        public String orderId;
        public String currency;
        public double price;
        public long ts; // 原始秒 → 转换后毫秒

        public OrderEvent() {
        }

        public OrderEvent(String orderId, String currency, double price, long ts) {
            this.orderId = orderId;
            this.currency = currency;
            this.price = price;
            this.ts = ts;
        }
    }
}
