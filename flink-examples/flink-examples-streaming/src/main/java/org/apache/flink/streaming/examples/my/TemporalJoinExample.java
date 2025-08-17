package org.apache.flink.streaming.examples.my;

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
104,USD,300,4
105,USD,400,20

nc -lk 9999
USD,6.50,0
USD,6.60,4
USD,6.70,8
USD,6.80,15
 */
public class TemporalJoinExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        WatermarkStrategy<OrderEvent> wmOrders = WatermarkStrategy
                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<OrderEvent>) (e, ts) -> e.ts);

        WatermarkStrategy<RateEvent> wmRates = WatermarkStrategy
                .<RateEvent>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner((SerializableTimestampAssigner<RateEvent>) (e, ts) -> e.ts);

        // ----------- Socket Sources -----------
        DataStream<String> ordersLines = env.socketTextStream("localhost", 9998);
        DataStream<String> ratesLines = env.socketTextStream("localhost", 9999);

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

        DataStream<RateEvent> rates = ratesLines
                .filter(s -> s != null && !s.trim().isEmpty())
                .map(line -> {
                    // 格式: currency,rate,timestampSeconds
                    String[] f = line.trim().split("\\s*,\\s*");
                    String currency = f[0];
                    double rate = Double.parseDouble(f[1]);
                    long tsMillis = Long.parseLong(f[2]) * 1000;
                    return new RateEvent(currency, rate, tsMillis);
                })
                .assignTimestampsAndWatermarks(wmRates);

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
                        .build()
        );
        tEnv.createTemporaryView("Orders", ordersT);

        Table ratesT = tEnv.fromDataStream(
                rates,
                Schema.newBuilder()
                        .column("currency", DataTypes.STRING().notNull())
                        .column("rate", DataTypes.DOUBLE())
                        .column("ts", DataTypes.BIGINT())
                        .columnByExpression("rt", "TO_TIMESTAMP_LTZ(ts, 3)")
                        .watermark("rt", "rt - INTERVAL '2' SECOND")
                        // Temporal Join 需要右表有主键（NOT ENFORCED）
                        .primaryKey("currency")
                        .build()
        );
        tEnv.createTemporaryView("Rates", ratesT);

        // ----------- Temporal Table Join（RowTime）-----------
        String sql =
                "SELECT " +
                        "  o.orderId, o.currency, o.price, " +
                        "  r.rate, " +
                        "  o.rt AS order_time, " +
                        "  CAST(o.price * r.rate AS DOUBLE) AS cny " +
                        "FROM Orders o " +
                        "LEFT JOIN Rates FOR SYSTEM_TIME AS OF o.rt AS r " +
                        "ON o.currency = r.currency";

        Table result = tEnv.sqlQuery(sql);

        // 把结果回到 DataStream 打印
        tEnv.toChangelogStream(result).print("RESULT");

        env.execute("Temporal RowTime Join via Socket");
    }

    public static class RateEvent {
        public String currency;
        public double rate;
        public long ts; // 原始秒 → 转换后毫秒

        public RateEvent() {
        }

        public RateEvent(String currency, double rate, long ts) {
            this.currency = currency;
            this.rate = rate;
            this.ts = ts;
        }
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
