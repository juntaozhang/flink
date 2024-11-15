package org.apache.flink.streaming.examples.my.cogroup;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/*
nc –lk 19998
1#1

nc -lk 19999
1#1
 */
public class Window3JoinExample {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 16);
        conf.setLong(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 600_000);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.setParallelism(1);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        10, org.apache.flink.api.common.time.Time.seconds(10)));
        final SingleOutputStreamOperator<User> userDS =
                env.socketTextStream("localhost", 19998, "\n", 1000)
                        .name("19998")
                        .filter(StringUtils::isNotBlank)
                        .map(
                                new RichMapFunction<String, User>() {
                                    @Override
                                    public User map(String value) throws Exception {
                                        String[] arr = value.split(",");
                                        return new User(arr[0], Long.parseLong(arr[1]));
                                    }
                                })
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<User>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (event, timestamp) -> event.getEventTime()));

        final SingleOutputStreamOperator<Order> orderDS =
                env.socketTextStream("localhost", 19999, "\n", 1000)
                        .name("19999")
                        .filter(StringUtils::isNotBlank)
                        .map(
                                new RichMapFunction<String, Order>() {
                                    @Override
                                    public Order map(String value) throws Exception {
                                        String[] arr = value.split(",");
                                        return new Order(arr[0], Long.parseLong(arr[1]));
                                    }
                                })
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Order>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (event, timestamp) -> event.getEventTime()));

        final SingleOutputStreamOperator<Payment> paymentDS =
                env.socketTextStream("localhost", 19997, "\n", 1000)
                        .name("19997")
                        .filter(StringUtils::isNotBlank)
                        .map(
                                new RichMapFunction<String, Payment>() {
                                    @Override
                                    public Payment map(String value) throws Exception {
                                        String[] arr = value.split(",");
                                        return new Payment(arr[0], Long.parseLong(arr[1]));
                                    }
                                })
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Payment>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (event, timestamp) -> event.getEventTime()));

        userDS.coGroup(orderDS)
                .where(User::getUserId)
                .equalTo(Order::getUserId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                .apply(
                        new CoGroupFunction<User, Order, User>() {
                            @Override
                            public void coGroup(
                                    Iterable<User> users,
                                    Iterable<Order> orders,
                                    Collector<User> out) {
                                for (User user : users) {
                                    user.orders.addAll(Lists.newArrayList(orders));
                                    out.collect(user);
                                }
                            }
                        })
                .coGroup(paymentDS)
                .where(User::getUserId)
                .equalTo(Payment::getUserId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                .apply(
                        new CoGroupFunction<User, Payment, User>() {
                            @Override
                            public void coGroup(
                                    Iterable<User> users,
                                    Iterable<Payment> payments,
                                    Collector<User> out) {
                                for (User user : users) {
                                    user.payments.addAll(Lists.newArrayList(payments));
                                    out.collect(user);
                                }
                            }
                        })
                .print("[coGroup]");

        env.execute();
        env.close();
    }

    @Data
    public static class User {
        private final String userId;
        private final long eventTime;
        private final List<Order> orders;
        private final List<Payment> payments;

        public User(String userId, Long eventTime) {
            this.userId = userId;
            this.eventTime = eventTime;
            this.orders = Lists.newArrayList();
            this.payments = Lists.newArrayList();
        }

        @Override
        public String toString() {
            return userId
                    + "{eventTime="
                    + eventTime
                    + ", orders=["
                    + StringUtils.join(orders, "|")
                    + "]}"
                    + ", payments=["
                    + StringUtils.join(payments, "|")
                    + "]}";
        }
    }

    @Data
    public static class Order {
        private final String userId;
        private final long eventTime;

        public Order(String userId, Long eventTime) {
            this.userId = userId;
            this.eventTime = eventTime;
        }
    }

    @Data
    public static class Payment {
        private final String userId;
        private final long eventTime;

        public Payment(String userId, Long eventTime) {
            this.userId = userId;
            this.eventTime = eventTime;
        }
    }
}
