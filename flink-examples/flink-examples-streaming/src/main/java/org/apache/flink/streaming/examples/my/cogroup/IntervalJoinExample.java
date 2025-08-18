package org.apache.flink.streaming.examples.my.cogroup;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import lombok.Data;
import lombok.val;

import java.text.ParseException;
import java.time.Duration;

/*
nc –l 9998
1#1

nc -l 9999
1#1#1
2#1#3
3#1#7
4#1#4
5#1#5
 */
public class IntervalJoinExample {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 16);
        conf.setLong(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 600_000);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.setParallelism(1);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        10, org.apache.flink.api.common.time.Time.seconds(10)));
        OutputTag<User> userLateTag = new OutputTag<User>("late-user") {};
        OutputTag<Order> orderLateTag = new OutputTag<Order>("late-order") {};

        final SingleOutputStreamOperator<User> userDS =
                env.socketTextStream("localhost", 9998, "\n", 1000)
                        .map(
                                new RichMapFunction<String, User>() {
                                    @Override
                                    public User map(String value) throws Exception {
                                        String[] arr = value.split("#");
                                        return new User(arr[0], arr[1]);
                                    }
                                })
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
                                        //.<User>forMonotonousTimestamps()
                                         .<User>forBoundedOutOfOrderness(Duration.ofMillis(5))
                                        .withTimestampAssigner(
                                                (event, timestamp) -> event.getEventTime()));

        final SingleOutputStreamOperator<Order> orderDS =
                env.socketTextStream("localhost", 9999, "\n", 1000)
                        .map(
                                new RichMapFunction<String, Order>() {
                                    @Override
                                    public Order map(String value) throws Exception {
                                        String[] arr = value.split("#");
                                        return new Order(arr[0], arr[1], arr[2]);
                                    }
                                })
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Order>forMonotonousTimestamps()
                                        //
                                        // .<Order>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                        .withTimestampAssigner(
                                                (event, timestamp) -> event.getEventTime()));

        val result =
                userDS.keyBy(User::getUserId)
                        .intervalJoin(orderDS.keyBy(Order::getUserId))
                        .between(Time.milliseconds(-1), Time.milliseconds(2))
                        .sideOutputLeftLateData(userLateTag)
                        .sideOutputRightLateData(orderLateTag)
                        .process(
                                new ProcessJoinFunction<User, Order, User>() {
                                    @Override
                                    public void processElement(
                                            User user,
                                            Order order,
                                            Context context,
                                            Collector<User> collector) {
                                        user.setOrder(order);
                                        collector.collect(user);
                                    }
                                });
        result.print("[normal]");
        result.getSideOutput(userLateTag).print("[late][user]");
        result.getSideOutput(orderLateTag).print("[late][order]");
        env.execute();
        env.close();
    }

    @Data
    public static class User {
        private final String userId;
        private final long eventTime;
        private Order order;

        public User(String userId, String eventTime) throws ParseException {
            this.userId = userId;
            this.eventTime = Long.parseLong(eventTime);
        }

        @Override
        public String toString() {
            return "User{" +
                    "userId='" + userId + '\'' +
                    ", eventTime=" + eventTime +
                    ", " + order +
                    '}';
        }
    }

    @Data
    public static class Order {
        private final String id;
        private final String userId;
        private final long eventTime;

        public Order(String id, String userId, String eventTime) throws ParseException {
            this.id = id;
            this.userId = userId;
            this.eventTime = Long.parseLong(eventTime);
        }

        @Override
        public String toString() {
            return "Order{" +
                    "id='" + id + '\'' +
                    ", userId='" + userId + '\'' +
                    ", eventTime=" + eventTime +
                    '}';
        }
    }
}
