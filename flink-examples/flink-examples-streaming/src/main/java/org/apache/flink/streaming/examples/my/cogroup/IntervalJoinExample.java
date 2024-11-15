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

import com.google.common.collect.Lists;
import lombok.Data;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.util.List;

/*
nc –l 19998
1#2021-05-08 11:10:01

nc -l 19999
1#1#2021-05-08 11:10:01
2#1#2021-05-08 11:10:03
2#1#2021-05-08 11:09:57
2#1#2021-05-08 11:10:04
2#1#2021-05-08 11:10:05
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
                env.socketTextStream("localhost", 19998, "\n", 1000)
                        .map(
                                new RichMapFunction<String, User>() {
                                    @Override
                                    public User map(String value) throws Exception {
                                        String[] arr = value.split("#");
                                        return new User(arr[0], arr[1]);
                                    }
                                })
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<User>forMonotonousTimestamps()
                                        //
                                        // .<User>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                        .withTimestampAssigner(
                                                (event, timestamp) -> event.getEventTime()));

        final SingleOutputStreamOperator<Order> orderDS =
                env.socketTextStream("localhost", 19999, "\n", 1000)
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
                        .between(Time.seconds(-2), Time.seconds(2))
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
                                        user.orders.add(order);
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
        private final List<Order> orders;

        public User(String userId, String eventTime) throws ParseException {
            this.userId = userId;
            this.eventTime = DateUtils.parseDate(eventTime, "yyyy-MM-dd HH:mm:ss").getTime();
            this.orders = Lists.newArrayList();
        }

        @Override
        public String toString() {
            return userId
                    + "{eventTime="
                    + DateFormatUtils.format(eventTime, "HH:mm:ss")
                    + ", orders=["
                    + StringUtils.join(orders, "|")
                    + "]}";
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
            this.eventTime = DateUtils.parseDate(eventTime, "yyyy-MM-dd HH:mm:ss").getTime();
        }

        @Override
        public String toString() {
            return "Order{"
                    + "id='"
                    + id
                    + '\''
                    + ", eventTime="
                    + DateFormatUtils.format(eventTime, "HH:mm:ss")
                    + '}';
        }
    }
}
