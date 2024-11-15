package org.apache.flink.streaming.examples.my.cogroup;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import lombok.Data;
import lombok.val;

/*
nc –l 19998
1#a1

nc -l 19999
1#1#b1
2#1#b2
3#2#b3
 */
public class WindowJoinProcessingTimeExample {
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
                                });

        final SingleOutputStreamOperator<Order> orderDS =
                env.socketTextStream("localhost", 19999, "\n", 1000)
                        .map(
                                new RichMapFunction<String, Order>() {
                                    @Override
                                    public Order map(String value) throws Exception {
                                        String[] arr = value.split("#");
                                        return new Order(arr[0], arr[1], arr[2]);
                                    }
                                });

        val result =
                userDS.join(orderDS)
                        .where(User::getUserId)
                        .equalTo(Order::getUserId)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                        .apply(
                                (user, order) -> {
                                    user.setOrder(order);
                                    return user;
                                });
        result.print("[normal]");
        env.execute();
        env.close();
    }

    @Data
    public static class User {
        private final String userId;
        private final String desc;
        private Order order;

        public User(String userId, String desc) {
            this.userId = userId;
            this.desc = desc;
            this.order = null;
        }

        @Override
        public String toString() {
            return userId + "{desc=" + desc + "," + order;
        }
    }

    @Data
    public static class Order {
        private final String id;
        private final String userId;
        private final String desc;

        public Order(String id, String userId, String desc) {
            this.id = id;
            this.userId = userId;
            this.desc = desc;
        }

        @Override
        public String toString() {
            return "Order{id='" + id + '\'' + ", desc=" + desc + '}';
        }
    }
}
