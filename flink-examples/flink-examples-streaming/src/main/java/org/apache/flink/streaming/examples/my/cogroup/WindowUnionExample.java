package org.apache.flink.streaming.examples.my.cogroup;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
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

import com.google.common.base.Joiner;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/*
nc –lk 19998
1#1

nc -lk 19999
1#1
 */
public class WindowUnionExample {
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
        final SingleOutputStreamOperator<User> ds1 = getUserDS(env, 19997);
        final SingleOutputStreamOperator<User> ds2 = getUserDS(env, 19998);
        final SingleOutputStreamOperator<User> ds3 = getUserDS(env, 19999);

        ds1.union(ds2, ds3)
                .keyBy(User::getUserId)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                .aggregate(
                        new AggregateFunction<User, Users, Users>() {
                            @Override
                            public Users createAccumulator() {
                                return new Users();
                            }

                            @Override
                            public Users add(User value, Users accumulator) {
                                return accumulator.add(value);
                            }

                            @Override
                            public Users getResult(Users accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Users merge(Users a, Users b) {
                                return a.addAll(b);
                            }
                        })
                .print();

        env.execute();
        env.close();
    }

    private static SingleOutputStreamOperator<User> getUserDS(
            StreamExecutionEnvironment env, int port) {
        return env.socketTextStream("localhost", port, "\n", 1000)
                .name(String.valueOf(port))
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
                                .withTimestampAssigner((event, timestamp) -> event.getEventTime())
                        //                                .withIdleness(Duration.ofSeconds(10))
                        );
    }

    @Data
    public static class Users {
        private List<User> list = new ArrayList<>();

        public Users add(User user) {
            list.add(user);
            return this;
        }

        public Users addAll(Users users) {
            list.addAll(users.list);
            return this;
        }

        @Override
        public String toString() {
            return "Users{" + Joiner.on("|").join(list) + '}';
        }
    }

    @Data
    public static class User {
        private final String userId;
        private final long eventTime;

        public User(String userId, Long eventTime) {
            this.userId = userId;
            this.eventTime = eventTime;
        }
    }
}
