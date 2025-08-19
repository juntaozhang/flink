package org.apache.flink.streaming.examples.my.windowing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import com.google.common.collect.Lists;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;

/*
-Dparallelism.default=3

nc -kl 19999
1,1
 */
@Slf4j
public class WindowWatermarkExample {
    public static void main(String[] args) throws Exception {
        new WindowWatermarkExample().run(args);
    }

    public void run(String[] args) throws Exception {
        log.info("args ==> {}", StringUtils.join(args, ","));
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
        Configuration conf = new Configuration();
        conf.setInteger(RestOptions.PORT, 8082);
        conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 16);
        conf.setLong(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 600_000);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(1);
        env.enableCheckpointing(30_000);
        env.getCheckpointConfig()
                .setCheckpointStorage(
                        Paths.get("/tmp/" + WindowWatermarkExample.class.getSimpleName())
                                .toUri()
                                .toString());

//        env.getCheckpointConfig().enableUnalignedCheckpoints();
//        env.getCheckpointConfig()
//                .setExternalizedCheckpointCleanup(
        //
        // CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //        env.setRestartStrategy(
        //                RestartStrategies.fixedDelayRestart(
        //                        10, org.apache.flink.api.common.time.Time.seconds(10)));

        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        OutputTag<User> lateDataTag = new OutputTag<User>("late") {
        };

        final SingleOutputStreamOperator<User> a =
                env.socketTextStream("localhost", 19999, "\n", 1000)
                        .map(
                                new RichMapFunction<String, User>() {
                                    @Override
                                    public User map(String value) throws Exception {
                                        String[] arr = value.split(",");
                                        return new User(arr[0], arr[1]);
                                    }
                                })
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy
//                                         .<User>forMonotonousTimestamps()
                                         .<User>forBoundedOutOfOrderness(Duration.ofMillis(2))
                                        .withTimestampAssigner(
                                                (event, timestamp) -> event.getEventTime())
//                                        .withIdleness(Duration.ofMillis(10)) // 用于处理空闲输入的特性。它主要用于解决在多并行度数据流中，某些分区或数据源在一段时间内没有数据流入时，导致 Watermark 无法正常推进的问题
                                // todo 两个数据源，一个快，一个慢，watermarkGroup设置相同，看看是否会对齐
                                // .withWatermarkAlignment("my-watermark-group", Duration.ofMillis(5))
                        );

        final SingleOutputStreamOperator<User> b =
                a.keyBy(User::getId)
                        .window(TumblingEventTimeWindows.of(Time.milliseconds(5)))
//                         .allowedLateness(Time.seconds(2))
                        .sideOutputLateData(lateDataTag)
                        .aggregate(
                                new AggregateFunction<User, User, User>() {
                                    @Override
                                    public User createAccumulator() {
                                        return null;
                                    }

                                    @Override
                                    public User add(User u1, User u2) {
                                        return merge0(u1, u2);
                                    }

                                    public User merge0(User u1, User u2) {
                                        if (u1 == null && u2 == null) {
                                            return null;
                                        } else if (u1 == null) {
                                            return u2;
                                        } else if (u2 == null) {
                                            return u1;
                                        } else {
                                            if (u1.getEventTime() < u2.getEventTime()) {
                                                u1.setEventTime(u2.getEventTime());
                                            }
                                            u1.tracking.addAll(u2.tracking);
                                            return u1;
                                        }
                                    }

                                    @Override
                                    public User getResult(User accumulator) {
                                        return accumulator;
                                    }

                                    @Override
                                    public User merge(User u1, User u2) {
                                        return merge0(u1, u2);
                                    }
                                })
                        .setParallelism(2);
        // b.getSideOutput(lateDataTag).print("[late]");
        b.print("[normal]");

        env.execute();
        env.close();
    }

    @Data
    public static class User {
        private String id;
        private long eventTime;
        private List<Long> tracking;

        public User(String id, String eventTime) {
            this.id = id;
            this.eventTime = Long.parseLong(eventTime);
            this.tracking = Lists.newArrayList(this.eventTime);
        }

        @Override
        public String toString() {
            return id
                    + "{eventTime="
                    + eventTime
                    + ", tracking="
                    + StringUtils.join(tracking, "|")
                    + "}";
        }
    }
}
