package org.apache.flink.streaming.examples;

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
import org.apache.flink.util.OutputTag;

import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.commons.lang3.time.DateUtils;

import java.text.ParseException;
import java.time.Duration;
import java.util.List;


/*
    nc -l 19999
    1#2021-05-08 11:29:01
    7#2021-05-08 11:29:07
    nc –l 19998
    13#2021-05-08 11:29:01
 */
public class WindowWatermarkExample {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 16);
        conf.setLong(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 600_000);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setParallelism(2);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.enableCheckpointing(60_000);
  	    env.getCheckpointConfig().setCheckpointStorage("file:///Users/juntzhang/src/juntzhang/flink/flink-examples/flink-examples-streaming/checkpoints/StreamingWindowWatermark");
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10,
                org.apache.flink.api.common.time.Time.seconds(10)));
        OutputTag<User> lateDataTag = new OutputTag<User>("late") {
        };

        final SingleOutputStreamOperator<User> a = env
                .socketTextStream("localhost", 19999, "\n", 1000)
                .map(new RichMapFunction<String, User>() {
                    @Override
                    public User map(String value) throws Exception {
                        String[] arr = value.split("#");
                        return new User(arr[0], arr[1]);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<User>forMonotonousTimestamps()
//                                .<User>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, timestamp) -> event.getEventTime())
                                .withIdleness(Duration.ofSeconds(10))
                                // todo 两个数据源，一个快，一个慢，watermarkGroup设置相同，看看是否会对齐
                                //.withWatermarkAlignment("my-watermark-group", Duration.ofSeconds(5))
                );

        final SingleOutputStreamOperator<User> b = a.keyBy(User::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //.allowedLateness(Time.seconds(2))
                .sideOutputLateData(lateDataTag)
                .aggregate(new AggregateFunction<User, User, User>() {
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
                });
        // b.getSideOutput(lateDataTag).print("[late]");
        b.print("[normal]");


        env.execute();
        env.close();
    }

    @Data
    static class User {
        private String id;
        private List<String> tracking;
        private long eventTime;


        public User(String id, String eventTime) throws ParseException {
            this.id = id;
            this.eventTime = DateUtils
                    .parseDate(eventTime, "yyyy-MM-dd HH:mm:ss")
                    .getTime();
            this.tracking = Lists.newArrayList(getEventTimeStr());
        }

        public String getEventTimeStr() {
            return DateFormatUtils.format(eventTime, "HH:mm:ss");
        }

        @Override
        public String toString() {
            return id + "{eventTime=" + getEventTimeStr() + ", tracking=" + StringUtils.join(tracking, "|") + "}";
        }
    }
}

