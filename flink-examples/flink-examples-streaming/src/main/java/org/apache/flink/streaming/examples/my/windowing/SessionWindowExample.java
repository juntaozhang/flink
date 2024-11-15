package org.apache.flink.streaming.examples.my.windowing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import lombok.Data;

import java.util.List;

/*
nc –l 19998
1,100
 */
public class SessionWindowExample {
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

        final SingleOutputStreamOperator<Pojo> userDS =
                env.socketTextStream("localhost", 19998, "\n", 1000)
                        .name("19998")
                        .map(
                                new RichMapFunction<String, Pojo>() {
                                    @Override
                                    public Pojo map(String value) throws Exception {
                                        String[] arr = value.split(",");
                                        return new Pojo(arr[0], Long.parseLong(arr[1]));
                                    }
                                })
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Pojo>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (event, timestamp) -> event.getEventTime()));

        userDS.keyBy(Pojo::getId)
                .window(EventTimeSessionWindows.withGap(Time.milliseconds(10)))
                .reduce(
                        (value1, value2) -> {
                            value1.getTracking().addAll(value2.getTracking());
                            return value1;
                        })
                .print("[normal]");

        env.execute();
        env.close();
    }

    @Data
    public static class Pojo {
        private final String id;
        private final long eventTime;
        private List<Long> tracking;

        public Pojo(String userId, long eventTime) {
            this.id = userId;
            this.eventTime = eventTime;
            this.tracking = Lists.newArrayList(eventTime);
        }

        @Override
        public String toString() {
            return "Pojo{"
                    + "id='"
                    + id
                    + '\''
                    + ", eventTime="
                    + eventTime
                    + ", tracking="
                    + Joiner.on(",").join(tracking)
                    + '}';
        }
    }
}
