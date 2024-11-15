package org.apache.flink.streaming.examples.my.windowing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.List;

/*
test GlobalWindows Custom Trigger

nc –l 19998
1,100
 */
public class GlobalWindowExample {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 16);
        conf.setLong(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 600_000);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setStateBackend(new HashMapStateBackend());
        env.setParallelism(1);
        env.enableCheckpointing(10_000);
        env.getCheckpointConfig()
                .setCheckpointStorage(
                        Paths.get("checkpoints/" + GlobalWindowExample.class.getSimpleName())
                                .toUri()
                                .toString());
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        10, org.apache.flink.api.common.time.Time.seconds(10)));

        final SingleOutputStreamOperator<Pojo> userDS =
                env.socketTextStream("localhost", 19998, "\n", 1000)
                        .name("19998")
                        .filter(StringUtils::isNotBlank)
                        .map(
                                new RichMapFunction<String, Pojo>() {
                                    @Override
                                    public Pojo map(String value) {
                                        String[] arr = value.split(",");
                                        return new Pojo(arr[0], Long.parseLong(arr[1]));
                                    }
                                })
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<Pojo>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (event, timestamp) -> event.getEventTime()));

        userDS.keyBy(
                        new KeySelector<Pojo, String>() {
                            @Override
                            public String getKey(Pojo value) {
                                return value.getId();
                            }
                        })
                .window(GlobalWindows.create())
                .trigger(new AlwaysTrigger())
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
    public static class Pojo implements Serializable {
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

    private static class Sum implements ReduceFunction<Long> {
        private static final long serialVersionUID = 1L;

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }

    private static class AlwaysTrigger extends Trigger<Pojo, GlobalWindow> {
        private static final long serialVersionUID = 1L;
        private final ReducingStateDescriptor<Long> stateDesc =
                new ReducingStateDescriptor<>("count", new Sum(), LongSerializer.INSTANCE);

        @Override
        public TriggerResult onElement(
                Pojo element, long timestamp, GlobalWindow window, TriggerContext ctx) {
            ReducingState<Long> count = ctx.getPartitionedState(stateDesc);
            try {
                count.add(1L);
                if (count.get() >= 5) {
                    count.clear();
                    return TriggerResult.FIRE_AND_PURGE; // 这里会清除state
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {}

        @Override
        public void onMerge(GlobalWindow window, OnMergeContext ctx) {}
    }
}
