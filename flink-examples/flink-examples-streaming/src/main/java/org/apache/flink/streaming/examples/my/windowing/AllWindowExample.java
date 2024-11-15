package org.apache.flink.streaming.examples.my.windowing;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import lombok.Data;
import org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.List;

/*
test GlobalWindows Custom Trigger

nc –l 19998
1,100
 */
public class AllWindowExample {
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

        userDS.windowAll(TumblingEventTimeWindows.of(Time.milliseconds(5)))
                .apply(
                        new AllWindowFunction<Pojo, String, TimeWindow>() {
                            @Override
                            public void apply(
                                    TimeWindow window,
                                    Iterable<Pojo> values,
                                    Collector<String> out) {
                                out.collect(Joiner.on("|").join(values));
                            }
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
}
