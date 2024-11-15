package org.apache.flink.streaming.examples.my.cogroup;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;

/*
nc –lk 19998
1,1
2,1

nc -lk 19999
1,a,1
2,b,1
 */
public class ConnectedStreamWithOnTimerExample {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 16);
        conf.setLong(HeartbeatManagerOptions.HEARTBEAT_TIMEOUT, 600_000);
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        env.setStateBackend(new EmbeddedRocksDBStateBackend());
        env.setParallelism(1);
        //        env.enableCheckpointing(10_000);
        //        env
        //                .getCheckpointConfig()
        //                .setCheckpointStorage(Paths
        //                        .get("checkpoints/" +
        // ConnectedStreamExample2.class.getSimpleName())
        //                        .toUri()
        //                        .toString());
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        10, org.apache.flink.api.common.time.Time.seconds(10)));
        final SingleOutputStreamOperator<Pojo> stream =
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

        final SingleOutputStreamOperator<KV> kv =
                env.socketTextStream("localhost", 19999, "\n", 1000)
                        .name("19999")
                        .filter(StringUtils::isNotBlank)
                        .map(
                                new RichMapFunction<String, KV>() {
                                    @Override
                                    public KV map(String value) {
                                        String[] arr = value.split(",");
                                        return new KV(arr[0], arr[1], arr[2]);
                                    }
                                })
                        .assignTimestampsAndWatermarks(
                                WatermarkStrategy.<KV>forMonotonousTimestamps()
                                        .withTimestampAssigner(
                                                (event, timestamp) ->
                                                        event.getEventTime())); // 如果kv
        // 没有watermark，不会触发AbstractStreamOperator.processWatermark

        stream.keyBy(Pojo::getKey)
                .connect(kv.keyBy(KV::getKey))
                .process(
                        new KeyedCoProcessFunction<String, Pojo, KV, Pojo>() {
                            private transient ValueState<String> state;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                ValueStateDescriptor<String> stateDescriptor =
                                        new ValueStateDescriptor<>("state", Types.STRING);
                                //                        stateDescriptor.enableTimeToLive(
                                //                                StateTtlConfig
                                //
                                // .newBuilder(Time.seconds(10))
                                //
                                // .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                //
                                // .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                                //                                        .build()
                                //                        );
                                state = getRuntimeContext().getState(stateDescriptor);
                            }

                            @Override
                            public void processElement1(
                                    Pojo pojo,
                                    KeyedCoProcessFunction<String, Pojo, KV, Pojo>.Context ctx,
                                    Collector<Pojo> out)
                                    throws Exception {
                                pojo.setValue(state.value());
                                out.collect(pojo);
                                // if 6 <= kv.timestamp, then trigger onTimer @
                                // InternalTimerServiceImpl.advanceWatermark
                                // 如果没有registerEventTimeTimer 则不会触发
                                ctx.timerService().registerEventTimeTimer(1);
                                ctx.timerService()
                                        .registerEventTimeTimer(2); // 触发了第2次， 如果相同的ts不会重复触发
                            }

                            @Override
                            public void processElement2(
                                    KV kv,
                                    KeyedCoProcessFunction<String, Pojo, KV, Pojo>.Context ctx,
                                    Collector<Pojo> out)
                                    throws Exception {
                                state.update(kv.value);
                                //
                                // ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 1);
                            }

                            @Override
                            public void onTimer(
                                    long timestamp,
                                    KeyedCoProcessFunction<String, Pojo, KV, Pojo>.OnTimerContext
                                            ctx,
                                    Collector<Pojo> out)
                                    throws Exception {
                                System.out.printf(
                                        "onTimer trigger => timestamp:%s key:%s value:%s%n",
                                        timestamp, ctx.getCurrentKey(), state.value());
                            }
                        })
                .print();

        env.execute();
        env.close();
    }

    @Data
    public static class Pojo {
        private final String key;
        private final long eventTime;
        private String value;

        public Pojo(String key, Long eventTime) {
            this.key = key;
            this.value = null;
            this.eventTime = eventTime;
        }
    }

    @Data
    public static class KV {
        private final String key;
        private final String value;
        private final long eventTime;

        public KV(String key, String value, String eventTime) {
            this.key = key;
            this.value = value;
            this.eventTime = Long.parseLong(eventTime);
        }
    }
}
