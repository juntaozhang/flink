package org.apache.flink.streaming.examples.my.cogroup;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HeartbeatManagerOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import lombok.Data;
import lombok.val;
import org.apache.commons.lang3.StringUtils;

/*
nc –lk 19998
1,1
2,1

nc -lk 19999
1,a,1
2,b,1
 */
public class ConnectedBroadcastStreamExample {
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
                                });

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
                                });

        OutputTag<Pojo> earlyTag = new OutputTag<Pojo>("early") {};

        MapStateDescriptor<String, KV> stateDescriptor =
                new MapStateDescriptor<>("kvState", String.class, KV.class);
        val result =
                stream.keyBy(Pojo::getKey)
                        .connect(kv.broadcast(stateDescriptor))
                        .process(
                                new KeyedBroadcastProcessFunction<String, Pojo, KV, Pojo>() {
                                    @Override
                                    public void processElement(
                                            Pojo pojo,
                                            KeyedBroadcastProcessFunction<String, Pojo, KV, Pojo>
                                                            .ReadOnlyContext
                                                    ctx,
                                            Collector<Pojo> out)
                                            throws Exception {
                                        KV kv =
                                                ctx.getBroadcastState(stateDescriptor)
                                                        .get(pojo.key);
                                        if (kv != null) {
                                            pojo.setValue(kv.value);
                                            out.collect(pojo);
                                        } else {
                                            ctx.output(earlyTag, pojo);
                                        }
                                    }

                                    @Override
                                    public void processBroadcastElement(
                                            KV value,
                                            KeyedBroadcastProcessFunction<String, Pojo, KV, Pojo>
                                                            .Context
                                                    ctx,
                                            Collector<Pojo> out)
                                            throws Exception {
                                        ctx.getBroadcastState(stateDescriptor)
                                                .put(value.key, value);
                                    }
                                })
                        .setParallelism(2);
        result.print();
        result.getSideOutput(earlyTag).print("[early]");

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
