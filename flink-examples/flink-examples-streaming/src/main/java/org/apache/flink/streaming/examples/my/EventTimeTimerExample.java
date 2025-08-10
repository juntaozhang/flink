package org.apache.flink.streaming.examples.my;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Paths;
import java.time.Duration;

/**
 * nc -kl 19999
 * <p>
 * user1,ADD_TO_CART,1
 * user1,ADD_TO_CART,2
 * user1,PAY_ORDER,4
 * user2,ADD_TO_CART,4
 * user3,ADD_TO_CART,7
 * user2,PAY_ORDER,7
 */
public class EventTimeTimerExample {

    public static void main(String[] args) throws Exception {
        String baseDir =
                Paths
                        .get("checkpoints/" + EventTimeTimerExample.class.getSimpleName())
                        .toUri()
                        .toString();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用事件时间语义
        env.setParallelism(1);
//        env.getConfig().setAutoWatermarkInterval(100);
        env.enableCheckpointing(30_000);
        env.getCheckpointConfig().setCheckpointStorage(baseDir + "/chk/");
        DataStream<String> input = env.socketTextStream("localhost", 19999);

        DataStream<UserEvent> clicks = input
                .map(line -> {
                    String[] parts = line.split(",");
                    return new UserEvent(parts[0], parts[1], Long.parseLong(parts[2]));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<UserEvent>forMonotonousTimestamps()
                        .withTimestampAssigner((e, ts) -> e.eventTime)
                );

        clicks
                .keyBy(click -> click.userId)
                .process(new AbandonCartDetectFunction())
                .print();

        env.execute("Event Time Timer Example");
    }

    // 输入数据类型
    public static class UserEvent {
        public String userId;
        public String action;     // "ADD_TO_CART" or "PAY_ORDER"
        public long eventTime;    // 事件时间戳（ms）

        public UserEvent(String userId, String action, long eventTime) {
            this.userId = userId;
            this.action = action;
            this.eventTime = eventTime;
        }

        @Override
        public String toString() {
            return "UserEvent{" +
                    "userId='" + userId + '\'' +
                    ", action='" + action + '\'' +
                    ", eventTime=" + eventTime +
                    '}';
        }
    }

    public static class AbandonCartDetectFunction extends KeyedProcessFunction<String, UserEvent, String> {

        private transient ValueState<Long> timerState;

        @Override
        public void open(Configuration parameters) {
            timerState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("payTimeoutTimer", Long.class)
            );
        }

        @Override
        public void processElement(
                UserEvent event,
                Context ctx,
                Collector<String> out) throws Exception {
            if ("ADD_TO_CART".equals(event.action)) {
                // 注册定时器，2ms后触发
                long timerTs = event.eventTime + Duration.ofMillis(2).toMillis();

                Long ts = timerState.value();
                if (ts != null) {
                    // update timer
                    ctx.timerService().deleteEventTimeTimer(ts);
                }
                ctx.timerService().registerEventTimeTimer(timerTs);
                timerState.update(timerTs);
            } else if ("PAY_ORDER".equals(event.action)) {
                // 如果用户支付了，就取消定时器
                Long ts = timerState.value();
                if (ts != null && ts >= event.eventTime) {
                    ctx.timerService().deleteEventTimeTimer(ts);
                    timerState.clear();
                    out.collect("用户 [" + event.userId + "] 已支付，取消定时器。");
                } else {
                    out.collect("用户 [" + event.userId + "] 支付超时，请重新添加。");
                }
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
            out.collect("用户 [" + ctx.getCurrentKey() + "] 加购后2ms未支付，触发营销提醒！");
            timerState.clear();
        }
    }
}
