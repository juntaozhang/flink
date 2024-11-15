package org.apache.flink;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

public class DataStreamTest {

    //    @Test
    //    void testTrigger() throws Exception {
    //        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    //
    //        DataStream<Tuple2<String, Integer>> stream = env.fromElements(
    //                Tuple2.of("key1", 1),
    //                Tuple2.of("key2", 2)
    //        ).keyBy(0)
    //                .window(TumblingEventTimeWindows.of(Time.seconds(1)))
    //                .trigger(ContinuousProcessingTimeTrigger.create())
    //    }

    @Test
    void testCoGroup() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, Integer>> stream1 =
                env.fromElements(Tuple2.of("key1", 1), Tuple2.of("key2", 2));

        DataStream<Tuple2<String, Integer>> stream2 =
                env.fromElements(Tuple2.of("key1", 3), Tuple2.of("key2", 4));

        DataStream<Tuple2<String, Integer>> coGroupedStream =
                stream1.coGroup(stream2)
                        .where(tuple -> tuple.f0)
                        .equalTo(tuple -> tuple.f0)
                        .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .apply(
                                new CoGroupFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>>() {
                                    @Override
                                    public void coGroup(
                                            Iterable<Tuple2<String, Integer>> first,
                                            Iterable<Tuple2<String, Integer>> second,
                                            Collector<Tuple2<String, Integer>> out) {
                                        for (Tuple2<String, Integer> left : first) {
                                            for (Tuple2<String, Integer> right : second) {
                                                out.collect(
                                                        new Tuple2<>(left.f0, left.f1 + right.f1));
                                            }
                                        }
                                    }
                                });

        coGroupedStream.print();

        env.execute("CoGroup Example");
    }
}
