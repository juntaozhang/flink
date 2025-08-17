package org.apache.flink.streaming.examples.my;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordcountExample {
    public static void main(String[] args) throws Exception {

        Configuration config = new Configuration();
        config.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);

        DataStream<String> text = env.socketTextStream("localhost", 19999);

        DataStream<String> words = text.flatMap((String line, Collector<String> out) -> {
            for (String word : line.split(" ")) {
                out.collect(word);
            }
        }).returns(Types.STRING);

        DataStream<Tuple2<String, Integer>> wordOnes = words.map(word -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));

        DataStream<Tuple2<String, Integer>> counts = wordOnes
                .keyBy(tuple -> tuple.f0)
                .sum(1)
                .setParallelism(2);

        counts.print().setParallelism(2);
        env.execute("WordCount Example");
    }
}
