package org.apache.flink.table.examples.java.my;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class IntervalExample {
    public static void main(String[] args) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.configure(new Configuration().set(RestOptions.PORT, RestOptions.PORT.defaultValue()));
        final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        /*
           INTERVAL DAY
           INTERVAL DAY(p1)
           INTERVAL DAY(p1) TO HOUR
           INTERVAL DAY(p1) TO MINUTE
           INTERVAL DAY(p1) TO SECOND(p2)
           INTERVAL HOUR
           INTERVAL HOUR TO MINUTE
           INTERVAL HOUR TO SECOND(p2)
           INTERVAL MINUTE
           INTERVAL MINUTE TO SECOND(p2)
           INTERVAL SECOND
           INTERVAL SECOND(p2)
        */
        tEnv.executeSql("""
                SELECT
                     INTERVAL '+1 02:30:45.123456' DAY TO SECOND -- only use second(3)
                    --,INTERVAL '1.123456789' SECOND(9)   -- illegal in calcite
                    --,INTERVAL '1.123456' SECOND(6)      -- illegal in flink, FlinkTypeFactory#toLogicalType
                    --,INTERVAL '1.123456' SECOND(2)      -- not working of precision
                    --,INTERVAL '1.1' SECOND(2)      -- not working of precision
                    --,INTERVAL '1.123' SECOND(2)      -- not working of precision
                    --,INTERVAL '-1 02:30' DAY TO MINUTE
                    --,INTERVAL '2 02' DAY TO HOUR
                    --,INTERVAL '2' DAY(2)
                    --,INTERVAL '200' DAY(3)
                    ,INTERVAL '+100 02:30:45.123456' DAY(3) TO SECOND(6)
                    --,INTERVAL '22:30:45.123456' HOUR TO SECOND
                    --,INTERVAL '22:30' HOUR TO MINUTE
                    --,INTERVAL '22' HOUR
                    --,INTERVAL '30:45.123456' MINUTE TO SECOND
                    --,INTERVAL '30' MINUTE
                    --,INTERVAL '-10.123456' SECOND(2)
                """).print();

        /*
         * INTERVAL YEAR
         * INTERVAL YEAR(p)
         * INTERVAL YEAR(p) TO MONTH
         * INTERVAL MONTH
         */
//        tEnv.executeSql(
//                        """
//                SELECT
//                    INTERVAL '+99' MONTH
//                    --,INTERVAL '+100' MONTH  -- MM
//                    --,INTERVAL '-100' YEAR -- YY, FlinkTypeSystem.getDefaultPrecision is 2
//                    --,INTERVAL '+1' YEAR
//                    --,INTERVAL '+0100' YEAR(4)
//                    ,INTERVAL '+99-11' YEAR TO MONTH -- here is a bug
//                    ,INTERVAL '+1000-11' YEAR(4) TO MONTH
//                """)
//                .print();

        //        java.time.Period period = java.time.Period.of(2025, 2, 3);
        //        System.out.println(period);

        //        tEnv.executeSql("""
        //                select  cast(INTERVAL '1' SECOND(2) as INTERVAL SECOND(1))
        //                """).print();

    }
}
