package org.apache.flink.streaming.test.utils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.Test;
import sun.misc.Signal;

import java.io.Serializable;

public class MyTest {
    int t = 1; // t is not serializable, because of MyTest is not serializable

    @Test
    void testClosureCleaner() {
        Factor factor = new Factor();
        MapFunction<Integer, Integer> myFunction =
                new MapFunction<Integer, Integer>() {
                    @Override
                    public Integer map(Integer value) {
                        return value * factor.factor * t;
                        //                return value * factor.factor;
                    }
                };
        factor.factor = 3;
        System.out.println(factor);
        ClosureCleaner.clean(myFunction, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
        /*
        Caused by: java.io.NotSerializableException: org.apache.flink.streaming.test.MyTest
        at java.io.ObjectOutputStream.writeObject0(ObjectOutputStream.java:1184)
        */
    }

    @Test
    void testSignal() {
        new Handler("TERM"); // kill -15 TERM (software termination signal)
        new Handler("HUP"); // kill -1
        new Handler("INT"); // kill -2 通常用于中断程序的执行

        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("接收到中断信号，程序退出");
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    public static class Factor implements Serializable {
        public int factor = 2;
    }

    private static class Handler implements sun.misc.SignalHandler {

        private final sun.misc.SignalHandler prevHandler;

        Handler(String name) {
            prevHandler = Signal.handle(new Signal(name), this);
        }

        /**
         * Handle an incoming signal.
         *
         * @param signal The incoming signal
         */
        @Override
        public void handle(Signal signal) {
            System.out.printf(
                    "RECEIVED SIGNAL %s: SIG%s. Shutting down as requested.%n",
                    signal.getNumber(), signal.getName());
            prevHandler.handle(signal);
        }
    }

    public static class Person {
        public String name;
        public int age;

        public Person() {}

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return "Person{name='" + name + "', age=" + age + '}';
        }
    }

    @Test
    void testTypeInformation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //        TypeInformation<Person> personTypeInfo = TypeExtractor.getForClass(Person.class);
        //        System.out.println("TypeInformation: " + personTypeInfo);

        DataStream<Person> personStream =
                env.fromElements(
                        new Person("Alice", 30),
                        new Person("Bob", 25)) /*.returns(personTypeInfo)*/;

        personStream.print();
        env.execute("TypeInformation Example");
    }
}
