package org.apache.flink.streaming.examples.my.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.DeadLetter;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Server {
    public static void main(String[] args) {
        Config config = ConfigFactory.load("server.conf");
        ActorSystem actorSystem = ActorSystem.create("server", config);
        ActorRef hi = actorSystem.actorOf(Props.create(HiActor.class), "hi");
        actorSystem.actorOf(Props.create(HelloActor.class), "hello");
        ActorRef listener =
                actorSystem.actorOf(Props.create(DeadLetterListener.class), "deadLetterListener");
        actorSystem.eventStream().subscribe(listener, DeadLetter.class);
        //        actorSystem
        //                .actorSelection("/user/hi")
        //                .tell(new Message("hi from server"), ActorRef.noSender());
        //        hi.tell(new Message("hi from server"), ActorRef.noSender());
    }

    public static class HiActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(
                            Message.class,
                            message -> {
                                log.info("Hi Message, receive {}", message);
                                getSender()
                                        .tell(
                                                new Message("echo " + message.getMessage()),
                                                getSelf());
                            })
                    .match(
                            FunctionCall.class,
                            functionCall -> {
                                log.info("Hi FunctionCall, receive {}", functionCall);
                                getSender().tell(new Message("echo result"), getSelf());
                            })
                    .build();
        }
    }

    public static class DeadLetterListener extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(
                            DeadLetter.class,
                            deadLetter -> {
                                System.out.println("Received dead letter: " + deadLetter);
                            })
                    .build();
        }
    }

    public static class HelloActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(
                            Message.class,
                            message -> {
                                log.info("Hello Message, receive {}", message);
                            })
                    .build();
        }
    }
}
