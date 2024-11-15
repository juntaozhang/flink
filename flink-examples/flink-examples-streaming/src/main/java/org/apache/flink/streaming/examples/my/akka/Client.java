package org.apache.flink.streaming.examples.my.akka;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.Props;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Client {
    public static void main(String[] args) throws InterruptedException {
        Config config = ConfigFactory.load("client.conf");
        ActorSystem actorSystem = ActorSystem.create("client", config);
        ActorRef actor = actorSystem.actorOf(Props.create(ClientActor.class), "client");
        actorSystem
                .actorSelection("akka://server@127.0.0.1:28080/user/hi")
                .tell(new Message("hi"), actor);
        actorSystem
                .actorSelection("akka://server@127.0.0.1:28080/user/hello")
                .tell(new Message("hello"), actor);
        actorSystem
                .actorSelection("akka://server@127.0.0.1:28080/user/*")
                .tell(new Message("ok"), actor);
        actorSystem
                .actorSelection("akka://server@127.0.0.1:28080/user/hi")
                .tell(new FunctionCall("1+2"), actor);
        Thread.sleep(3000);
        actorSystem.stop(actor);
        actorSystem.terminate();
    }

    public static class ClientActor extends AbstractActor {

        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .match(
                            Message.class,
                            message -> {
                                log.info("receive {},", message);
                            })
                    .build();
        }
    }
}
