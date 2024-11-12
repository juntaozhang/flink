package org.apache.flink.streaming.examples.my.akka;

import lombok.Data;

import java.io.Serializable;

@Data
public class Message implements Serializable {
    public final String message;

    public Message(String message) {
        this.message = message;
    }
}
