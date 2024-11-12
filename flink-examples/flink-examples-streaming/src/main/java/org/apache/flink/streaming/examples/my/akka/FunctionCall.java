package org.apache.flink.streaming.examples.my.akka;

import lombok.Data;

import java.io.Serializable;

@Data
public class FunctionCall implements Serializable {
    public final String expression;

    public FunctionCall(String message) {
        this.expression = message;
    }
}
