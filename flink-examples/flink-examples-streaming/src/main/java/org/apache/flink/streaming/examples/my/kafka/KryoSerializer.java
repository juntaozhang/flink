package org.apache.flink.streaming.examples.my.kafka;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class KryoSerializer implements Serializer<UserEvent> {
    private final Kryo kryo;

    public KryoSerializer() {
        kryo = new Kryo();
        kryo.register(UserEvent.class);
        kryo.setReferences(true);
    }

    @Override
    public byte[] serialize(String topic, UserEvent data) {
        if (data == null) {
            return null;
        }
        try (ByteArrayOutputStream stream = new ByteArrayOutputStream();
             Output output = new Output(stream)) {
            kryo.writeClassAndObject(output, data);
            output.flush();
            return stream.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
