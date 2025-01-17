package org.apache.flink.streaming.examples.my.kafka;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class KryoDeserializer implements Deserializer<UserEvent> {
    private final Kryo kryo;

    public KryoDeserializer() {
        kryo = new Kryo();
        kryo.register(UserEvent.class);
        kryo.setReferences(true);
    }

    public static void main(String[] args) {
        try (
                KryoSerializer serializer = new KryoSerializer();
                KryoDeserializer deserializer = new KryoDeserializer()) {
            byte[] data = serializer.serialize("test", UserEvent.click(12));
            UserEvent userEvent = deserializer.deserialize("test", data);

            System.out.println(userEvent);
            data = serializer.serialize(
                    "test",
                    UserEvent.watermark(12));
            userEvent = deserializer.deserialize("test", data);
            System.out.println(userEvent);
        }
    }

    @Override
    public UserEvent deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try (
                InputStream inputStream = new ByteArrayInputStream(data);
                Input input = new Input(inputStream)) {
            return (UserEvent) kryo.readClassAndObject(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
