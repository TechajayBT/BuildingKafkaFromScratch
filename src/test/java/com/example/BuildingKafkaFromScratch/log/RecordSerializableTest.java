package com.example.BuildingKafkaFromScratch.log;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class RecordSerializableTest {

    @Test
    void shouldSerializeAndDeserializeRecord(){
        Record original = new Record(42,"user-123","Hello Kafka".getBytes(StandardCharsets.UTF_8));

        RecordSerializable serializer = new RecordSerializable();

        byte[] data = serializer.serialize(original);

        Record restored = serializer.deserialize(data);

        assertEquals(original.offset(),restored.offset());
        assertEquals(original.key(),restored.key());
        assertArrayEquals(original.value(),restored.value());
    }
}