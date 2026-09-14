package com.example.BuildingKafkaFromScratch.protocol;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;

import static org.junit.jupiter.api.Assertions.*;

class decodeCreateTopic
{
    @Test
    void decodeCreateTopicRequest()
            throws Exception {

        ByteArrayOutputStream output =
                new ByteArrayOutputStream();

        BinaryWriter writer =
                new BinaryWriter(output);

        writer.writeShort(
                ApiKey.CREATE_TOPIC.id()
        );

        writer.writeShort(0);

        writer.writeInt(42);

        writer.writeString("orders");

        writer.writeInt(3);

        writer.flush();

        RequestDecoder decoder =
                new RequestDecoder();

        Request request =
                decoder.decode(output.toByteArray());

        assertEquals(
                ApiKey.CREATE_TOPIC,
                request.header().apiKey()
        );

        assertEquals(
                42,
                request.header().correlationId()
        );

        assertEquals(
                "orders",
                request.topic()
        );

        assertEquals(
                3,
                request.partitionCount()
        );
    }
}