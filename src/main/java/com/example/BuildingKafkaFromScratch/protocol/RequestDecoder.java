package com.example.BuildingKafkaFromScratch.protocol;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class RequestDecoder {
    public Request decode(byte[] payload)
        throws IOException{
        BinaryReader reader = new BinaryReader(
                new ByteArrayInputStream(payload)
        );

        short apiKeyId = reader.readShort();

        short apiVersion = reader.readShort();

        int correlationId = reader.readInt();

        ApiKey apiKey = ApiKey.fromId(apiKeyId);

        RequestHeader header =
                new RequestHeader(
                        apiKey,
                        apiVersion,
                        correlationId
                );

        return switch (apiKey){
            case CREATE_TOPIC -> {
                String topic = reader.readString();
                int partitionCount = reader.readInt();
                yield new Request(
                        header,
                        topic,
                        partitionCount
                );
            }

            default ->
                throw new IOException(
                        "Unsupported API: " + apiKey
                );
        };
    }
}
