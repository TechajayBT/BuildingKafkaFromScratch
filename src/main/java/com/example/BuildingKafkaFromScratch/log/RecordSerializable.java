package com.example.BuildingKafkaFromScratch.log;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class RecordSerializable {
    public byte[] serialize(Record record) {

        byte[] keyBytes =
                record.key().getBytes(StandardCharsets.UTF_8);

        byte[] valueBytes =
                record.value();

        int recordLength =
                Long.BYTES
                        + Integer.BYTES
                        + keyBytes.length
                        + Integer.BYTES
                        + valueBytes.length;

        ByteBuffer buffer =
                ByteBuffer.allocate(
                        Integer.BYTES + recordLength
                );

        buffer.putInt(recordLength);
        buffer.putLong(record.offset());
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valueBytes.length);
        buffer.put(valueBytes);

        return buffer.array();
    }

    public Record deserialize(byte[] data) {
        ByteBuffer buffer = ByteBuffer.wrap(data);

        int recordLength = buffer.getInt();

        if (recordLength != buffer.remaining()) {
            throw new IllegalArgumentException(
                    "Invalid record length: " + recordLength
            );
        }

        long offset = buffer.getLong();

        int keyLength = buffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);

        String key = new String(
                keyBytes,
                StandardCharsets.UTF_8
        );

        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value);

        return new Record(offset, key, value);
    }
}
