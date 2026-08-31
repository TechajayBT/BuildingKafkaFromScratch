package com.example.BuildingKafkaFromScratch.log;

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class RecordSerializable {
    public byte[] serializable(Record record){
        byte[] keyBytes = record.key().getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = record.value();

        int size = Long.BYTES + Integer.BYTES + keyBytes.length+Integer.BYTES+valueBytes.length ;

        ByteBuffer buffer = ByteBuffer.allocate(size);

        buffer.putLong(record.offset());
        buffer.putInt(keyBytes.length);
        buffer.put(keyBytes);
        buffer.putInt(valueBytes.length);
        buffer.put(valueBytes);

        return buffer.array();
    }

    public Record deserialize(byte[] data){
        ByteBuffer buffer = ByteBuffer.wrap(data);
        long offset = buffer.getLong();
        int keyLength = buffer.getInt();
        byte[] keyBytes = new byte[keyLength];
        buffer.get(keyBytes);
        String key = new String(keyBytes,StandardCharsets.UTF_8);
        int valueLength = buffer.getInt();
        byte[] value = new byte[valueLength];
        buffer.get(value);

        return new Record(offset,key,value);
    }
}
