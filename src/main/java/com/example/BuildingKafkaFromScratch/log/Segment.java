package com.example.BuildingKafkaFromScratch.log;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class Segment implements AutoCloseable{
    private final long baseOffset;
    private final FileChannel fileChannel;
    private final RecordSerializable serializer;

    public Segment(Path path, long baseOffset) throws IOException {
        this.baseOffset = baseOffset;

        this.fileChannel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        this.serializer = new RecordSerializable();
    }

    public long baseOffset(){
        return baseOffset;
    }
    public long append(String key, byte[] value) throws IOException {

        long offset = nextOffset();

        Record record = new Record(
                offset,
                key,
                value
        );

        byte[] body = serializer.serialize(record);

        ByteBuffer buffer = ByteBuffer.allocate(
                Integer.BYTES + body.length
        );

        // Record length
        buffer.putInt(body.length);

        // Record body
        buffer.put(body);

        buffer.flip();

        fileChannel.position(fileChannel.size());

        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }

        return offset;
    }

    private long nextOffset() throws IOException {
        // Temporary implementation.
        // We will replace this when Segment recovery is implemented.
        return baseOffset;
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
}
