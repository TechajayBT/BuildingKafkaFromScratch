package com.example.BuildingKafkaFromScratch.log;


import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class AppendOnlyLog implements AutoCloseable {

    private final FileChannel fileChannel;
    private final RecordSerializable serializer;

    private long nextOffset;

    public AppendOnlyLog(Path path) throws IOException {

        this.fileChannel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        this.serializer = new RecordSerializable();

        this.nextOffset = 0;
    }

    public long append(String key, byte[] value)
            throws IOException {

        long offset = nextOffset++;

        Record record = new Record(
                offset,
                key,
                value
        );

        byte[] data =
                serializer.serializable(record);

        fileChannel.position(fileChannel.size());

        ByteBuffer buffer =
                ByteBuffer.wrap(data);

        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }

        return offset;
    }

    public List<Record> read(long fromOffset)
            throws IOException {

        List<Record> records =
                new ArrayList<>();

        fileChannel.position(0);

        while (fileChannel.position()
                < fileChannel.size()) {

            Record record =
                    readRecord();

            if (record.offset() >= fromOffset) {
                records.add(record);
            }
        }

        return records;
    }

    private Record readRecord()
            throws IOException {

        long offset = readLong();

        int keyLength = readInt();

        byte[] keyBytes =
                readBytes(keyLength);

        int valueLength = readInt();

        byte[] valueBytes =
                readBytes(valueLength);

        return new Record(
                offset,
                new String(
                        keyBytes,
                        java.nio.charset.StandardCharsets.UTF_8
                ),
                valueBytes
        );
    }

    private long readLong()
            throws IOException {

        ByteBuffer buffer =
                ByteBuffer.allocate(Long.BYTES);

        readFully(buffer);

        buffer.flip();

        return buffer.getLong();
    }

    private int readInt()
            throws IOException {

        ByteBuffer buffer =
                ByteBuffer.allocate(Integer.BYTES);

        readFully(buffer);

        buffer.flip();

        return buffer.getInt();
    }

    private byte[] readBytes(int length)
            throws IOException {

        if (length < 0) {
            throw new IOException(
                    "Invalid length: " + length
            );
        }

        ByteBuffer buffer =
                ByteBuffer.allocate(length);

        readFully(buffer);

        return buffer.array();
    }

    private void readFully(ByteBuffer buffer)
            throws IOException {

        while (buffer.hasRemaining()) {

            int bytesRead =
                    fileChannel.read(buffer);

            if (bytesRead == -1) {
                throw new EOFException(
                        "Unexpected end of log"
                );
            }
        }
    }

    @Override
    public void close()
            throws IOException {

        fileChannel.close();
    }
}