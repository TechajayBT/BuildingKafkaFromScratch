package com.example.BuildingKafkaFromScratch.log;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

public class Segment implements AutoCloseable {

    private final long baseOffset;
    private final FileChannel fileChannel;
    private final RecordSerializable serializer;
    private final SparseIndex index;

    private long nextOffset;

    public Segment(Path path, long baseOffset) throws IOException {

        this.baseOffset = baseOffset;

        this.fileChannel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        this.serializer = new RecordSerializable();
        this.index = new SparseIndex();

        this.nextOffset = recoverNextOffset();
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long size() throws IOException{
        return fileChannel.size();
    }
    public long nextOffset() {
        return nextOffset;
    }

    public long append(String key, byte[] value)
            throws IOException {

        long offset = nextOffset;

        long filePosition = fileChannel.size();

        Record record = new Record(
                offset,
                key,
                value
        );

        byte[] data = serializer.serialize(record);

        ByteBuffer buffer = ByteBuffer.wrap(data);

        fileChannel.position(filePosition);

        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }

        if (offset % 100 == 0) {
            index.add(offset, filePosition);
        }

        nextOffset++;

        return offset;
    }

    private long recoverNextOffset()
            throws IOException {

        if (fileChannel.size() == 0) {
            return baseOffset;
        }

        fileChannel.position(0);

        long lastOffset = baseOffset - 1;

        while (fileChannel.position() < fileChannel.size()) {

            long recordPosition =
                    fileChannel.position();

            try {

                Record record = readRecord();

                if (record.offset() % 100 == 0) {
                    index.add(
                            record.offset(),
                            recordPosition
                    );
                }

                lastOffset = record.offset();

            } catch (EOFException e) {

                fileChannel.truncate(recordPosition);

                break;
            }
        }

        fileChannel.position(fileChannel.size());

        return lastOffset + 1;
    }

    private Record readRecord()
            throws IOException {

        // First read the record length.
        int recordLength = readInt();

        if (recordLength <= 0) {
            throw new IOException(
                    "Invalid record length: " + recordLength
            );
        }

        // Check that the complete record actually exists.
        long remaining =
                fileChannel.size()
                        - fileChannel.position();

        if (recordLength > remaining) {
            throw new EOFException(
                    "Incomplete record"
            );
        }

        ByteBuffer buffer =
                ByteBuffer.allocate(recordLength);

        readFully(buffer);

        buffer.flip();

        long offset = buffer.getLong();

        int keyLength = buffer.getInt();

        if (keyLength < 0 ||
                keyLength > buffer.remaining()) {

            throw new IOException(
                    "Invalid key length: " + keyLength
            );
        }

        byte[] keyBytes =
                new byte[keyLength];

        buffer.get(keyBytes);

        int valueLength =
                buffer.getInt();

        if (valueLength < 0 ||
                valueLength > buffer.remaining()) {

            throw new IOException(
                    "Invalid value length: " + valueLength
            );
        }

        byte[] valueBytes =
                new byte[valueLength];

        buffer.get(valueBytes);

        return new Record(
                offset,
                new String(
                        keyBytes,
                        StandardCharsets.UTF_8
                ),
                valueBytes
        );
    }

    private int readInt()
            throws IOException {

        ByteBuffer buffer =
                ByteBuffer.allocate(Integer.BYTES);

        readFully(buffer);

        buffer.flip();

        return buffer.getInt();
    }

    private void readFully(ByteBuffer buffer)
            throws IOException {

        while (buffer.hasRemaining()) {

            int bytesRead =
                    fileChannel.read(buffer);

            if (bytesRead == -1) {
                throw new EOFException(
                        "Unexpected end of file"
                );
            }
        }
    }

    public List<Record> read(long fromOffset) throws IOException{
        List<Record> records = new ArrayList<>();
        Long position = index.findPosition(fromOffset);
        if (position == null) {
            fileChannel.position(0);
        } else {
            fileChannel.position(position);
        }
        while(fileChannel.position() < fileChannel.size()){
            Record record = readRecord();
            if(record.offset() >= fromOffset){
                records.add(record);
            }
        }
        return records;
    }

    @Override
    public void close()
            throws IOException {

        fileChannel.close();
    }
}