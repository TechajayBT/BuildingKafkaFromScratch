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

public class AppendOnlyLog implements AutoCloseable {

    private static final int INDEX_INTERVAL = 100;
    private final FileChannel fileChannel;
    private final RecordSerializable serializer;

    private final List<IndexEntry> index = new ArrayList<>();
    private long nextOffset;

    public AppendOnlyLog(Path path) throws IOException {

        this.fileChannel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE
        );

        this.serializer = new RecordSerializable();

        recover();
    }

    private void recover() throws IOException{
        nextOffset = 0;
        fileChannel.position(0);

        while(fileChannel.position() < fileChannel.size()){
            long recordPosition = fileChannel.position();
            try {
                Record record = readRecord();
                if(record.offset()%INDEX_INTERVAL==0){
                    index.add(new IndexEntry(record.offset(),recordPosition));
                }
                nextOffset = record.offset()+1;
            }
            catch(IOException e){
                fileChannel.truncate(recordPosition);
                break;
            }
        }
        fileChannel.position(fileChannel.size());
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
                serializer.serialize(record);

        long filePosition = fileChannel.size();

        if(offset % INDEX_INTERVAL == 0){
            index.add(new IndexEntry(offset,filePosition));
        }

        fileChannel.position(filePosition);

        ByteBuffer buffer =
                ByteBuffer.wrap(data);

        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }

        return offset;
    }

    private IndexEntry findIndexEntry(long offset){
        IndexEntry result = null;
        for(IndexEntry entry: index){
            if(entry.offset() > offset){
                break;
            }
            result = entry;
        }
        return result;
    }

    public List<Record> read(long fromOffset)
            throws IOException {

        List<Record> records =
                new ArrayList<>();

        IndexEntry entry = findIndexEntry(fromOffset);

        long startPosition = entry == null ? 0 : entry.filePosition();

        fileChannel.position(startPosition);

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

    private Record readRecord() throws IOException {

        int recordLength = readInt();

        if (recordLength <= 0) {
            throw new IOException(
                    "Invalid record length: " + recordLength
            );
        }

        long remaining = fileChannel.size()-fileChannel.position();

        if(recordLength>remaining){
            throw new EOFException("Incomplete record");
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