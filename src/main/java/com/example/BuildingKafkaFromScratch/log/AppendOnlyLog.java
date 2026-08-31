package com.example.BuildingKafkaFromScratch.log;


import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class AppendOnlyLog implements AutoCloseable{
    private final FileChannel fileChannel;
    private final RecordSerializable serializer;

    private long nextOffset;

    public AppendOnlyLog(Path path)  throws IOException{
        this.fileChannel = FileChannel.open(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
        this.serializer = new RecordSerializable();
        this.nextOffset=0;
    }

    public long append(String key,byte[] value) throws IOException{
        long offset = nextOffset++;
        Record record= new Record(
                offset,
                key,
                value
        );
        byte[] data = serializer.serializable(record);

        fileChannel.position(fileChannel.size());

        fileChannel.write(java.nio.ByteBuffer.wrap(data));

        return offset;
    }
    @Override
    public void close() throws IOException{
        fileChannel.close();
    }

}
