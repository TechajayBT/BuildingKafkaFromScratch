package com.example.BuildingKafkaFromScratch.protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

public class BinaryWriter {
    private final DataOutputStream out;
    public BinaryWriter(OutputStream outputStream){
        this.out =
                new DataOutputStream(outputStream);
    }

    public void writeByte(int value) throws IOException{
        out.writeByte(value);
    }

    public void writeShort(int value) throws IOException{
        out.writeShort(value);
    }

    public void writeInt(int value) throws IOException{
        out.writeInt(value);
    }

    public void writeLong(int value) throws IOException{
        out.writeLong(value);
    }

    public void writeString(String value) throws IOException{
        byte[] bytes =
                value.getBytes(StandardCharsets.UTF_8);

        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public void writeBytes(byte[] value) throws IOException{
        out.writeInt(value.length);
        out.write(value);
    }

    public void flush() throws IOException{
        out.flush();
    }
}
