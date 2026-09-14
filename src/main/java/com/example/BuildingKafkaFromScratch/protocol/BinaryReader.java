package com.example.BuildingKafkaFromScratch.protocol;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class BinaryReader {
    private final DataInputStream in;
    public BinaryReader(InputStream inputStream){
        this.in = new DataInputStream(inputStream);
    }
    public byte readByte() throws IOException{
        return in.readByte();
    }

    public short readShort() throws IOException{
        return in.readShort();
    }

    public int readInt() throws IOException{
        return in.readInt();
    }

    public long readLong() throws IOException{
        return in.readLong();
    }

    public String readString() throws IOException{
        int length = in.readInt();
        if(length < 0) {
            throw new IOException(
                    "Invalid string length: " + length
            );
        }
        byte[] bytes = new byte[length];
        in.readFully(bytes);

        return new String(bytes
        , StandardCharsets.UTF_8);
    }

    public byte[] readBytes() throws IOException{
        int length = in.readInt();
        if(length < 0) {
            throw new IOException(
                    "Invalid byte array length: "+length
            );
        }
        byte[] bytes = new byte[length];

        in.readFully(bytes);

        return bytes;
    }
}
