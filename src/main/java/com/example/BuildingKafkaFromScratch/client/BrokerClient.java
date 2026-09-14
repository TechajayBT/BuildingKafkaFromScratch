package com.example.BuildingKafkaFromScratch.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class BrokerClient implements AutoCloseable{
    private final Socket socket;
    private final BufferedReader reader;
    private final PrintWriter writer;

    public BrokerClient(String host, int port)
            throws IOException {

        socket = new Socket(host, port);

        reader = new BufferedReader(
                new InputStreamReader(
                        socket.getInputStream(),
                        StandardCharsets.UTF_8
                )
        );

        writer = new PrintWriter(
                socket.getOutputStream(),
                true
        );
    }

    public String send(String request)
            throws IOException {

        writer.println(request);

        return reader.readLine();
    }

    @Override
    public void close()
            throws IOException {

        socket.close();
    }

    public static void main(String[] args)
            throws Exception {

        try (BrokerClient client =
                     new BrokerClient("localhost", 9092)) {
           System.out.println(client.send(
                   "FETCH orders 0 0"
           ));
        }
    }
}
