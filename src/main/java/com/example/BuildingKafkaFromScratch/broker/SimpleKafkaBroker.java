package com.example.BuildingKafkaFromScratch.broker;


import com.example.BuildingKafkaFromScratch.log.PartitionLog;
import com.example.BuildingKafkaFromScratch.log.Record;
import com.example.BuildingKafkaFromScratch.log.Topic;
import com.example.BuildingKafkaFromScratch.log.TopicManager;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;

public class SimpleKafkaBroker {

    private final TopicManager topicManager;

    public SimpleKafkaBroker(Path dataDirectory)
            throws IOException {

        this.topicManager =
                new TopicManager(dataDirectory);

        topicManager.createTopic(
                "orders",
                3
        );
    }

    public void start(int port)
            throws IOException {

        try (ServerSocket serverSocket =
                     new ServerSocket(port)) {

            System.out.println(
                    "Kafka broker started on port "
                            + port
            );

            while (true) {

                Socket socket =
                        serverSocket.accept();

                Thread thread =
                        new Thread(
                                () -> handleClient(socket)
                        );

                thread.start();
            }
        }
    }

    private void handleClient(Socket socket) {

        System.out.println(
                "Client connected: "
                        + socket.getRemoteSocketAddress()
        );

        try (
                socket;
                BufferedReader reader =
                        new BufferedReader(
                                new InputStreamReader(
                                        socket.getInputStream(),
                                        StandardCharsets.UTF_8
                                )
                        );
                PrintWriter writer =
                        new PrintWriter(
                                new OutputStreamWriter(
                                        socket.getOutputStream(),
                                        StandardCharsets.UTF_8
                                ),
                                true
                        )
        ) {

            String request;

            while ((request = reader.readLine()) != null) {

                String response =
                        handleRequest(request);

                writer.println(response);
            }

        } catch (Exception e) {

            System.err.println(
                    "Client error: "
                            + e.getMessage()
            );
        }
    }

    private String handleRequest(String request)
            throws IOException {

        String[] parts =
                request.split(" ", 5);

        if (parts.length == 0) {
            return "ERROR empty request";
        }

        return switch (parts[0].toUpperCase()) {

            case "PRODUCE" ->
                    handleProduce(parts);

            case "FETCH" ->
                    handleFetch(parts);

            default ->
                    "ERROR unknown command";
        };
    }

    private String handleProduce(
            String[] parts
    ) throws IOException {

        if (parts.length < 5) {
            return "ERROR usage: PRODUCE topic partition key value";
        }

        String topicName = parts[1];

        int partitionId =
                Integer.parseInt(parts[2]);

        String key = parts[3];

        String value = parts[4];

        Topic topic =
                topicManager.getTopic(topicName);

        PartitionLog partition =
                topic.partition(partitionId);

        long offset =
                partition.append(
                        key,
                        value.getBytes(StandardCharsets.UTF_8)
                );

        return "OK " + offset;
    }

    private String handleFetch(
            String[] parts
    ) throws IOException {

        if (parts.length < 4) {
            return "ERROR usage: FETCH topic partition offset";
        }

        String topicName = parts[1];

        int partitionId =
                Integer.parseInt(parts[2]);

        long offset =
                Long.parseLong(parts[3]);

        Topic topic =
                topicManager.getTopic(topicName);

        PartitionLog partition =
                topic.partition(partitionId);

        List<Record> records =
                partition.read(offset);

        StringBuilder response =
                new StringBuilder("OK");

        for (Record record : records) {

            String value =
                    new String(
                            record.value(),
                            StandardCharsets.UTF_8
                    );

            response.append(" ")
                    .append(record.offset())
                    .append(":")
                    .append(value);
        }

        return response.toString();
    }
}