package com.example.BuildingKafkaFromScratch.log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TopicManager implements AutoCloseable{
    private final Path dataDirectory;
    private final Map<String,Topic> topics = new ConcurrentHashMap<>();

    public TopicManager(Path dataDirectory) throws IOException{
        this.dataDirectory = dataDirectory;
        Files.createDirectories(dataDirectory);

        loadTopics();
    }

    public Topic createTopic(String name,
                             int partitionCount) throws IOException{
        if(name == null || name.isBlank()){
            throw new IllegalArgumentException(
                    "Topic name cannot be empty"
            );
        }

        if(partitionCount<=0){
            throw new IllegalArgumentException(
                    "Partition count must be greater than 0"
            );
        }

        if(topics.containsKey(name)){
            throw new IllegalArgumentException(
                    "Topic already exists: "+name
            );
        }
        Topic topic = new Topic(
                name,
                partitionCount,
                dataDirectory.resolve(name)
        );
        topics.put(name,topic);

        return topic;
    }

    public Topic getTopic(String name){
        Topic topic = topics.get(name);
        if(topic == null){
            throw new IllegalArgumentException(
                    "Unknown topic: " + name
            );
        }
        return topic;
    }

    private void loadTopics() throws IOException{
        try(var paths = Files.list(dataDirectory)){
            for(Path topicDirectory : paths
                    .filter(Files::isDirectory)
                    .toList()){
                String topicName =
                        topicDirectory
                                .getFileName().toString();

                int partitionCount = discoverPartitionCount(topicDirectory);

                if(partitionCount <= 0){
                    continue;
                }

                Topic topic =
                        new Topic(
                                topicName,
                                partitionCount,
                                topicDirectory
                        );

                topics.put(topicName,topic);
            }
        }
    }

    private int discoverPartitionCount(Path topicDirectory)
            throws IOException{
        int maxPartitionId = -1;
        try(var paths = Files.list(topicDirectory)){
            for(Path path : paths
                    .filter(Files::isDirectory)
                    .toList()){
                String name = path.getFileName().toString();
                if(!name.startsWith("partition-")){
                    continue;
                }
                String id = name.substring("partition-".length());
                int partitionId;
                try{
                    partitionId = Integer.parseInt(id);
                } catch(NumberFormatException e){
                    continue;
                }

                maxPartitionId = Math.max(maxPartitionId,partitionId);
            }
        }
        return maxPartitionId + 1;
    }

    @Override
    public void close() throws IOException{
        for(Topic topic : topics.values()){
            topic.close();
        }
    }
}
