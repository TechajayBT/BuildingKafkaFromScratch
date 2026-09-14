package com.example.BuildingKafkaFromScratch.log;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TopicManager implements AutoCloseable{
    private final Path dataDirectory;
    private final Map<String,Topic> topics = new ConcurrentHashMap<>();

    public TopicManager(Path dataDirectory){
        this.dataDirectory = dataDirectory;
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

    @Override
    public void close() throws IOException{
        for(Topic topic : topics.values()){
            topic.close();
        }
    }
}
