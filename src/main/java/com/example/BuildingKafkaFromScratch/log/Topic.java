package com.example.BuildingKafkaFromScratch.log;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class Topic implements AutoCloseable{
    private final String name;
    private final List<PartitionLog> partitions;

    public Topic(String name,
                 int partitionCount,
                 Path directory) throws IOException{
        this.name = name;
        this.partitions = new ArrayList<>();
        for(int i=0;i<partitionCount;i++){
            Path partitionDirectory =
                    directory.resolve("partition-" + i);

            partitions.add(new PartitionLog(
                    partitionDirectory,i
            ));
        }
    }
    public String name(){
        return name;
    }

    public PartitionLog partition(int partitionId){
        if(partitionId<0 || partitionId>=partitions.size()){
            throw new IllegalArgumentException(
                    "Invalid partition: " + partitionId
            );
        }
        return partitions.get(partitionId);
    }

    public int partitionCount(){
        return partitions.size();
    }
    @Override
    public void close() throws IOException{
        for(PartitionLog partition : partitions){
            partition.close();
        }
    }
}
