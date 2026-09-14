package com.example.BuildingKafkaFromScratch.protocol;

public record Request(
    RequestHeader header,
    String topic,
    int partitionCount){
}
