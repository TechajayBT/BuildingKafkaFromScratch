package com.example.BuildingKafkaFromScratch.log;

public record IndexEntry(
        long offset,
        long filePosition
){}
