package com.example.BuildingKafkaFromScratch.log;

public record Record (
    long offset,
    String key,
    byte[] value)
{
}
