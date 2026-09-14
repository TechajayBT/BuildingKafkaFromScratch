package com.example.BuildingKafkaFromScratch.protocol;

public record RequestHeader(
        ApiKey apiKey,
        short apiVersion,
        int correlationId)
{
}
