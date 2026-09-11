package com.example.BuildingKafkaFromScratch.client;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BrokerClientTest {

    @Test
    void shouldProduceRecord() throws Exception{
        try(BrokerClient client =
                new BrokerClient("localhost",9092)){
            String response = client.send(
                    "PRODUCE orders 0 user1 hello"
            );

            assertEquals(
                    "OK 0",
                    response
            );
        }
    }

}