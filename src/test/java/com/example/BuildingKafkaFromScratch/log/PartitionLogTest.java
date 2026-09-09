package com.example.BuildingKafkaFromScratch.log;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class PartitionLogTest {
    @Test
    void shouldAppendAndReadRecords()
            throws Exception {

        Path directory =
                Files.createTempDirectory(
                        "partition-0"
                );

        try (PartitionLog log =
                     new PartitionLog(
                             directory,
                             0
                     )) {

            log.append(
                    "key1",
                    "A".getBytes(StandardCharsets.UTF_8)
            );

            log.append(
                    "key2",
                    "B".getBytes(StandardCharsets.UTF_8)
            );

            log.append(
                    "key3",
                    "C".getBytes(StandardCharsets.UTF_8)
            );

            List<Record> records =
                    log.read(1);

            assertEquals(2, records.size());

            assertEquals(1, records.get(0).offset());

            assertEquals(2, records.get(1).offset());

            assertEquals(
                    "B",
                    new String(
                            records.get(0).value(),
                            StandardCharsets.UTF_8
                    )
            );
        }
    }
}