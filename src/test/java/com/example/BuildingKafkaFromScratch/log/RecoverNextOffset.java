package com.example.BuildingKafkaFromScratch.log;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.*;

class RecoverNextOffset {
    @Test
    void shouldRecoverNextOffset() throws Exception {

        Path path = Files.createTempFile(
                "segment",
                ".log"
        );

        try (Segment segment = new Segment(path, 2000)) {

            assertEquals(
                    2000,
                    segment.append(
                            "key",
                            "A".getBytes(StandardCharsets.UTF_8)
                    )
            );

            assertEquals(
                    2001,
                    segment.append(
                            "key",
                            "B".getBytes(StandardCharsets.UTF_8)
                    )
            );

            assertEquals(
                    2002,
                    segment.append(
                            "key",
                            "C".getBytes(StandardCharsets.UTF_8)
                    )
            );
        }

        // Simulate restart
        try (Segment segment = new Segment(path, 2000)) {

            assertEquals(
                    2003,
                    segment.append(
                            "key",
                            "D".getBytes(StandardCharsets.UTF_8)
                    )
            );
        }
    }
}