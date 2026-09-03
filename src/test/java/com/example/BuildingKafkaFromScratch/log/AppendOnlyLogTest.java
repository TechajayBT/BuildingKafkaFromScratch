package com.example.BuildingKafkaFromScratch.log;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AppendOnlyLogTest {
    @Test
    void appendShouldReturnIncreasingOffsets() throws Exception{
        Path path = Files.createTempFile("simple-kafka",".log");
        try(AppendOnlyLog log = new AppendOnlyLog(path)){
            long offset1 = log.append(
                    "key1",
                    "A".getBytes(StandardCharsets.UTF_8)
            );
            long offset2 = log.append(
                    "key2",
                    "B".getBytes(StandardCharsets.UTF_8)
            );
            long offset3 = log.append(
                    "key3",
                    "C".getBytes(StandardCharsets.UTF_8)
            );
            assertEquals(0,offset1);
            assertEquals(1,offset2);
            assertEquals(2,offset3);
        }
    }

    @Test
    void shouldReadRecordsFromOffset() throws Exception {

        Path path = Files.createTempFile(
                "simple-kafka",
                ".log"
        );

        try (AppendOnlyLog log =
                     new AppendOnlyLog(path)) {

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

            assertEquals(
                    1,
                    records.get(0).offset()
            );

            assertEquals(
                    2,
                    records.get(1).offset()
            );

            assertEquals(
                    "B",
                    new String(
                            records.get(0).value(),
                            StandardCharsets.UTF_8
                    )
            );

            assertEquals(
                    "C",
                    new String(
                            records.get(1).value(),
                            StandardCharsets.UTF_8
                    )
            );
        }
    }

    @Test
    void shouldRecoverOffsetAfterRestart() throws Exception {

        Path path = Files.createTempFile(
                "simple-kafka",
                ".log"
        );

        try (AppendOnlyLog log =
                     new AppendOnlyLog(path)) {

            assertEquals(
                    0,
                    log.append(
                            "key1",
                            "A".getBytes(StandardCharsets.UTF_8)
                    )
            );

            assertEquals(
                    1,
                    log.append(
                            "key2",
                            "B".getBytes(StandardCharsets.UTF_8)
                    )
            );

            assertEquals(
                    2,
                    log.append(
                            "key3",
                            "C".getBytes(StandardCharsets.UTF_8)
                    )
            );
        }

        // Simulate broker restart
        try (AppendOnlyLog log =
                     new AppendOnlyLog(path)) {

            assertEquals(
                    3,
                    log.append(
                            "key4",
                            "D".getBytes(StandardCharsets.UTF_8)
                    )
            );
        }
    }

    @Test
    void shouldRecoverFromPartialRecord() throws Exception {

        Path path = Files.createTempFile(
                "simple-kafka",
                ".log"
        );

        // Write valid records.
        try (AppendOnlyLog log = new AppendOnlyLog(path)) {

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
        }

        // Simulate a crash during the next record.
        //
        // Open the file and append only a few bytes.
        try (FileChannel channel = FileChannel.open(
                path,
                StandardOpenOption.WRITE,
                StandardOpenOption.APPEND
        )) {

            ByteBuffer partialRecord =
                    ByteBuffer.allocate(12);

            partialRecord.putInt(100); // says record is 100 bytes
            partialRecord.putLong(3);  // offset = 3

            partialRecord.flip();

            while (partialRecord.hasRemaining()) {
                channel.write(partialRecord);
            }
        }

        // Simulate broker restart.
        try (AppendOnlyLog log = new AppendOnlyLog(path)) {

            // Partial record should have been removed.
            assertEquals(
                    3,
                    log.append(
                            "key4",
                            "D".getBytes(StandardCharsets.UTF_8)
                    )
            );

            List<Record> records = log.read(0);

            assertEquals(4, records.size());

            assertEquals(0, records.get(0).offset());
            assertEquals(1, records.get(1).offset());
            assertEquals(2, records.get(2).offset());
            assertEquals(3, records.get(3).offset());
        }
    }
}