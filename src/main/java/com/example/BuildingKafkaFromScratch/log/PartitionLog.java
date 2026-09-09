package com.example.BuildingKafkaFromScratch.log;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class PartitionLog implements AutoCloseable {

    private static final long MAX_SEGMENT_SIZE = 10 * 1024 * 1024;
    private final int partitionId;
    private final Path directory;

    private final List<Segment> segments = new ArrayList<>();

    private Segment activeSegment;

    public PartitionLog(
            Path directory,
            int partitionId
    ) throws IOException {

        this.directory = directory;
        this.partitionId = partitionId;

        Files.createDirectories(directory);

        loadSegments();
    }

    private void loadSegments()
            throws IOException {

        try (var paths = Files.list(directory)) {

            List<Path> logFiles = paths
                    .filter(path ->
                            path.getFileName()
                                    .toString()
                                    .endsWith(".log"))
                    .sorted(Comparator.comparing(Path::toString))
                    .toList();

            for (Path path : logFiles) {

                long baseOffset =
                        extractBaseOffset(path);

                Segment segment =
                        new Segment(
                                path,
                                baseOffset
                        );

                segments.add(segment);
            }
        }

        if (segments.isEmpty()) {

            createSegment(0);

        } else {

            activeSegment =
                    segments.get(segments.size() - 1);
        }
    }

    private long extractBaseOffset(Path path) {

        String filename =
                path.getFileName().toString();

        String number =
                filename.substring(
                        0,
                        filename.length() - 4
                );

        return Long.parseLong(number);
    }

    private void createSegment(long baseOffset)
            throws IOException {

        Path path =
                directory.resolve(
                        String.format(
                                "%020d.log",
                                baseOffset
                        )
                );

        Segment segment =
                new Segment(
                        path,
                        baseOffset
                );

        segments.add(segment);

        activeSegment = segment;
    }

    public synchronized long append(
            String key,
            byte[] value
    ) throws IOException {

        Record record =
                new Record(
                        activeSegment.nextOffset(),
                        key,
                        value
                );

        byte[] serialized =
                new RecordSerializable()
                        .serialize(record);

        if (activeSegment.size() > 0 &&
                activeSegment.size()
                        + Integer.BYTES
                        + serialized.length
                        > MAX_SEGMENT_SIZE) {

            createSegment(
                    activeSegment.nextOffset()
            );
        }

        return activeSegment.append(
                key,
                value
        );
    }

    public List<Record> read(long fromOffset)
            throws IOException {

        List<Record> result =
                new ArrayList<>();

        Segment segment =
                findSegment(fromOffset);

        if (segment == null) {
            return result;
        }

        int index =
                segments.indexOf(segment);

        for (int i = index;
             i < segments.size();
             i++) {

            result.addAll(
                    segments.get(i)
                            .read(fromOffset)
            );
        }

        return result;
    }

    private Segment findSegment(long offset) {

        Segment result = null;

        for (Segment segment : segments) {

            if (segment.baseOffset() <= offset) {
                result = segment;
            } else {
                break;
            }
        }

        return result;
    }
    @Override
    public void close() throws IOException {

        for (Segment segment : segments) {
            segment.close();
        }
    }
}
