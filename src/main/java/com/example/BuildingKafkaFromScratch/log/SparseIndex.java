package com.example.BuildingKafkaFromScratch.log;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class SparseIndex {
    private final NavigableMap<Long, Long> entries =
            new TreeMap<>();

    public void add(long offset, long filePosition) {
        entries.put(offset, filePosition);
    }

    public Long findPosition(long offset) {

        Map.Entry<Long, Long> entry =
                entries.floorEntry(offset);

        if (entry == null) {
            return null;
        }

        return entry.getValue();
    }

    public int size() {
        return entries.size();
    }
}
