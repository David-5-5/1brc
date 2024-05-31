/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.onebrc;

import static java.lang.Double.parseDouble;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.summarizingDouble;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.RandomAccessFile;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Refer to https://questdb.io/blog/billion-row-challenge-step-by-step/
 */
public class CalculateAverage_step7 {
    private static final String FILE = "./measurements.txt";

    static final File file = new File("measurements.txt");
    static final long length = file.length();
    static final int chunkCount = Runtime.getRuntime().availableProcessors();
    final long[] chunkStartOffsets = new long[chunkCount];
    static ExecutorService executorService = Executors.newFixedThreadPool(chunkCount);

    public static void main(String[] args) throws Exception {
        var start = System.currentTimeMillis();
        Step3.calculate();
        System.err.format("Took %,d ms\n", System.currentTimeMillis() - start);
    }

    static class Step1 {
        static void calculate() throws Exception {
            @SuppressWarnings("resource")
            var allStats = new BufferedReader(new FileReader(FILE))
                    .lines()
                    .parallel()
                    .collect(
                            groupingBy(line -> line.substring(0, line.indexOf(';')),
                                    summarizingDouble(line -> parseDouble(line.substring(line.indexOf(';') + 1)))));
            var result = allStats.entrySet().stream().collect(Collectors.toMap(
                    Entry::getKey,
                    e -> {
                        var stats = e.getValue();
                        return String.format("%.1f/%.1f/%.1f",
                                stats.getMin(), stats.getAverage(), stats.getMax());
                    },
                    (l, r) -> r,
                    TreeMap::new));
            System.out.println(result);
        }
    }

    /**
     * Optimization 1: Parallelize I/O
     * Optimization 2: Directly parse temperature as int
     * 
     * Duraton : 2.454s
     */
    static class Step2 {

        static void calculate() throws Exception {
            final var results = new StationStats[chunkCount][];
            @SuppressWarnings("unchecked")
            final Future<StationStats[]>[] futures = new Future[chunkCount];

            final var chunkStartOffsets = new long[chunkCount];

            try (var file = new RandomAccessFile(new File(FILE), "r")) {
                for (int i = 1; i < chunkStartOffsets.length; i++) {
                    var start = length * i / chunkStartOffsets.length;
                    file.seek(start);
                    while (file.read() != (byte) '\n') {
                    }
                    start = file.getFilePointer();
                    chunkStartOffsets[i] = start;
                }
                MemorySegment mappedFile = file.getChannel().map(
                        MapMode.READ_ONLY, 0, length, Arena.global());

                for (int i = 0; i < chunkCount; i++) {
                    final long chunkStart = chunkStartOffsets[i];
                    final long chunkLimit = (i + 1 < chunkCount) ? chunkStartOffsets[i + 1] : length;
                    futures[i] = executorService
                            .submit(new ChunkProcessor(mappedFile.asSlice(chunkStart, chunkLimit - chunkStart)));
                }

                for (int i = 0; i < chunkCount; i++) {
                    results[i] = futures[i].get();
                }
                executorService.shutdown();

                var totalsMap = new TreeMap<String, StationStats>();
                for (var statsArray : results) {
                    for (var stats : statsArray) {
                        totalsMap.merge(stats.name, stats, (old, curr) -> {
                            old.count += curr.count;
                            old.sum += curr.sum;
                            old.min = Math.min(old.min, curr.min);
                            old.max = Math.max(old.max, curr.max);
                            return old;
                        });
                    }
                }
                System.out.println(totalsMap);

            }
        }

        static class ChunkProcessor extends AbstractCallable {
            private final Map<String, StationStats> statsMap = new HashMap<>();

            ChunkProcessor(MemorySegment chunk) {
                super(chunk);
            }

            @Override
            public StationStats[] call() throws Exception {
                for (var cursor = 0L; cursor < chunk.byteSize();) {
                    var semicolonPos = findByte(cursor, ';');
                    var newlinePos = findByte(semicolonPos + 1, '\n');
                    var name = stringAt(cursor, semicolonPos);
                    // Variant 1:
                    // var temp = Double.parseDouble(stringAt(semicolonPos + 1, newlinePos));
                    // var intTemp = (int) Math.round(10 * temp);

                    // Variant 2:
                    var intTemp = parseTemperature(semicolonPos);

                    var stats = statsMap.computeIfAbsent(name, k -> new StationStats(name));
                    stats.sum += intTemp;
                    stats.count++;
                    stats.min = Math.min(stats.min, intTemp);
                    stats.max = Math.max(stats.max, intTemp);
                    cursor = newlinePos + 1;
                }

                return statsMap.values().toArray(StationStats[]::new);
            }
        }
    }

    abstract static class AbstractCallable implements Callable<StationStats[]> {
        protected final MemorySegment chunk;

        AbstractCallable(MemorySegment chunk) {
            this.chunk = chunk;
        }

        int parseTemperature(long semicolonPos) {
            long off = semicolonPos + 1;
            int sign = 1;
            byte b = chunk.get(JAVA_BYTE, off++);
            if (b == '-') {
                sign = -1;
                b = chunk.get(JAVA_BYTE, off++);
            }
            int temp = b - '0';
            b = chunk.get(JAVA_BYTE, off++);
            if (b != '.') {
                temp = 10 * temp + b - '0';
                // we found two integer digits. The next char is definitely '.', skip it:
                off++;
            }
            b = chunk.get(JAVA_BYTE, off);
            temp = 10 * temp + b - '0';
            return sign * temp;
        }

        long findByte(long cursor, int b) {
            for (var i = cursor; i < chunk.byteSize(); i++) {
                if (chunk.get(JAVA_BYTE, i) == b) {
                    return i;
                }
            }
            throw new RuntimeException(((char) b) + " not found");
        }

        String stringAt(long start, long limit) {
            return new String(
                    chunk.asSlice(start, limit - start).toArray(JAVA_BYTE),
                    StandardCharsets.UTF_8);
        }
    }

    static class StationStats implements Comparable<StationStats> {
        String name;
        int sum;
        int count;
        private int min = Integer.MAX_VALUE;
        private int max = Integer.MIN_VALUE;

        StationStats(String name) {
            this.name = name;
        }

        StationStats(StatsAcc acc, MemorySegment chunk) {
            name = new String(chunk.asSlice(acc.nameOffset, acc.nameLen).toArray(JAVA_BYTE), StandardCharsets.UTF_8);
            sum = acc.sum;
            count = acc.count;
            min = acc.min;
            max = acc.max;
        }

        @Override
        public String toString() {
            return String.format("%.1f/%.1f/%.1f", min / 10.0, Math.round((double) sum / count) / 10.0, max / 10.0);
        }

        @Override
        public boolean equals(Object that) {
            return that.getClass() == StationStats.class && ((StationStats) that).name.equals(this.name);
        }

        @Override
        public int compareTo(StationStats that) {
            return name.compareTo(that.name);
        }
    }

    /**
     * Use MappedByteBuffer repalce of the new MemorySegment
     * 
     * Duraton : 2.111s
     */
    static class Step2WithFileChannel {

        static void calculate() throws Exception {
            final var results = new StationStats[chunkCount][];
            @SuppressWarnings("unchecked")
            final Future<StationStats[]>[] futures = new Future[chunkCount];

            final var chunkStartOffsets = new long[chunkCount];

            try (var file = new RandomAccessFile(new File(FILE), "r")) {
                for (int i = 1; i < chunkStartOffsets.length; i++) {
                    var start = length * i / chunkStartOffsets.length;
                    file.seek(start);
                    while (file.read() != (byte) '\n') {
                    }
                    start = file.getFilePointer();
                    chunkStartOffsets[i] = start;
                }
                FileChannel channel = file.getChannel();

                for (int i = 0; i < chunkCount; i++) {
                    final long chunkStart = chunkStartOffsets[i];
                    final long chunkLimit = (i + 1 < chunkCount) ? chunkStartOffsets[i + 1] : length;
                    futures[i] = executorService
                            .submit(new ChunkProcessor(
                                    channel.map(FileChannel.MapMode.READ_ONLY, chunkStart,
                                            chunkLimit - chunkStart)));
                }

                for (int i = 0; i < chunkCount; i++) {
                    results[i] = futures[i].get();
                }
                executorService.shutdown();

                var totalsMap = new TreeMap<String, StationStats>();
                for (var statsArray : results) {
                    for (var stats : statsArray) {
                        totalsMap.merge(stats.name, stats, (old, curr) -> {
                            old.count += curr.count;
                            old.sum += curr.sum;
                            old.min = Math.min(old.min, curr.min);
                            old.max = Math.max(old.max, curr.max);
                            return old;
                        });
                    }
                }
                System.out.println(totalsMap);

            }
        }

        static class ChunkProcessor implements Callable<StationStats[]> {
            private final Map<String, StationStats> statsMap = new HashMap<>();
            private MappedByteBuffer chunk;

            ChunkProcessor(MappedByteBuffer chunk) {
                this.chunk = chunk;
            }

            @Override
            public StationStats[] call() throws Exception {
                for (var cursor = 0; cursor < chunk.capacity();) {
                    var semicolonPos = findByte(cursor, ';');
                    var newlinePos = findByte(semicolonPos + 1, '\n');
                    var name = stringAt(cursor, semicolonPos);
                    // Variant 1:
                    // var temp = Double.parseDouble(stringAt(semicolonPos + 1, newlinePos));
                    // var intTemp = (int) Math.round(10 * temp);

                    // Variant 2:
                    var intTemp = parseTemperature(semicolonPos);

                    var stats = statsMap.computeIfAbsent(name, k -> new StationStats(name));
                    stats.sum += intTemp;
                    stats.count++;
                    stats.min = Math.min(stats.min, intTemp);
                    stats.max = Math.max(stats.max, intTemp);
                    cursor = newlinePos + 1;
                }

                return statsMap.values().toArray(StationStats[]::new);
            }

            int parseTemperature(int semicolonPos) {
                int off = semicolonPos + 1;
                int sign = 1;
                byte b = chunk.get(off++);
                if (b == '-') {
                    sign = -1;
                    b = chunk.get(off++);
                }
                int temp = b - '0';
                b = chunk.get(off++);
                if (b != '.') {
                    temp = 10 * temp + b - '0';
                    // we found two integer digits. The next char is definitely '.', skip it:
                    off++;
                }
                b = chunk.get(off);
                temp = 10 * temp + b - '0';
                return sign * temp;
            }

            int findByte(int cursor, int b) {
                for (var i = cursor; i < chunk.capacity(); i++) {
                    if (chunk.get(i) == b) {
                        return i;
                    }
                }
                throw new RuntimeException(((char) b) + " not found");
            }

            String stringAt(int start, int limit) {
                byte[] dst = new byte[limit - start];
                chunk.get(start, dst);
                return new String(dst, StandardCharsets.UTF_8);
            }

        }
    }

    static class StatsAcc {
        long nameOffset;
        long nameLen;
        int hash;
        int sum;
        int count;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;

        StatsAcc(int hash, long nameOffset, long nameLen) {
            this.hash = hash;
            this.nameOffset = nameOffset;
            this.nameLen = nameLen;
        }

        public boolean nameEquals(MemorySegment chunk, long otherNameOffset, long otherNameLimit) {
            var otherNameLen = otherNameLimit - otherNameOffset;
            // Avoid construct instance of String for performance
            return nameLen == otherNameLen &&
                    chunk.asSlice(nameOffset, nameLen).mismatch(chunk.asSlice(otherNameOffset, nameLen)) == -1;
        }
    }

    /**
     * Optimization 3: Custom hashtable
     * 
     * Duraton : s
     */
    static class Step3 {

        static void calculate() throws Exception {
            final var results = new StationStats[chunkCount][];
            @SuppressWarnings("unchecked")
            final Future<StationStats[]>[] futures = new Future[chunkCount];

            final var chunkStartOffsets = new long[chunkCount];

            try (var file = new RandomAccessFile(new File(FILE), "r")) {
                for (int i = 1; i < chunkStartOffsets.length; i++) {
                    var start = length * i / chunkStartOffsets.length;
                    file.seek(start);
                    while (file.read() != (byte) '\n') {
                    }
                    start = file.getFilePointer();
                    chunkStartOffsets[i] = start;
                }
                MemorySegment mappedFile = file.getChannel().map(
                        MapMode.READ_ONLY, 0, length, Arena.global());

                for (int i = 0; i < chunkCount; i++) {
                    final long chunkStart = chunkStartOffsets[i];
                    final long chunkLimit = (i + 1 < chunkCount) ? chunkStartOffsets[i + 1] : length;
                    futures[i] = executorService
                            .submit(new ChunkProcessor(mappedFile.asSlice(chunkStart, chunkLimit - chunkStart)));
                }

                for (int i = 0; i < chunkCount; i++) {
                    results[i] = futures[i].get();
                }
                executorService.shutdown();

                var totalsMap = new TreeMap<String, StationStats>();
                for (var statsArray : results) {
                    for (var stats : statsArray) {
                        totalsMap.merge(stats.name, stats, (old, curr) -> {
                            old.count += curr.count;
                            old.sum += curr.sum;
                            old.min = Math.min(old.min, curr.min);
                            old.max = Math.max(old.max, curr.max);
                            return old;
                        });
                    }
                }
                System.out.println(totalsMap);

            }
        }

        static class ChunkProcessor extends AbstractCallable {
            private static final int HASHTABLE_SIZE = 2048;
            private final StatsAcc[] hashtable = new StatsAcc[HASHTABLE_SIZE];

            ChunkProcessor(MemorySegment chunk) {
                super(chunk);
            }

            @Override
            public StationStats[] call() throws Exception {
                for (var cursor = 0L; cursor < chunk.byteSize();) {
                    var semicolonPos = findByte(cursor, ';');
                    var newlinePos = findByte(semicolonPos + 1, '\n');
                    var intTemp = parseTemperature(semicolonPos);

                    var stats = findAcc(cursor, semicolonPos);

                    stats.sum += intTemp;
                    stats.count++;
                    stats.min = Math.min(stats.min, intTemp);
                    stats.max = Math.max(stats.max, intTemp);
                    cursor = newlinePos + 1;
                }

                return Arrays.stream(hashtable)
                        .filter(Objects::nonNull)
                        .map(acc -> new StationStats(acc, chunk))
                        .toArray(StationStats[]::new);
            }

            private StatsAcc findAcc(long cursor, long semicolonPos) {
                int hash = hash(cursor, semicolonPos);
                int initialPos = hash & (HASHTABLE_SIZE - 1);
                int slotPos = initialPos;
                while (true) {
                    var acc = hashtable[slotPos];
                    if (acc == null) {
                        acc = new StatsAcc(hash, cursor, semicolonPos - cursor);
                        hashtable[slotPos] = acc;
                        return acc;
                    }
                    if (acc.hash == hash && acc.nameEquals(chunk, cursor, semicolonPos)) {
                        return acc;
                    }
                    slotPos = (slotPos + 1) & (HASHTABLE_SIZE - 1);
                    if (slotPos == initialPos) {
                        throw new RuntimeException(String.format("hash %x, acc.hash %x", hash, acc.hash));
                    }
                }
            }

            private int hash(long startOffset, long limitOffset) {
                int h = 17;
                for (long off = startOffset; off < limitOffset; off++) {
                    h = 31 * h + ((int) chunk.get(JAVA_BYTE, off) & 0xFF);
                }
                return h;
            }
        }
    }

    /**
     * Optimization 4: sun.misc.Unsafe, SWAR
     * 
     * Duraton : s
     */
    static class Step4 {

        static void calculate() throws Exception {
            final var results = new StationStats[chunkCount][];
            @SuppressWarnings("unchecked")
            final Future<StationStats[]>[] futures = new Future[chunkCount];

            final var chunkStartOffsets = new long[chunkCount];

            try (var file = new RandomAccessFile(new File(FILE), "r")) {
                for (int i = 1; i < chunkStartOffsets.length; i++) {
                    var start = length * i / chunkStartOffsets.length;
                    file.seek(start);
                    while (file.read() != (byte) '\n') {
                    }
                    start = file.getFilePointer();
                    chunkStartOffsets[i] = start;
                }
                MemorySegment mappedFile = file.getChannel().map(
                        MapMode.READ_ONLY, 0, length, Arena.global());

                for (int i = 0; i < chunkCount; i++) {
                    final long chunkStart = chunkStartOffsets[i];
                    final long chunkLimit = (i + 1 < chunkCount) ? chunkStartOffsets[i + 1] : length;
                    futures[i] = executorService
                            .submit(new ChunkProcessor(mappedFile.asSlice(chunkStart, chunkLimit - chunkStart)));
                }

                for (int i = 0; i < chunkCount; i++) {
                    results[i] = futures[i].get();
                }
                executorService.shutdown();

                var totalsMap = new TreeMap<String, StationStats>();
                for (var statsArray : results) {
                    for (var stats : statsArray) {
                        totalsMap.merge(stats.name, stats, (old, curr) -> {
                            old.count += curr.count;
                            old.sum += curr.sum;
                            old.min = Math.min(old.min, curr.min);
                            old.max = Math.max(old.max, curr.max);
                            return old;
                        });
                    }
                }
                System.out.println(totalsMap);

            }
        }

        static class ChunkProcessor extends AbstractCallable {
            private static final int HASHTABLE_SIZE = 2048;
            private final StatsAcc[] hashtable = new StatsAcc[HASHTABLE_SIZE];

            ChunkProcessor(MemorySegment chunk) {
                super(chunk);
            }

            @Override
            public StationStats[] call() throws Exception {
                for (var cursor = 0L; cursor < chunk.byteSize();) {
                    var semicolonPos = findByte(cursor, ';');
                    var newlinePos = findByte(semicolonPos + 1, '\n');
                    var intTemp = parseTemperature(semicolonPos);

                    var stats = findAcc(cursor, semicolonPos);

                    stats.sum += intTemp;
                    stats.count++;
                    stats.min = Math.min(stats.min, intTemp);
                    stats.max = Math.max(stats.max, intTemp);
                    cursor = newlinePos + 1;
                }

                return Arrays.stream(hashtable)
                        .filter(Objects::nonNull)
                        .map(acc -> new StationStats(acc, chunk))
                        .toArray(StationStats[]::new);
            }

            private StatsAcc findAcc(long cursor, long semicolonPos) {
                int hash = hash(cursor, semicolonPos);
                int initialPos = hash & (HASHTABLE_SIZE - 1);
                int slotPos = initialPos;
                while (true) {
                    var acc = hashtable[slotPos];
                    if (acc == null) {
                        acc = new StatsAcc(hash, cursor, semicolonPos - cursor);
                        hashtable[slotPos] = acc;
                        return acc;
                    }
                    if (acc.hash == hash && acc.nameEquals(chunk, cursor, semicolonPos)) {
                        return acc;
                    }
                    slotPos = (slotPos + 1) & (HASHTABLE_SIZE - 1);
                    if (slotPos == initialPos) {
                        throw new RuntimeException(String.format("hash %x, acc.hash %x", hash, acc.hash));
                    }
                }
            }

            private int hash(long startOffset, long limitOffset) {
                int h = 17;
                for (long off = startOffset; off < limitOffset; off++) {
                    h = 31 * h + ((int) chunk.get(JAVA_BYTE, off) & 0xFF);
                }
                return h;
            }
        }
    }

}