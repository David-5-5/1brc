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
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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

import sun.misc.Unsafe;

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
        Step5.calculate();
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

        /**
         * 
         * @param acc
         * @since Step4
         */
        StationStats(StatsAcc acc) {
            name = acc.exportNameString();
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
        int nameLen;
        long hash;
        int sum;
        int count;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        /**
         * @since Step4
         */
        long[] name;
        // end of added by Step4

        /**
         * @since Step5
         */
        private static final long[] emptyTail = new long[0];
        private static final int NAMETAIL_OFFSET = 2 * Long.BYTES;
        boolean exportNameString2 = false;
        long nameWord0;
        long nameWord1;
        long[] nameTail;
        // end of added by Step5

        StatsAcc(long hash, long nameOffset, int nameLen) {
            this.hash = hash;
            this.nameOffset = nameOffset;
            this.nameLen = nameLen;
        }

        /**
         * 
         * @param inputBase
         * @param hash
         * @param nameStartOffset
         * @param nameLen
         * @param lastNameWord
         * 
         * @since Step4
         */
        public StatsAcc(long inputBase, long hash, long nameStartOffset, int nameLen, long lastNameWord) {
            this.hash = hash;
            this.nameLen = nameLen;
            name = new long[(nameLen - 1) / 8 + 1];
            for (int i = 0; i < name.length - 1; i++) {
                name[i] = getLong(inputBase, nameStartOffset + i * Long.BYTES);
            }
            name[name.length - 1] = lastNameWord;
        }

        /**
         * 
         * @param inputBase
         * @param hash
         * @param nameStartOffset
         * @param nameLen
         * @param nameWord0
         * @param nameWord1
         * @param lastNameWord
         * 
         * @since Step5
         */
        public StatsAcc(long inputBase, long hash, long nameStartOffset, int nameLen,
                        long nameWord0, long nameWord1, long lastNameWord) {
            this.hash = hash;
            this.nameLen = nameLen;
            this.nameWord0 = nameWord0;
            this.nameWord1 = nameWord1;
            int nameTailLen = (nameLen - 1) / 8 - 1;
            if (nameTailLen > 0) {
                nameTail = new long[nameTailLen];
                int i = 0;
                for (; i < nameTailLen - 1; i++) {
                    nameTail[i] = getLong(inputBase, nameStartOffset + (i + 2L) * Long.BYTES);
                }
                nameTail[i] = lastNameWord;
            }
            else {
                nameTail = emptyTail;
            }
            exportNameString2 = true;
        }

        public boolean nameEquals(MemorySegment chunk, long otherNameOffset, long otherNameLimit) {
            var otherNameLen = otherNameLimit - otherNameOffset;
            // Avoid construct instance of String for performance
            return nameLen == otherNameLen &&
                    chunk.asSlice(nameOffset, nameLen).mismatch(chunk.asSlice(otherNameOffset, nameLen)) == -1;
        }

        /**
         * 
         * @param nameWord0
         * @param nameWord1
         * @return
         * @since Step5
         */
        boolean nameEquals2(long nameWord0, long nameWord1) {
            return this.nameWord0 == nameWord0 && this.nameWord1 == nameWord1;
        }

        /**
         * 
         * @param inputBase
         * @param inputNameStart
         * @param inputNameLen
         * @param inputWord0
         * @param inputWord1
         * @param lastInputWord
         * @return
         * @since Step5
         */
        boolean nameEquals(long inputBase, long inputNameStart, long inputNameLen, long inputWord0, long inputWord1, long lastInputWord) {
            boolean mismatch0 = inputWord0 != nameWord0;
            boolean mismatch1 = inputWord1 != nameWord1;
            boolean mismatch = mismatch0 | mismatch1;
            if (mismatch | inputNameLen <= NAMETAIL_OFFSET) {
                return !mismatch;
            }
            int i = NAMETAIL_OFFSET;
            for (; i <= inputNameLen - Long.BYTES; i += Long.BYTES) {
                if (getLong(inputBase, inputNameStart + i) != nameTail[(i - NAMETAIL_OFFSET) / 8]) {
                    return false;
                }
            }
            return i == inputNameLen || lastInputWord == nameTail[(i - NAMETAIL_OFFSET) / 8];
        }

        /**
         * 
         * @param base
         * @param offset
         * @return
         * @since Step4
         */
        private static long getLong(long base, long offset) {
            return UNSAFE.getLong(base + offset);
        }

        /**
         * 
         * @param inputBase
         * @param inputNameStart
         * @param inputNameLen
         * @param lastInputWord
         * @return
         * @since Step4
         */
        boolean nameEquals(long inputBase, long inputNameStart, long inputNameLen, long lastInputWord) {
            int i = 0;
            for (; i <= inputNameLen - Long.BYTES; i += Long.BYTES) {
                if (getLong(inputBase, inputNameStart + i) != name[i / 8]) {
                    return false;
                }
            }
            return i == inputNameLen || lastInputWord == name[i / 8];
        }

        /**
         * 
         * @param temperature
         * @since Step4
         */
        void observe(int temperature) {
            sum += temperature;
            count++;
            min = Math.min(min, temperature);
            max = Math.max(max, temperature);
        }

        /**
         * 
         * @return
         * @since Step4
         */
        String exportNameString() {
            if (exportNameString2)
                return exportNameString2();
            var buf = ByteBuffer.allocate(name.length * 8).order(ByteOrder.LITTLE_ENDIAN);
            for (long nameWord : name) {
                buf.putLong(nameWord);
            }
            buf.flip();
            final var bytes = new byte[nameLen - 1];
            buf.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }

        /**
         * 
         * @return
         * @since Step5
         */
        private String exportNameString2() {
            var buf = ByteBuffer.allocate((2 + nameTail.length) * 8).order(ByteOrder.LITTLE_ENDIAN);
            buf.putLong(nameWord0);
            buf.putLong(nameWord1);
            for (long nameWord : nameTail) {
                buf.putLong(nameWord);
            }
            buf.flip();
            final var bytes = new byte[nameLen - 1];
            buf.get(bytes);
            return new String(bytes, StandardCharsets.UTF_8);
        }
    }

    /**
     * Optimization 3: Custom hashtable
     * 
     * Duraton : 1.643s
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
                        acc = new StatsAcc(hash, cursor, (int) (semicolonPos - cursor));
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
     * 获取实例的方法是通过静态方法 getUnsafe()。 需要注意的是，默认情况下 - 这将引发 SecurityException。
     * 幸运的是，我们可以使用反射来获取实例
     */
    private static final Unsafe UNSAFE = unsafe();

    private static Unsafe unsafe() {
        try {
            ;
            Field theUnsafe = Unsafe.class.getDeclaredField("theUnsafe");
            theUnsafe.setAccessible(true);
            return (Unsafe) theUnsafe.get(Unsafe.class);
        }
        catch (NoSuchFieldException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Optimization 4: sun.misc.Unsafe, SWAR
     * 
     * Duraton : 0.681s
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
            private final long inputBase;
            private final long inputSize;

            ChunkProcessor(MemorySegment chunk) {
                super(chunk);
                inputBase = chunk.address();
                inputSize = chunk.byteSize();
            }

            @Override
            public StationStats[] call() throws Exception {
                long cursor = 0;
                while (cursor < inputSize) {
                    long nameStartOffset = cursor;
                    long hash = 0;
                    int nameLen = 0;
                    while (true) {
                        // 使用 unsafe 直接读取内存
                        long nameWord = UNSAFE.getLong(inputBase + nameStartOffset + nameLen);
                        // seek for semicolon ';'
                        long matchBits = semicolonMatchBits(nameWord);
                        if (matchBits != 0) {
                            // Find the ';'
                            nameLen += nameLen(matchBits);
                            nameWord = maskWord(nameWord, matchBits);
                            hash = hash(hash, nameWord);
                            cursor += nameLen;
                            long tempWord = UNSAFE.getLong(inputBase + cursor);
                            int dotPos = dotPos(tempWord);
                            int temperature = parseTemperature(tempWord, dotPos);
                            cursor += (dotPos >> 3) + 3;
                            findAcc(hash, nameStartOffset, nameLen, nameWord).observe(temperature);
                            break;
                        }
                        hash = hash(hash, nameWord);
                        nameLen += Long.BYTES;
                    }
                }

                return Arrays.stream(hashtable)
                        .filter(Objects::nonNull)
                        .map(StationStats::new)
                        .toArray(StationStats[]::new);
            }

            private StatsAcc findAcc(long hash, long nameStartOffset, int nameLen, long lastNameWord) {
                int initialPos = (int) hash & (HASHTABLE_SIZE - 1);
                int slotPos = initialPos;
                while (true) {
                    var acc = hashtable[slotPos];
                    if (acc == null) {
                        acc = new StatsAcc(inputBase, hash, nameStartOffset, nameLen, lastNameWord);
                        ;
                        hashtable[slotPos] = acc;
                        return acc;
                    }
                    if (acc.nameEquals(inputBase, nameStartOffset, nameLen, lastNameWord)) {
                        return acc;
                    }
                    slotPos = (slotPos + 1) & (HASHTABLE_SIZE - 1);
                    if (slotPos == initialPos) {
                        throw new RuntimeException(String.format("hash %x, acc.hash %x", hash, acc.hash));
                    }
                }
            }

            private static final long BROADCAST_SEMICOLON = 0x3B3B3B3B3B3B3B3BL;
            private static final long BROADCAST_0x01 = 0x0101010101010101L;
            private static final long BROADCAST_0x80 = 0x8080808080808080L;

            private static long semicolonMatchBits(long word) {
                long diff = word ^ BROADCAST_SEMICOLON;
                return (diff - BROADCAST_0x01) & (~diff & BROADCAST_0x80);
            }

            // credit: artsiomkorzun
            private static long maskWord(long word, long matchBits) {
                long mask = matchBits ^ (matchBits - 1);
                return word & mask;
            }

            private static int nameLen(long separator) {
                return (Long.numberOfTrailingZeros(separator) >>> 3) + 1;
            }

            private static long hash(long prevHash, long word) {
                return Long.rotateLeft((prevHash ^ word) * 0x51_7c_c1_b7_27_22_0a_95L, 13);
            }

            private static final long DOT_BITS = 0x10101000;

            // credit: merykitty
            // The 4th binary digit of the ascii of a digit is 1 while
            // that of the '.' is 0. This finds the decimal separator.
            // The value can be 12, 20, 28
            private static int dotPos(long word) {
                return Long.numberOfTrailingZeros(~word & DOT_BITS);
            }

            private static final long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

            // credit: merykitty and royvanrijn
            private static int parseTemperature(long numberBytes, int dotPos) {
                // numberBytes contains the number: X.X, -X.X, XX.X or -XX.X
                final long invNumberBytes = ~numberBytes;

                // Calculates the sign
                final long signed = (invNumberBytes << 59) >> 63;
                final int _28MinusDotPos = (dotPos ^ 0b11100);
                final long minusFilter = ~(signed & 0xFF);
                // Use the pre-calculated decimal position to adjust the values
                final long digits = ((numberBytes & minusFilter) << _28MinusDotPos) & 0x0F000F0F00L;

                // Multiply by a magic (100 * 0x1000000 + 10 * 0x10000 + 1), to get the result
                final long absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
                // And apply the sign
                return (int) ((absValue + signed) ^ signed);
            }
        }
    }

    /**
     * Optimization 5: Win with statistics
     * 
     * upgrade from Step4
     * 
     * Duraton : 0.524s
     */
    static class Step5 {

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
            private static final int HASHTABLE_SIZE = 4096;
            private final StatsAcc[] hashtable = new StatsAcc[HASHTABLE_SIZE];
            private final long inputBase;
            private final long inputSize;

            ChunkProcessor(MemorySegment chunk) {
                super(chunk);
                inputBase = chunk.address();
                inputSize = chunk.byteSize();
            }

            @Override
            public StationStats[] call() throws Exception {
                long cursor = 0;
                long lastNameWord;
                while (cursor < inputSize) {
                    long nameStartOffset = cursor;
                    long nameWord0 = getLong(nameStartOffset);
                    long nameWord1 = getLong(nameStartOffset + Long.BYTES);
                    long matchBits0 = semicolonMatchBits(nameWord0);
                    long matchBits1 = semicolonMatchBits(nameWord1);

                    int temperature;
                    StatsAcc acc;
                    long hash;
                    int nameLen;
                    if ((matchBits0 | matchBits1) != 0) {
                        int nameLen0 = nameLen(matchBits0);
                        int nameLen1 = nameLen(matchBits1);
                        nameWord0 = maskWord(nameWord0, matchBits0);
                        // bit 3 of nameLen0 is on iff semicolon is not in nameWord0.
                        // this broadcasts bit 3 across the whole long word.
                        long nameWord1Mask = (long) nameLen0 << 60 >> 63;
                        // nameWord1 must be zero if semicolon is in nameWord0
                        nameWord1 = maskWord(nameWord1, matchBits1) & nameWord1Mask;
                        nameLen1 &= (int) (nameWord1Mask & 0b111);
                        nameLen = nameLen0 + nameLen1 + 1; // we'll include the semicolon in the name
                        lastNameWord = (nameWord0 & ~nameWord1Mask) | nameWord1;

                        cursor += nameLen;
                        long tempWord = getLong(cursor);
                        int dotPos = dotPos(tempWord);
                        temperature = parseTemperature(tempWord, dotPos);

                        cursor += (dotPos >> 3) + 3;
                        hash = hash(nameWord0);
                        acc = findAcc2(hash, nameWord0, nameWord1);
                        if (acc != null) {
                            acc.observe(temperature);
                            continue;
                        }
                    }
                    else {
                        hash = hash(nameWord0);
                        nameLen = 2 * Long.BYTES;
                        while (true) {
                            lastNameWord = getLong(nameStartOffset + nameLen);
                            long matchBits = semicolonMatchBits(lastNameWord);
                            if (matchBits != 0) {
                                nameLen += nameLen(matchBits) + 1;
                                lastNameWord = maskWord(lastNameWord, matchBits);
                                cursor += nameLen;
                                long tempWord = getLong(cursor);
                                int dotPos = dotPos(tempWord);
                                temperature = parseTemperature(tempWord, dotPos);
                                cursor += (dotPos >> 3) + 3;
                                break;
                            }
                            nameLen += Long.BYTES;
                        }
                    }
                    ensureAcc(hash, nameStartOffset, nameLen, nameWord0, nameWord1, lastNameWord).observe(temperature);
                }

                return Arrays.stream(hashtable)
                        .filter(Objects::nonNull)
                        .map(StationStats::new)
                        .toArray(StationStats[]::new);
            }

            private StatsAcc findAcc2(long hash, long nameWord0, long nameWord1) {
                int slotPos = (int) hash & (HASHTABLE_SIZE - 1);
                var acc = hashtable[slotPos];
                if (acc != null && acc.hash == hash && acc.nameEquals2(nameWord0, nameWord1)) {
                    return acc;
                }
                return null;
            }

            private StatsAcc ensureAcc(long hash, long nameStartOffset, int nameLen,
                                       long nameWord0, long nameWord1, long lastNameWord) {
                int initialPos = (int) hash & (HASHTABLE_SIZE - 1);
                int slotPos = initialPos;
                while (true) {
                    var acc = hashtable[slotPos];
                    if (acc == null) {
                        acc = new StatsAcc(inputBase, hash, nameStartOffset, nameLen, nameWord0, nameWord1,
                                lastNameWord);
                        hashtable[slotPos] = acc;
                        return acc;
                    }
                    if (acc.hash == hash && acc.nameEquals(inputBase, nameStartOffset, nameLen, nameWord0, nameWord1,
                            lastNameWord)) {
                        return acc;
                    }
                    slotPos = (slotPos + 1) & (HASHTABLE_SIZE - 1);
                    if (slotPos == initialPos) {
                        throw new RuntimeException(String.format("hash %x, acc.hash %x", hash, acc.hash));
                    }
                }
            }

            private long getLong(long offset) {
                return UNSAFE.getLong(inputBase + offset);
            }

            private static final long BROADCAST_SEMICOLON = 0x3B3B3B3B3B3B3B3BL;
            private static final long BROADCAST_0x01 = 0x0101010101010101L;
            private static final long BROADCAST_0x80 = 0x8080808080808080L;

            private static long semicolonMatchBits(long word) {
                long diff = word ^ BROADCAST_SEMICOLON;
                return (diff - BROADCAST_0x01) & (~diff & BROADCAST_0x80);
            }

            // credit: artsiomkorzun
            private static long maskWord(long word, long matchBits) {
                long mask = matchBits ^ (matchBits - 1);
                return word & mask;
            }

            private static int nameLen(long separator) {
                return (Long.numberOfTrailingZeros(separator) >>> 3);
            }

            private static long hash(long word) {
                return Long.rotateLeft(word * 0x51_7c_c1_b7_27_22_0a_95L, 17);
            }

            private static final long DOT_BITS = 0x10101000;

            // credit: merykitty
            // The 4th binary digit of the ascii of a digit is 1 while
            // that of the '.' is 0. This finds the decimal separator.
            // The value can be 12, 20, 28
            private static int dotPos(long word) {
                return Long.numberOfTrailingZeros(~word & DOT_BITS);
            }

            private static final long MAGIC_MULTIPLIER = (100 * 0x1000000 + 10 * 0x10000 + 1);

            // credit: merykitty and royvanrijn
            private static int parseTemperature(long numberBytes, int dotPos) {
                // numberBytes contains the number: X.X, -X.X, XX.X or -XX.X
                final long invNumberBytes = ~numberBytes;

                // Calculates the sign
                final long signed = (invNumberBytes << 59) >> 63;
                final int _28MinusDotPos = (dotPos ^ 0b11100);
                final long minusFilter = ~(signed & 0xFF);
                // Use the pre-calculated decimal position to adjust the values
                final long digits = ((numberBytes & minusFilter) << _28MinusDotPos) & 0x0F000F0F00L;

                // Multiply by a magic (100 * 0x1000000 + 10 * 0x10000 + 1), to get the result
                final long absValue = ((digits * MAGIC_MULTIPLIER) >>> 32) & 0x3FF;
                // And apply the sign
                return (int) ((absValue + signed) ^ signed);
            }
        }
    }

}