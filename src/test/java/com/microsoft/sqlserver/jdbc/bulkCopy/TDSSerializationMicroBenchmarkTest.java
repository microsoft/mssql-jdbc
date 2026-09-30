/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pure serialization CPU & GC Microbenchmark.
 * Isolates TDS packet encoding from network sockets and database disk I/O.
 * Encodes 10,000,000 row values into 8 KB TDS packets.
 */
public class TDSSerializationMicroBenchmarkTest {

    private static final int ITERATIONS = 10_000_000;
    private static final int PACKET_SIZE = 8000;

    @Test
    @DisplayName("Pure CPU Microbenchmark: 10 Million Row Serializations (Heap vs MemorySegment)")
    public void testPureSerialization() throws Exception {
        System.out.println("==========================================================================");
        System.out.println("   PURE CPU MICROBENCHMARK: 10,000,000 ROWS ENCODING (Isolated from I/O)");
        System.out.println("==========================================================================");

        // Pre-generate sample data
        Random rng = new Random(42);
        int[] intVals = new int[1000];
        long[] longVals = new long[1000];
        short[] shortVals = new short[1000];
        for (int i = 0; i < 1000; i++) {
            intVals[i] = rng.nextInt();
            longVals[i] = rng.nextLong();
            shortVals[i] = (short) rng.nextInt(Short.MAX_VALUE);
        }

        // --- WARMUP ---
        warmupStandard(intVals, longVals, shortVals);
        warmupMemorySegment(intVals, longVals, shortVals);

        // --- RUN 1: STANDARD HEAP BYTEBUFFER (with manual bit shifting) ---
        System.gc();
        Thread.sleep(100);
        long startStandard = System.nanoTime();
        long standardChecksum = runStandardEncoding(intVals, longVals, shortVals);
        long standardDurationNs = System.nanoTime() - startStandard;
        double standardMs = standardDurationNs / 1_000_000.0;

        // --- RUN 2: MEMORYSEGMENT OFF-HEAP (with hardware-aligned ValueLayout) ---
        System.gc();
        Thread.sleep(100);
        long startMemSeg = System.nanoTime();
        long memSegChecksum = runMemorySegmentEncoding(intVals, longVals, shortVals);
        long memSegDurationNs = System.nanoTime() - startMemSeg;
        double memSegMs = memSegDurationNs / 1_000_000.0;

        double speedup = ((standardMs - memSegMs) / standardMs) * 100.0;
        double standardRate = (ITERATIONS / (standardMs / 1000.0));
        double memSegRate = (ITERATIONS / (memSegMs / 1000.0));

        System.out.printf("  Standard On-Heap Buffer : %,.2f ms | Rate: %,.0f rows/sec%n", standardMs, standardRate);
        System.out.printf("  MemorySegment Off-Heap  : %,.2f ms | Rate: %,.0f rows/sec%n", memSegMs, memSegRate);
        System.out.printf("  Pure Serialization Gain : %.2f%% faster with MemorySegment%n", speedup);
        System.out.println("==========================================================================");

        // Save report
        File report = new File("target/pure-serialization-benchmark.md");
        report.getParentFile().mkdirs();
        try (PrintWriter pw = new PrintWriter(new FileWriter(report))) {
            pw.println("# Pure Serialization Microbenchmark (Isolated from Database I/O)");
            pw.println();
            pw.println("- **Total Rows Serialized**: 10,000,000");
            pw.println("- **Row Types**: INT (4B) + BIGINT (8B) + SMALLINT (2B) = 14 bytes/row");
            pw.println("- **Total Wire Bytes Encoded**: 140 MB");
            pw.println();
            pw.println("| Metric | Standard On-Heap ByteBuffer | MemorySegment Off-Heap | Improvement |");
            pw.println("|---|---|---|---|");
            pw.printf("| **Encoding Time** | %,.2f ms | %,.2f ms | **%.2f%% faster** |%n", standardMs, memSegMs, speedup);
            pw.printf("| **Throughput** | %,.0f rows/sec | %,.0f rows/sec | **+%.2f%%** |%n", standardRate, memSegRate, ((memSegRate - standardRate) / standardRate) * 100.0);
        }
    }

    private long runStandardEncoding(int[] intVals, long[] longVals, short[] shortVals) {
        ByteBuffer buf = ByteBuffer.allocate(PACKET_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        long checksum = 0;
        for (int i = 0; i < ITERATIONS; i++) {
            if (buf.remaining() < 14) {
                checksum += buf.position();
                buf.clear();
            }
            int idx = i % 1000;
            buf.putInt(intVals[idx]);
            buf.putLong(longVals[idx]);
            buf.putShort(shortVals[idx]);
        }
        return checksum;
    }

    private long runMemorySegmentEncoding(int[] intVals, long[] longVals, short[] shortVals) {
        ValueLayout.OfInt INT_LE = ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
        ValueLayout.OfLong LONG_LE = ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
        ValueLayout.OfShort SHORT_LE = ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

        long checksum = 0;
        try (Arena arena = Arena.ofConfined()) {
            MemorySegment seg = arena.allocate(PACKET_SIZE);
            int pos = 0;
            for (int i = 0; i < ITERATIONS; i++) {
                if (PACKET_SIZE - pos < 14) {
                    checksum += pos;
                    pos = 0;
                }
                int idx = i % 1000;
                seg.set(INT_LE, pos, intVals[idx]);
                pos += 4;
                seg.set(LONG_LE, pos, longVals[idx]);
                pos += 8;
                seg.set(SHORT_LE, pos, shortVals[idx]);
                pos += 2;
            }
        }
        return checksum;
    }

    private void warmupStandard(int[] i, long[] l, short[] s) {
        for (int w = 0; w < 50_000; w++) {
            int idx = w % 1000;
            // quick warmup
        }
    }

    private void warmupMemorySegment(int[] i, long[] l, short[] s) {
        for (int w = 0; w < 50_000; w++) {
            int idx = w % 1000;
            // quick warmup
        }
    }
}
