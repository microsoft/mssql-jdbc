/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;

/**
 * High-stress enterprise benchmark:
 * 4 concurrent worker threads under constrained heap, streaming 500,000 rows each (2,000,000 rows total)
 * with mixed schema: INT, BIGINT, DECIMAL, SMALLINT, and a 256-byte payload.
 *
 * Compares standard on-heap ByteBuffer vs MemorySegment off-heap staging under heap pressure.
 */
public class ConcurrentMemoryStressBenchmarkTest extends AbstractTest {

    private static final int CONCURRENT_THREADS = 4;
    private static final int ROWS_PER_THREAD = 250000; // 1,000,000 rows total per run
    private static final int BATCH_SIZE = 25000;

    @BeforeAll
    public static void setupTables() throws Exception {
        try (Connection con = getConnection();
             Statement stmt = con.createStatement()) {
            for (int t = 0; t < CONCURRENT_THREADS; t++) {
                String tableName = "stress_perf_" + t;
                TestUtils.dropTableIfExists(tableName, stmt);
                String createSql = "CREATE TABLE " + tableName + " ("
                        + "id INT NOT NULL, "
                        + "big_val BIGINT NOT NULL, "
                        + "amount DECIMAL(18, 4) NOT NULL, "
                        + "small_id SMALLINT NOT NULL, "
                        + "payload VARCHAR(256) NOT NULL"
                        + ")";
                stmt.execute(createSql);
            }
        }
    }

    @AfterAll
    public static void teardownTables() throws Exception {
        try (Connection con = getConnection();
             Statement stmt = con.createStatement()) {
            for (int t = 0; t < CONCURRENT_THREADS; t++) {
                TestUtils.dropTableIfExists("stress_perf_" + t, stmt);
            }
        }
    }

    private static long getGcTime() {
        long total = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            total += gc.getCollectionTime();
        }
        return total;
    }

    private static long getGcCount() {
        long total = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            total += gc.getCollectionCount();
        }
        return total;
    }

    @Test
    @DisplayName("Concurrent Stress Benchmark: 4 Threads x 250,000 Rows (1M rows total)")
    public void testConcurrentStressBenchmark() throws Exception {
        int totalRows = CONCURRENT_THREADS * ROWS_PER_THREAD;

        System.out.println("==========================================================================");
        System.out.println("   CONCURRENT MEMORY STRESS BENCHMARK (4 Threads x " + ROWS_PER_THREAD + " = " + totalRows + " rows)");
        System.out.println("==========================================================================");

        // -------------------------------------------------------------
        // RUN 1: STANDARD ON-HEAP BYTEBUFFER
        // -------------------------------------------------------------
        truncateAllTables();
        System.gc();
        Thread.sleep(200);

        long gcTimeBeforeStandard = getGcTime();
        long gcCountBeforeStandard = getGcCount();
        long startStandard = System.currentTimeMillis();

        runConcurrentWorkload(false);

        long standardElapsed = System.currentTimeMillis() - startStandard;
        long standardGcTime = getGcTime() - gcTimeBeforeStandard;
        long standardGcCount = getGcCount() - gcCountBeforeStandard;
        double standardThroughput = (totalRows / (standardElapsed / 1000.0));

        System.out.printf("  [Standard On-Heap] Elapsed: %d ms | Throughput: %,.0f rows/sec | GC Time: %d ms (%d cycles)%n",
                standardElapsed, standardThroughput, standardGcTime, standardGcCount);

        // -------------------------------------------------------------
        // RUN 2: MEMORYSEGMENT OFF-HEAP STAGING
        // -------------------------------------------------------------
        truncateAllTables();
        System.gc();
        Thread.sleep(200);

        long gcTimeBeforeMemSeg = getGcTime();
        long gcCountBeforeMemSeg = getGcCount();
        long startMemSeg = System.currentTimeMillis();

        runConcurrentWorkload(true);

        long memSegElapsed = System.currentTimeMillis() - startMemSeg;
        long memSegGcTime = getGcTime() - gcTimeBeforeMemSeg;
        long memSegGcCount = getGcCount() - gcCountBeforeMemSeg;
        double memSegThroughput = (totalRows / (memSegElapsed / 1000.0));

        System.out.printf("  [MemorySegment   ] Elapsed: %d ms | Throughput: %,.0f rows/sec | GC Time: %d ms (%d cycles)%n",
                memSegElapsed, memSegThroughput, memSegGcTime, memSegGcCount);

        // -------------------------------------------------------------
        // SUMMARY REPORT
        // -------------------------------------------------------------
        double speedup = ((double) (standardElapsed - memSegElapsed) / standardElapsed) * 100.0;
        double throughputGain = ((memSegThroughput - standardThroughput) / standardThroughput) * 100.0;
        double gcReduction = standardGcTime > 0 ? ((double) (standardGcTime - memSegGcTime) / standardGcTime) * 100.0 : 0.0;

        System.out.println("\n==========================================================================");
        System.out.println("                   FINAL STRESS BENCHMARK SUMMARY");
        System.out.println("==========================================================================");
        System.out.printf("  Standard On-Heap : %,d ms | Throughput: %,.0f rows/sec | GC Time: %d ms%n",
                standardElapsed, standardThroughput, standardGcTime);
        System.out.printf("  MemorySegment    : %,d ms | Throughput: %,.0f rows/sec | GC Time: %d ms%n",
                memSegElapsed, memSegThroughput, memSegGcTime);
        System.out.printf("  Speedup          : %.2f%% faster%n", speedup);
        System.out.printf("  Throughput Gain  : +%.2f%% more rows/sec%n", throughputGain);
        System.out.printf("  GC Time Reduction: %.2f%%%n", gcReduction);
        System.out.println("==========================================================================");

        // Write report file
        File reportFile = new File("target/concurrent-stress-benchmark.md");
        reportFile.getParentFile().mkdirs();
        try (PrintWriter pw = new PrintWriter(new FileWriter(reportFile))) {
            pw.println("# Concurrent Stress Benchmark: 4 Threads x 250,000 Rows (1,000,000 Rows Total)");
            pw.println();
            pw.println("- **Threads**: " + CONCURRENT_THREADS);
            pw.println("- **Rows per Thread**: " + ROWS_PER_THREAD);
            pw.println("- **Total Rows**: " + totalRows);
            pw.println("- **Row Schema**: `INT`, `BIGINT`, `DECIMAL(18,4)`, `SMALLINT`, `VARCHAR(256)`");
            pw.println();
            pw.println("| Metric | Standard On-Heap ByteBuffer | MemorySegment Off-Heap | Improvement |");
            pw.println("|---|---|---|---|");
            pw.printf("| **Total Time** | %,d ms | %,d ms | **%.2f%% faster** |%n", standardElapsed, memSegElapsed, speedup);
            pw.printf("| **Throughput** | %,.0f rows/sec | %,.0f rows/sec | **+%.2f%%** |%n", standardThroughput, memSegThroughput, throughputGain);
            pw.printf("| **GC Collections** | %d cycles | %d cycles | - |%n", standardGcCount, memSegGcCount);
            pw.printf("| **GC Pause Time** | %d ms | %d ms | **%.2f%% reduction** |%n", standardGcTime, memSegGcTime, gcReduction);
        }
    }

    private void truncateAllTables() throws Exception {
        try (Connection con = getConnection();
             Statement stmt = con.createStatement()) {
            for (int t = 0; t < CONCURRENT_THREADS; t++) {
                stmt.execute("TRUNCATE TABLE stress_perf_" + t);
            }
        }
    }

    private void runConcurrentWorkload(boolean useMemorySegment) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_THREADS);
        List<Callable<Void>> tasks = new ArrayList<>();

        for (int t = 0; t < CONCURRENT_THREADS; t++) {
            final int threadId = t;
            tasks.add(() -> {
                try (Connection con = getConnection()) {
                    StressBulkRecord record = new StressBulkRecord(ROWS_PER_THREAD);
                    try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
                        SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                        options.setBatchSize(BATCH_SIZE);
                        options.setBulkCopyTimeout(300);
                        options.setUseMemorySegment(useMemorySegment);
                        bulkCopy.setBulkCopyOptions(options);
                        bulkCopy.setDestinationTableName("stress_perf_" + threadId);
                        bulkCopy.writeToServer(record);
                    }
                }
                return null;
            });
        }

        List<Future<Void>> futures = executor.invokeAll(tasks);
        for (Future<Void> future : futures) {
            future.get();
        }
        executor.shutdown();

        // Verify counts
        try (Connection con = getConnection();
             Statement stmt = con.createStatement()) {
            for (int t = 0; t < CONCURRENT_THREADS; t++) {
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM stress_perf_" + t)) {
                    assertTrue(rs.next());
                    assertEquals(ROWS_PER_THREAD, rs.getInt(1));
                }
            }
        }
    }

    private static class StressBulkRecord implements ISQLServerBulkData {
        private static final long serialVersionUID = 1L;
        private final int totalRows;
        private int currentRow = 0;
        private final BigDecimal sampleDecimal = new BigDecimal("987654.3210");
        private final String samplePayload = "TelemetryData-Node-EastUS-Region-Zone1-MetricCode-991283-Status-OK-Payload-Segment-Verification";

        StressBulkRecord(int totalRows) {
            this.totalRows = totalRows;
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            return Set.of(1, 2, 3, 4, 5);
        }

        @Override
        public String getColumnName(int column) {
            switch (column) {
                case 1: return "id";
                case 2: return "big_val";
                case 3: return "amount";
                case 4: return "small_id";
                case 5: return "payload";
                default: return "";
            }
        }

        @Override
        public int getColumnType(int column) {
            switch (column) {
                case 1: return java.sql.Types.INTEGER;
                case 2: return java.sql.Types.BIGINT;
                case 3: return java.sql.Types.DECIMAL;
                case 4: return java.sql.Types.SMALLINT;
                case 5: return java.sql.Types.VARCHAR;
                default: return java.sql.Types.VARCHAR;
            }
        }

        @Override
        public int getPrecision(int column) {
            if (column == 3) return 18;
            if (column == 5) return 256;
            return 0;
        }

        @Override
        public int getScale(int column) {
            return column == 3 ? 4 : 0;
        }

        @Override
        public Object[] getRowData() {
            return new Object[] {
                currentRow,
                8000000000L + currentRow,
                sampleDecimal,
                (short) (currentRow % 1000),
                samplePayload
            };
        }

        @Override
        public boolean next() {
            if (currentRow < totalRows) {
                currentRow++;
                return true;
            }
            return false;
        }
    }
}
