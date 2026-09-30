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
 * High-scale performance benchmark on local Docker SQL instance:
 * 5 iterations with 1,000,000 rows each.
 * Compares standard on-heap ByteBuffer vs MemorySegment off-heap staging.
 */
public class LocalDockerMillionRowBenchmarkTest extends AbstractTest {

    private static final String TABLE_NAME = "perf_million_rows";
    private static final int TOTAL_ROWS = 1000000;
    private static final int BATCH_SIZE = 50000;
    private static final int ITERATIONS = 5;

    @BeforeAll
    public static void setupTable() throws Exception {
        try (Connection con = getConnection();
             Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            String createSql = "CREATE TABLE " + TABLE_NAME + " ("
                    + "id INT NOT NULL, "
                    + "big_val BIGINT NOT NULL, "
                    + "amount DECIMAL(18, 4) NOT NULL, "
                    + "small_id SMALLINT NOT NULL"
                    + ")";
            stmt.execute(createSql);
        }
    }

    @AfterAll
    public static void teardownTable() throws Exception {
        try (Connection con = getConnection();
             Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
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
    @DisplayName("Local Docker SQL Benchmark: 5 iterations x 1,000,000 rows")
    public void testMillionRowsBenchmark() throws Exception {
        System.out.println("==========================================================================");
        System.out.println("   STARTING LOCAL DOCKER BENCHMARK: " + ITERATIONS + " ITERATIONS x " + TOTAL_ROWS + " ROWS");
        System.out.println("==========================================================================");

        List<Long> standardTimes = new ArrayList<>();
        List<Long> memSegTimes = new ArrayList<>();
        List<Long> standardGcTimes = new ArrayList<>();
        List<Long> memSegGcTimes = new ArrayList<>();

        for (int i = 1; i <= ITERATIONS; i++) {
            System.out.println("\n>>> Running Iteration " + i + " of " + ITERATIONS + "...");

            // --- STANDARD ON-HEAP ---
            try (Connection con = getConnection();
                 Statement stmt = con.createStatement()) {
                stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
                System.gc();
                Thread.sleep(200);

                long gcBefore = getGcTime();
                long start = System.currentTimeMillis();

                runBulkInsert(con, TOTAL_ROWS, false);

                long elapsed = System.currentTimeMillis() - start;
                long gcTime = getGcTime() - gcBefore;
                standardTimes.add(elapsed);
                standardGcTimes.add(gcTime);
                System.out.printf("  [Iter %d] Standard On-Heap: %d ms (GC Time: %d ms) -> %,.0f rows/sec%n",
                        i, elapsed, gcTime, (TOTAL_ROWS / (elapsed / 1000.0)));
            }

            // --- MEMORYSEGMENT OFF-HEAP ---
            try (Connection con = getConnection();
                 Statement stmt = con.createStatement()) {
                stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
                System.gc();
                Thread.sleep(200);

                long gcBefore = getGcTime();
                long start = System.currentTimeMillis();

                runBulkInsert(con, TOTAL_ROWS, true);

                long elapsed = System.currentTimeMillis() - start;
                long gcTime = getGcTime() - gcBefore;
                memSegTimes.add(elapsed);
                memSegGcTimes.add(gcTime);
                System.out.printf("  [Iter %d] MemorySegment   : %d ms (GC Time: %d ms) -> %,.0f rows/sec%n",
                        i, elapsed, gcTime, (TOTAL_ROWS / (elapsed / 1000.0)));
            }
        }

        // Calculate averages
        double avgStandardTime = standardTimes.stream().mapToLong(Long::longValue).average().orElse(0);
        double avgMemSegTime = memSegTimes.stream().mapToLong(Long::longValue).average().orElse(0);
        double avgStandardGc = standardGcTimes.stream().mapToLong(Long::longValue).average().orElse(0);
        double avgMemSegGc = memSegGcTimes.stream().mapToLong(Long::longValue).average().orElse(0);

        double avgStandardRate = (TOTAL_ROWS / (avgStandardTime / 1000.0));
        double avgMemSegRate = (TOTAL_ROWS / (avgMemSegTime / 1000.0));
        double speedup = ((avgStandardTime - avgMemSegTime) / avgStandardTime) * 100.0;
        double throughputGain = ((avgMemSegRate - avgStandardRate) / avgStandardRate) * 100.0;

        System.out.println("\n==========================================================================");
        System.out.println("              LOCAL DOCKER 1M-ROW BENCHMARK FINAL SUMMARY");
        System.out.println("==========================================================================");
        System.out.printf("  Standard On-Heap Avg : %,.0f ms | Rate: %,.0f rows/sec | GC Time: %,.1f ms%n",
                avgStandardTime, avgStandardRate, avgStandardGc);
        System.out.printf("  MemorySegment    Avg : %,.0f ms | Rate: %,.0f rows/sec | GC Time: %,.1f ms%n",
                avgMemSegTime, avgMemSegRate, avgMemSegGc);
        System.out.printf("  Speedup Improvement  : %.2f%% faster overall%n", speedup);
        System.out.printf("  Throughput Gain      : +%.2f%% more rows/second%n", throughputGain);
        System.out.println("==========================================================================");

        // Write report file
        File reportFile = new File("target/docker-1M-benchmark.md");
        reportFile.getParentFile().mkdirs();
        try (PrintWriter pw = new PrintWriter(new FileWriter(reportFile))) {
            pw.println("# Local Docker SQL Benchmark: 1,000,000 Rows x 5 Iterations");
            pw.println();
            pw.println("- **Environment**: Docker (`mcr.microsoft.com/azure-sql-edge`) on localhost");
            pw.println("- **Total Rows per Iteration**: 1,000,000");
            pw.println("- **Batch Size**: " + BATCH_SIZE);
            pw.println("- **Total Iterations**: " + ITERATIONS);
            pw.println();
            pw.println("### Iteration Breakdown");
            pw.println("| Iteration | Standard On-Heap (ms) | MemorySegment (ms) | Speedup |");
            pw.println("|---|---|---|---|");
            for (int j = 0; j < ITERATIONS; j++) {
                double iterSpeedup = ((double) (standardTimes.get(j) - memSegTimes.get(j)) / standardTimes.get(j)) * 100.0;
                pw.printf("| Iteration %d | %,d ms | %,d ms | **%.2f%%** |%n", j + 1, standardTimes.get(j), memSegTimes.get(j), iterSpeedup);
            }
            pw.println();
            pw.println("### Aggregate Performance");
            pw.println("| Metric | Standard On-Heap ByteBuffer | MemorySegment Off-Heap | Delta |");
            pw.println("|---|---|---|---|");
            pw.printf("| **Average Duration** | %,.0f ms | %,.0f ms | **%.2f%% faster** |%n", avgStandardTime, avgMemSegTime, speedup);
            pw.printf("| **Average Throughput** | %,.0f rows/sec | %,.0f rows/sec | **+%.2f%%** |%n", avgStandardRate, avgMemSegRate, throughputGain);
            pw.printf("| **Average GC Time** | %,.1f ms | %,.1f ms | **%.1f%% reduction** |%n", avgStandardGc, avgMemSegGc,
                    avgStandardGc > 0 ? ((avgStandardGc - avgMemSegGc) / avgStandardGc) * 100.0 : 0.0);
        }
    }

    private void runBulkInsert(Connection con, int rows, boolean useMemorySegment) throws Exception {
        StreamBulkRecord record = new StreamBulkRecord(rows);
        try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
            SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
            options.setBatchSize(BATCH_SIZE);
            options.setBulkCopyTimeout(300);
            options.setUseMemorySegment(useMemorySegment);
            bulkCopy.setBulkCopyOptions(options);
            bulkCopy.setDestinationTableName(TABLE_NAME);
            bulkCopy.writeToServer(record);
        }
    }

    private static class StreamBulkRecord implements ISQLServerBulkData {
        private static final long serialVersionUID = 1L;
        private final int totalRows;
        private int currentRow = 0;
        private final BigDecimal sampleDecimal = new BigDecimal("12345.6789");

        StreamBulkRecord(int totalRows) {
            this.totalRows = totalRows;
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            return Set.of(1, 2, 3, 4);
        }

        @Override
        public String getColumnName(int column) {
            switch (column) {
                case 1: return "id";
                case 2: return "big_val";
                case 3: return "amount";
                case 4: return "small_id";
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
                default: return java.sql.Types.VARCHAR;
            }
        }

        @Override
        public int getPrecision(int column) {
            return column == 3 ? 18 : 0;
        }

        @Override
        public int getScale(int column) {
            return column == 3 ? 4 : 0;
        }

        @Override
        public Object[] getRowData() {
            return new Object[] {
                currentRow,
                5000000000L + currentRow,
                sampleDecimal,
                (short) (currentRow % 1000)
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
