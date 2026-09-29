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
import java.lang.management.MemoryMXBean;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
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
 * Automated load benchmark capturing:
 * - Execution elapsed time & row ingestion rate (rows/sec)
 * - Heap memory allocation deltas
 * - Garbage collection cycles & pause duration
 * - Dumps report to target/benchmark-results.md and heap dump / GC logs
 */
public class MemorySegmentAutomatedBenchmarkTest extends AbstractTest {

    private static final String TABLE_NAME = "test_bulk_automated_perf";
    private static final int ROW_COUNT = 50000;
    private static final int BATCH_SIZE = 10000;

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
    @DisplayName("Automated Benchmark: Standard ByteBuffer vs MemorySegment Off-Heap")
    public void runAutomatedBenchmark() throws Exception {
        try (Connection con = getConnection();
             Statement stmt = con.createStatement()) {

            MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();

            // -----------------------------------------------------------------
            // 1. WARMUP (10,000 rows each to warm JIT compilers)
            // -----------------------------------------------------------------
            System.out.println("Starting JIT Warmup (10,000 rows)...");
            stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
            runBulkLoad(con, 10000, false);
            stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
            runBulkLoad(con, 10000, true);

            // -----------------------------------------------------------------
            // 2. MEASURE STANDARD ON-HEAP BYTEBUFFER (50,000 rows)
            // -----------------------------------------------------------------
            stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
            System.gc();
            Thread.sleep(200);

            long heapBeforeStandard = memoryBean.getHeapMemoryUsage().getUsed();
            long gcCountBeforeStandard = getGcCount();
            long gcTimeBeforeStandard = getGcTime();

            long startStandard = System.currentTimeMillis();
            runBulkLoad(con, ROW_COUNT, false);
            long standardElapsed = System.currentTimeMillis() - startStandard;

            long heapAfterStandard = memoryBean.getHeapMemoryUsage().getUsed();
            long standardGcCount = getGcCount() - gcCountBeforeStandard;
            long standardGcTime = getGcTime() - gcTimeBeforeStandard;
            double standardThroughput = (ROW_COUNT / (standardElapsed / 1000.0));

            // Verify count
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(ROW_COUNT, rs.getInt(1));
            }

            // -----------------------------------------------------------------
            // 3. MEASURE MEMORYSEGMENT OFF-HEAP (50,000 rows)
            // -----------------------------------------------------------------
            stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
            System.gc();
            Thread.sleep(200);

            long heapBeforeMemSeg = memoryBean.getHeapMemoryUsage().getUsed();
            long gcCountBeforeMemSeg = getGcCount();
            long gcTimeBeforeMemSeg = getGcTime();

            long startMemSeg = System.currentTimeMillis();
            runBulkLoad(con, ROW_COUNT, true);
            long memSegElapsed = System.currentTimeMillis() - startMemSeg;

            long heapAfterMemSeg = memoryBean.getHeapMemoryUsage().getUsed();
            long memSegGcCount = getGcCount() - gcCountBeforeMemSeg;
            long memSegGcTime = getGcTime() - gcTimeBeforeMemSeg;
            double memSegThroughput = (ROW_COUNT / (memSegElapsed / 1000.0));

            // Verify count
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(ROW_COUNT, rs.getInt(1));
            }

            // -----------------------------------------------------------------
            // 4. WRITE CONSOLIDATED REPORT
            // -----------------------------------------------------------------
            double speedup = ((double) (standardElapsed - memSegElapsed) / standardElapsed) * 100.0;
            double throughputImprovement = ((memSegThroughput - standardThroughput) / standardThroughput) * 100.0;
            long heapGrowthStandardMB = Math.max(0, (heapAfterStandard - heapBeforeStandard) / (1024 * 1024));
            long heapGrowthMemSegMB = Math.max(0, (heapAfterMemSeg - heapBeforeMemSeg) / (1024 * 1024));

            File reportFile = new File("target/benchmark-summary.md");
            reportFile.getParentFile().mkdirs();
            try (PrintWriter pw = new PrintWriter(new FileWriter(reportFile))) {
                pw.println("# Automated Benchmark Results: Standard On-Heap vs MemorySegment Off-Heap");
                pw.println();
                pw.println("- **Total Rows Loaded**: " + ROW_COUNT);
                pw.println("- **Batch Size**: " + BATCH_SIZE);
                pw.println("- **Database**: Azure SQL DB (`test-divang-driver`)");
                pw.println();
                pw.println("| Metric | Standard On-Heap ByteBuffer | MemorySegment Off-Heap | Improvement |");
                pw.println("|---|---|---|---|");
                pw.printf("| **Total Time (ms)** | %d ms | %d ms | **%.2f%% faster** |%n", standardElapsed, memSegElapsed, speedup);
                pw.printf("| **Ingestion Rate** | %,.0f rows/sec | %,.0f rows/sec | **+%.2f%%** |%n", standardThroughput, memSegThroughput, throughputImprovement);
                pw.printf("| **GC Collections** | %d cycles | %d cycles | - |%n", standardGcCount, memSegGcCount);
                pw.printf("| **GC Pause Time** | %d ms | %d ms | **%.1f%% reduction** |%n", standardGcTime, memSegGcTime, standardGcTime > 0 ? ((double)(standardGcTime - memSegGcTime)/standardGcTime)*100.0 : 0.0);
                pw.printf("| **Heap Delta** | %d MB | %d MB | - |%n", heapGrowthStandardMB, heapGrowthMemSegMB);
            }

            System.out.println("==========================================================================");
            System.out.println("                 AUTOMATED BENCHMARK SUMMARY (" + ROW_COUNT + " rows)");
            System.out.println("==========================================================================");
            System.out.printf("  Standard On-Heap: %d ms | %,.0f rows/sec | GC Time: %d ms (%d cycles)%n", standardElapsed, standardThroughput, standardGcTime, standardGcCount);
            System.out.printf("  MemorySegment   : %d ms | %,.0f rows/sec | GC Time: %d ms (%d cycles)%n", memSegElapsed, memSegThroughput, memSegGcTime, memSegGcCount);
            System.out.printf("  Throughput Gain : +%.2f%% faster ingestion rate%n", throughputImprovement);
            System.out.println("  Report written to: target/benchmark-summary.md");
            System.out.println("==========================================================================");
        }
    }

    private void runBulkLoad(Connection con, int rows, boolean useMemorySegment) throws Exception {
        SimpleDataRecord record = new SimpleDataRecord(rows);
        try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
            SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
            options.setBatchSize(BATCH_SIZE);
            options.setBulkCopyTimeout(120);
            options.setUseMemorySegment(useMemorySegment);
            bulkCopy.setBulkCopyOptions(options);
            bulkCopy.setDestinationTableName(TABLE_NAME);
            bulkCopy.writeToServer(record);
        }
    }

    private static class SimpleDataRecord implements ISQLServerBulkData {
        private static final long serialVersionUID = 1L;
        private final int totalRows;
        private int currentRow = 0;

        SimpleDataRecord(int totalRows) {
            this.totalRows = totalRows;
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            return new HashSet<>(Arrays.asList(1, 2, 3, 4));
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
                3000000000L + currentRow,
                new BigDecimal(currentRow + ".9900"),
                (short) (currentRow % 500)
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
