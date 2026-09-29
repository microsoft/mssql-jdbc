/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
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
 * Performance comparison test between Standard On-Heap ByteBuffer BulkCopy
 * and MemorySegment Off-Heap BulkCopy against actual Azure SQL DB.
 */
public class MemorySegmentBulkCopyPerfComparisonLiveTest extends AbstractTest {

    private static final String TABLE_NAME = "test_bulk_perf_comparison";
    private static final int ROW_COUNT = 30000;
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

    private static long getUsedMemory() {
        System.gc();
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
    }

    @Test
    @DisplayName("Compare standard on-heap ByteBuffer vs MemorySegment off-heap BulkCopy")
    public void testComparePerformance() throws Exception {
        try (Connection con = getConnection();
             Statement stmt = con.createStatement()) {

            // --- RUN 1: Standard On-Heap ByteBuffer ---
            stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
            System.gc();
            Thread.sleep(100);
            long gcBeforeStandard = getGcTime();
            long memBeforeStandard = getUsedMemory();

            SimplePerfBulkRecord recordStandard = new SimplePerfBulkRecord(ROW_COUNT);
            long startStandard = System.currentTimeMillis();
            try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setBatchSize(BATCH_SIZE);
                options.setBulkCopyTimeout(120);
                options.setUseMemorySegment(false); // Standard on-heap path
                bulkCopy.setBulkCopyOptions(options);
                bulkCopy.setDestinationTableName(TABLE_NAME);
                bulkCopy.writeToServer(recordStandard);
            }
            long standardElapsed = System.currentTimeMillis() - startStandard;
            long gcStandardTime = getGcTime() - gcBeforeStandard;

            // Verify count
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(ROW_COUNT, rs.getInt(1));
            }

            // --- RUN 2: MemorySegment Off-Heap ---
            stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
            System.gc();
            Thread.sleep(100);
            long gcBeforeMemSeg = getGcTime();

            SimplePerfBulkRecord recordMemSeg = new SimplePerfBulkRecord(ROW_COUNT);
            long startMemSeg = System.currentTimeMillis();
            try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setBatchSize(BATCH_SIZE);
                options.setBulkCopyTimeout(120);
                options.setUseMemorySegment(true); // MemorySegment off-heap path
                bulkCopy.setBulkCopyOptions(options);
                bulkCopy.setDestinationTableName(TABLE_NAME);
                bulkCopy.writeToServer(recordMemSeg);
            }
            long memSegElapsed = System.currentTimeMillis() - startMemSeg;
            long gcMemSegTime = getGcTime() - gcBeforeMemSeg;

            // Verify count
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(ROW_COUNT, rs.getInt(1));
            }

            // --- Report ---
            System.out.println("=================================================================");
            System.out.println("               PERFORMANCE BENCHMARK RESULTS (" + ROW_COUNT + " rows)");
            System.out.println("=================================================================");
            System.out.printf("  Standard On-Heap ByteBuffer : %d ms (GC Time: %d ms)%n", standardElapsed, gcStandardTime);
            System.out.printf("  MemorySegment Off-Heap      : %d ms (GC Time: %d ms)%n", memSegElapsed, gcMemSegTime);
            double diffPct = ((double) (standardElapsed - memSegElapsed) / standardElapsed) * 100.0;
            System.out.printf("  Difference                  : %.2f%% faster with MemorySegment%n", diffPct);
            System.out.println("=================================================================");
        }
    }

    private static class SimplePerfBulkRecord implements ISQLServerBulkData {
        private static final long serialVersionUID = 1L;
        private final int totalRows;
        private int currentRow = 0;

        SimplePerfBulkRecord(int totalRows) {
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
                2000000000L + currentRow,
                new BigDecimal(currentRow + ".7500"),
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
