/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
 * Validates BulkCopy using off-heap MemorySegment serialization against actual SQL DB.
 */
public class MemorySegmentBulkCopyLiveTest extends AbstractTest {

    private static final String TABLE_NAME = "test_memorysegment_bulk";
    private static final int ROW_COUNT = 10000;

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

    @Test
    @DisplayName("BulkCopy 10,000 rows through MemorySegment off-heap staging")
    public void testMemorySegmentBulkCopy() throws Exception {
        try (Connection con = getConnection();
             Statement stmt = con.createStatement()) {
            stmt.execute("TRUNCATE TABLE " + TABLE_NAME);

            SimpleBulkRecord record = new SimpleBulkRecord(ROW_COUNT);

            long startTime = System.currentTimeMillis();
            try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setBatchSize(5000);
                options.setBulkCopyTimeout(60);
                bulkCopy.setBulkCopyOptions(options);
                bulkCopy.setDestinationTableName(TABLE_NAME);
                bulkCopy.writeToServer(record);
            }
            long elapsed = System.currentTimeMillis() - startTime;
            System.out.println(">>> MemorySegment BulkCopy successfully inserted " + ROW_COUNT + " rows into Azure SQL DB in " + elapsed + " ms");

            // Verify row count in Azure SQL DB
            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(ROW_COUNT, rs.getInt(1), "Row count in destination table should match inserted rows");
            }

            // Verify data integrity for first and last rows
            try (ResultSet rs = stmt.executeQuery("SELECT TOP 1 id, big_val, small_id FROM " + TABLE_NAME + " ORDER BY id ASC")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertEquals(1000000001L, rs.getLong(2));
                assertEquals((short) 1, rs.getShort(3));
            }

            try (ResultSet rs = stmt.executeQuery("SELECT TOP 1 id, big_val, small_id FROM " + TABLE_NAME + " ORDER BY id DESC")) {
                assertTrue(rs.next());
                assertEquals(ROW_COUNT, rs.getInt(1));
                assertEquals(1000000000L + ROW_COUNT, rs.getLong(2));
                assertEquals((short) (ROW_COUNT % 1000), rs.getShort(3));
            }
        }
    }

    private static class SimpleBulkRecord implements ISQLServerBulkData {
        private static final long serialVersionUID = 1L;
        private final int totalRows;
        private int currentRow = 0;

        SimpleBulkRecord(int totalRows) {
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
                1000000000L + currentRow,
                new BigDecimal(currentRow + ".5000"),
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
