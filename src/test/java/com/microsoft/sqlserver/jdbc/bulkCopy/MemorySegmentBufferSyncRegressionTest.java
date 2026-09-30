/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;

/**
 * Regression coverage for two {@code TDSWriter} buffer-bookkeeping bugs found while validating the
 * {@code useMemorySegment} bulk-copy staging option:
 *
 * <ol>
 * <li>{@code heapStagingBuffer} was not kept in sync with the reference swap performed in
 * {@code TDSWriter.flush()}, so after the first heap-path packet swap it could alias the wrong buffer.</li>
 * <li>{@code heapSocketBuffer} had the same problem: after an odd number of heap-path swaps it could alias the
 * same object as {@code heapStagingBuffer}, so a later {@code enableMemorySegment(false)} would collapse
 * {@code stagingBuffer} and {@code socketBuffer} into a single object and corrupt the TDS stream.</li>
 * </ol>
 *
 * <p>
 * Both bugs only manifest when a connection's {@code TDSWriter} is reused across multiple TDS messages while
 * {@code useMemorySegment} is toggled on and off (which happens once per internal bulk-copy batch, and once
 * around every ordinary statement execution). A single, isolated bulk-copy call on a fresh connection does not
 * reproduce them, which is why these scenarios specifically exercise repeated/interleaved usage on one
 * connection rather than a single one-shot copy.
 *
 * <p>
 * These tests assert actual row content (via SQL-side aggregate checks), not just row counts, since a partial
 * buffer corruption could in principle preserve the row count while corrupting individual values.
 *
 * <p>
 * {@code setUseMemorySegment(true)} is safe to call regardless of JRE version: on JRE &lt; 22 it silently falls
 * back to the standard on-heap path (see {@code TDSMemorySegmentStaging.isSupported()}), so these tests provide
 * useful coverage on every supported profile, not only jre22+.
 */
public class MemorySegmentBufferSyncRegressionTest extends AbstractTest {

    private static final String TABLE_NAME = "memseg_buffer_sync_regression";

    @BeforeEach
    public void setupTable() throws Exception {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT NOT NULL, val INT NOT NULL)");
        }
    }

    @AfterEach
    public void teardownTable() throws Exception {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
        }
    }

    /**
     * Reproduces bug #1/#2's precondition directly: several independent bulk-copy operations, each with
     * {@code useMemorySegment(true)}, run one after another on the *same* connection (and therefore the same
     * {@code TDSWriter}). Each iteration triggers its own enable/disable cycle; if either heap buffer field goes
     * stale, a later iteration corrupts the stream or the retrieved data.
     */
    @Test
    @DisplayName("Repeated bulk copies with useMemorySegment on the same connection preserve data")
    public void testRepeatedBulkCopyOnSameConnectionPreservesData() throws Exception {
        final int rowsPerIteration = 5_000;
        final int repetitions = 5;

        try (Connection con = getConnection()) {
            for (int iteration = 0; iteration < repetitions; iteration++) {
                try (Statement stmt = con.createStatement()) {
                    stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
                }

                DeterministicRowSource source = new DeterministicRowSource(rowsPerIteration);
                try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
                    SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                    options.setBatchSize(2_000);
                    options.setBulkCopyTimeout(120);
                    options.setUseMemorySegment(true);
                    bulkCopy.setBulkCopyOptions(options);
                    bulkCopy.setDestinationTableName(TABLE_NAME);
                    bulkCopy.writeToServer(source);
                }

                assertRowsMatchExpected(con, rowsPerIteration,
                        "iteration " + iteration + " of " + repetitions);
            }
        }
    }

    /**
     * Reproduces the exact sequence that originally triggered the crash: an ordinary {@code Statement} execution
     * (which always runs through the heap path and swaps buffers) immediately followed by a bulk copy with
     * {@code useMemorySegment(true)} on the same connection. Repeated so that heap <-> off-heap transitions
     * happen more than once on the same {@code TDSWriter}.
     */
    @Test
    @DisplayName("Bulk copy with useMemorySegment preceded by a plain statement on the same connection preserves data")
    public void testBulkCopyPrecededByPlainStatementOnSameConnection() throws Exception {
        final int rowsPerIteration = 5_000;
        final int repetitions = 5;

        try (Connection con = getConnection()) {
            for (int iteration = 0; iteration < repetitions; iteration++) {
                // Ordinary statement on the same connection/TDSWriter immediately before the bulk copy.
                try (Statement stmt = con.createStatement()) {
                    stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
                }

                DeterministicRowSource source = new DeterministicRowSource(rowsPerIteration);
                try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
                    SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                    options.setBatchSize(2_000);
                    options.setBulkCopyTimeout(120);
                    options.setUseMemorySegment(true);
                    bulkCopy.setBulkCopyOptions(options);
                    bulkCopy.setDestinationTableName(TABLE_NAME);
                    bulkCopy.writeToServer(source);
                }

                assertRowsMatchExpected(con, rowsPerIteration,
                        "iteration " + iteration + " of " + repetitions);
            }
        }
    }

    /**
     * A single {@code writeToServer} call whose row count exceeds the batch size executes multiple internal
     * bulk-copy batches, each with its own enable/disable cycle around the same {@code TDSWriter}. This
     * verifies every row survives that multi-batch cycling intact, not just the final count.
     */
    @Test
    @DisplayName("Multi-batch bulk copy with useMemorySegment preserves all rows across internal batches")
    public void testMultiBatchBulkCopyWithMemorySegmentPreservesAllRows() throws Exception {
        final int totalRows = 50_000;
        final int batchSize = 4_000; // Forces 13 internal batches, each with its own enable/disable cycle.

        try (Connection con = getConnection()) {
            DeterministicRowSource source = new DeterministicRowSource(totalRows);
            try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setBatchSize(batchSize);
                options.setBulkCopyTimeout(180);
                options.setUseMemorySegment(true);
                bulkCopy.setBulkCopyOptions(options);
                bulkCopy.setDestinationTableName(TABLE_NAME);
                bulkCopy.writeToServer(source);
            }

            assertRowsMatchExpected(con, totalRows, "single multi-batch writeToServer call");
        }
    }

    /**
     * Verifies row count and a full-table aggregate checksum (SUM of both columns) against the closed-form
     * expected values for {@link DeterministicRowSource}'s {@code id, id * 2} pattern. A checksum mismatch with
     * a correct row count would indicate silent data corruption rather than dropped/duplicated rows.
     */
    private static void assertRowsMatchExpected(Connection con, int expectedRows, String context) throws Exception {
        long expectedIdSum = (long) expectedRows * (expectedRows - 1) / 2; // sum of 0..expectedRows-1
        long expectedValSum = expectedIdSum * 2; // val = id * 2

        try (Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT COUNT(*), SUM(CAST(id AS BIGINT)), SUM(CAST(val AS BIGINT)) FROM " + TABLE_NAME)) {
            assertTrue(rs.next(), "Expected a result row for " + context);
            int actualRows = rs.getInt(1);
            long actualIdSum = rs.getLong(2);
            long actualValSum = rs.getLong(3);

            assertEquals(expectedRows, actualRows, "Row count mismatch for " + context);
            assertEquals(expectedIdSum, actualIdSum, "id checksum mismatch for " + context
                    + " (indicates corrupted/duplicated/dropped data, not just a count difference)");
            assertEquals(expectedValSum, actualValSum, "val checksum mismatch for " + context
                    + " (indicates corrupted/duplicated/dropped data, not just a count difference)");
        }
    }

    /**
     * Deterministic two-column row source: {@code id = 0..totalRows-1}, {@code val = id * 2}. The closed-form
     * relationship lets the test verify full-table integrity via a cheap SQL-side aggregate instead of pulling
     * every row back into the JVM.
     */
    private static final class DeterministicRowSource implements ISQLServerBulkData {
        private static final long serialVersionUID = 1L;
        private final int totalRows;
        private int currentRow = -1;

        DeterministicRowSource(int totalRows) {
            this.totalRows = totalRows;
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            return new HashSet<>(Arrays.asList(1, 2));
        }

        @Override
        public String getColumnName(int column) {
            return column == 1 ? "id" : "val";
        }

        @Override
        public int getColumnType(int column) {
            return java.sql.Types.INTEGER;
        }

        @Override
        public int getPrecision(int column) {
            return 0;
        }

        @Override
        public int getScale(int column) {
            return 0;
        }

        @Override
        public Object[] getRowData() {
            return new Object[] { currentRow, currentRow * 2 };
        }

        @Override
        public boolean next() {
            if (currentRow + 1 < totalRows) {
                currentRow++;
                return true;
            }
            return false;
        }
    }
}
