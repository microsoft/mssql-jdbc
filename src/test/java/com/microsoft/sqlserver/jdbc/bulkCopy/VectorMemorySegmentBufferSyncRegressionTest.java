/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

/**
 * VECTOR-specific counterpart to {@link MemorySegmentBufferSyncRegressionTest}: same buffer-bookkeeping bug
 * (stale {@code heapStagingBuffer}/{@code heapSocketBuffer} references after {@code TDSWriter} buffer swaps),
 * same repeated/interleaved-usage scenarios, but against a {@code VECTOR} column instead of plain scalar types.
 * This matters independently of the scalar-type coverage because {@code VECTOR} is serialized through a
 * different code path in {@code SQLServerBulkCopy}/{@code TDSWriter} (see the {@code case microsoft.sql.Types.VECTOR}
 * branches), so a fix verified only against INT/BIGINT/DECIMAL/SMALLINT does not by itself prove VECTOR is safe.
 *
 * <p>
 * Requires a server with native {@code VECTOR} support (Azure SQL DB, or SQL Server 2025+). Servers without it
 * (e.g. SQL Server 2022 or Azure SQL Edge) cause this test to be skipped via {@link Assumptions#assumeTrue}
 * rather than fail, since VECTOR support is a server capability, not a driver bug.
 */
public class VectorMemorySegmentBufferSyncRegressionTest extends AbstractTest {

    private static final String TABLE_NAME = "vector_memseg_buffer_sync_regression";
    private static final int VECTOR_DIMS = 8;

    private String vectorConnectionString;

    @BeforeEach
    public void setupTable() throws Exception {
        vectorConnectionString = getConnectionString() + ";vectorTypeSupport=v1;";

        try (Connection con = DriverManager.getConnection(vectorConnectionString);
                Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            try {
                stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT NOT NULL, embedding VECTOR("
                        + VECTOR_DIMS + ") NOT NULL)");
            } catch (Exception e) {
                Assumptions.assumeTrue(false,
                        "Skipping: server does not support the VECTOR data type (" + e.getMessage() + ")");
            }
        }
    }

    @AfterEach
    public void teardownTable() throws Exception {
        try (Connection con = DriverManager.getConnection(vectorConnectionString);
                Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
        }
    }

    /**
     * Several independent VECTOR bulk-copy operations, each with {@code useMemorySegment(true)}, run one after
     * another on the same connection/{@code TDSWriter}. Each iteration triggers its own enable/disable cycle.
     */
    @Test
    @DisplayName("Repeated VECTOR bulk copies with useMemorySegment on the same connection preserve data")
    public void testRepeatedVectorBulkCopyOnSameConnectionPreservesData() throws Exception {
        final int rowsPerIteration = 2_000;
        final int repetitions = 5;

        try (Connection con = DriverManager.getConnection(vectorConnectionString)) {
            for (int iteration = 0; iteration < repetitions; iteration++) {
                try (Statement stmt = con.createStatement()) {
                    stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
                }

                DeterministicVectorRowSource source = new DeterministicVectorRowSource(rowsPerIteration, VECTOR_DIMS);
                try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
                    SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                    options.setBatchSize(1_000);
                    options.setBulkCopyTimeout(120);
                    options.setUseMemorySegment(true);
                    bulkCopy.setBulkCopyOptions(options);
                    bulkCopy.setDestinationTableName(TABLE_NAME);
                    bulkCopy.writeToServer(source);
                }

                assertAllVectorsMatchExpected(con, rowsPerIteration, "iteration " + iteration + " of " + repetitions);
            }
        }
    }

    /**
     * Reproduces the exact originally-crashing sequence with VECTOR data: an ordinary {@code Statement}
     * (heap-path swap) immediately followed by a VECTOR bulk copy with {@code useMemorySegment(true)}.
     */
    @Test
    @DisplayName("VECTOR bulk copy with useMemorySegment preceded by a plain statement preserves data")
    public void testVectorBulkCopyPrecededByPlainStatementOnSameConnection() throws Exception {
        final int rowsPerIteration = 2_000;
        final int repetitions = 5;

        try (Connection con = DriverManager.getConnection(vectorConnectionString)) {
            for (int iteration = 0; iteration < repetitions; iteration++) {
                // Execute a plain Statement query through the standard heap path immediately before BulkCopy
                try (Statement stmt = con.createStatement()) {
                    stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
                    try (ResultSet rs = stmt.executeQuery("SELECT @@SPID, GETDATE()")) {
                        assertTrue(rs.next(), "Statement query before bulk copy should return a row");
                    }
                }

                DeterministicVectorRowSource source = new DeterministicVectorRowSource(rowsPerIteration, VECTOR_DIMS);
                try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
                    SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                    options.setBatchSize(1_000);
                    options.setBulkCopyTimeout(120);
                    options.setUseMemorySegment(true);
                    bulkCopy.setBulkCopyOptions(options);
                    bulkCopy.setDestinationTableName(TABLE_NAME);
                    bulkCopy.writeToServer(source);
                }

                assertAllVectorsMatchExpected(con, rowsPerIteration, "iteration " + iteration + " of " + repetitions);
            }
        }
    }

    /**
     * A single {@code writeToServer} call whose row count exceeds the batch size triggers multiple internal
     * bulk-copy batches with VECTOR data, each with its own enable/disable cycle.
     */
    @Test
    @DisplayName("Multi-batch VECTOR bulk copy with useMemorySegment preserves all rows")
    public void testMultiBatchVectorBulkCopyWithMemorySegmentPreservesAllRows() throws Exception {
        final int totalRows = 12_000;
        final int batchSize = 1_000; // Forces 12 internal batches, each with its own enable/disable cycle.

        try (Connection con = DriverManager.getConnection(vectorConnectionString)) {
            DeterministicVectorRowSource source = new DeterministicVectorRowSource(totalRows, VECTOR_DIMS);
            try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setBatchSize(batchSize);
                options.setBulkCopyTimeout(180);
                options.setUseMemorySegment(true);
                bulkCopy.setBulkCopyOptions(options);
                bulkCopy.setDestinationTableName(TABLE_NAME);
                bulkCopy.writeToServer(source);
            }

            assertAllVectorsMatchExpected(con, totalRows, "single multi-batch writeToServer call");
        }
    }

    /**
     * Fetches every row back and verifies both the row count and each vector's exact float values against
     * {@link DeterministicVectorRowSource}'s {@code [id, id+1, ..., id+dims-1]} pattern. Full-row comparison is
     * used (rather than a SQL-side aggregate) because VECTOR columns cannot be cheaply checksummed in T-SQL, and
     * row counts alone would not catch a corrupted-but-present vector.
     */
    private static void assertAllVectorsMatchExpected(Connection con, int expectedRows, String context)
            throws Exception {
        try (Statement stmt = con.createStatement();
                ResultSet rs = stmt
                        .executeQuery("SELECT id, embedding FROM " + TABLE_NAME + " ORDER BY id ASC")) {
            int actualRows = 0;
            while (rs.next()) {
                int id = rs.getInt(1);
                assertEquals(actualRows, id, "Row order/id mismatch for " + context);

                Object vectorObj = rs.getObject(2, Vector.class);
                assertTrue(vectorObj instanceof Vector, "Expected a Vector object for " + context);
                Object[] data = ((Vector) vectorObj).getData();
                assertEquals(VECTOR_DIMS, data.length, "Vector dimension count mismatch for " + context);
                for (int d = 0; d < VECTOR_DIMS; d++) {
                    float expected = id + d;
                    float actual = ((Number) data[d]).floatValue();
                    assertEquals(expected, actual, 0.0001f,
                            "Vector element " + d + " mismatch for id=" + id + " in " + context
                                    + " (indicates corrupted data, not just a count difference)");
                }
                actualRows++;
            }
            assertEquals(expectedRows, actualRows, "Row count mismatch for " + context);
        }
    }

    /**
     * Deterministic row source: {@code id = 0..totalRows-1}, {@code embedding = [id, id+1, ..., id+dims-1]}
     * (FLOAT32). The simple, per-row-unique pattern makes any corruption (dropped row, duplicated row,
     * misaligned value) detectable by full-row comparison.
     */
    private static final class DeterministicVectorRowSource implements ISQLServerBulkData {
        private static final long serialVersionUID = 1L;
        private final int totalRows;
        private final int dims;
        private int currentRow = -1;

        DeterministicVectorRowSource(int totalRows, int dims) {
            this.totalRows = totalRows;
            this.dims = dims;
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            return new HashSet<>(Arrays.asList(1, 2));
        }

        @Override
        public String getColumnName(int column) {
            return column == 1 ? "id" : "embedding";
        }

        @Override
        public int getColumnType(int column) {
            return column == 1 ? java.sql.Types.INTEGER : microsoft.sql.Types.VECTOR;
        }

        @Override
        public int getPrecision(int column) {
            return column == 2 ? dims : 0;
        }

        @Override
        public int getScale(int column) {
            return 0;
        }

        @Override
        public Object[] getRowData() {
            Float[] values = new Float[dims];
            for (int d = 0; d < dims; d++) {
                values[d] = (float) (currentRow + d);
            }
            Vector vector = new Vector(dims, VectorDimensionType.FLOAT32, values);
            return new Object[] { currentRow, vector };
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
