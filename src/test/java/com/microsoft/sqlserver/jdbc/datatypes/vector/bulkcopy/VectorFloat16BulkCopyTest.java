/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.datatypes.vector.bulkcopy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.vectorJsonTest;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

/**
 * Test class for FLOAT16 vector bulk copy operations.
 *
 * This class extends {@link VectorBulkCopyTest} and provides FLOAT16-specific
 * configuration. All test methods are inherited from the abstract base class.
 *
 * SQL Syntax: VECTOR(n, float16) where n is the dimension count
 * Scale: 2 (FLOAT16)
 * Max Dimensions: 3996
 *
 * Note: This test class is tagged with {@link Constants#vectorFloat16Test}
 * for pipeline exclusion since the server does not yet support FLOAT16 vectors.
 */
@RunWith(JUnitPlatform.class)
@DisplayName("Test Vector Bulk Copy - FLOAT16")
@vectorJsonTest
@Tag(Constants.vectorTest)
@Tag(Constants.vectorFloat16Test)
public class VectorFloat16BulkCopyTest extends VectorBulkCopyTest {

    @Override
    protected VectorDimensionType getVectorDimensionType() {
        return VectorDimensionType.FLOAT16;
    }

    @Override
    protected String getColumnDefinition(int dimensionCount) {
        // FLOAT16 requires explicit type specification
        return "VECTOR(" + dimensionCount + ", float16)";
    }

    @Override
    protected int getScale() {
        return 2; // FLOAT16 scale
    }

    @Override
    protected String getTypeName() {
        return "FLOAT16";
    }

    /**
     * The server implementation restricts vectors to a total of 8000 bytes.
     * Subtracting the 8 byte header leaves 7992 bytes for data.
     * For float16, each dimension requires 2 bytes, so the maximum number of dimensions is 3996.
     * Calculation: (3996 * 2) + 8 == 8000
     */
    @Override
    protected int getMaxDimensionCount() {
        return 3996;
    }

    @Override
    protected String getRequiredVectorTypeSupport() {
        return "v2"; // FLOAT16 requires v2
    }

    // ============================================================================
    // Mixed Vector Type Bulk Copy Tests (FLOAT32 + FLOAT16 in same table)
    // ============================================================================

    /**
     * Nested test class for bulk copy operations on tables containing both FLOAT32
     * and FLOAT16 vector columns. Requires a v2 connection.
     *
     * Covers:
     * - Table-to-table bulk copy with mixed types
     * - ISQLServerBulkData with mixed-type columns
     * - CSV bulk copy with mixed-type columns
     * - useBulkCopyForBatchInsert with mixed types
     * - Multiple rows including nulls
     */
    @Nested
    @DisplayName("Mixed FLOAT32 + FLOAT16 Bulk Copy Tests")
    @Tag(Constants.vectorTest)
    @Tag(Constants.vectorFloat16Test)
    @TestInstance(TestInstance.Lifecycle.PER_CLASS)
    class MixedVectorTypeBulkCopyTest {

        private SQLServerConnection mixedBcConnection;
        private String csvFilePath;
        private static final String MIXED_CSV_FILE = "BulkCopyCSVTestInputMixedVectorType.csv";
        private final String mixedUuid = UUID.randomUUID().toString().replaceAll("-", "");

        @org.junit.jupiter.api.BeforeAll
        public void setupMixedBulkCopyTests() throws Exception {
            String connStr = connectionString + ";vectorTypeSupport=v2";
            mixedBcConnection = (SQLServerConnection) DriverManager.getConnection(connStr);

            byte negotiatedVersion = mixedBcConnection.getNegotiatedVectorVersion();
            org.junit.jupiter.api.Assumptions.assumeTrue(negotiatedVersion >= 2,
                    "Server negotiated vector version " + negotiatedVersion
                            + ", but mixed-type bulk copy tests require version 2. Skipping.");

            csvFilePath = TestUtils.getCurrentClassPath();
        }

        @AfterAll
        public void cleanupMixedBulkCopyTests() throws SQLException {
            if (mixedBcConnection != null && !mixedBcConnection.isClosed()) {
                mixedBcConnection.close();
            }
        }

        private Float[] data(float... values) {
            Float[] arr = new Float[values.length];
            for (int i = 0; i < values.length; i++) {
                arr[i] = values[i];
            }
            return arr;
        }

        /**
         * Type-aware comparison: FLOAT16 columns get tolerance, FLOAT32 get exact match.
         */
        private void assertMixedVectorEquals(Object[] expected, Object[] actual,
                VectorDimensionType type, String message) {
            if (expected == null) {
                assertNull(actual, message + " - expected null but actual is not null");
                return;
            }
            assertNotNull(actual, message + " - actual is null");
            assertEquals(expected.length, actual.length, message + " - length mismatch");

            boolean isFloat16 = (type == VectorDimensionType.FLOAT16);
            for (int i = 0; i < expected.length; i++) {
                float expectedVal = (Float) expected[i];
                float actualVal = (Float) actual[i];
                float tolerance = isFloat16 ? Math.max(0.0005f * Math.abs(expectedVal), 1e-7f) : 0.0f;
                assertEquals(expectedVal, actualVal, tolerance,
                        message + " at index " + i);
            }
        }

        private String getMixedConnectionString() {
            return connectionString + ";vectorTypeSupport=v2";
        }

        // ================================================================
        // Table-to-Table Bulk Copy
        // ================================================================

        /**
         * Tests bulk copy from a source table with FLOAT32 + FLOAT16 columns
         * to a destination table with the same schema, including null rows.
         */
        @Test
        public void testMixedBulkCopyTableToTable() throws SQLException {
            String srcTable = TestUtils.escapeSingleQuotes(
                    AbstractSQLGenerator.escapeIdentifier(
                            RandomUtil.getIdentifier("srcMixedBC_" + mixedUuid.substring(0, 8))));
            String dstTable = TestUtils.escapeSingleQuotes(
                    AbstractSQLGenerator.escapeIdentifier(
                            RandomUtil.getIdentifier("dstMixedBC_" + mixedUuid.substring(0, 8))));

            try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                    Statement stmt = conn.createStatement()) {

                stmt.executeUpdate("CREATE TABLE " + srcTable
                        + " (v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");
                stmt.executeUpdate("CREATE TABLE " + dstTable
                        + " (v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");

                // Insert rows: non-null, nulls, non-null
                Float[] f32Data1 = data(1.0f, 2.0f, 3.0f);
                Float[] f16Data1 = data(0.5f, 1.5f, 2.5f);
                Float[] f32Data2 = data(4.0f, 5.0f, 6.0f);
                Float[] f16Data2 = data(10.0f, 20.0f, 30.0f);

                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT INTO " + srcTable + " (v_float32, v_float16) VALUES (?, ?)")) {
                    // Row 1: both non-null
                    pstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, f32Data1),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT16, f16Data1),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();

                    // Row 2: both null
                    pstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, null),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT16, null),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();

                    // Row 3: both non-null
                    pstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, f32Data2),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT16, f16Data2),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }

                // Bulk copy from source to destination
                try (ResultSet resultSet = stmt.executeQuery("SELECT * FROM " + srcTable);
                        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {
                    bulkCopy.setDestinationTableName(dstTable);
                    bulkCopy.writeToServer(resultSet);
                }

                // Validate destination
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT v_float32, v_float16 FROM " + dstTable)) {
                    // Row 1
                    assertTrue(rs.next(), "Expected row 1.");
                    assertMixedVectorEquals(f32Data1,
                            rs.getObject("v_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "Row 1 FLOAT32");
                    assertMixedVectorEquals(f16Data1,
                            rs.getObject("v_float16", Vector.class).getData(),
                            VectorDimensionType.FLOAT16, "Row 1 FLOAT16");

                    // Row 2: nulls
                    assertTrue(rs.next(), "Expected row 2.");
                    assertNull(rs.getObject("v_float32", Vector.class).getData(),
                            "Row 2 FLOAT32 should be null.");
                    assertNull(rs.getObject("v_float16", Vector.class).getData(),
                            "Row 2 FLOAT16 should be null.");

                    // Row 3
                    assertTrue(rs.next(), "Expected row 3.");
                    assertMixedVectorEquals(f32Data2,
                            rs.getObject("v_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "Row 3 FLOAT32");
                    assertMixedVectorEquals(f16Data2,
                            rs.getObject("v_float16", Vector.class).getData(),
                            VectorDimensionType.FLOAT16, "Row 3 FLOAT16");

                    assertFalse(rs.next(), "Expected only 3 rows.");
                }
            } finally {
                try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                        Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(srcTable, stmt);
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }

        // ================================================================
        // ISQLServerBulkData
        // ================================================================

        /**
         * Tests bulk copy via ISQLServerBulkData with two vector columns of
         * different types (FLOAT32 + FLOAT16).
         */
        @Test
        public void testMixedBulkCopyISQLServerBulkData() throws SQLException {
            String dstTable = TestUtils.escapeSingleQuotes(
                    AbstractSQLGenerator.escapeIdentifier(
                            RandomUtil.getIdentifier("dstMixedBulkData_" + mixedUuid.substring(0, 8))));

            try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                    Statement stmt = conn.createStatement();
                    SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {

                stmt.executeUpdate("CREATE TABLE " + dstTable
                        + " (v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");

                Float[] f32Data = data(1.0f, 2.0f, 3.0f);
                Float[] f16Data = data(0.5f, 1.5f, 2.5f);
                Vector f32 = new Vector(3, VectorDimensionType.FLOAT32, f32Data);
                Vector f16 = new Vector(3, VectorDimensionType.FLOAT16, f16Data);

                MixedVectorBulkData bulkData = new MixedVectorBulkData(f32, f16, 3);

                bulkCopy.setDestinationTableName(dstTable);
                bulkCopy.writeToServer(bulkData);

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + dstTable)) {
                    assertTrue(rs.next(), "No data in destination.");
                    assertMixedVectorEquals(f32Data,
                            rs.getObject("v_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "FLOAT32 mismatch");
                    assertMixedVectorEquals(f16Data,
                            rs.getObject("v_float16", Vector.class).getData(),
                            VectorDimensionType.FLOAT16, "FLOAT16 mismatch");
                }
            } finally {
                try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                        Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }

        /**
         * Tests ISQLServerBulkData with multiple rows including null vectors
         * across mixed FLOAT32 and FLOAT16 columns.
         */
        @Test
        public void testMixedBulkCopyISQLServerBulkDataMultiRow() throws SQLException {
            String dstTable = TestUtils.escapeSingleQuotes(
                    AbstractSQLGenerator.escapeIdentifier(
                            RandomUtil.getIdentifier("dstMixedMulti_" + mixedUuid.substring(0, 8))));

            try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                    Statement stmt = conn.createStatement();
                    SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {

                stmt.executeUpdate("CREATE TABLE " + dstTable
                        + " (v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");

                Float[] f32Data = data(1.0f, 2.0f, 3.0f);
                Float[] f16Data = data(0.5f, 1.5f, 2.5f);

                List<Object[]> rows = new ArrayList<>();
                // Row 1: both non-null
                rows.add(new Object[] {
                        new Vector(3, VectorDimensionType.FLOAT32, f32Data),
                        new Vector(3, VectorDimensionType.FLOAT16, f16Data)});
                // Row 2: both null
                rows.add(new Object[] {
                        new Vector(3, VectorDimensionType.FLOAT32, null),
                        new Vector(3, VectorDimensionType.FLOAT16, null)});
                // Row 3: FLOAT32 non-null, FLOAT16 null
                rows.add(new Object[] {
                        new Vector(3, VectorDimensionType.FLOAT32, f32Data),
                        new Vector(3, VectorDimensionType.FLOAT16, null)});

                MixedVectorBulkDataMultiRow bulkData = new MixedVectorBulkDataMultiRow(rows, 3);
                bulkCopy.setDestinationTableName(dstTable);
                bulkCopy.writeToServer(bulkData);

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + dstTable)) {
                    // Row 1
                    assertTrue(rs.next(), "Expected row 1.");
                    assertMixedVectorEquals(f32Data,
                            rs.getObject("v_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "Row 1 FLOAT32");
                    assertMixedVectorEquals(f16Data,
                            rs.getObject("v_float16", Vector.class).getData(),
                            VectorDimensionType.FLOAT16, "Row 1 FLOAT16");

                    // Row 2: nulls
                    assertTrue(rs.next(), "Expected row 2.");
                    assertNull(rs.getObject("v_float32", Vector.class).getData(),
                            "Row 2 FLOAT32 should be null.");
                    assertNull(rs.getObject("v_float16", Vector.class).getData(),
                            "Row 2 FLOAT16 should be null.");

                    // Row 3: FLOAT32 non-null, FLOAT16 null
                    assertTrue(rs.next(), "Expected row 3.");
                    assertMixedVectorEquals(f32Data,
                            rs.getObject("v_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "Row 3 FLOAT32");
                    assertNull(rs.getObject("v_float16", Vector.class).getData(),
                            "Row 3 FLOAT16 should be null.");

                    assertFalse(rs.next(), "Expected only 3 rows.");
                }
            } finally {
                try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                        Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }

        // ================================================================
        // CSV Bulk Copy
        // ================================================================

        /**
         * Tests bulk copy from CSV with mixed FLOAT32 and FLOAT16 vector columns.
         * The CSV contains two vector columns with different scales (4 for FLOAT32,
         * 2 for FLOAT16).
         */
        @Test
        @vectorJsonTest
        public void testMixedBulkCopyFromCSV() throws SQLException {
            String dstTable = AbstractSQLGenerator.escapeIdentifier(
                    RandomUtil.getIdentifier("dstMixedCSV_" + mixedUuid.substring(0, 8)));
            String fileName = csvFilePath + MIXED_CSV_FILE;

            try (Connection con = DriverManager.getConnection(getMixedConnectionString());
                    Statement stmt = con.createStatement();
                    SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con);
                    SQLServerBulkCSVFileRecord fileRecord = new SQLServerBulkCSVFileRecord(
                            fileName, null, ",", true)) {

                stmt.executeUpdate("CREATE TABLE " + dstTable
                        + " (v_float32 VECTOR(3), v_float16 VECTOR(3, float16));");

                // Column 1: FLOAT32 (scale=4), Column 2: FLOAT16 (scale=2)
                fileRecord.addColumnMetadata(1, "v_float32", microsoft.sql.Types.VECTOR, 3, 4);
                fileRecord.addColumnMetadata(2, "v_float16", microsoft.sql.Types.VECTOR, 3, 2);
                fileRecord.setEscapeColumnDelimitersCSV(true);

                bulkCopy.setDestinationTableName(dstTable);

                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setKeepIdentity(false);
                options.setTableLock(true);
                options.setBulkCopyTimeout(60);
                bulkCopy.setBulkCopyOptions(options);

                bulkCopy.writeToServer(fileRecord);

                // Expected: row 1 = both null, row 2 = both non-null, row 3 = both non-null
                List<Float[][]> expectedData = Arrays.asList(
                        new Float[][] {null, null},
                        new Float[][] {data(1.0f, 2.0f, 3.0f), data(0.5f, 1.5f, 2.5f)},
                        new Float[][] {data(4.0f, 5.0f, 6.0f), data(10.0f, 20.0f, 30.0f)});

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + dstTable)) {
                    int rowCount = 0;
                    while (rs.next()) {
                        Vector f32 = rs.getObject("v_float32", Vector.class);
                        Vector f16 = rs.getObject("v_float16", Vector.class);

                        Float[][] expected = expectedData.get(rowCount);
                        Object[] actualF32 = f32 != null ? f32.getData() : null;
                        Object[] actualF16 = f16 != null ? f16.getData() : null;

                        assertMixedVectorEquals(expected[0], actualF32,
                                VectorDimensionType.FLOAT32,
                                "CSV row " + (rowCount + 1) + " FLOAT32");
                        assertMixedVectorEquals(expected[1], actualF16,
                                VectorDimensionType.FLOAT16,
                                "CSV row " + (rowCount + 1) + " FLOAT16");
                        rowCount++;
                    }
                    assertEquals(expectedData.size(), rowCount, "CSV row count mismatch.");
                }
            } finally {
                try (Connection con = DriverManager.getConnection(getMixedConnectionString());
                        Statement stmt = con.createStatement()) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }

        // ================================================================
        // useBulkCopyForBatchInsert
        // ================================================================

        /**
         * Tests batch insert with useBulkCopyForBatchInsert=true on a table
         * with both FLOAT32 and FLOAT16 vector columns.
         */
        @Test
        public void testMixedBulkCopyForBatchInsert() throws Exception {
            String tableName = RandomUtil.getIdentifier("MixedBCBatchInsert_" + mixedUuid.substring(0, 8));
            String connStr = connectionString + ";vectorTypeSupport=v2;useBulkCopyForBatchInsert=true;";

            try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connStr);
                    Statement stmt = conn.createStatement()) {

                stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " (v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");

                Float[] f32Data1 = data(1.0f, 2.0f, 3.0f);
                Float[] f16Data1 = data(0.5f, 1.5f, 2.5f);
                Float[] f32Data2 = data(4.0f, 5.0f, 6.0f);
                Float[] f16Data2 = data(10.0f, 20.0f, 30.0f);

                String sqlString = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " (v_float32, v_float16) VALUES (?, ?)";
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn
                        .prepareStatement(sqlString)) {
                    // Row 1: both non-null
                    pstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, f32Data1),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT16, f16Data1),
                            microsoft.sql.Types.VECTOR);
                    pstmt.addBatch();

                    // Row 2: both null
                    pstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, null),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT16, null),
                            microsoft.sql.Types.VECTOR);
                    pstmt.addBatch();

                    // Row 3: both non-null
                    pstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, f32Data2),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT16, f16Data2),
                            microsoft.sql.Types.VECTOR);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                }

                try (ResultSet rs = stmt.executeQuery("SELECT v_float32, v_float16 FROM "
                        + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                    // Row 1
                    assertTrue(rs.next(), "Expected row 1.");
                    assertMixedVectorEquals(f32Data1,
                            rs.getObject("v_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "Batch row 1 FLOAT32");
                    assertMixedVectorEquals(f16Data1,
                            rs.getObject("v_float16", Vector.class).getData(),
                            VectorDimensionType.FLOAT16, "Batch row 1 FLOAT16");

                    // Row 2: nulls
                    assertTrue(rs.next(), "Expected row 2.");
                    assertNull(rs.getObject("v_float32", Vector.class).getData(),
                            "Batch row 2 FLOAT32 should be null.");
                    assertNull(rs.getObject("v_float16", Vector.class).getData(),
                            "Batch row 2 FLOAT16 should be null.");

                    // Row 3
                    assertTrue(rs.next(), "Expected row 3.");
                    assertMixedVectorEquals(f32Data2,
                            rs.getObject("v_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "Batch row 3 FLOAT32");
                    assertMixedVectorEquals(f16Data2,
                            rs.getObject("v_float16", Vector.class).getData(),
                            VectorDimensionType.FLOAT16, "Batch row 3 FLOAT16");

                    assertFalse(rs.next(), "Expected only 3 rows.");
                }
            } finally {
                try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                        Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(
                            AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                }
            }
        }

        /**
         * Tests batch insert with useBulkCopyForBatchInsert=true where one column
         * has data and the other is null, alternating patterns per row.
         */
        @Test
        public void testMixedBulkCopyForBatchInsertAlternatingNulls() throws Exception {
            String tableName = RandomUtil.getIdentifier("MixedBCAltNull_" + mixedUuid.substring(0, 8));
            String connStr = connectionString + ";vectorTypeSupport=v2;useBulkCopyForBatchInsert=true;";

            try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connStr);
                    Statement stmt = conn.createStatement()) {

                stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " (v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");

                Float[] f32Data = data(1.0f, 2.0f, 3.0f);
                Float[] f16Data = data(0.5f, 1.5f, 2.5f);

                String sqlString = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " (v_float32, v_float16) VALUES (?, ?)";
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn
                        .prepareStatement(sqlString)) {
                    // Row 1: FLOAT32 non-null, FLOAT16 null
                    pstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, f32Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT16, null),
                            microsoft.sql.Types.VECTOR);
                    pstmt.addBatch();

                    // Row 2: FLOAT32 null, FLOAT16 non-null
                    pstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, null),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT16, f16Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.addBatch();

                    pstmt.executeBatch();
                }

                try (ResultSet rs = stmt.executeQuery("SELECT v_float32, v_float16 FROM "
                        + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                    // Row 1
                    assertTrue(rs.next(), "Expected row 1.");
                    assertMixedVectorEquals(f32Data,
                            rs.getObject("v_float32", Vector.class).getData(),
                            VectorDimensionType.FLOAT32, "Alt row 1 FLOAT32");
                    assertNull(rs.getObject("v_float16", Vector.class).getData(),
                            "Alt row 1 FLOAT16 should be null.");

                    // Row 2
                    assertTrue(rs.next(), "Expected row 2.");
                    assertNull(rs.getObject("v_float32", Vector.class).getData(),
                            "Alt row 2 FLOAT32 should be null.");
                    assertMixedVectorEquals(f16Data,
                            rs.getObject("v_float16", Vector.class).getData(),
                            VectorDimensionType.FLOAT16, "Alt row 2 FLOAT16");

                    assertFalse(rs.next(), "Expected only 2 rows.");
                }
            } finally {
                try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                        Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(
                            AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                }
            }
        }

        // ================================================================
        // Bulk Copy with Different Dimension Counts
        // ================================================================

        /**
         * Tests bulk copy table-to-table where FLOAT32 has 3 dimensions and
         * FLOAT16 has 5 dimensions. Validates per-column precision metadata
         * is handled independently.
         */
        @Test
        public void testMixedBulkCopyDifferentDimensions() throws SQLException {
            String srcTable = TestUtils.escapeSingleQuotes(
                    AbstractSQLGenerator.escapeIdentifier(
                            RandomUtil.getIdentifier("srcMixedDiffDim_" + mixedUuid.substring(0, 8))));
            String dstTable = TestUtils.escapeSingleQuotes(
                    AbstractSQLGenerator.escapeIdentifier(
                            RandomUtil.getIdentifier("dstMixedDiffDim_" + mixedUuid.substring(0, 8))));

            try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                    Statement stmt = conn.createStatement()) {

                stmt.executeUpdate("CREATE TABLE " + srcTable
                        + " (v_float32 VECTOR(3), v_float16 VECTOR(5, float16))");
                stmt.executeUpdate("CREATE TABLE " + dstTable
                        + " (v_float32 VECTOR(3), v_float16 VECTOR(5, float16))");

                Float[] f32Data = {1.0f, 2.0f, 3.0f};
                Float[] f16Data = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f};

                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT INTO " + srcTable + " (v_float32, v_float16) VALUES (?, ?)")) {
                    pstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, f32Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(2, new Vector(5, VectorDimensionType.FLOAT16, f16Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }

                try (ResultSet resultSet = stmt.executeQuery("SELECT * FROM " + srcTable);
                        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {
                    bulkCopy.setDestinationTableName(dstTable);
                    bulkCopy.writeToServer(resultSet);
                }

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + dstTable)) {
                    assertTrue(rs.next(), "No data in destination.");
                    Vector f32 = rs.getObject("v_float32", Vector.class);
                    Vector f16 = rs.getObject("v_float16", Vector.class);
                    assertEquals(3, f32.getDimensionCount(), "FLOAT32 dim count mismatch.");
                    assertEquals(5, f16.getDimensionCount(), "FLOAT16 dim count mismatch.");
                    assertMixedVectorEquals(f32Data, f32.getData(),
                            VectorDimensionType.FLOAT32, "FLOAT32 diff dim data mismatch");
                    assertMixedVectorEquals(f16Data, f16.getData(),
                            VectorDimensionType.FLOAT16, "FLOAT16 diff dim data mismatch");
                }
            } finally {
                try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                        Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(srcTable, stmt);
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }

        // ================================================================
        // Bulk Copy Column Mapping Mismatch
        // ================================================================

        /**
         * Tests bulk copy with explicit column mapping that swaps FLOAT32 and
         * FLOAT16 columns. The server should reject the type mismatch when a
         * FLOAT32 source column is mapped to a FLOAT16 destination column
         * (and vice versa).
         */
        @Test
        public void testMixedBulkCopyColumnMappingMismatch() throws SQLException {
            String srcTable = TestUtils.escapeSingleQuotes(
                    AbstractSQLGenerator.escapeIdentifier(
                            RandomUtil.getIdentifier("srcMixedMap_" + mixedUuid.substring(0, 8))));
            String dstTable = TestUtils.escapeSingleQuotes(
                    AbstractSQLGenerator.escapeIdentifier(
                            RandomUtil.getIdentifier("dstMixedMap_" + mixedUuid.substring(0, 8))));

            try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                    Statement stmt = conn.createStatement()) {

                stmt.executeUpdate("CREATE TABLE " + srcTable
                        + " (v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");
                stmt.executeUpdate("CREATE TABLE " + dstTable
                        + " (v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");

                Float[] f32Data = {1.0f, 2.0f, 3.0f};
                Float[] f16Data = {0.5f, 1.5f, 2.5f};

                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT INTO " + srcTable + " (v_float32, v_float16) VALUES (?, ?)")) {
                    pstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, f32Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT16, f16Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }

                // Bulk copy with SWAPPED column mappings: FLOAT32→FLOAT16, FLOAT16→FLOAT32
                try (ResultSet resultSet = stmt.executeQuery("SELECT * FROM " + srcTable);
                        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {
                    bulkCopy.setDestinationTableName(dstTable);
                    bulkCopy.addColumnMapping("v_float32", "v_float16"); // FLOAT32 source → FLOAT16 dest
                    bulkCopy.addColumnMapping("v_float16", "v_float32"); // FLOAT16 source → FLOAT32 dest
                    bulkCopy.writeToServer(resultSet);
                    fail("Expected an exception due to type mismatch in swapped column mapping.");
                }
            } catch (Exception e) {
                // Server should reject the type mismatch
                assertTrue(e.getMessage() != null && !e.getMessage().isEmpty(),
                        "Expected a non-empty error message for column mapping type mismatch.");
            } finally {
                try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                        Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(srcTable, stmt);
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }

        // ================================================================
        // Bulk Copy Dimension Mismatch (per-column)
        // ================================================================

        /**
         * Tests bulk copy where one column's dimensions match but the other's don't.
         * Source: VECTOR(3) + VECTOR(3, float16)
         * Destination: VECTOR(3) + VECTOR(5, float16)
         * The FLOAT32 column should match fine, but the FLOAT16 column should fail
         * with a dimension mismatch error.
         */
        @Test
        public void testMixedBulkCopyDimensionMismatch() throws SQLException {
            String srcTable = TestUtils.escapeSingleQuotes(
                    AbstractSQLGenerator.escapeIdentifier(
                            RandomUtil.getIdentifier("srcMixedDimErr_" + mixedUuid.substring(0, 8))));
            String dstTable = TestUtils.escapeSingleQuotes(
                    AbstractSQLGenerator.escapeIdentifier(
                            RandomUtil.getIdentifier("dstMixedDimErr_" + mixedUuid.substring(0, 8))));

            try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                    Statement stmt = conn.createStatement()) {

                stmt.executeUpdate("CREATE TABLE " + srcTable
                        + " (v_float32 VECTOR(3), v_float16 VECTOR(3, float16))");
                // Destination has different FLOAT16 dimension count
                stmt.executeUpdate("CREATE TABLE " + dstTable
                        + " (v_float32 VECTOR(3), v_float16 VECTOR(5, float16))");

                Float[] f32Data = {1.0f, 2.0f, 3.0f};
                Float[] f16Data = {0.5f, 1.5f, 2.5f};

                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT INTO " + srcTable + " (v_float32, v_float16) VALUES (?, ?)")) {
                    pstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, f32Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT16, f16Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }

                try (ResultSet resultSet = stmt.executeQuery("SELECT * FROM " + srcTable);
                        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {
                    bulkCopy.setDestinationTableName(dstTable);
                    bulkCopy.writeToServer(resultSet);
                    fail("Expected dimension mismatch error for FLOAT16 column.");
                }
            } catch (Exception e) {
                assertTrue(e.getMessage().contains("The vector dimensions 3 and 5 do not match"),
                        "Expected dimension mismatch error, got: " + e.getMessage());
            } finally {
                try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                        Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(srcTable, stmt);
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }

        // ================================================================
        // Bulk Copy with Explicit VECTOR(3, float32) syntax
        // ================================================================

        /**
         * Tests bulk copy with explicit VECTOR(3, float32) and VECTOR(3, float16)
         * column definitions in both source and destination tables.
         */
        @Test
        public void testMixedBulkCopyExplicitFloat32Syntax() throws SQLException {
            String srcTable = TestUtils.escapeSingleQuotes(
                    AbstractSQLGenerator.escapeIdentifier(
                            RandomUtil.getIdentifier("srcExplicitBC_" + mixedUuid.substring(0, 8))));
            String dstTable = TestUtils.escapeSingleQuotes(
                    AbstractSQLGenerator.escapeIdentifier(
                            RandomUtil.getIdentifier("dstExplicitBC_" + mixedUuid.substring(0, 8))));

            try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                    Statement stmt = conn.createStatement()) {

                // Use explicit VECTOR(3, float32) syntax
                stmt.executeUpdate("CREATE TABLE " + srcTable
                        + " (v_float32 VECTOR(3, float32), v_float16 VECTOR(3, float16))");
                stmt.executeUpdate("CREATE TABLE " + dstTable
                        + " (v_float32 VECTOR(3, float32), v_float16 VECTOR(3, float16))");

                Float[] f32Data = {1.0f, 2.0f, 3.0f};
                Float[] f16Data = {0.5f, 1.5f, 2.5f};

                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT INTO " + srcTable + " (v_float32, v_float16) VALUES (?, ?)")) {
                    pstmt.setObject(1, new Vector(3, VectorDimensionType.FLOAT32, f32Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.setObject(2, new Vector(3, VectorDimensionType.FLOAT16, f16Data),
                            microsoft.sql.Types.VECTOR);
                    pstmt.executeUpdate();
                }

                try (ResultSet resultSet = stmt.executeQuery("SELECT * FROM " + srcTable);
                        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {
                    bulkCopy.setDestinationTableName(dstTable);
                    bulkCopy.writeToServer(resultSet);
                }

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + dstTable)) {
                    assertTrue(rs.next(), "No data in destination.");
                    Vector f32 = rs.getObject("v_float32", Vector.class);
                    Vector f16 = rs.getObject("v_float16", Vector.class);
                    assertEquals(VectorDimensionType.FLOAT32, f32.getVectorDimensionType(),
                            "Expected FLOAT32 type.");
                    assertEquals(VectorDimensionType.FLOAT16, f16.getVectorDimensionType(),
                            "Expected FLOAT16 type.");
                    assertMixedVectorEquals(f32Data, f32.getData(),
                            VectorDimensionType.FLOAT32, "Explicit FLOAT32 BC data");
                    assertMixedVectorEquals(f16Data, f16.getData(),
                            VectorDimensionType.FLOAT16, "Explicit FLOAT16 BC data");
                    assertFalse(rs.next(), "Expected only 1 row.");
                }
            } finally {
                try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                        Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(srcTable, stmt);
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }

        // ================================================================
        // ISQLServerBulkData with Different Dimension Counts
        // ================================================================

        /**
         * Tests ISQLServerBulkData where FLOAT32 has 3 dimensions and FLOAT16
         * has 5 dimensions, verifying per-column precision in the bulk data API.
         */
        @Test
        public void testMixedBulkCopyISQLServerBulkDataDifferentDims() throws SQLException {
            String dstTable = TestUtils.escapeSingleQuotes(
                    AbstractSQLGenerator.escapeIdentifier(
                            RandomUtil.getIdentifier("dstMixedBDDiffDim_" + mixedUuid.substring(0, 8))));

            try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                    Statement stmt = conn.createStatement();
                    SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {

                stmt.executeUpdate("CREATE TABLE " + dstTable
                        + " (v_float32 VECTOR(3), v_float16 VECTOR(5, float16))");

                Float[] f32Data = {1.0f, 2.0f, 3.0f};
                Float[] f16Data = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f};
                Vector f32 = new Vector(3, VectorDimensionType.FLOAT32, f32Data);
                Vector f16 = new Vector(5, VectorDimensionType.FLOAT16, f16Data);

                // Custom ISQLServerBulkData with different precisions per column
                ISQLServerBulkData bulkData = new ISQLServerBulkData() {
                    boolean hasData = true;

                    @Override
                    public Set<Integer> getColumnOrdinals() {
                        Set<Integer> ords = new HashSet<>();
                        ords.add(1);
                        ords.add(2);
                        return ords;
                    }

                    @Override
                    public String getColumnName(int column) {
                        return column == 1 ? "v_float32" : "v_float16";
                    }

                    @Override
                    public int getColumnType(int column) {
                        return microsoft.sql.Types.VECTOR;
                    }

                    @Override
                    public int getPrecision(int column) {
                        return column == 1 ? 3 : 5; // Different dim counts
                    }

                    @Override
                    public int getScale(int column) {
                        return column == 1 ? 4 : 2; // FLOAT32=4, FLOAT16=2
                    }

                    @Override
                    public Object[] getRowData() {
                        return new Object[] {f32, f16};
                    }

                    @Override
                    public boolean next() {
                        if (!hasData)
                            return false;
                        hasData = false;
                        return true;
                    }
                };

                bulkCopy.setDestinationTableName(dstTable);
                bulkCopy.writeToServer(bulkData);

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + dstTable)) {
                    assertTrue(rs.next(), "No data in destination.");
                    Vector resF32 = rs.getObject("v_float32", Vector.class);
                    Vector resF16 = rs.getObject("v_float16", Vector.class);
                    assertEquals(3, resF32.getDimensionCount(), "FLOAT32 dim count mismatch.");
                    assertEquals(5, resF16.getDimensionCount(), "FLOAT16 dim count mismatch.");
                    assertMixedVectorEquals(f32Data, resF32.getData(),
                            VectorDimensionType.FLOAT32, "BulkData diff dim FLOAT32");
                    assertMixedVectorEquals(f16Data, resF16.getData(),
                            VectorDimensionType.FLOAT16, "BulkData diff dim FLOAT16");
                }
            } finally {
                try (Connection conn = DriverManager.getConnection(getMixedConnectionString());
                        Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(dstTable, stmt);
                }
            }
        }

        // ================================================================
        // Inner Helper Classes for Mixed Bulk Data
        // ================================================================

        /**
         * ISQLServerBulkData for a single row with two vector columns
         * (FLOAT32 + FLOAT16).
         */
        class MixedVectorBulkData implements ISQLServerBulkData {
            boolean anyMoreData = true;
            Object[] rowData;
            int precision;

            MixedVectorBulkData(Vector f32, Vector f16, int precision) {
                this.rowData = new Object[] {f32, f16};
                this.precision = precision;
            }

            @Override
            public Set<Integer> getColumnOrdinals() {
                Set<Integer> ords = new HashSet<>();
                ords.add(1);
                ords.add(2);
                return ords;
            }

            @Override
            public String getColumnName(int column) {
                return column == 1 ? "v_float32" : "v_float16";
            }

            @Override
            public int getColumnType(int column) {
                return microsoft.sql.Types.VECTOR;
            }

            @Override
            public int getPrecision(int column) {
                return precision;
            }

            @Override
            public int getScale(int column) {
                return column == 1 ? 4 : 2; // FLOAT32=4, FLOAT16=2
            }

            @Override
            public Object[] getRowData() {
                return rowData;
            }

            @Override
            public boolean next() {
                if (!anyMoreData)
                    return false;
                anyMoreData = false;
                return true;
            }
        }

        /**
         * ISQLServerBulkData for multiple rows with two vector columns
         * (FLOAT32 + FLOAT16).
         */
        class MixedVectorBulkDataMultiRow implements ISQLServerBulkData {
            List<Object[]> data;
            int precision;
            int counter = 0;

            MixedVectorBulkDataMultiRow(List<Object[]> data, int precision) {
                this.data = data;
                this.precision = precision;
            }

            @Override
            public Set<Integer> getColumnOrdinals() {
                Set<Integer> ords = new HashSet<>();
                ords.add(1);
                ords.add(2);
                return ords;
            }

            @Override
            public String getColumnName(int column) {
                return column == 1 ? "v_float32" : "v_float16";
            }

            @Override
            public int getColumnType(int column) {
                return microsoft.sql.Types.VECTOR;
            }

            @Override
            public int getPrecision(int column) {
                return precision;
            }

            @Override
            public int getScale(int column) {
                return column == 1 ? 4 : 2;
            }

            @Override
            public Object[] getRowData() {
                return data.get(counter++);
            }

            @Override
            public boolean next() {
                return counter < data.size();
            }
        }
    }
}
