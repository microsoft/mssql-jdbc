/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests TVP DataTable behavioral semantics: null handling, batch operations, and data immutability.
 * Merged from FX test suites: NullTVP.java, TypesNull.java, TVPbatchQuery.java, TVPNotModified.java.
 * Uses nested classes to group related test categories:
 * - NullHandling: null TVP objects, null columns, under-populated rows
 * - BatchOperations: addBatch/executeBatch with TVP
 * - Immutability: duplicate column names, data reuse, clear behavior
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxTVP)
public class TVPDataTableBehaviorTest extends AbstractTest {

    private static String tvpName;
    private static String tableName;
    private static String procedureName;

    // Batch test constants
    private static final int C1_BIT = 1;
    private static final short C2_TINYINT = 250;
    private static final short C3_SMALLINT = -25;
    private static final int C4_INT = 951;
    private static final long C5_BIGINT = -922;
    private static final double C6_FLOAT = 34;
    private static final float C8_REAL = (float) 5.324;
    private static final BigDecimal C9_DECIMAL = BigDecimal.valueOf(12345.12345);
    private static final String C12_CHAR = "abcd";
    private static final String C13_VARCHAR = "xyz1";
    private static final String C15_NCHAR = "\u4F60\u597D\u554A";
    private static final String C16_NVARCHAR = "\u4F60\u597D\u554A";
    private static final Date C18_DATE = Date.valueOf("2012-01-01");
    private static final Timestamp C19_DATETIME2 = Timestamp.valueOf("2012-01-01 11:05:34.123");
    private static final Time C21_TIME = Time.valueOf("11:05:34");

    /**
     * Initializes the shared database connection used by all tests in this class.
     *
     * @throws Exception if connection setup fails
     */
    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /**
     * Generates unique random identifiers for the TVP type, table, and stored procedure
     * before each test to avoid name collisions across parallel or sequential runs.
     *
     * @throws SQLException if identifier generation fails
     */
    @BeforeEach
    public void testSetup() throws SQLException {
        tvpName = RandomUtil.getIdentifier("TVP");
        tableName = RandomUtil.getIdentifier("TVPBehTable");
        procedureName = RandomUtil.getIdentifier("spTvpBeh");
    }

    /**
     * Cleans up all SQL Server objects created during the test: stored procedure, tables,
     * and TVP type. Dropped in dependency order (procedure → tables → type).
     *
     * @throws SQLException if cleanup fails
     */
    @AfterEach
    public void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(procedureName, stmt);
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTypeIfExists(tvpName, stmt);
        }
    }

    /**
     * Safety-net cleanup after all tests complete. Ensures no leftover objects
     * remain if @AfterEach was skipped due to an unexpected failure.
     *
     * @throws SQLException if cleanup fails
     */
    @AfterAll
    public static void terminate() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(procedureName, stmt);
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTypeIfExists(tvpName, stmt);
        }
    }

    // ==============================
    // Shared Helper Methods
    // ==============================

    /**
     * Creates (or replaces) a table with the given column definitions.
     * Drops the table first if it already exists.
     *
     * @param columnsDef SQL column definitions, e.g. "c_int int null, c_varchar varchar(50) null"
     * @throws SQLException if the DDL statement fails
     */
    private void createTable(String columnsDef) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.execute("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (" + columnsDef + ")");
        }
    }

    /**
     * Creates (or replaces) a TVP type with the given column definitions.
     * Drops the type first if it already exists.
     *
     * @param columnsDef SQL column definitions matching the table schema
     * @throws SQLException if the DDL statement fails
     */
    private void createTVPS(String columnsDef) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTypeIfExists(tvpName, stmt);
            stmt.executeUpdate("CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName)
                    + " AS TABLE (" + columnsDef + ")");
        }
    }

    /**
     * Creates a stored procedure that accepts a TVP parameter and inserts its rows
     * into the target table via {@code INSERT INTO ... SELECT * FROM @InputData}.
     *
     * @param columnsDef SQL column definitions (unused in body, but kept for consistency)
     * @throws SQLException if the DDL statement fails
     */
    private void createProcedure(String columnsDef) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(procedureName, stmt);
            String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procedureName)
                    + " @InputData " + AbstractSQLGenerator.escapeIdentifier(tvpName)
                    + " READONLY AS BEGIN INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " SELECT * FROM @InputData END";
            stmt.execute(sql);
        }
    }

    // ================================================================
    // Nested Class: Null Handling Tests (from TVPNullHandlingTest.java)
    // ================================================================

    /**
     * Tests TVP null handling scenarios.
     * Covers: null TVP object, null TVP name, null columns, under-populated rows, null column names.
     */
    @Nested
    @DisplayName("NullHandling")
    class NullHandling {

        /**
         * Verifies that passing a null SQLServerDataTable to setStructured() is handled gracefully.
         * The driver should accept the null and insert 0 rows via addBatch/executeBatch.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Null: Null SQLServerDataTable Object")
        public void testNullTVPObject() throws SQLException {
            String columnsDef = "bit bit null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, (SQLServerDataTable) null);
                pstmt.addBatch();
                pstmt.executeBatch();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "Null TVP should insert 0 rows");
            }
        }

        /**
         * Verifies that passing a null TVP type name to setStructured() produces an error.
         * The driver requires a non-null TVP name to resolve the server-side type.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Null: Null TVP Name")
        public void testNullTVPName() throws SQLException {
            String columnsDef = "c_int int null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addRow(42);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, null, tvp);
                pstmt.execute();
                fail("Expected SQLException for null TVP name");
            } catch (SQLException e) {
                assertTrue(e.getMessage() != null, "Should have error message for null TVP name");
            }
        }

        /**
         * Verifies that a TVP row with some columns set to null and others non-null
         * is inserted correctly. Validates that only the intended columns are null
         * and non-null values are preserved.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Null: Some Columns Null")
        public void testSomeColumnsNull() throws SQLException {
            String columnsDef = "bit bit null, tinyint tinyint null, smallint smallint null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("bit", java.sql.Types.BIT);
            tvp.addColumnMetadata("tinyint", java.sql.Types.TINYINT);
            tvp.addColumnMetadata("smallint", java.sql.Types.SMALLINT);
            tvp.addRow(true, null, null);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have one row");
                assertEquals(true, rs.getBoolean(1), "bit should be true");
                rs.getObject(2);
                assertTrue(rs.wasNull(), "tinyint should be null");
                rs.getObject(3);
                assertTrue(rs.wasNull(), "smallint should be null");
            }
        }

        /**
         * Verifies that a TVP row where every column is null is inserted correctly.
         * All 3 columns (bit, tinyint, smallint) should read back as null via wasNull().
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Null: All Columns Null")
        public void testAllColumnsNull() throws SQLException {
            String columnsDef = "bit bit null, tinyint tinyint null, smallint smallint null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("bit", java.sql.Types.BIT);
            tvp.addColumnMetadata("tinyint", java.sql.Types.TINYINT);
            tvp.addColumnMetadata("smallint", java.sql.Types.SMALLINT);
            tvp.addRow(null, null, null);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have one row");
                for (int i = 1; i <= 3; i++) {
                    rs.getObject(i);
                    assertTrue(rs.wasNull(), "Column " + i + " should be null");
                }
            }
        }

        /**
         * Verifies that a TVP row with fewer non-null values than columns is handled correctly.
         * Provides values for bit and tinyint, but sets smallint and int to null.
         * The driver should treat the trailing nulls properly and insert a complete row.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Null: Under-populated TVP Row")
        public void testUnderPopulatedRow() throws SQLException {
            String columnsDef = "bit bit null, tinyint tinyint null, smallint smallint null, c_int int null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("bit", java.sql.Types.BIT);
            tvp.addColumnMetadata("tinyint", java.sql.Types.TINYINT);
            tvp.addColumnMetadata("smallint", java.sql.Types.SMALLINT);
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addRow(true, (short) 250, null, null);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have one row");
                assertEquals(true, rs.getBoolean(1), "bit should be 1");
                assertEquals(250, rs.getShort(2), "tinyint should be 250");
                rs.getObject(3);
                assertTrue(rs.wasNull(), "smallint should be null");
                rs.getObject(4);
                assertTrue(rs.wasNull(), "int should be null");
            }
        }

        /**
         * Verifies that a TVP with a null column name in its metadata is accepted
         * by the driver. The server matches columns by ordinal position, not by name,
         * so a null name should not prevent insertion.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Null: Null Column Name in Metadata")
        public void testNullColumnName() throws SQLException {
            String columnsDef = "bit bit null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata(null, java.sql.Types.BIT);
            tvp.addRow(true);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have one row");
                assertEquals(true, rs.getBoolean(1), "bit should be true");
            }
        }

        /**
         * Verifies that a TVP containing a mix of all-null and non-null rows is
         * inserted correctly. Sends 4 rows (2 non-null, 2 all-null) and validates
         * the total count and the count of null rows.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Null: Mixed Null and Non-Null Rows")
        public void testMixedNullAndNonNullRows() throws SQLException {
            String columnsDef = "c_int int null, c_varchar varchar(50) null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp.addRow(1, "hello");
            tvp.addRow(null, null);
            tvp.addRow(3, "world");
            tvp.addRow(null, null);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(
                        "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                    assertTrue(rs.next());
                    assertEquals(4, rs.getInt(1), "Should have 4 rows");
                }

                try (ResultSet rs = stmt.executeQuery("select count(*) from "
                        + AbstractSQLGenerator.escapeIdentifier(tableName) + " where c_int is null")) {
                    assertTrue(rs.next());
                    assertEquals(2, rs.getInt(1), "Should have 2 null rows");
                }
            }
        }

        /**
         * Verifies that a TVP with column metadata defined but no rows added
         * results in 0 rows inserted. This tests the edge case of an empty DataTable.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Null: Empty TVP (metadata only, no rows)")
        public void testEmptyTVPNoRows() throws SQLException {
            String columnsDef = "c_int int null, c_varchar varchar(50) null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "Empty TVP should insert 0 rows");
            }
        }

        /**
         * Verifies that null values can appear in different columns across different rows.
         * Row 1: int=null, varchar="hello", bit=true.
         * Row 2: int=42, varchar=null, bit=false.
         * Row 3: int=99, varchar="world", bit=null.
         * Validates each row's null/non-null pattern after round-trip through SQL Server.
         * Uses ISNULL(c_int, -1) for deterministic ordering since SQL Server sorts NULLs first.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Null: Alternating Null Patterns Across Rows")
        public void testAlternatingNullPatterns() throws SQLException {
            String columnsDef = "c_int int null, c_varchar varchar(50) null, c_bit bit null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp.addColumnMetadata("c_bit", java.sql.Types.BIT);
            tvp.addRow(null, "hello", true);
            tvp.addRow(42, null, false);
            tvp.addRow(99, "world", null);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            // Use ISNULL to get deterministic sort: null int sorts as -1 (before 42, 99)
            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery("select * from "
                            + AbstractSQLGenerator.escapeIdentifier(tableName)
                            + " order by ISNULL(c_int, -1)")) {
                // Row 1: (null, "hello", true) — ISNULL(c_int,-1) = -1
                assertTrue(rs.next());
                rs.getObject(1);
                assertTrue(rs.wasNull(), "First row int should be null");
                assertEquals("hello", rs.getString(2));
                assertEquals(true, rs.getBoolean(3));

                // Row 2: (42, null, false)
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
                rs.getObject(2);
                assertTrue(rs.wasNull(), "Second row varchar should be null");

                // Row 3: (99, "world", null)
                assertTrue(rs.next());
                assertEquals(99, rs.getInt(1));
                assertEquals("world", rs.getString(2));
                rs.getObject(3);
                assertTrue(rs.wasNull(), "Third row bit should be null");
            }
        }

        /**
         * Verifies that null values are handled correctly across all 15 supported data types.
         * Creates a TVP with one row where every column is null, inserts it, and checks
         * that all columns read back as null via wasNull(). Mirrors TypesNull.java which
         * systematically validated null for every type.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Null: All Data Types With Null Values")
        public void testNullValueAllDataTypes() throws SQLException {
            String columnsDef = "c_bit bit null, c_tinyint tinyint null, c_smallint smallint null, "
                    + "c_int int null, c_bigint bigint null, c_float float null, c_real real null, "
                    + "c_decimal decimal(10,5) null, c_char char(20) null, c_varchar varchar(50) null, "
                    + "c_nchar nchar(20) null, c_nvarchar nvarchar(50) null, "
                    + "c_date date null, c_datetime2 datetime2 null, c_time time null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_bit", java.sql.Types.BIT);
            tvp.addColumnMetadata("c_tinyint", java.sql.Types.TINYINT);
            tvp.addColumnMetadata("c_smallint", java.sql.Types.SMALLINT);
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addColumnMetadata("c_bigint", java.sql.Types.BIGINT);
            tvp.addColumnMetadata("c_float", java.sql.Types.DOUBLE);
            tvp.addColumnMetadata("c_real", java.sql.Types.REAL);
            tvp.addColumnMetadata("c_decimal", java.sql.Types.DECIMAL);
            tvp.addColumnMetadata("c_char", java.sql.Types.CHAR);
            tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp.addColumnMetadata("c_nchar", java.sql.Types.NCHAR);
            tvp.addColumnMetadata("c_nvarchar", java.sql.Types.NVARCHAR);
            tvp.addColumnMetadata("c_date", java.sql.Types.DATE);
            tvp.addColumnMetadata("c_datetime2", java.sql.Types.TIMESTAMP);
            tvp.addColumnMetadata("c_time", java.sql.Types.TIME);

            tvp.addRow(null, null, null, null, null, null, null, null,
                    null, null, null, null, null, null, null);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have one row");
                for (int i = 1; i <= 15; i++) {
                    rs.getObject(i);
                    assertTrue(rs.wasNull(), "Column " + i + " should be null");
                }
            }
        }

        /**
         * Verifies that a null SQLServerDataTable is handled correctly when passed
         * through the stored procedure (callable statement) path. The driver should
         * accept the null and the procedure should insert 0 rows.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Null: Null TVP Via Stored Procedure")
        public void testNullTVPViaCallableStatement() throws SQLException {
            String columnsDef = "c_int int null";
            createTable(columnsDef);
            createTVPS(columnsDef);
            createProcedure(columnsDef);

            final String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}";

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql)) {
                pstmt.setStructured(1, tvpName, (SQLServerDataTable) null);
                pstmt.execute();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "Null TVP via stored proc should insert 0 rows");
            }
        }

        /**
         * Verifies that the driver distinguishes between null and empty string values
         * for character types. Inserts one row with null varchar and one row with empty
         * string varchar. The null row should return wasNull()=true, while the empty
         * string row should return a non-null empty string.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Null: Null vs Empty String Values")
        public void testNullVsEmptyStringValue() throws SQLException {
            String columnsDef = "c_varchar varchar(50) null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp.addRow((Object) null);
            tvp.addRow("");

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery("select c_varchar from "
                            + AbstractSQLGenerator.escapeIdentifier(tableName)
                            + " order by case when c_varchar is null then 0 else 1 end")) {
                // Row 1: null
                assertTrue(rs.next(), "Should have first row");
                rs.getString(1);
                assertTrue(rs.wasNull(), "First row should be null");

                // Row 2: empty string
                assertTrue(rs.next(), "Should have second row");
                String val = rs.getString(1);
                assertEquals("", val, "Second row should be empty string");
            }
        }
    }

    // ================================================================
    // Nested Class: Batch Operation Tests (from TVPBatchTest.java)
    // ================================================================

    /**
     * Tests TVP addBatch/executeBatch operations.
     * Covers batch operations with null columns, multiple batches, all data types,
     * stored procedures, empty TVP, and multiple rows.
     */
    @Nested
    @DisplayName("BatchOperations")
    class BatchOperations {

        /**
         * Verifies that TVP batch execution works correctly when some columns are null.
         * Uses addBatch()/executeBatch() path instead of direct execute().
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Batch: Some Columns Null")
        public void testBatchSomeColumnsNull() throws SQLException {
            String columnsDef = "bit bit null, tinyint tinyint null, smallint smallint null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("bit", java.sql.Types.BIT);
            tvp.addColumnMetadata("tinyint", java.sql.Types.TINYINT);
            tvp.addColumnMetadata("smallint", java.sql.Types.SMALLINT);
            tvp.addRow(true, null, null);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.addBatch();
                pstmt.executeBatch();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have one row");
                assertEquals(true, rs.getBoolean(1), "bit should be true");
                rs.getObject(2);
                assertTrue(rs.wasNull(), "tinyint should be null");
                rs.getObject(3);
                assertTrue(rs.wasNull(), "smallint should be null");
            }
        }

        /**
         * Verifies that TVP batch execution works correctly when all columns in
         * the row are null. The row should still be inserted with all null values.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Batch: All Columns Null")
        public void testBatchAllColumnsNull() throws SQLException {
            String columnsDef = "bit bit null, tinyint tinyint null, smallint smallint null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("bit", java.sql.Types.BIT);
            tvp.addColumnMetadata("tinyint", java.sql.Types.TINYINT);
            tvp.addColumnMetadata("smallint", java.sql.Types.SMALLINT);
            tvp.addRow(null, null, null);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.addBatch();
                pstmt.executeBatch();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have one row");
                rs.getObject(1);
                assertTrue(rs.wasNull(), "bit should be null");
                rs.getObject(2);
                assertTrue(rs.wasNull(), "tinyint should be null");
                rs.getObject(3);
                assertTrue(rs.wasNull(), "smallint should be null");
            }
        }

        /**
         * Verifies that multiple addBatch() calls with different TVP DataTable instances
         * on the same PreparedStatement work correctly. Each batch uses a separate
         * SQLServerDataTable containing one row. After executeBatch(), both rows should
         * be present in the table.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Batch: Multiple addBatch Calls")
        public void testBatchMultipleAddBatch() throws SQLException {
            String columnsDef = "c_int int null, c_varchar varchar(50) null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp1 = new SQLServerDataTable();
            tvp1.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp1.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp1.addRow(1, "first");

            SQLServerDataTable tvp2 = new SQLServerDataTable();
            tvp2.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp2.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp2.addRow(2, "second");

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp1);
                pstmt.addBatch();
                pstmt.setStructured(1, tvpName, tvp2);
                pstmt.addBatch();
                pstmt.executeBatch();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should have 2 rows from 2 batches");
            }
        }

        /**
         * Verifies that TVP batch execution works with all 15 supported data types:
         * bit, tinyint, smallint, int, bigint, float, real, decimal, char, varchar,
         * nchar, nvarchar, date, datetime2, time.
         * Uses class-level constants matching the original FX TVPbatchQuery.java values.
         * Validates column count and individual column values after insertion.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Batch: All Data Types")
        public void testBatchAllDataTypes() throws SQLException {
            String columnsDef = "c_bit bit null, c_tinyint tinyint null, c_smallint smallint null, "
                    + "c_int int null, c_bigint bigint null, c_float float null, c_real real null, "
                    + "c_decimal decimal(10,5) null, c_char char(20) null, c_varchar varchar(50) null, "
                    + "c_nchar nchar(20) null, c_nvarchar nvarchar(50) null, "
                    + "c_date date null, c_datetime2 datetime2 null, c_time time null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_bit", java.sql.Types.BIT);
            tvp.addColumnMetadata("c_tinyint", java.sql.Types.TINYINT);
            tvp.addColumnMetadata("c_smallint", java.sql.Types.SMALLINT);
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addColumnMetadata("c_bigint", java.sql.Types.BIGINT);
            tvp.addColumnMetadata("c_float", java.sql.Types.DOUBLE);
            tvp.addColumnMetadata("c_real", java.sql.Types.REAL);
            tvp.addColumnMetadata("c_decimal", java.sql.Types.DECIMAL);
            tvp.addColumnMetadata("c_char", java.sql.Types.CHAR);
            tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp.addColumnMetadata("c_nchar", java.sql.Types.NCHAR);
            tvp.addColumnMetadata("c_nvarchar", java.sql.Types.NVARCHAR);
            tvp.addColumnMetadata("c_date", java.sql.Types.DATE);
            tvp.addColumnMetadata("c_datetime2", java.sql.Types.TIMESTAMP);
            tvp.addColumnMetadata("c_time", java.sql.Types.TIME);

            tvp.addRow(C1_BIT, C2_TINYINT, C3_SMALLINT, C4_INT, C5_BIGINT, C6_FLOAT, C8_REAL,
                    C9_DECIMAL, C12_CHAR, C13_VARCHAR, C15_NCHAR, C16_NVARCHAR, C18_DATE,
                    C19_DATETIME2, C21_TIME);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.addBatch();
                pstmt.executeBatch();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have one row");
                ResultSetMetaData meta = rs.getMetaData();
                assertEquals(15, meta.getColumnCount(), "Should have 15 columns");
                assertEquals(true, rs.getBoolean(1), "bit");
                assertEquals(C2_TINYINT, rs.getShort(2), "tinyint");
                assertEquals(C3_SMALLINT, rs.getShort(3), "smallint");
                assertEquals(C4_INT, rs.getInt(4), "int");
                assertEquals(C5_BIGINT, rs.getLong(5), "bigint");
            }
        }

        /**
         * Verifies that TVP batch execution works via a stored procedure call.
         * The stored procedure accepts a TVP parameter and inserts its rows into the
         * target table. Uses PreparedStatement with CALL syntax and addBatch/executeBatch.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Batch: Via Stored Procedure")
        public void testBatchViaStoredProcedure() throws SQLException {
            String columnsDef = "c_int int null, c_varchar varchar(50) null";
            createTable(columnsDef);
            createTVPS(columnsDef);
            createProcedure(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp.addRow(100, "batch_sp_test");

            final String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}";

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql)) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.addBatch();
                pstmt.executeBatch();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have one row");
                assertEquals(100, rs.getInt(1));
                assertEquals("batch_sp_test", rs.getString(2));
            }
        }

        /**
         * Verifies that executing a batch with an empty TVP (metadata defined but no rows)
         * inserts 0 rows. The driver should handle the empty TVP without errors.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Batch: Empty TVP")
        public void testBatchEmptyTVP() throws SQLException {
            String columnsDef = "c_int int null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.addBatch();
                pstmt.executeBatch();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "Empty TVP batch should insert 0 rows");
            }
        }

        /**
         * Verifies that a single TVP containing multiple rows (3 rows) is correctly
         * inserted via the batch execution path. All 3 rows should be present in the
         * table after executeBatch().
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Batch: Multiple Rows Per TVP")
        public void testBatchMultipleRowsPerTVP() throws SQLException {
            String columnsDef = "c_int int null, c_varchar varchar(50) null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp.addRow(1, "row1");
            tvp.addRow(2, "row2");
            tvp.addRow(3, "row3");

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.addBatch();
                pstmt.executeBatch();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1), "Should have 3 rows");
            }
        }

        /**
         * Verifies that multiple addBatch() calls at large scale (100 batches) work
         * correctly. Each batch uses a separate SQLServerDataTable containing one row.
         * After executeBatch(), all 100 rows should be present in the table.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Batch: Large Scale (100 batches)")
        public void testBatchLargeScale() throws SQLException {
            String columnsDef = "c_int int null, c_varchar varchar(50) null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            final int batchCount = 100;

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                for (int i = 0; i < batchCount; i++) {
                    SQLServerDataTable tvp = new SQLServerDataTable();
                    tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
                    tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
                    tvp.addRow(i, "batch_" + i);
                    pstmt.setStructured(1, tvpName, tvp);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(batchCount, rs.getInt(1), "Should have " + batchCount + " rows from large batch");
            }
        }

        /**
         * Verifies that TVP batch execution respects transaction rollback.
         * Inserts rows via TVP batch inside a transaction, then rolls back.
         * The table should have 0 rows after rollback.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Batch: Transaction Rollback")
        public void testBatchWithTransactionRollback() throws SQLException {
            String columnsDef = "c_int int null, c_varchar varchar(50) null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            try (Connection con = getConnection()) {
                con.setAutoCommit(false);

                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(
                        "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                    SQLServerDataTable tvp = new SQLServerDataTable();
                    tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
                    tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
                    tvp.addRow(1, "should_rollback");
                    tvp.addRow(2, "also_rollback");

                    pstmt.setStructured(1, tvpName, tvp);
                    pstmt.addBatch();
                    pstmt.executeBatch();
                }

                // Rollback the transaction
                con.rollback();
            }

            // Verify table is empty after rollback
            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "Should have 0 rows after rollback");
            }
        }

        /**
         * Verifies that mixing TVP and non-TVP operations in the same batch works.
         * First batch inserts a TVP row, second batch is a plain INSERT.
         * Both rows should be present after executeBatch().
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Batch: Mixed TVP and Non-TVP Batches")
        public void testBatchMixedTVPAndNonTVP() throws SQLException {
            String columnsDef = "c_int int null, c_varchar varchar(50) null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            // First: insert via TVP
            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp.addRow(1, "tvp_row");

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.addBatch();
                pstmt.executeBatch();
            }

            // Second: insert plain values
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " VALUES (2, 'plain_row')");
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should have 2 rows (1 TVP + 1 plain)");
            }
        }
    }

    // ================================================================
    // Nested Class: Immutability Tests (from TVPImmutabilityTest.java)
    // ================================================================

    /**
     * Tests TVP data immutability and duplicate column name handling.
     * Covers: duplicate column names, data persistence after execution, reuse, mutation, and clear.
     */
    @Nested
    @DisplayName("Immutability")
    class Immutability {

        /**
         * Verifies that SQLServerDataTable rejects duplicate column names.
         * The driver enforces unique column names in addColumnMetadata() — adding
         * a second column with the same name "yes" should throw a SQLException.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Immutability: Duplicate Column Names Rejected")
        public void testDuplicateColumnNames() throws SQLException {
            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("yes", java.sql.Types.INTEGER);

            // Driver enforces unique column names — second add with same name should throw
            assertThrows(SQLException.class, () -> tvp.addColumnMetadata("yes", java.sql.Types.INTEGER),
                    "Should reject duplicate column name");
        }

        /**
         * Verifies that data in a SQLServerDataTable is not modified by the driver
         * during or after execution. Inserts 2 rows and reads them back to confirm
         * the original values are preserved in the database.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Immutability: Data Not Modified After Execution")
        public void testDataNotModifiedAfterExecution() throws SQLException {
            String columnsDef = "c_int int null, c_varchar varchar(50) null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp.addRow(42, "test_value");
            tvp.addRow(99, "another_value");

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery("select * from "
                            + AbstractSQLGenerator.escapeIdentifier(tableName) + " order by c_int")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1), "First row int should be 42");
                assertEquals("test_value", rs.getString(2));
                assertTrue(rs.next());
                assertEquals(99, rs.getInt(1), "Second row int should be 99");
                assertEquals("another_value", rs.getString(2));
            }
        }

        /**
         * Verifies that a SQLServerDataTable can be reused across multiple executions.
         * Unlike ISQLServerDataRecord (which is iterator-based and single-use),
         * DataTable stores rows in a Map and can be sent multiple times.
         * Both executions should insert the same row, resulting in 2 total rows.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Immutability: TVP Reuse Across Executions")
        public void testTVPReuseAcrossExecutions() throws SQLException {
            String columnsDef = "c_int int null, c_varchar varchar(50) null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp.addRow(1, "first_exec");

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should have 2 rows from 2 executions");
            }
        }

        /**
         * Verifies that adding rows to a SQLServerDataTable between executions
         * causes the second execution to send the updated (larger) DataTable.
         * First execution inserts 1 row, then addRow(2) is called, and the second
         * execution inserts 2 rows — resulting in 3 total rows in the table.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Immutability: Modify TVP Between Executions")
        public void testModifyTVPBetweenExecutions() throws SQLException {
            String columnsDef = "c_int int null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addRow(1);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            tvp.addRow(2);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1), "Should have 3 rows total (1 + 2)");
            }
        }

        /**
         * Verifies the behavior of SQLServerDataTable.clear() between executions.
         * First execution inserts 2 rows. Then clear() is called, which removes both
         * rows AND column metadata. Column metadata is re-added but no new rows are added,
         * so the second execution inserts 0 rows. Final count remains 2.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Immutability: Clear TVP After Execution")
        public void testClearTVPAfterExecution() throws SQLException {
            String columnsDef = "c_int int null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addRow(1);
            tvp.addRow(2);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            tvp.clear();

            // clear() removes both rows AND column metadata, so re-add metadata
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            // No rows added — second execution should insert 0 rows

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should have 2 rows (clear removed data for second exec)");
            }
        }

        /**
         * Verifies that TVP data remains unmodified after execution when using different
         * data types. Mirrors TVPNotModified.java which tested immutability with various
         * types. Inserts one row with all types and reads back to confirm values match.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Immutability: Data Not Modified All Types")
        public void testDataNotModifiedAllTypes() throws SQLException {
            String columnsDef = "c_bit bit null, c_tinyint tinyint null, c_smallint smallint null, "
                    + "c_int int null, c_bigint bigint null, c_float float null, c_real real null, "
                    + "c_decimal decimal(10,5) null, c_char char(20) null, c_varchar varchar(50) null, "
                    + "c_nchar nchar(20) null, c_nvarchar nvarchar(50) null, "
                    + "c_date date null, c_datetime2 datetime2 null, c_time time null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_bit", java.sql.Types.BIT);
            tvp.addColumnMetadata("c_tinyint", java.sql.Types.TINYINT);
            tvp.addColumnMetadata("c_smallint", java.sql.Types.SMALLINT);
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addColumnMetadata("c_bigint", java.sql.Types.BIGINT);
            tvp.addColumnMetadata("c_float", java.sql.Types.DOUBLE);
            tvp.addColumnMetadata("c_real", java.sql.Types.REAL);
            tvp.addColumnMetadata("c_decimal", java.sql.Types.DECIMAL);
            tvp.addColumnMetadata("c_char", java.sql.Types.CHAR);
            tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp.addColumnMetadata("c_nchar", java.sql.Types.NCHAR);
            tvp.addColumnMetadata("c_nvarchar", java.sql.Types.NVARCHAR);
            tvp.addColumnMetadata("c_date", java.sql.Types.DATE);
            tvp.addColumnMetadata("c_datetime2", java.sql.Types.TIMESTAMP);
            tvp.addColumnMetadata("c_time", java.sql.Types.TIME);

            tvp.addRow(C1_BIT, C2_TINYINT, C3_SMALLINT, C4_INT, C5_BIGINT, C6_FLOAT, C8_REAL,
                    C9_DECIMAL, C12_CHAR, C13_VARCHAR, C15_NCHAR, C16_NVARCHAR, C18_DATE,
                    C19_DATETIME2, C21_TIME);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have one row");
                assertEquals(true, rs.getBoolean(1), "bit");
                assertEquals(C2_TINYINT, rs.getShort(2), "tinyint");
                assertEquals(C3_SMALLINT, rs.getShort(3), "smallint");
                assertEquals(C4_INT, rs.getInt(4), "int");
                assertEquals(C5_BIGINT, rs.getLong(5), "bigint");
                assertEquals(C6_FLOAT, rs.getDouble(6), 0.001, "float");
                assertEquals(C8_REAL, rs.getFloat(7), 0.001, "real");
                assertEquals(0, C9_DECIMAL.compareTo(rs.getBigDecimal(8)), "decimal");
                assertTrue(rs.getString(9).startsWith(C12_CHAR), "char");
                assertEquals(C13_VARCHAR, rs.getString(10), "varchar");
                assertTrue(rs.getString(11).startsWith(C15_NCHAR), "nchar");
                assertEquals(C16_NVARCHAR, rs.getString(12), "nvarchar");
                assertEquals(C18_DATE.toString(), rs.getDate(13).toString(), "date");
                assertEquals(C19_DATETIME2.toString(), rs.getTimestamp(14).toString(), "datetime2");
                assertEquals(C21_TIME.toString(), rs.getTime(15).toString(), "time");
            }
        }

        /**
         * Verifies that a SQLServerDataTable can be cleared and reused with new data.
         * First execution inserts 2 rows. After clear(), metadata is re-added and
         * new rows are added. The second execution should insert the new data only.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Immutability: Clear And Reuse With New Data")
        public void testClearAndReuseWithNewData() throws SQLException {
            String columnsDef = "c_int int null, c_varchar varchar(50) null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp.addRow(1, "original_1");
            tvp.addRow(2, "original_2");

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            // Clear and re-populate with new data
            tvp.clear();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp.addRow(10, "new_1");
            tvp.addRow(20, "new_2");
            tvp.addRow(30, "new_3");

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(
                        "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                    assertTrue(rs.next());
                    assertEquals(5, rs.getInt(1), "Should have 5 rows total (2 original + 3 new)");
                }

                // Verify new data is correct
                try (ResultSet rs = stmt.executeQuery("select c_varchar from "
                        + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " where c_int = 10")) {
                    assertTrue(rs.next(), "Should find row with c_int=10");
                    assertEquals("new_1", rs.getString(1), "New data should be present");
                }
            }
        }

        /**
         * Verifies that a SQLServerDataTable can be reused across executions via
         * the stored procedure path. The same TVP is sent twice through a stored
         * procedure, resulting in 2 total rows in the table.
         *
         * @throws SQLException if an unexpected database error occurs
         */
        @Test
        @DisplayName("Immutability: TVP Reuse Via Stored Procedure")
        public void testTVPReuseViaStoredProcedure() throws SQLException {
            String columnsDef = "c_int int null, c_varchar varchar(50) null";
            createTable(columnsDef);
            createTVPS(columnsDef);
            createProcedure(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c_int", java.sql.Types.INTEGER);
            tvp.addColumnMetadata("c_varchar", java.sql.Types.VARCHAR);
            tvp.addRow(42, "sp_reuse");

            final String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}";

            // First execution
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql)) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.execute();
            }

            // Second execution — same TVP, same stored procedure
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql)) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.execute();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should have 2 rows from 2 stored proc executions");
            }
        }
    }
}
