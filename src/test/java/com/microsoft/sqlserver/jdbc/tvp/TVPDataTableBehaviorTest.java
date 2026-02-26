/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
public class TVPDataTableBehaviorTest extends AbstractTest {

    private static String tvpName;
    private static String tableName;
    private static String procedureName;

    // Batch test constants matching FX TVPbatchQuery.java
    static final int C1_BIT = 1;
    static final short C2_TINYINT = 250;
    static final short C3_SMALLINT = -25;
    static final int C4_INT = 951;
    static final long C5_BIGINT = -922;
    static final double C6_FLOAT = 34;
    static final double C7_FLOAT = 1.123;
    static final float C8_REAL = (float) 5.324;
    static final BigDecimal C9_DECIMAL = BigDecimal.valueOf(12345.12345);
    static final String C12_CHAR = "abcd";
    static final String C13_VARCHAR = "xyz1";
    static final String C14_VARCHARMAX = "hello world!!";
    static final String C15_NCHAR = "\u4F60\u597D\u554A";
    static final String C16_NVARCHAR = "\u4F60\u597D\u554A";
    static final Date C18_DATE = Date.valueOf("2012-01-01");
    static final Timestamp C19_DATETIME2 = Timestamp.valueOf("2012-01-01 11:05:34.123");
    static final Time C21_TIME = Time.valueOf("11:05:34");

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @BeforeEach
    public void testSetup() throws SQLException {
        tvpName = RandomUtil.getIdentifier("TVP");
        tableName = RandomUtil.getIdentifier("TVPBehTable");
        procedureName = RandomUtil.getIdentifier("spTvpBeh");
    }

    @AfterEach
    public void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(procedureName, stmt);
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTypeIfExists(tvpName, stmt);
        }
    }

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

    private void createTable(String columnsDef) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.execute("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (" + columnsDef + ")");
        }
    }

    private void createTVPS(String columnsDef) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTypeIfExists(tvpName, stmt);
            stmt.executeUpdate("CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName)
                    + " AS TABLE (" + columnsDef + ")");
        }
    }

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
     * Tests TVP null handling scenarios ported from FX NullTVP.java and TypesNull.java.
     * Covers: null TVP object, null TVP name, null columns, under-populated rows, null column names.
     */
    @Nested
    @DisplayName("NullHandling")
    class NullHandling {

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
            } catch (SQLException e) {
                assertTrue(e.getMessage() != null, "Should have error message for null TVP name");
            }
        }

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

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1), "Should have 4 rows");
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery("select count(*) from "
                            + AbstractSQLGenerator.escapeIdentifier(tableName) + " where c_int is null")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should have 2 null rows");
            }
        }

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

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery("select * from "
                            + AbstractSQLGenerator.escapeIdentifier(tableName) + " order by c_varchar")) {
                assertTrue(rs.next());
                rs.getObject(1);
                assertTrue(rs.wasNull(), "First row int should be null");
                assertEquals("hello", rs.getString(2));
                assertEquals(true, rs.getBoolean(3));

                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
                rs.getObject(2);
                assertTrue(rs.wasNull(), "Second row varchar should be null");

                assertTrue(rs.next());
                assertEquals(99, rs.getInt(1));
                assertEquals("world", rs.getString(2));
                rs.getObject(3);
                assertTrue(rs.wasNull(), "Third row bit should be null");
            }
        }
    }

    // ================================================================
    // Nested Class: Batch Operation Tests (from TVPBatchTest.java)
    // ================================================================

    /**
     * Tests TVP addBatch/executeBatch operations ported from FX TVPbatchQuery.java.
     * Covers batch operations with null columns, multiple batches, all data types,
     * stored procedures, empty TVP, and multiple rows.
     */
    @Nested
    @DisplayName("BatchOperations")
    class BatchOperations {

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
    }

    // ================================================================
    // Nested Class: Immutability Tests (from TVPImmutabilityTest.java)
    // ================================================================

    /**
     * Tests TVP data immutability and duplicate column name handling ported from FX TVPNotModified.java.
     * Covers: duplicate column names, data persistence after execution, reuse, mutation, and clear.
     */
    @Nested
    @DisplayName("Immutability")
    class Immutability {

        @Test
        @DisplayName("Immutability: Duplicate Column Names")
        public void testDuplicateColumnNames() throws SQLException {
            String columnsDef = "c_int int null, c_int2 int null";
            createTable(columnsDef);
            createTVPS(columnsDef);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("yes", java.sql.Types.INTEGER);
            tvp.addColumnMetadata("yes", java.sql.Types.INTEGER);
            tvp.addRow(1);
            tvp.addRow(1);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.executeUpdate();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should have 2 rows");
            }
        }

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
    }
}
