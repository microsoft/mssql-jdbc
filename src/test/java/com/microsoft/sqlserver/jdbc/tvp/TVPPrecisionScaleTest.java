/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
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

import microsoft.sql.DateTimeOffset;


/**
 * Tests TVP precision and scale handling ported from FX PrecisionScale.java.
 * Validates a 33-column TVP covering varying precision/scale for numeric, decimal, float, real,
 * time(0/4/7), datetime2(3/4/7), datetimeoffset(0/4/7), and char/varchar/nchar/nvarchar with specific widths.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class TVPPrecisionScaleTest extends AbstractTest {

    private static String tvpName;
    private static String tableName;

    // Test constants matching FX PrecisionScale.java
    static final short C1_TINYINT = 250;
    static final short C2_TINYINT = 0;
    static final String C1_NUMERIC = "1234.12345";
    static final String C2_NUMERIC = "12345678912345.12345";
    static final String C3_NUMERIC = "1234567891234567891234.65981";
    static final String C4_NUMERIC = "123456789123456912345678912345678.12354";
    static final String C1_DECIMAL = "1234.12345";
    static final String C2_DECIMAL = "12345678912345.12345";
    static final String C3_DECIMAL = "1234567891234567891234.65981";
    static final String C4_DECIMAL = "123456789123456912345678912345678.12354";
    static final double C1_FLOAT = 1.85;
    static final String C2_FLOAT = "1.79E308";
    static final String C1_REAL = "3.4";
    static final String C2_REAL = "3.4E38";
    static final Time C1_TIME = Time.valueOf("11:05:34");
    static final Timestamp C1_DATETIME2 = Timestamp.valueOf("2012-01-01 11:05:34.123");
    static final String C1_CHAR = "abcd";
    static final String C2_CHAR = "abcdefghijklmnopqrst";
    static final String C1_VARCHAR = "xyz1xyz1";
    static final String C2_VARCHAR = "xyz1xyz1xyz1xyz1xyz1";
    static final String C3_VARCHAR = "xyz1xyz1xyz1xyz1xyz1xyz1xyz1xyz1xyz1xyz1xyz1xyz1xyz1xyz1xyz1";
    static final String C1_NCHAR = "\u4F60";
    static final String C2_NCHAR = "\u4F60\u597D\u554A\u4F60\u597D\u554A\u4F60\u597D\u554A";
    static final String C1_NVARCHAR = "\u4F60\u597D\u554A";
    static final String C2_NVARCHAR = "\u4F60\u597D\u554A\u4F60\u597D\u554A\u4F60\u597D\u554A\u4F60\u597D\u554A";
    static final String C3_NVARCHAR = "\u4F60\u597D\u554A\u4F60\u597D\u554A\u4F60\u597D\u554A\u4F60\u597D\u554A\u4F60\u597D\u554A\u4F60\u597D\u554A\u4F60\u597D\u554A\u4F60\u597D\u554A\u4F60\u597D\u554A";

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @BeforeEach
    public void testSetup() throws SQLException {
        tvpName = RandomUtil.getIdentifier("TVP");
        tableName = RandomUtil.getIdentifier("TVPPrecScale");
    }

    @AfterEach
    public void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTypeIfExists(tvpName, stmt);
        }
    }

    @AfterAll
    public static void terminate() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTypeIfExists(tvpName, stmt);
        }
    }

    // ==============================
    // Full Precision/Scale Test (33-column)
    // ==============================

    /**
     * Tests full 33-column precision/scale TVP matching FX PrecisionScale.java.
     * Covers varying precision numeric/decimal, float(15/30), real, time(0/4/7),
     * datetime2(3/4/7), datetimeoffset(0/4/7), char/varchar/nchar/nvarchar widths.
     */
    @Test
    @DisplayName("PrecisionScale: Full 33-Column TVP")
    public void testFull33ColumnPrecisionScale() throws SQLException {
        String columnsDef = "tinyint1 tinyint null, tinyint2 tinyint null, "
                + "numeric1 numeric(9,5) null, numeric2 numeric(19,5) null, "
                + "numeric3 numeric(28,5) null, numeric4 numeric(38,5) null, "
                + "decimal1 decimal(9,5) null, decimal2 decimal(19,5) null, "
                + "decimal3 decimal(28,5) null, decimal4 decimal(38,5) null, "
                + "float1 float(15) null, float2 float(30) null, "
                + "real1 real null, real2 real null, "
                + "Time1 time(0) null, Time2 time(4) null, Time3 time(7) null, "
                + "Datetime2_1 datetime2(3) null, Datetime2_2 datetime2(4) null, Datetime2_3 datetime2(7) null, "
                + "Datetimeoff_1 datetimeoffset(0) null, Datetimeoff_2 datetimeoffset(4) null, Datetimeoff_3 datetimeoffset(7) null, "
                + "Char1 char(8) null, Char2 char(40) null, "
                + "varchar1 varchar(8) null, varchar2 varchar(20) null, varchar3 varchar(max) null, "
                + "Nchar1 nchar(8) null, Nchar2 nchar(40) null, "
                + "Nvarchar1 nvarchar(8) null, Nvarchar2 nvarchar(60) null, Nvarchar3 nvarchar(max) null";

        createTable(columnsDef);
        createTVPS(columnsDef);

        DateTimeOffset dto0 = DateTimeOffset.valueOf(Timestamp.valueOf("2012-01-01 11:05:34"), 120);
        DateTimeOffset dto1 = DateTimeOffset.valueOf(C1_DATETIME2, 120);

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("tinyint1", java.sql.Types.TINYINT);
        tvp.addColumnMetadata("tinyint2", java.sql.Types.TINYINT);
        tvp.addColumnMetadata("numeric1", java.sql.Types.NUMERIC);
        tvp.addColumnMetadata("numeric2", java.sql.Types.NUMERIC);
        tvp.addColumnMetadata("numeric3", java.sql.Types.NUMERIC);
        tvp.addColumnMetadata("numeric4", java.sql.Types.NUMERIC);
        tvp.addColumnMetadata("decimal1", java.sql.Types.DECIMAL);
        tvp.addColumnMetadata("decimal2", java.sql.Types.DECIMAL);
        tvp.addColumnMetadata("decimal3", java.sql.Types.DECIMAL);
        tvp.addColumnMetadata("decimal4", java.sql.Types.DECIMAL);
        tvp.addColumnMetadata("float1", java.sql.Types.DOUBLE);
        tvp.addColumnMetadata("float2", java.sql.Types.DOUBLE);
        tvp.addColumnMetadata("real1", java.sql.Types.FLOAT);
        tvp.addColumnMetadata("real2", java.sql.Types.FLOAT);
        tvp.addColumnMetadata("TIME1", java.sql.Types.TIME);
        tvp.addColumnMetadata("TIME2", java.sql.Types.TIME);
        tvp.addColumnMetadata("TIME3", java.sql.Types.TIME);
        tvp.addColumnMetadata("datetime2_1", java.sql.Types.TIMESTAMP);
        tvp.addColumnMetadata("datetime2_2", java.sql.Types.TIMESTAMP);
        tvp.addColumnMetadata("datetime2_3", java.sql.Types.TIMESTAMP);
        tvp.addColumnMetadata("datetimeoffset_1", microsoft.sql.Types.DATETIMEOFFSET);
        tvp.addColumnMetadata("datetimeoffset_2", microsoft.sql.Types.DATETIMEOFFSET);
        tvp.addColumnMetadata("datetimeoffset_3", microsoft.sql.Types.DATETIMEOFFSET);
        tvp.addColumnMetadata("Char1", java.sql.Types.CHAR);
        tvp.addColumnMetadata("Char2", java.sql.Types.CHAR);
        tvp.addColumnMetadata("Varchar1", java.sql.Types.VARCHAR);
        tvp.addColumnMetadata("Varchar2", java.sql.Types.VARCHAR);
        tvp.addColumnMetadata("Varchar3", java.sql.Types.VARCHAR);
        tvp.addColumnMetadata("Nchar1", java.sql.Types.NCHAR);
        tvp.addColumnMetadata("Nchar2", java.sql.Types.NCHAR);
        tvp.addColumnMetadata("Nvarchar1", java.sql.Types.NVARCHAR);
        tvp.addColumnMetadata("Nvarchar2", java.sql.Types.NVARCHAR);
        tvp.addColumnMetadata("Nvarchar3", java.sql.Types.NVARCHAR);

        tvp.addRow(C1_TINYINT, C2_TINYINT, new BigDecimal(C1_NUMERIC), new BigDecimal(C2_NUMERIC),
                new BigDecimal(C3_NUMERIC), new BigDecimal(C4_NUMERIC), new BigDecimal(C1_DECIMAL),
                new BigDecimal(C2_DECIMAL), new BigDecimal(C3_DECIMAL), new BigDecimal(C4_DECIMAL), C1_FLOAT,
                Double.valueOf(C2_FLOAT), Float.valueOf(C1_REAL), Float.valueOf(C2_REAL), C1_TIME, C1_TIME, C1_TIME,
                C1_DATETIME2, C1_DATETIME2, C1_DATETIME2, dto0, dto1, dto1, C1_CHAR, C2_CHAR, C1_VARCHAR, C2_VARCHAR,
                C3_VARCHAR, C1_NCHAR, C2_NCHAR, C1_NVARCHAR, C2_NVARCHAR, C3_NVARCHAR);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.executeUpdate();
        }

        // Verify all columns
        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                ResultSet rs = stmt
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            assertTrue(rs.next(), "Should have one row");

            // Tinyint columns
            assertEquals(C1_TINYINT, rs.getShort(1), "tinyint1");
            assertEquals(C2_TINYINT, rs.getShort(2), "tinyint2");

            // Numeric columns - verify precision preserved
            assertEquals(0, new BigDecimal(C1_NUMERIC).compareTo(rs.getBigDecimal(3)),
                    "numeric(9,5) precision check");
            assertEquals(0, new BigDecimal(C2_NUMERIC).compareTo(rs.getBigDecimal(4)),
                    "numeric(19,5) precision check");
            assertEquals(0, new BigDecimal(C3_NUMERIC).setScale(5, java.math.RoundingMode.HALF_UP)
                    .compareTo(rs.getBigDecimal(5)), "numeric(28,5) precision check");
            assertEquals(0, new BigDecimal(C4_NUMERIC).setScale(5, java.math.RoundingMode.HALF_UP)
                    .compareTo(rs.getBigDecimal(6)), "numeric(38,5) precision check");

            // Decimal columns
            assertEquals(0, new BigDecimal(C1_DECIMAL).compareTo(rs.getBigDecimal(7)),
                    "decimal(9,5) precision check");
            assertEquals(0, new BigDecimal(C2_DECIMAL).compareTo(rs.getBigDecimal(8)),
                    "decimal(19,5) precision check");

            // Char/Varchar columns
            assertEquals(C1_CHAR, rs.getString(24).trim(), "char(8)");
            assertEquals(C2_CHAR, rs.getString(25).trim(), "char(40)");
            assertEquals(C1_VARCHAR, rs.getString(26), "varchar(8)");
            assertEquals(C2_VARCHAR, rs.getString(27), "varchar(20)");
            assertEquals(C3_VARCHAR, rs.getString(28), "varchar(max)");

            // NChar/NVarchar columns
            assertEquals(C1_NCHAR, rs.getString(29).trim(), "nchar(8)");
            assertEquals(C2_NCHAR, rs.getString(30).trim(), "nchar(40)");
            assertEquals(C1_NVARCHAR, rs.getString(31), "nvarchar(8)");
            assertEquals(C2_NVARCHAR, rs.getString(32), "nvarchar(60)");
            assertEquals(C3_NVARCHAR, rs.getString(33), "nvarchar(max)");
        }
    }

    // ==============================
    // Individual Precision/Scale Focus Tests
    // ==============================

    /**
     * Tests varying numeric precision levels: 9, 19, 28, 38 digits with scale 5.
     */
    @Test
    @DisplayName("PrecisionScale: Numeric Precision Levels")
    public void testNumericPrecisionLevels() throws SQLException {
        String columnsDef = "n1 numeric(9,5) null, n2 numeric(19,5) null, n3 numeric(28,5) null, n4 numeric(38,5) null";
        createTable(columnsDef);
        createTVPS(columnsDef);

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("n1", java.sql.Types.NUMERIC);
        tvp.addColumnMetadata("n2", java.sql.Types.NUMERIC);
        tvp.addColumnMetadata("n3", java.sql.Types.NUMERIC);
        tvp.addColumnMetadata("n4", java.sql.Types.NUMERIC);
        tvp.addRow(new BigDecimal(C1_NUMERIC), new BigDecimal(C2_NUMERIC), new BigDecimal(C3_NUMERIC),
                new BigDecimal(C4_NUMERIC));

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.executeUpdate();
        }

        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                ResultSet rs = stmt
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            assertTrue(rs.next());
            assertEquals(0, new BigDecimal(C1_NUMERIC).compareTo(rs.getBigDecimal(1)), "numeric(9,5)");
            assertEquals(0, new BigDecimal(C2_NUMERIC).compareTo(rs.getBigDecimal(2)), "numeric(19,5)");
        }
    }

    /**
     * Tests varying decimal precision levels: 9, 19, 28, 38 digits with scale 5.
     */
    @Test
    @DisplayName("PrecisionScale: Decimal Precision Levels")
    public void testDecimalPrecisionLevels() throws SQLException {
        String columnsDef = "d1 decimal(9,5) null, d2 decimal(19,5) null, d3 decimal(28,5) null, d4 decimal(38,5) null";
        createTable(columnsDef);
        createTVPS(columnsDef);

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("d1", java.sql.Types.DECIMAL);
        tvp.addColumnMetadata("d2", java.sql.Types.DECIMAL);
        tvp.addColumnMetadata("d3", java.sql.Types.DECIMAL);
        tvp.addColumnMetadata("d4", java.sql.Types.DECIMAL);
        tvp.addRow(new BigDecimal(C1_DECIMAL), new BigDecimal(C2_DECIMAL), new BigDecimal(C3_DECIMAL),
                new BigDecimal(C4_DECIMAL));

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.executeUpdate();
        }

        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                ResultSet rs = stmt
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            assertTrue(rs.next());
            assertEquals(0, new BigDecimal(C1_DECIMAL).compareTo(rs.getBigDecimal(1)), "decimal(9,5)");
            assertEquals(0, new BigDecimal(C2_DECIMAL).compareTo(rs.getBigDecimal(2)), "decimal(19,5)");
        }
    }

    /**
     * Tests float(15) vs float(30) precision.
     */
    @Test
    @DisplayName("PrecisionScale: Float Precision Levels")
    public void testFloatPrecisionLevels() throws SQLException {
        String columnsDef = "f1 float(15) null, f2 float(30) null";
        createTable(columnsDef);
        createTVPS(columnsDef);

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("f1", java.sql.Types.DOUBLE);
        tvp.addColumnMetadata("f2", java.sql.Types.DOUBLE);
        tvp.addRow(C1_FLOAT, Double.valueOf(C2_FLOAT));

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.executeUpdate();
        }

        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                ResultSet rs = stmt
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            assertTrue(rs.next());
            assertEquals(C1_FLOAT, rs.getDouble(1), 0.01, "float(15)");
            assertEquals(Double.valueOf(C2_FLOAT), rs.getDouble(2), 1e300, "float(30)");
        }
    }

    /**
     * Tests time with precision 0, 4, and 7.
     */
    @Test
    @DisplayName("PrecisionScale: Time Precision Levels (0/4/7)")
    public void testTimePrecisionLevels() throws SQLException {
        String columnsDef = "t0 time(0) null, t4 time(4) null, t7 time(7) null";
        createTable(columnsDef);
        createTVPS(columnsDef);

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("t0", java.sql.Types.TIME);
        tvp.addColumnMetadata("t4", java.sql.Types.TIME);
        tvp.addColumnMetadata("t7", java.sql.Types.TIME);
        tvp.addRow(C1_TIME, C1_TIME, C1_TIME);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.executeUpdate();
        }

        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                ResultSet rs = stmt
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            assertTrue(rs.next());
            // Time values retrieved - base comparison
            Time t0 = rs.getTime(1);
            Time t4 = rs.getTime(2);
            Time t7 = rs.getTime(3);
            assertEquals(C1_TIME.toString(), t0.toString(), "time(0) base value check");
            assertEquals(C1_TIME.toString(), t4.toString(), "time(4) base value check");
            assertEquals(C1_TIME.toString(), t7.toString(), "time(7) base value check");
        }
    }

    /**
     * Tests datetime2 with precision 3, 4, and 7.
     */
    @Test
    @DisplayName("PrecisionScale: Datetime2 Precision Levels (3/4/7)")
    public void testDatetime2PrecisionLevels() throws SQLException {
        String columnsDef = "dt3 datetime2(3) null, dt4 datetime2(4) null, dt7 datetime2(7) null";
        createTable(columnsDef);
        createTVPS(columnsDef);

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("dt3", java.sql.Types.TIMESTAMP);
        tvp.addColumnMetadata("dt4", java.sql.Types.TIMESTAMP);
        tvp.addColumnMetadata("dt7", java.sql.Types.TIMESTAMP);
        tvp.addRow(C1_DATETIME2, C1_DATETIME2, C1_DATETIME2);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.executeUpdate();
        }

        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                ResultSet rs = stmt
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            assertTrue(rs.next());
            Timestamp ts3 = rs.getTimestamp(1);
            Timestamp ts4 = rs.getTimestamp(2);
            Timestamp ts7 = rs.getTimestamp(3);
            // datetime2(3) should have 3 fractional digit precision
            assertTrue(ts3 != null, "datetime2(3) should not be null");
            assertTrue(ts4 != null, "datetime2(4) should not be null");
            assertTrue(ts7 != null, "datetime2(7) should not be null");
        }
    }

    /**
     * Tests datetimeoffset with precision 0, 4, and 7.
     */
    @Test
    @DisplayName("PrecisionScale: DateTimeOffset Precision Levels (0/4/7)")
    public void testDateTimeOffsetPrecisionLevels() throws SQLException {
        String columnsDef = "dto0 datetimeoffset(0) null, dto4 datetimeoffset(4) null, dto7 datetimeoffset(7) null";
        createTable(columnsDef);
        createTVPS(columnsDef);

        DateTimeOffset dto0 = DateTimeOffset.valueOf(Timestamp.valueOf("2012-01-01 11:05:34"), 120);
        DateTimeOffset dto1 = DateTimeOffset.valueOf(C1_DATETIME2, 120);

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("dto0", microsoft.sql.Types.DATETIMEOFFSET);
        tvp.addColumnMetadata("dto4", microsoft.sql.Types.DATETIMEOFFSET);
        tvp.addColumnMetadata("dto7", microsoft.sql.Types.DATETIMEOFFSET);
        tvp.addRow(dto0, dto1, dto1);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.executeUpdate();
        }

        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                ResultSet rs = stmt
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            assertTrue(rs.next());
            Object obj0 = rs.getObject(1);
            Object obj4 = rs.getObject(2);
            Object obj7 = rs.getObject(3);
            assertTrue(obj0 != null, "datetimeoffset(0) should not be null");
            assertTrue(obj4 != null, "datetimeoffset(4) should not be null");
            assertTrue(obj7 != null, "datetimeoffset(7) should not be null");
        }
    }

    /**
     * Tests char/varchar with specific widths.
     */
    @Test
    @DisplayName("PrecisionScale: String Width Levels")
    public void testStringWidthLevels() throws SQLException {
        String columnsDef = "c8 char(8) null, c40 char(40) null, "
                + "v8 varchar(8) null, v20 varchar(20) null, vmax varchar(max) null, "
                + "nc8 nchar(8) null, nc40 nchar(40) null, "
                + "nv8 nvarchar(8) null, nv60 nvarchar(60) null, nvmax nvarchar(max) null";
        createTable(columnsDef);
        createTVPS(columnsDef);

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c8", java.sql.Types.CHAR);
        tvp.addColumnMetadata("c40", java.sql.Types.CHAR);
        tvp.addColumnMetadata("v8", java.sql.Types.VARCHAR);
        tvp.addColumnMetadata("v20", java.sql.Types.VARCHAR);
        tvp.addColumnMetadata("vmax", java.sql.Types.VARCHAR);
        tvp.addColumnMetadata("nc8", java.sql.Types.NCHAR);
        tvp.addColumnMetadata("nc40", java.sql.Types.NCHAR);
        tvp.addColumnMetadata("nv8", java.sql.Types.NVARCHAR);
        tvp.addColumnMetadata("nv60", java.sql.Types.NVARCHAR);
        tvp.addColumnMetadata("nvmax", java.sql.Types.NVARCHAR);
        tvp.addRow(C1_CHAR, C2_CHAR, C1_VARCHAR, C2_VARCHAR, C3_VARCHAR, C1_NCHAR, C2_NCHAR, C1_NVARCHAR, C2_NVARCHAR,
                C3_NVARCHAR);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.executeUpdate();
        }

        try (Connection con = getConnection(); Statement stmt = con.createStatement();
                ResultSet rs = stmt
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            assertTrue(rs.next());
            assertEquals(C1_CHAR, rs.getString(1).trim(), "char(8)");
            assertEquals(C2_CHAR, rs.getString(2).trim(), "char(40)");
            assertEquals(C1_VARCHAR, rs.getString(3), "varchar(8)");
            assertEquals(C2_VARCHAR, rs.getString(4), "varchar(20)");
            assertEquals(C3_VARCHAR, rs.getString(5), "varchar(max)");
            assertEquals(C1_NCHAR, rs.getString(6).trim(), "nchar(8)");
            assertEquals(C2_NCHAR, rs.getString(7).trim(), "nchar(40)");
            assertEquals(C1_NVARCHAR, rs.getString(8), "nvarchar(8)");
            assertEquals(C2_NVARCHAR, rs.getString(9), "nvarchar(60)");
            assertEquals(C3_NVARCHAR, rs.getString(10), "nvarchar(max)");
        }
    }

    // ==============================
    // Helper Methods
    // ==============================

    private void createTable(String columnsDef) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.execute("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (" + columnsDef + ")");
        }
    }

    private void createTVPS(String columnsDef) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTypeIfExists(tvpName, stmt);
            stmt.executeUpdate(
                    "CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName) + " AS TABLE (" + columnsDef + ")");
        }
    }
}
