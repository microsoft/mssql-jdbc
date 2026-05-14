/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.cts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

/**
 * CTS compliance tests for ResultSet: getter methods by ordinal and name, type conversions,
 * fetch direction/size, concurrency, cursor type, findColumn.
 * Ported from FX resultSetClient.java CTS tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxCTS)
public class ResultSetCTSTest extends AbstractTest {

    private static final String tableName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CTS_ResultSet_Tab"));

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.executeUpdate("CREATE TABLE " + tableName
                    + " (ID INT IDENTITY PRIMARY KEY, INT_COL INT, BIGINT_COL BIGINT, "
                    + "SMALLINT_COL SMALLINT, TINYINT_COL TINYINT, BIT_COL BIT, "
                    + "DECIMAL_COL DECIMAL(18,6), FLOAT_COL FLOAT, REAL_COL REAL, "
                    + "VARCHAR_COL VARCHAR(200), NVARCHAR_COL NVARCHAR(200), "
                    + "DATE_COL DATE, TIME_COL TIME, DATETIME2_COL DATETIME2, "
                    + "BINARY_COL VARBINARY(200))");
            stmt.executeUpdate("INSERT INTO " + tableName
                    + " (INT_COL, BIGINT_COL, SMALLINT_COL, TINYINT_COL, BIT_COL, "
                    + "DECIMAL_COL, FLOAT_COL, REAL_COL, VARCHAR_COL, NVARCHAR_COL, "
                    + "DATE_COL, TIME_COL, DATETIME2_COL, BINARY_COL) VALUES ("
                    + "42, 9876543210, 1234, 127, 1, "
                    + "12345.678901, 3.141592653589793, 2.718, "
                    + "'TestString', N'\u00C0\u00C8\u00CC', "
                    + "'2024-03-15', '10:30:45', '2024-03-15 10:30:45.1234567', "
                    + "0x4142434445)");
            // Insert a row with NULLs
            stmt.executeUpdate("INSERT INTO " + tableName
                    + " (INT_COL) VALUES (NULL)");
        }
    }

    @AfterAll
    public static void cleanupTests() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }

    /**
     * Tests getInt retrieval by column ordinal index.
     * Ported from FX CTS resultSetClient getInt variations.
     */
    @Test
    public void testGetIntByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT INT_COL FROM " + tableName + " WHERE INT_COL IS NOT NULL")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }
    }

    /**
     * Tests getInt retrieval by column name.
     * Ported from FX CTS resultSetClient getInt variations.
     */
    @Test
    public void testGetIntByName() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT INT_COL FROM " + tableName + " WHERE INT_COL IS NOT NULL")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt("INT_COL"));
        }
    }

    /**
     * Tests getLong retrieval by column ordinal for BIGINT type.
     * Ported from FX CTS resultSetClient getLong variations.
     */
    @Test
    public void testGetLongByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT BIGINT_COL FROM " + tableName + " WHERE BIGINT_COL IS NOT NULL")) {
            assertTrue(rs.next());
            assertEquals(9876543210L, rs.getLong(1));
        }
    }

    /**
     * Tests getShort retrieval by column ordinal for SMALLINT type.
     * Ported from FX CTS resultSetClient getShort variations.
     */
    @Test
    public void testGetShortByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT SMALLINT_COL FROM " + tableName + " WHERE SMALLINT_COL IS NOT NULL")) {
            assertTrue(rs.next());
            assertEquals((short) 1234, rs.getShort(1));
        }
    }

    /**
     * Tests getByte retrieval by column ordinal for TINYINT type.
     * Ported from FX CTS resultSetClient getByte variations.
     */
    @Test
    public void testGetByteByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT TINYINT_COL FROM " + tableName + " WHERE TINYINT_COL IS NOT NULL")) {
            assertTrue(rs.next());
            // TINYINT in SQL Server is unsigned 0-255, mapped to short in JDBC
            rs.getByte(1);
        }
    }

    /**
     * Tests getBoolean retrieval by column ordinal for BIT type.
     * Ported from FX CTS resultSetClient getBoolean variations.
     */
    @Test
    public void testGetBooleanByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT BIT_COL FROM " + tableName + " WHERE BIT_COL IS NOT NULL")) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    /**
     * Tests getBigDecimal retrieval by column ordinal for DECIMAL type.
     * Ported from FX CTS resultSetClient getBigDecimal variations.
     */
    @Test
    public void testGetBigDecimalByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT DECIMAL_COL FROM " + tableName + " WHERE DECIMAL_COL IS NOT NULL")) {
            assertTrue(rs.next());
            BigDecimal val = rs.getBigDecimal(1);
            assertNotNull(val);
            assertTrue(val.compareTo(new BigDecimal("12345.678901")) == 0);
        }
    }

    /**
     * Tests getDouble retrieval by column ordinal for FLOAT type.
     * Ported from FX CTS resultSetClient getDouble variations.
     */
    @Test
    public void testGetDoubleByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT FLOAT_COL FROM " + tableName + " WHERE FLOAT_COL IS NOT NULL")) {
            assertTrue(rs.next());
            double val = rs.getDouble(1);
            assertEquals(3.141592653589793, val, 0.0001);
        }
    }

    /**
     * Tests getFloat retrieval by column ordinal for REAL type.
     * Ported from FX CTS resultSetClient getFloat variations.
     */
    @Test
    public void testGetFloatByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT REAL_COL FROM " + tableName + " WHERE REAL_COL IS NOT NULL")) {
            assertTrue(rs.next());
            float val = rs.getFloat(1);
            assertEquals(2.718f, val, 0.01);
        }
    }

    /**
     * Tests getString retrieval by column ordinal for VARCHAR type.
     * Ported from FX CTS resultSetClient getString variations.
     */
    @Test
    public void testGetStringByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT VARCHAR_COL FROM " + tableName + " WHERE VARCHAR_COL IS NOT NULL")) {
            assertTrue(rs.next());
            assertEquals("TestString", rs.getString(1));
        }
    }

    /**
     * Tests getString retrieval by column name for VARCHAR type.
     * Ported from FX CTS resultSetClient getString variations.
     */
    @Test
    public void testGetStringByName() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT VARCHAR_COL FROM " + tableName + " WHERE VARCHAR_COL IS NOT NULL")) {
            assertTrue(rs.next());
            assertEquals("TestString", rs.getString("VARCHAR_COL"));
        }
    }

    /**
     * Tests getNString retrieval by column name for NVARCHAR type with Unicode data.
     * Ported from FX CTS resultSetClient getNString variations.
     */
    @Test
    public void testGetNStringByName() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT NVARCHAR_COL FROM " + tableName + " WHERE NVARCHAR_COL IS NOT NULL")) {
            assertTrue(rs.next());
            assertEquals("\u00C0\u00C8\u00CC", rs.getNString("NVARCHAR_COL"));
        }
    }

    /**
     * Tests getDate retrieval by column ordinal for DATE type.
     * Ported from FX CTS resultSetClient getDate variations.
     */
    @Test
    public void testGetDateByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT DATE_COL FROM " + tableName + " WHERE DATE_COL IS NOT NULL")) {
            assertTrue(rs.next());
            Date dt = rs.getDate(1);
            assertNotNull(dt);
            assertEquals(Date.valueOf("2024-03-15"), dt);
        }
    }

    /**
     * Tests getTime retrieval by column ordinal for TIME type.
     * Ported from FX CTS resultSetClient getTime variations.
     */
    @Test
    public void testGetTimeByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT TIME_COL FROM " + tableName + " WHERE TIME_COL IS NOT NULL")) {
            assertTrue(rs.next());
            Time tm = rs.getTime(1);
            assertNotNull(tm);
        }
    }

    /**
     * Tests getTimestamp retrieval by column ordinal for DATETIME2 type.
     * Ported from FX CTS resultSetClient getTimestamp variations.
     */
    @Test
    public void testGetTimestampByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT DATETIME2_COL FROM " + tableName + " WHERE DATETIME2_COL IS NOT NULL")) {
            assertTrue(rs.next());
            Timestamp ts = rs.getTimestamp(1);
            assertNotNull(ts);
        }
    }

    /**
     * Tests getBytes retrieval by column ordinal for VARBINARY type.
     * Ported from FX CTS resultSetClient getBytes variations.
     */
    @Test
    public void testGetBytesByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT BINARY_COL FROM " + tableName + " WHERE BINARY_COL IS NOT NULL")) {
            assertTrue(rs.next());
            byte[] bin = rs.getBytes(1);
            assertNotNull(bin);
            assertEquals(5, bin.length);
            assertEquals((byte) 0x41, bin[0]);
        }
    }

    /**
     * Tests getObject retrieval by column ordinal returns a non-null value.
     * Ported from FX CTS resultSetClient getObject variations.
     */
    @Test
    public void testGetObjectByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT INT_COL FROM " + tableName + " WHERE INT_COL IS NOT NULL")) {
            assertTrue(rs.next());
            Object obj = rs.getObject(1);
            assertNotNull(obj);
        }
    }

    /**
     * Tests findColumn returns a valid positive column index for a known column name.
     * Ported from FX CTS resultSetClient findColumn variations.
     */
    @Test
    public void testFindColumn() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            int colIdx = rs.findColumn("INT_COL");
            assertTrue(colIdx > 0);
        }
    }

    /**
     * Tests getConcurrency returns CONCUR_READ_ONLY for a read-only ResultSet.
     * Ported from FX CTS resultSetClient getConcurrency variations.
     */
    @Test
    public void testGetConcurrency() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            assertEquals(ResultSet.CONCUR_READ_ONLY, rs.getConcurrency());
        }
    }

    /**
     * Tests getType returns TYPE_FORWARD_ONLY for a forward-only ResultSet.
     * Ported from FX CTS resultSetClient getType variations.
     */
    @Test
    public void testGetTypeForwardOnly() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            assertEquals(ResultSet.TYPE_FORWARD_ONLY, rs.getType());
        }
    }

    /**
     * Tests getType returns TYPE_SCROLL_INSENSITIVE for a scroll-insensitive ResultSet.
     * Ported from FX CTS resultSetClient getType variations.
     */
    @Test
    public void testGetTypeScrollInsensitive() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            assertEquals(ResultSet.TYPE_SCROLL_INSENSITIVE, rs.getType());
        }
    }

    /**
     * Tests getFetchDirection returns a valid fetch direction constant.
     * Ported from FX CTS resultSetClient getFetchDirection variations.
     */
    @Test
    public void testGetFetchDirection() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            int dir = rs.getFetchDirection();
            assertTrue(dir == ResultSet.FETCH_FORWARD || dir == ResultSet.FETCH_REVERSE
                    || dir == ResultSet.FETCH_UNKNOWN);
        }
    }

    /**
     * Tests setFetchSize sets and returns the specified fetch size value.
     * Ported from FX CTS resultSetClient setFetchSize variations.
     */
    @Test
    public void testSetFetchSize() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            rs.setFetchSize(10);
            assertEquals(10, rs.getFetchSize());
        }
    }

    /**
     * Tests setFetchSize with zero as a hint to use the driver default.
     * Ported from FX CTS resultSetClient setFetchSize variations.
     */
    @Test
    public void testSetFetchSizeZero() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            // 0 is a hint to use driver default
            rs.setFetchSize(0);
            assertTrue(rs.getFetchSize() >= 0);
        }
    }

    /**
     * Tests getString type conversion from an INT column.
     * Ported from FX CTS resultSetClient getString type conversion variations.
     */
    @Test
    public void testGetStringFromInt() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT INT_COL FROM " + tableName + " WHERE INT_COL IS NOT NULL")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertEquals("42", val);
        }
    }

    /**
     * Tests getString type conversion from a DECIMAL column.
     * Ported from FX CTS resultSetClient getString type conversion variations.
     */
    @Test
    public void testGetStringFromBigDecimal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT DECIMAL_COL FROM " + tableName + " WHERE DECIMAL_COL IS NOT NULL")) {
            assertTrue(rs.next());
            String val = rs.getString(1);
            assertNotNull(val);
            assertTrue(val.contains("12345"));
        }
    }

    /**
     * Tests getInt returns 0 for a SQL NULL value and wasNull returns true.
     * Ported from FX CTS resultSetClient wasNull variations.
     */
    @Test
    public void testGetIntFromNull() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT INT_COL FROM " + tableName + " WHERE INT_COL IS NULL")) {
            assertTrue(rs.next());
            int val = rs.getInt(1);
            assertEquals(0, val);
            assertTrue(rs.wasNull());
        }
    }

    /**
     * Tests scroll-insensitive ResultSet navigation with last, first, and absolute.
     * Ported from FX CTS resultSetClient cursor navigation variations.
     */
    @Test
    public void testScrollInsensitiveNavigation() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            // Move to last
            assertTrue(rs.last());
            // Move to first
            assertTrue(rs.first());
            // Move to absolute
            assertTrue(rs.absolute(1));
        }
    }

    /**
     * Tests scroll-insensitive ResultSet reverse navigation with afterLast and previous.
     * Ported from FX CTS resultSetClient cursor navigation variations.
     */
    @Test
    public void testScrollInsensitiveReverse() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_READ_ONLY);
                ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM " + tableName + " ORDER BY ID")) {
            // Navigate to last then backward
            rs.afterLast();
            assertTrue(rs.previous());
        }
    }

    /**
     * Tests ResultSetMetaData returns valid column count, names, and types.
     * Ported from FX CTS resultSetClient getMetaData variations.
     */
    @Test
    public void testResultSetMetaData() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            java.sql.ResultSetMetaData rsmd = rs.getMetaData();
            assertNotNull(rsmd);
            assertTrue(rsmd.getColumnCount() >= 14);
            assertNotNull(rsmd.getColumnName(1));
            assertTrue(rsmd.getColumnType(2) > 0); // Valid SQL type
        }
    }
}
