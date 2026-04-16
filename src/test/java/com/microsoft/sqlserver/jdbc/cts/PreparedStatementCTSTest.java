/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.cts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;

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
 * CTS compliance tests for PreparedStatement: setObject type matrix, parameter metadata,
 * clearParameters, setNull for all JDBC types, execute/executeQuery/executeUpdate.
 * Ported from FX prepStmtClient.java CTS tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxCTS)
public class PreparedStatementCTSTest extends AbstractTest {

    private static final String tableName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CTS_PrepStmt_Tab"));
    private static final String allTypesTable = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CTS_AllTypes_Tab"));

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.executeUpdate("CREATE TABLE " + tableName
                    + " (ID INT IDENTITY PRIMARY KEY, INT_VAL INT, VARCHAR_VAL VARCHAR(200), "
                    + "DECIMAL_VAL DECIMAL(18,6), FLOAT_VAL FLOAT, BIT_VAL BIT, "
                    + "DATE_VAL DATE, TIME_VAL TIME, TIMESTAMP_VAL DATETIME2, "
                    + "BINARY_VAL VARBINARY(200), BIGINT_VAL BIGINT, SMALLINT_VAL SMALLINT, "
                    + "TINYINT_VAL TINYINT, NVARCHAR_VAL NVARCHAR(200))");

            TestUtils.dropTableIfExists(allTypesTable, stmt);
            stmt.executeUpdate("CREATE TABLE " + allTypesTable
                    + " (INT_COL INT, VARCHAR_COL VARCHAR(200), DECIMAL_COL DECIMAL(18,6), "
                    + "FLOAT_COL FLOAT, DATE_COL DATE, BIN_COL VARBINARY(200))");
            stmt.executeUpdate("INSERT INTO " + allTypesTable
                    + " VALUES (100, 'TestValue', 123.456789, 3.14, '2024-03-15', 0x414243)");
        }
    }

    @AfterAll
    public static void cleanupTests() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTableIfExists(allTypesTable, stmt);
        }
    }

    /**
     * Tests that PreparedStatement.getMetaData returns valid ResultSetMetaData.
     * Ported from FX CTS prepStmtClient getMetaData variations.
     */
    @Test
    public void testGetMetaData() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + allTypesTable)) {
            ResultSetMetaData rsmd = ps.getMetaData();
            assertNotNull(rsmd);
            assertTrue(rsmd.getColumnCount() >= 6);
        }
    }

    /**
     * Tests that clearParameters resets all bound parameters, causing execution to fail.
     * Ported from FX CTS prepStmtClient clearParameters variations.
     */
    @Test
    public void testClearParameters() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (INT_VAL, VARCHAR_VAL) VALUES (?, ?)")) {
            ps.setInt(1, 42);
            ps.setString(2, "test");
            ps.clearParameters();
            // After clearing, execution without setting params should throw
            assertThrows(SQLException.class, () -> ps.executeUpdate());
        }
    }

    /**
     * Tests executeQuery with a parameterized WHERE clause returns expected results.
     * Ported from FX CTS prepStmtClient executeQuery variations.
     */
    @Test
    public void testExecuteQuery() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + allTypesTable + " WHERE INT_COL = ?")) {
            ps.setInt(1, 100);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(100, rs.getInt("INT_COL"));
                assertEquals("TestValue", rs.getString("VARCHAR_COL"));
            }
        }
    }

    /**
     * Tests executeUpdate inserts a row and returns correct update count.
     * Ported from FX CTS prepStmtClient executeUpdate variations.
     */
    @Test
    public void testExecuteUpdate() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (INT_VAL, VARCHAR_VAL) VALUES (?, ?)")) {
            ps.setInt(1, 999);
            ps.setString(2, "CTS_Update");
            int rows = ps.executeUpdate();
            assertEquals(1, rows);
        }
    }

    /**
     * Tests execute with a SELECT COUNT returns true and produces a ResultSet.
     * Ported from FX CTS prepStmtClient execute variations.
     */
    @Test
    public void testExecute() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + allTypesTable)) {
            boolean hasResultSet = ps.execute();
            assertTrue(hasResultSet);
            try (ResultSet rs = ps.getResultSet()) {
                assertTrue(rs.next());
                assertTrue(rs.getInt(1) >= 1);
            }
        }
    }

    /**
     * Tests setBigDecimal parameter binding and round-trip retrieval.
     * Ported from FX CTS prepStmtClient setBigDecimal variations.
     */
    @Test
    public void testSetBigDecimal() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (DECIMAL_VAL) VALUES (?)")) {
            BigDecimal val = new BigDecimal("12345.678901");
            ps.setBigDecimal(1, val);
            ps.executeUpdate();
        }
        // Verify
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT TOP 1 DECIMAL_VAL FROM " + tableName
                                + " WHERE DECIMAL_VAL IS NOT NULL ORDER BY ID DESC")) {
            assertTrue(rs.next());
            BigDecimal result = rs.getBigDecimal(1);
            assertNotNull(result);
        }
    }

    /**
     * Tests setBoolean parameter binding for a BIT column.
     * Ported from FX CTS prepStmtClient setBoolean variations.
     */
    @Test
    public void testSetBoolean() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (BIT_VAL) VALUES (?)")) {
            ps.setBoolean(1, true);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setByte parameter binding for a TINYINT column.
     * Ported from FX CTS prepStmtClient setByte variations.
     */
    @Test
    public void testSetByte() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (TINYINT_VAL) VALUES (?)")) {
            ps.setByte(1, (byte) 127);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setShort parameter binding for a SMALLINT column.
     * Ported from FX CTS prepStmtClient setShort variations.
     */
    @Test
    public void testSetShort() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (SMALLINT_VAL) VALUES (?)")) {
            ps.setShort(1, (short) 32000);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setInt parameter binding with Integer.MAX_VALUE.
     * Ported from FX CTS prepStmtClient setInt variations.
     */
    @Test
    public void testSetInt() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (INT_VAL) VALUES (?)")) {
            ps.setInt(1, Integer.MAX_VALUE);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setLong parameter binding with Long.MAX_VALUE for a BIGINT column.
     * Ported from FX CTS prepStmtClient setLong variations.
     */
    @Test
    public void testSetLong() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (BIGINT_VAL) VALUES (?)")) {
            ps.setLong(1, Long.MAX_VALUE);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setFloat parameter binding for a FLOAT column.
     * Ported from FX CTS prepStmtClient setFloat variations.
     */
    @Test
    public void testSetFloat() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (FLOAT_VAL) VALUES (?)")) {
            ps.setFloat(1, 3.14159f);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setDouble parameter binding for a FLOAT column.
     * Ported from FX CTS prepStmtClient setDouble variations.
     */
    @Test
    public void testSetDouble() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (FLOAT_VAL) VALUES (?)")) {
            ps.setDouble(1, Math.PI);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setString parameter binding for a VARCHAR column.
     * Ported from FX CTS prepStmtClient setString variations.
     */
    @Test
    public void testSetString() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (VARCHAR_VAL) VALUES (?)")) {
            ps.setString(1, "PreparedStatement CTS Test String");
            ps.executeUpdate();
        }
    }

    /**
     * Tests setDate parameter binding for a DATE column.
     * Ported from FX CTS prepStmtClient setDate variations.
     */
    @Test
    public void testSetDate() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (DATE_VAL) VALUES (?)")) {
            ps.setDate(1, Date.valueOf("2024-06-15"));
            ps.executeUpdate();
        }
    }

    /**
     * Tests setTime parameter binding for a TIME column.
     * Ported from FX CTS prepStmtClient setTime variations.
     */
    @Test
    public void testSetTime() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (TIME_VAL) VALUES (?)")) {
            ps.setTime(1, Time.valueOf("14:30:00"));
            ps.executeUpdate();
        }
    }

    /**
     * Tests setTimestamp parameter binding for a DATETIME2 column.
     * Ported from FX CTS prepStmtClient setTimestamp variations.
     */
    @Test
    public void testSetTimestamp() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (TIMESTAMP_VAL) VALUES (?)")) {
            ps.setTimestamp(1, Timestamp.valueOf("2024-06-15 14:30:45.123"));
            ps.executeUpdate();
        }
    }

    /**
     * Tests setBytes parameter binding for a VARBINARY column.
     * Ported from FX CTS prepStmtClient setBytes variations.
     */
    @Test
    public void testSetBytes() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (BINARY_VAL) VALUES (?)")) {
            ps.setBytes(1, new byte[] {0x41, 0x42, 0x43, 0x44, 0x45});
            ps.executeUpdate();
        }
    }

    /**
     * Tests setNull with Types.INTEGER for an INT column.
     * Ported from FX CTS prepStmtClient setNull variations.
     */
    @Test
    public void testSetNullInt() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (INT_VAL) VALUES (?)")) {
            ps.setNull(1, Types.INTEGER);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setNull with Types.VARCHAR for a VARCHAR column.
     * Ported from FX CTS prepStmtClient setNull variations.
     */
    @Test
    public void testSetNullVarchar() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (VARCHAR_VAL) VALUES (?)")) {
            ps.setNull(1, Types.VARCHAR);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setNull with Types.DECIMAL for a DECIMAL column.
     * Ported from FX CTS prepStmtClient setNull variations.
     */
    @Test
    public void testSetNullDecimal() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (DECIMAL_VAL) VALUES (?)")) {
            ps.setNull(1, Types.DECIMAL);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setNull with Types.DATE for a DATE column.
     * Ported from FX CTS prepStmtClient setNull variations.
     */
    @Test
    public void testSetNullDate() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (DATE_VAL) VALUES (?)")) {
            ps.setNull(1, Types.DATE);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setNull with Types.TIMESTAMP for a DATETIME2 column.
     * Ported from FX CTS prepStmtClient setNull variations.
     */
    @Test
    public void testSetNullTimestamp() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (TIMESTAMP_VAL) VALUES (?)")) {
            ps.setNull(1, Types.TIMESTAMP);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setNull with Types.VARBINARY for a VARBINARY column.
     * Ported from FX CTS prepStmtClient setNull variations.
     */
    @Test
    public void testSetNullBinary() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (BINARY_VAL) VALUES (?)")) {
            ps.setNull(1, Types.VARBINARY);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setObject type conversion from Integer to VARCHAR.
     * Ported from FX CTS prepStmtClient setObject type conversion variations.
     */
    @Test
    public void testSetObjectIntToVarchar() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (VARCHAR_VAL) VALUES (?)")) {
            ps.setObject(1, 42, Types.VARCHAR);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setObject type conversion from String to INTEGER.
     * Ported from FX CTS prepStmtClient setObject type conversion variations.
     */
    @Test
    public void testSetObjectStringToInt() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (INT_VAL) VALUES (?)")) {
            ps.setObject(1, "12345", Types.INTEGER);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setObject type conversion from String to DECIMAL.
     * Ported from FX CTS prepStmtClient setObject type conversion variations.
     */
    @Test
    public void testSetObjectStringToDecimal() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (DECIMAL_VAL) VALUES (?)")) {
            ps.setObject(1, "999.123456", Types.DECIMAL);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setObject with BigDecimal value targeting DECIMAL type.
     * Ported from FX CTS prepStmtClient setObject type conversion variations.
     */
    @Test
    public void testSetObjectBigDecimalToDecimal() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (DECIMAL_VAL) VALUES (?)")) {
            ps.setObject(1, new BigDecimal("54321.654321"), Types.DECIMAL);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setObject type conversion from Double to FLOAT.
     * Ported from FX CTS prepStmtClient setObject type conversion variations.
     */
    @Test
    public void testSetObjectDoubleToFloat() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (FLOAT_VAL) VALUES (?)")) {
            ps.setObject(1, 2.71828, Types.FLOAT);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setObject with Date value targeting DATE type.
     * Ported from FX CTS prepStmtClient setObject type conversion variations.
     */
    @Test
    public void testSetObjectStringToDate() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (DATE_VAL) VALUES (?)")) {
            ps.setObject(1, Date.valueOf("2024-12-25"), Types.DATE);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setObject with Timestamp value targeting TIMESTAMP type.
     * Ported from FX CTS prepStmtClient setObject type conversion variations.
     */
    @Test
    public void testSetObjectStringToTimestamp() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (TIMESTAMP_VAL) VALUES (?)")) {
            ps.setObject(1, Timestamp.valueOf("2024-12-25 23:59:59.999"), Types.TIMESTAMP);
            ps.executeUpdate();
        }
    }

    /**
     * Tests setNString parameter binding for an NVARCHAR column with Unicode characters.
     * Ported from FX CTS prepStmtClient setNString variations.
     */
    @Test
    public void testSetNString() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (NVARCHAR_VAL) VALUES (?)")) {
            ps.setNString(1, "\u00C0\u00C8\u00CC\u00D2\u00D9"); // ÀÈÌÒÙ
            ps.executeUpdate();
        }
        // Verify round-trip
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT TOP 1 NVARCHAR_VAL FROM " + tableName
                                + " WHERE NVARCHAR_VAL IS NOT NULL ORDER BY ID DESC")) {
            assertTrue(rs.next());
            assertEquals("\u00C0\u00C8\u00CC\u00D2\u00D9", rs.getNString(1));
        }
    }

    /**
     * Tests binding multiple parameters of different types in a single insert and verifies round-trip.
     * Ported from FX CTS prepStmtClient multiple parameter variations.
     */
    @Test
    public void testMultipleParamExecution() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName
                                + " (INT_VAL, VARCHAR_VAL, DECIMAL_VAL, FLOAT_VAL, BIT_VAL, DATE_VAL) VALUES (?,?,?,?,?,?)")) {
            ps.setInt(1, 777);
            ps.setString(2, "MultiParam");
            ps.setBigDecimal(3, new BigDecimal("99.99"));
            ps.setDouble(4, 1.618);
            ps.setBoolean(5, false);
            ps.setDate(6, Date.valueOf("2024-01-01"));
            int rows = ps.executeUpdate();
            assertEquals(1, rows);
        }
        // Verify
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "SELECT * FROM " + tableName + " WHERE INT_VAL = ? AND VARCHAR_VAL = ?")) {
            ps.setInt(1, 777);
            ps.setString(2, "MultiParam");
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(777, rs.getInt("INT_VAL"));
                assertEquals("MultiParam", rs.getString("VARCHAR_VAL"));
                assertFalse(rs.getBoolean("BIT_VAL"));
            }
        }
    }
}
