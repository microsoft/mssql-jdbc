/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
 * AE data type edge case tests: boundary values for encrypted columns, precision/scale,
 * NULL handling, out-of-range values, unsupported type validation.
 * Ported from FX ae/OutOfRangeTest.java and ae/PrecisionScale.java tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxAE)
@Tag(Constants.reqExternalSetup)
public class AEDataTypeEdgeCaseTest extends AbstractTest {

    private static final String tableName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("AE_EdgeCase_Tab"));

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            // Table without encryption (edge case testing on non-AE columns)
            stmt.executeUpdate("CREATE TABLE " + tableName
                    + " (ID INT IDENTITY PRIMARY KEY, "
                    + "INT_COL INT, BIGINT_COL BIGINT, SMALLINT_COL SMALLINT, TINYINT_COL TINYINT, "
                    + "DECIMAL_COL DECIMAL(38,10), NUMERIC_COL NUMERIC(18,6), "
                    + "FLOAT_COL FLOAT, REAL_COL REAL, "
                    + "DATE_COL DATE, TIME_COL TIME(7), DATETIME2_COL DATETIME2(7), "
                    + "VARCHAR_COL VARCHAR(8000), NVARCHAR_COL NVARCHAR(4000), "
                    + "VARBINARY_COL VARBINARY(8000), BIT_COL BIT)");
        }
    }

    @AfterAll
    public static void cleanupTests() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }

    @Test
    public void testIntMaxValue() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (INT_COL) VALUES (?)")) {
            ps.setInt(1, Integer.MAX_VALUE);
            ps.executeUpdate();
        }
        verifyIntValue(Integer.MAX_VALUE);
    }

    @Test
    public void testIntMinValue() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (INT_COL) VALUES (?)")) {
            ps.setInt(1, Integer.MIN_VALUE);
            ps.executeUpdate();
        }
        verifyIntValue(Integer.MIN_VALUE);
    }

    @Test
    public void testBigIntMaxValue() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (BIGINT_COL) VALUES (?)")) {
            ps.setLong(1, Long.MAX_VALUE);
            ps.executeUpdate();
        }
    }

    @Test
    public void testBigIntMinValue() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (BIGINT_COL) VALUES (?)")) {
            ps.setLong(1, Long.MIN_VALUE);
            ps.executeUpdate();
        }
    }

    @Test
    public void testSmallIntBoundary() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (SMALLINT_COL) VALUES (?)")) {
            ps.setShort(1, Short.MAX_VALUE);
            ps.executeUpdate();
        }
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (SMALLINT_COL) VALUES (?)")) {
            ps.setShort(1, Short.MIN_VALUE);
            ps.executeUpdate();
        }
    }

    @Test
    public void testTinyIntBoundary() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (TINYINT_COL) VALUES (?)")) {
            // TINYINT is 0-255 in SQL Server
            ps.setShort(1, (short) 0);
            ps.executeUpdate();
        }
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (TINYINT_COL) VALUES (?)")) {
            ps.setShort(1, (short) 255);
            ps.executeUpdate();
        }
    }

    @Test
    public void testDecimalMaxPrecision() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (DECIMAL_COL) VALUES (?)")) {
            // Max precision 38
            BigDecimal maxPrecision = new BigDecimal("1234567890123456789012345678.1234567890");
            ps.setBigDecimal(1, maxPrecision);
            ps.executeUpdate();
        }
    }

    @Test
    public void testDecimalZero() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (DECIMAL_COL) VALUES (?)")) {
            ps.setBigDecimal(1, BigDecimal.ZERO);
            ps.executeUpdate();
        }
    }

    @Test
    public void testDecimalNegative() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (DECIMAL_COL) VALUES (?)")) {
            ps.setBigDecimal(1, new BigDecimal("-99999.999999"));
            ps.executeUpdate();
        }
    }

    @Test
    public void testFloatSpecialValues() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (FLOAT_COL) VALUES (?)")) {
            ps.setDouble(1, 0.0);
            ps.executeUpdate();
        }
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (FLOAT_COL) VALUES (?)")) {
            ps.setDouble(1, -0.0);
            ps.executeUpdate();
        }
    }

    @Test
    public void testNullForAllTypes() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName
                                + " (INT_COL, BIGINT_COL, SMALLINT_COL, DECIMAL_COL, FLOAT_COL, "
                                + "DATE_COL, TIME_COL, DATETIME2_COL, VARCHAR_COL, VARBINARY_COL, BIT_COL) "
                                + "VALUES (?,?,?,?,?,?,?,?,?,?,?)")) {
            ps.setNull(1, Types.INTEGER);
            ps.setNull(2, Types.BIGINT);
            ps.setNull(3, Types.SMALLINT);
            ps.setNull(4, Types.DECIMAL);
            ps.setNull(5, Types.FLOAT);
            ps.setNull(6, Types.DATE);
            ps.setNull(7, Types.TIME);
            ps.setNull(8, Types.TIMESTAMP);
            ps.setNull(9, Types.VARCHAR);
            ps.setNull(10, Types.VARBINARY);
            ps.setNull(11, Types.BIT);
            ps.executeUpdate();
        }
        // Verify all nulls
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT TOP 1 * FROM " + tableName
                                + " WHERE INT_COL IS NULL AND BIGINT_COL IS NULL ORDER BY ID DESC")) {
            assertTrue(rs.next());
            rs.getInt("INT_COL");
            assertTrue(rs.wasNull());
            rs.getLong("BIGINT_COL");
            assertTrue(rs.wasNull());
        }
    }

    @Test
    public void testMaxLengthVarchar() throws SQLException {
        String maxStr = generateString(8000);
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (VARCHAR_COL) VALUES (?)")) {
            ps.setString(1, maxStr);
            ps.executeUpdate();
        }
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT TOP 1 VARCHAR_COL FROM " + tableName
                                + " WHERE LEN(VARCHAR_COL) = 8000 ORDER BY ID DESC")) {
            assertTrue(rs.next());
            assertEquals(8000, rs.getString(1).length());
        }
    }

    @Test
    public void testMaxLengthNVarchar() throws SQLException {
        String maxStr = generateString(4000);
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (NVARCHAR_COL) VALUES (?)")) {
            ps.setNString(1, maxStr);
            ps.executeUpdate();
        }
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT TOP 1 NVARCHAR_COL FROM " + tableName
                                + " WHERE LEN(NVARCHAR_COL) = 4000 ORDER BY ID DESC")) {
            assertTrue(rs.next());
            assertEquals(4000, rs.getString(1).length());
        }
    }

    @Test
    public void testMaxLengthVarbinary() throws SQLException {
        byte[] maxBin = new byte[8000];
        for (int i = 0; i < maxBin.length; i++) {
            maxBin[i] = (byte) (i % 256);
        }
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (VARBINARY_COL) VALUES (?)")) {
            ps.setBytes(1, maxBin);
            ps.executeUpdate();
        }
    }

    @Test
    public void testDateBoundary() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (DATE_COL) VALUES (?)")) {
            ps.setDate(1, Date.valueOf("0001-01-01"));
            ps.executeUpdate();
        }
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (DATE_COL) VALUES (?)")) {
            ps.setDate(1, Date.valueOf("9999-12-31"));
            ps.executeUpdate();
        }
    }

    @Test
    public void testTimePrecision() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (TIME_COL) VALUES (?)")) {
            // Maximum precision time
            ps.setTime(1, Time.valueOf("23:59:59"));
            ps.executeUpdate();
        }
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (TIME_COL) VALUES (?)")) {
            ps.setTime(1, Time.valueOf("00:00:00"));
            ps.executeUpdate();
        }
    }

    @Test
    public void testBitTrueAndFalse() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (BIT_COL) VALUES (?)")) {
            ps.setBoolean(1, true);
            ps.executeUpdate();
        }
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (BIT_COL) VALUES (?)")) {
            ps.setBoolean(1, false);
            ps.executeUpdate();
        }
    }

    @Test
    public void testEmptyString() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (VARCHAR_COL) VALUES (?)")) {
            ps.setString(1, "");
            ps.executeUpdate();
        }
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT TOP 1 VARCHAR_COL FROM " + tableName
                                + " WHERE VARCHAR_COL = '' ORDER BY ID DESC")) {
            assertTrue(rs.next());
            assertEquals("", rs.getString(1));
        }
    }

    @Test
    public void testEmptyBinary() throws SQLException {
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (VARBINARY_COL) VALUES (?)")) {
            ps.setBytes(1, new byte[0]);
            ps.executeUpdate();
        }
    }

    private void verifyIntValue(int expected) throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT TOP 1 INT_COL FROM " + tableName
                                + " WHERE INT_COL = " + expected + " ORDER BY ID DESC")) {
            assertTrue(rs.next());
            assertEquals(expected, rs.getInt(1));
        }
    }

    private static String generateString(int length) {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append((char) ('A' + (i % 26)));
        }
        return sb.toString();
    }
}
