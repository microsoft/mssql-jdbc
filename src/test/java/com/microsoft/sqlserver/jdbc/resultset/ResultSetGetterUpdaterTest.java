/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.resultset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.NClob;
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
 * ResultSet getter/updater tests: accessor methods for all data types by ordinal and name,
 * type conversions, stream accessors (BinaryStream, CharacterStream, AsciiStream),
 * updater methods with moveToInsertRow.
 * Ported from FX resultset/resultsettest.java getter/updater tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxResultSet)
public class ResultSetGetterUpdaterTest extends AbstractTest {

    private static final String tableName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("RSGetUpd_Tab"));

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.executeUpdate("CREATE TABLE " + tableName
                    + " (ID INT IDENTITY PRIMARY KEY, INT_COL INT, BIGINT_COL BIGINT, "
                    + "SMALLINT_COL SMALLINT, TINYINT_COL TINYINT, BIT_COL BIT, "
                    + "DECIMAL_COL DECIMAL(18,6), FLOAT_COL FLOAT, REAL_COL REAL, "
                    + "VARCHAR_COL VARCHAR(500), NVARCHAR_COL NVARCHAR(500), "
                    + "DATE_COL DATE, TIME_COL TIME, DATETIME2_COL DATETIME2, "
                    + "VARBINARY_COL VARBINARY(500), VARCHAR_MAX_COL VARCHAR(MAX), "
                    + "NVARCHAR_MAX_COL NVARCHAR(MAX), VARBINARY_MAX_COL VARBINARY(MAX))");
            stmt.executeUpdate("INSERT INTO " + tableName
                    + " (INT_COL, BIGINT_COL, SMALLINT_COL, TINYINT_COL, BIT_COL, "
                    + "DECIMAL_COL, FLOAT_COL, REAL_COL, VARCHAR_COL, NVARCHAR_COL, "
                    + "DATE_COL, TIME_COL, DATETIME2_COL, VARBINARY_COL, "
                    + "VARCHAR_MAX_COL, NVARCHAR_MAX_COL, VARBINARY_MAX_COL) VALUES ("
                    + "42, 9876543210, 1234, 127, 1, "
                    + "12345.678901, 3.14159, 2.718, "
                    + "'TestVarchar', N'\u00C0\u00C8\u00CC\u00D2\u00D9', "
                    + "'2024-03-15', '10:30:45', '2024-03-15 10:30:45.1234567', "
                    + "0x4142434445, "
                    + "'LongVarcharMaxValue', N'\u00C0\u00C8NVarcharMax', 0x464748494A)");
        }
    }

    @AfterAll
    public static void cleanupTests() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }

    // Getter tests by ordinal
    @Test
    public void testGetIntByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT INT_COL FROM " + tableName)) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt(1));
        }
    }

    @Test
    public void testGetLongByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT BIGINT_COL FROM " + tableName)) {
            assertTrue(rs.next());
            assertEquals(9876543210L, rs.getLong(1));
        }
    }

    @Test
    public void testGetBigDecimalByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT DECIMAL_COL FROM " + tableName)) {
            assertTrue(rs.next());
            BigDecimal val = rs.getBigDecimal(1);
            assertNotNull(val);
            assertTrue(val.compareTo(new BigDecimal("12345.678901")) == 0);
        }
    }

    @Test
    public void testGetFloatByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT REAL_COL FROM " + tableName)) {
            assertTrue(rs.next());
            float val = rs.getFloat(1);
            assertEquals(2.718f, val, 0.01);
        }
    }

    @Test
    public void testGetDoubleByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT FLOAT_COL FROM " + tableName)) {
            assertTrue(rs.next());
            double val = rs.getDouble(1);
            assertEquals(3.14159, val, 0.001);
        }
    }

    @Test
    public void testGetStringByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT VARCHAR_COL FROM " + tableName)) {
            assertTrue(rs.next());
            assertEquals("TestVarchar", rs.getString(1));
        }
    }

    @Test
    public void testGetNStringByOrdinal() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT NVARCHAR_COL FROM " + tableName)) {
            assertTrue(rs.next());
            assertEquals("\u00C0\u00C8\u00CC\u00D2\u00D9", rs.getNString(1));
        }
    }

    // Getter tests by name
    @Test
    public void testGetIntByName() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT INT_COL FROM " + tableName)) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt("INT_COL"));
        }
    }

    @Test
    public void testGetStringByName() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT VARCHAR_COL FROM " + tableName)) {
            assertTrue(rs.next());
            assertEquals("TestVarchar", rs.getString("VARCHAR_COL"));
        }
    }

    // Type conversion tests
    @Test
    public void testGetStringFromInt() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT INT_COL FROM " + tableName)) {
            assertTrue(rs.next());
            assertEquals("42", rs.getString(1));
        }
    }

    @Test
    public void testGetBooleanFromBit() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT BIT_COL FROM " + tableName)) {
            assertTrue(rs.next());
            assertTrue(rs.getBoolean(1));
        }
    }

    @Test
    public void testGetShortFromSmallint() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT SMALLINT_COL FROM " + tableName)) {
            assertTrue(rs.next());
            assertEquals((short) 1234, rs.getShort(1));
        }
    }

    // Date/time getter tests
    @Test
    public void testGetDate() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT DATE_COL FROM " + tableName)) {
            assertTrue(rs.next());
            Date dt = rs.getDate(1);
            assertNotNull(dt);
        }
    }

    @Test
    public void testGetTime() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT TIME_COL FROM " + tableName)) {
            assertTrue(rs.next());
            Time tm = rs.getTime(1);
            assertNotNull(tm);
        }
    }

    @Test
    public void testGetTimestamp() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT DATETIME2_COL FROM " + tableName)) {
            assertTrue(rs.next());
            Timestamp ts = rs.getTimestamp(1);
            assertNotNull(ts);
        }
    }

    // Binary data getter tests
    @Test
    public void testGetBytes() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT VARBINARY_COL FROM " + tableName)) {
            assertTrue(rs.next());
            byte[] bin = rs.getBytes(1);
            assertNotNull(bin);
            assertEquals(5, bin.length);
        }
    }

    // Stream accessor tests
    @Test
    public void testGetBinaryStream() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT VARBINARY_MAX_COL FROM " + tableName)) {
            assertTrue(rs.next());
            try (InputStream is = rs.getBinaryStream(1)) {
                assertNotNull(is);
                assertTrue(is.available() > 0 || is.read() != -1);
            }
        }
    }

    @Test
    public void testGetAsciiStream() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT VARCHAR_MAX_COL FROM " + tableName)) {
            assertTrue(rs.next());
            try (InputStream is = rs.getAsciiStream(1)) {
                assertNotNull(is);
            }
        }
    }

    @Test
    public void testGetCharacterStream() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT NVARCHAR_MAX_COL FROM " + tableName)) {
            assertTrue(rs.next());
            try (Reader reader = rs.getCharacterStream(1)) {
                assertNotNull(reader);
                assertTrue(reader.read() != -1);
            }
        }
    }

    // LOB accessor tests
    @Test
    public void testGetBlob() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT VARBINARY_MAX_COL FROM " + tableName)) {
            assertTrue(rs.next());
            Blob blob = rs.getBlob(1);
            assertNotNull(blob);
            assertTrue(blob.length() > 0);
            blob.free();
        }
    }

    @Test
    public void testGetClob() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT VARCHAR_MAX_COL FROM " + tableName)) {
            assertTrue(rs.next());
            Clob clob = rs.getClob(1);
            assertNotNull(clob);
            assertTrue(clob.length() > 0);
            clob.free();
        }
    }

    @Test
    public void testGetNClob() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT NVARCHAR_MAX_COL FROM " + tableName)) {
            assertTrue(rs.next());
            NClob nclob = rs.getNClob(1);
            assertNotNull(nclob);
            assertTrue(nclob.length() > 0);
            nclob.free();
        }
    }

    // Updater tests
    @Test
    public void testUpdateString() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            assertTrue(rs.next());
            rs.updateString("VARCHAR_COL", "Updated");
            rs.updateRow();
            assertEquals("Updated", rs.getString("VARCHAR_COL"));
            // Restore original
            rs.updateString("VARCHAR_COL", "TestVarchar");
            rs.updateRow();
        }
    }

    @Test
    public void testUpdateInt() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            assertTrue(rs.next());
            rs.updateInt("INT_COL", 99);
            rs.updateRow();
            assertEquals(99, rs.getInt("INT_COL"));
            // Restore
            rs.updateInt("INT_COL", 42);
            rs.updateRow();
        }
    }

    @Test
    public void testInsertRow() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            rs.moveToInsertRow();
            rs.updateString("VARCHAR_COL", "InsertedRow");
            rs.updateInt("INT_COL", 555);
            rs.insertRow();
            rs.moveToCurrentRow();
        }
        // Verify inserted row
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM " + tableName + " WHERE INT_COL = 555")) {
            assertTrue(rs.next());
            assertEquals("InsertedRow", rs.getString("VARCHAR_COL"));
        }
    }

    @Test
    public void testDeleteRow() throws SQLException {
        // Insert a row to delete
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("INSERT INTO " + tableName + " (INT_COL, VARCHAR_COL) VALUES (9999, 'ToDelete')");
        }
        // Delete it via cursor
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM " + tableName + " WHERE INT_COL = 9999")) {
            if (rs.next()) {
                rs.deleteRow();
            }
        }
        // Verify deleted
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT COUNT(*) FROM " + tableName + " WHERE INT_COL = 9999")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1));
        }
    }

    // Object accessor
    @Test
    public void testGetObject() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
            assertTrue(rs.next());
            Object intObj = rs.getObject("INT_COL");
            assertNotNull(intObj);
            Object strObj = rs.getObject("VARCHAR_COL");
            assertNotNull(strObj);
            assertTrue(strObj instanceof String);
        }
    }
}
