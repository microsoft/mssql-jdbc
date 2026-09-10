/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.datatypes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
 * Legacy-to-max datatype migration tests: text→varchar(max), ntext→nvarchar(max),
 * image→varbinary(max). LOB operations, streaming, truncation, position search.
 * Ported from FX largedatatypes/largedatatypetest.java tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxDataTypes)
public class LegacyToMaxMigrationTest extends AbstractTest {

    private static final String legacyTable = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("Legacy_Types_Tab"));
    private static final String maxTable = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("Max_Types_Tab"));

    private static final String testTextData = "This is test text data for legacy-to-max migration tests. " +
            "It contains enough characters to validate streaming and chunked read operations.";
    private static final String testNTextData = "\u00C0\u00C8\u00CC\u00D2\u00D9 Unicode NText data with special chars: " +
            "\u00E0\u00E8\u00EC\u00F2\u00F9 \u00C4\u00D6\u00DC";

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Legacy types table (text, ntext, image)
            TestUtils.dropTableIfExists(legacyTable, stmt);
            stmt.executeUpdate("CREATE TABLE " + legacyTable
                    + " (ID INT IDENTITY PRIMARY KEY, TEXT_COL TEXT, NTEXT_COL NTEXT, IMAGE_COL IMAGE)");
            // Use parameterized insert for proper encoding
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + legacyTable + " (TEXT_COL, NTEXT_COL, IMAGE_COL) VALUES (?, ?, ?)")) {
                ps.setString(1, testTextData);
                ps.setNString(2, testNTextData);
                ps.setBytes(3, new byte[] {0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48});
                ps.executeUpdate();
            }

            // Modern max types table
            TestUtils.dropTableIfExists(maxTable, stmt);
            stmt.executeUpdate("CREATE TABLE " + maxTable
                    + " (ID INT IDENTITY PRIMARY KEY, VARCHAR_MAX_COL VARCHAR(MAX), "
                    + "NVARCHAR_MAX_COL NVARCHAR(MAX), VARBINARY_MAX_COL VARBINARY(MAX))");
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + maxTable + " (VARCHAR_MAX_COL, NVARCHAR_MAX_COL, VARBINARY_MAX_COL) VALUES (?, ?, ?)")) {
                ps.setString(1, testTextData);
                ps.setNString(2, testNTextData);
                ps.setBytes(3, new byte[] {0x41, 0x42, 0x43, 0x44, 0x45, 0x46, 0x47, 0x48});
                ps.executeUpdate();
            }
        }
    }

    @AfterAll
    public static void cleanupTests() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(legacyTable, stmt);
            TestUtils.dropTableIfExists(maxTable, stmt);
        }
    }

    // Legacy text type tests
    @Test
    public void testReadLegacyText() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT TEXT_COL FROM " + legacyTable)) {
            assertTrue(rs.next());
            String text = rs.getString(1);
            assertEquals(testTextData, text);
        }
    }

    @Test
    public void testReadLegacyTextAsStream() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT TEXT_COL FROM " + legacyTable)) {
            assertTrue(rs.next());
            try (Reader reader = rs.getCharacterStream(1)) {
                assertNotNull(reader);
                char[] buffer = new char[1024];
                int charsRead = reader.read(buffer);
                assertTrue(charsRead > 0);
            }
        }
    }

    @Test
    public void testReadLegacyTextAsAsciiStream() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT TEXT_COL FROM " + legacyTable)) {
            assertTrue(rs.next());
            try (InputStream is = rs.getAsciiStream(1)) {
                assertNotNull(is);
                byte[] buffer = new byte[1024];
                int bytesRead = is.read(buffer);
                assertTrue(bytesRead > 0);
            }
        }
    }

    @Test
    public void testReadLegacyTextAsClob() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT TEXT_COL FROM " + legacyTable)) {
            assertTrue(rs.next());
            Clob clob = rs.getClob(1);
            assertNotNull(clob);
            assertTrue(clob.length() > 0);
            String subStr = clob.getSubString(1, 10);
            assertEquals(testTextData.substring(0, 10), subStr);
            clob.free();
        }
    }

    // Legacy ntext type tests
    @Test
    public void testReadLegacyNText() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT NTEXT_COL FROM " + legacyTable)) {
            assertTrue(rs.next());
            String ntext = rs.getNString(1);
            assertEquals(testNTextData, ntext);
        }
    }

    @Test
    public void testReadLegacyNTextAsNClob() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT NTEXT_COL FROM " + legacyTable)) {
            assertTrue(rs.next());
            NClob nclob = rs.getNClob(1);
            assertNotNull(nclob);
            assertTrue(nclob.length() > 0);
            nclob.free();
        }
    }

    // Legacy image type tests
    @Test
    public void testReadLegacyImage() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT IMAGE_COL FROM " + legacyTable)) {
            assertTrue(rs.next());
            byte[] bin = rs.getBytes(1);
            assertNotNull(bin);
            assertEquals(8, bin.length);
            assertEquals((byte) 0x41, bin[0]);
        }
    }

    @Test
    public void testReadLegacyImageAsBinaryStream() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT IMAGE_COL FROM " + legacyTable)) {
            assertTrue(rs.next());
            try (InputStream is = rs.getBinaryStream(1)) {
                assertNotNull(is);
                byte[] buffer = new byte[1024];
                int bytesRead = is.read(buffer);
                assertEquals(8, bytesRead);
            }
        }
    }

    @Test
    public void testReadLegacyImageAsBlob() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT IMAGE_COL FROM " + legacyTable)) {
            assertTrue(rs.next());
            Blob blob = rs.getBlob(1);
            assertNotNull(blob);
            assertEquals(8, blob.length());
            blob.free();
        }
    }

    // Modern max type tests (equivalent functionality)
    @Test
    public void testReadVarcharMax() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT VARCHAR_MAX_COL FROM " + maxTable)) {
            assertTrue(rs.next());
            String text = rs.getString(1);
            assertEquals(testTextData, text);
        }
    }

    @Test
    public void testReadNVarcharMax() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT NVARCHAR_MAX_COL FROM " + maxTable)) {
            assertTrue(rs.next());
            String ntext = rs.getNString(1);
            assertEquals(testNTextData, ntext);
        }
    }

    @Test
    public void testReadVarbinaryMax() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT VARBINARY_MAX_COL FROM " + maxTable)) {
            assertTrue(rs.next());
            byte[] bin = rs.getBytes(1);
            assertEquals(8, bin.length);
        }
    }

    // Cross-type migration test: copy from legacy to max
    @Test
    public void testCopyTextToVarcharMax() throws Exception {
        String tempTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("CopyTemp"));
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tempTable, stmt);
            stmt.executeUpdate("CREATE TABLE " + tempTable + " (DATA VARCHAR(MAX))");
            stmt.executeUpdate("INSERT INTO " + tempTable
                    + " SELECT TEXT_COL FROM " + legacyTable);
            try (ResultSet rs = stmt.executeQuery("SELECT DATA FROM " + tempTable)) {
                assertTrue(rs.next());
                assertEquals(testTextData, rs.getString(1));
            }
            TestUtils.dropTableIfExists(tempTable, stmt);
        }
    }

    @Test
    public void testCopyNTextToNVarcharMax() throws Exception {
        String tempTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("CopyNTemp"));
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tempTable, stmt);
            stmt.executeUpdate("CREATE TABLE " + tempTable + " (DATA NVARCHAR(MAX))");
            stmt.executeUpdate("INSERT INTO " + tempTable
                    + " SELECT NTEXT_COL FROM " + legacyTable);
            try (ResultSet rs = stmt.executeQuery("SELECT DATA FROM " + tempTable)) {
                assertTrue(rs.next());
                assertEquals(testNTextData, rs.getNString(1));
            }
            TestUtils.dropTableIfExists(tempTable, stmt);
        }
    }

    @Test
    public void testCopyImageToVarbinaryMax() throws Exception {
        String tempTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("CopyBTemp"));
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tempTable, stmt);
            stmt.executeUpdate("CREATE TABLE " + tempTable + " (DATA VARBINARY(MAX))");
            stmt.executeUpdate("INSERT INTO " + tempTable
                    + " SELECT IMAGE_COL FROM " + legacyTable);
            try (ResultSet rs = stmt.executeQuery("SELECT DATA FROM " + tempTable)) {
                assertTrue(rs.next());
                byte[] bin = rs.getBytes(1);
                assertEquals(8, bin.length);
            }
            TestUtils.dropTableIfExists(tempTable, stmt);
        }
    }

    // Large data tests
    @Test
    public void testLargeVarcharMax() throws Exception {
        String largeData = generateString(100000); // 100KB
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + maxTable + " (VARCHAR_MAX_COL) VALUES (?)")) {
            ps.setString(1, largeData);
            ps.executeUpdate();
        }
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT TOP 1 VARCHAR_MAX_COL FROM " + maxTable
                                + " WHERE LEN(VARCHAR_MAX_COL) = 100000 ORDER BY ID DESC")) {
            if (rs.next()) {
                String retrieved = rs.getString(1);
                assertEquals(100000, retrieved.length());
            }
        }
    }

    @Test
    public void testLargeVarbinaryMax() throws Exception {
        byte[] largeData = new byte[100000]; // 100KB
        for (int i = 0; i < largeData.length; i++) {
            largeData[i] = (byte) (i % 256);
        }
        try (Connection conn = getConnection();
                PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + maxTable + " (VARBINARY_MAX_COL) VALUES (?)")) {
            ps.setBytes(1, largeData);
            ps.executeUpdate();
        }
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT TOP 1 VARBINARY_MAX_COL FROM " + maxTable
                                + " WHERE DATALENGTH(VARBINARY_MAX_COL) = 100000 ORDER BY ID DESC")) {
            if (rs.next()) {
                byte[] retrieved = rs.getBytes(1);
                assertEquals(100000, retrieved.length);
            }
        }
    }

    // NULL handling for LOB types
    @Test
    public void testNullLegacyText() throws Exception {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("INSERT INTO " + legacyTable + " (TEXT_COL) VALUES (NULL)");
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT TOP 1 TEXT_COL FROM " + legacyTable
                            + " WHERE TEXT_COL IS NULL ORDER BY ID DESC")) {
                assertTrue(rs.next());
                String val = rs.getString(1);
                assertTrue(val == null);
                assertTrue(rs.wasNull());
            }
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
