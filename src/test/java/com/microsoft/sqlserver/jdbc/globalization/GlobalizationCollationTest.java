/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.globalization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

/**
 * Covers FX TCCollations (testSetters, testGetters) and TCServerCollations
 * (test1, test2).
 * Tests collation round-trip, MBCS conversion, _SC collations, and Turkish
 * CP1254.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.xAzureSQLDB)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxGlobalization)
@DisplayName("Globalization Collation Tests")
public class GlobalizationCollationTest extends AbstractTest {

    // Tracks databases created during tests for cleanup
    private static final List<String> createdDatabases = new ArrayList<>();

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @AfterAll
    public static void cleanUp() {
        // Clean up all databases created during testing
        for (String dbName : createdDatabases) {
            try {
                TestUtils.dropDatabaseIfExists(dbName, connectionString);
            } catch (SQLException e) {
                // Best-effort cleanup; log and continue
                System.err.println("Warning: Failed to drop database " + dbName + ": " + e.getMessage());
            }
        }
    }

    /**
     * Tests setString round-trip with SSPAU=true/false and MBCS-to-Unicode getter
     * comparison across 12 language collations in a single database per combo.
     * Covers FX TCCollations.testSetters and TCCollations.testGetters.
     */
    @ParameterizedTest(name = "Collation: {0} collation={1} SSPAU={3}")
    @MethodSource("com.microsoft.sqlserver.jdbc.globalization.GlobalizationTestData#collationSSPAUProvider")
    @DisplayName("TCCollations — setString round-trip + MBCS getter comparison")
    public void testCollationSetterAndGetter(String language, String collation, String sampleData,
            boolean sspauValue) throws SQLException {

        String dbName = RandomUtil.getIdentifierForDB("GlobCol_" + language.replaceAll("[^a-zA-Z]", ""));
        String setterTable = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("GlobSetTbl"));
        String getterTable = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("GlobGetTbl"));
        createdDatabases.add(dbName);

        // Create database with target collation
        try (Connection masterConn = PrepUtil.getConnection(connectionString + ";databaseName=master");
                Statement stmt = masterConn.createStatement()) {
            try {
                stmt.executeUpdate("CREATE DATABASE [" + dbName + "] COLLATE " + collation);
            } catch (SQLException e) {
                createdDatabases.remove(dbName);
                assumeTrue(false, "Collation " + collation + " not supported for " + language +
                        ": " + e.getMessage());
            }
        }

        // Connect with SSPAU for setter test
        String dbConnStr = connectionString + ";databaseName=" + dbName +
                ";sendStringParametersAsUnicode=" + sspauValue;

        try (Connection conn = PrepUtil.getConnection(dbConnStr);
                Statement stmt = conn.createStatement()) {

            // --- Setter test (FX TCCollations.testSetters) ---
            stmt.executeUpdate("CREATE TABLE " + setterTable + " (" +
                    "id INT IDENTITY(1,1) PRIMARY KEY, " +
                    "vc_col VARCHAR(200) COLLATE " + collation + ", " +
                    "nvc_col NVARCHAR(200))");

            // Insert via setString
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO " + setterTable + " (vc_col, nvc_col) VALUES (?, ?)")) {
                pstmt.setString(1, sampleData);
                pstmt.setNString(2, sampleData);
                int rows = pstmt.executeUpdate();
                assertEquals(1, rows, "Expected exactly 1 row inserted");
            }

            // Verify: SELECT back and compare both columns
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT vc_col, nvc_col FROM " + setterTable)) {
                assertTrue(rs.next(), "Expected at least one row");
                String nvcActual = rs.getString(2);
                assertEquals(sampleData, nvcActual,
                        "Unicode column mismatch for " + language + " with SSPAU=" + sspauValue);

                String vcActual = rs.getString(1);
                assertNotNull(vcActual,
                        "varchar column returned null for " + language + " with SSPAU=" + sspauValue);
                vcActual = vcActual.trim();
                assertEquals(nvcActual, vcActual,
                        "MBCS vs Unicode mismatch for " + language + " with SSPAU=" + sspauValue);
            }

            // --- Getter test (FX TCCollations.testGetters) ---
            stmt.executeUpdate("CREATE TABLE " + getterTable + " (" +
                    "id INT IDENTITY(1,1) PRIMARY KEY, " +
                    "nvc_col NVARCHAR(200), " +
                    "vc_col VARCHAR(200) COLLATE " + collation + ")");

            // Insert same data via literal into both columns
            stmt.executeUpdate("INSERT INTO " + getterTable +
                    " (nvc_col, vc_col) VALUES (N'" +
                    TestUtils.escapeSingleQuotes(sampleData) + "', N'" +
                    TestUtils.escapeSingleQuotes(sampleData) + "')");

            // Compare getString from both columns
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT nvc_col, vc_col FROM " + getterTable)) {
                assertTrue(rs.next(), "Expected at least one row");
                String unicodeResult = rs.getString(1);
                String mbcsResult = rs.getString(2);
                assertEquals(unicodeResult, mbcsResult,
                        "MBCS getString should match Unicode getString for " + language +
                                " (collation: " + collation + ", SSPAU=" + sspauValue + ")");
            }
        } finally {
            TestUtils.dropDatabaseIfExists(dbName, connectionString);
            createdDatabases.remove(dbName);
        }
    }

    /**
     * Provides all _SC collations from sys.fn_helpcollations() for server collation
     * tests.
     * Covers FX TCServerCollations.test1 with dynamic discovery of supported
     * collations.
     */
    static Stream<Arguments> serverCollationProvider() throws SQLException {
        List<Arguments> args = new ArrayList<>();
        try (Connection conn = PrepUtil.getConnection(connectionString);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT name FROM sys.fn_helpcollations() WHERE name LIKE '%\\_SC%' ESCAPE '\\' ORDER BY name")) {
            while (rs.next()) {
                args.add(Arguments.of(rs.getString(1)));
            }
        }
        // If no _SC collations found, include at least a common one for basic testing
        if (args.isEmpty()) {
            args.add(Arguments.of("Latin1_General_CI_AS"));
        }
        return args.stream();
    }

    /**
     * Tests nvarchar insert/select round-trip with dynamically discovered _SC
     * collations.
     * Covers FX TCServerCollations.test1.
     */
    @ParameterizedTest(name = "ServerCollation: {0}")
    @MethodSource("serverCollationProvider")
    @DisplayName("TCServerCollations.test1 — _SC collation round-trip")
    public void testServerCollationRoundTrip(String collationName) throws SQLException {
        String tableName = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("GlobSC"));
        String testData = "Hello \u00C4\u00D6\u00DC \u0410\u0411 \u4F60\u597D";

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            try {
                stmt.executeUpdate("CREATE TABLE " + tableName + " (" +
                        "id INT IDENTITY(1,1) PRIMARY KEY, " +
                        "data NVARCHAR(200) COLLATE " + collationName + ")");
            } catch (SQLException e) {
                if (e.getMessage().contains("not supported") || e.getMessage().contains("Cannot resolve")) {
                    assumeTrue(false, "Collation not supported: " + collationName +
                            ": " + e.getMessage());
                }
                throw e;
            }

            // Insert and verify
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO " + tableName + " (data) VALUES (?)")) {
                pstmt.setNString(1, testData);
                pstmt.executeUpdate();
            }

            try (ResultSet rs = stmt.executeQuery("SELECT data FROM " + tableName)) {
                assertTrue(rs.next(), "Expected at least one row");
                assertEquals(testData, rs.getString(1),
                        "Data mismatch with collation " + collationName);
            }
        } finally {
            try (Connection conn = getConnection();
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * Tests Turkish ğ (U+011F) round-trip via updatable ResultSet with CP1254
     * collation.
     * Covers FX TCServerCollations.test2 (VSTS #234287).
     */
    @Test
    @DisplayName("TCServerCollations.test2 — Turkish CP1254 ğ character via updatable ResultSet")
    public void testTurkishCP1254Character() throws SQLException {
        String tableName = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("GlobTR"));
        int expectedCharCode = 0x011F; // ğ
        String expectedStr = new String(new char[] { (char) expectedCharCode });

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            // Create table with varchar(1) using Turkish collation
            try {
                stmt.executeUpdate("CREATE TABLE " + tableName + " (" +
                        "id INT PRIMARY KEY, " +
                        "data VARCHAR(1) COLLATE Turkish_CI_AS)");
            } catch (SQLException e) {
                assumeTrue(false, "Turkish collation not supported: " + e.getMessage());
            }

            // Insert via updatable ResultSet to use column's charset
            try (Statement updStmt = conn.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = updStmt.executeQuery("SELECT * FROM " + tableName)) {
                rs.moveToInsertRow();
                rs.updateInt(1, expectedCharCode);
                rs.updateString(2, expectedStr);
                rs.insertRow();
            }

            // Verify
            try (ResultSet rs = stmt.executeQuery("SELECT data FROM " + tableName)) {
                assertTrue(rs.next(), "Expected at least one row");
                String actual = rs.getString(1);
                assertEquals(expectedCharCode, (int) actual.charAt(0),
                        "Turkish ğ character (U+011F) not preserved in CP1254 round-trip");
            }
        } finally {
            try (Connection conn = getConnection();
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * Tests nvarchar round-trip at boundary column sizes (10/100/4000).
     * Extends FX TCCollations coverage.
     */
    @ParameterizedTest(name = "ColumnSizes: {0} collation={1}")
    @MethodSource("com.microsoft.sqlserver.jdbc.globalization.GlobalizationTestData#collationLanguageProvider")
    @DisplayName("TCCollations extended — nvarchar boundary-size round-trip")
    public void testCollationColumnSizes(String language, String collation,
            String sampleData) throws SQLException {

        String tableName = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("GlobSize"));
        int[] sizes = { 10, 100, 4000 };

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            for (int size : sizes) {
                TestUtils.dropTableIfExists(tableName, stmt);

                stmt.executeUpdate("CREATE TABLE " + tableName + " (" +
                        "id INT IDENTITY(1,1) PRIMARY KEY, " +
                        "data NVARCHAR(" + size + ") COLLATE " + collation + ")");

                // Repeat sample data to fill the column (up to size limit, leaving margin)
                String testData = sampleData;
                while (testData.length() < size / 2 && testData.length() < 2000) {
                    testData = testData + " " + sampleData;
                }
                // Truncate if too long
                if (testData.length() > size) {
                    testData = testData.substring(0, size);
                }

                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT INTO " + tableName + " (data) VALUES (?)")) {
                    pstmt.setNString(1, testData);
                    pstmt.executeUpdate();
                }

                try (ResultSet rs = stmt.executeQuery("SELECT data FROM " + tableName)) {
                    assertTrue(rs.next(), "Expected at least one row");
                    assertEquals(testData, rs.getString(1),
                            "Data mismatch for " + language + " at size " + size);
                }
            }
        } finally {
            try (Connection conn = getConnection();
                    Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }
}
