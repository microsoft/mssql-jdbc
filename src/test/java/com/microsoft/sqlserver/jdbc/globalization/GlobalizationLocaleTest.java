/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.globalization;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Locale;
import java.util.TimeZone;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
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

/**
 * Covers FX TCQueries (test1, test2), TCCaseSensitivity, TCCalendar (test1,
 * test2), and TCLocales.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxGlobalization)
@DisplayName("Globalization Locale, Query, CaseSensitivity & Calendar Tests")
public class GlobalizationLocaleTest extends AbstractTest {

    private static String tableName;
    private static String escapedTableName;

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        tableName = RandomUtil.getIdentifier("GlobLocale");
        escapedTableName = AbstractSQLGenerator.escapeIdentifier(tableName);

        // Create a baseline table with some globalized data for locale/calendar tests
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("CREATE TABLE " + escapedTableName + " (" +
                    "id INT IDENTITY(1,1) PRIMARY KEY, " +
                    "str_col NVARCHAR(200), " +
                    "date_col DATE DEFAULT '2025-06-15', " +
                    "time_col TIME DEFAULT '14:30:45.1234567', " +
                    "dt2_col DATETIME2 DEFAULT '2025-06-15 14:30:45.1234567')");
            stmt.executeUpdate("INSERT INTO " + escapedTableName +
                    " (str_col) VALUES (N'Hello World \u00C4\u00D6\u00DC \u0410\u0411 \u4F60\u597D')");
        }
    }

    @AfterAll
    public static void cleanUp() throws SQLException {
        if (null != connection && !connection.isClosed()) {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(escapedTableName, stmt);
            }
        }
    }

    /** Provides non-RTL language samples for globalized identifier tests. */
    static Stream<Arguments> globalizedIdentifierProvider() {
        return GlobalizationTestData.LANGUAGE_SAMPLES.entrySet().stream()
                .filter(e -> !e.getKey().equals("Arabic"))
                // Skip RTL languages for identifiers — SQL Server has limited support for
                // RTL characters in identifiers even with bracket quoting
                .map(e -> Arguments.of(e.getKey(), e.getValue()));
    }

    /**
     * Tests INSERT/SELECT/UPDATE with Unicode table and column names (4 variations:
     * literal/param × insert/update).
     * Covers FX TCQueries.test1.
     */
    @ParameterizedTest(name = "GlobalizedIdentifiers: {0}")
    @MethodSource("globalizedIdentifierProvider")
    @DisplayName("TCQueries.test1 — Queries with globalized table/column names")
    public void testGlobalizedIdentifiers(String language, String sampleData) throws SQLException {

        // Use first 3 chars as identifier prefix
        String prefix = sampleData.substring(0, Math.min(3, sampleData.length()));
        String unicodeTableName = "[" + prefix + "_" + RandomUtil.getIdentifier("T") + "]";
        String unicodeColName = "[" + prefix + "_col]";

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            // Create table with Unicode identifier
            stmt.executeUpdate("CREATE TABLE " + unicodeTableName + " (" +
                    "id INT IDENTITY(1,1) PRIMARY KEY, " +
                    unicodeColName + " NVARCHAR(200))");

            // --- Variation 1: INSERT with literal ---
            stmt.executeUpdate("INSERT INTO " + unicodeTableName + " (" +
                    unicodeColName + ") VALUES (N'" +
                    TestUtils.escapeSingleQuotes(sampleData) + "')");

            // --- Variation 2: INSERT with parameter ---
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO " + unicodeTableName + " (" + unicodeColName + ") VALUES (?)")) {
                pstmt.setNString(1, sampleData);
                pstmt.executeUpdate();
            }

            // --- Variation 3: SELECT with literal (verify data) ---
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT " + unicodeColName + " FROM " + unicodeTableName)) {
                assertTrue(rs.next(), "Expected rows in table with Unicode name");
                assertEquals(sampleData, rs.getString(1),
                        "Data mismatch for globalized identifier in " + language);
            }

            // --- Variation 4: UPDATE with parameter ---
            String updatedData = sampleData + " updated";
            try (PreparedStatement pstmt = conn.prepareStatement(
                    "UPDATE " + unicodeTableName + " SET " + unicodeColName +
                            " = ? WHERE id = 1")) {
                pstmt.setNString(1, updatedData);
                int rows = pstmt.executeUpdate();
                assertEquals(1, rows, "Expected 1 row updated");
            }

            // Verify update
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT " + unicodeColName + " FROM " + unicodeTableName + " WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(updatedData, rs.getString(1),
                        "Updated data mismatch for globalized identifier in " + language);
            }
        } finally {
            try (Connection conn = getConnection();
                    Statement stmt = conn.createStatement()) {
                // Drop using raw SQL since the identifier may contain special chars
                try {
                    stmt.executeUpdate("DROP TABLE IF EXISTS " + unicodeTableName);
                } catch (SQLException ignored) {
                    // best-effort cleanup
                }
            }
        }
    }

    /**
     * Tests pure Unicode literal in a SELECT statement to verify driver transmits
     * Unicode correctly.
     * Covers FX TCQueries.test2.
     */
    @ParameterizedTest(name = "UnicodeLiteral: {0}")
    @MethodSource("globalizedIdentifierProvider")
    @DisplayName("TCQueries.test2 — Pure Unicode literal in SELECT")
    public void testPureUnicodeLiteral(String language, String sampleData) throws SQLException {

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            // SELECT N'<unicode data>' — verifies driver correctly transmits Unicode
            // literals
            try (ResultSet rs = stmt.executeQuery(
                    "SELECT N'" + TestUtils.escapeSingleQuotes(sampleData) + "'")) {
                assertTrue(rs.next(), "Expected one row");
                assertEquals(sampleData, rs.getString(1),
                        "Pure Unicode literal mismatch for " + language);
            }
        }
    }

    /**
     * Tests findColumn case-insensitive mapping across languages (4 variations:
     * upper↔lower). Covers FX TCCaseSensitivity.test1.
     */
    @ParameterizedTest(name = "CaseSensitivity: {0}")
    @MethodSource("com.microsoft.sqlserver.jdbc.globalization.GlobalizationTestData#caseSensitivityProvider")
    @DisplayName("TCCaseSensitivity.test1 — findColumn across languages (all 4 variations)")
    public void testCaseSensitivity(String language, char[] upperChars,
            char[] lowerChars) throws SQLException {

        // Variation 1: Upper(server) → Upper(client)
        verifyCaseSensitivity(language, upperChars, upperChars, "upper→upper");
        // Variation 2: Upper(server) → Lower(client)
        verifyCaseSensitivity(language, upperChars, lowerChars, "upper→lower");
        // Variation 3: Lower(server) → Lower(client)
        verifyCaseSensitivity(language, lowerChars, lowerChars, "lower→lower");
        // Variation 4: Lower(server) → Upper(client)
        verifyCaseSensitivity(language, lowerChars, upperChars, "lower→upper");
    }

    /**
     * Creates columns aliased with serverChars, then calls findColumn with
     * clientChars.
     */
    private void verifyCaseSensitivity(String language, char[] serverChars,
            char[] clientChars, String variationLabel) throws SQLException {

        // Build SELECT with column aliases using server-side characters
        StringBuilder selectCols = new StringBuilder();
        for (int i = 0; i < serverChars.length; i++) {
            if (i > 0)
                selectCols.append(", ");
            selectCols.append("'data" + i + "' AS [" + serverChars[i] + "]");
        }

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT " + selectCols.toString())) {

            assertTrue(rs.next());

            for (int c = 0; c < clientChars.length; c++) {
                String colName = String.valueOf(clientChars[c]);
                int expected = c + 1;
                int actual = rs.findColumn(colName);
                assertEquals(expected, actual,
                        "findColumn(" + variationLabel + ") ordinal mismatch for '" +
                                colName + "' in " + language);
            }
        }
    }

    /**
     * Tests getTime/getTimestamp with Calendar objects in various locales.
     * Covers FX TCCalendar.test1.
     */
    @ParameterizedTest(name = "Calendar: locale={0} type={1}")
    @MethodSource("com.microsoft.sqlserver.jdbc.globalization.GlobalizationTestData#calendarLocaleProvider")
    @DisplayName("TCCalendar.test1 — getDate/getTime/getTimestamp with Calendar locales")
    public void testCalendarLocale(Locale locale, String sqlType) throws SQLException {

        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"), locale);

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT date_col, time_col, dt2_col FROM " + escapedTableName)) {

            assertTrue(rs.next(), "Expected at least one row");

            // Validate against known values: time='14:30:45',
            // dt2='2025-06-15 14:30:45'
            switch (sqlType) {
                case "TIME": {
                    Time timeVal = rs.getTime(2, calendar);
                    assertNotNull(timeVal,
                            "getTime with Calendar(" + locale + ") returned null");
                    // Validate the hour/minute/second components match the inserted value
                    Calendar result = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                    result.setTime(timeVal);
                    assertEquals(14, result.get(Calendar.HOUR_OF_DAY),
                            "Hour mismatch with Calendar locale " + locale);
                    assertEquals(30, result.get(Calendar.MINUTE),
                            "Minute mismatch with Calendar locale " + locale);
                    assertEquals(45, result.get(Calendar.SECOND),
                            "Second mismatch with Calendar locale " + locale);
                    break;
                }
                case "DATETIME2": {
                    Timestamp tsVal = rs.getTimestamp(3, calendar);
                    assertNotNull(tsVal,
                            "getTimestamp with Calendar(" + locale + ") returned null");
                    // Validate both date and time components
                    Calendar result = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
                    result.setTime(tsVal);
                    assertEquals(2025, result.get(Calendar.YEAR),
                            "TS Year mismatch with Calendar locale " + locale);
                    assertEquals(Calendar.JUNE, result.get(Calendar.MONTH),
                            "TS Month mismatch with Calendar locale " + locale);
                    assertEquals(15, result.get(Calendar.DAY_OF_MONTH),
                            "TS Day mismatch with Calendar locale " + locale);
                    assertEquals(14, result.get(Calendar.HOUR_OF_DAY),
                            "TS Hour mismatch with Calendar locale " + locale);
                    assertEquals(30, result.get(Calendar.MINUTE),
                            "TS Minute mismatch with Calendar locale " + locale);
                    assertEquals(45, result.get(Calendar.SECOND),
                            "TS Second mismatch with Calendar locale " + locale);
                    break;
                }
            }
        }
    }

    /**
     * Tests stored proc temporal output params (TIME/DATETIME2) with Calendar
     * locales. Covers FX TCCalendar.test2.
     */
    @ParameterizedTest(name = "CalendarOutParam: locale={0} type={1}")
    @MethodSource("com.microsoft.sqlserver.jdbc.globalization.GlobalizationTestData#calendarLocaleProvider")
    @DisplayName("TCCalendar.test2 — Stored proc output params with Calendar locales")
    public void testCalendarOutputParams(Locale locale, String sqlType) throws SQLException {

        String procName = AbstractSQLGenerator.escapeIdentifier(
                RandomUtil.getIdentifier("GlobCalProc"));
        Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("GMT"), locale);

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            // Create stored procedure with temporal output parameter
            String paramType = sqlType.equals("TIME") ? "TIME" : "DATETIME2";

            stmt.executeUpdate("CREATE PROCEDURE " + procName +
                    " @argIn " + paramType + ", " +
                    " @argOut " + paramType + " OUTPUT " +
                    " AS SET @argOut = @argIn");

            try (CallableStatement cs = conn.prepareCall("{call " + procName + "(?, ?)}")) {
                switch (sqlType) {
                    case "TIME":
                        Time timeIn = Time.valueOf("14:30:45");
                        cs.setTime(1, timeIn, calendar);
                        cs.registerOutParameter(2, java.sql.Types.TIME);
                        cs.execute();
                        Time timeOut = cs.getTime(2, calendar);
                        assertNotNull(timeOut,
                                "Output TIME param with Calendar(" + locale + ") returned null");
                        assertEquals(timeIn.toString(), timeOut.toString(),
                                "Output TIME value mismatch with Calendar locale " + locale);
                        break;

                    case "DATETIME2":
                        Timestamp tsIn = Timestamp.valueOf("2025-06-15 14:30:45.123");
                        cs.setTimestamp(1, tsIn, calendar);
                        cs.registerOutParameter(2, java.sql.Types.TIMESTAMP);
                        cs.execute();
                        Timestamp tsOut = cs.getTimestamp(2, calendar);
                        assertNotNull(tsOut,
                                "Output DATETIME2 param with Calendar(" + locale + ") returned null");
                        assertEquals(tsIn.toString(), tsOut.toString(),
                                "Output DATETIME2 value mismatch with Calendar locale " + locale);
                        break;
                }
            } finally {
                TestUtils.dropProcedureIfExists(procName, stmt);
            }
        }
    }

    /**
     * Tests getString with every available JVM locale to verify no locale-dependent
     * failures.
     * Covers FX TCLocales.test.
     */
    @ParameterizedTest(name = "Locale: {0}")
    @MethodSource("com.microsoft.sqlserver.jdbc.globalization.GlobalizationTestData#localeProvider")
    @DisplayName("TCLocales.test — getString with each JVM locale")
    public void testGetStringWithLocale(Locale locale) throws SQLException {
        Locale originalLocale = Locale.getDefault();
        String expectedData = "Hello World \u00C4\u00D6\u00DC \u0410\u0411 \u4F60\u597D";

        try {
            Locale.setDefault(locale);

            try (Connection conn = getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT str_col FROM " + escapedTableName)) {

                assertTrue(rs.next(), "Expected at least one row");
                String actual = rs.getString(1);
                assertEquals(expectedData, actual,
                        "getString mismatch with JVM locale " + locale);
            }
        } finally {
            Locale.setDefault(originalLocale);
        }
    }
}
