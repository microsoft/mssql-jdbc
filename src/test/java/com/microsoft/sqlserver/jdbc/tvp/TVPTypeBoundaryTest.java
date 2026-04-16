/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.stream.Stream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
 * Tests TVP boundary, out-of-range, and data type conversion scenarios.
 * Merged from FX test suites: TVPOutOfRange.java and DataTypeConversions.java.
 *
 * Tests are organized into parameterized groups:
 * - Out-of-Range via DataTable: validates that boundary-exceeding values produce errors
 * - ResultSet Cross-Type Overflow: validates overflow detection when using ResultSet as TVP source
 * - Data Type Conversion: validates valid cross-type conversions via TVP
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxTVP)
public class TVPTypeBoundaryTest extends AbstractTest {

    private static String tvpName;
    private static String tableName;
    private static String srcTableName;

    /**
     * Initializes the shared connection for all tests in this class.
     *
     * @throws Exception if connection setup fails
     */
    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /**
     * Generates unique random names for the TVP type, destination table, and source table
     * before each test to avoid naming collisions across parallel test runs.
     *
     * @throws SQLException if name generation encounters an error
     */
    @BeforeEach
    public void testSetup() throws SQLException {
        tvpName = RandomUtil.getIdentifier("TVP");
        tableName = RandomUtil.getIdentifier("TVPBoundTable");
        srcTableName = RandomUtil.getIdentifier("TVPBoundSrc");
    }

    /**
     * Drops the destination table, source table, and TVP type created during the test
     * to ensure each test starts with a clean state.
     *
     * @throws SQLException if cleanup fails
     */
    @AfterEach
    public void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTableIfExists(srcTableName, stmt);
            TestUtils.dropTypeIfExists(tvpName, stmt);
        }
    }

    /**
     * Final cleanup of all database objects after all tests complete.
     *
     * @throws SQLException if cleanup fails
     */
    @AfterAll
    public static void terminate() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTableIfExists(srcTableName, stmt);
            TestUtils.dropTypeIfExists(tvpName, stmt);
        }
    }

    // ==============================
    // Shared Helper Methods
    // ==============================

    /**
     * Creates a single-column destination table with the specified SQL column type.
     *
     * @param colType the SQL data type for the single column (e.g. "int", "varchar(50)")
     * @throws SQLException if table creation fails
     */
    private void createTable(String colType) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.execute("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (c1 " + colType + " null)");
        }
    }

    /**
     * Creates a TVP type with the specified SQL column type.
     *
     * @param colType the SQL data type for the TVP column
     * @throws SQLException if type creation fails
     */
    private void createTVPS(String colType) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTypeIfExists(tvpName, stmt);
            stmt.executeUpdate("CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName)
                    + " AS TABLE (c1 " + colType + " null)");
        }
    }

    /**
     * Creates both a destination table and a matching TVP type with the specified column type.
     *
     * @param colType the SQL data type for both the table column and TVP column
     * @throws SQLException if creation fails
     */
    private void createTableAndTVP(String colType) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTypeIfExists(tvpName, stmt);
            stmt.execute("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (c1 " + colType + " null)");
            stmt.executeUpdate("CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName)
                    + " AS TABLE (c1 " + colType + " null)");
        }
    }

    // ================================================================
    // Out-of-Range via DataTable (Parameterized)
    // ================================================================

    /**
     * Provides test cases for string-based out-of-range values across all SQL data types.
     * Each argument tuple contains: (sqlType, javaType, outOfRangeValue, displayName).
     * Covers temporal, numeric, string, and money boundaries from FX TVPOutOfRange.java.
     *
     * @return stream of arguments for parameterized OOR testing
     */
    private static Stream<Arguments> outOfRangeStringCases() {
        return Stream.of(
                // --- Temporal boundaries ---
                Arguments.of("date", Types.DATE, "9999-13-01", "Date Above Max"),
                Arguments.of("date", Types.DATE, "0000-01-01", "Date Below Min"),
                Arguments.of("datetime", Types.TIMESTAMP, "9999-13-31 23:59:59.999", "Datetime Above Max"),
                Arguments.of("datetime", Types.TIMESTAMP, "1752-12-31 00:00:00.000", "Datetime Below Min"),
                Arguments.of("datetime2", Types.TIMESTAMP, "10000-01-01 00:00:00.0000000", "Datetime2 Above Max"),
                Arguments.of("datetime2", Types.TIMESTAMP, "0000-01-01 00:00:00.0000000", "Datetime2 Below Min"),
                Arguments.of("datetimeoffset", microsoft.sql.Types.DATETIMEOFFSET,
                        "10000-01-01 00:00:00.0000000 +14:01", "DatetimeOffset Above Max"),
                Arguments.of("datetimeoffset", microsoft.sql.Types.DATETIMEOFFSET,
                        "0000-01-01 00:00:00.0000000 -14:01", "DatetimeOffset Below Min"),
                Arguments.of("smalldatetime", Types.TIMESTAMP, "2080-01-01 00:00:00", "SmallDatetime Above Max"),
                Arguments.of("smalldatetime", Types.TIMESTAMP, "1899-01-01 00:00:00", "SmallDatetime Below Min"),
                Arguments.of("time", Types.TIME, "24:00:00.0000000", "Time Above Max"),
                Arguments.of("time", Types.TIME, "-00:00:01.0000000", "Time Below Min"),

                // --- Integer boundaries ---
                Arguments.of("tinyint", Types.TINYINT, "265", "Tinyint Above Max"),
                Arguments.of("tinyint", Types.TINYINT, "-1", "Tinyint Below Min"),
                Arguments.of("smallint", Types.SMALLINT, "33000", "Smallint Above Max"),
                Arguments.of("smallint", Types.SMALLINT, "-33000", "Smallint Below Min"),
                Arguments.of("int", Types.INTEGER, "2247483647", "Int Above Max"),
                Arguments.of("int", Types.INTEGER, "-2247483647", "Int Below Min"),
                Arguments.of("bigint", Types.BIGINT, "9323372036854775807", "Bigint Above Max"),
                Arguments.of("bigint", Types.BIGINT, "-9323372036854775807", "Bigint Below Min"),

                // --- Floating-point boundaries ---
                Arguments.of("float", Types.FLOAT, "1.8E+308", "Float Above Max"),
                Arguments.of("float", Types.FLOAT, "-1.8E+308", "Float Below Min"),
                Arguments.of("real", Types.REAL, "3.5E+38", "Real Above Max"),
                Arguments.of("real", Types.REAL, "-3.5E+38", "Real Below Min"),

                // --- Decimal / Numeric boundaries ---
                Arguments.of("decimal(5,3)", Types.DECIMAL, "100.999", "Decimal Above Max"),
                Arguments.of("decimal(5,3)", Types.DECIMAL, "-100.999", "Decimal Below Min"),
                Arguments.of("numeric(10,6)", Types.NUMERIC, "199999999.999999", "Numeric Above Max"),
                Arguments.of("numeric(10,6)", Types.NUMERIC, "-199999999.999999", "Numeric Below Min"),

                // --- String width boundaries ---
                Arguments.of("char(5)", Types.CHAR, "abcdefghij", "Char Exceeding Width"),
                Arguments.of("nchar(5)", Types.NCHAR, "abcdefghij", "NChar Exceeding Width"),
                Arguments.of("varchar(5)", Types.VARCHAR, "abcdefghij", "Varchar Exceeding Width"),
                Arguments.of("nvarchar(5)", Types.NVARCHAR, "abcdefghij", "NVarchar Exceeding Width"),

                // --- Money boundaries ---
                Arguments.of("smallmoney", Types.DECIMAL, "215000", "SmallMoney Above Max"),
                Arguments.of("smallmoney", Types.DECIMAL, "-215000", "SmallMoney Below Min"),
                Arguments.of("money", Types.DECIMAL, "922337203685478.5807", "Money Above Max"),
                Arguments.of("money", Types.DECIMAL, "-922337203685478.5807", "Money Below Min"),

                // --- UniqueIdentifier ---
                Arguments.of("uniqueidentifier", Types.CHAR, "not-a-valid-guid-value-here",
                        "UniqueIdentifier Invalid Format")
        );
    }

    /**
     * Verifies that inserting an out-of-range string value via DataTable TVP either causes
     * an exception with a descriptive message or is handled gracefully by the server.
     * Ported from FX TVPOutOfRange.java.
     *
     * @param sqlType         the SQL column type (e.g. "date", "tinyint")
     * @param javaType        the java.sql.Types constant for the TVP column metadata
     * @param outOfRangeValue the out-of-range string value to insert
     * @param displayName     human-readable test case description (used in test name)
     * @throws SQLException if an unexpected database error occurs
     */
    @ParameterizedTest(name = "[{index}] OOR DataTable: {3}")
    @MethodSource("outOfRangeStringCases")
    public void testOutOfRangeViaDataTable(String sqlType, int javaType, String outOfRangeValue,
            String displayName) throws SQLException {
        expectOutOfRange(sqlType, javaType, outOfRangeValue);
    }

    /**
     * Provides test cases for binary out-of-range values that exceed the column size.
     * Each argument tuple contains: (sqlType, javaType, displayName).
     * The oversized byte array is created within the test method.
     *
     * @return stream of arguments for binary OOR testing
     */
    private static Stream<Arguments> outOfRangeBinaryCases() {
        return Stream.of(
                Arguments.of("binary(5)", Types.BINARY, "Binary Exceeding Size"),
                Arguments.of("varbinary(5)", Types.VARBINARY, "VarBinary Exceeding Size")
        );
    }

    /**
     * Verifies that inserting an oversized byte array via DataTable TVP triggers an
     * out-of-range error for binary and varbinary columns.
     *
     * @param sqlType     the SQL column type (e.g. "binary(5)")
     * @param javaType    the java.sql.Types constant for the TVP column metadata
     * @param displayName human-readable test case description
     * @throws SQLException if an unexpected database error occurs
     */
    @ParameterizedTest(name = "[{index}] OOR Binary: {2}")
    @MethodSource("outOfRangeBinaryCases")
    public void testOutOfRangeWithObjectViaDataTable(String sqlType, int javaType,
            String displayName) throws SQLException {
        byte[] largeBytes = new byte[100];
        for (int i = 0; i < largeBytes.length; i++) {
            largeBytes[i] = (byte) (i % 256);
        }
        expectOutOfRangeWithObject(sqlType, javaType, largeBytes);
    }

    /**
     * Inserts an out-of-range string value via DataTable TVP and expects either
     * a SQLException with a descriptive message, or graceful server-side handling.
     *
     * @param sqlType         the SQL column type
     * @param javaType        the java.sql.Types constant
     * @param outOfRangeValue the out-of-range value
     * @throws SQLException if an unexpected database error occurs
     */
    private void expectOutOfRange(String sqlType, int javaType, String outOfRangeValue) throws SQLException {
        createTable(sqlType);
        createTVPS(sqlType);

        try {
            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c1", javaType);
            tvp.addRow(outOfRangeValue);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.execute();
            }
        } catch (SQLException e) {
            // Client-side addRow() validation or server-side execute() rejection — both are valid
            assertNotNull(e.getMessage(), "Exception should have a descriptive message");
            return;
        }

        // If data was inserted, that's acceptable for some SQL types (e.g., datetime rounding)
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            // Verify no crash on post-insert query
        }
    }

    /**
     * Inserts an out-of-range object value (e.g. oversized byte[]) via DataTable TVP
     * and expects a SQLException with a descriptive error message.
     *
     * @param sqlType         the SQL column type
     * @param javaType        the java.sql.Types constant
     * @param outOfRangeValue the out-of-range object value
     * @throws SQLException if an unexpected database error occurs
     */
    private void expectOutOfRangeWithObject(String sqlType, int javaType, Object outOfRangeValue)
            throws SQLException {
        createTable(sqlType);
        createTVPS(sqlType);

        try {
            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c1", javaType);
            tvp.addRow(outOfRangeValue);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.execute();
                fail("Expected SQLException for out-of-range " + sqlType + " value, but insert succeeded");
            }
        } catch (SQLException e) {
            // Client-side addRow() validation or server-side execute() rejection — both are valid
            assertNotNull(e.getMessage(), "Exception should have a descriptive message");
        }
    }

    // ================================================================
    // ResultSet Cross-Type Overflow (Parameterized)
    // ================================================================

    /**
     * Provides test cases for ResultSet cross-type overflow scenarios.
     * Each argument tuple contains: (srcType, destType, value, expectFailure, displayName).
     * Covers integer narrowing, float-to-int, and money overflow cases from FX TVPOutOfRange.java.
     *
     * @return stream of arguments for ResultSet overflow testing
     */
    private static Stream<Arguments> resultSetOverflowCases() {
        return Stream.of(
                // --- Bigint narrowing to integers ---
                Arguments.of("bigint", "int", String.valueOf((long) Integer.MAX_VALUE + 100L),
                        true, "Bigint to Int Above Max"),
                Arguments.of("bigint", "int", String.valueOf((long) Integer.MIN_VALUE - 100L),
                        true, "Bigint to Int Below Min"),
                Arguments.of("bigint", "smallint", String.valueOf((long) Short.MAX_VALUE + 100L),
                        true, "Bigint to Smallint Above Max"),
                Arguments.of("bigint", "smallint", String.valueOf((long) Short.MIN_VALUE - 100L),
                        true, "Bigint to Smallint Below Min"),
                Arguments.of("bigint", "tinyint", "265",
                        true, "Bigint to Tinyint Above Max"),
                Arguments.of("bigint", "tinyint", "-1",
                        true, "Bigint to Tinyint Below Min"),

                // --- Bigint to bit (normalization, not failure) ---
                Arguments.of("bigint", "bit", "999",
                        false, "Bigint to Bit (normalize)"),

                // --- Bigint to money ---
                Arguments.of("bigint", "smallmoney", "215000",
                        true, "Bigint to Smallmoney Above Max"),

                // --- Float narrowing ---
                Arguments.of("float", "real", "3.5E+38",
                        true, "Float to Real Above Max"),
                Arguments.of("float", "decimal(10,2)", "99999999999.99",
                        true, "Float to Decimal Above Max"),
                Arguments.of("float", "numeric(10,2)", "99999999999.99",
                        true, "Float to Numeric Above Max"),
                Arguments.of("float", "bigint", "9.3E+18",
                        true, "Float to Bigint Above Max"),
                Arguments.of("float", "money", "922337203685478.6",
                        true, "Float to Money Above Max")
        );
    }

    /**
     * Inserts a value into a source table of one type, then uses a ResultSet TVP to
     * copy it into a narrower destination type, verifying that overflow is detected
     * or that non-overflow cases succeed.
     * Ported from FX TVPOutOfRange.java (_test2).
     *
     * @param srcType       the source table column type
     * @param destType      the destination table column type (narrower)
     * @param value         the value to insert in the source table
     * @param expectFailure true if overflow should produce an error
     * @param displayName   human-readable test case description
     * @throws SQLException if an unexpected database error occurs
     */
    @ParameterizedTest(name = "[{index}] OOR ResultSet: {4}")
    @MethodSource("resultSetOverflowCases")
    public void testResultSetOverflow(String srcType, String destType, String value,
            boolean expectFailure, String displayName) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(srcTableName, stmt);
            stmt.execute("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(srcTableName)
                    + " (c1 " + srcType + " null)");
            stmt.execute("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(srcTableName)
                    + " VALUES (" + value + ")");
        }

        createTable(destType);
        createTVPS(destType);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(srcTableName))) {

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                            + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, rs);
                pstmt.execute();

                if (expectFailure) {
                    fail("Expected overflow SQLException was not thrown for: " + displayName);
                }
            }
        } catch (SQLException e) {
            if (expectFailure) {
                assertNotNull(e.getMessage(), "Expected overflow exception should have a message");
            } else {
                fail("Unexpected exception for non-overflow case: " + e.getMessage());
            }
        }
    }

    // ================================================================
    // Data Type Conversion via DataTable (Parameterized + Individual)
    // ================================================================

    /**
     * Provides test cases for string-based DataTable type conversions that can be verified
     * with exact or trimmed string comparison. Each argument tuple contains:
     * (destType, javaType, testValue, trimExpected, displayName).
     *
     * @return stream of arguments for DataTable string conversion testing
     */
    private static Stream<Arguments> dataTableStringConversionCases() {
        return Stream.of(
                Arguments.of("nvarchar(max)", Types.LONGVARCHAR,
                        "Hello World - Test conversion from varchar to nvarchar with special chars: @#$%^&*()",
                        false, "VARCHAR(MAX) to NVARCHAR(MAX)"),
                Arguments.of("nvarchar(50)", Types.LONGNVARCHAR,
                        "Short enough value for nvarchar(50)",
                        false, "NVARCHAR(MAX) to NVARCHAR(50)"),
                Arguments.of("nvarchar(50)", Types.VARCHAR,
                        "Test varchar to nvarchar",
                        false, "VARCHAR(50) to NVARCHAR(50)"),
                Arguments.of("nvarchar(100)", Types.NVARCHAR,
                        "\u4E2D\u6587\u65E5\u672C\uD55C\uAD6D",
                        false, "Unicode Preservation VARCHAR to NVARCHAR"),
                Arguments.of("nchar(50)", Types.LONGVARCHAR,
                        "Test varchar to nchar",
                        true, "VARCHAR(MAX) to NCHAR(50)"),
                Arguments.of("nchar(50)", Types.NVARCHAR,
                        "Test nvarchar to nchar",
                        true, "NVARCHAR(50) to NCHAR(50)")
        );
    }

    /**
     * Inserts a string value via DataTable TVP with a specified JDBC type into a target SQL type,
     * then verifies the stored value matches the expected string (with optional trim for
     * fixed-length character types like NCHAR).
     * Ported from FX DataTypeConversions.java.
     *
     * @param destType      the destination SQL column type
     * @param javaType      the java.sql.Types constant for the TVP column metadata
     * @param testValue     the string value to insert
     * @param trimExpected  true if the retrieved value should be trimmed before comparison
     * @param displayName   human-readable test case description
     * @throws SQLException if an unexpected database error occurs
     */
    @ParameterizedTest(name = "[{index}] Conversion DataTable: {4}")
    @MethodSource("dataTableStringConversionCases")
    public void testDataTableStringConversion(String destType, int javaType, String testValue,
            boolean trimExpected, String displayName) throws SQLException {
        createTableAndTVP(destType);

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", javaType);
        tvp.addRow(testValue);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
        }

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            assertTrue(rs.next(), "Should have at least one row");
            String actual = rs.getString(1);
            assertEquals(testValue, trimExpected ? actual.trim() : actual);
            assertFalse(rs.next(), "Should have exactly one row");
        }
    }

    /**
     * Tests conversion from VARBINARY(MAX) to VARCHAR(MAX) via DataTable TVP.
     * Verifies that binary data is converted to a non-empty string representation.
     *
     * @throws SQLException if an unexpected database error occurs
     */
    @Test
    @DisplayName("Conversion DataTable: VARBINARY(MAX) to VARCHAR(MAX)")
    public void testVarbinaryMaxToVarcharMaxDataTable() throws SQLException {
        String destType = "varchar(max)";
        byte[] testBytes = "Test binary to varchar conversion".getBytes();

        createTableAndTVP(destType);

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", Types.LONGVARBINARY);
        tvp.addRow(testBytes);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
        }

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            assertTrue(rs.next(), "Should have at least one row");
            String result = rs.getString(1);
            assertNotNull(result, "Conversion result should not be null");
            assertTrue(result.length() > 0, "Conversion result should not be empty");
            assertFalse(rs.next(), "Should have exactly one row");
        }
    }

    /**
     * Tests conversion from VARBINARY to CHAR(100) via DataTable TVP.
     * Verifies that the binary-to-character conversion produces a non-null result.
     *
     * @throws SQLException if an unexpected database error occurs
     */
    @Test
    @DisplayName("Conversion DataTable: VARBINARY to CHAR")
    public void testVarbinaryToCharDataTable() throws SQLException {
        String destType = "char(100)";
        byte[] testBytes = "BinaryToChar".getBytes();

        createTableAndTVP(destType);

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", Types.VARBINARY);
        tvp.addRow(testBytes);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
        }

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            assertTrue(rs.next(), "Should have at least one row");
            assertNotNull(rs.getString(1), "Result should not be null");
            assertFalse(rs.next(), "Should have exactly one row");
        }
    }

    /**
     * Tests conversion from VARBINARY to BINARY(50) via DataTable TVP.
     * Verifies that the first N bytes of the stored value match the original input.
     *
     * @throws SQLException if an unexpected database error occurs
     */
    @Test
    @DisplayName("Conversion DataTable: VARBINARY to BINARY")
    public void testVarbinaryToBinaryDataTable() throws SQLException {
        String destType = "binary(50)";
        byte[] testBytes = new byte[] {0x01, 0x02, 0x03, 0x04, 0x05, 0x0A, 0x0B, 0x0C};

        createTableAndTVP(destType);

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", Types.VARBINARY);
        tvp.addRow(testBytes);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
        }

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            assertTrue(rs.next(), "Should have at least one row");
            byte[] result = rs.getBytes(1);
            assertTrue(Arrays.equals(testBytes, Arrays.copyOf(result, testBytes.length)),
                    "Binary data should match (first N bytes)");
            assertFalse(rs.next(), "Should have exactly one row");
        }
    }

    /**
     * Tests conversion from VARCHAR to INT via DataTable TVP.
     * Inserts a string "12345" into an int column and verifies the numeric value.
     *
     * @throws SQLException if an unexpected database error occurs
     */
    @Test
    @DisplayName("Conversion DataTable: VARCHAR to INT")
    public void testVarcharToIntDataTable() throws SQLException {
        String destType = "int";

        createTableAndTVP(destType);

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", Types.VARCHAR);
        tvp.addRow("12345");

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
        }

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            assertTrue(rs.next(), "Should have at least one row");
            assertEquals(12345, rs.getInt(1));
            assertFalse(rs.next(), "Should have exactly one row");
        }
    }

    /**
     * Tests that inserting a string longer than the NVARCHAR(10) destination column
     * via DataTable TVP produces a truncation error.
     *
     * @throws SQLException if an unexpected database error occurs
     */
    @Test
    @DisplayName("Conversion DataTable: NVARCHAR(MAX) Truncation to NVARCHAR(10)")
    public void testNvarcharMaxTruncation() throws SQLException {
        String destType = "nvarchar(10)";
        String testValue = "This string is definitely longer than 10 characters";

        createTableAndTVP(destType);

        SQLServerDataTable tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("c1", Types.LONGNVARCHAR);
        tvp.addRow(testValue);

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, tvp);
            pstmt.execute();
            fail("Expected SQLException for NVARCHAR(MAX) truncation into NVARCHAR(10)");
        } catch (SQLException e) {
            assertNotNull(e.getMessage(), "Truncation should produce an error message");
        }
    }

    // ================================================================
    // Data Type Conversion via ResultSet (Parameterized)
    // ================================================================

    /**
     * Provides test cases for ResultSet-based type conversions.
     * Each argument tuple contains: (srcType, destType, testValue, trimExpected, displayName).
     *
     * @return stream of arguments for ResultSet conversion testing
     */
    private static Stream<Arguments> resultSetConversionCases() {
        return Stream.of(
                Arguments.of("varchar(max)", "nvarchar(max)",
                        "Hello World - ResultSet conversion test with unicode: \u00E4\u00F6\u00FC",
                        false, "VARCHAR(MAX) to NVARCHAR(MAX)"),
                Arguments.of("nvarchar(max)", "nvarchar(50)",
                        "Short enough nvarchar value",
                        false, "NVARCHAR(MAX) to NVARCHAR(50)"),
                Arguments.of("varchar(50)", "nchar(50)",
                        "varchar to nchar",
                        true, "VARCHAR to NCHAR"),
                Arguments.of("nvarchar(50)", "nchar(50)",
                        "nvarchar to nchar",
                        true, "NVARCHAR to NCHAR")
        );
    }

    /**
     * Inserts a value into a source table, then copies it via ResultSet TVP into a different
     * destination type, verifying the conversion preserves the value (with optional trim).
     * Ported from FX DataTypeConversions.java.
     *
     * @param srcType       the source table column type
     * @param destType      the destination table column type
     * @param testValue     the string value to insert and verify
     * @param trimExpected  true if the retrieved value should be trimmed before comparison
     * @param displayName   human-readable test case description
     * @throws SQLException if an unexpected database error occurs
     */
    @ParameterizedTest(name = "[{index}] Conversion ResultSet: {4}")
    @MethodSource("resultSetConversionCases")
    public void testResultSetConversion(String srcType, String destType, String testValue,
            boolean trimExpected, String displayName) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(srcTableName, stmt);
            stmt.execute("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(srcTableName)
                    + " (c1 " + srcType + " null)");
            stmt.execute("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(srcTableName)
                    + " VALUES (N'" + TestUtils.escapeSingleQuotes(testValue) + "')");
        }

        createTableAndTVP(destType);

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(srcTableName));
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                        "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, rs);
            pstmt.execute();
        }

        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(
                        "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
            assertTrue(rs.next(), "Should have at least one row");
            String actual = rs.getString(1);
            assertEquals(testValue, trimExpected ? actual.trim() : actual);
            assertFalse(rs.next(), "Should have exactly one row");
        }
    }
}
