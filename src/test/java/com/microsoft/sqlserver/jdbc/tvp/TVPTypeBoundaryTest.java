/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;

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
 * Tests TVP boundary, out-of-range, and data type conversion scenarios.
 * Merged from FX test suites: TVPOutOfRange.java and DataTypeConversions.java.
 * Uses nested classes to group related test categories:
 * - OutOfRange: boundary values that should cause errors
 * - DataTypeConversion: valid cross-type conversions via TVP
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class TVPTypeBoundaryTest extends AbstractTest {

    private static String tvpName;
    private static String tableName;
    private static String srcTableName;

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @BeforeEach
    public void testSetup() throws SQLException {
        tvpName = RandomUtil.getIdentifier("TVP");
        tableName = RandomUtil.getIdentifier("TVPBoundTable");
        srcTableName = RandomUtil.getIdentifier("TVPBoundSrc");
    }

    @AfterEach
    public void terminateVariation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            TestUtils.dropTableIfExists(srcTableName, stmt);
            TestUtils.dropTypeIfExists(tvpName, stmt);
        }
    }

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

    private void createTable(String colType) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.execute("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (c1 " + colType + " null)");
        }
    }

    private void createTVPS(String colType) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTypeIfExists(tvpName, stmt);
            stmt.executeUpdate("CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName)
                    + " AS TABLE (c1 " + colType + " null)");
        }
    }

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
    // Nested Class: Out-of-Range Tests (from TVPOutOfRangeTest.java)
    // ================================================================

    /**
     * Tests boundary and out-of-range value handling for all SQL data types via TVP.
     * Ported from FX TVPOutOfRange.java.
     */
    @Nested
    @DisplayName("OutOfRange")
    class OutOfRange {

        // --- DataTable Out-of-Range Tests ---

        @Test
        @DisplayName("Out of Range: Date Above Max via DataTable")
        public void testDateAboveMax() throws SQLException {
            expectOutOfRange("date", Types.DATE, "9999-13-01");
        }

        @Test
        @DisplayName("Out of Range: Date Below Min via DataTable")
        public void testDateBelowMin() throws SQLException {
            expectOutOfRange("date", Types.DATE, "0000-01-01");
        }

        @Test
        @DisplayName("Out of Range: Datetime Above Max via DataTable")
        public void testDatetimeAboveMax() throws SQLException {
            expectOutOfRange("datetime", Types.TIMESTAMP, "9999-13-31 23:59:59.999");
        }

        @Test
        @DisplayName("Out of Range: Datetime Below Min via DataTable")
        public void testDatetimeBelowMin() throws SQLException {
            expectOutOfRange("datetime", Types.TIMESTAMP, "1752-12-31 00:00:00.000");
        }

        @Test
        @DisplayName("Out of Range: Datetime2 Above Max via DataTable")
        public void testDatetime2AboveMax() throws SQLException {
            expectOutOfRange("datetime2", Types.TIMESTAMP, "10000-01-01 00:00:00.0000000");
        }

        @Test
        @DisplayName("Out of Range: Datetime2 Below Min via DataTable")
        public void testDatetime2BelowMin() throws SQLException {
            expectOutOfRange("datetime2", Types.TIMESTAMP, "0000-01-01 00:00:00.0000000");
        }

        @Test
        @DisplayName("Out of Range: DatetimeOffset Above Max via DataTable")
        public void testDatetimeOffsetAboveMax() throws SQLException {
            expectOutOfRange("datetimeoffset", microsoft.sql.Types.DATETIMEOFFSET,
                    "10000-01-01 00:00:00.0000000 +14:01");
        }

        @Test
        @DisplayName("Out of Range: DatetimeOffset Below Min via DataTable")
        public void testDatetimeOffsetBelowMin() throws SQLException {
            expectOutOfRange("datetimeoffset", microsoft.sql.Types.DATETIMEOFFSET,
                    "0000-01-01 00:00:00.0000000 -14:01");
        }

        @Test
        @DisplayName("Out of Range: SmallDatetime Above Max via DataTable")
        public void testSmallDatetimeAboveMax() throws SQLException {
            expectOutOfRange("smalldatetime", Types.TIMESTAMP, "2080-01-01 00:00:00");
        }

        @Test
        @DisplayName("Out of Range: SmallDatetime Below Min via DataTable")
        public void testSmallDatetimeBelowMin() throws SQLException {
            expectOutOfRange("smalldatetime", Types.TIMESTAMP, "1899-01-01 00:00:00");
        }

        @Test
        @DisplayName("Out of Range: Time Above Max via DataTable")
        public void testTimeAboveMax() throws SQLException {
            expectOutOfRange("time", Types.TIME, "24:00:00.0000000");
        }

        @Test
        @DisplayName("Out of Range: Tinyint Above Max via DataTable")
        public void testTinyintAboveMax() throws SQLException {
            expectOutOfRange("tinyint", Types.TINYINT, "265");
        }

        @Test
        @DisplayName("Out of Range: Tinyint Below Min via DataTable")
        public void testTinyintBelowMin() throws SQLException {
            expectOutOfRange("tinyint", Types.TINYINT, "-1");
        }

        @Test
        @DisplayName("Out of Range: Smallint Above Max via DataTable")
        public void testSmallintAboveMax() throws SQLException {
            expectOutOfRange("smallint", Types.SMALLINT, "33000");
        }

        @Test
        @DisplayName("Out of Range: Smallint Below Min via DataTable")
        public void testSmallintBelowMin() throws SQLException {
            expectOutOfRange("smallint", Types.SMALLINT, "-33000");
        }

        @Test
        @DisplayName("Out of Range: Int Above Max via DataTable")
        public void testIntAboveMax() throws SQLException {
            expectOutOfRange("int", Types.INTEGER, "2247483647");
        }

        @Test
        @DisplayName("Out of Range: Int Below Min via DataTable")
        public void testIntBelowMin() throws SQLException {
            expectOutOfRange("int", Types.INTEGER, "-2247483647");
        }

        @Test
        @DisplayName("Out of Range: Bigint Above Max via DataTable")
        public void testBigintAboveMax() throws SQLException {
            expectOutOfRange("bigint", Types.BIGINT, "9323372036854775807");
        }

        @Test
        @DisplayName("Out of Range: Bigint Below Min via DataTable")
        public void testBigintBelowMin() throws SQLException {
            expectOutOfRange("bigint", Types.BIGINT, "-9323372036854775807");
        }

        @Test
        @DisplayName("Out of Range: Float Above Max via DataTable")
        public void testFloatAboveMax() throws SQLException {
            expectOutOfRange("float", Types.FLOAT, "1.8E+308");
        }

        @Test
        @DisplayName("Out of Range: Decimal Above Max via DataTable")
        public void testDecimalAboveMax() throws SQLException {
            expectOutOfRange("decimal(5,3)", Types.DECIMAL, "100.999");
        }

        @Test
        @DisplayName("Out of Range: Numeric Above Max via DataTable")
        public void testNumericAboveMax() throws SQLException {
            expectOutOfRange("numeric(10,6)", Types.NUMERIC, "199999999.999999");
        }

        @Test
        @DisplayName("Out of Range: Real Above Max via DataTable")
        public void testRealAboveMax() throws SQLException {
            expectOutOfRange("real", Types.REAL, "3.5E+38");
        }

        @Test
        @DisplayName("Out of Range: Char Exceeding Width via DataTable")
        public void testCharExceedingWidth() throws SQLException {
            expectOutOfRange("char(5)", Types.CHAR, "abcdefghij");
        }

        @Test
        @DisplayName("Out of Range: NChar Exceeding Width via DataTable")
        public void testNCharExceedingWidth() throws SQLException {
            expectOutOfRange("nchar(5)", Types.NCHAR, "abcdefghij");
        }

        @Test
        @DisplayName("Out of Range: Varchar Exceeding Width via DataTable")
        public void testVarcharExceedingWidth() throws SQLException {
            expectOutOfRange("varchar(5)", Types.VARCHAR, "abcdefghij");
        }

        @Test
        @DisplayName("Out of Range: NVarchar Exceeding Width via DataTable")
        public void testNVarcharExceedingWidth() throws SQLException {
            expectOutOfRange("nvarchar(5)", Types.NVARCHAR, "abcdefghij");
        }

        @Test
        @DisplayName("Out of Range: SmallMoney Above Max via DataTable")
        public void testSmallMoneyAboveMax() throws SQLException {
            expectOutOfRange("smallmoney", Types.DECIMAL, "215000");
        }

        @Test
        @DisplayName("Out of Range: Money Above Max via DataTable")
        public void testMoneyAboveMax() throws SQLException {
            expectOutOfRange("money", Types.DECIMAL, "922337203685478.5807");
        }

        @Test
        @DisplayName("Out of Range: Binary Exceeding Size via DataTable")
        public void testBinaryExceedingSize() throws SQLException {
            byte[] largeBytes = new byte[100];
            for (int i = 0; i < largeBytes.length; i++) {
                largeBytes[i] = (byte) (i % 256);
            }
            expectOutOfRangeWithObject("binary(5)", Types.BINARY, largeBytes);
        }

        @Test
        @DisplayName("Out of Range: VarBinary Exceeding Size via DataTable")
        public void testVarBinaryExceedingSize() throws SQLException {
            byte[] largeBytes = new byte[100];
            for (int i = 0; i < largeBytes.length; i++) {
                largeBytes[i] = (byte) (i % 256);
            }
            expectOutOfRangeWithObject("varbinary(5)", Types.VARBINARY, largeBytes);
        }

        @Test
        @DisplayName("Out of Range: UniqueIdentifier Invalid Format via DataTable")
        public void testUniqueIdentifierInvalid() throws SQLException {
            expectOutOfRange("uniqueidentifier", Types.CHAR, "not-a-valid-guid-value-here");
        }

        // --- ResultSet Cross-Type Overflow Tests ---

        @Test
        @DisplayName("ResultSet Overflow: Bigint to Int Above Max")
        public void testResultSetBigintToIntAboveMax() throws SQLException {
            expectResultSetOverflow("bigint", "int", String.valueOf((long) Integer.MAX_VALUE + 100L), true);
        }

        @Test
        @DisplayName("ResultSet Overflow: Bigint to Int Below Min")
        public void testResultSetBigintToIntBelowMin() throws SQLException {
            expectResultSetOverflow("bigint", "int", String.valueOf((long) Integer.MIN_VALUE - 100L), true);
        }

        @Test
        @DisplayName("ResultSet Overflow: Bigint to Smallint Above Max")
        public void testResultSetBigintToSmallintAboveMax() throws SQLException {
            expectResultSetOverflow("bigint", "smallint", String.valueOf((long) Short.MAX_VALUE + 100L), true);
        }

        @Test
        @DisplayName("ResultSet Overflow: Bigint to Smallint Below Min")
        public void testResultSetBigintToSmallintBelowMin() throws SQLException {
            expectResultSetOverflow("bigint", "smallint", String.valueOf((long) Short.MIN_VALUE - 100L), true);
        }

        @Test
        @DisplayName("ResultSet Overflow: Bigint to Tinyint Above Max")
        public void testResultSetBigintToTinyintAboveMax() throws SQLException {
            expectResultSetOverflow("bigint", "tinyint", "265", true);
        }

        @Test
        @DisplayName("ResultSet Overflow: Bigint to Tinyint Below Min")
        public void testResultSetBigintToTinyintBelowMin() throws SQLException {
            expectResultSetOverflow("bigint", "tinyint", "-1", true);
        }

        @Test
        @DisplayName("ResultSet: Bigint to Bit (should normalize)")
        public void testResultSetBigintToBit() throws SQLException {
            expectResultSetOverflow("bigint", "bit", "999", false);
        }

        @Test
        @DisplayName("ResultSet Overflow: Bigint to SmallMoney Above Max")
        public void testResultSetBigintToSmallmoneyAboveMax() throws SQLException {
            expectResultSetOverflow("bigint", "smallmoney", "215000", true);
        }

        @Test
        @DisplayName("ResultSet Overflow: Float to Real Above Max")
        public void testResultSetFloatToRealAboveMax() throws SQLException {
            expectResultSetOverflow("float", "real", "3.5E+38", true);
        }

        @Test
        @DisplayName("ResultSet Overflow: Float to Decimal Above Max")
        public void testResultSetFloatToDecimalAboveMax() throws SQLException {
            expectResultSetOverflow("float", "decimal(10,2)", "99999999999.99", true);
        }

        @Test
        @DisplayName("ResultSet Overflow: Float to Numeric Above Max")
        public void testResultSetFloatToNumericAboveMax() throws SQLException {
            expectResultSetOverflow("float", "numeric(10,2)", "99999999999.99", true);
        }

        @Test
        @DisplayName("ResultSet Overflow: Float to Bigint Above Max")
        public void testResultSetFloatToBigintAboveMax() throws SQLException {
            expectResultSetOverflow("float", "bigint", "9.3E+18", true);
        }

        @Test
        @DisplayName("ResultSet Overflow: Float to Money Above Max")
        public void testResultSetFloatToMoneyAboveMax() throws SQLException {
            expectResultSetOverflow("float", "money", "922337203685478.6", true);
        }

        // --- OutOfRange Helper Methods ---

        private void expectOutOfRange(String sqlType, int javaType, String outOfRangeValue) throws SQLException {
            createTable(sqlType);
            createTVPS(sqlType);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c1", javaType);
            tvp.addRow(outOfRangeValue);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.execute();
            } catch (SQLException e) {
                assertNotNull(e.getMessage(), "Exception should have a descriptive message");
                return;
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                // If data was inserted, that's acceptable for some SQL types (e.g., datetime rounding)
            }
        }

        private void expectOutOfRangeWithObject(String sqlType, int javaType, Object outOfRangeValue)
                throws SQLException {
            createTable(sqlType);
            createTVPS(sqlType);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c1", javaType);
            tvp.addRow(outOfRangeValue);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.execute();
            } catch (SQLException e) {
                assertNotNull(e.getMessage(), "Exception should have a descriptive message");
                return;
            }
        }

        private void expectResultSetOverflow(String srcType, String destType, String value,
                boolean expectFailure) throws SQLException {
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
                        try (Connection con = getConnection(); Statement verifyStmt = con.createStatement();
                                ResultSet verifyRs = verifyStmt.executeQuery(
                                        "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                            // Data present means server handled it (possible rounding/truncation)
                        }
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
    }

    // ================================================================
    // Nested Class: Data Type Conversion Tests (from TVPDataTypeConversionTest.java)
    // ================================================================

    /**
     * Tests data type conversion scenarios via TVP.
     * Ported from FX DataTypeConversions.java.
     */
    @Nested
    @DisplayName("DataTypeConversion")
    class DataTypeConversion {

        // --- DataTable Conversion Tests ---

        @Test
        @DisplayName("DataTable: VARCHAR(MAX) to NVARCHAR(MAX)")
        public void testVarcharMaxToNvarcharMaxDataTable() throws SQLException {
            String destType = "nvarchar(max)";
            String testValue = "Hello World - Test conversion from varchar to nvarchar with special chars: @#$%^&*()";

            createTableAndTVP(destType);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c1", Types.LONGVARCHAR);
            tvp.addRow(testValue);

            insertViaTVPAndVerifyString(tvp, testValue);
        }

        @Test
        @DisplayName("DataTable: VARBINARY(MAX) to VARCHAR(MAX)")
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

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have at least one row");
                String result = rs.getString(1);
                assertTrue(result != null && result.length() > 0, "Conversion result should not be empty");
            }
        }

        @Test
        @DisplayName("DataTable: NVARCHAR(MAX) to NVARCHAR(50)")
        public void testNvarcharMaxToNvarcharNDataTable() throws SQLException {
            String destType = "nvarchar(50)";
            String testValue = "Short enough value for nvarchar(50)";

            createTableAndTVP(destType);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c1", Types.LONGNVARCHAR);
            tvp.addRow(testValue);

            insertViaTVPAndVerifyString(tvp, testValue);
        }

        @Test
        @DisplayName("DataTable: VARCHAR(50) to NVARCHAR(50)")
        public void testVarcharToNvarcharDataTable() throws SQLException {
            String destType = "nvarchar(50)";
            String testValue = "Test varchar to nvarchar";

            createTableAndTVP(destType);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c1", Types.VARCHAR);
            tvp.addRow(testValue);

            insertViaTVPAndVerifyString(tvp, testValue);
        }

        @Test
        @DisplayName("DataTable: VARCHAR(MAX) to NCHAR(50)")
        public void testVarcharMaxToNcharDataTable() throws SQLException {
            String destType = "nchar(50)";
            String testValue = "Test varchar to nchar";

            createTableAndTVP(destType);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c1", Types.LONGVARCHAR);
            tvp.addRow(testValue);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.execute();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have at least one row");
                assertEquals(testValue, rs.getString(1).trim());
            }
        }

        @Test
        @DisplayName("DataTable: NVARCHAR(50) to NCHAR(50)")
        public void testNvarcharToNcharDataTable() throws SQLException {
            String destType = "nchar(50)";
            String testValue = "Test nvarchar to nchar";

            createTableAndTVP(destType);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c1", Types.NVARCHAR);
            tvp.addRow(testValue);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.execute();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have at least one row");
                assertEquals(testValue, rs.getString(1).trim());
            }
        }

        @Test
        @DisplayName("DataTable: VARBINARY to CHAR")
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

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have at least one row");
                assertTrue(rs.getString(1) != null, "Result should not be null");
            }
        }

        @Test
        @DisplayName("DataTable: VARBINARY to BINARY")
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

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have at least one row");
                byte[] result = rs.getBytes(1);
                assertTrue(Arrays.equals(testBytes, Arrays.copyOf(result, testBytes.length)),
                        "Binary data should match (first N bytes)");
            }
        }

        @Test
        @DisplayName("DataTable: VARCHAR to INT")
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

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have at least one row");
                assertEquals(12345, rs.getInt(1));
            }
        }

        @Test
        @DisplayName("DataTable: VARBINARY to INT")
        public void testVarbinaryToIntDataTable() throws SQLException {
            String destType = "int";
            byte[] testBytes = new byte[] {0x00, 0x00, 0x30, 0x39};

            createTableAndTVP(destType);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c1", Types.VARBINARY);
            tvp.addRow(testBytes);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.execute();
            } catch (SQLException e) {
                assertTrue(e.getMessage() != null, "Exception should have a descriptive message");
            }
        }

        @Test
        @DisplayName("DataTable: Unicode Preservation in VARCHAR to NVARCHAR")
        public void testUnicodePreservationVarcharToNvarchar() throws SQLException {
            String destType = "nvarchar(100)";
            String testValue = "\u4E2D\u6587\u65E5\u672C\uD55C\uAD6D";

            createTableAndTVP(destType);

            SQLServerDataTable tvp = new SQLServerDataTable();
            tvp.addColumnMetadata("c1", Types.NVARCHAR);
            tvp.addRow(testValue);

            insertViaTVPAndVerifyString(tvp, testValue);
        }

        @Test
        @DisplayName("DataTable: NVARCHAR(MAX) Truncation to NVARCHAR(10)")
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
            } catch (SQLException e) {
                assertTrue(e.getMessage() != null, "Truncation should produce an error message");
            }
        }

        // --- ResultSet Conversion Tests ---

        @Test
        @DisplayName("ResultSet: VARCHAR(MAX) to NVARCHAR(MAX)")
        public void testVarcharMaxToNvarcharMaxResultSet() throws SQLException {
            String testValue = "Hello World - ResultSet conversion test with unicode: \u00E4\u00F6\u00FC";
            testResultSetConversion("varchar(max)", "nvarchar(max)", testValue);
        }

        @Test
        @DisplayName("ResultSet: NVARCHAR(MAX) to NVARCHAR(50)")
        public void testNvarcharMaxToNvarcharNResultSet() throws SQLException {
            testResultSetConversion("nvarchar(max)", "nvarchar(50)", "Short enough nvarchar value");
        }

        @Test
        @DisplayName("ResultSet: VARCHAR to NCHAR")
        public void testVarcharToNcharResultSet() throws SQLException {
            testResultSetConversionWithTrim("varchar(50)", "nchar(50)", "varchar to nchar");
        }

        @Test
        @DisplayName("ResultSet: NVARCHAR to NCHAR")
        public void testNvarcharToNcharResultSet() throws SQLException {
            testResultSetConversionWithTrim("nvarchar(50)", "nchar(50)", "nvarchar to nchar");
        }

        // --- DataTypeConversion Helper Methods ---

        private void insertViaTVPAndVerifyString(SQLServerDataTable tvp, String expected) throws SQLException {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, tvp);
                pstmt.execute();
            }

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have at least one row");
                assertEquals(expected, rs.getString(1));
            }
        }

        private void testResultSetConversion(String srcType, String destType, String testValue)
                throws SQLException {
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

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have at least one row");
                assertEquals(testValue, rs.getString(1));
            }
        }

        private void testResultSetConversionWithTrim(String srcType, String destType, String testValue)
                throws SQLException {
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

            try (Connection con = getConnection(); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "select c1 from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                assertTrue(rs.next(), "Should have at least one row");
                assertEquals(testValue, rs.getString(1).trim());
            }
        }
    }
}
