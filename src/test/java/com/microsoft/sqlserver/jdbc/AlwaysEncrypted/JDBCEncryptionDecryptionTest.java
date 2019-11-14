/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.LinkedList;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.RandomData;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

import microsoft.sql.DateTimeOffset;


/**
 * Tests Decryption and encryption of values
 *
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xSQLv12)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.xAzureSQLDB)
public class JDBCEncryptionDecryptionTest extends AESetup {

    private boolean nullable = false;

    enum TestCase {
        NORMAL,
        SETOBJECT,
        SETOBJECT_WITH_JDBCTYPES,
        SETOBJECT_WITH_JAVATYPES,
        SETOBJECT_NULL,
        NULL
    }

    /**
     * Junit test case for char set string for string values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSpecificSetter() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekJks, charTable, values, TestCase.NORMAL, false);
            testChars(stmt, cekAkv, charTable, values, TestCase.NORMAL, false);
        }
    }

    /**
     * Junit test case for char set string for string values using windows certificate store
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSpecificSetterWindows() throws SQLException {
        org.junit.Assume.assumeTrue(isWindows);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekWin, charTable, values, TestCase.NORMAL, false);
        }
    }

    /**
     * Junit test case for char set object for string values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSetObject() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekJks, charTable, values, TestCase.SETOBJECT, false);
            testChars(stmt, cekAkv, charTable, values, TestCase.SETOBJECT, false);
        }
    }

    /**
     * Junit test case for char set object for jdbc string values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSetObjectWithJDBCTypes() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekJks, charTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, false);
            testChars(stmt, cekAkv, charTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, false);
        }
    }

    /**
     * Junit test case for char set string for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSpecificSetterNull() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = {null, null, null, null, null, null, null, null, null};

            testChars(stmt, cekJks, charTable, values, TestCase.NORMAL, false);
            testChars(stmt, cekAkv, charTable, values, TestCase.NORMAL, false);
        }
    }

    /**
     * Junit test case for char set object for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSetObjectNull() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = {null, null, null, null, null, null, null, null, null};

            testChars(stmt, cekJks, charTable, values, TestCase.SETOBJECT, false);
            testChars(stmt, cekAkv, charTable, values, TestCase.SETOBJECT, false);
        }
    }

    /**
     * Junit test case for char set null for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSetNull() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = {null, null, null, null, null, null, null, null, null};

            testChars(stmt, cekJks, charTable, values, TestCase.NULL, false);
            testChars(stmt, cekAkv, charTable, values, TestCase.NULL, false);
        }
    }

    /**
     * Junit test case for binary set binary for binary values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySpecificSetter() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(false);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.NORMAL, false);
            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.NORMAL, false);
        }
    }

    /**
     * Junit test case for binary set binary for binary values using windows certificate store
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySpecificSetterWindows() throws SQLException {
        org.junit.Assume.assumeTrue(isWindows);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(false);

            testBinaries(stmt, cekWin, binaryTable, values, TestCase.NORMAL, false);
        }
    }

    /**
     * Junit test case for binary set object for binary values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySetobject() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(false);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.SETOBJECT, false);
            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.SETOBJECT, false);
        }
    }

    /**
     * Junit test case for binary set null for binary values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySetNull() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(true);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.NULL, false);
            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.NULL, false);
        }
    }

    /**
     * Junit test case for binary set binary for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySpecificSetterNull() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(true);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.NORMAL, false);
            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.NORMAL, false);
        }
    }

    /**
     * Junit test case for binary set object for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarysetObjectNull() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(true);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.SETOBJECT_NULL, false);
            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.SETOBJECT_NULL, false);
        }
    }

    /**
     * Junit test case for binary set object for jdbc type binary values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySetObjectWithJDBCTypes() throws SQLException {

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(false);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, false);
            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, false);
        }
    }

    /**
     * Junit test case for date set date for date values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSpecificSetter() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.NORMAL, false);
            testDates(stmt, cekAkv, dateTable, values, TestCase.NORMAL, false);
        }
    }

    /**
     * Junit test case for date set date for date values using windows certificate store
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSpecificSetterWindows() throws SQLException {
        org.junit.Assume.assumeTrue(isWindows);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekWin, dateTable, values, TestCase.NORMAL, false);
        }
    }

    /**
     * Junit test case for date set object for date values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSetObject() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT, false);
            testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT, false);
        }
    }

    /**
     * Junit test case for date set object for java date values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSetObjectWithJavaType() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT_WITH_JAVATYPES, false);
            testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT_WITH_JAVATYPES, false);
        }
    }

    /**
     * Junit test case for date set object for jdbc date values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSetObjectWithJDBCType() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, false);
            testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, false);
        }
    }

    /**
     * Junit test case for date set date for min/max date values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSpecificSetterMinMaxValue() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            RandomData.returnMinMax = true;
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.NORMAL, false);
            testDates(stmt, cekAkv, dateTable, values, TestCase.NORMAL, false);
        }
    }

    /**
     * Junit test case for date set date for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSetNull() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            RandomData.returnNull = true;
            nullable = true;
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.NULL, false);
            testDates(stmt, cekAkv, dateTable, values, TestCase.NULL, false);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Junit test case for date set object for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSetObjectNull() throws SQLException {
        RandomData.returnNull = true;
        nullable = true;

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT_NULL, false);
            testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT_NULL, false);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Junit test case for numeric set numeric for numeric values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSpecificSetter() throws TestAbortedException, Exception {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {

            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL, false);
            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL, false);
        }
    }

    /**
     * Junit test case for numeric set numeric for numeric values using windows certificate store
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSpecificSetterWindows() throws TestAbortedException, Exception {
        org.junit.Assume.assumeTrue(isWindows);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {

            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekWin, numericTable, values1, values2, TestCase.NORMAL, false);
        }
    }

    /**
     * Junit test case for numeric set object for numeric values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSetObject() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.SETOBJECT, false);
            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.SETOBJECT, false);
        }
    }

    /**
     * Junit test case for numeric set object for jdbc type numeric values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSetObjectWithJDBCTypes() throws SQLException {

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.SETOBJECT_WITH_JDBCTYPES, false);
            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.SETOBJECT_WITH_JDBCTYPES, false);
        }
    }

    /**
     * Junit test case for numeric set numeric for max numeric values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSpecificSetterMaxValue() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {

            String[] values1 = {Boolean.TRUE.toString(), "255", "32767", "2147483647", "9223372036854775807",
                    "1.79E308", "1.123", "3.4E38", "999999999999999999", "12345.12345", "999999999999999999",
                    "567812.78", "214748.3647", "922337203685477.5807", "999999999999999999999999.9999",
                    "999999999999999999999999.9999"};
            String[] values2 = {Boolean.TRUE.toString(), "255", "32767", "2147483647", "9223372036854775807",
                    "1.79E308", "1.123", "3.4E38", "999999999999999999", "12345.12345", "999999999999999999",
                    "567812.78", "214748.3647", "922337203685477.5807", "999999999999999999999999.9999",
                    "999999999999999999999999.9999"};

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL, false);
            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL, false);
        }
    }

    /**
     * Junit test case for numeric set numeric for min numeric values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSpecificSetterMinValue() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values1 = {Boolean.FALSE.toString(), "0", "-32768", "-2147483648", "-9223372036854775808",
                    "-1.79E308", "1.123", "-3.4E38", "999999999999999999", "12345.12345", "999999999999999999",
                    "567812.78", "-214748.3648", "-922337203685477.5808", "999999999999999999999999.9999",
                    "999999999999999999999999.9999"};
            String[] values2 = {Boolean.FALSE.toString(), "0", "-32768", "-2147483648", "-9223372036854775808",
                    "-1.79E308", "1.123", "-3.4E38", "999999999999999999", "12345.12345", "999999999999999999",
                    "567812.78", "-214748.3648", "-922337203685477.5808", "999999999999999999999999.9999",
                    "999999999999999999999999.9999"};

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL, false);
            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL, false);
        }
    }

    /**
     * Junit test case for numeric set numeric for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSpecificSetterNull() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            nullable = true;
            RandomData.returnNull = true;
            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NULL, false);
            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NULL, false);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Junit test case for numeric set object for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSpecificSetterSetObjectNull() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            nullable = true;
            RandomData.returnNull = true;
            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NULL, false);
            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NULL, false);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Junit test case for numeric set numeric for null normalization values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericNormalization() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values1 = {Boolean.TRUE.toString(), "1", "127", "100", "100", "1.123", "1.123", "1.123",
                    "123456789123456789", "12345.12345", "987654321123456789", "567812.78", "7812.7812", "7812.7812",
                    "999999999999999999999999.9999", "999999999999999999999999.9999"};
            String[] values2 = {Boolean.TRUE.toString(), "1", "127", "100", "100", "1.123", "1.123", "1.123",
                    "123456789123456789", "12345.12345", "987654321123456789", "567812.78", "7812.7812", "7812.7812",
                    "999999999999999999999999.9999", "999999999999999999999999.9999"};

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL, false);
            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL, false);
        }
    }

    @Test
    public void testAEFMTOnly() throws SQLException {
        try (SQLServerConnection c = PrepUtil.getConnection(AETestConnectionString + ";useFmtOnly=true", AEInfo);
                Statement s = c.createStatement()) {
            dropTables(s);
            createTable(NUMERIC_TABLE_AE, cekJks, numericTable);
            String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(NUMERIC_TABLE_AE) + " values( "
                    + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
                    + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";
            try (PreparedStatement p = c.prepareStatement(sql)) {
                ParameterMetaData pmd = p.getParameterMetaData();
                assertTrue(pmd.getParameterCount() == 48);
            }
        }
    }

    void testChar(SQLServerStatement stmt, String[] values) throws SQLException {
        String sql = "select * from " + AbstractSQLGenerator.escapeIdentifier(CHAR_TABLE_AE);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {
            try (ResultSet rs = (stmt == null) ? pstmt.executeQuery() : stmt.executeQuery(sql)) {
                int numberOfColumns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    testGetString(rs, numberOfColumns, values);
                    testGetObject(rs, numberOfColumns, values);
                }
            }
        }

    }

    void testBinary(SQLServerStatement stmt, LinkedList<byte[]> values) throws SQLException {
        String sql = "select * from " + AbstractSQLGenerator.escapeIdentifier(BINARY_TABLE_AE.toString());

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {
            try (ResultSet rs = (stmt == null) ? pstmt.executeQuery() : stmt.executeQuery(sql)) {
                int numberOfColumns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    testGetStringForBinary(rs, numberOfColumns, values);
                    testGetBytes(rs, numberOfColumns, values);
                    testGetObjectForBinary(rs, numberOfColumns, values);
                }
            }
        }
    }

    void testDate(SQLServerStatement stmt, LinkedList<Object> values1) throws SQLException {
        String sql = "select * from " + AbstractSQLGenerator.escapeIdentifier(DATE_TABLE_AE);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {
            try (ResultSet rs = (stmt == null) ? pstmt.executeQuery() : stmt.executeQuery(sql)) {
                int numberOfColumns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    // testGetStringForDate(rs, numberOfColumns, values1); //TODO: Disabling, since getString throws
                    // verification error for zero temporal
                    // types
                    testGetObjectForTemporal(rs, numberOfColumns, values1);
                    testGetDate(rs, numberOfColumns, values1);
                }
            }
        }
    }

    void testGetObject(ResultSet rs, int numberOfColumns, String[] values) throws SQLException {
        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            try {
                String objectValue1 = ("" + rs.getObject(i)).trim();
                String objectValue2 = ("" + rs.getObject(i + 1)).trim();
                String objectValue3 = ("" + rs.getObject(i + 2)).trim();

                boolean matches = objectValue1.equalsIgnoreCase("" + values[index])
                        && objectValue2.equalsIgnoreCase("" + values[index])
                        && objectValue3.equalsIgnoreCase("" + values[index]);

                if (("" + values[index]).length() >= 1000) {
                    assertTrue(matches,
                            TestResource.getResource("R_decryptionFailed") + "getObject(): " + i + ", " + (i + 1) + ", "
                                    + (i + 2) + ".\n" + TestResource.getResource("R_expectedValueAtIndex") + index);
                } else {
                    assertTrue(matches,
                            TestResource.getResource("R_decryptionFailed") + "getObject(): " + objectValue1 + ", "
                                    + objectValue2 + ", " + objectValue3 + ".\n"
                                    + TestResource.getResource("R_expectedValue") + values[index]);
                }
            } finally {
                index++;
            }
        }
    }

    void testGetObjectForTemporal(ResultSet rs, int numberOfColumns, LinkedList<Object> values) throws SQLException {
        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            try {
                String objectValue1 = ("" + rs.getObject(i)).trim();
                String objectValue2 = ("" + rs.getObject(i + 1)).trim();
                String objectValue3 = ("" + rs.getObject(i + 2)).trim();

                Object expected = null;
                if (rs.getMetaData().getColumnTypeName(i).equalsIgnoreCase("smalldatetime")) {
                    expected = TestUtils.roundSmallDateTimeValue(values.get(index));
                } else if (rs.getMetaData().getColumnTypeName(i).equalsIgnoreCase("datetime")) {
                    expected = TestUtils.roundDatetimeValue(values.get(index));
                } else {
                    expected = values.get(index);
                }
                assertTrue(
                        objectValue1.equalsIgnoreCase("" + expected) && objectValue2.equalsIgnoreCase("" + expected)
                                && objectValue3.equalsIgnoreCase("" + expected),
                        TestResource.getResource("R_decryptionFailed") + "getObject(): " + objectValue1 + ", "
                                + objectValue2 + ", " + objectValue3 + ".\n"
                                + TestResource.getResource("R_expectedValue") + expected);
            } finally {
                index++;
            }
        }
    }

    void testGetObjectForBinary(ResultSet rs, int numberOfColumns, LinkedList<byte[]> values) throws SQLException {
        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            byte[] objectValue1 = (byte[]) rs.getObject(i);
            byte[] objectValue2 = (byte[]) rs.getObject(i + 1);
            byte[] objectValue3 = (byte[]) rs.getObject(i + 2);

            byte[] expectedBytes = null;

            if (null != values.get(index)) {
                expectedBytes = values.get(index);
            }

            try {
                if (null != values.get(index)) {
                    for (int j = 0; j < expectedBytes.length; j++) {
                        assertTrue(
                                expectedBytes[j] == objectValue1[j] && expectedBytes[j] == objectValue2[j]
                                        && expectedBytes[j] == objectValue3[j],
                                TestResource.getResource("R_decryptionFailed") + "getObject(): " + objectValue1 + ", "
                                        + objectValue2 + ", " + objectValue3 + ".\n");
                    }
                }
            } finally {
                index++;
            }
        }
    }

    void testGetBigDecimal(ResultSet rs, int numberOfColumns, String[] values) throws SQLException {

        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {

            String decimalValue1 = "" + rs.getBigDecimal(i);
            String decimalValue2 = "" + rs.getBigDecimal(i + 1);
            String decimalValue3 = "" + rs.getBigDecimal(i + 2);
            String value = values[index];

            if (decimalValue1.equalsIgnoreCase("0") && (value.equalsIgnoreCase(Boolean.TRUE.toString())
                    || value.equalsIgnoreCase(Boolean.FALSE.toString()))) {
                decimalValue1 = Boolean.FALSE.toString();
                decimalValue2 = Boolean.FALSE.toString();
                decimalValue3 = Boolean.FALSE.toString();
            } else if (decimalValue1.equalsIgnoreCase("1") && (value.equalsIgnoreCase(Boolean.TRUE.toString())
                    || value.equalsIgnoreCase(Boolean.FALSE.toString()))) {
                decimalValue1 = Boolean.TRUE.toString();
                decimalValue2 = Boolean.TRUE.toString();
                decimalValue3 = Boolean.TRUE.toString();
            }

            if (null != value) {
                if (value.equalsIgnoreCase("1.79E308")) {
                    value = "1.79E+308";
                } else if (value.equalsIgnoreCase("3.4E38")) {
                    value = "3.4E+38";
                }

                if (value.equalsIgnoreCase("-1.79E308")) {
                    value = "-1.79E+308";
                } else if (value.equalsIgnoreCase("-3.4E38")) {
                    value = "-3.4E+38";
                }
            }

            try {
                assertTrue(
                        decimalValue1.equalsIgnoreCase("" + value) && decimalValue2.equalsIgnoreCase("" + value)
                                && decimalValue3.equalsIgnoreCase("" + value),
                        TestResource.getResource("R_decryptionFailed") + "getBigDecimal(): " + decimalValue1 + ", "
                                + decimalValue2 + ", " + decimalValue3 + ".\n"
                                + TestResource.getResource("R_expectedValue") + value);
            } finally {
                index++;
            }
        }
    }

    void testGetString(ResultSet rs, int numberOfColumns, String[] values) throws SQLException {

        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            String stringValue1 = ("" + rs.getString(i)).trim();
            String stringValue2 = ("" + rs.getString(i + 1)).trim();
            String stringValue3 = ("" + rs.getString(i + 2)).trim();

            if (stringValue1.equalsIgnoreCase("0") && (values[index].equalsIgnoreCase(Boolean.TRUE.toString())
                    || values[index].equalsIgnoreCase(Boolean.FALSE.toString()))) {
                stringValue1 = Boolean.FALSE.toString();
                stringValue2 = Boolean.FALSE.toString();
                stringValue3 = Boolean.FALSE.toString();
            } else if (stringValue1.equalsIgnoreCase("1") && (values[index].equalsIgnoreCase(Boolean.TRUE.toString())
                    || values[index].equalsIgnoreCase(Boolean.FALSE.toString()))) {
                stringValue1 = Boolean.TRUE.toString();
                stringValue2 = Boolean.TRUE.toString();
                stringValue3 = Boolean.TRUE.toString();
            }
            try {

                boolean matches = stringValue1.equalsIgnoreCase("" + values[index])
                        && stringValue2.equalsIgnoreCase("" + values[index])
                        && stringValue3.equalsIgnoreCase("" + values[index]);

                if (("" + values[index]).length() >= 1000) {
                    assertTrue(matches, TestResource.getResource("R_decryptionFailed") + " getString():" + i + ", "
                            + (i + 1) + ", " + (i + 2) + ".\n" + TestResource.getResource("R_expectedValue") + index);
                } else {
                    assertTrue(matches,
                            TestResource.getResource("R_decryptionFailed") + " getString(): " + stringValue1 + ", "
                                    + stringValue2 + ", " + stringValue3 + ".\n"
                                    + TestResource.getResource("R_expectedValue") + values[index]);
                }
            } finally {
                index++;
            }
        }
    }

    // not testing this for now.
    @SuppressWarnings("unused")
    void testGetStringForDate(ResultSet rs, int numberOfColumns, LinkedList<Object> values) throws SQLException {

        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            String stringValue1 = ("" + rs.getString(i)).trim();
            String stringValue2 = ("" + rs.getString(i + 1)).trim();
            String stringValue3 = ("" + rs.getString(i + 2)).trim();

            try {
                if (index == 3) {
                    assertTrue(
                            stringValue1.contains("" + values.get(index))
                                    && stringValue2.contains("" + values.get(index))
                                    && stringValue3.contains("" + values.get(index)),
                            TestResource.getResource("R_decryptionFailed") + "getString(): " + stringValue1 + ", "
                                    + stringValue2 + ", " + stringValue3 + ".\n"
                                    + TestResource.getResource("R_expectedValue") + values.get(index));
                } else if (index == 4) // round value for datetime
                {
                    Object datetimeValue = "" + TestUtils.roundDatetimeValue(values.get(index));
                    assertTrue(
                            stringValue1.equalsIgnoreCase("" + datetimeValue)
                                    && stringValue2.equalsIgnoreCase("" + datetimeValue)
                                    && stringValue3.equalsIgnoreCase("" + datetimeValue),
                            TestResource.getResource("R_decryptionFailed") + "getString(): " + stringValue1 + ", "
                                    + stringValue2 + ", " + stringValue3 + ".\n"
                                    + TestResource.getResource("R_expectedValue") + datetimeValue);
                } else if (index == 5) // round value for smalldatetime
                {
                    Object smalldatetimeValue = "" + TestUtils.roundSmallDateTimeValue(values.get(index));
                    assertTrue(
                            stringValue1.equalsIgnoreCase("" + smalldatetimeValue)
                                    && stringValue2.equalsIgnoreCase("" + smalldatetimeValue)
                                    && stringValue3.equalsIgnoreCase("" + smalldatetimeValue),
                            TestResource.getResource("R_decryptionFailed") + "getString(): " + stringValue1 + ", "
                                    + stringValue2 + ", " + stringValue3 + ".\n"
                                    + TestResource.getResource("R_expectedValue") + smalldatetimeValue);
                } else {
                    assertTrue(
                            stringValue1.contains("" + values.get(index))
                                    && stringValue2.contains("" + values.get(index))
                                    && stringValue3.contains("" + values.get(index)),
                            TestResource.getResource("R_decryptionFailed") + "getString(): " + stringValue1 + ", "
                                    + stringValue2 + ", " + stringValue3 + ".\n"
                                    + TestResource.getResource("R_expectedValue") + values.get(index));
                }
            } finally {
                index++;
            }
        }
    }

    void testGetBytes(ResultSet rs, int numberOfColumns, LinkedList<byte[]> values) throws SQLException {
        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            byte[] b1 = rs.getBytes(i);
            byte[] b2 = rs.getBytes(i + 1);
            byte[] b3 = rs.getBytes(i + 2);

            byte[] expectedBytes = null;

            if (null != values.get(index)) {
                expectedBytes = values.get(index);
            }

            try {
                if (null != values.get(index)) {
                    for (int j = 0; j < expectedBytes.length; j++) {
                        assertTrue(expectedBytes[j] == b1[j] && expectedBytes[j] == b2[j] && expectedBytes[j] == b3[j],
                                TestResource.getResource("R_decryptionFailed") + "getObject(): " + b1 + ", " + b2 + ", "
                                        + b3 + ".\n");
                    }
                }
            } finally {
                index++;
            }
        }
    }

    void testGetStringForBinary(ResultSet rs, int numberOfColumns, LinkedList<byte[]> values) throws SQLException {

        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            String stringValue1 = ("" + rs.getString(i)).trim();
            String stringValue2 = ("" + rs.getString(i + 1)).trim();
            String stringValue3 = ("" + rs.getString(i + 2)).trim();

            StringBuffer expected = new StringBuffer();
            String expectedStr = null;

            if (null != values.get(index)) {
                for (byte b : values.get(index)) {
                    expected.append(String.format("%02X", b));
                }
                expectedStr = "" + expected.toString();
            } else {
                expectedStr = "null";
            }

            try {
                assertTrue(
                        stringValue1.startsWith(expectedStr) && stringValue2.startsWith(expectedStr)
                                && stringValue3.startsWith(expectedStr),
                        TestResource.getResource("R_decryptionFailed") + "getString(): " + stringValue1 + ", "
                                + stringValue2 + ", " + stringValue3 + ".\n"
                                + TestResource.getResource("R_expectedValue") + expectedStr);
            } finally {
                index++;
            }
        }
    }

    void testGetDate(ResultSet rs, int numberOfColumns, LinkedList<Object> values) throws SQLException {
        for (int i = 1; i <= numberOfColumns; i = i + 3) {

            if (rs instanceof SQLServerResultSet) {

                String stringValue1 = null;
                String stringValue2 = null;
                String stringValue3 = null;
                String expected = null;

                switch (i) {

                    case 1:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getDate(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getDate(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getDate(i + 2);
                        expected = "" + values.get(0);
                        break;

                    case 4:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getTimestamp(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getTimestamp(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getTimestamp(i + 2);
                        expected = "" + values.get(1);
                        break;

                    case 7:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getDateTimeOffset(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getDateTimeOffset(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getDateTimeOffset(i + 2);
                        expected = "" + values.get(2);
                        break;

                    case 10:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getTime(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getTime(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getTime(i + 2);
                        expected = "" + values.get(3);
                        break;

                    case 13:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getDateTime(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getDateTime(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getDateTime(i + 2);
                        expected = "" + TestUtils.roundDatetimeValue(values.get(4));
                        break;

                    case 16:
                        stringValue1 = "" + ((SQLServerResultSet) rs).getSmallDateTime(i);
                        stringValue2 = "" + ((SQLServerResultSet) rs).getSmallDateTime(i + 1);
                        stringValue3 = "" + ((SQLServerResultSet) rs).getSmallDateTime(i + 2);
                        expected = "" + TestUtils.roundSmallDateTimeValue(values.get(5));
                        break;

                    default:
                        fail(TestResource.getResource("R_switchFailed"));
                }

                assertTrue(
                        stringValue1.equalsIgnoreCase(expected) && stringValue2.equalsIgnoreCase(expected)
                                && stringValue3.equalsIgnoreCase(expected),
                        TestResource.getResource("R_decryptionFailed") + "testGetDate: " + stringValue1 + ", "
                                + stringValue2 + ", " + stringValue3 + ".\n"
                                + TestResource.getResource("R_expectedValue") + expected);
            }

            else {
                fail(TestResource.getResource("R_resultsetNotInstance"));
            }
        }
    }

    void testNumeric(Statement stmt, String[] numericValues, boolean isNull) throws SQLException {
        String sql = "select * from " + AbstractSQLGenerator.escapeIdentifier(NUMERIC_TABLE_AE);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {
            try (SQLServerResultSet rs = (stmt == null) ? (SQLServerResultSet) pstmt.executeQuery()
                                                        : (SQLServerResultSet) stmt.executeQuery(sql)) {
                int numberOfColumns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    testGetString(rs, numberOfColumns, numericValues);
                    testGetObject(rs, numberOfColumns, numericValues);
                    testGetBigDecimal(rs, numberOfColumns, numericValues);
                    if (!isNull)
                        testWithSpecifiedtype(rs, numberOfColumns, numericValues);
                    else {
                        String[] nullNumericValues = {Boolean.FALSE.toString(), "0", "0", "0", "0", "0.0", "0.0", "0.0",
                                null, null, null, null, null, null, null, null};
                        testWithSpecifiedtype(rs, numberOfColumns, nullNumericValues);
                    }
                }
            }
        }
    }

    void testWithSpecifiedtype(SQLServerResultSet rs, int numberOfColumns, String[] values) throws SQLException {

        String value1, value2, value3, expectedValue = null;
        int index = 0;

        // bit
        value1 = "" + rs.getBoolean(1);
        value2 = "" + rs.getBoolean(2);
        value3 = "" + rs.getBoolean(3);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // tiny
        value1 = "" + rs.getShort(4);
        value2 = "" + rs.getShort(5);
        value3 = "" + rs.getShort(6);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // smallint
        value1 = "" + rs.getShort(7);
        value2 = "" + rs.getShort(8);
        value3 = "" + rs.getShort(8);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // int
        value1 = "" + rs.getInt(10);
        value2 = "" + rs.getInt(11);
        value3 = "" + rs.getInt(12);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // bigint
        value1 = "" + rs.getLong(13);
        value2 = "" + rs.getLong(14);
        value3 = "" + rs.getLong(15);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // float
        value1 = "" + rs.getDouble(16);
        value2 = "" + rs.getDouble(17);
        value3 = "" + rs.getDouble(18);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // float(30)
        value1 = "" + rs.getDouble(19);
        value2 = "" + rs.getDouble(20);
        value3 = "" + rs.getDouble(21);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // real
        value1 = "" + rs.getFloat(22);
        value2 = "" + rs.getFloat(23);
        value3 = "" + rs.getFloat(24);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // decimal
        value1 = "" + rs.getBigDecimal(25);
        value2 = "" + rs.getBigDecimal(26);
        value3 = "" + rs.getBigDecimal(27);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // decimal (10,5)
        value1 = "" + rs.getBigDecimal(28);
        value2 = "" + rs.getBigDecimal(29);
        value3 = "" + rs.getBigDecimal(30);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // numeric
        value1 = "" + rs.getBigDecimal(31);
        value2 = "" + rs.getBigDecimal(32);
        value3 = "" + rs.getBigDecimal(33);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // numeric (8,2)
        value1 = "" + rs.getBigDecimal(34);
        value2 = "" + rs.getBigDecimal(35);
        value3 = "" + rs.getBigDecimal(36);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // smallmoney
        value1 = "" + rs.getSmallMoney(37);
        value2 = "" + rs.getSmallMoney(38);
        value3 = "" + rs.getSmallMoney(39);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // money
        value1 = "" + rs.getMoney(40);
        value2 = "" + rs.getMoney(41);
        value3 = "" + rs.getMoney(42);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // decimal(28,4)
        value1 = "" + rs.getBigDecimal(43);
        value2 = "" + rs.getBigDecimal(44);
        value3 = "" + rs.getBigDecimal(45);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;

        // numeric(28,4)
        value1 = "" + rs.getBigDecimal(46);
        value2 = "" + rs.getBigDecimal(47);
        value3 = "" + rs.getBigDecimal(48);

        expectedValue = values[index];
        Compare(expectedValue, value1, value2, value3);
        index++;
    }

    /**
     * Alter Column encryption on deterministic columns to randomized - this will trigger enclave to re-encrypt
     * 
     * @param stmt
     * @param tableName
     * @param table
     * @param values
     * @throws SQLException
     */
    private void testAlterColumnEncryption(SQLServerStatement stmt, String tableName, String table[][],
            String cekName) throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo)) {
            for (int i = 0; i < table.length; i++) {
                // alter deterministic to randomized
                String sql = "ALTER TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " ALTER COLUMN "
                        + ColumnType.DETERMINISTIC.name() + table[i][0] + " " + table[i][1]
                        + String.format(encryptSql, ColumnType.RANDOMIZED.name(), cekName) + ")";
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {
                    stmt.execute(sql);
                    if (!TestUtils.isAEv2(con)) {
                        fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                    }
                } catch (SQLException e) {
                    if (!TestUtils.isAEv2(con)) {
                        fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                    } else {
                        fail(TestResource.getResource("R_AlterAEv2Error") + e.getMessage() + "Query: " + sql);
                    }
                }
            }
        }
    }

    private void testRichQuery(SQLServerStatement stmt, String tableName, String table[][],
            String[] values) throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo)) {
            for (int i = 0; i < table.length; i++) {
                String sql = "SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE "
                        + ColumnType.PLAIN.name() + table[i][0] + "= ?";
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {
                    switch (table[i][2]) {
                        case "CHAR":
                        case "LONGVARCHAR":
                            pstmt.setString(1, values[i + 1 / 3]);
                            break;
                        case "NCHAR":
                        case "LONGNVARCHAR":
                            pstmt.setNString(1, values[i + 1 / 3]);
                            break;
                        case "GUID":
                            pstmt.setUniqueIdentifier(1, null);
                            pstmt.setUniqueIdentifier(1, Constants.UID);
                            break;
                        case "BIT":
                            if (values[i + 1 / 3].equals("null")) {
                                pstmt.setObject(1, null, java.sql.Types.BIT);
                            } else {
                                pstmt.setBoolean(1,
                                        (values[i + 1 / 3].equalsIgnoreCase(Boolean.TRUE.toString())) ? true : false);
                            }
                            break;
                        case "TINYINT":
                            if (values[i + 1 / 3].equals("null")) {
                                pstmt.setObject(1, null, java.sql.Types.TINYINT);
                            } else {
                                pstmt.setShort(1, Short.valueOf(values[i + 1 / 3]));
                            }
                            break;
                        case "SMALLINT":
                            if (values[i + 1 / 3].equals("null")) {
                                pstmt.setObject(1, null, java.sql.Types.SMALLINT);
                            } else {
                                pstmt.setShort(1, Short.valueOf(values[i + 1 / 3]));
                            }
                            break;
                        case "INTEGER":
                            if (values[i + 1 / 3].equals("null")) {
                                pstmt.setObject(1, null, java.sql.Types.INTEGER);
                            } else {
                                pstmt.setInt(1, Integer.valueOf(values[i + 1 / 3]));
                            }
                            break;
                        case "BIGINT":
                            if (values[i + 1 / 3].equals("null")) {
                                pstmt.setObject(1, null, java.sql.Types.BIGINT);
                            } else {
                                pstmt.setLong(1, Long.valueOf(values[i + 1 / 3]));
                            }
                            break;
                        case "DOUBLE":
                            if (values[i + 1 / 3].equals("null")) {
                                pstmt.setObject(1, null, java.sql.Types.DOUBLE);
                            } else {
                                pstmt.setDouble(1, Double.valueOf(values[i + 1 / 3]));
                            }
                            break;
                        case "FLOAT":
                            if (values[i + 1 / 3].equals("null")) {
                                pstmt.setObject(1, null, java.sql.Types.DOUBLE);
                            } else {
                                pstmt.setFloat(1, Float.valueOf(values[i + 1 / 3]));
                            }
                            break;
                        case "DECIMAL":
                            if (values[i + 1 / 3].equals("null")) {
                                pstmt.setObject(1, null, java.sql.Types.DECIMAL);
                            } else {
                                pstmt.setBigDecimal(1, new BigDecimal(values[i + 1 / 3]));
                            }
                            break;
                        case "SMALLMONEY":
                            if (values[i + 1 / 3].equals("null")) {
                                pstmt.setObject(1, null, microsoft.sql.Types.SMALLMONEY);
                            } else {
                                pstmt.setSmallMoney(1, new BigDecimal(values[i + 1 / 3]));
                            }
                            break;
                        case "MONEY":
                            if (values[i + 1 / 3].equals("null")) {
                                pstmt.setObject(1, null, microsoft.sql.Types.MONEY);
                            } else {
                                pstmt.setMoney(1, new BigDecimal(values[i + 1 / 3]));
                            }
                            break;
                        default:
                            fail(TestResource.getResource("R_invalidObjectName") + ": " + table[i][2]);
                    }

                    try (ResultSet rs = (pstmt.executeQuery())) {
                        if (!TestUtils.isAEv2(con)) {
                            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                        }

                        int numberOfColumns = rs.getMetaData().getColumnCount();
                        while (rs.next()) {
                            testGetString(rs, numberOfColumns, values);
                            testGetObject(rs, numberOfColumns, values);
                        }
                    } catch (SQLException e) {
                        if (!TestUtils.isAEv2(con)) {
                            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                        } else {
                            fail(TestResource.getResource("R_RichQueryError") + e.getMessage() + "Query: " + sql);
                        }
                    }
                } catch (Exception e) {
                    fail(TestResource.getResource("R_RichQueryError") + e.getMessage() + "Query: " + sql);
                }

            }
        }
    }

    private void testRichQueryDate(SQLServerStatement stmt, String tableName, String table[][],
            LinkedList<Object> values) throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo)) {
            for (int i = 0; i < table.length; i++) {
                String sql = "SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE "
                        + ColumnType.PLAIN.name() + table[i][0] + "= ?";
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {
                    switch (table[i][2]) {
                        case "DATE":
                            pstmt.setDate(1, (Date) values.get(i + 1 / 3));
                            break;
                        case "TIMESTAMP":
                            pstmt.setTimestamp(1, (Timestamp) values.get(i + 1 / 3));
                            break;
                        case "DATETIMEOFFSET":
                            pstmt.setDateTimeOffset(1, (DateTimeOffset) values.get(i + 1 / 3));
                            break;
                        case "TIME":
                            pstmt.setTime(1, (Time) values.get(i + 1 / 3));
                            break;
                        case "DATETIME":
                            pstmt.setDateTime(1, (Timestamp) values.get(i + 1 / 3));
                            break;
                        case "SMALLDATETIME":
                            pstmt.setSmallDateTime(1, (Timestamp) values.get(i + 1 / 3));
                            break;
                        default:
                            fail(TestResource.getResource("R_invalidObjectName") + ": " + table[i][2]);
                    }

                    try (ResultSet rs = (pstmt.executeQuery())) {
                        if (!TestUtils.isAEv2(con)) {
                            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                        }

                        int numberOfColumns = rs.getMetaData().getColumnCount();
                        while (rs.next()) {
                            testGetObjectForTemporal(rs, numberOfColumns, values);
                            testGetDate(rs, numberOfColumns, values);
                        }
                    } catch (SQLException e) {
                        if (!TestUtils.isAEv2(con)) {
                            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                        } else {
                            fail(TestResource.getResource("R_RichQueryError") + e.getMessage() + "Query: " + sql);
                        }
                    }
                } catch (Exception e) {
                    fail(TestResource.getResource("R_RichQueryError") + e.getMessage() + "Query: " + sql);
                }
            }
        }
    }

    private void testRichQuery(SQLServerStatement stmt, String tableName, String table[][],
            LinkedList<byte[]> values) throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo)) {
            for (int i = 0; i < table.length; i++) {
                String sql = "SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE "
                        + ColumnType.PLAIN.name() + table[i][0] + "= ?";
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {
                    switch (table[i][2]) {
                        case "BINARY":
                            pstmt.setBytes(1, (byte[]) values.get(i + 1 / 3));
                            break;
                        default:
                            fail(TestResource.getResource("R_invalidObjectName") + ": " + table[i][2]);
                    }

                    try (ResultSet rs = (pstmt.executeQuery())) {
                        if (!TestUtils.isAEv2(con)) {
                            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                        }

                        int numberOfColumns = rs.getMetaData().getColumnCount();
                        while (rs.next()) {
                            testGetStringForBinary(rs, numberOfColumns, values);
                            testGetBytes(rs, numberOfColumns, values);
                            testGetObjectForBinary(rs, numberOfColumns, values);
                        }
                    } catch (SQLException e) {
                        if (!TestUtils.isAEv2(con)) {
                            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                        } else {
                            fail(TestResource.getResource("R_RichQueryError") + e.getMessage() + "Query: " + sql);
                        }
                    }
                } catch (Exception e) {
                    fail(TestResource.getResource("R_RichQueryError") + e.getMessage() + "Query: " + sql);
                }
            }
        }
    }

    void Compare(String expectedValue, String value1, String value2, String value3) {

        if (null != expectedValue) {
            if (expectedValue.equalsIgnoreCase("1.79E+308")) {
                expectedValue = "1.79E308";
            } else if (expectedValue.equalsIgnoreCase("3.4E+38")) {
                expectedValue = "3.4E38";
            }

            if (expectedValue.equalsIgnoreCase("-1.79E+308")) {
                expectedValue = "-1.79E308";
            } else if (expectedValue.equalsIgnoreCase("-3.4E+38")) {
                expectedValue = "-3.4E38";
            }
        }

        assertTrue(
                value1.equalsIgnoreCase("" + expectedValue) && value2.equalsIgnoreCase("" + expectedValue)
                        && value3.equalsIgnoreCase("" + expectedValue),
                TestResource.getResource("R_decryptionFailed") + "getBigDecimal(): " + value1 + ", " + value2 + ", "
                        + value3 + ".\n" + TestResource.getResource("R_expectedValue"));
    }

    void testChars(SQLServerStatement stmt, String cekName, String[][] table, String[] values, TestCase testCase,
            boolean isTestEnclave) throws SQLException {
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(CHAR_TABLE_AE), stmt);
        createTable(CHAR_TABLE_AE, cekName, table);

        switch (testCase) {
            case NORMAL:
                populateCharNormalCase(values);
                break;
            case SETOBJECT:
                populateCharSetObject(values);
                break;
            case SETOBJECT_NULL:
                populateDateSetObjectNull();
                break;
            case SETOBJECT_WITH_JDBCTYPES:
                populateCharSetObjectWithJDBCTypes(values);
                break;
            case NULL:
                populateCharNullCase();
                break;
            default:
                fail(TestResource.getResource("R_switchFailed"));
                break;
        }

        testChar(stmt, values);
        testChar(null, values);

        if (isTestEnclave) {
            if (null == getConfiguredProperty(Constants.ENCLAVE_ATTESTATIONURL)
                    || null == getConfiguredProperty(Constants.ENCLAVE_ATTESTATIONPROTOCOL)) {
                fail(TestResource.getResource("R_reqExternalSetup"));
            }

            testAlterColumnEncryption(stmt, CHAR_TABLE_AE, table, cekName);
            testRichQuery(stmt, CHAR_TABLE_AE, table, values);
        }
    }

    void testBinaries(SQLServerStatement stmt, String cekName, String[][] table, LinkedList<byte[]> values,
            TestCase testCase, boolean isTestEnclave) throws SQLException {
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(BINARY_TABLE_AE), stmt);
        createTable(BINARY_TABLE_AE, cekName, table);

        switch (testCase) {
            case NORMAL:
                populateBinaryNormalCase(values);
                break;
            case SETOBJECT:
                populateBinarySetObject(values);
            case SETOBJECT_WITH_JDBCTYPES:
                populateBinarySetObjectWithJDBCType(values);
                break;
            case SETOBJECT_NULL:
                populateBinarySetObject(null);
                break;
            case NULL:
                populateBinaryNullCase();
                break;
            default:
                fail(TestResource.getResource("R_switchFailed"));
                break;
        }

        testBinary(stmt, values);
        testBinary(null, values);

        if (isTestEnclave) {
            if (null == getConfiguredProperty(Constants.ENCLAVE_ATTESTATIONURL)
                    || null == getConfiguredProperty(Constants.ENCLAVE_ATTESTATIONPROTOCOL)) {
                fail(TestResource.getResource("R_reqExternalSetup"));
            }

            testAlterColumnEncryption(stmt, BINARY_TABLE_AE, table, cekName);
            testRichQuery(stmt, BINARY_TABLE_AE, table, values);
        }
    }

    void testDates(SQLServerStatement stmt, String cekName, String[][] table, LinkedList<Object> values,
            TestCase testCase, boolean isTestEnclave) throws SQLException {
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(DATE_TABLE_AE), stmt);
        createTable(DATE_TABLE_AE, cekName, table);

        switch (testCase) {
            case NORMAL:
                populateDateNormalCase(values);
                break;
            case SETOBJECT:
                populateDateSetObject(values, "");
                break;
            case SETOBJECT_WITH_JDBCTYPES:
                populateDateSetObject(values, "setwithJDBCType");
                break;
            case SETOBJECT_WITH_JAVATYPES:
                populateDateSetObject(values, "setwithJavaType");
                break;
            case SETOBJECT_NULL:
                populateDateNullCase();
                break;
            case NULL:
                populateDateNullCase();
                break;
            default:
                fail(TestResource.getResource("R_switchFailed"));
                break;
        }

        testDate(stmt, values);
        testDate(null, values);

        if (isTestEnclave) {
            if (null == getConfiguredProperty(Constants.ENCLAVE_ATTESTATIONURL)
                    || null == getConfiguredProperty(Constants.ENCLAVE_ATTESTATIONPROTOCOL)) {
                fail(TestResource.getResource("R_reqExternalSetup"));
            }

            testAlterColumnEncryption(stmt, DATE_TABLE_AE, table, cekName);
            testRichQueryDate(stmt, DATE_TABLE_AE, table, values);
        }
    }

    void testNumerics(SQLServerStatement stmt, String cekName, String[][] table, String[] values1, String[] values2,
            TestCase testCase, boolean isTestEnclave) throws SQLException {
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(NUMERIC_TABLE_AE), stmt);
        createTable(NUMERIC_TABLE_AE, cekName, table);

        boolean isNull = false;
        switch (testCase) {
            case NORMAL:
                populateNumeric(values1);
                break;
            case SETOBJECT:
                populateNumericSetObject(values1);
                break;
            case SETOBJECT_WITH_JDBCTYPES:
                populateNumericSetObjectWithJDBCTypes(values1);
                break;
            case NULL:
                populateNumericNullCase(values1);
                isNull = true;
                break;
            default:
                fail(TestResource.getResource("R_switchFailed"));
                break;
        }

        testNumeric(stmt, values1, isNull);
        testNumeric(null, values2, isNull);

        if (isTestEnclave) {
            if (null == getConfiguredProperty(Constants.ENCLAVE_ATTESTATIONURL)
                    || null == getConfiguredProperty(Constants.ENCLAVE_ATTESTATIONPROTOCOL)) {
                fail(TestResource.getResource("R_reqExternalSetup"));
            }

            testAlterColumnEncryption(stmt, NUMERIC_TABLE_AE, table, cekName);
            testRichQuery(stmt, NUMERIC_TABLE_AE, table, values1);
            testRichQuery(stmt, NUMERIC_TABLE_AE, table, values2);
        }
    }
}
