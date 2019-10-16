/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
    public void testCharSpecificSetter_aev1() throws SQLException {
        testCharSpecificSetter(false);
    }

    @Tag(Constants.xSQLv15)
    @Test
    public void testCharSpecificSetter_aev2() throws SQLException {
        testCharSpecificSetter(isAEv2Supported);
    }

    private void testCharSpecificSetter(boolean isTestEnclave) throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekJks, charTable, values, TestCase.NORMAL, isTestEnclave);

            if (null != cekWin) {
                testChars(stmt, cekWin, charTable, values, TestCase.NORMAL, isTestEnclave);
            }

            if (null != cekAkv) {
                testChars(stmt, cekAkv, charTable, values, TestCase.NORMAL, isTestEnclave);
            }
        }
    }

    /**
     * Junit test case for char set object for string values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSetObject_aev1() throws SQLException {
        testCharSetObject(false);
    }

    @Tag(Constants.xSQLv15)
    @Test
    public void testCharSetObject_aev2() throws SQLException {
        testCharSetObject(isAEv2Supported);
    }

    private void testCharSetObject(boolean isTestEnclave) throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekJks, charTable, values, TestCase.SETOBJECT, isTestEnclave);

            if (null != cekWin) {
                testChars(stmt, cekWin, charTable, values, TestCase.SETOBJECT, isTestEnclave);
            }

            if (null != cekAkv) {
                testChars(stmt, cekAkv, charTable, values, TestCase.SETOBJECT, isTestEnclave);
            }

        }
    }

    /**
     * Junit test case for char set object for jdbc string values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSetObjectWithJDBCTypes_aev1() throws SQLException {
        testCharSetObjectWithJDBCTypes(false);
    }

    @Tag(Constants.xSQLv15)
    @Test
    public void testCharSetObjectWithJDBCTypes_aev2() throws SQLException {
        testCharSetObjectWithJDBCTypes(isAEv2Supported);
    }

    private void testCharSetObjectWithJDBCTypes(boolean isTestEnclave) throws SQLException {

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekJks, charTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, isTestEnclave);

            if (null != cekWin) {
                testChars(stmt, cekWin, charTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, isTestEnclave);
            }

            if (null != cekAkv) {
                testChars(stmt, cekAkv, charTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, isTestEnclave);
            }
        }
    }

    /**
     * Junit test case for char set string for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSpecificSetterNull() throws SQLException {
        testCharSpecificSetterNull(false);
    }

    @Tag(Constants.xSQLv15)
    @Test
    public void testCharSpecificSetterNull_aev2() throws SQLException {
        testCharSpecificSetterNull(isAEv2Supported);
    }

    private void testCharSpecificSetterNull(boolean isTestEnclave) throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = {null, null, null, null, null, null, null, null, null};

            testChars(stmt, cekJks, charTable, values, TestCase.NORMAL, isTestEnclave);

            if (null != cekWin) {
                testChars(stmt, cekWin, charTable, values, TestCase.NORMAL, isTestEnclave);
            }

            if (null != cekAkv) {
                testChars(stmt, cekAkv, charTable, values, TestCase.NORMAL, isTestEnclave);
            }
        }
    }

    /**
     * Junit test case for char set object for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSetObjectNull_aev1() throws SQLException {
        testCharSetObjectNull(false);
    }

    @Tag(Constants.xSQLv15)
    @Test
    public void testCharSetObjectNull_aev2() throws SQLException {
        testCharSetObjectNull(isAEv2Supported);
    }

    private void testCharSetObjectNull(boolean isTestEnclave) throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = {null, null, null, null, null, null, null, null, null};

            testChars(stmt, cekJks, charTable, values, TestCase.SETOBJECT, isTestEnclave);

            if (null != cekWin) {
                testChars(stmt, cekWin, charTable, values, TestCase.SETOBJECT, isTestEnclave);
            }

            if (null != cekAkv) {
                testChars(stmt, cekAkv, charTable, values, TestCase.SETOBJECT, isTestEnclave);
            }
        }
    }

    /**
     * Junit test case for char set null for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSetNull_aev1() throws SQLException {
        testCharSetNull(false);
    }

    @Tag(Constants.xSQLv15)
    @Test
    public void testCharSetNull_aev2() throws SQLException {
        testCharSetNull(isAEv2Supported);
    }

    private void testCharSetNull(boolean isTestEnclave) throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = {null, null, null, null, null, null, null, null, null};

            testChars(stmt, cekJks, charTable, values, TestCase.NULL, isTestEnclave);

            if (null != cekWin) {
                testChars(stmt, cekWin, charTable, values, TestCase.NULL, isTestEnclave);
            }

            if (null != cekAkv) {
                testChars(stmt, cekAkv, charTable, values, TestCase.NULL, isTestEnclave);
            }
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
            LinkedList<byte[]> values = createbinaryValues(false);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.NORMAL);

            if (null != cekWin) {
                testBinaries(stmt, cekWin, binaryTable, values, TestCase.NORMAL);
            }

            if (null != cekAkv) {
                testBinaries(stmt, cekAkv, binaryTable, values, TestCase.NORMAL);
            }
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
            LinkedList<byte[]> values = createbinaryValues(false);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.SETOBJECT);

            if (null != cekWin) {
                testBinaries(stmt, cekWin, binaryTable, values, TestCase.SETOBJECT);
            }

            if (null != cekAkv) {
                testBinaries(stmt, cekAkv, binaryTable, values, TestCase.SETOBJECT);
            }
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
            LinkedList<byte[]> values = createbinaryValues(true);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.NULL);

            if (null != cekWin) {
                testBinaries(stmt, cekWin, binaryTable, values, TestCase.NULL);
            }

            if (null != cekAkv) {
                testBinaries(stmt, cekAkv, binaryTable, values, TestCase.NULL);
            }
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
            LinkedList<byte[]> values = createbinaryValues(true);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.NORMAL);

            if (null != cekWin) {
                testBinaries(stmt, cekWin, binaryTable, values, TestCase.NORMAL);
            }

            if (null != cekAkv) {
                testBinaries(stmt, cekAkv, binaryTable, values, TestCase.NORMAL);
            }
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
            LinkedList<byte[]> values = createbinaryValues(true);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.SETOBJECT_NULL);

            if (null != cekWin) {
                testBinaries(stmt, cekWin, binaryTable, values, TestCase.SETOBJECT_NULL);
            }

            if (null != cekAkv) {
                testBinaries(stmt, cekAkv, binaryTable, values, TestCase.SETOBJECT_NULL);
            }
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
            LinkedList<byte[]> values = createbinaryValues(false);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES);

            if (null != cekWin) {
                testBinaries(stmt, cekWin, binaryTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES);
            }

            if (null != cekAkv) {
                testBinaries(stmt, cekAkv, binaryTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES);
            }
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

            testDates(stmt, cekJks, dateTable, values, TestCase.NORMAL);

            if (null != cekWin) {
                testDates(stmt, cekWin, dateTable, values, TestCase.NORMAL);
            }

            if (null != cekAkv) {
                testDates(stmt, cekAkv, dateTable, values, TestCase.NORMAL);
            }
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

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT);

            if (null != cekWin) {
                testDates(stmt, cekWin, dateTable, values, TestCase.SETOBJECT);
            }

            if (null != cekAkv) {
                testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT);
            }
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

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT_WITH_JAVATYPES);

            if (null != cekWin) {
                testDates(stmt, cekWin, dateTable, values, TestCase.SETOBJECT_WITH_JAVATYPES);
            }

            if (null != cekAkv) {
                testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT_WITH_JAVATYPES);
            }
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

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES);

            if (null != cekWin) {
                testDates(stmt, cekWin, dateTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES);
            }

            if (null != cekAkv) {
                testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES);
            }
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

            testDates(stmt, cekJks, dateTable, values, TestCase.NORMAL);

            if (null != cekWin) {
                testDates(stmt, cekWin, dateTable, values, TestCase.NORMAL);
            }

            if (null != cekAkv) {
                testDates(stmt, cekAkv, dateTable, values, TestCase.NORMAL);
            }
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

            testDates(stmt, cekJks, dateTable, values, TestCase.NULL);

            if (null != cekWin) {
                testDates(stmt, cekWin, dateTable, values, TestCase.NULL);
            }

            if (null != cekAkv) {
                testDates(stmt, cekAkv, dateTable, values, TestCase.NULL);
            }
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

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT_NULL);

            if (null != cekWin) {
                testDates(stmt, cekWin, dateTable, values, TestCase.SETOBJECT_NULL);
            }

            if (null != cekAkv) {
                testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT_NULL);
            }
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL);

            if (null != cekWin) {
                testNumerics(stmt, cekWin, numericTable, values1, values2, TestCase.NORMAL);
            }

            if (null != cekAkv) {
                testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL);
            }
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.SETOBJECT);

            if (null != cekWin) {
                testNumerics(stmt, cekWin, numericTable, values1, values2, TestCase.SETOBJECT);
            }

            if (null != cekAkv) {
                testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.SETOBJECT);
            }
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.SETOBJECT_WITH_JDBCTYPES);

            if (null != cekWin) {
                testNumerics(stmt, cekWin, numericTable, values1, values2, TestCase.SETOBJECT_WITH_JDBCTYPES);
            }

            if (null != cekAkv) {
                testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.SETOBJECT_WITH_JDBCTYPES);
            }
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL);

            if (null != cekWin) {
                testNumerics(stmt, cekWin, numericTable, values1, values2, TestCase.NORMAL);
            }

            if (null != cekAkv) {
                testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL);
            }
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL);

            if (null != cekWin) {
                testNumerics(stmt, cekWin, numericTable, values1, values2, TestCase.NORMAL);
            }

            if (null != cekAkv) {
                testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL);
            }
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NULL);

            if (null != cekWin) {
                testNumerics(stmt, cekWin, numericTable, values1, values2, TestCase.NULL);
            }

            if (null != cekAkv) {
                testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NULL);
            }
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NULL);

            if (null != cekWin) {
                testNumerics(stmt, cekWin, numericTable, values1, values2, TestCase.NULL);
            }

            if (null != cekAkv) {
                testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NULL);
            }
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL);

            if (null != cekWin) {
                testNumerics(stmt, cekWin, numericTable, values1, values2, TestCase.NORMAL);
            }

            if (null != cekAkv) {
                testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL);
            }
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

    private void testChar(SQLServerStatement stmt, String[] values) throws SQLException {
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

    private void testBinary(SQLServerStatement stmt, LinkedList<byte[]> values) throws SQLException {
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

    private void testDate(SQLServerStatement stmt, LinkedList<Object> values1) throws SQLException {
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

    private void testGetObject(ResultSet rs, int numberOfColumns, String[] values) throws SQLException {
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

    private void testGetObjectForTemporal(ResultSet rs, int numberOfColumns,
            LinkedList<Object> values) throws SQLException {
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

    private void testGetObjectForBinary(ResultSet rs, int numberOfColumns,
            LinkedList<byte[]> values) throws SQLException {
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

    private void testGetBigDecimal(ResultSet rs, int numberOfColumns, String[] values) throws SQLException {

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

    private void testGetString(ResultSet rs, int numberOfColumns, String[] values) throws SQLException {

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
    private void testGetStringForDate(ResultSet rs, int numberOfColumns,
            LinkedList<Object> values) throws SQLException {

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

    private void testGetBytes(ResultSet rs, int numberOfColumns, LinkedList<byte[]> values) throws SQLException {
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

    private void testGetStringForBinary(ResultSet rs, int numberOfColumns,
            LinkedList<byte[]> values) throws SQLException {

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

    private void testGetDate(ResultSet rs, int numberOfColumns, LinkedList<Object> values) throws SQLException {
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

    private void testNumeric(Statement stmt, String[] numericValues, boolean isNull) throws SQLException {
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

    private void testWithSpecifiedtype(SQLServerResultSet rs, int numberOfColumns,
            String[] values) throws SQLException {

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
    private void testAlterColumnEncryption(SQLServerStatement stmt, String tableName, String table[][], String cekName,
            String[] values) throws SQLException {
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
                            pstmt.setString(1, values[i / 3]);
                            break;
                        case "NCHAR":
                        case "LONGNVARCHAR":
                            pstmt.setNString(1, values[i / 3]);
                            break;
                        case "GUID":
                            pstmt.setUniqueIdentifier(1, null);
                            pstmt.setUniqueIdentifier(1, Constants.UID);
                            break;
                        default:
                            System.out.println("die");

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

    private void Compare(String expectedValue, String value1, String value2, String value3) {

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

    private void testChars(SQLServerStatement stmt, String cekName, String[][] table, String[] values,
            TestCase testcase, boolean isTestEnclave) throws SQLException {
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(CHAR_TABLE_AE), stmt);
        createTable(CHAR_TABLE_AE, cekName, table);

        switch (testcase) {
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
        }

        testChar(stmt, values);
        testChar(null, values);

        if (isTestEnclave && null != getConfiguredProperty(Constants.ENCLAVE_ATTESTATIONURL)) {
            testAlterColumnEncryption(stmt, CHAR_TABLE_AE, table, cekName, values);
            testRichQuery(stmt, CHAR_TABLE_AE, table, values);
        }
    }

    private void testBinaries(SQLServerStatement stmt, String cekName, String[][] table, LinkedList<byte[]> values,
            TestCase testcase) throws SQLException {
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(BINARY_TABLE_AE), stmt);
        createTable(BINARY_TABLE_AE, cekName, table);

        switch (testcase) {
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
        }

        testBinary(stmt, values);
        testBinary(null, values);
    }

    private void testDates(SQLServerStatement stmt, String cekName, String[][] table, LinkedList<Object> values,
            TestCase testcase) throws SQLException {
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(DATE_TABLE_AE), stmt);
        createTable(DATE_TABLE_AE, cekName, table);

        switch (testcase) {
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
            case NULL:
                populateDateNullCase();
                break;
        }

        testDate(stmt, values);
        testDate(null, values);
    }

    private void testNumerics(SQLServerStatement stmt, String cekName, String[][] table, String[] values1,
            String[] values2, TestCase testcase) throws SQLException {
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(NUMERIC_TABLE_AE), stmt);
        createTable(NUMERIC_TABLE_AE, cekName, table);

        boolean isNull = false;
        switch (testcase) {
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
        }

        testNumeric(stmt, values1, isNull);
        testNumeric(null, values2, isNull);
    }
}
