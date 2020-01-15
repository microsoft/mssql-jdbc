/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.security.cert.CertificateException;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

import org.junit.Test;
import org.junit.jupiter.api.Tag;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.microsoft.sqlserver.jdbc.EnclavePackageTest;
import com.microsoft.sqlserver.jdbc.RandomData;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Tests Enclave decryption and encryption of values
 *
 */
@RunWith(Parameterized.class)
@Tag(Constants.xSQLv12)
@Tag(Constants.xSQLv14)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.xAzureSQLDB)
@Tag(Constants.reqExternalSetup)
public class EnclaveTest extends JDBCEncryptionDecryptionTest {

    public EnclaveTest(String serverName, String url, String protocol) throws Exception {
        super(serverName, url, protocol);
        setupEnclave();
    }

    private boolean nullable = false;
    private static boolean isAEv2 = false;

    public void setupEnclave() throws Exception {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo)) {
            isAEv2 = TestUtils.isAEv2(con);
        } catch (SQLException e) {
            isAEv2 = false;
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }

        org.junit.Assume.assumeTrue(isAEv2);

        EnclavePackageTest.setupEnclave(enclaveAttestationUrl, enclaveAttestationProtocol);
    }

    /**
     * Tests basic connection.
     * 
     * @throws SQLException
     *         when an error occurs
     */
    @Test
    public void testBasicConnection() throws SQLException {
        org.junit.Assume.assumeTrue(isAEv2);

        EnclavePackageTest.testBasicConnection();
    }

    /**
     * Tests invalid connection property combinations.
     */
    @Test
    public void testInvalidProperties() {
        org.junit.Assume.assumeTrue(isAEv2);

        EnclavePackageTest.testInvalidProperties();
    }

    /*
     * Test calling verifyColumnMasterKeyMetadata for non enclave computation
     */
    @Test
    public void testVerifyCMKNoEnclave() {
        EnclavePackageTest.testVerifyCMKNoEnclave();
    }

    /*
     * Test calling verifyColumnMasterKeyMetadata with untrusted key path
     */
    @Test
    public void testVerifyCMKUntrusted() {
        EnclavePackageTest.testVerifyCMKUntrusted();
    }

    /*
     * Test getEnclavePackage with null enclaveSession
     */
    @Test
    public void testGetEnclavePackage() {
        EnclavePackageTest.testGetEnclavePackage();
    }

    /*
     * Test invalidEnclaveSession
     */
    @Test
    public void testInvalidEnclaveSession() {
        EnclavePackageTest.testInvalidEnclaveSession();
    }

    /*
     * Test VSM createSessionSecret with bad server response
     */
    @Test
    public void testNullSessionSecret() throws SQLServerException {
        EnclavePackageTest.testNullSessionSecret();
    }

    /*
     * Test bad session secret
     */
    @Test
    public void testBadSessionSecret() throws SQLServerException {
        EnclavePackageTest.testBadSessionSecret();
    }

    /*
     * Test null Attestation response
     */
    @Test
    public void testNullAttestationResponse() throws SQLServerException {
        EnclavePackageTest.testNullAttestationResponse();
    }

    /*
     * Test bad Attestation response
     */
    @Test
    public void testBadAttestationResponse() throws SQLServerException {
        EnclavePackageTest.testBadAttestationResponse();
    }

    /*
     * Test bad certificate signature
     */
    @Test
    public void testBadCertSignature() throws SQLServerException, CertificateException {
        EnclavePackageTest.testBadCertSignature();
    }

    /*
     * Negative Test - AEv2 not supported
     */
    @Test
    public void testAEv2NotSupported() {
        org.junit.Assume.assumeFalse(isAEv2);

        EnclavePackageTest.testAEv2NotSupported();
    }

    /*
     * Negative Test = AEv2 not enabled
     */
    @Test
    public void testAEv2Disabled() throws SQLException {
        org.junit.Assume.assumeTrue(isAEv2);

        // connection string w/o AEv2
        String testConnectionString = TestUtils.removeProperty(AETestConnectionString,
                Constants.ENCLAVE_ATTESTATIONURL);
        testConnectionString = TestUtils.removeProperty(testConnectionString, Constants.ENCLAVE_ATTESTATIONPROTOCOL);

        try (SQLServerConnection con = PrepUtil.getConnection(testConnectionString);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);
            testChars(stmt, cekJks, charTable, values, TestCase.NORMAL, true);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Throwable e) {
            // testChars called fail()
            assertTrue(e.getMessage().contains(TestResource.getResource("R_AlterAEv2Error")));
        }
    }

    /**
     * Test case for char set string for string values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSpecificSetter() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekJks, charTable, values, TestCase.NORMAL, true);
            testChars(stmt, cekAkv, charTable, values, TestCase.NORMAL, true);
        }
    }

    /**
     * Test case for char set string for string values using windows certificate store
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSpecificSetterWindows() throws SQLException {
        org.junit.Assume.assumeTrue(System.getProperty("os.name").startsWith("Windows"));

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekWin, charTable, values, TestCase.NORMAL, true);
        }
    }

    /**
     * Test case for char set object for string values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSetObject() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekJks, charTable, values, TestCase.SETOBJECT, true);
            testChars(stmt, cekAkv, charTable, values, TestCase.SETOBJECT, true);
        }
    }

    /**
     * Test case for char set object for jdbc string values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSetObjectWithJDBCTypes() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekJks, charTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, true);
            testChars(stmt, cekAkv, charTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, true);
        }
    }

    /**
     * Test case for char set string for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSpecificSetterNull() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = {null, null, null, null, null, null, null, null, null};

            testChars(stmt, cekJks, charTable, values, TestCase.NORMAL, true);
            testChars(stmt, cekAkv, charTable, values, TestCase.NORMAL, true);
        }
    }

    /**
     * Test case for char set object for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSetObjectNull() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = {null, null, null, null, null, null, null, null, null};

            testChars(stmt, cekJks, charTable, values, TestCase.SETOBJECT, true);
            testChars(stmt, cekAkv, charTable, values, TestCase.SETOBJECT, true);
        }
    }

    /**
     * Test case for char set null for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testCharSetNull() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = {null, null, null, null, null, null, null, null, null};

            testChars(stmt, cekJks, charTable, values, TestCase.NULL, true);
            testChars(stmt, cekAkv, charTable, values, TestCase.NULL, true);
        }
    }

    /**
     * Test case for binary set binary for binary values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySpecificSetter() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(false);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.NORMAL, true);
            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.NORMAL, true);
        }
    }

    /**
     * Test case for binary set binary for binary values using windows certificate store
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySpecificSetterWindows() throws SQLException {
        org.junit.Assume.assumeTrue(System.getProperty("os.name").startsWith("Windows"));

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(false);

            testBinaries(stmt, cekWin, binaryTable, values, TestCase.NORMAL, true);
        }
    }

    /**
     * Test case for binary set object for binary values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySetobject() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(false);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.SETOBJECT, true);
            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.SETOBJECT, true);
        }
    }

    /**
     * Test case for binary set null for binary values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySetNull() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(true);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.NULL, true);
            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.NULL, true);
        }
    }

    /**
     * Test case for binary set binary for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySpecificSetterNull() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(true);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.NORMAL, true);
            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.NORMAL, true);
        }
    }

    /**
     * Test case for binary set object for null values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarysetObjectNull() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(true);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.SETOBJECT_NULL, true);
            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.SETOBJECT_NULL, true);
        }
    }

    /**
     * Test case for binary set object for jdbc type binary values
     * 
     * @throws SQLException
     */
    @Test
    public void testBinarySetObjectWithJDBCTypes() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(false);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, true);
            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, true);
        }
    }

    /**
     * Test case for date set date for date values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSpecificSetter() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.NORMAL, true);
            testDates(stmt, cekAkv, dateTable, values, TestCase.NORMAL, true);
        }
    }

    /**
     * Test case for date set date for date values using windows certificate store
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSpecificSetterWindows() throws SQLException {
        org.junit.Assume.assumeTrue(System.getProperty("os.name").startsWith("Windows"));

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekWin, dateTable, values, TestCase.NORMAL, true);
        }
    }

    /**
     * Test case for date set object for date values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSetObject() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT, true);
            testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT, true);
        }
    }

    /**
     * Test case for date set object for java date values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSetObjectWithJavaType() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT_WITH_JAVATYPES, true);
            testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT_WITH_JAVATYPES, true);
        }
    }

    /**
     * Test case for date set object for jdbc date values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSetObjectWithJDBCType() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, true);
            testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, true);
        }
    }

    /**
     * Test case for date set date for min/max date values
     * 
     * @throws SQLException
     */
    @Test
    public void testDateSpecificSetterMinMaxValue() throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            RandomData.returnMinMax = true;
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.NORMAL, true);
            testDates(stmt, cekAkv, dateTable, values, TestCase.NORMAL, true);
        }
    }

    /**
     * Test case for date set date for null values
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

            testDates(stmt, cekJks, dateTable, values, TestCase.NULL, true);
            testDates(stmt, cekAkv, dateTable, values, TestCase.NULL, true);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Test case for date set object for null values
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

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT_NULL, true);
            testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT_NULL, true);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Test case for numeric set numeric for numeric values
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSpecificSetter() throws Exception {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {

            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL, true);
            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL, true);
        }
    }

    /**
     * Test case for numeric set numeric for numeric values using windows certificate store
     * 
     * @throws SQLException
     */
    @Test
    public void testNumericSpecificSetterWindows() throws Exception {
        org.junit.Assume.assumeTrue(System.getProperty("os.name").startsWith("Windows"));

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {

            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekWin, numericTable, values1, values2, TestCase.NORMAL, true);
        }
    }

    /**
     * Test case for numeric set object for numeric values F
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.SETOBJECT, true);
            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.SETOBJECT, true);
        }
    }

    /**
     * Test case for numeric set object for jdbc type numeric values
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.SETOBJECT_WITH_JDBCTYPES, true);
            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.SETOBJECT_WITH_JDBCTYPES, true);
        }
    }

    /**
     * Test case for numeric set numeric for max numeric values
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL, true);
            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL, true);
        }
    }

    /**
     * Test case for numeric set numeric for min numeric values
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL, true);
            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL, true);
        }
    }

    /**
     * Test case for numeric set numeric for null values
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NULL, true);
            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NULL, true);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Test case for numeric set object for null values
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NULL, true);
            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NULL, true);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Test case for numeric set numeric for null normalization values
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL, true);
            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL, true);
        }
    }

    /**
     * Test FMTOnly with Always Encrypted
     * 
     * @throws SQLException
     */
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
}
