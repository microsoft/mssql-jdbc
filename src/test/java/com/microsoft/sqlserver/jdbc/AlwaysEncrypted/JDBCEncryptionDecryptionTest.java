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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.sqlserver.jdbc.EnclavePackageTest;
import com.microsoft.sqlserver.jdbc.RandomData;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionAzureKeyVaultProvider;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionJavaKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerKeyVaultAuthenticationCallback;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.jdbc.AlwaysEncrypted.AESetup.ColumnType;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

import microsoft.sql.DateTimeOffset;


/**
 * Tests Decryption and encryption of values
 *
 */
@RunWith(Parameterized.class)
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

    /*
     * Test getting/setting JKS name
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testJksName(String serverName, String url, String protocol) throws Exception {
        try {
            SQLServerColumnEncryptionJavaKeyStoreProvider jksp = new SQLServerColumnEncryptionJavaKeyStoreProvider(
                    javaKeyPath, new char[1]);
            String keystoreName = "keystoreName";
            jksp.setName(keystoreName);
            assertTrue(jksp.getName().equals(keystoreName));
        } catch (SQLServerException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /*
     * Test getting/setting AKV name
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testAkvName(String serverName, String url, String protocol) throws Exception {
        try {
            SQLServerColumnEncryptionAzureKeyVaultProvider akv = new SQLServerColumnEncryptionAzureKeyVaultProvider(
                    authenticationCallback);
            String keystoreName = "keystoreName";
            akv.setName(keystoreName);
            assertTrue(akv.getName().equals(keystoreName));
        } catch (SQLServerException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /*
     * Test bad Java Key Store
     */
    @SuppressWarnings("unused")
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testBadJks(String serverName, String url, String protocol) throws Exception {
        try {
            SQLServerColumnEncryptionJavaKeyStoreProvider jksp = new SQLServerColumnEncryptionJavaKeyStoreProvider(null,
                    null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidConnectionSetting")));
        }
    }

    /*
     * Test bad Azure Key Vault
     */
    @SuppressWarnings("unused")
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testBadAkv(String serverName, String url, String protocol) throws Exception {
        try {
            SQLServerColumnEncryptionAzureKeyVaultProvider akv = new SQLServerColumnEncryptionAzureKeyVaultProvider(
                    null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_NullValue")));
        }
    }

    /*
     * Test bad encryptColumnEncryptionKey for JKS
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testJksBadEncryptColumnEncryptionKey(String serverName, String url, String protocol) throws Exception {
        SQLServerColumnEncryptionJavaKeyStoreProvider jksp = null;
        char[] secret = new char[1];
        try {
            jksp = new SQLServerColumnEncryptionJavaKeyStoreProvider(javaKeyPath, secret);
        } catch (SQLServerException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }

        // null masterKeyPath
        try {
            jksp.encryptColumnEncryptionKey(null, null, null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidMasterKeyDetails")));
        }

        // empty cek
        try {
            byte[] emptyCek = new byte[0];
            jksp.encryptColumnEncryptionKey(javaKeyPath, Constants.CEK_ALGORITHM, emptyCek);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_EmptyColumnEncryptionKey")));
        }
    }

    /*
     * Test bad encryptColumnEncryptionKey for AKV
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testAkvBadEncryptColumnEncryptionKey(String serverName, String url, String protocol) throws Exception {
        SQLServerColumnEncryptionAzureKeyVaultProvider akv = null;
        try {
            akv = new SQLServerColumnEncryptionAzureKeyVaultProvider(authenticationCallback);
        } catch (SQLServerException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }

        // null encryptedColumnEncryptionKey
        try {
            akv.encryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_NullColumnEncryptionKey")));
        }

        // empty encryptedColumnEncryptionKey
        try {
            byte[] emptyCek = new byte[0];
            akv.encryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, emptyCek);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_EmptyCEK")));
        }
    }

    /*
     * Test decryptColumnEncryptionKey for JKS
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testJksDecryptColumnEncryptionKey(String serverName, String url, String protocol) throws Exception {
        SQLServerColumnEncryptionJavaKeyStoreProvider jksp = null;
        char[] secret = new char[1];
        try {
            jksp = new SQLServerColumnEncryptionJavaKeyStoreProvider("badkeypath", secret);
        } catch (SQLServerException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }

        // null masterKeyPath
        try {
            jksp.decryptColumnEncryptionKey(null, null, null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidMasterKeyDetails")));
        }

        // bad keystore
        try {
            byte[] emptyCek = new byte[0];
            jksp.decryptColumnEncryptionKey("keypath", "algorithm", emptyCek);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_KeyStoreNotFound")));
        }

        try {
            jksp = new SQLServerColumnEncryptionJavaKeyStoreProvider(javaKeyPath, secret);
        } catch (SQLServerException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }

        // bad cert
        try {
            byte[] badCek = new byte[1];
            jksp.decryptColumnEncryptionKey(javaKeyAliases, "RSA_OAEP", badCek);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_invalidKeyStoreFile")));
        }
    }

    /*
     * Test decryptColumnEncryptionKey for AKV
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testAkvDecryptColumnEncryptionKey(String serverName, String url, String protocol) throws Exception {
        SQLServerColumnEncryptionAzureKeyVaultProvider akv = null;
        try {
            akv = new SQLServerColumnEncryptionAzureKeyVaultProvider(authenticationCallback);
        } catch (SQLServerException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }

        // null akvpath
        try {
            akv.decryptColumnEncryptionKey(null, "", null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_AKVPathNull")));
        }

        // invalid akvpath
        try {
            akv.decryptColumnEncryptionKey("keypath", "", null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_AKVMasterKeyPathInvalid")));
        }

        // invalid akvpath url
        try {
            akv.decryptColumnEncryptionKey("http:///^[!#$&-;=?-[]_a-", "", null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_AKVURLInvalid")));
        }

        // null encryptedColumnEncryptionKey
        try {
            akv.decryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_NullEncryptedColumnEncryptionKey")));
        }

        // empty encryptedColumnEncryptionKey
        try {
            byte[] emptyCek = new byte[0];
            akv.decryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, emptyCek);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_EmptyEncryptedColumnEncryptionKey")));
        }

        // invalid algorithm
        try {
            byte[] badCek = new byte[1];
            akv.decryptColumnEncryptionKey(keyIDs[0], "invalidAlgo", badCek);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidKeyEncryptionAlgorithm")));
        }

        // bad encryptedColumnEncryptionKey
        try {
            byte[] badCek = new byte[1];
            akv.decryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, badCek);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidEcryptionAlgorithmVersion")));
        }
    }

    /**
     * Junit test case for char set string for string values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testCharSpecificSetter(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekJks, charTable, values, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for char set string for string values for AKV
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testCharSpecificSetterAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekAkv, charTable, values, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for char set string for string values using windows certificate store
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testCharSpecificSetterWindows(String serverName, String url, String protocol) throws Exception {
        org.junit.Assume.assumeTrue(isWindows);

        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekWin, charTable, values, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for char set object for string values using AKV
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testCharSetObjectAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekAkv, charTable, values, TestCase.SETOBJECT);
        }
    }

    /**
     * Junit test case for char set object for string values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testCharSetObject(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekJks, charTable, values, TestCase.SETOBJECT);
        }
    }

    /**
     * Junit test case for char set object for jdbc string values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testCharSetObjectWithJDBCTypesAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekAkv, charTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES);
        }
    }

    /**
     * Junit test case for char set object for jdbc string values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testCharSetObjectWithJDBCTypes(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekJks, charTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES);
        }
    }

    /**
     * Junit test case for char set string for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testCharSpecificSetterNullAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = {null, null, null, null, null, null, null, null, null};

            testChars(stmt, cekAkv, charTable, values, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for char set string for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testCharSpecificSetterNull(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = {null, null, null, null, null, null, null, null, null};

            testChars(stmt, cekJks, charTable, values, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for char set object for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testCharSetObjectNullAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = {null, null, null, null, null, null, null, null, null};

            testChars(stmt, cekAkv, charTable, values, TestCase.SETOBJECT);
        }
    }

    /**
     * Junit test case for char set object for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testCharSetObjectNull(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = {null, null, null, null, null, null, null, null, null};

            testChars(stmt, cekJks, charTable, values, TestCase.SETOBJECT);
        }
    }

    /**
     * Junit test case for char set null for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testCharSetNullAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = {null, null, null, null, null, null, null, null, null};

            testChars(stmt, cekAkv, charTable, values, TestCase.NULL);
        }
    }

    /**
     * Junit test case for char set null for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testCharSetNull(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = {null, null, null, null, null, null, null, null, null};

            testChars(stmt, cekJks, charTable, values, TestCase.NULL);
        }
    }

    /**
     * Junit test case for binary set binary for binary values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testBinarySpecificSetterAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(false);

            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for binary set binary for binary values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testBinarySpecificSetter(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(false);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for binary set binary for binary values using windows certificate store
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testBinarySpecificSetterWindows(String serverName, String url, String protocol) throws Exception {
        org.junit.Assume.assumeTrue(isWindows);

        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(false);

            testBinaries(stmt, cekWin, binaryTable, values, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for binary set object for binary values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testBinarySetobjectAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(false);

            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.SETOBJECT);
        }
    }

    /**
     * Junit test case for binary set object for binary values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testBinarySetobject(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(false);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.SETOBJECT);
        }
    }

    /**
     * Junit test case for binary set null for binary values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testBinarySetNullAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(true);

            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.NULL);
        }
    }

    /**
     * Junit test case for binary set null for binary values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testBinarySetNull(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(true);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.NULL);
        }
    }

    /**
     * Junit test case for binary set binary for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testBinarySpecificSetterNullAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(true);

            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for binary set binary for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testBinarySpecificSetterNull(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(true);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for binary set object for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testBinarysetObjectNullAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(true);

            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.SETOBJECT_NULL);
        }
    }

    /**
     * Junit test case for binary set object for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testBinarysetObjectNull(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(true);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.SETOBJECT_NULL);
        }
    }

    /**
     * Junit test case for binary set object for jdbc type binary values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testBinarySetObjectWithJDBCTypesAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(false);

            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES);
        }
    }

    /**
     * Junit test case for binary set object for jdbc type binary values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testBinarySetObjectWithJDBCTypes(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<byte[]> values = createBinaryValues(false);

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES);
        }
    }

    /**
     * Junit test case for date set date for date values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testDateSpecificSetterAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekAkv, dateTable, values, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for date set date for date values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testDateSpecificSetter(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for date set date for date values using windows certificate store
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testDateSpecificSetterWindows(String serverName, String url, String protocol) throws Exception {
        org.junit.Assume.assumeTrue(isWindows);

        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekWin, dateTable, values, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for date set object for date values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testDateSetObjectAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT);
        }
    }

    /**
     * Junit test case for date set object for date values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testDateSetObject(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT);
        }
    }

    /**
     * Junit test case for date set object for java date values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testDateSetObjectWithJavaTypeAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT_WITH_JAVATYPES);
        }
    }

    /**
     * Junit test case for date set object for java date values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testDateSetObjectWithJavaType(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT_WITH_JAVATYPES);
        }
    }

    /**
     * Junit test case for date set object for jdbc date values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testDateSetObjectWithJDBCTypeAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES);
        }
    }

    /**
     * Junit test case for date set object for jdbc date values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testDateSetObjectWithJDBCType(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES);
        }
    }

    /**
     * Junit test case for date set date for min/max date values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testDateSpecificSetterMinMaxValueAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            RandomData.returnMinMax = true;
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekAkv, dateTable, values, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for date set date for min/max date values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testDateSpecificSetterMinMaxValue(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            RandomData.returnMinMax = true;
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for date set date for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testDateSetNullAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            RandomData.returnNull = true;
            nullable = true;
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekAkv, dateTable, values, TestCase.NULL);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Junit test case for date set date for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testDateSetNull(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            RandomData.returnNull = true;
            nullable = true;
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.NULL);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Junit test case for date set object for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testDateSetObjectNullAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        RandomData.returnNull = true;
        nullable = true;

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT_NULL);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Junit test case for date set object for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testDateSetObjectNull(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        RandomData.returnNull = true;
        nullable = true;

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            LinkedList<Object> values = createTemporalTypes(nullable);

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT_NULL);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Junit test case for numeric set numeric for numeric values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testNumericSpecificSetterAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {

            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for numeric set numeric for numeric values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testNumericSpecificSetter(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {

            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for numeric set numeric for numeric values using windows certificate store
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testNumericSpecificSetterWindows(String serverName, String url, String protocol) throws Exception {
        org.junit.Assume.assumeTrue(isWindows);

        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {

            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekWin, numericTable, values1, values2, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for numeric set object for numeric values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testNumericSetObjectAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.SETOBJECT);
        }
    }

    /**
     * Junit test case for numeric set object for numeric values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testNumericSetObject(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.SETOBJECT);
        }
    }

    /**
     * Junit test case for numeric set object for jdbc type numeric values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testNumericSetObjectWithJDBCTypesAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.SETOBJECT_WITH_JDBCTYPES);
        }
    }

    /**
     * Junit test case for numeric set object for jdbc type numeric values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testNumericSetObjectWithJDBCTypes(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.SETOBJECT_WITH_JDBCTYPES);
        }
    }

    /**
     * Junit test case for numeric set numeric for max numeric values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testNumericSpecificSetterMaxValueAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

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

            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for numeric set numeric for max numeric values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testNumericSpecificSetterMaxValue(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

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
        }
    }

    /**
     * Junit test case for numeric set numeric for min numeric values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testNumericSpecificSetterMinValueAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

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

            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for numeric set numeric for min numeric values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testNumericSpecificSetterMinValue(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

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
        }
    }

    /**
     * Junit test case for numeric set numeric for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testNumericSpecificSetterNullAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            nullable = true;
            RandomData.returnNull = true;
            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NULL);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Junit test case for numeric set numeric for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testNumericSpecificSetterNull(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            nullable = true;
            RandomData.returnNull = true;
            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NULL);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Junit test case for numeric set object for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testNumericSpecificSetterSetObjectNullAkv(String serverName, String url,
            String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            nullable = true;
            RandomData.returnNull = true;
            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NULL);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Junit test case for numeric set object for null values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testNumericSpecificSetterSetObjectNull(String serverName, String url,
            String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            nullable = true;
            RandomData.returnNull = true;
            String[] values1 = createNumericValues(nullable);
            String[] values2 = new String[values1.length];
            System.arraycopy(values1, 0, values2, 0, values1.length);

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NULL);
        }

        nullable = false;
        RandomData.returnNull = false;
    }

    /**
     * Junit test case for numeric set numeric for null normalization values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testNumericNormalizationAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values1 = {Boolean.TRUE.toString(), "1", "127", "100", "100", "1.123", "1.123", "1.123",
                    "123456789123456789", "12345.12345", "987654321123456789", "567812.78", "7812.7812", "7812.7812",
                    "999999999999999999999999.9999", "999999999999999999999999.9999"};
            String[] values2 = {Boolean.TRUE.toString(), "1", "127", "100", "100", "1.123", "1.123", "1.123",
                    "123456789123456789", "12345.12345", "987654321123456789", "567812.78", "7812.7812", "7812.7812",
                    "999999999999999999999999.9999", "999999999999999999999999.9999"};

            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for numeric set numeric for null normalization values
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testNumericNormalization(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values1 = {Boolean.TRUE.toString(), "1", "127", "100", "100", "1.123", "1.123", "1.123",
                    "123456789123456789", "12345.12345", "987654321123456789", "567812.78", "7812.7812", "7812.7812",
                    "999999999999999999999999.9999", "999999999999999999999999.9999"};
            String[] values2 = {Boolean.TRUE.toString(), "1", "127", "100", "100", "1.123", "1.123", "1.123",
                    "123456789123456789", "12345.12345", "987654321123456789", "567812.78", "7812.7812", "7812.7812",
                    "999999999999999999999999.9999", "999999999999999999999999.9999"};

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL);
        }
    }

    /**
     * Junit test case for testing FMTOnly
     * 
     * @param serverName
     * @param url
     * @param protocol
     * @throws Exception
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testAEFMTOnly(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try (SQLServerConnection c = PrepUtil.getConnection(AETestConnectionString + ";useFmtOnly=true", AEInfo);
                Statement s = c.createStatement()) {
            createTable(NUMERIC_TABLE_AE, cekJks, numericTable);
            String sql = "insert into " + NUMERIC_TABLE_AE + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
                    + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
                    + "?,?,?," + "?,?,?," + "?,?,?" + ")";
            try (PreparedStatement p = c.prepareStatement(sql)) {
                ParameterMetaData pmd = p.getParameterMetaData();
                assertTrue(pmd.getParameterCount() == 48);
            }
        }
    }

    /*
     * Negative Test - AEv2 not supported
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testAEv2NotSupported(String serverName, String url, String protocol) throws Exception {
        org.junit.Assume.assumeTrue(null == url || null == protocol);
        EnclavePackageTest.testAEv2NotSupported(serverName, url, protocol);
    }

    /*
     * Negative Test = AEv2 not enabled
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testAEv2Disabled(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        // connection string w/o AEv2
        String testConnectionString = TestUtils.removeProperty(AETestConnectionString,
                Constants.ENCLAVE_ATTESTATIONURL);
        testConnectionString = TestUtils.removeProperty(testConnectionString, Constants.ENCLAVE_ATTESTATIONPROTOCOL);

        try (SQLServerConnection con = PrepUtil.getConnection(testConnectionString);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);
            TestUtils.dropTableIfExists(CHAR_TABLE_AE, stmt);
            createTable(CHAR_TABLE_AE, cekJks, charTable);
            populateCharNormalCase(values);
            testAlterColumnEncryption(stmt, CHAR_TABLE_AE, charTable, cekJks);
        } catch (Throwable e) {
            // testChars called fail()
            assertTrue(e.getMessage().contains(TestResource.getResource("R_AlterAEv2Error")));
        } finally {
            try (SQLServerConnection con = PrepUtil.getConnection(testConnectionString);
                    SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
                TestUtils.dropTableIfExists(CHAR_TABLE_AE, stmt);
            }
        }
    }

    void testChar(SQLServerStatement stmt, String[] values) throws SQLException {
        String sql = "select * from " + CHAR_TABLE_AE;

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
        String sql = "select * from " + BINARY_TABLE_AE;

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
        String sql = "select * from " + DATE_TABLE_AE;

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
        String sql = "select * from " + NUMERIC_TABLE_AE;

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

    void testChars(SQLServerStatement stmt, String cekName, String[][] table, String[] values,
            TestCase testCase) throws SQLException {
        TestUtils.dropTableIfExists(CHAR_TABLE_AE, stmt);
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
    }

    void testBinaries(SQLServerStatement stmt, String cekName, String[][] table, LinkedList<byte[]> values,
            TestCase testCase) throws SQLException {
        TestUtils.dropTableIfExists(BINARY_TABLE_AE, stmt);
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
    }

    void testDates(SQLServerStatement stmt, String cekName, String[][] table, LinkedList<Object> values,
            TestCase testCase) throws SQLException {
        TestUtils.dropTableIfExists(DATE_TABLE_AE, stmt);
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
    }

    void testNumerics(SQLServerStatement stmt, String cekName, String[][] table, String[] values1, String[] values2,
            TestCase testCase) throws SQLException {
        TestUtils.dropTableIfExists(NUMERIC_TABLE_AE, stmt);
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
                String sql = "ALTER TABLE " + tableName + " ALTER COLUMN " + ColumnType.DETERMINISTIC.name()
                        + table[i][0] + " " + table[i][1]
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

    SQLServerKeyVaultAuthenticationCallback authenticationCallback = new SQLServerKeyVaultAuthenticationCallback() {
        // @Override
        ExecutorService service = Executors.newFixedThreadPool(2);

        public String getAccessToken(String authority, String resource, String scope) {

            AuthenticationResult result = null;
            try {
                AuthenticationContext context = new AuthenticationContext(authority, false, service);
                ClientCredential cred = new ClientCredential(applicationClientID, applicationKey);
                Future<AuthenticationResult> future = context.acquireToken(resource, cred, null);
                result = future.get();
            } catch (Exception e) {
                fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
            }
            return result.getAccessToken();
        }
    };
}
