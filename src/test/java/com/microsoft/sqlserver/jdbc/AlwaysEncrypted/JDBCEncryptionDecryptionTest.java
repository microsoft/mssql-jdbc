/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IClientCredential;
import com.microsoft.sqlserver.jdbc.SQLServerKeyVaultAuthenticationCallback;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashSet;
import java.util.LinkedList;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;

import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.microsoft.sqlserver.jdbc.RandomData;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionAzureKeyVaultProvider;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionJavaKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

import microsoft.sql.DateTimeOffset;


/**
 * Tests Decryption and encryption of values
 *
 */
@RunWith(Parameterized.class)
@Tag(Constants.xSQLv11)
@Tag(Constants.xSQLv12)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.xAzureSQLDB)
@Tag(Constants.reqExternalSetup)
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
        setAEConnectionString(serverName, url, protocol);

        try {
            SQLServerColumnEncryptionJavaKeyStoreProvider jksp = new SQLServerColumnEncryptionJavaKeyStoreProvider(
                    javaKeyPath, new char[1]);
            String keystoreName = "keystoreName";
            jksp.setName(keystoreName);
            assertTrue(jksp.getName().equals(keystoreName),
                    "JKS name: " + jksp.getName() + " keystoreName: " + keystoreName);
        } catch (SQLServerException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    /*
     * Test getting/setting AKV name
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testAkvName(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        SQLServerColumnEncryptionAzureKeyVaultProvider akv = new SQLServerColumnEncryptionAzureKeyVaultProvider(
                applicationClientID, applicationKey);
        String keystoreName = "keystoreName";
        akv.setName(keystoreName);
        assertTrue(akv.getName().equals(keystoreName), "AKV name: " + akv.getName() + " keystoreName: " + keystoreName);
    }

    /*
     * Test bad Java Key Store
     */
    @SuppressWarnings("unused")
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testBadJks(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try {
            SQLServerColumnEncryptionJavaKeyStoreProvider jksp = new SQLServerColumnEncryptionJavaKeyStoreProvider(null,
                    null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidConnectionSetting")), e.getMessage());
        }
    }

    /*
     * Test bad Azure Key Vault using SQLServerKeyVaultAuthenticationCallback
     */
    @SuppressWarnings("unused")
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testBadAkvCallback(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try {
            SQLServerColumnEncryptionAzureKeyVaultProvider akv = new SQLServerColumnEncryptionAzureKeyVaultProvider(
                    (SQLServerKeyVaultAuthenticationCallback) null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_NullValue")), e.getMessage());
        }
    }

    /*
     * Test bad Azure Key Vault using TokenCredential
     */
    @SuppressWarnings("unused")
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testBadAkvTokenCredential(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try {
            SQLServerColumnEncryptionAzureKeyVaultProvider akv = new SQLServerColumnEncryptionAzureKeyVaultProvider(
                    (TokenCredential) null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_NullValue")), e.getMessage());
        }
    }

    /*
     * Test bad encryptColumnEncryptionKey for JKS
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testJksBadEncryptColumnEncryptionKey(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

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
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidMasterKeyDetails")), e.getMessage());
        }

        // empty cek
        try {
            byte[] emptyCek = new byte[0];
            jksp.encryptColumnEncryptionKey(javaKeyPath, Constants.CEK_ALGORITHM, emptyCek);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_EmptyColumnEncryptionKey")), e.getMessage());
        }
    }

    /*
     * Test bad encryptColumnEncryptionKey for AKV
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testAkvBadEncryptColumnEncryptionKey(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        SQLServerColumnEncryptionAzureKeyVaultProvider akv = null;
        akv = new SQLServerColumnEncryptionAzureKeyVaultProvider(applicationClientID, applicationKey);

        // null encryptedColumnEncryptionKey
        try {
            akv.encryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_NullColumnEncryptionKey")), e.getMessage());
        }

        // empty encryptedColumnEncryptionKey
        try {
            byte[] emptyCek = new byte[0];
            akv.encryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, emptyCek);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_EmptyCEK")), e.getMessage());
        }
    }

    /*
     * Test decryptColumnEncryptionKey for JKS
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testJksDecryptColumnEncryptionKey(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

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
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidMasterKeyDetails")), e.getMessage());
        }

        // bad keystore
        try {
            byte[] emptyCek = new byte[0];
            jksp.decryptColumnEncryptionKey("keypath", "algorithm", emptyCek);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_KeyStoreNotFound")), e.getMessage());
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
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_invalidKeyStoreFile")), e.getMessage());
        }
    }

    /*
     * Test decryptColumnEncryptionKey for AKV
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testAkvDecryptColumnEncryptionKey(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        SQLServerColumnEncryptionAzureKeyVaultProvider akv = null;
        akv = new SQLServerColumnEncryptionAzureKeyVaultProvider(applicationClientID, applicationKey);

        // null akvpath
        try {
            akv.decryptColumnEncryptionKey(null, "", null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_AKVPathNull")), e.getMessage());
        }

        // invalid akvpath
        try {
            akv.decryptColumnEncryptionKey("keypath", "", null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_AKVMasterKeyPathInvalid")), e.getMessage());
        }

        // invalid akvpath url
        try {
            akv.decryptColumnEncryptionKey("http:///^[!#$&-;=?-[]_a-", "", null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_AKVURLInvalid")), e.getMessage());
        }

        // null encryptedColumnEncryptionKey
        try {
            akv.decryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, null);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_NullEncryptedColumnEncryptionKey")),
                    e.getMessage());
        }

        // empty encryptedColumnEncryptionKey
        try {
            byte[] emptyCek = new byte[0];
            akv.decryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, emptyCek);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_EmptyEncryptedColumnEncryptionKey")),
                    e.getMessage());
        }

        // invalid algorithm
        try {
            byte[] badCek = new byte[1];
            akv.decryptColumnEncryptionKey(keyIDs[0], "invalidAlgo", badCek);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidKeyEncryptionAlgorithm")),
                    e.getMessage());
        }

        // bad encryptedColumnEncryptionKey
        try {
            byte[] badCek = new byte[1];
            akv.decryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, badCek);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidEcryptionAlgorithmVersion")),
                    e.getMessage());
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

            testChars(stmt, cekJks, charTable, values, TestCase.NORMAL, false);
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

            testChars(stmt, cekAkv, charTable, values, TestCase.NORMAL, false);
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
        setAEConnectionString(serverName, url, protocol);

        org.junit.Assume.assumeTrue(isWindows);

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(nullable);

            testChars(stmt, cekWin, charTable, values, TestCase.NORMAL, false);
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

            testChars(stmt, cekAkv, charTable, values, TestCase.SETOBJECT, false);
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

            testChars(stmt, cekJks, charTable, values, TestCase.SETOBJECT, false);
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

            testChars(stmt, cekAkv, charTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, false);
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

            testChars(stmt, cekJks, charTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, false);
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

            testChars(stmt, cekAkv, charTable, values, TestCase.NORMAL, false);
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

            testChars(stmt, cekJks, charTable, values, TestCase.NORMAL, false);
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

            testChars(stmt, cekAkv, charTable, values, TestCase.SETOBJECT, false);
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

            testChars(stmt, cekJks, charTable, values, TestCase.SETOBJECT, false);
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

            testChars(stmt, cekAkv, charTable, values, TestCase.NULL, false);
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

            testChars(stmt, cekJks, charTable, values, TestCase.NULL, false);
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

            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.NORMAL, false);
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

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.NORMAL, false);
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

            testBinaries(stmt, cekWin, binaryTable, values, TestCase.NORMAL, false);
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

            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.SETOBJECT, false);
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

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.SETOBJECT, false);
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

            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.NULL, false);
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

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.NULL, false);
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

            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.NORMAL, false);
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

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.NORMAL, false);
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

            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.SETOBJECT_NULL, false);
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

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.SETOBJECT_NULL, false);
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

            testBinaries(stmt, cekAkv, binaryTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, false);
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

            testBinaries(stmt, cekJks, binaryTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, false);
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

            testDates(stmt, cekAkv, dateTable, values, TestCase.NORMAL, false);
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

            testDates(stmt, cekJks, dateTable, values, TestCase.NORMAL, false);
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

            testDates(stmt, cekWin, dateTable, values, TestCase.NORMAL, false);
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

            testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT, false);
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

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT, false);
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

            testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT_WITH_JAVATYPES, false);
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

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT_WITH_JAVATYPES, false);
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

            testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, false);
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

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT_WITH_JDBCTYPES, false);
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

            testDates(stmt, cekAkv, dateTable, values, TestCase.NORMAL, false);
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

            testDates(stmt, cekJks, dateTable, values, TestCase.NORMAL, false);
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

            testDates(stmt, cekAkv, dateTable, values, TestCase.NULL, false);
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

            testDates(stmt, cekJks, dateTable, values, TestCase.NULL, false);
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

            testDates(stmt, cekAkv, dateTable, values, TestCase.SETOBJECT_NULL, false);
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

            testDates(stmt, cekJks, dateTable, values, TestCase.SETOBJECT_NULL, false);
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

            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL, false);
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL, false);
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

            testNumerics(stmt, cekWin, numericTable, values1, values2, TestCase.NORMAL, false);
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

            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.SETOBJECT, false);
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.SETOBJECT, false);
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

            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.SETOBJECT_WITH_JDBCTYPES, false);
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.SETOBJECT_WITH_JDBCTYPES, false);
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

            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL, false);
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL, false);
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

            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL, false);
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL, false);
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

            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NULL, false);
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NULL, false);
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NULL, false);
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

            testNumerics(stmt, cekAkv, numericTable, values1, values2, TestCase.NORMAL, false);
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

            testNumerics(stmt, cekJks, numericTable, values1, values2, TestCase.NORMAL, false);
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
                assertTrue(pmd.getParameterCount() == 48, "parameter count: " + pmd.getParameterCount());
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
                    AECommon.testGetString(rs, numberOfColumns, values);
                    AECommon.testGetObject(rs, numberOfColumns, values);
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

    // not testing this for now.
    @SuppressWarnings("unused")
    protected static void testGetStringForDate(ResultSet rs, int numberOfColumns,
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
                    AECommon.testGetString(rs, numberOfColumns, numericValues);
                    AECommon.testGetObject(rs, numberOfColumns, numericValues);
                    AECommon.testGetBigDecimal(rs, numberOfColumns, numericValues);
                    if (!isNull)
                        AECommon.testWithSpecifiedtype(rs, numberOfColumns, numericValues);
                    else {
                        String[] nullNumericValues = {Boolean.FALSE.toString(), "0", "0", "0", "0", "0.0", "0.0", "0.0",
                                null, null, null, null, null, null, null, null};
                        AECommon.testWithSpecifiedtype(rs, numberOfColumns, nullNumericValues);
                    }
                }
            }
        }
    }

    private void testRichQuery(SQLServerStatement stmt, String tableName, String table[][],
            String[] values) throws SQLException {
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo)) {
            for (int i = 0; i < table.length; i++) {
                String sql = "SELECT * FROM " + tableName + " WHERE " + ColumnType.PLAIN.name() + table[i][0] + "= ?";
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
                        case "GUIDSTRING":
                            pstmt.setUniqueIdentifier(1, Constants.UID);
                            break;
                        case "GUID":
                            pstmt.setObject(1, UUID.fromString(Constants.UID));
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
                            AECommon.testGetString(rs, numberOfColumns, values);
                            AECommon.testGetObject(rs, numberOfColumns, values);
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
                String sql = "SELECT * FROM " + tableName + " WHERE " + ColumnType.PLAIN.name() + table[i][0] + "= ?";
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
                String sql = "SELECT * FROM " + tableName + " WHERE " + ColumnType.PLAIN.name() + table[i][0] + "= ?";
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

    void testChars(SQLServerStatement stmt, String cekName, String[][] table, String[] values, TestCase testCase,
            boolean isTestEnclave) throws SQLException {
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
                populateCharSetObjectNull();
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
            if (null == enclaveAttestationUrl || null == enclaveAttestationProtocol) {
                fail(TestResource.getResource("R_reqExternalSetup"));
            }

            testAlterColumnEncryption(stmt, CHAR_TABLE_AE, table, cekName);
            testRichQuery(stmt, CHAR_TABLE_AE, table, values);
        }
    }

    void testBinaries(SQLServerStatement stmt, String cekName, String[][] table, LinkedList<byte[]> values,
            TestCase testCase, boolean isTestEnclave) throws SQLException {
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

        if (isTestEnclave) {
            if (null == enclaveAttestationUrl || null == enclaveAttestationProtocol) {
                fail(TestResource.getResource("R_reqExternalSetup"));
            }

            testAlterColumnEncryption(stmt, BINARY_TABLE_AE, table, cekName);
            testRichQuery(stmt, BINARY_TABLE_AE, table, values);
        }
    }

    void testDates(SQLServerStatement stmt, String cekName, String[][] table, LinkedList<Object> values,
            TestCase testCase, boolean isTestEnclave) throws SQLException {
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

        if (isTestEnclave) {
            if (null == enclaveAttestationUrl || null == enclaveAttestationProtocol) {
                fail(TestResource.getResource("R_reqExternalSetup"));
            }

            testAlterColumnEncryption(stmt, DATE_TABLE_AE, table, cekName);
            testRichQueryDate(stmt, DATE_TABLE_AE, table, values);
        }
    }

    void testNumerics(SQLServerStatement stmt, String cekName, String[][] table, String[] values1, String[] values2,
            TestCase testCase, boolean isTestEnclave) throws SQLException {
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

        if (isTestEnclave) {
            if (null == enclaveAttestationUrl || null == enclaveAttestationProtocol) {
                fail(TestResource.getResource("R_reqExternalSetup"));
            }

            testAlterColumnEncryption(stmt, NUMERIC_TABLE_AE, table, cekName);
            testRichQuery(stmt, NUMERIC_TABLE_AE, table, values1);
            testRichQuery(stmt, NUMERIC_TABLE_AE, table, values2);
        }
    }

    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testAkvNameWithAuthCallback(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        try {
            SQLServerColumnEncryptionAzureKeyVaultProvider akv = new SQLServerColumnEncryptionAzureKeyVaultProvider(
                    authenticationCallback);
            String keystoreName = "keystoreName";
            akv.setName(keystoreName);
            assertTrue(akv.getName().equals(keystoreName),
                    "AKV name: " + akv.getName() + " keystoreName: " + keystoreName);
        } catch (SQLServerException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testAkvNameWithTokenCredential(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        ClientSecretCredential credential = new ClientSecretCredentialBuilder().tenantId(tenantID)
                .clientId(applicationClientID).clientSecret(applicationKey).build();

        try {
            SQLServerColumnEncryptionAzureKeyVaultProvider akv = new SQLServerColumnEncryptionAzureKeyVaultProvider(
                    credential);
            String keystoreName = "keystoreName";
            akv.setName(keystoreName);
            assertTrue(akv.getName().equals(keystoreName),
                    "AKV name: " + akv.getName() + " keystoreName: " + keystoreName);
        } catch (SQLServerException e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    @ParameterizedTest
    @MethodSource("enclaveParams")
    @Tag(Constants.reqExternalSetup)
    public void testAkvBadEncryptColumnEncryptionKeyWithAuthCallback(String serverName, String url,
            String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

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
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_NullColumnEncryptionKey")), e.getMessage());
        }
        // empty encryptedColumnEncryptionKey
        try {
            byte[] emptyCek = new byte[0];
            akv.encryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, emptyCek);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_EmptyCEK")), e.getMessage());
        }
    }

    SQLServerKeyVaultAuthenticationCallback authenticationCallback = new SQLServerKeyVaultAuthenticationCallback() {
        @Override
        public String getAccessToken(String authority, String resource, String scope) {
            try {
                IClientCredential credential = ClientCredentialFactory.createFromSecret(applicationKey);
                ConfidentialClientApplication confidentialClientApplication = ConfidentialClientApplication
                        .builder(applicationClientID, credential).authority(authority).build();
                Set<String> scopes = new HashSet<>();
                scopes.add(scope);
                return confidentialClientApplication.acquireToken(ClientCredentialParameters.builder(scopes).build())
                        .get().accessToken();
            } catch (Exception e) {
                fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
            }
            return null;
        }
    };
}
