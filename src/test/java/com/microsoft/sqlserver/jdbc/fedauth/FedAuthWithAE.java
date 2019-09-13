/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.aad.adal4j.ClientCredential;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionAzureKeyVaultProvider;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionJavaKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerKeyVaultAuthenticationCallback;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class FedAuthWithAE extends AbstractTest {
    private static String charTablePrefix = "FedAuthWithAECharTable_", charTableOld, charTableNew, charTableJKS;
    private static String cmkName1Prefix = "CMK1_", cmkName1;
    private static String cmkName2Prefix = "CMK2_", cmkName2;
    private static String cmkName3Prefix = "CMK3_", cmkName3;
    private static String cekNamePrefix = "CEK_FedAuthWithAE", cekName;
    private static String team = "_jdbc_";
    static SQLServerDataSource ds = null;

    @BeforeAll
    public static void setupTests() throws Throwable {
        getFedauthInfo();
        if (null == ds) {
            ds = FedauthTest.getDataSource();
            @SuppressWarnings("deprecation")
            Long n1 = new Long((long) (Math.random() * Math.pow(10, 10)));
            charTableOld = charTablePrefix + team + "_Old_" + n1.toString();
            charTableNew = charTablePrefix + team + "_New_" + n1.toString();
            charTableJKS = charTablePrefix + team + "_JKS_" + n1.toString();
            cmkName1 = cmkName1Prefix + team + n1.toString();
            cmkName2 = cmkName2Prefix + team + n1.toString();
            cmkName3 = cmkName3Prefix + team + n1.toString();
            cekName = cekNamePrefix + team + n1.toString();
        }
    }

    @Test
    public void testFedAuthWithAE_JKS() throws SQLException {
        try (Connection connection = ds.getConnection(); Statement stmt = connection.createStatement()) {

            TestUtils.dropTableIfExists(charTableJKS, stmt);
            dropCEK(stmt);
            dropCMK(stmt, cmkName1);

            setupCMK_JKS(cmkName1, stmt);
            createCEK(cmkName1, setupKeyStoreProvider_JKS(), stmt, javaKeyAliases[0]);
            createCharTable(stmt, charTableJKS);

            String[] charValues = {"hello world!!!", "hello world!!!", "hello world!!!", "你好啊~~~", "你好啊~~~", "你好啊~~~"};
            populateCharNormalCase(charValues, connection, charTableJKS);
            testChar(charValues, stmt, charTableJKS);

        } catch (SQLServerException e) {
            fail(e.getMessage());
        } finally {
            if (null != ds) {
                try (Connection connection = ds.getConnection(); Statement stmt = connection.createStatement()) {
                    TestUtils.dropTableIfExists(charTableJKS, stmt);
                    dropCEK(stmt);
                    dropCMK(stmt, cmkName1);
                    dropCMK(stmt, cmkName2);
                    dropCMK(stmt, cmkName3);
                }
            }
        }
    }

    @Test
    public void testFedAuthWithAE_AKV() throws SQLException {
        String[] charValues = {"hello world!!!", "hello world!!!", "hello world!!!", "你好啊~~~", "你好啊~~~", "你好啊~~~"};
        try (Connection connection = ds.getConnection(); Statement stmt = connection.createStatement()) {
            callDbccFreeProcCache();

            TestUtils.dropTableIfExists(charTableNew, stmt);
            dropCEK(stmt);
            dropCMK(stmt, cmkName2);
            setupCMK_AKVNew(cmkName2, stmt);

            createCEK(cmkName2, setupKeyStoreProvider_AKVNew(), stmt, keyIds[0]);
            createCharTable(stmt, charTableNew);

            populateCharNormalCase(charValues, connection, charTableNew);
            testChar(charValues, stmt, charTableNew);

            TestUtils.dropTableIfExists(charTableNew, stmt);
            dropCEK(stmt);
            dropCMK(stmt, cmkName2);

            callDbccFreeProcCache();

            dropCMK(stmt, cmkName3);
            setupCMK_AKVOld(cmkName3, stmt);

            createCEK(cmkName3, setupKeyStoreProvider_AKVOld(), stmt, keyIds[0]);
            createCharTable(stmt, charTableOld);

            populateCharNormalCase(charValues, connection, charTableOld);
            testChar(charValues, stmt, charTableOld);

        } catch (SQLServerException e) {
            fail(e.getMessage());
        } finally {
            if (null != ds) {
                try (Connection connection = ds.getConnection(); Statement stmt = connection.createStatement()) {
                    TestUtils.dropTableIfExists(charTableOld, stmt);
                    TestUtils.dropTableIfExists(charTableNew, stmt);
                    dropCEK(stmt);
                    dropCMK(stmt, cmkName1);
                    dropCMK(stmt, cmkName2);
                    dropCMK(stmt, cmkName3);
                }
            }
        }
    }

    private void testChar(String[] values, Statement stmt, String charTable) throws SQLException {
        try (ResultSet rs = stmt.executeQuery("select * from [" + charTable + "]")) {
            int numberOfColumns = rs.getMetaData().getColumnCount();
            while (rs.next()) {
                testGetString(rs, numberOfColumns, values);
            }
        }
    }

    private void testGetString(ResultSet rs, int numberOfColumns, String[] values) throws SQLException {
        int index = 0;
        for (int i = 1; i <= numberOfColumns; i = i + 3) {
            String stringValue1 = ("" + rs.getString(i)).trim();
            String stringValue2 = ("" + rs.getString(i + 1)).trim();
            String stringValue3 = ("" + rs.getString(i + 2)).trim();
            try {
                assertTrue(
                        "stringValue1:" + stringValue1 + " stringValue2: " + stringValue2 + " stringValue2: "
                                + stringValue2,
                        stringValue1.equalsIgnoreCase("" + values[index])
                                && stringValue2.equalsIgnoreCase("" + values[index])
                                && stringValue3.equalsIgnoreCase("" + values[index]));
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                index++;
            }
        }
    }

    private void populateCharNormalCase(String[] charValues, Connection connection,
            String charTable) throws SQLException {
        String sql = "insert into [" + charTable + "] values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
                + "?,?,?" + ")";
        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {

            // char
            for (int i = 1; i <= 3; i++) {
                pstmt.setString(i, charValues[0]);
            }

            // varchar
            for (int i = 4; i <= 6; i++) {
                pstmt.setString(i, charValues[1]);
            }

            // varchar(max)
            for (int i = 7; i <= 9; i++) {
                pstmt.setString(i, charValues[2]);
            }

            // nchar
            for (int i = 10; i <= 12; i++) {
                pstmt.setNString(i, charValues[3]);
            }

            // nvarchar
            for (int i = 13; i <= 15; i++) {
                pstmt.setNString(i, charValues[4]);
            }

            // varchar(max)
            for (int i = 16; i <= 18; i++) {
                pstmt.setNString(i, charValues[5]);
            }

            pstmt.execute();
        }
    }

    private void createCharTable(Statement stmt, String charTable) throws SQLException {
        String sql = "create table [" + charTable + "] (" + "PlainChar char(20) NULL,"
                + "RandomizedChar char(20) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicChar char(20) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + "PlainVarchar varchar(50) NULL,"
                + "RandomizedVarchar varchar(50) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicVarchar varchar(50) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + "PlainVarcharMax varchar(max) NULL,"
                + "RandomizedVarcharMax varchar(max) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicVarcharMax varchar(max) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + "PlainNchar nchar(30) NULL,"
                + "RandomizedNchar nchar(30) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicNchar nchar(30) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + "PlainNvarchar nvarchar(60) NULL,"
                + "RandomizedNvarchar nvarchar(60) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicNvarchar nvarchar(60) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"

                + "PlainNvarcharMax nvarchar(max) NULL,"
                + "RandomizedNvarcharMax nvarchar(max) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicNvarcharMax nvarchar(max) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL" + ");";

        try {
            stmt.execute(sql);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private void createCEK(String cmkName, SQLServerColumnEncryptionKeyStoreProvider storeProvider, Statement stmt,
            String encryptionKey) throws SQLServerException, SQLException {
        String letters = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        byte[] valuesDefault = letters.getBytes();
        byte[] key = storeProvider.encryptColumnEncryptionKey(encryptionKey, "RSA_OAEP", valuesDefault);
        String cekSql = " if not exists (SELECT name from sys.column_encryption_keys where name='" + cekName + "')"
                + " begin" + " CREATE COLUMN ENCRYPTION KEY " + cekName + " WITH VALUES " + "(COLUMN_MASTER_KEY = "
                + cmkName + ", ALGORITHM = 'RSA_OAEP', ENCRYPTED_VALUE = 0x" + bytesToHexString(key, key.length) + ")"
                + ";" + " end";
        stmt.execute(cekSql);
    }

    private void dropCEK(Statement stmt) throws SQLServerException, SQLException {
        String cekSql = " if exists (SELECT name from sys.column_encryption_keys where name='" + cekName + "')"
                + " begin" + " drop column encryption key " + cekName + " end";
        stmt.execute(cekSql);
    }

    private void dropCMK(Statement stmt, String cmkName) throws SQLServerException, SQLException {
        String cekSql = " if exists (SELECT name from sys.column_master_keys where name='" + cmkName + "')" + " begin"
                + " drop column master key " + cmkName + " end";
        stmt.execute(cekSql);
    }

    private void setupCMK_JKS(String cmkName, Statement stmt) throws SQLException {
        createCMK(cmkName, "MSSQL_JAVA_KEYSTORE", FedauthTest.javaKeyAliases[0], stmt);
    }

    private void setupCMK_AKVOld(String cmkName, Statement stmt) throws SQLException {
        createCMK(cmkName, "AZURE_KEY_VAULT", keyIds[0], stmt);
    }

    private void setupCMK_AKVNew(String cmkName, Statement stmt) throws SQLException {
        createCMK(cmkName, "AZURE_KEY_VAULT", keyIds[0], stmt);
    }

    private SQLServerColumnEncryptionKeyStoreProvider setupKeyStoreProvider_JKS() throws SQLException {
        return new SQLServerColumnEncryptionJavaKeyStoreProvider(jksPaths[0], secretstrJks.toCharArray());
    }

    private SQLServerColumnEncryptionKeyStoreProvider setupKeyStoreProvider_AKVNew() throws SQLServerException {
        return registerAKVProvider(
                new SQLServerColumnEncryptionAzureKeyVaultProvider(FedauthTest.applicationClientId, applicationKey));
    }

    private SQLServerColumnEncryptionKeyStoreProvider setupKeyStoreProvider_AKVOld() throws SQLServerException {
        ExecutorService service = Executors.newFixedThreadPool(2);
        SQLServerKeyVaultAuthenticationCallback authenticationCallback = new SQLServerKeyVaultAuthenticationCallback() {
            @Override
            public String getAccessToken(String authority, String resource, String scope) {
                AuthenticationResult result = null;
                try {
                    AuthenticationContext context = new AuthenticationContext(authority, false, service);
                    ClientCredential cred = new ClientCredential(applicationClientId, applicationKey);

                    Future<AuthenticationResult> future = context.acquireToken(resource, cred, null);
                    result = future.get();
                    return result.getAccessToken();
                } catch (Exception e) {
                    fail(e.getMessage());
                    return null;
                }
            }
        };
        return new SQLServerColumnEncryptionAzureKeyVaultProvider(authenticationCallback);
    }

    private SQLServerColumnEncryptionKeyStoreProvider registerAKVProvider(
            SQLServerColumnEncryptionKeyStoreProvider provider) throws SQLServerException {
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> map1 = new HashMap<String, SQLServerColumnEncryptionKeyStoreProvider>();
        map1.put(provider.getName(), provider);
        SQLServerConnection.registerColumnEncryptionKeyStoreProviders(map1);
        return provider;
    }

    private void createCMK(String cmkName, String keyStoreName, String keyPath, Statement stmt) throws SQLException {
        try {
            String sql = " if not exists (SELECT name from sys.column_master_keys where name='" + cmkName + "')"
                    + " begin" + " CREATE COLUMN MASTER KEY " + cmkName + " WITH (KEY_STORE_PROVIDER_NAME = '"
                    + keyStoreName + "', KEY_PATH = '" + keyPath + "')" + " end";
            stmt.execute(sql);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private void callDbccFreeProcCache() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString);
                Statement stmt = connection.createStatement()) {
            stmt.execute("DBCC FREEPROCCACHE");
        }
    }

    final static char[] hexChars = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    private String bytesToHexString(byte[] b, int length) {
        StringBuilder sb = new StringBuilder(length * 2);
        for (int i = 0; i < length; i++) {
            int hexVal = b[i] & 0xFF;
            sb.append(hexChars[(hexVal & 0xF0) >> 4]);
            sb.append(hexChars[(hexVal & 0x0F)]);
        }
        return sb.toString();
    }

    @AfterAll
    public static void terminate() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(charTableOld, stmt);
            TestUtils.dropTableIfExists(charTableNew, stmt);
            TestUtils.dropTableIfExists(charTableJKS, stmt);
        }
    }
}
