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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionAzureKeyVaultProvider;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionJavaKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.fedAuth)
public class FedauthWithAE extends FedauthCommon {

    static String cmkName1 = Constants.CMK_NAME + "fedauthAE1";
    static String cmkName2 = Constants.CMK_NAME + "fedauthAE2";
    static String cmkName3 = Constants.CMK_NAME + "fedauthAE3";
    static String cekName = Constants.CEK_NAME + "fedauthAE";
    static String charTableJKS = TestUtils
            .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("JDBC_FedAuthAE_jks")));
    static String charTableAKV = TestUtils
            .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("JDBC_FedAuthAE_akv")));

    static SQLServerDataSource ds = new SQLServerDataSource();

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();

        ds.setServerName(azureServer);
        ds.setDatabaseName(azureDatabase);
        ds.setUser(azureUserName);
        ds.setPassword(azurePassword);
        ds.setAuthentication(SqlAuthentication.ActiveDirectoryPassword.toString());
        ds.setColumnEncryptionSetting(Constants.ENABLED);
        ds.setKeyStoreAuthentication(Constants.JAVA_KEY_STORE_SECRET);
        ds.setKeyStoreLocation(fedauthJksPaths[0]);
        ds.setKeyStoreSecret(Constants.JKS_SECRET_STRING);
    }

    @Test
    public void testFedAuthWithAE_JKS() throws SQLException {
        try (Connection connection = ds.getConnection(); Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(charTableJKS, stmt);
            dropCEK(stmt);
            dropCMK(stmt, cmkName1);

            setupCMK_JKS(cmkName1, stmt);
            createCEK(cmkName1, setupKeyStoreProvider_JKS(), stmt, fedauthJavaKeyAliases[0]);

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

            TestUtils.dropTableIfExists(charTableAKV, stmt);
            dropCEK(stmt);
            dropCMK(stmt, cmkName2);
            setupCMK_AKV(cmkName2, stmt);

            createCEK(cmkName2, setupKeyStoreProvider_AKV(), stmt, keyIDs[0]);
            createCharTable(stmt, charTableAKV);

            populateCharNormalCase(charValues, connection, charTableAKV);
            testChar(charValues, stmt, charTableAKV);

            TestUtils.dropTableIfExists(charTableAKV, stmt);
            dropCEK(stmt);
            dropCMK(stmt, cmkName2);

            callDbccFreeProcCache();
        } catch (SQLServerException e) {
            fail(e.getMessage());
        } finally {
            if (null != ds) {
                try (Connection connection = ds.getConnection(); Statement stmt = connection.createStatement()) {
                    TestUtils.dropTableIfExists(charTableAKV, stmt);
                    dropCEK(stmt);
                    dropCMK(stmt, cmkName1);
                    dropCMK(stmt, cmkName2);
                    dropCMK(stmt, cmkName3);
                }
            }
        }
    }

    private void testChar(String[] values, Statement stmt, String charTable) throws SQLException {
        try (ResultSet rs = stmt.executeQuery("select * from " + charTable)) {
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
                assertTrue(stringValue1.equalsIgnoreCase("" + values[index])
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
        String sql = "insert into " + charTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
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
        String sql = "create table " + charTable + " (" + "PlainChar char(20) NULL,"
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
        createCMK(cmkName, Constants.JAVA_KEY_STORE_NAME, fedauthJavaKeyAliases[0], stmt);
    }

    private void setupCMK_AKV(String cmkName, Statement stmt) throws SQLException {
        createCMK(cmkName, Constants.AZURE_KEY_VAULT_NAME, keyIDs[0], stmt);
    }

    private SQLServerColumnEncryptionKeyStoreProvider setupKeyStoreProvider_JKS() throws SQLException {
        return new SQLServerColumnEncryptionJavaKeyStoreProvider(fedauthJksPaths[0],
                Constants.JKS_SECRET_STRING.toCharArray());
    }

    private SQLServerColumnEncryptionKeyStoreProvider setupKeyStoreProvider_AKV() throws SQLServerException {
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
        return registerAKVProvider(
                new SQLServerColumnEncryptionAzureKeyVaultProvider(applicationClientID, applicationKey));
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
        try (Connection connection = DriverManager.getConnection(adPasswordConnectionStr);
                Statement stmt = connection.createStatement()) {
            TestUtils.freeProcCache(stmt);
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
        try (Connection conn = DriverManager.getConnection(adPasswordConnectionStr);
                Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(charTableAKV, stmt);
            TestUtils.dropTableIfExists(charTableJKS, stmt);
        }
    }
}
