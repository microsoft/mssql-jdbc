/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.parametermetadata;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionAzureKeyVaultProvider;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
public class ParameterMetaDataCacheTest extends AbstractTest {
    private static final String firstTableName = "firstTableName";
    private static final String secondTableName = "secondTableName";
    private final String cmkName = "Wibble";
    private final String cekName = "MyColumnKey";

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    public void tableSetup() throws SQLServerException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(firstTableName, stmt);
            TestUtils.dropTableIfExists(secondTableName, stmt);
            dropCEK();
            dropCMK(cmkName);
            createCMK(cmkName, keyIDs[0]);
            createCEK(cmkName, setupKeyStoreProvider(), keyIDs[0]);
            createTable(firstTableName, "firstTableDeterministicColumn", "firstTableRandomizedColumn");
            createTable(secondTableName, "secondTableDeterministicColumn", "secondTableRandomizedColumn");
        } catch (SQLException e) {
            fail(e.getMessage());
        }

    }

    @Test
    public void testParameterMetaDataCacheSecure() throws SQLServerException {
        tableSetup();
        // Insert data into the first
        try {
            populateTable(connection, firstTableName, "firstTableDeterministicColumn", "firstTableRandomizedColumn");
        } catch (SQLException e) {
            fail(e.getMessage());
        }

        // Then we insert data into the second
        // Then we check if the metadata for the first table is visible
    }

    private void createTable(String tableName, String column1, String column2) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("create table " + tableName + " (col1 int identity(1,1) primary key," + column1
                    + " [nvarchar](32) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (" + "COLUMN_ENCRYPTION_KEY="
                    + cekName + ",ENCRYPTION_TYPE=Deterministic," + "ALGORITHM='AEAD_AES_256_CBC_HMAC_SHA_256') NULL,"
                    + column2 + " [nvarchar](32) COLLATE Latin1_General_BIN2 ENCRYPTED WITH ("
                    + "COLUMN_ENCRYPTION_KEY=" + cekName + ",ENCRYPTION_TYPE=Randomized,"
                    + "ALGORITHM='AEAD_AES_256_CBC_HMAC_SHA_256') NULL)");

        }
    }

    private void populateTable(Connection connection, String charTable, String firstColumn,
            String secondColumn) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(
                    "declare @firstColumn nvarchar(32) = 'Test' declare @secondColumn nvarchar(32) = 'Data' insert into "
                            + charTable + "(" + firstColumn + ", " + secondColumn
                            + ") values (@firstColumn, @secondColumn)");
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private void createCMK(String cmkName, String keyPath) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            String sql = " if not exists (SELECT name from sys.column_master_keys where name='" + cmkName + "')"
                    + " begin" + " CREATE COLUMN MASTER KEY " + cmkName + " WITH (KEY_STORE_PROVIDER_NAME = '"
                    + Constants.AZURE_KEY_VAULT_NAME + "', KEY_PATH = '" + keyPath + "')" + " end";
            stmt.execute(sql);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private void createCEK(String cmkName, SQLServerColumnEncryptionKeyStoreProvider storeProvider,
            String encryptionKey) throws SQLServerException, SQLException {
        try (Statement stmt = connection.createStatement()) {
            String letters = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
            byte[] valuesDefault = letters.getBytes();
            byte[] key = storeProvider.encryptColumnEncryptionKey(encryptionKey, "RSA_OAEP", valuesDefault);
            String cekSql = " if not exists (SELECT name from sys.column_encryption_keys where name='" + cekName + "')"
                    + " begin" + " CREATE COLUMN ENCRYPTION KEY " + cekName + " WITH VALUES " + "(COLUMN_MASTER_KEY = "
                    + cmkName + ", ALGORITHM = 'RSA_OAEP', ENCRYPTED_VALUE = 0x" + bytesToHexString(key, key.length)
                    + ")" + ";" + " end";
            stmt.execute(cekSql);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

    }

    private void dropCMK(String cmkName) throws SQLServerException, SQLException {
        try (Statement stmt = connection.createStatement()) {
            String cekSql = " if exists (SELECT name from sys.column_master_keys where name='" + cmkName + "')"
                    + " begin" + " drop column master key " + cmkName + " end";
            stmt.execute(cekSql);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

    }

    private void dropCEK() throws SQLServerException, SQLException {
        try (Statement stmt = connection.createStatement()) {
            String cekSql = " if exists (SELECT name from sys.column_encryption_keys where name='" + cekName + "')"
                    + " begin" + " drop column encryption key " + cekName + " end";
            stmt.execute(cekSql);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

    }

    private SQLServerColumnEncryptionKeyStoreProvider setupKeyStoreProvider() throws SQLServerException {
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
        return registerProvider(
                new SQLServerColumnEncryptionAzureKeyVaultProvider(applicationClientID, applicationKey));
    }

    private SQLServerColumnEncryptionKeyStoreProvider registerProvider(
            SQLServerColumnEncryptionKeyStoreProvider provider) throws SQLServerException {
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> map1 = new HashMap<String, SQLServerColumnEncryptionKeyStoreProvider>();
        map1.put(provider.getName(), provider);
        SQLServerConnection.registerColumnEncryptionKeyStoreProviders(map1);
        return provider;
    }

    private String bytesToHexString(byte[] b, int length) {
        final char[] hexChars = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
        StringBuilder sb = new StringBuilder(length * 2);
        for (int i = 0; i < length; i++) {
            int hexVal = b[i] & 0xFF;
            sb.append(hexChars[(hexVal & 0xF0) >> 4]);
            sb.append(hexChars[(hexVal & 0x0F)]);
        }
        return sb.toString();
    }
}
