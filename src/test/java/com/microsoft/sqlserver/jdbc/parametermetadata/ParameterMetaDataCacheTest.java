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

import com.microsoft.sqlserver.jdbc.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests for caching parameter metadata in sp_describe_parameter_encryption calls
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class ParameterMetaDataCacheTest extends AbstractTest {
    private static final String firstTable = "firstTable";
    private static final String secondTable = "secondTable";
    private final String cmkName = "my_cmk";
    private final String cekName = "my_cek_1";
    private final String cekNameAlt = "my_cek_2";

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    public void tableSetup() throws SQLServerException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(firstTable, stmt);
            TestUtils.dropTableIfExists(secondTable, stmt);

            dropCEK(cekName);
            dropCMK(cmkName);
            createCMK(cmkName, keyIDs[0]);
            createCEK(cekName, cmkName, setupKeyStoreProvider(), keyIDs[0]);

            createTable(cekName, firstTable, "firstColumn", "secondColumn");
            createTable(cekName, secondTable, "firstColumn", "secondColumn");
        } catch (SQLException e) {
            fail(e.getMessage());
        }

    }

    /**
     * 
     * Tests caching of parameter metadata by running a query to be cached, another to replace parameter information,
     * then the first again to measure the difference in time between the two runs.
     * 
     * @throws SQLServerException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testParameterMetaDataCache() throws SQLServerException {
        tableSetup();
        try {
            updateTable(connection, firstTable, "firstColumn", "secondColumn", "'test1'", "'data1'");
            updateTable(connection, secondTable, "firstColumn", "secondColumn", "'test2'", "'data2'");
            
            long firstRun = timedTestSelect(connection, firstTable, "firstColumn", "'test1'");
            selectFromTable(connection, secondTable, "firstColumn", "'test2'");
            long secondRun = timedTestSelect(connection, firstTable, "firstColumn", "'test1'");
            
            // As long as there is a noticible performance improvement, caching is working as intended. For now
            // the threshold measured is 10%.
            double threshold = 0.1;
            assertTrue(1 - (secondRun / firstRun) > threshold);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    /**
     * 
     * Tests that the enclave is retried when using secure enclaves (assuming the server supports this). This is done
     * by executing a query generating metadata in the cache, changing the CEK to make the metadata stale, and running
     * the query again. The query should fail, but retry and pass.
     * 
     * @throws SQLServerException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testRetryWithSecureCache() throws SQLServerException {
        tableSetup();
        try {
            updateTable(connection, firstTable, "firstColumn", "secondColumn", "'test1'", "'data1'");
            selectFromTable(connection, firstTable, "firstColumn", "'test1'");

            
            createCEK(cekNameAlt, cmkName, setupKeyStoreProvider(), keyIDs[0]);
            alterTable(connection, firstTable,"firstColumn", cekNameAlt);
            
            selectFromTable(connection, firstTable, "firstColumn", "'test1'");
            alterTable(connection, firstTable,"firstColumn", cekNameAlt);
            dropCEK(cekNameAlt);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private void createTable(String cekName, String tableName, String column1, String column2) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("create table " + tableName + " (col1 int identity(1,1) primary key," + column1
                    + " [nvarchar](32) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (" + "COLUMN_ENCRYPTION_KEY="
                    + cekName + ",ENCRYPTION_TYPE=Deterministic," + "ALGORITHM='AEAD_AES_256_CBC_HMAC_SHA_256') NULL,"
                    + column2 + " [nvarchar](32) COLLATE Latin1_General_BIN2 ENCRYPTED WITH ("
                    + "COLUMN_ENCRYPTION_KEY=" + cekName + ",ENCRYPTION_TYPE=Randomized,"
                    + "ALGORITHM='AEAD_AES_256_CBC_HMAC_SHA_256') NULL)");
        }
    }

    private void updateTable(Connection connection, String tableName, String firstColumn, String secondColumn,
            String firstData, String secondData) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("declare @firstColumn nvarchar(32) = " + firstData
                    + " declare @secondColumn nvarchar(32) = " + secondData + " insert into " + tableName + "("
                    + firstColumn + ", " + secondColumn + ") values (@firstColumn, @secondColumn)");
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private void selectFromTable(Connection connection, String tableName, String firstColumn,
            String firstData) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(" select * from " + tableName + " where " + firstColumn + " = " + firstData + ";");
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private void alterTable(Connection connection, String tableName, String firstColumn,
            String newCek) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("alter table " + tableName + " modify " + firstColumn + " encrypt with " + newCek);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }
    
    private long timedTestSelect(Connection connection, String tableName, String firstColumn,
            String firstData) throws SQLException {
        long timer = System.currentTimeMillis();
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(" select * from " + tableName + " where " + firstColumn + " = " + firstData + ";");
        } catch (SQLException e) {
            fail(e.getMessage());
        }
        return System.currentTimeMillis() - timer;
    }

    private void createCMK(String cmkName, String keyPath) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            String sql = " if not exists (SELECT name from sys.column_master_keys where name='" + cmkName + "')"
                    + " begin" + " CREATE COLUMN MASTER KEY " + cmkName + " WITH (KEY_STORE_PROVIDER_NAME = '"
                    + Constants.WINDOWS_KEY_STORE_NAME + "', KEY_PATH = '" + keyPath + "')" + " end";
            stmt.execute(sql);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private void createCEK(String cekName, String cmkName, SQLServerColumnEncryptionKeyStoreProvider storeProvider,
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

    private void dropCEK(String cekName) throws SQLServerException, SQLException {
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
