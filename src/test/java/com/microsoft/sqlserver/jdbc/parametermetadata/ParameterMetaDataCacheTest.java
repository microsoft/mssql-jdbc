/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.parametermetadata;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionAzureKeyVaultProvider;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests for caching parameter metadata in sp_describe_parameter_encryption calls
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xSQLv11)
@Tag(Constants.xSQLv12)
@Tag(Constants.xSQLv14)
public class ParameterMetaDataCacheTest extends AbstractTest {
    private final String firstTable = TestUtils.escapeSingleQuotes(
            AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("CacheParamMetaTable1")));
    private final String secondTable = TestUtils.escapeSingleQuotes(
            AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("CacheParamMetaTable2")));
    private final String cmkName = Constants.CMK_NAME + "cacheParameterMetadata1";
    private final String cekName = Constants.CEK_NAME + "cacheParameterMetadata1";
    private final String cekNameAlt = Constants.CEK_NAME + "cacheParameterMetadata2";
    private final String sampleData = "CacheParameterMetadata_testData";
    private final String sampleData2 = "CacheParameterMetadata_testData2";
    private final String columnName = "CacheParameterMetadata_column";

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString, "columnEncryptionSetting", "Enabled");
        setConnection();
    }

    private void tableSetup() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(firstTable, stmt);
            TestUtils.dropTableIfExists(secondTable, stmt);

            dropCEK(cekName);
            dropCMK(cmkName);

            createCMK(cmkName, keyIDs[0]);
            createCEK(cekName, cmkName, setupKeyStoreProvider(), keyIDs[0]);
            createTable(cekName, firstTable, columnName);
            createTable(cekName, secondTable, columnName);
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
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.reqExternalSetup)
    public void testParameterMetaDataCache() throws Exception {
        tableSetup();
        updateTable(firstTable, sampleData);
        updateTable(secondTable, sampleData2);

        long firstRun = timedTestSelect(firstTable, sampleData);
        selectTable(secondTable, columnName, sampleData2);
        long secondRun = timedTestSelect(firstTable, sampleData);

        // As long as there is a noticeable performance improvement, caching is working as intended. For now
        // the threshold measured is 5%.
        double threshold = 0.05;
        assertTrue(1 - (secondRun / firstRun) > threshold);
    }

    /**
     * 
     * Tests that the enclave is retried when using secure enclaves (assuming the server supports this). This is done by
     * executing a query generating metadata in the cache, changing the CEK to make the metadata stale, and running the
     * query again. The query should fail, but retry and pass. Currently disabled as secure enclaves are not supported.
     * 
     * @throws SQLServerException
     */
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.reqExternalSetup)
    @Tag("unused")
    public void testRetryWithSecureCache() throws Exception {
        tableSetup();
        try {
            updateTable(firstTable, sampleData);
            selectTable(firstTable, columnName, sampleData);

            createCEK(cekNameAlt, cmkName, setupKeyStoreProvider(), keyIDs[0]);
            alterTable(firstTable, columnName, cekNameAlt);

            selectTable(firstTable, columnName, sampleData);
            alterTable(firstTable, columnName, cekName);
        } finally {
            dropCEK(cekNameAlt);
        }
    }

    private static void createTable(String cekName, String tableName, String columnName) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("create table " + tableName + " (" + columnName
                    + " varchar(max) COLLATE Latin1_General_BIN2 ENCRYPTED WITH "
                    + "(ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', "
                    + "COLUMN_ENCRYPTION_KEY = " + cekName + ") NULL,);");
        }
    }

    private static void updateTable(String tableName,
            String sampleData) throws SQLTimeoutException, SQLServerException {
        String sql = "insert into " + tableName + " values( ? )";
        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql)) {
            pstmt.setObject(1, sampleData, java.sql.Types.VARCHAR);
            pstmt.execute();
            pstmt.close();
        }
    }

    private static void selectTable(String tableName, String clmnName,
            String sampleData) throws SQLServerException, SQLTimeoutException {
        String sql = "select * from " + tableName + " where " + clmnName + " = ( ? )";
        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql)) {
            pstmt.setObject(1, sampleData, java.sql.Types.VARCHAR);
            pstmt.execute();
            pstmt.close();
        }
    }

    private static void alterTable(String tableName, String clmnName, 
            String newCek) throws SQLTimeoutException, SQLServerException {
        String sql = "alter table " + tableName + " alter column " + clmnName + " encrypt with " + newCek;
        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql)) {
            pstmt.execute();
            pstmt.close();
        }
    }

    private long timedTestSelect(String tableName, String firstData) throws SQLServerException, SQLTimeoutException {
        long timer = System.currentTimeMillis();
        selectTable(tableName, columnName, firstData);
        return System.currentTimeMillis() - timer;
    }

    private void createCMK(String cmk, String keyPath) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = connection.createStatement()) {
            String sql = " if not exists (SELECT name from sys.column_master_keys where name='" + cmkName + "')"
                    + " begin" + " CREATE COLUMN MASTER KEY " + cmk + " WITH (KEY_STORE_PROVIDER_NAME = '"
                    + Constants.AZURE_KEY_VAULT_NAME + "', KEY_PATH = '" + keyPath + "')" + " end";
            stmt.execute(sql);
        }
    }

    private void createCEK(String cek, String cmk, SQLServerColumnEncryptionKeyStoreProvider storeProvider,
            String encryptionKey) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = connection.createStatement()) {
            String letters = Constants.CEK_STRING;
            byte[] valuesDefault = letters.getBytes();
            byte[] key = storeProvider.encryptColumnEncryptionKey(encryptionKey, Constants.CEK_ALGORITHM,
                    valuesDefault);
            String cekSql = " if not exists (SELECT name from sys.column_encryption_keys where name='" + cek + "')"
                    + " begin" + " CREATE COLUMN ENCRYPTION KEY " + cek + " WITH VALUES " + "(COLUMN_MASTER_KEY = "
                    + cmk + ", ALGORITHM = '" + Constants.CEK_ALGORITHM + "', ENCRYPTED_VALUE = 0x"
                    + TestUtils.bytesToHexString(key, key.length) + ")" + ";" + " end";
            stmt.execute(cekSql);
        }
    }

    private void dropCMK(String cmk) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = connection.createStatement()) {
            String cekSql = " if exists (SELECT name from sys.column_master_keys where name='" + cmk + "')" + " begin"
                    + " drop column master key " + cmk + " end";
            stmt.execute(cekSql);
        }
    }

    private void dropCEK(String cek) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString);
                Statement stmt = connection.createStatement()) {
            String cekSql = " if exists (SELECT name from sys.column_encryption_keys where name='" + cek + "')"
                    + " begin" + " drop column encryption key " + cek + " end";
            stmt.execute(cekSql);
        }
    }

    private SQLServerColumnEncryptionKeyStoreProvider setupKeyStoreProvider() throws SQLServerException {
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
        return registerProvider(
                new SQLServerColumnEncryptionAzureKeyVaultProvider(applicationClientID, applicationKey));
    }

    private SQLServerColumnEncryptionKeyStoreProvider registerProvider(
            SQLServerColumnEncryptionKeyStoreProvider provider) throws SQLServerException {
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> map1 = new HashMap<>();
        map1.put(provider.getName(), provider);
        SQLServerConnection.registerColumnEncryptionKeyStoreProviders(map1);
        return provider;
    }
}
