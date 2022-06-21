/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.parametermetadata;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.*;
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
@Tag(Constants.xSQLv11)
@Tag(Constants.xSQLv12)
@Tag(Constants.xSQLv14)
public class ParameterMetaDataCacheTest extends AbstractTest {
    private static final String firstTable = "firstTable";
    private static final String secondTable = "secondTable";
    private final String cmkName = "my_cmk";
    private final String cekName = "my_cek_1";
    private final String cekNameAlt = "my_cek_2";
    private final String sampleData = "testData";
    private final String sampleData2 = "testData2";

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
    
                dropCEK(cekNameAlt);
                dropCEK(cekName);
                dropCMK(cmkName);
                
                createCMK(cmkName, keyIDs[0]);
                createCEK(cekName, cmkName, setupKeyStoreProvider(), keyIDs[0]);
                createTable(cekName, firstTable);
                createTable(cekName, secondTable);
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
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.reqExternalSetup)
    public void testParameterMetaDataCache() throws Exception {
        tableSetup();
        try {
            updateTable(firstTable, sampleData);
            updateTable(secondTable, sampleData2);
            
            long firstRun = timedTestSelect(connection, firstTable, "DeterministicVarcharMax", sampleData);
            selectTable(secondTable, sampleData2);
            long secondRun = timedTestSelect(connection, firstTable, "DeterministicVarcharMax", sampleData);

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
     * the query again. The query should fail, but retry and pass. Currently disabled as secure enclaves are not
     * supported.
     * 
     * @throws SQLServerException
     */
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.reqExternalSetup)
    public void testRetryWithSecureCache() throws Exception {
        tableSetup();
        try {
            updateTable(firstTable, sampleData);
            selectTable(firstTable, sampleData);
            
            createCEK(cekNameAlt, cmkName, setupKeyStoreProvider(), keyIDs[0]);
            alterTable(firstTable,cekNameAlt);

            selectTable(firstTable, sampleData);
            alterTable(firstTable,cekName);
            dropCEK(cekNameAlt);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }
    

    private static void createTable(String cekName, String tableName) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); 
            Statement stmt = connection.createStatement()) {
                stmt.executeUpdate("create table " + tableName + " ("
                        + "DeterministicVarcharMax varchar(max) COLLATE Latin1_General_BIN2 ENCRYPTED WITH " 
                        + "(ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', " 
                        + "COLUMN_ENCRYPTION_KEY = " + cekName + ") NULL,);");
        }
    }

    private static void updateTable(String tableName, String sampleData) throws SQLException {
        String sql = "insert into " + tableName + " values( ? )";
        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql);
        pstmt.setObject(1,sampleData, java.sql.Types.VARCHAR);
        pstmt.execute();
        pstmt.close();
    }
    
    private static void selectTable(String tableName, String sampleData) throws SQLException {
        String sql = "select * from " + tableName + " where DeterministicVarcharMax = ( ? )";
        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql);
        pstmt.setObject(1,sampleData, java.sql.Types.VARCHAR);
        pstmt.execute();
        pstmt.close();
    }
    
    private static void alterTable(String tableName, String newCek) throws SQLException {
        String sql = "alter table " + tableName + " alter column DeterministicVarcharMax nvarchar(max)";
        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(sql);
        pstmt.execute();
        pstmt.close();
    }
    
    private long timedTestSelect(Connection connection, String tableName, String firstColumn,
            String firstData) throws SQLException {
        long timer = System.currentTimeMillis();
        selectTable(firstTable, sampleData);
        return System.currentTimeMillis() - timer;
    }

    private void createCMK(String cmkName, String keyPath) throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); 
            Statement stmt = connection.createStatement()) {
                String sql = " if not exists (SELECT name from sys.column_master_keys where name='" + cmkName + "')"
                        + " begin" + " CREATE COLUMN MASTER KEY " + cmkName + " WITH (KEY_STORE_PROVIDER_NAME = '"
                        + Constants.AZURE_KEY_VAULT_NAME+ "', KEY_PATH = '"+ keyPath +"')" + " end";
                stmt.execute(sql);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private void createCEK(String cekName, String cmkName, SQLServerColumnEncryptionKeyStoreProvider storeProvider,
            String encryptionKey) throws SQLServerException, SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); 
            Statement stmt = connection.createStatement()) {
                String letters = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
                byte[] valuesDefault = letters.getBytes();
                byte[] key = storeProvider.encryptColumnEncryptionKey(encryptionKey, "RSA_OAEP", valuesDefault);
                String cekSql = " if not exists (SELECT name from sys.column_encryption_keys where name='" 
                        + cekName + "')" + " begin" + " CREATE COLUMN ENCRYPTION KEY " + cekName 
                        + " WITH VALUES " + "(COLUMN_MASTER_KEY = " + cmkName 
                        + ", ALGORITHM = 'RSA_OAEP', ENCRYPTED_VALUE = 0x" + bytesToHexString(key, key.length)
                        + ")" + ";" + " end";
                stmt.execute(cekSql);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

    }

    private void dropCMK(String cmkName) throws SQLServerException, SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); 
            Statement stmt = connection.createStatement()) {
                String cekSql = " if exists (SELECT name from sys.column_master_keys where name='" + cmkName + "')"
                        + " begin" + " drop column master key " + cmkName + " end";
                stmt.execute(cekSql);
        } catch (SQLException e) {
            fail(e.getMessage());
        }

    }

    private void dropCEK(String cekName) throws SQLServerException, SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); 
            Statement stmt = connection.createStatement()) {
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
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> map1 = 
            new HashMap<String, SQLServerColumnEncryptionKeyStoreProvider>();
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
