/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made available under the terms of the
 * MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import com.microsoft.sqlserver.testframework.DummyKeyStoreProvider;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.security.MessageDigest;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

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
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatementColumnEncryptionSetting;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Test multi-user Azure Key Store provider.
 */
@RunWith(JUnitPlatform.class)
public class MultiUserAKVTest extends AESetup {

    private static Map<String, SQLServerColumnEncryptionKeyStoreProvider> requiredKeyStoreProvider = new HashMap<>();
    private static Map<String, SQLServerColumnEncryptionKeyStoreProvider> notRequiredKeyStoreProvider = new HashMap<>();
    
    private static final String notRequiredProviderName = "DummyProvider2";
    private static final String requiredProviderName = "DUMMY_PROVIDER";
    private static final String cekCacheSizeGetterName = "getColumnEncryptionKeyCacheSize";
    private static final String cmkMetadataCacheSizeGetterName = "getCmkMetadataSignatureVerificationCacheSize";
    private static final String cekCacheName = "columnEncryptionKeyCache";

    private static final String customProviderTableName = TestUtils
            .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("JDBCCustomProvider")));

    private static final String dummyProviderTableName = TestUtils
    .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("JDBCDummyProvider")));
    
    private static boolean isMasterKeyPathSetup = false;
    private static final char[] hexChars = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    @BeforeAll
    public static void testSetup() throws Exception {
        requiredKeyStoreProvider.put(requiredProviderName, new DummyKeyStoreProvider());
        notRequiredKeyStoreProvider.put(notRequiredProviderName, new DummyKeyStoreProvider());
                
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

        isMasterKeyPathSetup = !(null == keyIDs[0] || keyIDs[0].trim().isEmpty());
    }

    @Test
    public void decryptedCekIsCachedDuringDecryption() throws Exception{
        SQLServerColumnEncryptionAzureKeyVaultProvider provider = createAKVProvider();

        if (null == provider) {
            fail(TestResource.getResource("R_AKVProviderNull"));
        }

        if (!isMasterKeyPathSetup) {
            fail(TestResource.getResource("R_masterKeyPathNullOrEmpty"));
        }

        byte[] plaintextKey1 = { 1, 2, 3 };
        byte[] plaintextKey2 = { 1, 2, 3 };
        byte[] plaintextKey3 = { 0, 1, 2, 3 };

        byte[] encryptedKey1 = provider.encryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, plaintextKey1);
        byte[] encryptedKey2 = provider.encryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, plaintextKey2);
        byte[] encryptedKey3 = provider.encryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, plaintextKey3);

        byte[] decryptedKey1 = provider.decryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, encryptedKey1);
        assertEquals(1, getCacheSize(cekCacheSizeGetterName, provider));
        assertArrayEquals(plaintextKey1, decryptedKey1);

        // this should not create a new entry in cek cache
        decryptedKey1 = provider.decryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, encryptedKey1);
        assertEquals(1, getCacheSize(cekCacheSizeGetterName, provider));
        assertArrayEquals(plaintextKey1, decryptedKey1);

        byte[] decryptedKey2 = provider.decryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, encryptedKey2);
        assertEquals(2, getCacheSize(cekCacheSizeGetterName, provider));
        assertArrayEquals(plaintextKey2, decryptedKey2);
        
        byte[] decryptedKey3 = provider.decryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, encryptedKey3);
        assertEquals(3, getCacheSize(cekCacheSizeGetterName, provider));
        assertArrayEquals(plaintextKey3, decryptedKey3);
        
    }

    @Test
    public void signatureVerificationResultIsCachedDuringVerification() throws Exception{
        SQLServerColumnEncryptionAzureKeyVaultProvider provider = createAKVProvider();

        if (provider == null) {
            fail(TestResource.getResource("R_AKVProviderNull"));
        }

        if (!isMasterKeyPathSetup) {
            fail(TestResource.getResource("R_masterKeyPathNullOrEmpty"));
        }

        byte[] signature1 = signColumnMasterKeyMetadata(provider, keyIDs[0], true);
        byte[] signature2 = signColumnMasterKeyMetadata(provider, keyIDs[0], true);
        byte[] signatureWithoutEnclave = signColumnMasterKeyMetadata(provider, keyIDs[0], false);

        assertTrue(provider.verifyColumnMasterKeyMetadata(keyIDs[0], true, signature1));
        assertEquals(1, getCacheSize(cmkMetadataCacheSizeGetterName, provider));

        // this should not create a new entry in cmk metadata signature verification cache
        assertTrue(provider.verifyColumnMasterKeyMetadata(keyIDs[0], true, signature1));
        assertEquals(1, getCacheSize(cmkMetadataCacheSizeGetterName, provider));

        assertTrue(provider.verifyColumnMasterKeyMetadata(keyIDs[0], true, signature2));
        assertEquals(1, getCacheSize(cmkMetadataCacheSizeGetterName, provider));

        assertFalse(provider.verifyColumnMasterKeyMetadata(keyIDs[0], false, signatureWithoutEnclave));
        assertEquals(1, getCacheSize(cmkMetadataCacheSizeGetterName, provider));
    }

    @Test
    public void cekCacheEntryIsEvictedAfterTtlExpires() throws Exception {
        SQLServerColumnEncryptionAzureKeyVaultProvider provider = createAKVProvider();

        if (provider == null) {
            fail(TestResource.getResource("R_AKVProviderNull"));
        }

        if (!isMasterKeyPathSetup) {
            fail(TestResource.getResource("R_masterKeyPathNullOrEmpty"));
        }

        provider.setColumnEncryptionCacheTtl(Duration.ofSeconds(10));
        byte[] plaintextKey = { 1, 2, 3 };
        byte[] encryptedKey = provider.encryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, plaintextKey);

        provider.decryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, encryptedKey);
        assertTrue(cekCacheContainsKey(encryptedKey, provider));
        assertEquals(1, getCacheSize(cekCacheSizeGetterName, provider));

        Thread.sleep(TimeUnit.SECONDS.toMillis(11));
        assertFalse(cekCacheContainsKey(encryptedKey, provider));
        assertEquals(0, getCacheSize(cekCacheSizeGetterName, provider));
    }

    @Test
    public void cekCacheShouldBeDisabledWhenAkvProviderIsRegisteredGlobally() throws Exception {
        SQLServerColumnEncryptionAzureKeyVaultProvider provider = createAKVProvider();
        
        if (provider == null) {
            fail(TestResource.getResource("R_AKVProviderNull"));
        }

        if (!isMasterKeyPathSetup) {
            fail(TestResource.getResource("R_masterKeyPathNullOrEmpty"));
        }

        Map<String, SQLServerColumnEncryptionKeyStoreProvider> providerMap = new HashMap<String, SQLServerColumnEncryptionKeyStoreProvider>();
        providerMap.put(Constants.AZURE_KEY_VAULT_NAME, provider);
        SQLServerConnection.registerColumnEncryptionKeyStoreProviders(providerMap);

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo)) {
            Field globalCustomCacheField = SQLServerConnection.class.getDeclaredField("globalCustomColumnEncryptionKeyStoreProviders");
            globalCustomCacheField.setAccessible(true);

            Map<String, SQLServerColumnEncryptionKeyStoreProvider> globalCacheFieldValue = (Map<String, SQLServerColumnEncryptionKeyStoreProvider>) globalCustomCacheField.get(con);

            Method method = globalCacheFieldValue.getClass().getDeclaredMethod("get", Object.class);
            SQLServerColumnEncryptionAzureKeyVaultProvider providerInGlobalCache = (SQLServerColumnEncryptionAzureKeyVaultProvider) method.invoke(globalCacheFieldValue, Constants.AZURE_KEY_VAULT_NAME);   

            byte[] plaintextKey = { 1, 2, 3 };
            byte[] encryptedKey = providerInGlobalCache.encryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, plaintextKey);

            providerInGlobalCache.decryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, encryptedKey);
            assertEquals(0, getCacheSize(cekCacheSizeGetterName, providerInGlobalCache));
        } catch (SQLException e) {
            fail(e.getMessage());
        } finally {
            // clean up
            SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
        }
    }

    @Test
    public void testLocalCekCacheIsScopedToProvider() throws Exception {
        SQLServerColumnEncryptionAzureKeyVaultProvider provider = createAKVProvider();
        
        if (provider == null) {
            fail(TestResource.getResource("R_AKVProviderNull"));
        }

        if (!isMasterKeyPathSetup) {
            fail(TestResource.getResource("R_masterKeyPathNullOrEmpty"));
        }
        
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> providerMap = new HashMap<String, SQLServerColumnEncryptionKeyStoreProvider>();
        providerMap.put(Constants.AZURE_KEY_VAULT_NAME, akvProvider);
        SQLServerConnection.registerColumnEncryptionKeyStoreProviders(providerMap);

        int customerId = 10;
        String customerName = "Microsoft";
        createTableForCustomProvider(AETestConnectionString, customProviderTableName, cekAkv);
        insertData(customProviderTableName, customerId, customerName);        
        
        String sql = "SELECT CustomerId, CustomerName FROM " + customProviderTableName + " WHERE CustomerId = ?";

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                    SQLServerStatementColumnEncryptionSetting.Enabled)) {            
            
            pstmt.setInt(1, customerId);
            
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                int intValue = rs.getInt(1);
                String strValue = rs.getString(2);
                assertTrue((customerId == intValue) && strValue.equalsIgnoreCase(customerName));
            }
            
            // Clean up global custom providers
            SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
            
            // Register key store provider on statement level
            providerMap.put(Constants.AZURE_KEY_VAULT_NAME, provider);
            pstmt.registerColumnEncryptionKeyStoreProvidersOnStatement(providerMap);
            
            // Execute a query using provider from statement-level cache. this will cache the cek in the local cek cache
            ResultSet rs2 = pstmt.executeQuery();
            while (rs2.next()) {
                int intValue = rs2.getInt(1);
                String strValue = rs2.getString(2);
                assertTrue((customerId == intValue) && strValue.equalsIgnoreCase(customerName));
            }
            
            // Register invalid key store provider on statement level. This will overwrite the previous one.
            SQLServerColumnEncryptionAzureKeyVaultProvider providerWithBadCred = new SQLServerColumnEncryptionAzureKeyVaultProvider("badApplicationID", "badApplicationKey");
            providerMap.put(Constants.AZURE_KEY_VAULT_NAME, providerWithBadCred);
            pstmt.registerColumnEncryptionKeyStoreProvidersOnStatement(providerMap);

            // The following query should fail due to an empty cek cache and invalid credentials
            try (ResultSet rs3 = pstmt.executeQuery()) {
                int numberOfColumns = rs3.getMetaData().getColumnCount(); 
                while (rs3.next()) {
                    int intValue = rs3.getInt(1);
                    String strValue = rs3.getString(2);
                    assertTrue((customerId == intValue) && strValue.equalsIgnoreCase(customerName));
                }               
                fail("Expected SQLServerException is not caught.");
            } catch (SQLServerException ex) {
                assertTrue(ex.getMessage().contains("AADSTS700016"));
            }            
        } finally {
            dropObject(AETestConnectionString, "TABLE", customProviderTableName);
        }
    }

    @Test
    public void testConnectionCustomKeyStoreProviderDuringAeQuery() throws Exception {
        DummyKeyStoreProvider dummyProvider = new DummyKeyStoreProvider();

        if (!isMasterKeyPathSetup) {
            fail(TestResource.getResource("R_masterKeyPathNullOrEmpty"));
        }

        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> providerMap = new HashMap<String, SQLServerColumnEncryptionKeyStoreProvider>();
        providerMap.put(Constants.DUMMY_KEYSTORE_NAME, dummyProvider);
        SQLServerConnection.registerColumnEncryptionKeyStoreProviders(providerMap);

        // Create an empty table for testing
        createTableForCustomProvider(AETestConnectionString, dummyProviderTableName, cekDummy);
        
        int customerId = 10;
        String sql = "SELECT CustomerId, CustomerName FROM " + dummyProviderTableName + " WHERE CustomerId = ?";

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo)) {           
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                SQLServerStatementColumnEncryptionSetting.Enabled)) {
                pstmt.setInt(1, customerId);
                ResultSet rs = pstmt.executeQuery(); 
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception ex) {
                // kz debug
                System.out.println(ex.getMessage());
                assertTrue(ex instanceof UnsupportedOperationException);
            }         
            
            // Register not required provider at connection instance level.
            // It should not fall back to the global cache so the right provider will not be found.
            con.registerColumnEncryptionKeyStoreProvidersOnConnection(notRequiredKeyStoreProvider);
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                SQLServerStatementColumnEncryptionSetting.Enabled)) {
                pstmt.setInt(1, customerId);
                ResultSet rs = pstmt.executeQuery(); 
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception ex) {
                // kz debug
                System.out.println(ex.getMessage());
                assertTrue(ex.getMessage().matches(TestUtils.formatErrorMsg("R_UnrecognizedConnectionKeyStoreProviderName")));
            }
            
            // required provider in connection instance cache
            // if the instance cache is not empty, it is always checked for the provider.
            // => if the provider is found, it must have been retrieved from the instance cache and not the global cache
            con.registerColumnEncryptionKeyStoreProvidersOnConnection(requiredKeyStoreProvider);
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                SQLServerStatementColumnEncryptionSetting.Enabled)) {
                pstmt.setInt(1, customerId);
                ResultSet rs = pstmt.executeQuery(); 
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception ex) {
                // kz debug
                System.out.println(ex.getMessage());
                assertTrue(ex instanceof UnsupportedOperationException);
            }

            // not required provider will replace the previous entry so required provider will not be found
            con.registerColumnEncryptionKeyStoreProvidersOnConnection(notRequiredKeyStoreProvider);
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                SQLServerStatementColumnEncryptionSetting.Enabled)) {
                pstmt.setInt(1, customerId);
                ResultSet rs = pstmt.executeQuery(); 
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception ex) {
                // kz debug
                System.out.println(ex.getMessage());
                assertTrue(ex.getMessage().matches(TestUtils.formatErrorMsg("R_UnrecognizedConnectionKeyStoreProviderName")));
            }            
        } finally {
            dropObject(AETestConnectionString, "TABLE", customProviderTableName);
            SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();            
        }
    }

    // This will fail on Enclave Servers during createEnclaveSession()
    @Tag(Constants.xSQLv15)
    @Test
    public void testStatementCustomKeyStoreProviderDuringAeQuery() throws Exception {
        DummyKeyStoreProvider dummyProvider = new DummyKeyStoreProvider();

        if (!isMasterKeyPathSetup) {
            fail(TestResource.getResource("R_masterKeyPathNullOrEmpty"));
        }

        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> providerMap = new HashMap<String, SQLServerColumnEncryptionKeyStoreProvider>();
        providerMap.put(Constants.DUMMY_KEYSTORE_NAME, dummyProvider);
        SQLServerConnection.registerColumnEncryptionKeyStoreProviders(providerMap);

        // Create an empty table for testing
        String connString = connectionString + ";sendTimeAsDateTime=false" + ";columnEncryptionSetting=enabled";
        createCMK(connString, cmkDummy, Constants.DUMMY_KEYSTORE_NAME, keyIDs[0], Constants.CMK_SIGNATURE_AKV);
        createCEK(connString, cmkDummy, cekDummy, akvProvider);

        createTableForCustomProvider(connString, customProviderTableName, cekDummy);
        
        int customerId = 10;
        String sql = "SELECT CustomerId, CustomerName FROM " + customProviderTableName + " WHERE CustomerId = ?";        

        try (SQLServerConnection con = PrepUtil.getConnection(connString, AEInfo);           
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                    SQLServerStatementColumnEncryptionSetting.Enabled)) {
            pstmt.setInt(1, customerId);

            // DummyProvider in global cache will be used.
            // Provider will be found but it will throw when its methods are called
            try {
                ResultSet rs = pstmt.executeQuery(); 
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception ex) {
                assertTrue(ex instanceof UnsupportedOperationException);
            }
            
            // Required provider will be found in statement instance level.
            pstmt.registerColumnEncryptionKeyStoreProvidersOnStatement(requiredKeyStoreProvider);
            try {
                ResultSet rs = pstmt.executeQuery(); 
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception ex) {
                assertTrue(ex instanceof UnsupportedOperationException);
            }
            
            // Register not required provider at statement instance level.
            // It should not fall back to the global cache so the right provider will not be found.
            pstmt.registerColumnEncryptionKeyStoreProvidersOnStatement(notRequiredKeyStoreProvider);
            try {
                ResultSet rs = pstmt.executeQuery(); 
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception ex) {
                assertTrue(ex.getMessage().matches(TestUtils.formatErrorMsg("R_UnrecognizedStatementKeyStoreProviderName")));
            }
            
            // Register required provider at connection level but keep not required provider at statement level.
            // This should not fall back to connection level or global level.
            con.registerColumnEncryptionKeyStoreProvidersOnConnection(requiredKeyStoreProvider);
            try {
                ResultSet rs = pstmt.executeQuery(); 
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception ex) {
                assertTrue(ex.getMessage().matches(TestUtils.formatErrorMsg("R_UnrecognizedStatementKeyStoreProviderName")));
            }

            // The new statement instance should have an empty cache and query will fall back to connection level
            // which contains the required provider
            try (SQLServerPreparedStatement pstmt2 = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                    SQLServerStatementColumnEncryptionSetting.Enabled)) {
                pstmt2.setInt(1, customerId);
                
                try {
                    ResultSet rs = pstmt2.executeQuery(); 
                    fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                } catch (Exception ex) {
                    // kz debug
                    System.out.println(ex.getMessage());
                    assertTrue(ex instanceof UnsupportedOperationException);
                }
            }
        } finally {
            dropObject(connString, "TABLE", customProviderTableName);
            dropObject(connString, "CEK", cekDummy);
            dropObject(connString, "CMK", cmkDummy);

            SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
        }
    }

    private void insertData(String tableName, int customId, String customName) {
        String sqlQuery = "INSERT INTO " + tableName + " VALUES ( ?, ? )"; 

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sqlQuery,
                    SQLServerStatementColumnEncryptionSetting.Enabled)) { 
            pstmt.setInt(1, customId);
            pstmt.setString(2, customName);
            pstmt.executeUpdate();
            System.out.println("1 Record inserted successfully.");                              
        } catch(SQLException e) {
            fail(e.getMessage());
        }        
    }

    private void createTableForCustomProvider(String connString, String tableName, String cekName) {
        String sqlQuery = "CREATE TABLE " + tableName + " (" 
                + " [CustomerId] [int] ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [" + cekName + "], ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256'), "
                + " [CustomerName] [varchar](50) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [" + cekName + "], ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') "
                + ")";

        try (SQLServerConnection con = PrepUtil.getConnection(connString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) { 
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.execute(sqlQuery);                              
        } catch(SQLException e) {
            fail(e.getMessage());
        }        
    }

    private void dropObject(String connString, String objectType, String objectName) {
        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(connString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            switch(objectType) {
                case "TABLE":
                    TestUtils.dropTableIfExists(objectName, stmt);
                    break;
                case "CEK":
                    dropCEK(objectName, stmt);
                    break;
                case "CMK":
                    dropCMK(objectName, stmt);
                    break;
                default:
                    break;
            }
        } catch(Exception ex) {
            fail(ex.getMessage());
        }
    }

    private boolean cekCacheContainsKey(byte[] encryptedKey, SQLServerColumnEncryptionAzureKeyVaultProvider provider) throws Exception {
        assertFalse(null == encryptedKey || 0 == encryptedKey.length);        

        String encryptedCEKHexString = byteToHexDisplayString(encryptedKey);    

        Field cekCacheField = provider.getClass().getDeclaredField(cekCacheName);
        cekCacheField.setAccessible(true);

        Object fieldValue = cekCacheField.get(provider);
        Method method = fieldValue.getClass().getDeclaredMethod("contains", Object.class);
        method.setAccessible(true);

        boolean result = (boolean)method.invoke(fieldValue, encryptedCEKHexString);
        
        return result;
    }

    private byte[] signColumnMasterKeyMetadata(SQLServerColumnEncryptionAzureKeyVaultProvider provider, String masterKeyPath, boolean allowEnclaveComputations) throws Exception {

        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(Constants.AZURE_KEY_VAULT_NAME.toLowerCase().getBytes(java.nio.charset.StandardCharsets.UTF_16LE));
        md.update(masterKeyPath.toLowerCase().getBytes(java.nio.charset.StandardCharsets.UTF_16LE));
        
        if (allowEnclaveComputations) {
            md.update("true".getBytes(java.nio.charset.StandardCharsets.UTF_16LE));
        } else {
            md.update("false".getBytes(java.nio.charset.StandardCharsets.UTF_16LE));
        }

        byte[] dataToSign = md.digest();
        if (null == dataToSign) {
            fail("data to sign is null or empty.");
        }

        assertTrue(dataToSign.length > 0);

        Method method = provider.getClass().getDeclaredMethod("AzureKeyVaultSignHashedData", byte[].class, String.class);
        method.setAccessible(true);

        byte[] signature = (byte[]) method.invoke(provider, dataToSign, masterKeyPath);

        if (null == signature || 0 == signature.length) {
            fail("Signature of column master key metadata is null or empty.");
        }

        return signature;
    }
    
    private int getCacheSize(String methodName, SQLServerColumnEncryptionAzureKeyVaultProvider provider) throws Exception {
        Method method = provider.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);
        
        int count = (int)method.invoke(provider);

        return count;     
    }

    private SQLServerColumnEncryptionAzureKeyVaultProvider createAKVProvider() throws Exception {
        
        SQLServerColumnEncryptionAzureKeyVaultProvider azureKeyVaultProvider = null;

        if (null != applicationClientID && null != applicationKey) {
            File file = null;
            try {
                file = new File(Constants.MSSQL_JDBC_PROPERTIES);
                try (OutputStream os = new FileOutputStream(file);) {
                    Properties props = new Properties();
                    // Append to the list of hardcoded endpoints
                    props.setProperty(Constants.AKV_TRUSTED_ENDPOINTS_KEYWORD, ";vault.azure.net");
                    props.store(os, "");
                }
                azureKeyVaultProvider = new SQLServerColumnEncryptionAzureKeyVaultProvider(applicationClientID, applicationKey);
                
            } finally {
                if (null != file) {
                    file.delete();
                }
            }
        }

        return azureKeyVaultProvider;
    }

    private static String byteToHexDisplayString(byte[] b) {
        if (null == b)
            return "(null)";
        int hexVal;
        StringBuilder sb = new StringBuilder(b.length * 2 + 2);
        sb.append("0x");
        for (byte aB : b) {
            hexVal = aB & 0xFF;
            sb.append(hexChars[(hexVal & 0xF0) >> 4]);
            sb.append(hexChars[(hexVal & 0x0F)]);
        }
        return sb.toString();
    }
}