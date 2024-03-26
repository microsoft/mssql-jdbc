/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
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
import java.text.MessageFormat;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
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
@Tag(Constants.xSQLv11)
@Tag(Constants.xSQLv12)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.xAzureSQLDB)
@Tag(Constants.reqExternalSetup)
public class MultiUserAKVTest extends AESetup {

    private static Map<String, SQLServerColumnEncryptionKeyStoreProvider> requiredKeyStoreProvider = new HashMap<>();
    private static Map<String, SQLServerColumnEncryptionKeyStoreProvider> notRequiredKeyStoreProvider = new HashMap<>();

    private static final String notRequiredProviderName = "UNWANTED_DUMMY_PROVIDER";
    private static final String requiredProviderName = "DUMMY_PROVIDER";
    private static final String cekCacheSizeGetterName = "getColumnEncryptionKeyCacheSize";
    private static final String cmkMetadataCacheSizeGetterName = "getCmkMetadataSignatureVerificationCacheSize";
    private static final String cekCacheName = "columnEncryptionKeyCache";

    private static final String customProviderTableName = TestUtils
            .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("JDBCCustomProvider")));

    private static final String dummyProviderTableName = TestUtils
            .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("JDBCDummyProvider")));

    private static String cmkDummy = Constants.CMK_NAME + "_DUMMY"; // For dummyKeyStoreProvider
    private static String cekDummy = Constants.CEK_NAME + "_DUMMY"; // For dummyKeyStoreProvider

    private static boolean isMasterKeyPathSetup = false;

    @BeforeAll
    public static void testSetup() throws Exception {
        requiredKeyStoreProvider.put(requiredProviderName, new DummyKeyStoreProvider());
        notRequiredKeyStoreProvider.put(notRequiredProviderName, new DummyKeyStoreProvider());

        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

        isMasterKeyPathSetup = !(null == keyIDs[0] || keyIDs[0].trim().isEmpty());
    }

    @AfterAll
    public static void testCleanUp() throws Exception {
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> tempMap = new HashMap<String, SQLServerColumnEncryptionKeyStoreProvider>();

        if (null != jksProvider) {
            tempMap.put(Constants.CUSTOM_KEYSTORE_NAME, jksProvider);
        }

        if (null != akvProvider && null != applicationClientID && null != applicationKey) {
            tempMap.put(Constants.AZURE_KEY_VAULT_NAME, akvProvider);
        }

        if (tempMap.size() > 0) {
            SQLServerConnection.registerColumnEncryptionKeyStoreProviders(tempMap);
        }
    }

    @Test
    @Tag(Constants.reqExternalSetup)
    public void decryptedCekIsCachedDuringDecryption() throws Exception {
        SQLServerColumnEncryptionAzureKeyVaultProvider provider = createAKVProvider();

        if (null == provider) {
            fail(TestResource.getResource("R_AKVProviderNull"));
        }

        if (!isMasterKeyPathSetup) {
            Object[] msgArg = {"master key path"};
            fail((new MessageFormat(TestResource.getResource("R_objectNullOrEmpty"))).format(msgArg));
        }

        byte[] plaintextKey1 = {1, 2, 3};
        byte[] plaintextKey2 = {1, 2, 3};
        byte[] plaintextKey3 = {0, 1, 2, 3};

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
    @Tag(Constants.reqExternalSetup)
    public void signatureVerificationResultIsCachedDuringVerification() throws Exception {
        SQLServerColumnEncryptionAzureKeyVaultProvider provider = createAKVProvider();

        if (provider == null) {
            fail(TestResource.getResource("R_AKVProviderNull"));
        }

        if (!isMasterKeyPathSetup) {
            Object[] msgArg = {"master key path"};
            fail((new MessageFormat(TestResource.getResource("R_objectNullOrEmpty"))).format(msgArg));
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
    @Tag(Constants.reqExternalSetup)
    public void cekCacheEntryIsEvictedAfterTtlExpires() throws Exception {
        SQLServerColumnEncryptionAzureKeyVaultProvider provider = createAKVProvider();

        if (provider == null) {
            fail(TestResource.getResource("R_AKVProviderNull"));
        }

        if (!isMasterKeyPathSetup) {
            Object[] msgArg = {"master key path"};
            fail((new MessageFormat(TestResource.getResource("R_objectNullOrEmpty"))).format(msgArg));
        }

        provider.setColumnEncryptionCacheTtl(Duration.ofSeconds(10));
        byte[] plaintextKey = {1, 2, 3};
        byte[] encryptedKey = provider.encryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, plaintextKey);

        provider.decryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM, encryptedKey);
        assertTrue(cekCacheContainsKey(encryptedKey, provider));
        assertEquals(1, getCacheSize(cekCacheSizeGetterName, provider));

        Thread.sleep(TimeUnit.SECONDS.toMillis(11));
        assertFalse(cekCacheContainsKey(encryptedKey, provider));
        assertEquals(0, getCacheSize(cekCacheSizeGetterName, provider));
    }

    @SuppressWarnings("unchecked")
    @Test
    @Tag(Constants.reqExternalSetup)
    public void cekCacheShouldBeDisabledWhenAkvProviderIsRegisteredGlobally() throws Exception {
        SQLServerColumnEncryptionAzureKeyVaultProvider provider = createAKVProvider();

        if (provider == null) {
            fail(TestResource.getResource("R_AKVProviderNull"));
        }

        if (!isMasterKeyPathSetup) {
            Object[] msgArg = {"master key path"};
            fail((new MessageFormat(TestResource.getResource("R_objectNullOrEmpty"))).format(msgArg));
        }

        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> providerMap = new HashMap<String, SQLServerColumnEncryptionKeyStoreProvider>();
        providerMap.put(Constants.AZURE_KEY_VAULT_NAME, provider);
        SQLServerConnection.registerColumnEncryptionKeyStoreProviders(providerMap);

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo)) {
            Field globalCustomCacheField = SQLServerConnection.class
                    .getDeclaredField("globalCustomColumnEncryptionKeyStoreProviders");
            globalCustomCacheField.setAccessible(true);

            Map<String, SQLServerColumnEncryptionKeyStoreProvider> globalCacheFieldValue = (Map<String, SQLServerColumnEncryptionKeyStoreProvider>) globalCustomCacheField
                    .get(con);

            Method method = globalCacheFieldValue.getClass().getDeclaredMethod("get", Object.class);
            SQLServerColumnEncryptionAzureKeyVaultProvider providerInGlobalCache = (SQLServerColumnEncryptionAzureKeyVaultProvider) method
                    .invoke(globalCacheFieldValue, Constants.AZURE_KEY_VAULT_NAME);

            byte[] plaintextKey = {1, 2, 3};
            byte[] encryptedKey = providerInGlobalCache.encryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM,
                    plaintextKey);

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
    @Tag(Constants.reqExternalSetup)
    public void testLocalCekCacheIsScopedToProvider() throws Exception {
        SQLServerColumnEncryptionAzureKeyVaultProvider provider = createAKVProvider();

        if (provider == null) {
            fail(TestResource.getResource("R_AKVProviderNull"));
        }

        if (!isMasterKeyPathSetup) {
            Object[] msgArg = {"master key path"};
            fail((new MessageFormat(TestResource.getResource("R_objectNullOrEmpty"))).format(msgArg));
        }

        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
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
                        SQLServerStatementColumnEncryptionSetting.ENABLED)) {

            pstmt.setInt(1, customerId);

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    int intValue = rs.getInt(1);
                    String strValue = rs.getString(2);
                    assertTrue((customerId == intValue) && strValue.equalsIgnoreCase(customerName));
                }
            }

            // Clean up global custom providers
            SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

            // Register key store provider on statement level
            providerMap.put(Constants.AZURE_KEY_VAULT_NAME, provider);
            pstmt.registerColumnEncryptionKeyStoreProvidersOnStatement(providerMap);

            // Execute a query using provider from statement-level cache. this will cache the cek in the local cek cache
            try (ResultSet rs2 = pstmt.executeQuery()) {
                while (rs2.next()) {
                    int intValue = rs2.getInt(1);
                    String strValue = rs2.getString(2);
                    assertTrue((customerId == intValue) && strValue.equalsIgnoreCase(customerName));
                }
            }

            // Register invalid key store provider on statement level. This will overwrite the previous one.
            SQLServerColumnEncryptionAzureKeyVaultProvider providerWithBadCred = new SQLServerColumnEncryptionAzureKeyVaultProvider(
                    "badApplicationID", "badApplicationKey");
            providerMap.put(Constants.AZURE_KEY_VAULT_NAME, providerWithBadCred);
            pstmt.registerColumnEncryptionKeyStoreProvidersOnStatement(providerMap);

            // The following query should fail due to an empty cek cache and invalid credentials
            try (ResultSet rs3 = pstmt.executeQuery()) {
                while (rs3.next()) {
                    int intValue = rs3.getInt(1);
                    String strValue = rs3.getString(2);
                    assertTrue((customerId == intValue) && strValue.equalsIgnoreCase(customerName));
                }
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (SQLServerException ex) {
                assertTrue(ex.getMessage().contains("AADSTS700016"));
            }
        } finally {
            dropObject(AETestConnectionString, "TABLE", customProviderTableName);
            SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
        }
    }

    @Test
    @Tag(Constants.reqExternalSetup)
    public void testConnectionCustomKeyStoreProviderDuringAeQuery() throws Exception {
        DummyKeyStoreProvider dummyProvider = new DummyKeyStoreProvider();

        if (akvProvider == null) {
            fail(TestResource.getResource("R_AKVProviderNull"));
        }

        if (!isMasterKeyPathSetup) {
            Object[] msgArg = {"master key path"};
            fail((new MessageFormat(TestResource.getResource("R_objectNullOrEmpty"))).format(msgArg));
        }

        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> providerMap = new HashMap<String, SQLServerColumnEncryptionKeyStoreProvider>();
        providerMap.put(Constants.DUMMY_KEYSTORE_NAME, dummyProvider);
        SQLServerConnection.registerColumnEncryptionKeyStoreProviders(providerMap);

        // Create cmk and cek for DummyKeyStoreProvider
        createCMK(AETestConnectionString, cmkDummy, Constants.DUMMY_KEYSTORE_NAME, keyIDs[0],
                TestUtils.byteToHexDisplayString(akvProvider.signColumnMasterKeyMetadata(keyIDs[0], true)));
        createCEK(AETestConnectionString, cmkDummy, cekDummy, akvProvider);

        // Create an empty table for testing
        createTableForCustomProvider(AETestConnectionString, dummyProviderTableName, cekDummy);

        int customerId = 10;
        String sql = "SELECT CustomerId, CustomerName FROM " + dummyProviderTableName + " WHERE CustomerId = ?";

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo)) {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                    SQLServerStatementColumnEncryptionSetting.ENABLED)) {
                pstmt.setInt(1, customerId);
                pstmt.executeQuery();
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception ex) {
                assertTrue(ex instanceof UnsupportedOperationException);
            }

            /*
             * Register not required provider at connection instance level. It should not fall back to the global cache
             * so the right provider will not be found.
             */
            con.registerColumnEncryptionKeyStoreProvidersOnConnection(notRequiredKeyStoreProvider);
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                    SQLServerStatementColumnEncryptionSetting.ENABLED)) {
                pstmt.setInt(1, customerId);
                pstmt.executeQuery();
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception ex) {
                assertTrue(ex.getMessage()
                        .matches(TestUtils.formatErrorMsg("R_UnrecognizedConnectionKeyStoreProviderName")));
            }

            /*
             * Required provider in connection instance cache. If the instance cache is not empty, it is always checked
             * for the provider. If the provider is found, it must have been retrieved from the instance cache and not
             * the global cache.
             */
            con.registerColumnEncryptionKeyStoreProvidersOnConnection(requiredKeyStoreProvider);
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                    SQLServerStatementColumnEncryptionSetting.ENABLED)) {
                pstmt.setInt(1, customerId);
                pstmt.executeQuery();
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception ex) {
                assertTrue(ex instanceof UnsupportedOperationException);
            }

            // Not required provider will replace the previous entry so required provider will not be found.
            con.registerColumnEncryptionKeyStoreProvidersOnConnection(notRequiredKeyStoreProvider);
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                    SQLServerStatementColumnEncryptionSetting.ENABLED)) {
                pstmt.setInt(1, customerId);
                pstmt.executeQuery();
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception ex) {
                assertTrue(ex.getMessage()
                        .matches(TestUtils.formatErrorMsg("R_UnrecognizedConnectionKeyStoreProviderName")));
            }
        } finally {
            dropObject(AETestConnectionString, "TABLE", dummyProviderTableName);
            dropObject(AETestConnectionString, "CEK", cekDummy);
            dropObject(AETestConnectionString, "CMK", cmkDummy);
            SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
        }
    }

    @Test
    @Tag(Constants.reqExternalSetup)
    public void testStatementCustomKeyStoreProviderDuringAeQuery() throws Exception {
        DummyKeyStoreProvider dummyProvider = new DummyKeyStoreProvider();

        if (!isMasterKeyPathSetup) {
            Object[] msgArg = {"master key path"};
            fail((new MessageFormat(TestResource.getResource("R_objectNullOrEmpty"))).format(msgArg));
        }

        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
        Map<String, SQLServerColumnEncryptionKeyStoreProvider> providerMap = new HashMap<String, SQLServerColumnEncryptionKeyStoreProvider>();
        providerMap.put(Constants.DUMMY_KEYSTORE_NAME, dummyProvider);
        SQLServerConnection.registerColumnEncryptionKeyStoreProviders(providerMap);

        // Create an empty table for testing
        createCMK(AETestConnectionString, cmkDummy, Constants.DUMMY_KEYSTORE_NAME, keyIDs[0],
                TestUtils.byteToHexDisplayString(akvProvider.signColumnMasterKeyMetadata(keyIDs[0], true)));
        createCEK(AETestConnectionString, cmkDummy, cekDummy, akvProvider);

        createTableForCustomProvider(AETestConnectionString, customProviderTableName, cekDummy);

        int customerId = 10;
        String sql = "SELECT CustomerId, CustomerName FROM " + customProviderTableName + " WHERE CustomerId = ?";

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        SQLServerStatementColumnEncryptionSetting.ENABLED)) {
            pstmt.setInt(1, customerId);

            /*
             * DummyProvider in global cache will be used. Provider will be found but it will throw when its methods are
             * called.
             */
            try {
                pstmt.executeQuery();
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception ex) {
                assertTrue(ex instanceof UnsupportedOperationException);
            }

            // Required provider will be found in statement instance level.
            pstmt.registerColumnEncryptionKeyStoreProvidersOnStatement(requiredKeyStoreProvider);
            try {
                pstmt.executeQuery();
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception ex) {
                assertTrue(ex instanceof UnsupportedOperationException);
            }

            /*
             * Register not required provider at statement instance level. It should not fall back to the global cache
             * so the right provider will not be found.
             */
            pstmt.registerColumnEncryptionKeyStoreProvidersOnStatement(notRequiredKeyStoreProvider);
            try {
                pstmt.executeQuery();
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception ex) {
                assertTrue(ex.getMessage()
                        .matches(TestUtils.formatErrorMsg("R_UnrecognizedStatementKeyStoreProviderName")));
            }

            /*
             * Register required provider at connection level but keep not required provider at statement level. This
             * should not fall back to connection level or global level.
             */
            con.registerColumnEncryptionKeyStoreProvidersOnConnection(requiredKeyStoreProvider);
            try {
                pstmt.executeQuery();
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception ex) {
                assertTrue(ex.getMessage()
                        .matches(TestUtils.formatErrorMsg("R_UnrecognizedStatementKeyStoreProviderName")));
            }

            /*
             * The new statement instance should have an empty cache and query will fall back to connection level which
             * contains the required provider
             */
            try (SQLServerPreparedStatement pstmt2 = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                    SQLServerStatementColumnEncryptionSetting.ENABLED)) {
                pstmt2.setInt(1, customerId);

                try {
                    pstmt2.executeQuery();
                    fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                } catch (Exception ex) {
                    assertTrue(ex instanceof UnsupportedOperationException);
                }
            }
        } finally {
            dropObject(AETestConnectionString, "TABLE", customProviderTableName);
            dropObject(AETestConnectionString, "CEK", cekDummy);
            dropObject(AETestConnectionString, "CMK", cmkDummy);

            SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();
        }
    }

    private void insertData(String tableName, int customId, String customName) {
        String sqlQuery = "INSERT INTO " + tableName + " VALUES ( ?, ? )";

        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString + ";sendStringParametersAsUnicode=false;", AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sqlQuery,
                        SQLServerStatementColumnEncryptionSetting.ENABLED)) {
            pstmt.setInt(1, customId);
            pstmt.setString(2, customName);
            pstmt.executeUpdate();
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private void createTableForCustomProvider(String connString, String tableName, String cekName) {
        String sqlQuery = "CREATE TABLE " + tableName + " ("
                + " [CustomerId] [int] ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = [" + cekName
                + "], ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256'), "
                + " [CustomerName] [varchar](50) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (COLUMN_ENCRYPTION_KEY = ["
                + cekName + "], ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256') " + ")";

        try (SQLServerConnection con = PrepUtil.getConnection(connString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.execute(sqlQuery);
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    private boolean cekCacheContainsKey(byte[] encryptedKey,
            SQLServerColumnEncryptionAzureKeyVaultProvider provider) throws Exception {
        assertFalse(null == encryptedKey || 0 == encryptedKey.length);

        String encryptedCEKHexString = TestUtils.byteToHexDisplayString(encryptedKey);

        Field cekCacheField = provider.getClass().getDeclaredField(cekCacheName);
        cekCacheField.setAccessible(true);

        Object fieldValue = cekCacheField.get(provider);
        Method method = fieldValue.getClass().getDeclaredMethod("contains", Object.class);
        method.setAccessible(true);

        boolean result = (boolean) method.invoke(fieldValue, encryptedCEKHexString);

        return result;
    }

    private byte[] signColumnMasterKeyMetadata(SQLServerColumnEncryptionAzureKeyVaultProvider provider,
            String masterKeyPath, boolean allowEnclaveComputations) throws Exception {

        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(Constants.AZURE_KEY_VAULT_NAME.toLowerCase().getBytes(java.nio.charset.StandardCharsets.UTF_16LE));
        md.update(masterKeyPath.toLowerCase().getBytes(java.nio.charset.StandardCharsets.UTF_16LE));
        md.update(Boolean.toString(allowEnclaveComputations).getBytes(java.nio.charset.StandardCharsets.UTF_16LE));

        byte[] dataToSign = md.digest();
        if (null == dataToSign) {
            fail("data to sign is null or empty.");
            Object[] msgArg = {"dataToSign"};
            fail((new MessageFormat(TestResource.getResource("R_objectNullOrEmpty"))).format(msgArg));
        }

        assertTrue(dataToSign.length > 0);

        Method method = provider.getClass().getDeclaredMethod("azureKeyVaultSignHashedData", byte[].class,
                String.class);
        method.setAccessible(true);

        byte[] signature = (byte[]) method.invoke(provider, dataToSign, masterKeyPath);

        if (null == signature || 0 == signature.length) {
            Object[] msgArg = {"Signature of column master key metadata"};
            fail((new MessageFormat(TestResource.getResource("R_objectNullOrEmpty"))).format(msgArg));
        }

        return signature;
    }

    private int getCacheSize(String methodName,
            SQLServerColumnEncryptionAzureKeyVaultProvider provider) throws Exception {
        Method method = provider.getClass().getDeclaredMethod(methodName);
        method.setAccessible(true);

        return (int) method.invoke(provider);
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
                azureKeyVaultProvider = new SQLServerColumnEncryptionAzureKeyVaultProvider(applicationClientID,
                        applicationKey);

            } finally {
                if (null != file) {
                    file.delete();
                }
            }
        }

        return azureKeyVaultProvider;
    }
}
