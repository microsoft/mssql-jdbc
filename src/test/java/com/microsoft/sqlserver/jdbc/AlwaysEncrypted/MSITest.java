/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.azure.identity.CredentialUnavailableException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionAzureKeyVaultProvider;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Tests involving MSI authentication
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.MSI)
@Tag(Constants.requireSecret)
public class MSITest extends AESetup {

    /*
     * Test MSI auth
     */
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xSQLv15)
    @Tag(Constants.xSQLv16)
    @Test
    public void testManagedIdentityAuth() throws SQLException {
        String connStr = connectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.USER, "");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.PASSWORD, "");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.AUTHENTICATION, "ActiveDirectoryMSI");

        testSimpleConnect(connStr);

        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.AUTHENTICATION, "ActiveDirectoryManagedIdentity");

        testSimpleConnect(connStr);
    }

    private void testSimpleConnect(String connStr) {
        try (SQLServerConnection con = PrepUtil.getConnection(connStr)) {} catch (Exception e) {
            fail(TestResource.getResource("R_loginFailed") + e.getMessage());
        }
    }

    /*
     * Test Managed Identity auth with Managed Identity client ID
     */
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xSQLv15)
    @Tag(Constants.xSQLv16)
    @Test
    public void testManagedIdentityAuthWithManagedIdentityClientId() throws SQLException {
        String connStr = connectionString;

        // Test with user=<managed-identity-client-id>
        try {
            connStr = TestUtils.addOrOverrideProperty(connStr, Constants.USER, managedIdentityClientId);
            connStr = TestUtils.addOrOverrideProperty(connStr, Constants.PASSWORD, "");
            connStr = TestUtils.addOrOverrideProperty(connStr, Constants.AUTHENTICATION, "ActiveDirectoryMSI");

            // Set msiClientId to incorrect managed identity client ID. Since "User" is set with the ID, "User" should override msiClientId.
            // Otherwise, test should fail with the incorrect "msiClientId" property value because "User" was not overrided
            connStr = TestUtils.addOrOverrideProperty(connStr, Constants.MSICLIENTID,
                    "incorrect-managed-identity-client-id");
            try (SQLServerConnection con = PrepUtil.getConnection(connStr)) {}
            connStr = TestUtils.addOrOverrideProperty(connStr, Constants.AUTHENTICATION,
                    "ActiveDirectoryManagedIdentity");
            try (SQLServerConnection con = PrepUtil.getConnection(connStr)) {}
        } catch (CredentialUnavailableException ce) {
            fail("\"User\" was overrided by incorrect managed identity client ID set in \"msiClientId\" property.");
        }

        // Test with msiClientId=<managed-identity-client-id>
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.USER, "");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.MSICLIENTID, managedIdentityClientId);
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.AUTHENTICATION, "ActiveDirectoryMSI");
        try (SQLServerConnection con = PrepUtil.getConnection(connStr)) {}
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.AUTHENTICATION, "ActiveDirectoryManagedIdentity");
        try (SQLServerConnection con = PrepUtil.getConnection(connStr)) {}
    }

    /*
     * Test Managed Identity auth using datasource
     */
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xSQLv15)
    @Tag(Constants.xSQLv16)
    @Test
    public void testDSManagedIdentityAuth() throws SQLException {
        String connStr = connectionString;

        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.USER, "");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.PASSWORD, "");
        SQLServerDataSource ds = new SQLServerDataSource();
        AbstractTest.updateDataSource(connStr, ds);

        ds.setAuthentication("ActiveDirectoryMSI");

        try (SQLServerConnection con = (SQLServerConnection) ds.getConnection()) {}

        ds.setAuthentication("ActiveDirectoryManagedIdentity");

        try (SQLServerConnection con = (SQLServerConnection) ds.getConnection()) {}
    }

    /*
     * Test Managed Identity auth with a Managed Identity client ID using datasource
     */
    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xSQLv15)
    @Tag(Constants.xSQLv16)
    @Test
    public void testDSManagedIdentityAuthWithManagedIdentityClientId() throws SQLException {
        String connStr = connectionString;

        // Test with user=<managed-identity-client-id>
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.USER, managedIdentityClientId);
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.PASSWORD, "");

        SQLServerDataSource ds = new SQLServerDataSource();
        AbstractTest.updateDataSource(connStr, ds);
        ds.setAuthentication("ActiveDirectoryMSI");
        try (SQLServerConnection con = (SQLServerConnection) ds.getConnection()) {}
        ds.setAuthentication("ActiveDirectoryManagedIdentity");
        try (SQLServerConnection con = (SQLServerConnection) ds.getConnection()) {}

        // Test with msiClientId=<managed-identity-client-id>
        ds.setUser("");
        ds.setMSIClientId(managedIdentityClientId);
        ds.setAuthentication("ActiveDirectoryMSI");
        try (SQLServerConnection con = (SQLServerConnection) ds.getConnection()) {}
        ds.setAuthentication("ActiveDirectoryManagedIdentity");
        try (SQLServerConnection con = (SQLServerConnection) ds.getConnection()) {}
    }

    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xSQLv15)
    @Tag(Constants.xSQLv16)
    @Test
    public void testActiveDirectoryDefaultAuth() throws SQLException {
        String connStr = connectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.USER, managedIdentityClientId);
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.PASSWORD, "");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.AUTHENTICATION, "ActiveDirectoryDefault");

        // With Managed Identity client ID
        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(connStr)) {}

        // Without Managed Identity client ID
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.USER, "");
        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(connStr)) {}
    }

    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xSQLv15)
    @Tag(Constants.xSQLv16)
    @Test
    public void testActiveDirectoryDefaultAuthDS() throws SQLException {
        String connStr = connectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.USER, "");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.PASSWORD, "");

        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setAuthentication("ActiveDirectoryDefault");
        ds.setMSIClientId(managedIdentityClientId);
        AbstractTest.updateDataSource(connStr, ds);

        // With msiClientId property
        try (SQLServerConnection con = (SQLServerConnection) ds.getConnection()) {}

        // Without msiClientId property
        ds.setMSIClientId("");
        try (SQLServerConnection con = (SQLServerConnection) ds.getConnection()) {}

        // With user property
        ds.setUser(managedIdentityClientId);
        try (SQLServerConnection con = (SQLServerConnection) ds.getConnection()) {}

        // Without user property
        ds.setUser("");
        try (SQLServerConnection con = (SQLServerConnection) ds.getConnection()) {}
    }

    /*
     * Test AKV with MSI using datasource
     */
    @Test
    public void testDSAkvWithMSI() throws SQLException {
        String connStr = AETestConnectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_AUTHENTICATION,
                "KeyVaultManagedIdentity");
        SQLServerDataSource ds = new SQLServerDataSource();
        AbstractTest.updateDataSource(connStr, ds);
        testCharAkv(connStr);
    }

    /*
     * Test AKV with with credentials
     */
    @Test
    public void testCharAkvWithCred() throws SQLException {
        // add credentials to connection string
        String connStr = AETestConnectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_AUTHENTICATION, "KeyVaultClientSecret");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_PRINCIPALID, keyStorePrincipalId);
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_SECRET, keyStoreSecret);
        testCharAkv(connStr);
    }

    /*
     * Test AKV with with credentials using deprecated properties
     */
    @Test
    public void testCharAkvWithCredDeprecated() throws SQLException {
        // add deprecated connection properties
        String connStr = AETestConnectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYVAULTPROVIDER_CLIENTID, keyStorePrincipalId);
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYVAULTPROVIDER_CLIENTKEY, keyStoreSecret);
        testCharAkv(connStr);
    }

    /*
     * Test AKV with MSI
     */
    @Test
    public void testCharAkvWithMSI() throws SQLException {
        // set to use Managed Identity for keystore auth
        String connStr = AETestConnectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_AUTHENTICATION,
                "KeyVaultManagedIdentity");
        testCharAkv(connStr);
    }

    /*
     * Test AKV with MSI and and principal id
     */
    @Test
    public void testCharAkvWithMSIandPrincipalId() throws SQLException {
        // set to use Managed Identity for keystore auth and principal id
        String connStr = AETestConnectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_AUTHENTICATION,
                "KeyVaultManagedIdentity");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_PRINCIPALID, keyStorePrincipalId);
        testCharAkv(connStr);
    }

    /*
     * Test AKV with with missing credentials
     */
    @Test
    public void testNumericAkvMissingCred() throws SQLException {
        // set auth type to key vault client secret but do not provide secret
        String connStr = AETestConnectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_AUTHENTICATION, "KeyVaultClientSecret");
        try {
            testNumericAKV(connStr);
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_keyStoreSecretNotSet")), e.getMessage());
        }
    }

    /*
     * Test AKV with with keyStoreSecret secret but no keyStoreAuthentication
     */
    @Test
    public void testNumericAkvSecretNoAuth() throws SQLException {
        // set key store secret but do not specify authentication type
        String connStr = AETestConnectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_SECRET, keyStoreSecret);
        try {
            testNumericAKV(connStr);
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_keyStoreAuthenticationNotSet")),
                    e.getMessage());
        }
    }

    /*
     * Test AKV with with keyStorePrincipalId but no keyStoreAuthentication
     */
    @Test
    public void testNumericAkvPrincipalIdNoAuth() throws SQLException {
        // set principal id but do not specify authentication type
        String connStr = AETestConnectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_PRINCIPALID, keyStorePrincipalId);
        try {
            testNumericAKV(connStr);
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_keyStoreAuthenticationNotSet")),
                    e.getMessage());
        }
    }

    /*
     * Test AKV with with keyStoreLocation but no keyStoreAuthentication
     */
    @Test
    public void testNumericAkvLocationNoAuth() throws SQLException {
        // set key store location but do not specify authentication type
        String connStr = AETestConnectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_LOCATION, "location");
        try {
            testNumericAKV(connStr);
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_keyStoreAuthenticationNotSet")),
                    e.getMessage());
        }
    }

    /*
     * Test AKV with with bad credentials
     */
    @Test
    public void testNumericAkvWithBadCred() throws SQLException {
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

        // add credentials to connection string
        String connStr = AETestConnectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_AUTHENTICATION, "KeyVaultClientSecret");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_PRINCIPALID, "bad");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_SECRET, "bad");
        try {
            testNumericAKV(connStr);
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(TestResource.getResource("R_failedToDecrypt")), e.getMessage());
        }
    }

    /*
     * Test AKV with with credentials
     */
    @Test
    public void testNumericAkvWithCred() throws SQLException {
        // add credentials to connection string
        String connStr = AETestConnectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_AUTHENTICATION, "KeyVaultClientSecret");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_PRINCIPALID, keyStorePrincipalId);
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_SECRET, keyStoreSecret);
        testNumericAKV(connStr);
    }

    /*
     * Test AKV with MSI
     */
    @Test
    public void testNumericAkvWithMSI() throws SQLException {
        // set to use Managed Identity for keystore auth
        String connStr = AETestConnectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_AUTHENTICATION,
                "KeyVaultManagedIdentity");
        testNumericAKV(connStr);
    }

    /*
     * Test AKV with MSI and and principal id
     */
    @Test
    public void testNumericAkvWithMSIandPrincipalId() throws SQLException {
        // set to use Managed Identity for keystore auth and principal id
        String connStr = AETestConnectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_AUTHENTICATION,
                "KeyVaultManagedIdentity");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.KEYSTORE_PRINCIPALID, keyStorePrincipalId);
        testNumericAKV(connStr);
    }

    private void testCharAkv(String connStr) throws SQLException {
        String sql = "select * from " + CHAR_TABLE_AE;
        try (SQLServerConnection con = PrepUtil.getConnection(connStr);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement();
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {
            TestUtils.dropTableIfExists(CHAR_TABLE_AE, stmt);
            createTable(CHAR_TABLE_AE, cekAkv, charTable);
            String[] values = createCharValues(false);
            populateCharNormalCase(values);

            try (ResultSet rs = (stmt == null) ? pstmt.executeQuery() : stmt.executeQuery(sql)) {
                int numberOfColumns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    AECommon.testGetString(rs, numberOfColumns, values);
                    AECommon.testGetObject(rs, numberOfColumns, values);
                }
            }
        }
    }

    private void testNumericAKV(String connStr) throws SQLException {
        String sql = "select * from " + NUMERIC_TABLE_AE;
        try (SQLServerConnection con = PrepUtil.getConnection(connStr);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement();
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {
            TestUtils.dropTableIfExists(NUMERIC_TABLE_AE, stmt);
            createTable(NUMERIC_TABLE_AE, cekAkv, numericTable);
            String[] values = createNumericValues(false);
            populateNumeric(values);

            try (SQLServerResultSet rs = (stmt == null) ? (SQLServerResultSet) pstmt.executeQuery()
                                                        : (SQLServerResultSet) stmt.executeQuery(sql)) {
                int numberOfColumns = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    AECommon.testGetString(rs, numberOfColumns, values);
                    AECommon.testGetObject(rs, numberOfColumns, values);
                    AECommon.testGetBigDecimal(rs, numberOfColumns, values);
                    AECommon.testWithSpecifiedtype(rs, numberOfColumns, values);
                }
            }
        }
    }

    @BeforeEach
    public void registerAKVProvider() throws Exception {
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

        Map<String, SQLServerColumnEncryptionKeyStoreProvider> map = new HashMap<String, SQLServerColumnEncryptionKeyStoreProvider>();
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
                akvProvider = new SQLServerColumnEncryptionAzureKeyVaultProvider(applicationClientID, applicationKey);
                map.put(Constants.AZURE_KEY_VAULT_NAME, akvProvider);
            } finally {
                if (null != file) {
                    file.delete();
                }
            }
        }

        SQLServerConnection.registerColumnEncryptionKeyStoreProviders(map);
    }
}
