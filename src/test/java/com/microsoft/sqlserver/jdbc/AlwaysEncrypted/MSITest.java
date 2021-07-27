/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

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
public class MSITest extends AESetup {

    /*
     * Test MSI auth
     */
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xSQLv15)
    @Test
    public void testMSIAuth() throws SQLException {
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

        String connStr = connectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.USER, "");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.PASSWORD, "");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.AUTHENTICATION, "ActiveDirectoryMSI");

        try (SQLServerConnection con = PrepUtil.getConnection(connStr)) {} catch (Exception e) {
            fail(TestResource.getResource("R_loginFailed") + e.getMessage());
        }
    }

    /*
     * Test MSI auth with msiClientId
     */
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xSQLv15)
    @Test
    public void testMSIAuthWithMSIClientId() throws SQLException {
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

        String connStr = connectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.USER, "");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.PASSWORD, "");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.AUTHENTICATION, "ActiveDirectoryMSI");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.MSICLIENTID, msiClientId);

        try (SQLServerConnection con = PrepUtil.getConnection(connStr)) {} catch (Exception e) {
            fail(TestResource.getResource("R_loginFailed") + e.getMessage());
        }
    }

    /*
     * Test MSI auth using datasource
     */
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xSQLv15)
    @Test
    public void testDSMSIAuth() throws SQLException {
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

        String connStr = connectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.USER, "");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.PASSWORD, "");

        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setAuthentication("ActiveDirectoryMSI");
        AbstractTest.updateDataSource(connStr, ds);

        try (Connection con = ds.getConnection(); Statement stmt = con.createStatement()) {} catch (Exception e) {
            fail(TestResource.getResource("R_loginFailed") + e.getMessage());
        }
    }

    /*
     * Test MSI auth with msiClientId using datasource
     */
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xSQLv15)
    @Test
    public void testDSMSIAuthWithMSIClientId() throws SQLException {
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

        String connStr = connectionString;
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.USER, "");
        connStr = TestUtils.addOrOverrideProperty(connStr, Constants.PASSWORD, "");

        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setAuthentication("ActiveDirectoryMSI");
        ds.setMSIClientId(msiClientId);
        AbstractTest.updateDataSource(connStr, ds);

        try (Connection con = ds.getConnection(); Statement stmt = con.createStatement()) {} catch (Exception e) {
            fail(TestResource.getResource("R_loginFailed") + e.getMessage());
        }
    }

    /*
     * Test AKV with MSI using datasource
     */
    @Test
    public void testDSAkvWithMSI() throws SQLException {
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

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
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

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
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

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
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

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
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

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
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

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
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

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
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

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
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

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
            // https://docs.microsoft.com/en-us/azure/active-directory/develop/reference-aadsts-error-codes
            assertTrue(e.getMessage().contains("AADSTS700016"), e.getMessage());
        }
    }

    /*
     * Test AKV with with credentials
     */
    @Test
    public void testNumericAkvWithCred() throws SQLException {
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

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
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

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
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

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
}
