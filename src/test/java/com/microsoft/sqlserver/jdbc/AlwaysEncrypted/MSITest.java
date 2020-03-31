/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.aad.adal4j.AuthenticationException;
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
import static org.junit.jupiter.api.Assertions.assertTrue;


/**
 * Tests involving MSI authentication
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.MSI)
public class MSITest extends AESetup {

    /*
     * Test MSI auth using datasource
     */
    @Test
    public void testDSAuth() throws SQLException {
        // unregister the custom providers registered in AESetup
        SQLServerConnection.unregisterColumnEncryptionKeyStoreProviders();

        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setKeyStoreAuthentication("KeyVaultManagedIdentity");
        AbstractTest.updateDataSource(connectionString, ds);

        try (Connection con = ds.getConnection(); Statement stmt = con.createStatement()) {} catch (Exception e) {
            fail(TestResource.getResource("R_loginFailed") + e.getMessage());
        }
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
            assert (e.getMessage().contains("AuthenticationException"));
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
