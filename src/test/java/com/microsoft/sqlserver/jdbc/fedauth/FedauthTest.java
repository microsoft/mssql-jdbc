/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.SilentParameters;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.fedAuth)
@Tag(Constants.requireSecret)
public class FedauthTest extends FedauthCommon {
    static String charTable = TestUtils
            .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("JDBC_FedAuthTest")));

    private static String accessTokenCallbackConnectionString;

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString, "trustServerCertificate", "true");
        accessTokenCallbackConnectionString = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";";
        setConnection();
    }

    static class TrustStore {
        private File trustStoreFile;

        TrustStore(String certificateName) throws Exception {
            trustStoreFile = File.createTempFile("myTrustStore", null, new File("."));
            trustStoreFile.deleteOnExit();
            KeyStore ks = KeyStore.getInstance(Constants.JKS);
            ks.load(null, null);

            ks.setCertificateEntry(certificateName, getCertificate(certificateName));

            try (FileOutputStream os = new FileOutputStream(trustStoreFile)) {
                ks.store(os, "Any_String_<>_Not_Used_In_This_Code".toCharArray());
                os.flush();
            }
        }

        final String getFileName() throws Exception {
            return trustStoreFile.getCanonicalPath();
        }

        private static java.security.cert.Certificate getCertificate(String certname) throws Exception {
            try (FileInputStream is = new FileInputStream(certname)) {
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                return cf.generateCertificate(is);
            }
        }
    }

    @Test
    public void testActiveDirectoryPassword() throws Exception {
        try (Connection conn = DriverManager.getConnection(adPasswordConnectionStr)) {
            testUserName(conn, azureUserName, SqlAuthentication.ActiveDirectoryPassword);
            testCharTable(conn);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        // connection string with userName
        String connectionUrl = TestUtils.removeProperty(adPasswordConnectionStr, "user") + ";userName=" + azureUserName;
        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            testUserName(conn, azureUserName, SqlAuthentication.ActiveDirectoryPassword);
            testCharTable(conn);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testActiveDirectoryPasswordDS() throws Exception {
        SQLServerDataSource ds = new SQLServerDataSource();

        ds.setServerName(azureServer);
        ds.setDatabaseName(azureDatabase);
        ds.setUser(azureUserName);
        ds.setPassword(azurePassword);
        ds.setAuthentication(SqlAuthentication.ActiveDirectoryPassword.toString());

        try (Connection conn = ds.getConnection()) {
            testUserName(conn, azureUserName, SqlAuthentication.ActiveDirectoryPassword);
            testCharTable(conn);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testActiveDirectoryIntegratedDS() throws Exception {
        org.junit.Assume.assumeTrue(enableADIntegrated);

        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setServerName(azureServer);
        ds.setDatabaseName(azureDatabase);
        ds.setAuthentication(SqlAuthentication.ActiveDirectoryIntegrated.toString());

        try (Connection conn = ds.getConnection()) {
            testUserName(conn, azureUserName, SqlAuthentication.ActiveDirectoryIntegrated);
            testCharTable(conn);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testGroupAuthentication() throws SQLException {
        // connection string with userName
        String connectionUrl = TestUtils.removeProperty(TestUtils.removeProperty(adPasswordConnectionStr, "user"),
                "password") + ";userName=" + azureGroupUserName + ";password=" + azurePassword;
        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            testUserName(conn, azureGroupUserName, SqlAuthentication.ActiveDirectoryPassword);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        // connection string with user
        connectionUrl = TestUtils.removeProperty(TestUtils.removeProperty(adPasswordConnectionStr, "user"), "password")
                + ";user=" + azureGroupUserName + ";password=" + azurePassword;
        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            testUserName(conn, azureGroupUserName, SqlAuthentication.ActiveDirectoryPassword);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testGroupAuthenticationDS() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setServerName(azureServer);
        ds.setDatabaseName(azureDatabase);
        ds.setUser(azureGroupUserName);
        ds.setPassword(azurePassword);
        ds.setAuthentication(SqlAuthentication.ActiveDirectoryPassword.toString());

        try (Connection conn = ds.getConnection()) {
            testUserName(conn, azureGroupUserName, SqlAuthentication.ActiveDirectoryPassword);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testNotValidNotSpecified() throws SQLException {
        testNotValid(SqlAuthentication.NotSpecified.toString(), Constants.FALSE, false);
        testNotValid(SqlAuthentication.NotSpecified.toString(), Constants.FALSE, true);
        testNotValid(SqlAuthentication.NotSpecified.toString(), Constants.TRUE, true);
    }

    @Test
    public void testNotValidSqlPassword() throws SQLException {
        testNotValid(SqlAuthentication.SqlPassword.toString(), Constants.FALSE, true);
        testNotValid(SqlAuthentication.SqlPassword.toString(), Constants.TRUE, true);
    }

    @Test
    public void testNotValidActiveDirectoryIntegrated() throws SQLException {
        org.junit.Assume.assumeTrue(enableADIntegrated);

        testNotValid(SqlAuthentication.ActiveDirectoryIntegrated.toString(), Constants.FALSE, true);
        testNotValid(SqlAuthentication.ActiveDirectoryIntegrated.toString(), Constants.TRUE, true);
    }

    @Test
    public void testNotValidActiveDirectoryPassword() throws SQLException {
        testNotValid(SqlAuthentication.ActiveDirectoryPassword.toString(), Constants.FALSE, true);
        testNotValid(SqlAuthentication.ActiveDirectoryPassword.toString(), Constants.TRUE, true);
    }

    @Test
    public void testValidNotSpecified() throws SQLException {
        testValid(SqlAuthentication.NotSpecified.toString(), Constants.FALSE, false);
        testValid(SqlAuthentication.NotSpecified.toString(), Constants.FALSE, true);
        testValid(SqlAuthentication.NotSpecified.toString(), Constants.TRUE, true);
    }

    @Test
    public void testValidSqlPassword() throws SQLException {
        testValid(SqlAuthentication.SqlPassword.toString(), Constants.FALSE, true);
        testValid(SqlAuthentication.SqlPassword.toString(), Constants.TRUE, true);
    }

    @Test
    public void testValidActiveDirectoryIntegrated() throws SQLException {
        org.junit.Assume.assumeTrue(enableADIntegrated);

        testValid(SqlAuthentication.ActiveDirectoryIntegrated.toString(), Constants.FALSE, true);
        testValid(SqlAuthentication.ActiveDirectoryIntegrated.toString(), Constants.TRUE, true);
    }

    @Test
    public void testValidActiveDirectoryPassword() throws SQLException {
        testValid(SqlAuthentication.ActiveDirectoryPassword.toString(), Constants.FALSE, true);
        testValid(SqlAuthentication.ActiveDirectoryPassword.toString(), Constants.TRUE, true);
    }

    @Test
    public void testCorrectAccessToken() throws SQLException {
        String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase;
        Properties info = new Properties();
        info.setProperty("accesstoken", accessToken);

        try (Connection conn = DriverManager.getConnection(connectionUrl, info)) {
            testUserName(conn, azureUserName, SqlAuthentication.NotSpecified);
            testCharTable(conn);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testCorrectAccessTokenDS() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setServerName(azureServer);
        ds.setDatabaseName(azureDatabase);
        ds.setAccessToken(accessToken);

        try (Connection conn = ds.getConnection()) {} catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test AAD Password Authentication using username/password in connection string, data source and SSL
     * encryption, in addition to application name in order to use different authorities.
     *
     * @throws Exception
     *         if an exception occurs
     */
    @Test
    public void testAADPasswordApplicationName() throws Exception {
        String url = "jdbc:sqlserver://" + kustoServer + ";database=" + azureDatabase + ";user=" + azureUserName
                + ";password=" + azurePassword + ";Authentication="
                + SqlAuthentication.ActiveDirectoryPassword.toString() + ";hostNameInCertificate="
                + hostNameInCertificate + ";applicationName=" + applicationName
                + ";encrypt=true;trustServerCertificate=true;";
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(url);

        try (Connection con = ds.getConnection()) {} catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test AAD Service Principal Authentication using AADSecurePrincipalId/AADSecurePrincipalSecret in connection
     * string, data source and SSL encryption.
     * 
     * @deprecated
     */
    @Deprecated
    @Test
    public void testAADServicePrincipalAuthDeprecated() {
        String url = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";authentication="
                + SqlAuthentication.ActiveDirectoryServicePrincipal + ";AADSecurePrincipalId=" + applicationClientID
                + ";AADSecurePrincipalSecret=" + applicationKey;
        String urlEncrypted = url + ";encrypt=true;trustServerCertificate=true;";
        SQLServerDataSource ds = new SQLServerDataSource();
        updateDataSource(url, ds);
        try (Connection conn1 = DriverManager.getConnection(url); Connection conn2 = ds.getConnection();
                Connection conn3 = DriverManager.getConnection(urlEncrypted)) {
            assertNotNull(conn1);
            assertNotNull(conn2);
            assertNotNull(conn3);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test AAD Service Principal Authentication using username/password in connection string, data source and SSL
     * encryption.
     */
    @Test
    public void testAADServicePrincipalAuth() {
        String url = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";authentication="
                + SqlAuthentication.ActiveDirectoryServicePrincipal + ";Username=" + applicationClientID + ";Password="
                + applicationKey;
        String urlEncrypted = url + ";encrypt=true;trustServerCertificate=true;";
        SQLServerDataSource ds = new SQLServerDataSource();
        updateDataSource(url, ds);
        try (Connection conn1 = DriverManager.getConnection(url); Connection conn2 = ds.getConnection();
                Connection conn3 = DriverManager.getConnection(urlEncrypted)) {
            assertNotNull(conn1);
            assertNotNull(conn2);
            assertNotNull(conn3);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testAADServicePrincipalAuthFailureOnSubsequentConnectionsWithInvalidatedTokenCacheWithInvalidSecret() throws Exception {
        String url = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";authentication="
                + SqlAuthentication.ActiveDirectoryServicePrincipal + ";Username=" + applicationClientID + ";Password="
                + applicationKey;

        String invalidSecretUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";authentication="
                + SqlAuthentication.ActiveDirectoryServicePrincipal + ";Username=" + applicationClientID + ";Password="
                + "invalidSecret";

        // Should succeed on valid secret
        try (Connection connection = DriverManager.getConnection(url)) {}

        // Should fail on invalid secret
        try (Connection connection = DriverManager.getConnection(invalidSecretUrl)) {
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(TestResource.getResource("R_invalidClientSecret")),
                    "Expected R_invalidClientSecret error.");
        }
    }

    @Test
    public void testActiveDirectoryPasswordFailureOnSubsequentConnectionsWithInvalidatedTokenCacheWithInvalidPassword() throws Exception {

        // Should succeed on valid password
        try (Connection conn = DriverManager.getConnection(adPasswordConnectionStr)) {}

        // Should fail on invalid password
        try (Connection conn = DriverManager.getConnection(adPasswordConnectionStr + ";password=invalidPassword;")) {
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {
            MessageFormat form = new MessageFormat(TestResource.getResource("R_invalidAADAuth"));
            Object[] msgArgs = {azureUserName, "ActiveDirectoryPassword"};
            assertTrue(e.getMessage().contains(form.format(msgArgs)), "Expected R_invalidAADAuth error.");
        }
    }

    @Test
    public void testAADServicePrincipalCertAuthFailureOnSubsequentConnectionsWithInvalidatedTokenCacheWithInvalidPassword() throws Exception {
        // Should succeed on valid cert field values
        String url = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";authentication="
                + SqlAuthentication.ActiveDirectoryServicePrincipalCertificate + ";Username="
                + servicePrincipalCertificateApplicationClientId + ";clientCertificate=" + clientCertificate;

        // Should fail on invalid cert field values
        String invalidPasswordUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase
                + ";authentication=" + SqlAuthentication.ActiveDirectoryServicePrincipalCertificate + ";Username="
                + applicationClientID + ";password=invalidPassword;clientCertificate=" + clientCertificate;

        try (Connection conn = DriverManager.getConnection(url)) {}

        try (Connection conn = DriverManager.getConnection(invalidPasswordUrl)) {
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(TestResource.getResource("R_invalidCertFields")),
                    "Expected R_invalidCertFields error.");
        }
    }

    /**
     * Test invalid connection property combinations when using AAD Service Principal Authentication.
     */
    @Test
    public void testAADServicePrincipalAuthWrong() {
        String baseUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";authentication="
                + SqlAuthentication.ActiveDirectoryServicePrincipal + ";";
        // Wrong AADSecurePrincipalSecret provided.
        String url = baseUrl + "AADSecurePrincipalId=" + applicationClientID + ";AADSecurePrincipalSecret=wrongSecret";
        validateException(url, "R_MSALExecution");

        // Wrong AADSecurePrincipalId provided.
        url = baseUrl + "AADSecurePrincipalId=wrongId;AADSecurePrincipalSecret=" + applicationKey;
        validateException(url, "R_MSALExecution");

        // AADSecurePrincipalSecret/password not provided.
        url = baseUrl + "AADSecurePrincipalId=" + applicationClientID;
        validateException(url, "R_NoUserPasswordForActiveServicePrincipal");
        url = baseUrl + "Username=" + applicationClientID;
        validateException(url, "R_NoUserPasswordForActiveServicePrincipal");

        // AADSecurePrincipalId/username not provided.
        url = baseUrl + "AADSecurePrincipalSecret=" + applicationKey;
        validateException(url, "R_NoUserPasswordForActiveServicePrincipal");
        url = baseUrl + "password=" + applicationKey;
        validateException(url, "R_NoUserPasswordForActiveServicePrincipal");

        // Both AADSecurePrincipalId/username and AADSecurePrincipalSecret/password not provided.
        validateException(baseUrl, "R_NoUserPasswordForActiveServicePrincipal");

        // both username/password and AADSecurePrincipalId/AADSecurePrincipalSecret provided
        url = baseUrl + "Username=" + applicationClientID + ";password=" + applicationKey + ";AADSecurePrincipalId="
                + applicationClientID + ";AADSecurePrincipalSecret=" + applicationKey;
        validateException(url, "R_BothUserPasswordandDeprecated");
    }

    /**
     * Test AAD Service Principal Certificate Authentication using username/password in connection string, data source and SSL
     * encryption.
     */
    @Test
    public void testAADServicePrincipalCertAuth() {
        // certificate from AKV has no password
        String url = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";authentication="
                + SqlAuthentication.ActiveDirectoryServicePrincipalCertificate + ";Username="
                + servicePrincipalCertificateApplicationClientId + ";clientCertificate=" + clientCertificate;
        String urlEncrypted = url + ";encrypt=false;trustServerCertificate=true;";

        SQLServerDataSource ds = new SQLServerDataSource();
        updateDataSource(url, ds);
        try (Connection conn1 = DriverManager.getConnection(url); Connection conn2 = ds.getConnection();
                Connection conn3 = DriverManager.getConnection(urlEncrypted)) {
            assertNotNull(conn1);
            assertNotNull(conn2);
            assertNotNull(conn3);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Test invalid connection property combinations when using AAD Service Principal Certificate Authentication.
     */
    @Test
    public void testAADServicePrincipalCertAuthWrong() {
        String baseUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";authentication="
                + SqlAuthentication.ActiveDirectoryServicePrincipalCertificate + ";userName="
                + servicePrincipalCertificateApplicationClientId;

        // no certificate provided.
        String url = baseUrl;
        validateException(url, "R_NoUserOrCertForActiveServicePrincipalCertificate");

        // wrong principalid
        url = baseUrl + "user=wrongId;clientCertificate=" + "cert";
        validateException(url, "R_MSALExecution");

        // bad cert
        url = baseUrl + ";clientCertificate=badCert";
        validateException(url, "R_readCertError");

        // wrong certificate password
        url = baseUrl + ";clientCertificate=" + clientCertificate + ";password=wrongPassword";
        validateException(url, "R_readCertError");

        // wrong certificate key or password
        url = baseUrl + ";password=" + azurePassword + ";clientCertificate=" + clientCertificate
                + ";clientKey=wrongKey;" + "clientPassword=wrongPassword";
        validateException(url, "R_readCertError");
    }

    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.reqExternalSetup)
    @Test
    public void testAccessTokenCallbackClassConnection() throws Exception {
        String cs = TestUtils.addOrOverrideProperty(accessTokenCallbackConnectionString, "accessTokenCallbackClass",
                PooledConnectionTest.AccessTokenCallbackClass.class.getName());
        try (Connection conn1 = DriverManager.getConnection(cs)) {}
    }

    @Test
    public void testAccessTokenCache() {
        try {
            SilentParameters silentParameters = SilentParameters.builder(Collections.singleton(spn + "/.default"))
                    .build();

            // this will fail if not cached
            CompletableFuture<IAuthenticationResult> future = fedauthClientApp.acquireTokenSilently(silentParameters);
            IAuthenticationResult authenticationResult = future.get();
            assertNotNull(authenticationResult.accessToken());
            assertTrue(authenticationResult.accessToken().equals(accessToken), accessToken);
        } catch (Exception e) {
            fail(e.getMessage());
        }

    }

    private static void validateException(String url, String resourceKey) {
        try (Connection conn = DriverManager.getConnection(url)) {
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (SQLException e) {
            assertTrue(e.getMessage().replaceAll("\r\n", "").matches(TestUtils.formatErrorMsg(resourceKey)),
                    e.getMessage());
        }
    }

    private void testValid(String authentication, String encrypt, boolean trustServerCertificate) throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            if (!authentication.equalsIgnoreCase(SqlAuthentication.ActiveDirectoryIntegrated.toString())) {
                ds.setServerName(azureServer);
                ds.setDatabaseName(azureDatabase);
                ds.setUser(azureUserName);
                ds.setPassword(azurePassword);
            } else {
                ds.setServerName(azureServer);
                ds.setDatabaseName(azureDatabase);
            }
            ds.setAuthentication(authentication);
            ds.setEncrypt(encrypt);
            ds.setTrustServerCertificate(trustServerCertificate);

            try (Connection conn = ds.getConnection()) {}
        } catch (Exception e) {
            if (authentication.toLowerCase().contains("activedirectorypassword")) {
                fail(e.getMessage());
            } else if (authentication.toLowerCase().contains("activedirectoryintegrated")) {
                assertTrue(INVALID_EXCEPTION_MSG + ": " + e.getMessage(), e.getMessage().contains(ERR_MSG_LOGIN_FAILED)
                        || e.getMessage().contains(ERR_MSG_FAILED_AUTHENTICATE));
            } else {
                assertTrue(INVALID_EXCEPTION_MSG + ": " + e.getMessage(),
                        e.getMessage().contains(ERR_MSG_CANNOT_OPEN_SERVER)
                                || e.getMessage().startsWith(ERR_TCPIP_CONNECTION));
            }
        }
    }

    private void testNotValid(String authentication, String encrypt,
            boolean trustServerCertificate) throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            if (!authentication.equalsIgnoreCase(SqlAuthentication.ActiveDirectoryIntegrated.toString())) {
                ds.setServerName(azureServer);
                ds.setDatabaseName(azureDatabase);
                ds.setUser(azureUserName);
                ds.setPassword("WrongPassword");
            } else {
                ds.setServerName(azureServer);
                ds.setDatabaseName(azureDatabase);
            }

            ds.setAuthentication(authentication);
            ds.setEncrypt(encrypt);
            ds.setTrustServerCertificate(trustServerCertificate);

            try (Connection conn = ds.getConnection()) {}
            if (!authentication.equalsIgnoreCase(SqlAuthentication.ActiveDirectoryIntegrated.toString())) {
                fail(TestResource.getResource("R_expectedFailPassed"));
            }
        } catch (Exception e) {
            if (authentication.toLowerCase().contains("activedirectory")) {
                assertTrue(INVALID_EXCEPTION_MSG + ": " + e.getMessage(), e.getMessage().contains(ERR_MSG_LOGIN_FAILED)
                        || e.getMessage().contains(ERR_MSG_FAILED_AUTHENTICATE));
            } else {
                assertTrue(INVALID_EXCEPTION_MSG + ": " + e.getMessage(),
                        e.getMessage().contains(ERR_MSG_CANNOT_OPEN_SERVER)
                                || e.getMessage().startsWith(ERR_TCPIP_CONNECTION));
            }
        }
    }

    private void testCharTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            try {
                TestUtils.dropTableIfExists(charTable, stmt);
                createTable(stmt, charTable);
                populateCharTable(conn, charTable);
                testChar(stmt, charTable);
            } finally {
                TestUtils.dropTableIfExists(charTable, stmt);
            }
        }
    }

    @AfterAll
    public static void terminate() throws SQLException {
        try (Connection conn = DriverManager.getConnection(adPasswordConnectionStr);
                Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(charTable, stmt);
        }
    }
}
