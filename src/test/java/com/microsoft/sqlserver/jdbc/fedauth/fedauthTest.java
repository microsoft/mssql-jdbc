/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class fedauthTest extends AbstractTest {
    static boolean propertiesFetched = false;
    static String charTable = RandomUtil.getIdentifier("charTableFedAuth");

    static class TrustStore {
        private File trustStoreFile;

        final static String TRUST_STORE_PASSWORD = "Any_Password_<>_Not_Used_In_This_Code";

        TrustStore(String certificateName) throws Exception {
            trustStoreFile = File.createTempFile("myTrustStore", null, new File("."));
            trustStoreFile.deleteOnExit();
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(null, null);

            ks.setCertificateEntry(certificateName, getCertificate(certificateName));

            try (FileOutputStream os = new FileOutputStream(trustStoreFile)) {
                ks.store(os, TRUST_STORE_PASSWORD.toCharArray());
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

    @BeforeAll
    public static void setupTests() throws Throwable {
        getFedauthInfo();
    }

    static void skipIntegratedOnNonWindows() {
        if (!System.getProperty("os.name").toLowerCase(Locale.ENGLISH).startsWith("windows")) {
            enableADIntegrated = false;
            System.out.println("Skip testing ActiveDirectoryIntegrated on non-Windows OS");
        }
    }

    @Test
    public void testADPasswordAuthentication() throws Exception {
        // connection string with userName
        String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "userName="
                + azureUserName + ";password=" + azurePassword + ";"
                + "Authentication=ActiveDirectoryPassword;HostNameInCertificate=" + hostNameInCertificate;

        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            testUserName(conn, azureUserName);
            testCharTable(conn);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        // connection string with user
        connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "user=" + azureUserName
                + ";password=" + azurePassword + ";" + "Authentication=ActiveDirectoryPassword;HostNameInCertificate="
                + hostNameInCertificate;

        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            testUserName(conn, azureUserName);
            testCharTable(conn);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testADPasswordAuthenticationDS() throws Exception {
        SQLServerDataSource ds = new SQLServerDataSource();

        ds.setServerName(azureServer);
        ds.setDatabaseName(azureDatabase);
        ds.setUser(azureUserName);
        ds.setPassword(azurePassword);
        ds.setAuthentication("ActiveDirectoryPassword");
        ds.setHostNameInCertificate(hostNameInCertificate);

        try (Connection conn = ds.getConnection()) {
            testUserName(conn, azureUserName);
            testCharTable(conn);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testGroupAuthentication() throws SQLException {
        // connection string with userName
        String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "userName="
                + azureGroupUserName + ";password=" + azurePassword + ";"
                + "Authentication=ActiveDirectoryPassword;HostNameInCertificate=" + hostNameInCertificate;

        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            testUserName(conn, azureGroupUserName);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        // connection string with user
        connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "user="
                + azureGroupUserName + ";password=" + azurePassword + ";"
                + "Authentication=ActiveDirectoryPassword;HostNameInCertificate=" + hostNameInCertificate;

        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            testUserName(conn, azureGroupUserName);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            testUserName(conn, azureGroupUserName);
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
        ds.setAuthentication("ActiveDirectoryPassword");
        ds.setHostNameInCertificate(hostNameInCertificate);

        try (Connection conn = ds.getConnection()) {
            testUserName(conn, azureGroupUserName);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testNotValidNotSpecified() throws SQLException {
        _testNotValid("Notspecified", false, false);
        _testNotValid("Notspecified", false, true);
        _testNotValid("Notspecified", true, true);
    }

    @Test
    public void testNotValidSqlPassword() throws SQLException {
        _testNotValid("sqlpassword", false, true);
        _testNotValid("sqlpassword", true, true);
    }

    @Test
    public void testNotValidActiveDirectoryIntegrated() throws SQLException {
        _testNotValid("ActiveDirectoryIntegrated", false, true);
        _testNotValid("ActiveDirectoryIntegrated", true, true);
    }

    @Test
    public void testNotValidActiveDirectoryPassword() throws SQLException {
        _testNotValid("ActiveDirectoryPassword", false, true);
        _testNotValid("ActiveDirectoryPassword", true, true);
    }

    private void _testNotValid(String authentication, boolean encrypt,
            boolean trustServerCertificate) throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            if (!authentication.equalsIgnoreCase("ActiveDirectoryIntegrated")) {
                ds.setUser(azureUserName);
                ds.setPassword("WrongPassword");
            }
            ds.setAuthentication(authentication);
            ds.setEncrypt(encrypt);
            ds.setTrustServerCertificate(trustServerCertificate);

            try (Connection conn = ds.getConnection()) {}

            fail(TestResource.getResource("R_expectedFailPassed"));

        } catch (Exception e) {
            if (authentication.equalsIgnoreCase("ActiveDirectoryIntegrated")) {
                skipIntegratedOnNonWindows();
            }

            if (authentication.toLowerCase().contains("activedirectory")) {
                assertTrue(e.getMessage().contains(TestResource.getResource("R_loginFailed"))
                        || e.getMessage().contains("Failed to authenticate"));

            } else {
                assertTrue(e.getMessage().contains("Cannot open server"));
            }
        }
    }

    @Test
    public void testValidNotSpecified() throws SQLException {
        _testValid("Notspecified", false, false);
        _testValid("Notspecified", false, true);
        _testValid("Notspecified", true, true);
    }

    @Test
    public void testValidSqlPassword() throws SQLException {
        _testValid("sqlpassword", false, true);
        _testValid("sqlpassword", true, true);
    }

    @Test
    public void testValidActiveDirectoryIntegrated() throws SQLException {
        _testValid("ActiveDirectoryIntegrated", false, true);
        _testValid("ActiveDirectoryIntegrated", true, true);
    }

    @Test
    public void testValidActiveDirectoryPassword() throws SQLException {
        _testValid("ActiveDirectoryPassword", false, true);
        _testValid("ActiveDirectoryPassword", true, true);
    }

    @Test
    private void _testValid(String authentication, boolean encrypt,
            boolean trustServerCertificate) throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            if (!authentication.equalsIgnoreCase("ActiveDirectoryIntegrated")) {
                ds.setUser(azureUserName);
                ds.setPassword("WrongPassword");
            }

            ds.setAuthentication(authentication);
            ds.setEncrypt(encrypt);
            ds.setTrustServerCertificate(trustServerCertificate);

            try (Connection conn = ds.getConnection()) {}

            fail(TestResource.getResource("R_expectedFailPassed"));

        } catch (Exception e) {
            if (authentication.equalsIgnoreCase("ActiveDirectoryIntegrated")) {
                skipIntegratedOnNonWindows();
            }

            if (authentication.toLowerCase().contains("activedirectory")) {
                assertTrue(e.getMessage().contains(TestResource.getResource("R_loginFailed"))
                        || e.getMessage().contains("Failed to authenticate"));

            } else {
                assertTrue(e.getMessage().contains("Cannot open server"));
            }
        }
    }

    @Test
    public void testSQLPasswordWithTrustedSqlDB() throws SQLException {
        java.util.Properties info = new Properties();
        info.put("Authentication", "SqlPassword");

        try {
            String currentDir = System.getProperty("user.dir");
            System.out.println("Current dir using System:" +currentDir);
            String certificateName = TestUtils.getServerNameFromUrl(connectionString).trim() + ".cer";
            String ts = (new TrustStore(certificateName)).getFileName();
            info.put("trustStore", (new TrustStore(certificateName)).getFileName());
        } catch (Exception e) {
            fail(e.getMessage());
        }

        try (Connection conn = DriverManager.getConnection(connectionString, info)) {
            testUserName(conn, "sa");
            testCharTable(conn);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testCorrectAccessToken() throws SQLException {
        String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";"
                + "HostNameInCertificate=" + hostNameInCertificate;
        Properties info = new Properties();
        info.setProperty("accesstoken", accessToken);

        try (Connection conn = DriverManager.getConnection(connectionUrl, info)) {
            testUserName(conn, azureUserName);
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
        ds.setHostNameInCertificate(hostNameInCertificate);

        try (Connection conn = ds.getConnection()) {} catch (Exception e) {
            fail(e.getMessage());
        }
    }

    public static SQLServerDataSource getDataSource() {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setServerName(msAzureServer);
        ds.setDatabaseName(msAzureDatabase);
        ds.setUser(msAzureUserName);
        ds.setPassword(msAzurePassword);
        ds.setAuthentication("ActiveDirectoryPassword");
        ds.setHostNameInCertificate(hostNameInCertificate);
        ds.setColumnEncryptionSetting("enabled");
        ds.setKeyStoreAuthentication("JavaKeyStorePassword");
        ds.setKeyStoreLocation(jksPaths[0]);
        ds.setKeyStoreSecret(secretstrJks);
        return ds;
    }

    static void getFedauthInfo() {
        TestUtils.getPropertiesFromFile();
        try {
            AuthenticationContext context = new AuthenticationContext(stsurl, false, Executors.newFixedThreadPool(1));
            Future<AuthenticationResult> future = context.acquireToken(spn, fedauthClientId, azureUserName,
                    azurePassword, null);
            secondsBeforeExpiration = future.get().getExpiresAfter();
            accessToken = future.get().getAccessToken();
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void createTable(String charTable, Statement stmt) throws SQLException {
        stmt.execute("create table [" + charTable + "] (" + "PlainChar char(20) null,"
                + "PlainVarchar varchar(50) null," + "PlainVarcharMax varchar(max) null," + "PlainNchar nchar(30) null,"
                + "PlainNvarchar nvarchar(60) null," + "PlainNvarcharMax nvarchar(max) null" + ");");
    }

    private static void dropTable(String charTable, Statement stmt) throws SQLException {
        stmt.executeUpdate("if object_id('" + TestUtils.escapeSingleQuotes(charTable) + "','U') is not null"
                + " drop table " + AbstractSQLGenerator.escapeIdentifier(charTable));
    }

    private void populateCharTable(String charTable, Connection conn) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement("insert into "
                + AbstractSQLGenerator.escapeIdentifier(charTable) + " values( " + "?,?,?,?,?,?" + ")")) {
            for (int i = 1; i <= 6; i++) {
                pstmt.setString(i, "hello world!!!");
            }
            pstmt.execute();
        }
    }

    private void testChar(String charTable, Statement stmt) throws SQLException {
        try (ResultSet rs = stmt.executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(charTable))) {
            int numberOfColumns = rs.getMetaData().getColumnCount();
            rs.next();
            for (int i = 1; i <= numberOfColumns; i++) {
                assertTrue(rs.getString(i).trim().equals("hello world!!!"));
            }
        }
    }

    private void testUserName(Connection conn, String user) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT SUSER_SNAME()")) {
            rs.next();
            String retrievedUserName = rs.getString(1);
            assertTrue(retrievedUserName.equals(user));
        }
    }

    private void testCharTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            try {
                dropTable(charTable, stmt);
                createTable(charTable, stmt);
                populateCharTable(charTable, conn);
                testChar(charTable, stmt);
            } finally {
                dropTable(charTable, stmt);
            }
        }
    }

    @AfterAll
    public static void terminate() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            dropTable(charTable, stmt);
        }
    }
}
