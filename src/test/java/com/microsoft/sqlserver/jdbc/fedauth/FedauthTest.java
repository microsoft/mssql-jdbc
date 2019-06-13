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
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
public class FedauthTest extends AbstractTest {
    static String charTable = RandomUtil.getIdentifier("charTableFedAuth");

    static class TrustStore {
        private File trustStoreFile;

        static final String TRUST_STORE_PASSWORD = "Any_Password_<>_Not_Used_In_This_Code";

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
        testNotValid("Notspecified", false, false);
        testNotValid("Notspecified", false, true);
        testNotValid("Notspecified", true, true);
    }

    @Test
    public void testNotValidSqlPassword() throws SQLException {
        testNotValid("SqlPassword", false, true);
        testNotValid("SqlPassword", true, true);
    }

    @Tag(Constants.xWindows)
    @Test
    public void testNotValidActiveDirectoryIntegrated() throws SQLException {
        testNotValid("ActiveDirectoryIntegrated", false, true);
        testNotValid("ActiveDirectoryIntegrated", true, true);
    }

    @Test
    public void testNotValidActiveDirectoryPassword() throws SQLException {
        testNotValid("ActiveDirectoryPassword", false, true);
        testNotValid("ActiveDirectoryPassword", true, true);
    }

    @Test
    public void testValidNotSpecified() throws SQLException {
        testValid("Notspecified", false, false);
        testValid("Notspecified", false, true);
        testValid("Notspecified", true, true);
    }

    @Tag(Constants.xAzureSQLDB)
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLMI)
    @Test
    public void testValidSqlPassword() throws SQLException {
        testValid("SqlPassword", false, true);
        testValid("SqlPassword", true, true);
    }

    @Tag(Constants.xWindows)
    @Test
    public void testValidActiveDirectoryIntegrated() throws SQLException {
        testValid("ActiveDirectoryIntegrated", false, true);
        testValid("ActiveDirectoryIntegrated", true, true);
    }

    @Test
    public void testValidActiveDirectoryPassword() throws SQLException {
        testValid("ActiveDirectoryPassword", false, true);
        testValid("ActiveDirectoryPassword", true, true);
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
        ds.setServerName(azureServer);
        ds.setDatabaseName(azureDatabase);
        ds.setUser(azureUserName);
        ds.setPassword(azurePassword);
        ds.setAuthentication("ActiveDirectoryPassword");
        ds.setHostNameInCertificate(hostNameInCertificate);
        ds.setColumnEncryptionSetting("enabled");
        ds.setKeyStoreAuthentication("JavaKeyStorePassword");
        ds.setKeyStoreLocation(jksPaths[0]);
        ds.setKeyStoreSecret(secretstrJks);
        return ds;
    }

    private void testValid(String authentication, boolean encrypt, boolean trustServerCertificate) throws SQLException {
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
            if (authentication.toLowerCase().contains("activedirectory")) {
                assertTrue(e.getMessage().contains(TestResource.getResource("R_loginFailed"))
                        || e.getMessage().contains("Failed to authenticate"));

            } else {
                assertTrue(e.getMessage().contains("Cannot open server"));
            }
        }
    }

    private void testNotValid(String authentication, boolean encrypt,
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
            if (authentication.toLowerCase().contains("activedirectory")) {
                assertTrue(e.getMessage().contains(TestResource.getResource("R_loginFailed"))
                        || e.getMessage().contains("Failed to authenticate"));

            } else {
                assertTrue(e.getMessage().contains("Cannot open server"));
            }
        }
    }

    public static void testChar(Statement stmt, String charTable) throws SQLException {
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
                TestUtils.dropTableIfExists(charTable, stmt);
                createTable(stmt, charTable);
                populateCharTable(conn, charTable);
                testChar(stmt, charTable);
            } finally {
                TestUtils.dropTableIfExists(charTable, stmt);
            }
        }
    }

    public static void createTable(Statement stmt, String charTable) throws SQLException {
        stmt.execute("create table " + AbstractSQLGenerator.escapeIdentifier(charTable) + " ("
                + "PlainChar char(20) null," + "PlainVarchar varchar(50) null," + "PlainVarcharMax varchar(max) null,"
                + "PlainNchar nchar(30) null," + "PlainNvarchar nvarchar(60) null,"
                + "PlainNvarcharMax nvarchar(max) null" + ");");
    }

    public static void populateCharTable(Connection conn, String charTable) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement("insert into "
                + AbstractSQLGenerator.escapeIdentifier(charTable) + " values( " + "?,?,?,?,?,?" + ")")) {
            for (int i = 1; i <= 6; i++) {
                pstmt.setString(i, "hello world!!!");
            }
            pstmt.execute();
        }
    }

    @AfterAll
    public static void terminate() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(charTable, stmt);
        }
    }
}
