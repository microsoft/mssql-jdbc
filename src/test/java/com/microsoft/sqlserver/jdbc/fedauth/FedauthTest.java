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
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.Fedauth)
public class FedauthTest extends FedauthCommon {
    static String charTable = TestUtils
            .escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("JDBC_FedAuthTest")));

    static class TrustStore {
        private File trustStoreFile;

        static final String TRUST_STORE_PASSWORD = "Any_Password_<>_Not_Used_In_This_Code";

        TrustStore(String certificateName) throws Exception {
            trustStoreFile = File.createTempFile("myTrustStore", null, new File("."));
            trustStoreFile.deleteOnExit();
            KeyStore ks = KeyStore.getInstance(Constants.JKS);
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

    @Test
    public void testADPasswordAuthentication() throws Exception {
        try (Connection conn = DriverManager.getConnection(adPasswordConnectionStr)) {
            testUserName(conn, azureUserName);
            testCharTable(conn);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        // connection string with userName
        String connectionUrl = TestUtils.removeProperty(adPasswordConnectionStr, "user") + ";userName=" + azureUserName;
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

        try (Connection conn = ds.getConnection()) {
            testUserName(conn, azureUserName);
            testCharTable(conn);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testADIntegratedAuthenticationDS() throws Exception {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setServerName(azureServer);
        ds.setDatabaseName(azureDatabase);
        ds.setAuthentication("ActiveDirectoryIntegrated");

        try (Connection conn = ds.getConnection()) {
            testCharTable(conn);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testGroupAuthentication() throws SQLException {
        // connection string with userName
        String connectionUrl = TestUtils.removeProperty(adPasswordConnectionStr, "user") + ";userName="
                + azureGroupUserName;
        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            testUserName(conn, azureGroupUserName);
        } catch (Exception e) {
            fail(e.getMessage());
        }

        // connection string with user
        connectionUrl = TestUtils.removeProperty(adPasswordConnectionStr, "user") + ";user" + azureUserName;
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

    @Test
    public void testNotValidActiveDirectoryIntegrated() throws SQLException {
        org.junit.Assume.assumeTrue(isWindows);

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

    @Test
    public void testValidActiveDirectoryIntegrated() throws SQLException {
        org.junit.Assume.assumeTrue(isWindows);

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
        String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase;
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

        try (Connection conn = ds.getConnection()) {} catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void testValid(String authentication, boolean encrypt, boolean trustServerCertificate) throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            if (!authentication.equalsIgnoreCase("ActiveDirectoryIntegrated")) {
                ds.setUser(azureUserName);
                ds.setPassword(azurePassword);
            }
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setAuthentication(authentication);
            ds.setEncrypt(encrypt);
            ds.setTrustServerCertificate(trustServerCertificate);

            try (Connection conn = ds.getConnection()) {}
        } catch (Exception e) {
            if (authentication.toLowerCase().contains("activedirectorypassword")) {
                fail(e.getMessage());
            } else if (authentication.toLowerCase().contains("activedirectoryintegrated")) {
                assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage().contains(ERR_MSG_LOGIN_FAILED)
                        || e.getMessage().contains(ERR_MSG_FAILED_AUTHENTICATE));
            } else {
                assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                        e.getMessage().contains(ERR_MSG_CANNOT_OPEN_SERVER));
            }
        }
    }

    private void testNotValid(String authentication, boolean encrypt,
            boolean trustServerCertificate) throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            if (!authentication.equalsIgnoreCase("ActiveDirectoryIntegrated")) {
                ds.setUser(azureUserName);
                ds.setPassword("WrongPassword");
            }
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setAuthentication(authentication);
            ds.setEncrypt(encrypt);
            ds.setTrustServerCertificate(trustServerCertificate);

            try (Connection conn = ds.getConnection()) {}
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {
            if (authentication.toLowerCase().contains("activedirectory")) {
                assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage().contains(ERR_MSG_LOGIN_FAILED)
                        || e.getMessage().contains(ERR_MSG_FAILED_AUTHENTICATE));
            } else {
                assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                        e.getMessage().contains(ERR_MSG_CANNOT_OPEN_SERVER));
            }
        }
    }

    public static void testChar(Statement stmt, String charTable) throws SQLException {
        try (ResultSet rs = stmt.executeQuery("select * from " + charTable)) {
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
            assertTrue(user.equals(rs.getString(1)));
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
        stmt.execute("create table " + charTable + " (" + "PlainChar char(20) null," + "PlainVarchar varchar(50) null,"
                + "PlainVarcharMax varchar(max) null," + "PlainNchar nchar(30) null,"
                + "PlainNvarchar nvarchar(60) null," + "PlainNvarcharMax nvarchar(max) null" + ");");
    }

    public static void populateCharTable(Connection conn, String charTable) throws SQLException {
        try (PreparedStatement pstmt = conn
                .prepareStatement("insert into " + charTable + " values( " + "?,?,?,?,?,?" + ")")) {
            for (int i = 1; i <= 6; i++) {
                pstmt.setString(i, "hello world!!!");
            }
            pstmt.execute();
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
