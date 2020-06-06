/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
public class ErrorMessageTest extends FedauthCommon {

    private static final String ERR_MSG_SQL_AUTH_FAILED_SSL = "The driver could not establish a secure connection to SQL Server by using Secure Sockets Layer (SSL) encryption.";
    private static final String ERR_MSG_BOTH_USERNAME_PASSWORD = "Both \"User\" (or \"UserName\") and \"Password\" connection string keywords must be specified";
    private static final String ERR_MSG_CANNOT_SET_ACCESS_TOKEN = "Cannot set the AccessToken property";
    private static final String ERR_MSG_ACCESS_TOKEN_EMPTY = "AccesToken cannot be empty";

    @BeforeAll
    public static void setupTests() throws Throwable {
        FedauthCommon.getFedauthInfo();
    }

    @Test
    public void testWrongAccessToken() throws SQLException {
        Connection connection = null;
        SQLServerDataSource ds = null;
        try {
            // testWrongAccessToken with connectionStringUserName
            String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";"
                    + "HostNameInCertificate=" + hostNameInCertificate;

            Properties info = new Properties();
            info.setProperty("accesstoken", "test");

            connection = DriverManager.getConnection(connectionUrl, info);

            // testWrongAccessToken with dataSource
            ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setAccessToken("test");
            ds.setHostNameInCertificate(hostNameInCertificate);

            connection = ds.getConnection();
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(e.getMessage());
            }
            assertTrue(e.getMessage().contains(TestResource.getResource("R_loginFailed")));
        } finally {
            if (null != connection) {
                connection.close();
            }
        }
    }

    @Test
    public void testCorrectAccessTokenPassedInConnectionString() {
        String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "accesstoken="
                + accessToken + ";HostNameInCertificate=" + hostNameInCertificate;

        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
            assertTrue(e.getMessage().contains(TestResource.getResource("R_loginFailed")));
        }
    }

    @Test
    public void testNotProvide() throws SQLException {
        Connection connection = null;
        SQLServerDataSource ds = null;
        try {
            // testNotProvide with connectionStringUserName
            String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "userName="
                    + azureUserName + ";password=" + azurePassword + ";";

            connection = DriverManager.getConnection(connectionUrl);
            // testNotProvide with dataSource
            ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setPassword(azurePassword);

            connection = ds.getConnection();

            // testNotProvide with connectionStringUser
            connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "user="
                    + azureUserName + ";password=" + azurePassword + ";";
            connection = DriverManager.getConnection(connectionUrl);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(e.getMessage());
            }

            String wrongUserName = azureUserName.split("@")[1];
            assertTrue(
                    e.getMessage().startsWith("Cannot open server \"" + wrongUserName + "\" requested by the login."));
        } finally {
            if (null != connection) {
                connection.close();
            }
        }
    }

    @Test
    public void testSQLPasswordWithAzureDB() throws SQLException {
        Connection connection = null;
        SQLServerDataSource ds = null;
        try {
            // testSQLPasswordWithAzureDB with connectionStringUserName
            String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "userName="
                    + azureUserName + ";password=" + azurePassword + ";"
                    + "Authentication=SqlPassword;HostNameInCertificate=" + hostNameInCertificate;

            connection = DriverManager.getConnection(connectionUrl);

            // testSQLPasswordWithAzureDB with dataSource
            ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setPassword(azurePassword);
            ds.setAuthentication("SqlPassword");
            ds.setHostNameInCertificate(hostNameInCertificate);

            connection = ds.getConnection();

            // testSQLPasswordWithAzureDB with connectionStringUser
            connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "user="
                    + azureUserName + ";password=" + azurePassword + ";"
                    + "Authentication=SqlPassword;HostNameInCertificate=" + hostNameInCertificate;
            connection = DriverManager.getConnection(connectionUrl);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
            String wrongUserName = azureUserName.split("@")[1];
            assertTrue(
                    e.getMessage().startsWith("Cannot open server \"" + wrongUserName + "\" requested by the login."));
        } finally {
            if (null != connection) {
                connection.close();
            }
        }
    }

    @Tag(Constants.xAzureSQLDB)
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLMI)
    @Test
    public void testSQLPasswordWithUntrustedSqlDB() throws SQLException {
        Connection connection = null;
        try {
            java.util.Properties info = new Properties();
            info.put("Authentication", "SqlPassword");

            connection = DriverManager.getConnection(connectionString, info);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
            assertTrue(e.getMessage().startsWith(ERR_MSG_SQL_AUTH_FAILED_SSL));
        } finally {
            if (null != connection) {
                connection.close();
            }
        }
    }

    @Test
    public void testADPasswordUnregisteredUser() throws SQLException {
        String userName = "abc" + azureUserName;

        Connection connection = null;
        SQLServerDataSource ds = null;
        try {
            // testADPasswordUnregisteredUser with connectionStringUserName
            String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "userName="
                    + userName + ";password=" + azurePassword + ";"
                    + "Authentication=ActiveDirectoryPassword;HostNameInCertificate=" + hostNameInCertificate;

            connection = DriverManager.getConnection(connectionUrl);
            // testADPasswordUnregisteredUser with dataSource
            ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(userName);
            ds.setPassword(azurePassword);
            ds.setAuthentication("ActiveDirectoryPassword");
            ds.setHostNameInCertificate(hostNameInCertificate);

            connection = ds.getConnection();
            // testADPasswordUnregisteredUser with connectionStringUser
            connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "user=" + userName
                    + ";password=" + azurePassword + ";"
                    + "Authentication=ActiveDirectoryPassword;HostNameInCertificate=" + hostNameInCertificate;
            connection = DriverManager.getConnection(connectionUrl);

            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage()
                    .contains("Failed to authenticate the user " + userName
                            + " in Active Directory (Authentication=ActiveDirectoryPassword).")
                    && e.getCause().getCause().getMessage()
                            .contains("To sign into this application, the account must be added to"));
        } finally {
            if (null != connection) {
                connection.close();
            }
        }
    }

    @Tag(Constants.xAzureSQLDB)
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLMI)
    @Test
    public void testAuthenticationAgainstSQLServer() throws SQLException {
        java.util.Properties info = new Properties();
        info.put("TrustServerCertificate", "true");

        Connection connection = null;

        try {
            // testAuthenticationAgainstSQLServer with activedirectorypassword
            info.put("Authentication", "activedirectorypassword");
            connection = DriverManager.getConnection(connectionString, info);

            // testAuthenticationAgainstSQLServer with activedirectoryIntegrated
            info.put("Authentication", "activedirectoryIntegrated");

            // remove the username and password property
            String newConnectionURL = TestUtils.removeProperty(TestUtils.removeProperty(connectionString, "user"),
                    "password");
            connection = DriverManager.getConnection(newConnectionURL, info);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
            assertTrue(e.getMessage().contains(TestResource.getResource("R_loginFailed")));
        } finally {
            if (null != connection) {
                connection.close();
            }
        }
    }

    @Test
    public void testNotSpecified() throws SQLException {
        boolean retry = true;
        int trials = 0;
        Connection connection = null;
        SQLServerDataSource ds = null;
        while (retry && trials < 5) {
            trials++;
            try {
                // testNotSpecified with connectionStringUserName
                String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";"
                        + "userName=" + azureUserName + ";password=" + azurePassword + ";"
                        + "Authentication=NotSpecified;";
                connection = DriverManager.getConnection(connectionUrl);

                // testNotSpecified with dataSource
                ds = new SQLServerDataSource();

                ds.setServerName(azureServer);
                ds.setDatabaseName(azureDatabase);
                ds.setUser(azureUserName);
                ds.setPassword(azurePassword);
                ds.setAuthentication("NotSpecified");

                connection = ds.getConnection();

                // testNotSpecified with connectionStringUser
                connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "user="
                        + azureUserName + ";password=" + azurePassword + ";" + "Authentication=NotSpecified;";
                connection = DriverManager.getConnection(connectionUrl);
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception e) {
                if (!(e instanceof SQLServerException)) {
                    fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                }

                if (e.getMessage().startsWith("The TCP/IP connection to the host")) {
                    System.out.println("Re-attempting connection to " + azureServer);
                    continue;
                }

                String wrongUserName = azureUserName.split("@")[1];
                assertTrue(e.getMessage()
                        .startsWith("Cannot open server \"" + wrongUserName + "\" requested by the login."));
                retry = false;
            } finally {
                if (null != connection) {
                    connection.close();
                }
            }
        }
    }

    @Test
    public void testAccessTokenAgainstSQLServer() throws SQLException {
        java.util.Properties info = new Properties();
        info.put("accesstoken", accessToken);
        info.put("TrustServerCertificate", "true");
        String newConnectionURL = TestUtils.removeProperty(TestUtils.removeProperty(connectionString, "user"),
                "password");
        try (Connection connection = DriverManager.getConnection(newConnectionURL, info)) {
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
            assertTrue(e.getMessage().contains(TestResource.getResource("R_loginFailed")));
        }
    }

    @Test
    public void testADPasswordWrongPassword() throws SQLException {
        Connection connection = null;
        SQLServerDataSource ds = null;
        try {
            // testADPasswordWrongPassword with connectionStringUserName
            String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "userName="
                    + azureUserName + ";password=WrongPassword;"
                    + "Authentication=ActiveDirectoryPassword;HostNameInCertificate=" + hostNameInCertificate;
            connection = DriverManager.getConnection(connectionUrl);
            // testADPasswordWrongPassword with dataSource
            ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setPassword("WrongPassword");
            ds.setAuthentication("ActiveDirectoryPassword");
            ds.setHostNameInCertificate(hostNameInCertificate);

            connection = ds.getConnection();

            // testADPasswordWrongPassword with connectionStringUser
            connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "user="
                    + azureUserName + ";password=WrongPassword;"
                    + "Authentication=ActiveDirectoryPassword;HostNameInCertificate=" + hostNameInCertificate;
            System.out.println("connectionUrl: " + connectionUrl);

            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }

            System.out.println("error msg: " + e.getMessage());
            System.out.println("cause: " + e.getCause().getMessage());
            System.out.println("cause cause: " + e.getCause().getCause().getMessage());

            assertTrue(e.getMessage()
                    .contains("Failed to authenticate the user " + azureUserName
                            + " in Active Directory (Authentication=ActiveDirectoryPassword).")
                    && (e.getCause().getCause().getMessage().toLowerCase().contains("invalid username or password")
                            || e.getCause().getCause().getMessage().contains(
                                    "You've tried to sign in too many times with an incorrect user ID or password.")));

        } finally {
            if (null != connection) {
                connection.close();
            }
        }
    }

    @Test
    public void testSetAuthenticationWithIntegratedSecurityTrue() throws SQLException {
        Connection connection = null;
        String connectionUrl = null;

        try {
            // testSetAuthenticationWithIntegratedSecurityTrue with dataSource
            SQLServerDataSource ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setPassword(azurePassword);
            ds.setAuthentication("ActiveDirectoryPassword");
            ds.setHostNameInCertificate(hostNameInCertificate);
            ds.setIntegratedSecurity(true);
            connection = ds.getConnection();

            // testSetAuthenticationWithIntegratedSecurityTrue with connectionStringUserName
            connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "userName="
                    + azureUserName + ";password=" + azurePassword + ";"
                    + "Authentication=ActiveDirectoryPassword;HostNameInCertificate=" + hostNameInCertificate
                    + ";IntegratedSecurity=true;";
            connection = DriverManager.getConnection(connectionUrl);

            // testSetAuthenticationWithIntegratedSecurityTrue with connectionStringUser
            connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "user="
                    + azureUserName + ";password=" + azurePassword + ";"
                    + "Authentication=ActiveDirectoryPassword;HostNameInCertificate=" + hostNameInCertificate
                    + ";IntegratedSecurity=true;";
            connection = DriverManager.getConnection(connectionUrl);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
            assertTrue(e.getMessage()
                    .startsWith("Cannot set \"Authentication\" with \"IntegratedSecurity\" set to \"true\"."));
        } finally {
            if (null != connection) {
                connection.close();
            }
        }
    }

    @Test
    public void testADIntegratedWithUserAndPassword() throws SQLException {
        Connection connection = null;
        String connectionUrl = null;

        try {
            // testADIntegratedWithUserAndPassword with dataSource
            SQLServerDataSource ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setPassword(azurePassword);
            ds.setAuthentication("ActiveDirectoryIntegrated");
            ds.setHostNameInCertificate(hostNameInCertificate);
            connection = ds.getConnection();

            // testADIntegratedWithUserAndPassword with connectionStringUserName
            connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "userName="
                    + azureUserName + ";password=" + azurePassword + ";"
                    + "Authentication=ActiveDirectoryIntegrated;HostNameInCertificate=" + hostNameInCertificate;
            connection = DriverManager.getConnection(connectionUrl);

            // testADIntegratedWithUserAndPassword with connectionStringUser
            connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "user="
                    + azureUserName + ";password=" + azurePassword + ";"
                    + "Authentication=ActiveDirectoryIntegrated;HostNameInCertificate=" + hostNameInCertificate;
            connection = DriverManager.getConnection(connectionUrl);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
            assertTrue(e.getMessage().startsWith(
                    "Cannot use \"Authentication=ActiveDirectoryIntegrated\" with \"User\", \"UserName\" or \"Password\" connection string keywords."));
        } finally {
            if (null != connection) {
                connection.close();
            }
        }
    }

    @Test
    public void testSetBothAccessTokenAndAuthentication() throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setAuthentication("ActiveDirectoryIntegrated");
            ds.setAccessToken(accessToken);
            ds.setHostNameInCertificate(hostNameInCertificate);

            try (Connection connection = ds.getConnection()) {}
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
            assertTrue(e.getMessage().startsWith(ERR_MSG_CANNOT_SET_ACCESS_TOKEN));
        }
    }

    @Test
    public void testAccessTokenWithIntegratedSecurityTrue() throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setIntegratedSecurity(true);
            ds.setAccessToken(accessToken);
            ds.setHostNameInCertificate(hostNameInCertificate);

            try (Connection connection = ds.getConnection()) {}
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
            assertTrue(e.getMessage().startsWith(ERR_MSG_CANNOT_SET_ACCESS_TOKEN));
        }
    }

    @Test
    public void testAccessTokenWithUserAndPassword() throws SQLException {
        Connection connection = null;

        try {
            // testAccessTokenWithUserAndPassword with dataSource
            SQLServerDataSource ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setAccessToken(accessToken);
            ds.setUser(azureUserName);
            ds.setPassword(azurePassword);
            ds.setHostNameInCertificate(hostNameInCertificate);

            connection = ds.getConnection();
            // testAccessTokenWithUserAndPassword with connectionStringUserName
            String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "userName="
                    + azureUserName + ";password=" + azurePassword + ";" + "HostNameInCertificate="
                    + hostNameInCertificate;
            Properties info = new Properties();
            info.setProperty("accesstoken", accessToken);

            connection = DriverManager.getConnection(connectionUrl, info);

            // testAccessTokenWithUserAndPassword with connectionStringUser
            connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "user="
                    + azureUserName + ";password=" + azurePassword + ";" + "HostNameInCertificate="
                    + hostNameInCertificate;

            info = new Properties();
            info.setProperty("accesstoken", accessToken);

            connection = DriverManager.getConnection(connectionUrl, info);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
            assertTrue(e.getMessage().startsWith(ERR_MSG_CANNOT_SET_ACCESS_TOKEN));
        } finally {
            if (null != connection) {
                connection.close();
            }
        }
    }

    @Test
    public void testAccessTokenEmpty() throws SQLException {
        String accessToken = "";

        try {
            SQLServerDataSource ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setAccessToken(accessToken);
            ds.setHostNameInCertificate(hostNameInCertificate);

            try (Connection connection = ds.getConnection()) {}
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
            assertTrue(e.getMessage().contains(ERR_MSG_ACCESS_TOKEN_EMPTY));
        }
    }

    @Test
    public void testADPasswordWithoutUser() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setServerName(azureServer);
        ds.setDatabaseName(azureDatabase);
        ds.setPassword(azurePassword);
        ds.setAuthentication("ActiveDirectoryPassword");
        ds.setHostNameInCertificate(hostNameInCertificate);

        try {
            try (Connection connection = ds.getConnection()) {}
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
            assertTrue(e.getMessage().startsWith(ERR_MSG_BOTH_USERNAME_PASSWORD));
        }
    }

    @Test
    public void testADPasswordWithoutPassword() throws SQLException {
        Connection connection = null;
        String connectionUrl = null;

        try {
            // testADPasswordWithoutPassword with dataSource
            SQLServerDataSource ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setAuthentication("ActiveDirectoryPassword");
            ds.setHostNameInCertificate(hostNameInCertificate);
            connection = ds.getConnection();

            // testADPasswordWithoutPassword with connectionStringUserName
            connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "userName="
                    + azureUserName + ";" + "Authentication=ActiveDirectoryPassword;HostNameInCertificate="
                    + hostNameInCertificate;
            connection = DriverManager.getConnection(connectionUrl);

            // testADPasswordWithoutPassword with connectionStringUser
            connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "user="
                    + azureUserName + ";" + "Authentication=ActiveDirectoryPassword;HostNameInCertificate="
                    + hostNameInCertificate;
            connection = DriverManager.getConnection(connectionUrl);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
            assertTrue(e.getMessage().startsWith(ERR_MSG_BOTH_USERNAME_PASSWORD));
        } finally {
            if (null != connection) {
                connection.close();
            }
        }
    }

    @Test
    public void testSqlPasswordWithoutUser() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();

        ds.setServerName(azureServer);
        ds.setDatabaseName(azureDatabase);
        ds.setPassword(azurePassword);
        ds.setAuthentication("SqlPassword");
        ds.setHostNameInCertificate(hostNameInCertificate);

        try {
            try (Connection connection = ds.getConnection()) {}
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
            assertTrue(e.getMessage().startsWith(ERR_MSG_BOTH_USERNAME_PASSWORD));
        }
    }

    @Test
    public void testSqlPasswordWithoutPassword() throws SQLException {
        Connection connection = null;

        // testSqlPasswordWithoutPassword with dataSource
        try {
            SQLServerDataSource ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setAuthentication("SqlPassword");
            ds.setHostNameInCertificate(hostNameInCertificate);
            connection = ds.getConnection();
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));

            // testSqlPasswordWithoutPassword with connectionStringUserName
            String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "userName="
                    + azureUserName + ";" + "Authentication=SqlPassword;HostNameInCertificate=" + hostNameInCertificate;

            connection = DriverManager.getConnection(connectionUrl);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));

            // testSqlPasswordWithoutPassword with connectionStringUser
            connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "user="
                    + azureUserName + ";" + "Authentication=SqlPassword;HostNameInCertificate=" + hostNameInCertificate;

            connection = DriverManager.getConnection(connectionUrl);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
            assertTrue(e.getMessage().startsWith(ERR_MSG_BOTH_USERNAME_PASSWORD));
        } finally {
            if (null != connection) {
                connection.close();
            }
        }
    }

    @Test
    public void testInvalidAuthentication() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();

        ds.setServerName(azureServer);
        ds.setDatabaseName(azureDatabase);
        ds.setUser(azureUserName);
        ds.setPassword(azurePassword);
        ds.setAuthentication("ActiveDirectoryPass");
        ds.setHostNameInCertificate(hostNameInCertificate);

        try {
            try (Connection connection = ds.getConnection()) {}
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }
            assertTrue(
                    e.getMessage().startsWith("The authentication value") && e.getMessage().endsWith("is not valid."));
        }
    }
}
