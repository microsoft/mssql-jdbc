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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.Fedauth)
public class ErrorMessageTest extends FedauthCommon {

    String userName = "abc" + azureUserName;
    String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase;

    @Test
    public void testWrongAccessTokenWithConnectionStringUserName() throws SQLException {
        try {
            Properties info = new Properties();
            info.setProperty("accesstoken", "test");

            try (Connection connection = DriverManager.getConnection(connectionUrl, info)) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(e.getMessage());
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage().contains(ERR_MSG_LOGIN_FAILED));
        }
    }

    @Test
    public void testWrongAccessTokenWithDatasource() throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setAccessToken("test");

            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(e.getMessage());
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage().contains(ERR_MSG_LOGIN_FAILED));
        }
    }

    @Test
    public void testCorrectAccessTokenPassedInConnectionString() {
        try (Connection connection = DriverManager.getConnection(connectionUrl + ";" + accessToken)) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage().contains(ERR_MSG_LOGIN_FAILED));
        }
    }

    @Test
    public void testNotProvideWithConnectionStringUserName() throws SQLException {
        try (Connection connection = DriverManager
                .getConnection(connectionUrl + ";" + "userName=" + azureUserName + ";password=" + azurePassword)) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(e.getMessage());
            }

            String wrongUserName = azureUserName.split("@")[1];
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage()
                    .startsWith(ERR_MSG_CANNOT_OPEN_SERVER + " \"" + wrongUserName + "\" requested by the login."));
        }
    }

    @Test
    public void testNotProvideWithDatasource() throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setPassword(azurePassword);

            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(e.getMessage());
            }

            String wrongUserName = azureUserName.split("@")[1];
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage()
                    .startsWith(ERR_MSG_CANNOT_OPEN_SERVER + " \"" + wrongUserName + "\" requested by the login."));
        }
    }

    @Test
    public void testNotProvideWithConnectionStringUser() throws SQLException {
        try (Connection connection = DriverManager
                .getConnection(connectionUrl + ";" + "user=" + azureUserName + ";password=" + azurePassword)) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(e.getMessage());
            }

            String wrongUserName = azureUserName.split("@")[1];
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage()
                    .startsWith(ERR_MSG_CANNOT_OPEN_SERVER + " \"" + wrongUserName + "\" requested by the login."));
        }
    }

    @Test
    public void testSQLPasswordWithAzureDBWithConnectionStringUserName() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionUrl + ";" + "userName=" + azureUserName
                + ";password=" + azurePassword + ";" + "Authentication=SqlPassword")) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            String wrongUserName = azureUserName.split("@")[1];
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage()
                    .startsWith(ERR_MSG_CANNOT_OPEN_SERVER + " \"" + wrongUserName + "\" requested by the login."));
        }
    }

    @Test
    public void testSQLPasswordWithAzureDBWithDatasource() throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setPassword(azurePassword);
            ds.setAuthentication("SqlPassword");

            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            String wrongUserName = azureUserName.split("@")[1];
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage()
                    .startsWith(ERR_MSG_CANNOT_OPEN_SERVER + " \"" + wrongUserName + "\" requested by the login."));
        }
    }

    @Test
    public void testSQLPasswordWithAzureDBWithConnectionStringUser() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionUrl + ";" + "user=" + azureUserName
                + ";password=" + azurePassword + ";" + "Authentication=SqlPassword")) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            String wrongUserName = azureUserName.split("@")[1];
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage()
                    .startsWith(ERR_MSG_CANNOT_OPEN_SERVER + " \"" + wrongUserName + "\" requested by the login."));
        }
    }

    @Tag(Constants.xAzureSQLDB)
    @Tag(Constants.xAzureSQLDW)
    @Test
    public void testSQLPasswordWithUntrustedSqlDB() throws SQLException {
        try {
            java.util.Properties info = new Properties();
            info.put("Authentication", "SqlPassword");

            try (Connection connection = DriverManager.getConnection(
                    connectionUrl + ";" + "user=" + azureUserName + ";password=" + azurePassword, info)) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_SQL_AUTH_FAILED_SSL));
        }
    }

    @Test
    public void testADPasswordUnregisteredUserWithConnectionStringUserName() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionUrl + ";" + "userName=" + userName
                + ";password=" + azurePassword + ";" + "Authentication=ActiveDirectoryPassword")) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (SQLServerException e) {
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage()
                            .contains(ERR_MSG_FAILED_AUTHENTICATE + " the user " + userName
                                    + " in Active Directory (Authentication=ActiveDirectoryPassword).")
                            && e.getCause().getCause().getMessage().contains(ERR_MSG_SIGNIN_ADD));
        }
    }

    @Test
    public void testADPasswordUnregisteredUserWithDatasource() throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(userName);
            ds.setPassword(azurePassword);
            ds.setAuthentication("ActiveDirectoryPassword");

            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (SQLServerException e) {
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage()
                            .contains(ERR_MSG_FAILED_AUTHENTICATE + " the user " + userName
                                    + " in Active Directory (Authentication=ActiveDirectoryPassword).")
                            && e.getCause().getCause().getMessage().contains(ERR_MSG_SIGNIN_ADD));
        }
    }

    @Test
    public void testADPasswordUnregisteredUserWithConnectionStringUser() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionUrl + ";" + "user=" + userName + ";password="
                + azurePassword + ";" + "Authentication=ActiveDirectoryPassword")) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (SQLServerException e) {
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage()
                            .contains(ERR_MSG_FAILED_AUTHENTICATE + " the user " + userName
                                    + " in Active Directory (Authentication=ActiveDirectoryPassword).")
                            && e.getCause().getCause().getMessage().contains(ERR_MSG_SIGNIN_ADD));
        }
    }

    @Test
    public void testAuthenticationAgainstSQLServerWithActivedirectorypassword() throws SQLException {
        java.util.Properties info = new Properties();
        info.put("TrustServerCertificate", "true");
        info.put("Authentication", "activedirectorypassword");
        try (Connection connection = DriverManager.getConnection(connectionUrl, info)) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage().contains(ERR_MSG_LOGIN_FAILED));
        }
    }

    @Test
    public void testAuthenticationAgainstSQLServerWithActivedirectoryIntegrated() throws SQLException {
        java.util.Properties info = new Properties();
        info.put("TrustServerCertificate", "true");
        info.put("Authentication", "activedirectoryIntegrated");

        try (Connection connection = DriverManager.getConnection(connectionUrl, info)) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage().contains(ERR_MSG_LOGIN_FAILED));
        }
    }

    @Test
    public void testNotSpecifiedWithConnectionStringUserName() throws SQLException {
        boolean retry = true;
        int trials = 0;
        while (retry && trials < 5) {
            trials++;
            try {
                // testNotSpecified with connectionStringUserName
                try (Connection connection = DriverManager.getConnection(connectionUrl + "userName=" + azureUserName
                        + ";password=" + azurePassword + ";" + "Authentication=NotSpecified;")) {}
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            } catch (Exception e) {
                if (!(e instanceof SQLServerException)) {
                    fail(EXPECTED_EXCEPTION_NOT_THROWN);
                }

                if (e.getMessage().startsWith(TestResource.getResource("R_tcpipConnectionToHost"))) {
                    System.out.println("Re-attempting connection to " + azureServer);
                    continue;
                }

                String wrongUserName = azureUserName.split("@")[1];
                assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage()
                        .startsWith(ERR_MSG_CANNOT_OPEN_SERVER + " \"" + wrongUserName + "\" requested by the login."));
                retry = false;
            }
        }
    }

    @Test
    public void testNotSpecifiedWithDataSource() throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setPassword(azurePassword);
            ds.setAuthentication("NotSpecified");

            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }

            if (e.getMessage().startsWith(TestResource.getResource("R_tcpipConnectionToHost"))) {
                System.out.println("Re-attempting connection to " + azureServer);
            }

            String wrongUserName = azureUserName.split("@")[1];
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage()
                    .startsWith(ERR_MSG_CANNOT_OPEN_SERVER + " \"" + wrongUserName + "\" requested by the login."));
        }
    }

    @Test
    public void testNotSpecifiedWithConnectionStringUser() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionUrl + "user=" + azureUserName + ";password="
                + azurePassword + ";" + "Authentication=NotSpecified;")) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }

            if (e.getMessage().startsWith(TestResource.getResource("R_tcpipConnectionToHost"))) {
                System.out.println("Re-attempting connection to " + azureServer);
            }

            String wrongUserName = azureUserName.split("@")[1];
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage()
                    .startsWith(ERR_MSG_CANNOT_OPEN_SERVER + " \"" + wrongUserName + "\" requested by the login."));
        }
    }

    @Test
    public void testAccessTokenAgainstSQLServer() throws SQLException {
        java.util.Properties info = new Properties();
        info.put("accesstoken", accessToken);
        info.put("TrustServerCertificate", "true");

        try (Connection connection = DriverManager.getConnection(connectionUrl, info)) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage().contains(ERR_MSG_LOGIN_FAILED));
        }
    }

    @Test
    public void testADPasswordWrongPasswordWithConnectionStringUserName() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionUrl + "userName=" + azureUserName
                + ";password=WrongPassword;" + "Authentication=ActiveDirectoryPassword")) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }

            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage()
                    .contains(ERR_MSG_FAILED_AUTHENTICATE + " the user " + azureUserName
                            + " in Active Directory (Authentication=ActiveDirectoryPassword).")
                    && (e.getCause().getCause().getMessage().toLowerCase().contains("invalid username or password")
                            || e.getCause().getCause().getMessage().contains(ERR_MSG_SIGNIN_TOO_MANY)));
        }
    }

    @Test
    public void testADPasswordWrongPasswordWithDatasource() throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setPassword("WrongPassword");
            ds.setAuthentication("ActiveDirectoryPassword");

            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }

            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage()
                    .contains(ERR_MSG_FAILED_AUTHENTICATE + " the user " + azureUserName
                            + " in Active Directory (Authentication=ActiveDirectoryPassword).")
                    && (e.getCause().getCause().getMessage().toLowerCase().contains("invalid username or password")
                            || e.getCause().getCause().getMessage().contains(ERR_MSG_SIGNIN_TOO_MANY)));
        }
    }

    @Test
    public void testADPasswordWrongPasswordWithConnectionStringUser() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionUrl + ";" + "user=" + azureUserName
                + ";password=WrongPassword;" + "Authentication=ActiveDirectoryPassword")) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }

            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(), e.getMessage()
                    .contains(ERR_MSG_FAILED_AUTHENTICATE + " the user " + azureUserName
                            + " in Active Directory (Authentication=ActiveDirectoryPassword).")
                    && (e.getCause().getCause().getMessage().toLowerCase().contains("invalid username or password")
                            || e.getCause().getCause().getMessage().contains(ERR_MSG_SIGNIN_TOO_MANY)));
        }
    }

    @Test
    public void testSetAuthenticationWithIntegratedSecurityTrueWithDatasource() throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setPassword(azurePassword);
            ds.setAuthentication("ActiveDirectoryPassword");
            ds.setIntegratedSecurity(true);

            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_NOT_AUTH_AND_IS));
        }
    }

    @Test
    public void testSetAuthenticationWithIntegratedSecurityTrueWithConnectionStringUserName() throws SQLException {
        try (Connection connection = DriverManager
                .getConnection(connectionUrl + ";" + "userName=" + azureUserName + ";password=" + azurePassword + ";"
                        + "Authentication=ActiveDirectoryPassword" + ";IntegratedSecurity=true;")) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_NOT_AUTH_AND_IS));
        }
    }

    @Test
    public void testSetAuthenticationWithIntegratedSecurityTrueWithConnectionStringUser() throws SQLException {
        try (Connection connection = DriverManager
                .getConnection(connectionUrl + ";" + "user=" + azureUserName + ";password=" + azurePassword + ";"
                        + "Authentication=ActiveDirectoryPassword" + ";IntegratedSecurity=true;")) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_NOT_AUTH_AND_IS));
        }
    }

    @Test
    public void testADIntegratedWithUserAndPasswordWithDataSource() throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setPassword(azurePassword);
            ds.setAuthentication("ActiveDirectoryIntegrated");
            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_NOT_AUTH_AND_USER_PASSWORD));
        }
    }

    @Test
    public void testADIntegratedWithUserAndPasswordWithConnectionStringUserName() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionUrl + ";" + "userName=" + azureUserName
                + ";password=" + azurePassword + ";" + "Authentication=ActiveDirectoryIntegrated")) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_NOT_AUTH_AND_USER_PASSWORD));
        }
    }

    @Test
    public void testADIntegratedWithUserAndPasswordWithConnectionStringUser() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionUrl + ";" + "user=" + azureUserName
                + ";password=" + azurePassword + ";" + "Authentication=ActiveDirectoryIntegrated")) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_NOT_AUTH_AND_USER_PASSWORD));
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

            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_CANNOT_SET_ACCESS_TOKEN));
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

            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_CANNOT_SET_ACCESS_TOKEN));
        }
    }

    @Test
    public void testAccessTokenWithUserAndPasswordWithDatasource() throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setAccessToken(accessToken);
            ds.setUser(azureUserName);
            ds.setPassword(azurePassword);

            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_CANNOT_SET_ACCESS_TOKEN));
        }
    }

    @Test
    public void testAccessTokenWithUserAndPasswordWithConnectionStringUserName() throws SQLException {
        Properties info = new Properties();
        info.setProperty("accesstoken", accessToken);

        try (Connection connection = DriverManager.getConnection(
                connectionUrl + ";" + "userName=" + azureUserName + ";password=" + azurePassword, info)) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_CANNOT_SET_ACCESS_TOKEN));
        }
    }

    @Test
    public void testAccessTokenWithUserAndPasswordWithConnectionStringUser() throws SQLException {
        Properties info = new Properties();
        info.setProperty("accesstoken", accessToken);

        try (Connection connection = DriverManager
                .getConnection(connectionUrl + ";" + "user=" + azureUserName + ";password=" + azurePassword, info)) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_CANNOT_SET_ACCESS_TOKEN));
        }
    }

    @Test
    public void testAccessTokenEmpty() throws SQLException {
        try {
            String accessToken = "";
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setAccessToken(accessToken);

            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().contains(ERR_MSG_ACCESS_TOKEN_EMPTY));
        }
    }

    @Test
    public void testADPasswordWithoutUser() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setServerName(azureServer);
        ds.setDatabaseName(azureDatabase);
        ds.setPassword(azurePassword);
        ds.setAuthentication("ActiveDirectoryPassword");

        try {
            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_BOTH_USERNAME_PASSWORD));
        }
    }

    @Test
    public void testADPasswordWithoutPasswordWithDatasource() throws SQLException {
        try {
            // testADPasswordWithoutPassword with dataSource
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setAuthentication("ActiveDirectoryPassword");
            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_BOTH_USERNAME_PASSWORD));
        }
    }

    @Test
    public void testADPasswordWithoutPasswordWithConnectionStringUserName() throws SQLException {
        try (Connection connection = DriverManager.getConnection(
                connectionUrl + ";" + "userName=" + azureUserName + ";" + "Authentication=ActiveDirectoryPassword")) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_BOTH_USERNAME_PASSWORD));
        }
    }

    @Test
    public void testADPasswordWithoutPasswordWithConnectionStringUser() throws SQLException {
        try (Connection connection = DriverManager.getConnection(
                connectionUrl + ";" + "user=" + azureUserName + ";" + "Authentication=ActiveDirectoryPassword")) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_BOTH_USERNAME_PASSWORD));
        }
    }

    @Test
    public void testSqlPasswordWithoutUser() throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setPassword(azurePassword);
            ds.setAuthentication("SqlPassword");

            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_BOTH_USERNAME_PASSWORD));
        }
    }

    @Test
    public void testSqlPasswordWithoutPasswordWithDatasource() throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setAuthentication("SqlPassword");

            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_BOTH_USERNAME_PASSWORD));
        }
    }

    @Test
    public void testSqlPasswordWithoutPasswordWithConnectionStringUserName() throws SQLException {
        try (Connection connection = DriverManager.getConnection(
                connectionUrl + ";" + "userName=" + azureUserName + ";" + "Authentication=SqlPassword")) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_BOTH_USERNAME_PASSWORD));
        }
    }

    @Test
    public void testSqlPasswordWithoutPasswordWithConnectionStringUser() throws SQLException {
        try (Connection connection = DriverManager
                .getConnection(connectionUrl + ";" + "user=" + azureUserName + ";" + "Authentication=SqlPassword")) {
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith(ERR_MSG_BOTH_USERNAME_PASSWORD));
        }
    }

    @Test
    public void testInvalidAuthentication() throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setPassword(azurePassword);
            ds.setAuthentication("ActiveDirectoryPass");

            try (Connection connection = ds.getConnection()) {}
            fail(EXPECTED_EXCEPTION_NOT_THROWN);
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(EXPECTED_EXCEPTION_NOT_THROWN);
            }
            assertTrue(INVALID_EXCEPION_MSG + ": " + e.getMessage(),
                    e.getMessage().startsWith("The authentication value") && e.getMessage().endsWith("is not valid."));
        }
    }
}
