/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.LogManager;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;


@Tag(Constants.fedAuth)
public class FedauthCommon extends AbstractTest {

    static String azureServer = null;
    static String azureDatabase = null;
    static String azureUserName = null;
    static String azurePassword = null;
    static String azureGroupUserName = null;

    static boolean enableADIntegrated = false;

    static String spn = null;
    static String stsurl = null;
    static String fedauthClientId = null;
    static long secondsBeforeExpiration = -1;
    static String accessToken = null;
    static String[] fedauthJksPaths = null;
    static String[] fedauthJksPathsLinux = null;
    static String[] fedauthJavaKeyAliases = null;

    static final String INVALID_EXCEPION_MSG = TestResource.getResource("R_invalidExceptionMessage");
    static final String EXPECTED_EXCEPTION_NOT_THROWN = TestResource.getResource("R_expectedExceptionNotThrown");
    static final String ERR_MSG_LOGIN_FAILED = TestResource.getResource("R_loginFailed");
    static final String ERR_MSG_BOTH_USERNAME_PASSWORD_ACTIVEPASSWORD = TestUtils.R_BUNDLE
            .getString("R_NoUserPasswordForActivePassword");
    static final String ERR_MSG_BOTH_USERNAME_PASSWORD_SQLPASSWORD = TestUtils.R_BUNDLE
            .getString("R_NoUserPasswordForSqlPassword");
    static final String ERR_MSG_CANNOT_SET_ACCESS_TOKEN = "Cannot set the AccessToken property";
    static final String ERR_MSG_ACCESS_TOKEN_EMPTY = TestUtils.R_BUNDLE.getString("R_AccessTokenCannotBeEmpty");
    static final String ERR_MSG_FAILED_AUTHENTICATE = TestResource.getResource("R_failedToAuthenticate");
    static final String ERR_MSG_CANNOT_OPEN_SERVER = TestResource.getResource("R_cannotOpenServer");
    static final String ERR_MSG_CONNECTION_IS_CLOSED = TestResource.getResource("R_connectionIsClosed");
    static final String ERR_MSG_CONNECTION_CLOSED = TestResource.getResource("R_connectionClosed");
    static final String ERR_MSG_HAS_CLOSED = TestResource.getResource("R_hasClosed");
    static final String ERR_MSG_HAS_BEEN_CLOSED = TestResource.getResource("R_hasBeenClosed");
    static final String ERR_MSG_SIGNIN_TOO_MANY = TestResource.getResource("R_signinTooManyTimes");
    static final String ERR_MSG_NOT_AUTH_AND_IS = TestUtils.R_BUNDLE
            .getString("R_SetAuthenticationWhenIntegratedSecurityTrue");
    static final String ERR_MSG_NOT_AUTH_AND_USER_PASSWORD = TestUtils.R_BUNDLE
            .getString("R_IntegratedAuthenticationWithUserPassword");
    static final String ERR_MSG_SIGNIN_ADD = TestResource.getResource("R_toSigninAdd");
    static final String ERR_MSG_RESULTSET_IS_CLOSED = TestUtils.R_BUNDLE.getString("R_resultsetClosed");
    static final String ERR_MSG_SOCKET_CLOSED = TestResource.getResource("R_socketClosed");
    static final String ERR_TCPIP_CONNECTION = TestResource.getResource("R_tcpipConnectionToHost");

    enum SqlAuthentication {
        NotSpecified,
        SqlPassword,
        ActiveDirectoryPassword,
        ActiveDirectoryIntegrated;

        static SqlAuthentication valueOfString(String value) throws SQLServerException {
            SqlAuthentication method = null;

            if (value.toLowerCase(Locale.US).equalsIgnoreCase(SqlAuthentication.NotSpecified.toString())) {
                method = SqlAuthentication.NotSpecified;
            } else if (value.toLowerCase(Locale.US).equalsIgnoreCase(SqlAuthentication.SqlPassword.toString())) {
                method = SqlAuthentication.SqlPassword;
            } else if (value.toLowerCase(Locale.US)
                    .equalsIgnoreCase(SqlAuthentication.ActiveDirectoryPassword.toString())) {
                method = SqlAuthentication.ActiveDirectoryPassword;
            } else if (value.toLowerCase(Locale.US)
                    .equalsIgnoreCase(SqlAuthentication.ActiveDirectoryIntegrated.toString())) {
                method = SqlAuthentication.ActiveDirectoryIntegrated;
            }
            return method;
        }
    }

    static String adPasswordConnectionStr;
    static String adIntegratedConnectionStr;

    @BeforeEach
    public void setupEachTest() {
        getFedauthInfo();
    }

    @BeforeAll
    public static void getConfigs() throws Exception {
        azureServer = getConfiguredProperty("azureServer");
        azureDatabase = getConfiguredProperty("azureDatabase");
        azureUserName = getConfiguredProperty("azureUserName");
        azurePassword = getConfiguredProperty("azurePassword");
        azureGroupUserName = getConfiguredProperty("azureGroupUserName");

        String prop = getConfiguredProperty("enableADIntegrated");
        enableADIntegrated = (isWindows && null != prop && prop.equalsIgnoreCase("true")) ? true : false;

        adPasswordConnectionStr = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";user="
                + azureUserName + ";password=" + azurePassword + ";Authentication="
                + SqlAuthentication.ActiveDirectoryPassword.toString();

        adIntegratedConnectionStr = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase
                + ";Authentication=" + SqlAuthentication.ActiveDirectoryIntegrated.toString();

        fedauthJksPaths = getConfiguredProperty("fedauthJksPaths", "").split(Constants.SEMI_COLON);
        fedauthJavaKeyAliases = getConfiguredProperty("fedauthJavaKeyAliases", "").split(Constants.SEMI_COLON);

        spn = getConfiguredProperty("spn");
        stsurl = getConfiguredProperty("stsurl");
        fedauthClientId = getConfiguredProperty("fedauthClientId");

        // reset logging to avoid severe logs
        LogManager.getLogManager().reset();
    }

    /**
     * Get Fedauth info
     * 
     */
    static void getFedauthInfo() {
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

    void testUserName(Connection conn, String user, SqlAuthentication authentication) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT SUSER_SNAME()")) {
            rs.next();
            if (SqlAuthentication.ActiveDirectoryIntegrated != authentication) {
                assertTrue(user.equals(rs.getString(1)));
            } else {
                assertTrue(rs.getString(1).contains(System.getProperty("user.name")));
            }
        }
    }

    void testChar(Statement stmt, String charTable) throws SQLException {
        try (ResultSet rs = stmt.executeQuery("select * from " + charTable)) {
            int numberOfColumns = rs.getMetaData().getColumnCount();
            rs.next();
            for (int i = 1; i <= numberOfColumns; i++) {
                assertTrue(rs.getString(i).trim().equals("hello world!!!"));
            }
        }
    }

    void createTable(Statement stmt, String charTable) throws SQLException {
        try {
            stmt.execute(
                    "create table " + charTable + " (" + "PlainChar char(20) null," + "PlainVarchar varchar(50) null,"
                            + "PlainVarcharMax varchar(max) null," + "PlainNchar nchar(30) null,"
                            + "PlainNvarchar nvarchar(60) null," + "PlainNvarcharMax nvarchar(max) null" + ");");
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    void populateCharTable(Connection conn, String charTable) throws SQLException {
        try (PreparedStatement pstmt = conn
                .prepareStatement("insert into " + charTable + " values( " + "?,?,?,?,?,?" + ")")) {
            for (int i = 1; i <= 6; i++) {
                pstmt.setString(i, "hello world!!!");
            }
            pstmt.execute();
        }
    }
}
