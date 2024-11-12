/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.IClientCredential;
import com.microsoft.aad.msal4j.ITokenCacheAccessAspect;
import com.microsoft.aad.msal4j.ITokenCacheAccessContext;
import com.microsoft.aad.msal4j.MsalThrottlingException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.LogManager;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;


@Tag(Constants.fedAuth)
class FedauthTokenCache implements ITokenCacheAccessAspect {
    private static FedauthTokenCache instance = new FedauthTokenCache();
    private final Lock lock = new ReentrantLock();

    private FedauthTokenCache() {}

    static FedauthTokenCache getInstance() {
        return instance;
    }

    private String cache = null;

    @Override
    public void beforeCacheAccess(ITokenCacheAccessContext iTokenCacheAccessContext) {
        lock.lock();
        try {
            if (null != cache && null != iTokenCacheAccessContext && null != iTokenCacheAccessContext.tokenCache()) {
                iTokenCacheAccessContext.tokenCache().deserialize(cache);
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void afterCacheAccess(ITokenCacheAccessContext iTokenCacheAccessContext) {
        lock.lock();
        try {
            if (null != iTokenCacheAccessContext && iTokenCacheAccessContext.hasCacheChanged()
                    && null != iTokenCacheAccessContext.tokenCache())
                cache = iTokenCacheAccessContext.tokenCache().serialize();
        } finally {
            lock.unlock();
        }
    }

    static void clearUserTokenCache() {
        if (null != instance.cache && !instance.cache.isEmpty()) {
            instance.cache = null;
        }
    }
}


@Tag(Constants.fedAuth)
public class FedauthCommon extends AbstractTest {

    static String azureServer = null;
    static String azureDatabase = null;
    static String azureUserName = null;
    static String azurePassword = null;
    static String azureGroupUserName = null;

    static boolean enableADIntegrated = false;

    static String hostNameInCertificate = null;
    static String applicationName = null;
    static String kustoServer = null;
    static String spn = null;
    static String stsurl = null;
    static String fedauthClientId = null;
    static long secondsBeforeExpiration = -1;
    static String accessToken = null;
    static String certificatePassword = null;
    static String[] fedauthJksPaths = null;
    static String[] fedauthJksPathsLinux = null;
    static String[] fedauthJavaKeyAliases = null;

    static final String INVALID_EXCEPTION_MSG = TestResource.getResource("R_invalidExceptionMessage");
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
    static final String ERR_FAULT_ID3342 = "FaultMessage: ID3242";
    static final String ERR_FAULT_AUTH_FAIL = "FaultMessage: Authentication Failure";
    static final String ERR_MSG_NOT_AUTH_AND_IS = TestUtils.R_BUNDLE
            .getString("R_SetAuthenticationWhenIntegratedSecurityTrue");
    static final String ERR_MSG_NOT_AUTH_AND_USER_PASSWORD = TestUtils.R_BUNDLE
            .getString("R_IntegratedAuthenticationWithUserPassword");
    static final String ERR_MSG_SIGNIN_ADD = TestResource.getResource("R_toSigninAdd");
    static final String ERR_MSG_RESULTSET_IS_CLOSED = TestUtils.R_BUNDLE.getString("R_resultsetClosed");
    static final String ERR_MSG_SOCKET_CLOSED = TestResource.getResource("R_socketClosed");
    static final String ERR_TCPIP_CONNECTION = TestResource.getResource("R_tcpipConnectionToHost");
    static final String ERR_MSG_REQUEST_THROTTLED = "Request was throttled";
    static final String ERR_FAILED_FEDAUTH = TestResource.getResource("R_failedFedauth");

    enum SqlAuthentication {
        NotSpecified,
        SqlPassword,
        ActiveDirectoryPassword,
        ActiveDirectoryIntegrated,
        ActiveDirectoryServicePrincipal,
        ActiveDirectoryServicePrincipalCertificate;

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
            } else if (value.toLowerCase(Locale.US)
                    .equalsIgnoreCase(SqlAuthentication.ActiveDirectoryServicePrincipal.toString())) {
                method = SqlAuthentication.ActiveDirectoryServicePrincipal;
            }
            return method;
        }
    }

    static String adPasswordConnectionStr;
    static String adIntegratedConnectionStr;

    @BeforeAll
    public static void getConfigs() throws Exception {
        azureServer = getConfiguredProperty("azureServer");
        azureDatabase = getConfiguredProperty("azureDatabase");
        azureUserName = getConfiguredProperty("azureUserName");
        azurePassword = getConfiguredProperty("azurePassword");
        azureGroupUserName = getConfiguredProperty("azureGroupUserName");

        // password for service principal certificate
        certificatePassword = getConfiguredProperty("certificatePassword");

        String prop = getConfiguredProperty("enableADIntegrated");
        enableADIntegrated = (null != prop && prop.equalsIgnoreCase("true")) ? true : false;

        adPasswordConnectionStr = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";user="
                + azureUserName + ";password=" + azurePassword + ";Authentication="
                + SqlAuthentication.ActiveDirectoryPassword.toString();

        adIntegratedConnectionStr = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase
                + ";Authentication=" + SqlAuthentication.ActiveDirectoryIntegrated.toString();

        fedauthJksPaths = getConfiguredProperty("fedauthJksPaths", "").split(Constants.SEMI_COLON);
        fedauthJavaKeyAliases = getConfiguredProperty("fedauthJavaKeyAliases", "").split(Constants.SEMI_COLON);

        hostNameInCertificate = getConfiguredProperty("hostNameInCertificate");
        applicationName = getConfiguredProperty("applicationName");
        kustoServer = getConfiguredProperty("kustoServer");
        spn = getConfiguredProperty("spn");
        stsurl = getConfiguredProperty("stsurl");
        fedauthClientId = getConfiguredProperty("fedauthClientId");

        getFedauthInfo();

        // reset logging to avoid severe logs
        LogManager.getLogManager().reset();
    }

    /**
     * Get Fedauth info
     * 
     */
    static void getFedauthInfo() {
        int retry = 0;
        long interval = THROTTLE_RETRY_INTERVAL;
        while (retry <= THROTTLE_RETRY_COUNT) {
            try {
                Set<String> scopes = new HashSet<>();
                scopes.add(spn + "/.default");
                if (null == fedauthClientApp) {
                    IClientCredential credential = ClientCredentialFactory.createFromSecret(applicationKey);
                    fedauthClientApp = ConfidentialClientApplication.builder(applicationClientID, credential)
                            .executorService(Executors.newFixedThreadPool(1))
                            .setTokenCacheAccessAspect(FedauthTokenCache.getInstance()).authority(stsurl).build();
                }

                final CompletableFuture<IAuthenticationResult> future = fedauthClientApp
                        .acquireToken(ClientCredentialParameters.builder(scopes).build());

                final IAuthenticationResult authenticationResult = future.get();

                secondsBeforeExpiration = TimeUnit.MILLISECONDS
                        .toSeconds(authenticationResult.expiresOnDate().getTime() - new Date().getTime());
                accessToken = authenticationResult.accessToken();

                retry = THROTTLE_RETRY_COUNT + 1;
            } catch (MsalThrottlingException te) {
                interval = te.retryInMs();
                if (!checkForRetry(te, retry++, interval)) {
                    te.printStackTrace();
                    fail(ERR_FAILED_FEDAUTH + "no more retries: " + te.getMessage());
                }
            } catch (Exception e) {
                e.printStackTrace();
                fail(ERR_FAILED_FEDAUTH + e.getMessage());
            }
        }
    }

    static boolean checkForRetry(Exception e, int retry, long interval) {
        if (retry > THROTTLE_RETRY_COUNT) {
            return false;
        }
        try {
            Thread.sleep(interval);
        } catch (InterruptedException ex) {
            e.printStackTrace();

            fail(ERR_FAILED_FEDAUTH + ex.getMessage());
        }
        return true;
    }

    void testUserName(Connection conn, String user, SqlAuthentication authentication) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery("SELECT SUSER_SNAME()")) {
            rs.next();
            if (SqlAuthentication.ActiveDirectoryPassword == authentication
                    || SqlAuthentication.SqlPassword == authentication) {
                assertTrue(user.equals(rs.getString(1)), rs.getString(1));
            } else if (SqlAuthentication.ActiveDirectoryIntegrated == authentication) {
                if (isWindows) {
                    assertTrue(rs.getString(1).contains(System.getProperty("user.name")), rs.getString(1));
                } else {
                    // cannot verify user in kerberos tickets so just check it's not empty
                    assertTrue(!rs.getString(1).isEmpty());
                }
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
