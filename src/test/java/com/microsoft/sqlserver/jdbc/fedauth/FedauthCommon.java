package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.LogManager;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;


@Tag(Constants.Fedauth)
public class FedauthCommon extends AbstractTest {

    static String azureServer = null;
    static String azureDatabase = null;
    static String azureUserName = null;
    static String azurePassword = null;
    static String azureGroupUserName = null;

    static boolean enableADIntegrated = false;
    static String adIntegratedAzureServer = null;
    static String adIntegratedAzureDatabase = null;
    static String adIntegratedAzureUserName = null;
    static String adIntegratedAzurePassword = null;

    static String spn = null;
    static String stsurl = null;
    static String fedauthClientId = null;
    static long secondsBeforeExpiration = -1;
    static String accessToken = null;
    static String hostNameInCertificate = null;
    static String[] fedauthJksPaths = null;
    static String[] fedauthJksPathsLinux = null;
    static String[] fedauthJavaKeyAliases = null;

    static final String INVALID_EXCEPION_MSG = TestResource.getResource("R_invalidExceptionMessage");
    static final String EXPECTED_EXCEPTION_NOT_THROWN = TestResource.getResource("R_expectedExceptionNotThrown");
    static final String ERR_MSG_LOGIN_FAILED = TestResource.getResource("R_loginFailed");
    static final String ERR_MSG_SQL_AUTH_FAILED_SSL = TestResource.getResource("R_sslConnectionError");
    static final String ERR_MSG_BOTH_USERNAME_PASSWORD = "Both \"User\" (or \"UserName\") and \"Password\" connection string keywords must be specified";
    static final String ERR_MSG_CANNOT_SET_ACCESS_TOKEN = "Cannot set the AccessToken property";
    static final String ERR_MSG_ACCESS_TOKEN_EMPTY = "AccesToken cannot be empty";
    static final String ERR_MSG_FAILED_AUTHENTICATE = TestResource.getResource("R_failedToAuthenticate");
    static final String ERR_MSG_CANNOT_OPEN_SERVER = TestResource.getResource("R_cannotOpenServer");
    static final String ERR_MSG_CONNECTION_IS_CLOSED = TestResource.getResource("R_connectionIsClosed");
    static final String ERR_MSG_CONNECTION_CLOSED = TestResource.getResource("R_connectionClosed");
    static final String ERR_MSG_HAS_CLOSED = TestResource.getResource("R_hasClosed");
    static final String ERR_MSG_HAS_BEEN_CLOSED = TestResource.getResource("R_hasBeenClosed");
    static final String ERR_MSG_SIGNIN_TOO_MANY = TestResource.getResource("R_signinTooManyTimes");
    static final String ERR_MSG_NOT_AUTH_AND_IS = TestResource.getResource("R_notAuthandIS");
    static final String ERR_MSG_NOT_AUTH_AND_USER_PASSWORD = TestResource.getResource("R_notAuthandUserPassword");
    static final String ERR_MSG_SIGNIN_ADD = TestResource.getResource("R_toSigninAdd");
    static final String ERR_MSG_RESULTSET_IS_CLOSED = TestResource.getResource("R_resultset_IsClosed");

    @BeforeAll
    public static void getConfigs() throws Exception {
        azureServer = getConfiguredProperty("azureServer");
        azureDatabase = getConfiguredProperty("azureDatabase");
        azureUserName = getConfiguredProperty("azureUserName");
        azurePassword = getConfiguredProperty("azurePassword");
        azureGroupUserName = getConfiguredProperty("azureGroupUserName");

        adIntegratedAzureServer = getConfiguredProperty("adIntegratedAzureServer");
        adIntegratedAzureDatabase = getConfiguredProperty("adIntegratedAzureDatabase");
        adIntegratedAzureUserName = getConfiguredProperty("adIntegratedAzureUserName");
        adIntegratedAzurePassword = getConfiguredProperty("adIntegratedAzurePassword");

        fedauthJksPaths = getConfiguredProperty("fedauthJksPaths", "").split(Constants.SEMI_COLON);
        if (!isWindows) {
            fedauthJksPaths = getConfiguredProperty("fedauthJksPaths", "").split(Constants.SEMI_COLON);
        }
        fedauthJavaKeyAliases = getConfiguredProperty("fedauthJavaKeyAliases", "").split(Constants.SEMI_COLON);

        enableADIntegrated = !System.getProperty("os.name").startsWith(
                "Windows") ? false
                           : getConfiguredProperty("enableADIntegrated", "").equalsIgnoreCase("true") ? true : false;

        hostNameInCertificate = getConfiguredProperty("hostNameInCertificate");

        spn = getConfiguredProperty("spn");
        stsurl = getConfiguredProperty("stsurl");
        fedauthClientId = getConfiguredProperty("fedauthClientId");
        
        // reset logging to avoid server logs
        LogManager.getLogManager().reset();
    }

    /**
     * Get Fedauth info
     * 
     */
    protected static void getFedauthInfo() {
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
}
