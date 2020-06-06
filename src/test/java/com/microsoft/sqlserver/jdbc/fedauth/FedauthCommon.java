package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;

import com.microsoft.aad.adal4j.AuthenticationContext;
import com.microsoft.aad.adal4j.AuthenticationResult;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.AbstractTest;


@Tag(Constants.Fedauth)
public class FedauthCommon extends AbstractTest {

    public static String azureServer = null;
    public static String azureDatabase = null;
    public static String azureUserName = null;
    public static String azurePassword = null;
    public static String azureGroupUserName = null;

    public static boolean enableADIntegrated = false;
    public static String spn = null;
    public static String stsurl = null;
    public static String fedauthClientId = null;
    public static long secondsBeforeExpiration = -1;
    public static String accessToken = null;
    public static String hostNameInCertificate = null;
    public static String[] fedauthJksPaths = null;
    public static String[] fedauthJksPathsLinux = null;
    public static String[] fedauthJavaKeyAliases = null;

    @BeforeAll
    public static void getConfigs() throws Exception {
        azureServer = getConfiguredProperty("azureServer");
        azureDatabase = getConfiguredProperty("azureDatabase");
        azureUserName = getConfiguredProperty("azureUserName");
        azurePassword = getConfiguredProperty("azurePassword");
        azureGroupUserName = getConfiguredProperty("azureGroupUserName");

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
