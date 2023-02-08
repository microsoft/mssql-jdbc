/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import javax.security.auth.kerberos.KerberosPrincipal;
import com.microsoft.aad.msal4j.IAccount;
import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.IClientCredential;
import com.microsoft.aad.msal4j.IntegratedWindowsAuthenticationParameters;
import com.microsoft.aad.msal4j.InteractiveRequestParameters;
import com.microsoft.aad.msal4j.MsalInteractionRequiredException;
import com.microsoft.aad.msal4j.MsalThrottlingException;
import com.microsoft.aad.msal4j.PublicClientApplication;
import com.microsoft.aad.msal4j.SilentParameters;
import com.microsoft.aad.msal4j.SystemBrowserOptions;
import com.microsoft.aad.msal4j.UserNamePasswordParameters;
import com.microsoft.sqlserver.jdbc.SQLServerConnection.ActiveDirectoryAuthentication;

import com.microsoft.sqlserver.jdbc.SQLServerConnection.SqlFedAuthInfo;


class SQLServerMSAL4JUtils {

    static final String REDIRECTURI = "http://localhost";
    static final String SLASH_DEFAULT = "/.default";
    static final String ACCESS_TOKEN_EXPIRE = " Access token expires on the following date: ";

    private SQLServerMSAL4JUtils() {
        throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));
    }

    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.SQLServerMSAL4JUtils");

    static SqlAuthenticationToken getSqlFedAuthToken(SqlFedAuthInfo fedAuthInfo, String user, String password,
            String authenticationString) throws SQLServerException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            final PublicClientApplication pca = PublicClientApplication
                    .builder(ActiveDirectoryAuthentication.JDBC_FEDAUTH_CLIENT_ID).executorService(executorService)
                    .authority(fedAuthInfo.stsurl).build();
            final CompletableFuture<IAuthenticationResult> future = pca.acquireToken(UserNamePasswordParameters
                    .builder(Collections.singleton(fedAuthInfo.spn + SLASH_DEFAULT), user, password.toCharArray())
                    .build());

            final IAuthenticationResult authenticationResult = future.get();

            if (logger.isLoggable(Level.FINEST)) {
                logger.finest(logger.toString() + ACCESS_TOKEN_EXPIRE + authenticationResult.expiresOnDate());
            }

            return new SqlAuthenticationToken(authenticationResult.accessToken(), authenticationResult.expiresOnDate());
        } catch (MalformedURLException | InterruptedException e) {
            // re-interrupt thread
            Thread.currentThread().interrupt();

            throw new SQLServerException(e.getMessage(), e);
        } catch (MsalThrottlingException | ExecutionException e) {
            throw getCorrectedException(e, user, authenticationString);
        } finally {
            executorService.shutdown();
        }
    }

    static SqlAuthenticationToken getSqlFedAuthTokenPrincipal(SqlFedAuthInfo fedAuthInfo, String aadPrincipalID,
            String aadPrincipalSecret, String authenticationString) throws SQLServerException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            String defaultScopeSuffix = SLASH_DEFAULT;
            String scope = fedAuthInfo.spn.endsWith(defaultScopeSuffix) ? fedAuthInfo.spn
                                                                        : fedAuthInfo.spn + defaultScopeSuffix;
            Set<String> scopes = new HashSet<>();
            scopes.add(scope);
            IClientCredential credential = ClientCredentialFactory.createFromSecret(aadPrincipalSecret);
            ConfidentialClientApplication clientApplication = ConfidentialClientApplication
                    .builder(aadPrincipalID, credential).executorService(executorService).authority(fedAuthInfo.stsurl)
                    .build();
            final CompletableFuture<IAuthenticationResult> future = clientApplication
                    .acquireToken(ClientCredentialParameters.builder(scopes).build());
            final IAuthenticationResult authenticationResult = future.get();

            if (logger.isLoggable(Level.FINEST)) {
                logger.finest(logger.toString() + ACCESS_TOKEN_EXPIRE + authenticationResult.expiresOnDate());
            }

            return new SqlAuthenticationToken(authenticationResult.accessToken(), authenticationResult.expiresOnDate());
        } catch (MalformedURLException | InterruptedException e) {
            // re-interrupt thread
            Thread.currentThread().interrupt();

            throw new SQLServerException(e.getMessage(), e);
        } catch (MsalThrottlingException | ExecutionException e) {
            throw getCorrectedException(e, aadPrincipalID, authenticationString);
        } finally {
            executorService.shutdown();
        }
    }

    static SqlAuthenticationToken getSqlFedAuthTokenIntegrated(SqlFedAuthInfo fedAuthInfo,
            String authenticationString) throws SQLServerException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            /*
             * principal name does not matter, what matters is the realm name it gets the username in
             * principal_name@realm_name format
             */
            KerberosPrincipal kerberosPrincipal = new KerberosPrincipal("username");
            String user = kerberosPrincipal.getName();

            if (logger.isLoggable(Level.FINE)) {
                logger.fine(logger.toString() + " realm name is:" + kerberosPrincipal.getRealm());
            }

            final PublicClientApplication pca = PublicClientApplication
                    .builder(ActiveDirectoryAuthentication.JDBC_FEDAUTH_CLIENT_ID).executorService(executorService)
                    .authority(fedAuthInfo.stsurl).build();
            final CompletableFuture<IAuthenticationResult> future = pca
                    .acquireToken(IntegratedWindowsAuthenticationParameters
                            .builder(Collections.singleton(fedAuthInfo.spn + SLASH_DEFAULT), user).build());

            final IAuthenticationResult authenticationResult = future.get();

            if (logger.isLoggable(Level.FINEST)) {
                logger.finest(logger.toString() + ACCESS_TOKEN_EXPIRE + authenticationResult.expiresOnDate());
            }

            return new SqlAuthenticationToken(authenticationResult.accessToken(), authenticationResult.expiresOnDate());
        } catch (InterruptedException | IOException e) {
            // re-interrupt thread
            Thread.currentThread().interrupt();

            throw new SQLServerException(e.getMessage(), e);
        } catch (MsalThrottlingException | ExecutionException e) {
            throw getCorrectedException(e, "", authenticationString);
        } finally {
            executorService.shutdown();
        }
    }

    static SqlAuthenticationToken getSqlFedAuthTokenInteractive(SqlFedAuthInfo fedAuthInfo, String user,
            String authenticationString) throws SQLServerException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        try {
            PublicClientApplication pca = PublicClientApplication
                    .builder(ActiveDirectoryAuthentication.JDBC_FEDAUTH_CLIENT_ID).executorService(executorService)
                    .setTokenCacheAccessAspect(PersistentTokenCacheAccessAspect.getInstance())
                    .authority(fedAuthInfo.stsurl).logPii((logger.isLoggable(Level.FINE))).build();

            CompletableFuture<IAuthenticationResult> future = null;
            IAuthenticationResult authenticationResult = null;

            // try to acquire token silently if user account found in cache
            try {
                Set<IAccount> accountsInCache = pca.getAccounts().join();
                if (logger.isLoggable(Level.FINE)) {
                    StringBuilder acc = new StringBuilder();
                    if (accountsInCache != null) {
                        for (IAccount account : accountsInCache) {
                            if (acc.length() != 0)
                                acc.append(", ");
                            acc.append(account.username());
                        }
                    }
                    logger.fine(logger.toString() + "Accounts in cache = " + acc + ", size = "
                            + (accountsInCache == null ? null : accountsInCache.size()) + ", user = " + user);
                }
                if (null != accountsInCache && !accountsInCache.isEmpty() && null != user && !user.isEmpty()) {
                    IAccount account = getAccountByUsername(accountsInCache, user);
                    if (null != account) {
                        if (logger.isLoggable(Level.FINE)) {
                            logger.fine(logger.toString() + "Silent authentication for user:" + user);
                        }
                        SilentParameters silentParameters = SilentParameters
                                .builder(Collections.singleton(fedAuthInfo.spn + SLASH_DEFAULT), account).build();

                        future = pca.acquireTokenSilently(silentParameters);
                    }
                }
            } catch (MsalInteractionRequiredException e) {
                // not an error, need to get token interactively
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, e, () -> logger.toString() + "Need to get token interactively");
                }
            }

            if (null != future) {
                authenticationResult = future.get();
            } else {
                // acquire token interactively with system browser
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine(logger.toString() + "Interactive authentication");
                }
                InteractiveRequestParameters parameters = InteractiveRequestParameters.builder(new URI(REDIRECTURI))
                        .systemBrowserOptions(SystemBrowserOptions.builder()
                                .htmlMessageSuccess(SQLServerResource.getResource("R_MSALAuthComplete")).build())
                        .loginHint(user).scopes(Collections.singleton(fedAuthInfo.spn + SLASH_DEFAULT)).build();

                future = pca.acquireToken(parameters);
                authenticationResult = future.get();
            }

            if (logger.isLoggable(Level.FINEST)) {
                logger.finest(logger.toString() + ACCESS_TOKEN_EXPIRE + authenticationResult.expiresOnDate());
            }

            return new SqlAuthenticationToken(authenticationResult.accessToken(), authenticationResult.expiresOnDate());
        } catch (MalformedURLException | InterruptedException | URISyntaxException e) {
            // re-interrupt thread
            Thread.currentThread().interrupt();

            throw new SQLServerException(e.getMessage(), e);
        } catch (MsalThrottlingException | ExecutionException e) {
            throw getCorrectedException(e, user, authenticationString);
        } finally {
            executorService.shutdown();
        }
    }

    // Helper function to return account containing user name from set of accounts, or null if no match
    private static IAccount getAccountByUsername(Set<IAccount> accounts, String username) {
        if (!accounts.isEmpty()) {
            for (IAccount account : accounts) {
                if (account.username().equalsIgnoreCase(username)) {
                    return account;
                }
            }
        }
        return null;
    }

    private static SQLServerException getCorrectedException(Exception e, String user, String authenticationString) {
        Object[] msgArgs = {user, authenticationString};

        if (null == e.getCause() || null == e.getCause().getMessage()) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_MSALExecution"));

            // The case when Future's outcome has no AuthenticationResult but Exception.
            return new SQLServerException(form.format(msgArgs), null);
        } else {
            /*
             * the cause error message uses \\n\\r which does not give correct format change it to \r\n to provide
             * correct format. Also replace {} which confuses MessageFormat
             */
            String correctedErrorMessage = e.getCause().getMessage().replaceAll("\\\\r\\\\n", "\r\n")
                    .replaceAll("\\{", "\"").replaceAll("\\}", "\"");

            RuntimeException correctedAuthenticationException = new RuntimeException(correctedErrorMessage);
            MessageFormat form = new MessageFormat(
                    SQLServerException.getErrString("R_MSALExecution") + " " + correctedErrorMessage);

            /*
             * SQLServerException is caused by ExecutionException, which is caused by AuthenticationException to match
             * the exception tree before error message correction
             */
            ExecutionException correctedExecutionException = new ExecutionException(correctedAuthenticationException);

            return new SQLServerException(form.format(msgArgs), null, 0, correctedExecutionException);
        }
    }
}
