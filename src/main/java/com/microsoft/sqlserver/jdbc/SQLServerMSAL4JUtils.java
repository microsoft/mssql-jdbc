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
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;

import javax.security.auth.kerberos.KerberosPrincipal;

import com.microsoft.aad.msal4j.IAccount;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.IntegratedWindowsAuthenticationParameters;
import com.microsoft.aad.msal4j.InteractiveRequestParameters;
import com.microsoft.aad.msal4j.MsalClientException;
import com.microsoft.aad.msal4j.MsalException;
import com.microsoft.aad.msal4j.MsalInteractionRequiredException;
import com.microsoft.aad.msal4j.MsalServiceException;
import com.microsoft.aad.msal4j.MsalThrottlingException;
import com.microsoft.aad.msal4j.PublicClientApplication;
import com.microsoft.aad.msal4j.SilentParameters;
import com.microsoft.aad.msal4j.SystemBrowserOptions;
import com.microsoft.aad.msal4j.UserNamePasswordParameters;
import com.microsoft.sqlserver.jdbc.SQLServerConnection.ActiveDirectoryAuthentication;

import com.microsoft.sqlserver.jdbc.SQLServerConnection.SqlFedAuthInfo;


class SQLServerMSAL4JUtils {

    static final private java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.SQLServerMSAL4JUtils");

    private static ConcurrentHashMap<String, PublicClientApplication> clientAppCache = null;
    private static ExecutorService executorService = Executors.newFixedThreadPool(1);

    static SqlFedAuthToken getSqlFedAuthToken(SqlFedAuthInfo fedAuthInfo, String user, String password,
            String authenticationString) throws SQLServerException {
        try {
            final PublicClientApplication clientApplication = PublicClientApplication
                    .builder(ActiveDirectoryAuthentication.JDBC_FEDAUTH_CLIENT_ID).executorService(executorService)
                    .authority(fedAuthInfo.stsurl).build();
            final CompletableFuture<IAuthenticationResult> future = clientApplication
                    .acquireToken(UserNamePasswordParameters
                            .builder(Collections.singleton(fedAuthInfo.spn + "/.default"), user, password.toCharArray())
                            .build());

            final IAuthenticationResult authenticationResult = future.get();
            return new SqlFedAuthToken(authenticationResult.accessToken(), authenticationResult.expiresOnDate());
        } catch (MalformedURLException | InterruptedException e) {
            throw new SQLServerException(e.getMessage(), e);
        } catch (ExecutionException e) {
            handleMSALException(e, user, authenticationString);
            return null;
        }
    }

    static SqlFedAuthToken getSqlFedAuthTokenIntegrated(SqlFedAuthInfo fedAuthInfo,
            String authenticationString) throws SQLServerException {
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

            final PublicClientApplication clientApplication = PublicClientApplication
                    .builder(ActiveDirectoryAuthentication.JDBC_FEDAUTH_CLIENT_ID).executorService(executorService)
                    .authority(fedAuthInfo.stsurl).build();
            final CompletableFuture<IAuthenticationResult> future = clientApplication
                    .acquireToken(IntegratedWindowsAuthenticationParameters
                            .builder(Collections.singleton(fedAuthInfo.spn + "/.default"), user).build());

            final IAuthenticationResult authenticationResult = future.get();
            return new SqlFedAuthToken(authenticationResult.accessToken(), authenticationResult.expiresOnDate());
        } catch (InterruptedException | IOException e) {
            throw new SQLServerException(e.getMessage(), e);
        } catch (ExecutionException e) {
            handleMSALException(e, "", authenticationString);
            return null;
        }
    }

    /**
     * Helper function to return an account from a given set of accounts based on the given username, or return null if
     * no accounts in the set match
     */
    private static IAccount getAccountByUsername(Set<IAccount> accounts, String username) {
        if (!accounts.isEmpty()) {
            System.out.println("==Accounts in cache: " + accounts.size());
            for (IAccount account : accounts) {
                if (account.username().equals(username)) {
                    return account;
                }
            }
        }
        return null;
    }

    static SqlFedAuthToken getSqlFedAuthTokenInteractive(SqlFedAuthInfo fedAuthInfo, String user,
            String authenticationString) throws SQLServerException {
        String authority = fedAuthInfo.stsurl;
        PublicClientApplication clientApplication = null;
        try {
            if (null == clientAppCache) {
                clientAppCache = new ConcurrentHashMap<String, PublicClientApplication>();
                clientApplication = PublicClientApplication
                        .builder(ActiveDirectoryAuthentication.JDBC_FEDAUTH_CLIENT_ID).executorService(executorService)
                        .authority(authority).logPii(true).build();
                clientAppCache.putIfAbsent(authority, clientApplication);

            } else {
                clientApplication = clientAppCache.get(fedAuthInfo.stsurl);
            }

            if (null == clientApplication) {
                return null;
            }

            CompletableFuture<IAuthenticationResult> future = null;
            IAuthenticationResult authenticationResult = null;

            /*
             * try to acquire token silently. First call will fail since the token cache will not have any data for the
             * user
             */
            try {
                Set<IAccount> accountsInCache = clientApplication.getAccounts().join();
                if (null != accountsInCache && !accountsInCache.isEmpty() && null != user && !user.isEmpty()) {
                    IAccount account = getAccountByUsername(accountsInCache, user);

                    if (null != account) {
                        SilentParameters silentParameters = SilentParameters
                                .builder(Collections.singleton(fedAuthInfo.spn + "/.default"), account).build();

                        // authenticationResult = clientApplication.acquireTokenSilently(silentParameters).join();
                        future = clientApplication.acquireTokenSilently(silentParameters);
                    }
                }
            } catch (MsalInteractionRequiredException e) {
                // valid error, get token interactively
                if (logger.isLoggable(Level.INFO)) {
                    logger.fine(logger.toString() + " MSAL exception:" + e.getMessage());
                }
            } catch (MsalException e) {
                if (logger.isLoggable(Level.SEVERE)) {
                    logger.fine(logger.toString() + " MSAL exception:" + e.getMessage());
                }
                System.out.println("MSAL exception1:" + e.getMessage());
                if (e instanceof MsalServiceException) {
                    throw new SQLServerException(SQLServerException.getErrString("R_MSALServiceException"), null);
                } else if (e instanceof MsalThrottlingException) {
                    throw new SQLServerException(SQLServerException.getErrString("R_MSALThrottlingException"), null);
                } else {
                    throw new SQLServerException(SQLServerException.getErrString("R_MSALException"), null);
                }
            }

            if (null != future) {
                authenticationResult = future.get();
            } else {
                // try to acquire token interactively with system browser
                InteractiveRequestParameters parameters = InteractiveRequestParameters
                        .builder(new URI("http://localhost"))
                        .systemBrowserOptions(SystemBrowserOptions.builder()
                                .htmlMessageSuccess(SQLServerResource.getResource("R_MSALAuthComplete"))
                                .htmlMessageError("JDBC Error MSG").build())
                        .loginHint(user).scopes(Collections.singleton(fedAuthInfo.spn + "/.default")).build();

                // acquire token interactively with system browser
                future = clientApplication.acquireToken(parameters);
                authenticationResult = future.get();
            }

            return new SqlFedAuthToken(authenticationResult.accessToken(), authenticationResult.expiresOnDate());

        } catch (MalformedURLException | InterruptedException | URISyntaxException e) {
            throw new SQLServerException(e.getMessage(), e);
        } catch (ExecutionException e) {
            handleMSALException(e, user, authenticationString);
            return null;
        } finally {
            // this causes a problem
            // executorService.shutdown();
        }
    }

    static void handleMSALException(ExecutionException e, String user,
            String authenticationString) throws SQLServerException {
        if (logger.isLoggable(Level.SEVERE)) {
            logger.fine(logger.toString() + " MSAL exception:" + e.getMessage());
        }

        Throwable cause = e.getCause();
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_MSALExecution"));

        if (null == cause || null == cause.getMessage()) {
            Object[] msgArgs = {user, authenticationString, ""};
            // the case when Future's outcome has no AuthenticationResult but exception
            throw new SQLServerException(form.format(msgArgs), null);
        } else if (cause instanceof MsalThrottlingException) {
            Object[] msgArgs = {user, authenticationString, SQLServerResource.getResource("R_MSALThrottlingException")};
            throw new SQLServerException(form.format(msgArgs), null);
        } else {
            if (cause instanceof MsalClientException) {
                MessageFormat form2 = new MessageFormat(SQLServerException.getErrString("R_MSALClientErrorcode"));
                Object[] msgArgs2 = {((MsalClientException) cause).errorCode()};
                Object[] msgArgs = {user, authenticationString, form2.format(msgArgs2)};
                form = new MessageFormat(SQLServerException.getErrString("R_MSALExecution"));
                throw new SQLServerException(form.format(msgArgs), null);
            } else if (cause instanceof MsalServiceException) {
                MessageFormat form2 = new MessageFormat(SQLServerException.getErrString("R_MSALServiceStatus"));
                Object[] msgArgs2 = {((MsalServiceException) cause).statusMessage()};
                Object[] msgArgs = {user, authenticationString, form2.format(msgArgs2)};
                form = new MessageFormat(SQLServerException.getErrString("R_MSALExecution"));
                throw new SQLServerException(form.format(msgArgs), null);
            } else {
                /*
                 * the cause error message uses \\n\\r which does not give correct format change it to \r\n to provide
                 * correct format
                 */
                String correctedErrorMessage = e.getCause().getMessage().replaceAll("\\\\r\\\\n", "\r\n");
                RuntimeException correctedAuthenticationException = new RuntimeException(correctedErrorMessage);

                /*
                 * SQLServerException is caused by ExecutionException, which is caused by AuthenticationException to
                 * match the exception tree before error message correction
                 */
                ExecutionException correctedExecutionException = new ExecutionException(
                        correctedAuthenticationException);
                Object[] msgArgs = {user, authenticationString, correctedExecutionException};
                throw new SQLServerException(form.format(msgArgs), null, 0, correctedExecutionException);
            }
        }
    }
}
