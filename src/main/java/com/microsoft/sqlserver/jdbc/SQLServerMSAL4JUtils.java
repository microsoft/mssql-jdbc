/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;

import java.text.MessageFormat;

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

import javax.security.auth.kerberos.KerberosPrincipal;

import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import com.microsoft.aad.msal4j.IAccount;
import com.microsoft.aad.msal4j.ClientCredentialFactory;
import com.microsoft.aad.msal4j.ClientCredentialParameters;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;
import com.microsoft.aad.msal4j.IAuthenticationResult;
import com.microsoft.aad.msal4j.IClientCredential;
import com.microsoft.aad.msal4j.IntegratedWindowsAuthenticationParameters;
import com.microsoft.aad.msal4j.InteractiveRequestParameters;
import com.microsoft.aad.msal4j.MsalInteractionRequiredException;
import com.microsoft.aad.msal4j.PublicClientApplication;
import com.microsoft.aad.msal4j.SilentParameters;
import com.microsoft.aad.msal4j.SystemBrowserOptions;
import com.microsoft.aad.msal4j.UserNamePasswordParameters;

import com.microsoft.sqlserver.jdbc.SQLServerConnection.ActiveDirectoryAuthentication;
import com.microsoft.sqlserver.jdbc.SQLServerConnection.SqlFedAuthInfo;

import static com.microsoft.sqlserver.jdbc.Util.getHashedSecret;


class SQLServerMSAL4JUtils {

    static final String REDIRECTURI = "http://localhost";
    static final String SLASH_DEFAULT = "/.default";
    static final String ACCESS_TOKEN_EXPIRE = "access token expires: ";
    static final long TOKEN_WAIT_DURATION_MS = 20000;
    static final long TOKEN_SEM_WAIT_DURATION_MS = 5000;
    private static final TokenCacheMap TOKEN_CACHE_MAP = new TokenCacheMap();

    private final static String LOGCONTEXT = "MSAL version "
            + com.microsoft.aad.msal4j.PublicClientApplication.class.getPackage().getImplementationVersion() + ": ";

    private static final java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.SQLServerMSAL4JUtils");

    private SQLServerMSAL4JUtils() {
        throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));
    }

    private static final Semaphore sem = new Semaphore(1);

    static SqlAuthenticationToken getSqlFedAuthToken(SqlFedAuthInfo fedAuthInfo, String user, String password,
            String authenticationString, int millisecondsRemaining) throws SQLServerException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        if (logger.isLoggable(Level.FINEST)) {
            logger.finest(LOGCONTEXT + authenticationString + ": get FedAuth token for user: " + user);
        }

        boolean isSemAcquired = false;
        try {
            //
            //Just try to acquire the semaphore and if can't then proceed to attempt to get the token.
            //The purpose is to optimize the token acquisition process, the first caller succeeding does caching 
            //which is then leveraged by subsequent threads. However, if the first thread takes considerable time, 
            //then we want the others to also go and try after waiting for a while.
            //If we were to let say 30 threads try in parallel, they would all miss the cache and hit the AAD auth endpoints 
            //to get their tokens at the same time, stressing the auth endpoint.
            //
            isSemAcquired = sem.tryAcquire(Math.min(millisecondsRemaining, TOKEN_SEM_WAIT_DURATION_MS), TimeUnit.MILLISECONDS);

            String hashedSecret = getHashedSecret(new String[] {fedAuthInfo.stsurl, user, password});
            PersistentTokenCacheAccessAspect persistentTokenCacheAccessAspect = TOKEN_CACHE_MAP.getEntry(user,
                    hashedSecret);

            // check if account password was changed
            if (null == persistentTokenCacheAccessAspect) {
                persistentTokenCacheAccessAspect = new PersistentTokenCacheAccessAspect();
                TOKEN_CACHE_MAP.addEntry(hashedSecret, persistentTokenCacheAccessAspect);

                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(LOGCONTEXT + ": cache token for user: " + user);
                }
            } else {
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(LOGCONTEXT + ": retrieved cached token for user: " + user);
                }
            }

            final PublicClientApplication pca = PublicClientApplication
                    .builder(ActiveDirectoryAuthentication.JDBC_FEDAUTH_CLIENT_ID).executorService(executorService)
                    .setTokenCacheAccessAspect(persistentTokenCacheAccessAspect).authority(fedAuthInfo.stsurl).build();

            final CompletableFuture<IAuthenticationResult> future = pca.acquireToken(UserNamePasswordParameters
                    .builder(Collections.singleton(fedAuthInfo.spn + SLASH_DEFAULT), user, password.toCharArray())
                    .build());

            final IAuthenticationResult authenticationResult = future.get(Math.min(millisecondsRemaining, TOKEN_WAIT_DURATION_MS), TimeUnit.MILLISECONDS);

            if (logger.isLoggable(Level.FINER)) {
                logger.finer(
                        LOGCONTEXT + (authenticationResult.account() != null ? authenticationResult.account().username()
                                + ": " : "" + ACCESS_TOKEN_EXPIRE + authenticationResult.expiresOnDate()));
            }

            return new SqlAuthenticationToken(authenticationResult.accessToken(), authenticationResult.expiresOnDate());
        } catch (InterruptedException e) {
            // re-interrupt thread
            Thread.currentThread().interrupt();

            throw new SQLServerException(e.getMessage(), e);
        } catch (MalformedURLException | ExecutionException e) {
            throw getCorrectedException(e, user, authenticationString);
        } catch (TimeoutException e) {
            throw getCorrectedException(new SQLServerException(SQLServerException.getErrString("R_connectionTimedOut"), e), user, authenticationString);
        } finally {
            if (isSemAcquired) {
                sem.release();
            }
            executorService.shutdown();
        }
    }

    static SqlAuthenticationToken getSqlFedAuthTokenPrincipal(SqlFedAuthInfo fedAuthInfo, String aadPrincipalID,
            String aadPrincipalSecret, String authenticationString, int millisecondsRemaining) throws SQLServerException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        if (logger.isLoggable(Level.FINEST)) {
            logger.finest(LOGCONTEXT + authenticationString + ": get FedAuth token for principal: " + aadPrincipalID);
        }

        String defaultScopeSuffix = SLASH_DEFAULT;
        String scope = fedAuthInfo.spn.endsWith(defaultScopeSuffix) ? fedAuthInfo.spn
                                                                    : fedAuthInfo.spn + defaultScopeSuffix;
        Set<String> scopes = new HashSet<>();
        scopes.add(scope);

        boolean isSemAcquired = false;
        try {
            String hashedSecret = getHashedSecret(
                    new String[] {fedAuthInfo.stsurl, aadPrincipalID, aadPrincipalSecret});
            // Atomic get-or-create so a cold-start burst for the same principal shares a single
            // aspect instance instead of racing (check-then-act) and each building its own.
            java.util.concurrent.atomic.AtomicBoolean created = new java.util.concurrent.atomic.AtomicBoolean(false);
            PersistentTokenCacheAccessAspect persistentTokenCacheAccessAspect = TOKEN_CACHE_MAP
                    .getOrCreateEntry(aadPrincipalID, hashedSecret, created);

            // check if principal secret was changed
            if (created.get()) {
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(LOGCONTEXT + ": cache token for principal id: " + aadPrincipalID);
                }
            } else {
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(LOGCONTEXT + ": retrieved cached token for principal id: " + aadPrincipalID);
                }
            }

            IClientCredential credential = ClientCredentialFactory.createFromSecret(aadPrincipalSecret);
            ConfidentialClientApplication clientApplication = ConfidentialClientApplication
                    .builder(aadPrincipalID, credential).executorService(executorService)
                    .setTokenCacheAccessAspect(persistentTokenCacheAccessAspect).authority(fedAuthInfo.stsurl).build();

            //
            // Fast path: try to satisfy the request from the MSAL in-memory token cache (rehydrated
            // from the persistent cache aspect) WITHOUT taking the semaphore. A cache hit needs no
            // AAD round-trip, so it must not queue behind threads that are refreshing/fetching the token from Entra ID.
            // Serializing cache hits behind the gate is the bottleneck this avoids.
            //
            try {
                // acquireTokenSilently: MSAL's cache-only token lookup. It checks the client
                // application's in-memory token cache (rehydrated here from the persistent cache
                // aspect) for a still-valid access token matching these scopes and returns it
                // WITHOUT contacting Entra ID. If none is cached (or it is expired and cannot be
                // refreshed silently), the returned future fails with an ExecutionException, which
                // we treat as a cache miss and handle by falling through to the gated slow path.
                final IAuthenticationResult silentResult = clientApplication
                        .acquireTokenSilently(SilentParameters.builder(scopes).build())
                        .get(Math.min(millisecondsRemaining, TOKEN_WAIT_DURATION_MS), TimeUnit.MILLISECONDS);

                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(LOGCONTEXT + ": retrieved token silently for principal id: " + aadPrincipalID);
                }

                return new SqlAuthenticationToken(silentResult.accessToken(), silentResult.expiresOnDate());
            } catch (ExecutionException silentMiss) {
                // No valid access token cached for these scopes; fall through to the gated slow path.
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(LOGCONTEXT + ": silent token acquisition missed for principal id: " + aadPrincipalID);
                }
            }

            //
            // Slow path: only cache misses reach here. Gate the AAD call with the semaphore so a
            // burst of misses does not stampede the token endpoint. The first caller to acquire the
            // gate refreshes the token and populates the shared cache; callers that were waiting on
            // the gate pick up that freshly-cached token because acquireToken performs its own cache
            // lookup first (skipCache defaults to false) and therefore avoids a redundant AAD call.
            //
            //Just try to acquire the semaphore and if can't then proceed to attempt to get the token.
            //The purpose is to optimize the token acquisition process, the first caller succeeding does caching 
            //which is then leveraged by subsequent threads. However, if the first thread takes considerable time, 
            //then we want the others to also go and try after waiting for a while.
            //If we were to let say 30 threads try in parallel, they would all miss the cache and hit the AAD auth endpoints 
            //to get their tokens at the same time, stressing the auth endpoint.
            //
            isSemAcquired = sem.tryAcquire(Math.min(millisecondsRemaining, TOKEN_SEM_WAIT_DURATION_MS), TimeUnit.MILLISECONDS);

            final CompletableFuture<IAuthenticationResult> future = clientApplication
                    .acquireToken(ClientCredentialParameters.builder(scopes).build());
            final IAuthenticationResult authenticationResult = future.get(Math.min(millisecondsRemaining, TOKEN_WAIT_DURATION_MS), TimeUnit.MILLISECONDS);

            if (logger.isLoggable(Level.FINER)) {
                logger.finer(
                        LOGCONTEXT + (authenticationResult.account() != null ? authenticationResult.account().username()
                                + ": " : "" + ACCESS_TOKEN_EXPIRE + authenticationResult.expiresOnDate()));
            }

            return new SqlAuthenticationToken(authenticationResult.accessToken(), authenticationResult.expiresOnDate());
        } catch (InterruptedException e) {
            // re-interrupt thread
            Thread.currentThread().interrupt();

            throw new SQLServerException(e.getMessage(), e);
        } catch (MalformedURLException | ExecutionException e) {
            throw getCorrectedException(e, aadPrincipalID, authenticationString);
        } catch (TimeoutException e) {
            throw getCorrectedException(new SQLServerException(SQLServerException.getErrString("R_connectionTimedOut"), e), aadPrincipalID, authenticationString);
        } finally {
            if (isSemAcquired) {
                sem.release();
            }
            executorService.shutdown();
        }
    }

    static SqlAuthenticationToken getSqlFedAuthTokenPrincipalCertificate(SqlFedAuthInfo fedAuthInfo,
            String aadPrincipalID, String certFile, String certPassword, String certKey, String certKeyPassword,
            String authenticationString, int millisecondsRemaining) throws SQLServerException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        if (logger.isLoggable(Level.FINEST)) {
            logger.finest(LOGCONTEXT + authenticationString + ": get FedAuth token for principal certificate: "
                    + aadPrincipalID);
        }

        String defaultScopeSuffix = SLASH_DEFAULT;
        String scope = fedAuthInfo.spn.endsWith(defaultScopeSuffix) ? fedAuthInfo.spn
                                                                    : fedAuthInfo.spn + defaultScopeSuffix;
        Set<String> scopes = new HashSet<>();
        scopes.add(scope);

        boolean isSemAcquired = false;
        try {
            //
            //Just try to acquire the semaphore and if can't then proceed to attempt to get the token.
            //The purpose is to optimize the token acquisition process, the first caller succeeding does caching 
            //which is then leveraged by subsequent threads. However, if the first thread takes considerable time, 
            //then we want the others to also go and try after waiting for a while.
            //If we were to let say 30 threads try in parallel, they would all miss the cache and hit the AAD auth endpoints 
            //to get their tokens at the same time, stressing the auth endpoint.
            //
            isSemAcquired = sem.tryAcquire(Math.min(millisecondsRemaining, TOKEN_SEM_WAIT_DURATION_MS), TimeUnit.MILLISECONDS);

            String hashedSecret = getHashedSecret(new String[] {fedAuthInfo.stsurl, aadPrincipalID, certFile,
                    certPassword, certKey, certKeyPassword});
            PersistentTokenCacheAccessAspect persistentTokenCacheAccessAspect = TOKEN_CACHE_MAP.getEntry(aadPrincipalID,
                    hashedSecret);

            // check if cert was changed
            if (null == persistentTokenCacheAccessAspect) {
                persistentTokenCacheAccessAspect = new PersistentTokenCacheAccessAspect();
                TOKEN_CACHE_MAP.addEntry(hashedSecret, persistentTokenCacheAccessAspect);

                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(LOGCONTEXT + ": cache token for principal id: " + aadPrincipalID);
                }
            } else {
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(LOGCONTEXT + ": retrieved cached token for principal id: " + aadPrincipalID);
                }
            }

            ConfidentialClientApplication clientApplication = null;

            // check if cert is PKCS12 first
            try (InputStream is = new FileInputStream(certFile)) {
                KeyStore keyStore = SQLServerCertificateUtils.loadPKCS12KeyStore(certFile, certPassword);

                if (logger.isLoggable(Level.FINEST)) {
                    logger.finest(LOGCONTEXT + "certificate type: " + keyStore.getType());

                    // we don't need to do this unless logging enabled since MSAL will fail if cert is not valid
                    Enumeration<String> enumeration = keyStore.aliases();
                    while (enumeration.hasMoreElements()) {
                        String alias = enumeration.nextElement();
                        X509Certificate cert = (X509Certificate) keyStore.getCertificate(alias);
                        cert.checkValidity();
                        logger.finest(LOGCONTEXT + "certificate: " + cert.toString());
                    }
                }

                IClientCredential credential = ClientCredentialFactory.createFromCertificate(is, certPassword);
                clientApplication = ConfidentialClientApplication.builder(aadPrincipalID, credential)
                        .executorService(executorService).setTokenCacheAccessAspect(persistentTokenCacheAccessAspect)
                        .authority(fedAuthInfo.stsurl).build();
            } catch (FileNotFoundException e) {
                // re-throw if file not there no point to try another format
                throw new SQLServerException(SQLServerException.getErrString("R_readCertError") + e.getMessage(), null,
                        0, null);
            } catch (CertificateException | NoSuchAlgorithmException | IOException e) {
                // ignore not PKCS12 cert error, will try another format after this
                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(LOGCONTEXT + "Error loading PKCS12 certificate: " + e.getMessage());
                }
            }

            if (clientApplication == null) {
                // try loading X509 cert
                X509Certificate cert = (X509Certificate) SQLServerCertificateUtils.loadCertificate(certFile);

                if (logger.isLoggable(Level.FINER)) {
                    logger.finer(LOGCONTEXT + "certificate type: " + cert.getType());

                    // we don't really need to do this, MSAL will fail if cert is not valid, but good to check here and throw with proper error message
                    cert.checkValidity();
                    logger.finer(LOGCONTEXT + "certificate: " + cert.toString());
                }

                PrivateKey privateKey = SQLServerCertificateUtils.loadPrivateKey(certKey, certKeyPassword);

                IClientCredential credential = ClientCredentialFactory.createFromCertificate(privateKey, cert);
                clientApplication = ConfidentialClientApplication.builder(aadPrincipalID, credential)
                        .executorService(executorService).setTokenCacheAccessAspect(persistentTokenCacheAccessAspect)
                        .authority(fedAuthInfo.stsurl).build();
            }

            final CompletableFuture<IAuthenticationResult> future = clientApplication
                    .acquireToken(ClientCredentialParameters.builder(scopes).build());
            final IAuthenticationResult authenticationResult = future.get(Math.min(millisecondsRemaining, TOKEN_WAIT_DURATION_MS), TimeUnit.MILLISECONDS);

            if (logger.isLoggable(Level.FINER)) {
                logger.finer(
                        LOGCONTEXT + (authenticationResult.account() != null ? authenticationResult.account().username()
                                + ": " : "" + ACCESS_TOKEN_EXPIRE + authenticationResult.expiresOnDate()));
            }

            return new SqlAuthenticationToken(authenticationResult.accessToken(), authenticationResult.expiresOnDate());
        } catch (InterruptedException e) {
            // re-interrupt thread
            Thread.currentThread().interrupt();

            throw new SQLServerException(e.getMessage(), e);
        } catch (GeneralSecurityException e) {
            // this includes all certificate exceptions
            throw new SQLServerException(SQLServerException.getErrString("R_readCertError") + e.getMessage(), null, 0,
                    null);
        } catch (TimeoutException e) {
            throw getCorrectedException(new SQLServerException(SQLServerException.getErrString("R_connectionTimedOut"), e), aadPrincipalID, authenticationString);
        } catch (Exception e) {
            throw getCorrectedException(e, aadPrincipalID, authenticationString);

        } finally {
            if (isSemAcquired) {
                sem.release();
            }
            executorService.shutdown();
        }
    }

    static SqlAuthenticationToken getSqlFedAuthTokenIntegrated(SqlFedAuthInfo fedAuthInfo,
            String authenticationString, int millisecondsRemaining) throws SQLServerException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        /*
         * principal name does not matter, what matters is the realm name it gets the username in
         * principal_name@realm_name format
         */
        KerberosPrincipal kerberosPrincipal = new KerberosPrincipal("username");
        String user = kerberosPrincipal.getName();

        if (logger.isLoggable(Level.FINER)) {
            logger.finer(LOGCONTEXT + authenticationString + ": get FedAuth token integrated, user: " + user
                    + "realm name:" + kerberosPrincipal.getRealm());
        }

        boolean isSemAcquired = false;
        try {
            //
            //Just try to acquire the semaphore and if can't then proceed to attempt to get the token.
            //The purpose is to optimize the token acquisition process, the first caller succeeding does caching 
            //which is then leveraged by subsequent threads. However, if the first thread takes considerable time, 
            //then we want the others to also go and try after waiting for a while.
            //If we were to let say 30 threads try in parallel, they would all miss the cache and hit the AAD auth endpoints 
            //to get their tokens at the same time, stressing the auth endpoint.
            //
            isSemAcquired = sem.tryAcquire(Math.min(millisecondsRemaining, TOKEN_SEM_WAIT_DURATION_MS), TimeUnit.MILLISECONDS);

            final PublicClientApplication pca = PublicClientApplication
                    .builder(ActiveDirectoryAuthentication.JDBC_FEDAUTH_CLIENT_ID).executorService(executorService)
                    .setTokenCacheAccessAspect(PersistentTokenCacheAccessAspect.getInstance())
                    .authority(fedAuthInfo.stsurl).build();

            final CompletableFuture<IAuthenticationResult> future = pca
                    .acquireToken(IntegratedWindowsAuthenticationParameters
                            .builder(Collections.singleton(fedAuthInfo.spn + SLASH_DEFAULT), user).build());

            final IAuthenticationResult authenticationResult = future.get(Math.min(millisecondsRemaining, TOKEN_WAIT_DURATION_MS), TimeUnit.MILLISECONDS);

            if (logger.isLoggable(Level.FINER)) {
                logger.finer(
                        LOGCONTEXT + (authenticationResult.account() != null ? authenticationResult.account().username()
                                + ": " : "" + ACCESS_TOKEN_EXPIRE + authenticationResult.expiresOnDate()));
            }

            return new SqlAuthenticationToken(authenticationResult.accessToken(), authenticationResult.expiresOnDate());
        } catch (InterruptedException e) {
            // re-interrupt thread
            Thread.currentThread().interrupt();

            throw new SQLServerException(e.getMessage(), e);
        } catch (IOException | ExecutionException e) {
            throw getCorrectedException(e, user, authenticationString);
        } catch (TimeoutException e) {
            throw getCorrectedException(new SQLServerException(SQLServerException.getErrString("R_connectionTimedOut"), e), user, authenticationString);
        } finally {
            if (isSemAcquired) {
                sem.release();
            }
            executorService.shutdown();
        }
    }

    static SqlAuthenticationToken getSqlFedAuthTokenInteractive(SqlFedAuthInfo fedAuthInfo, String user,
            String authenticationString, int millisecondsRemaining) throws SQLServerException {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        if (logger.isLoggable(Level.FINER)) {
            logger.finer(LOGCONTEXT + authenticationString + ": get FedAuth token interactive for user: " + user);
        }

        boolean isSemAcquired = false;
        try {
            //
            //Just try to acquire the semaphore and if can't then proceed to attempt to get the token.
            //The purpose is to optimize the token acquisition process, the first caller succeeding does caching 
            //which is then leveraged by subsequent threads. However, if the first thread takes considerable time, 
            //then we want the others to also go and try after waiting for a while.
            //If we were to let say 30 threads try in parallel, they would all miss the cache and hit the AAD auth endpoints 
            //to get their tokens at the same time, stressing the auth endpoint.
            //
            isSemAcquired = sem.tryAcquire(Math.min(millisecondsRemaining, TOKEN_SEM_WAIT_DURATION_MS), TimeUnit.MILLISECONDS);

            PublicClientApplication pca = PublicClientApplication
                    .builder(ActiveDirectoryAuthentication.JDBC_FEDAUTH_CLIENT_ID).executorService(executorService)
                    .setTokenCacheAccessAspect(PersistentTokenCacheAccessAspect.getInstance())
                    .authority(fedAuthInfo.stsurl).build();

            CompletableFuture<IAuthenticationResult> future = null;
            IAuthenticationResult authenticationResult = null;

            // try to acquire token silently if user account found in cache
            try {
                Set<IAccount> accountsInCache = pca.getAccounts().join();
                if (logger.isLoggable(Level.FINEST)) {
                    StringBuilder acc = new StringBuilder();
                    if (accountsInCache != null) {
                        for (IAccount account : accountsInCache) {
                            if (acc.length() != 0) {
                                acc.append(", ");
                            }
                            acc.append(account.username());
                        }
                    }
                    if (logger.isLoggable(Level.FINEST)) {
                        logger.finest(LOGCONTEXT + "Accounts in cache = " + acc + ", size = "
                                + (accountsInCache == null ? null : accountsInCache.size()) + ", user = " + user);
                    }
                }
                if (null != accountsInCache && !accountsInCache.isEmpty() && null != user && !user.isEmpty()) {
                    IAccount account = getAccountByUsername(accountsInCache, user);
                    if (null != account) {
                        if (logger.isLoggable(Level.FINEST)) {
                            logger.finest(LOGCONTEXT + "Silent authentication for user:" + user);
                        }
                        SilentParameters silentParameters = SilentParameters
                                .builder(Collections.singleton(fedAuthInfo.spn + SLASH_DEFAULT), account).build();

                        future = pca.acquireTokenSilently(silentParameters);
                    }
                }
            } catch (MsalInteractionRequiredException e) {
                // not an error, need to get token interactively
                if (logger.isLoggable(Level.FINEST)) {
                    logger.log(Level.FINEST, e,
                            () -> LOGCONTEXT + "Need to get token interactively: " + e.reason().toString());
                }
            }

            if (null != future) {
                authenticationResult = future.get(Math.min(millisecondsRemaining, TOKEN_WAIT_DURATION_MS), TimeUnit.MILLISECONDS);
            } else {
                // acquire token interactively with system browser
                if (logger.isLoggable(Level.FINEST)) {
                    logger.finest(LOGCONTEXT + "Interactive authentication");
                }
                InteractiveRequestParameters parameters = buildInteractiveRequestParameters(
                        new URI(REDIRECTURI), user, fedAuthInfo.spn);

                future = pca.acquireToken(parameters);
                authenticationResult = future.get(Math.min(millisecondsRemaining, TOKEN_WAIT_DURATION_MS), TimeUnit.MILLISECONDS);
            }

            if (logger.isLoggable(Level.FINER)) {
                logger.finer(
                        LOGCONTEXT + (authenticationResult.account() != null ? authenticationResult.account().username()
                                + ": " : "" + ACCESS_TOKEN_EXPIRE + authenticationResult.expiresOnDate()));
            }

            return new SqlAuthenticationToken(authenticationResult.accessToken(), authenticationResult.expiresOnDate());
        } catch (InterruptedException e) {
            // re-interrupt thread
            Thread.currentThread().interrupt();

            throw new SQLServerException(e.getMessage(), e);
        } catch (MalformedURLException | URISyntaxException | ExecutionException e) {
            throw getCorrectedException(e, user, authenticationString);
        } catch (TimeoutException e) {
            throw getCorrectedException(new SQLServerException(SQLServerException.getErrString("R_connectionTimedOut"), e), user, authenticationString);
        } finally {
            if (isSemAcquired) {
                sem.release();
            }
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

    /**
     * Builds the MSAL4J InteractiveRequestParameters used by the ActiveDirectoryInteractive
     * authentication flow. Extracted from {@link #getSqlFedAuthTokenInteractive} so the
     * configuration (notably {@code response_mode=form_post}, which keeps the AAD
     * authorization response out of the redirect URL) can be unit-tested without driving
     * a real interactive sign-in.
     */
    static InteractiveRequestParameters buildInteractiveRequestParameters(URI redirectUri, String user, String spn) {
        return InteractiveRequestParameters.builder(redirectUri)
                .systemBrowserOptions(SystemBrowserOptions.builder()
                        .htmlMessageSuccess(SQLServerResource.getResource("R_MSALAuthComplete")).build())
                .loginHint(user)
                .extraQueryParameters(Collections.singletonMap("response_mode", "form_post"))
                .scopes(Collections.singleton(spn + SLASH_DEFAULT))
                .build();
    }

    private static SQLServerException getCorrectedException(Exception e, String user, String authenticationString) {
        Object[] msgArgs = {user, authenticationString};

        if (null == e.getCause() || null == e.getCause().getMessage()) {
            MessageFormat form = new MessageFormat(
                    SQLServerException.getErrString("R_MSALExecution") + " " + e.getMessage());

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

    private static class TokenCacheMap {
        private ConcurrentHashMap<String, PersistentTokenCacheAccessAspect> tokenCacheMap = new ConcurrentHashMap<>();

        PersistentTokenCacheAccessAspect getEntry(String value, String key) {
            PersistentTokenCacheAccessAspect persistentTokenCacheAccessAspect = tokenCacheMap.get(key);

            if (null != persistentTokenCacheAccessAspect) {
                long currentTime = System.currentTimeMillis();

                if (currentTime > persistentTokenCacheAccessAspect.getExpiryTime()) {
                    tokenCacheMap.remove(key);

                    persistentTokenCacheAccessAspect = new PersistentTokenCacheAccessAspect();
                    persistentTokenCacheAccessAspect
                            .setExpiryTime(currentTime + PersistentTokenCacheAccessAspect.TIME_TO_LIVE);

                    tokenCacheMap.put(key, persistentTokenCacheAccessAspect);

                    if (logger.isLoggable(Level.FINER)) {
                        logger.finer(LOGCONTEXT + ": entry expired for: " + value + " new entry will expire in: "
                                + TimeUnit.MILLISECONDS.toSeconds(PersistentTokenCacheAccessAspect.TIME_TO_LIVE) + "s");
                    }
                }
            }

            return persistentTokenCacheAccessAspect;
        }

        /**
         * Atomically returns the aspect for {@code key}, creating (or replacing an expired) one under
         * the map's per-bin lock so concurrent callers for the same key share a single instance.
         * {@code createdOut} is set to true when a fresh aspect was created for this call.
         */
        PersistentTokenCacheAccessAspect getOrCreateEntry(String value, String key,
                java.util.concurrent.atomic.AtomicBoolean createdOut) {
            return tokenCacheMap.compute(key, (k, existing) -> {
                long currentTime = System.currentTimeMillis();
                if (null == existing || currentTime > existing.getExpiryTime()) {
                    PersistentTokenCacheAccessAspect fresh = new PersistentTokenCacheAccessAspect();
                    fresh.setExpiryTime(currentTime + PersistentTokenCacheAccessAspect.TIME_TO_LIVE);
                    if (null != createdOut) {
                        createdOut.set(true);
                    }
                    if (logger.isLoggable(Level.FINER)) {
                        logger.finer(LOGCONTEXT + ": add entry for: " + value + ", will expire in: "
                                + TimeUnit.MILLISECONDS.toSeconds(PersistentTokenCacheAccessAspect.TIME_TO_LIVE)
                                + "s");
                    }
                    return fresh;
                }
                return existing;
            });
        }

        void addEntry(String key, PersistentTokenCacheAccessAspect value) {
            value.setExpiryTime(System.currentTimeMillis() + PersistentTokenCacheAccessAspect.TIME_TO_LIVE);
            tokenCacheMap.put(key, value);
            if (logger.isLoggable(Level.FINER)) {
                logger.finer(LOGCONTEXT + ": add entry for: " + value + ", will expire in: "
                        + TimeUnit.MILLISECONDS.toSeconds(PersistentTokenCacheAccessAspect.TIME_TO_LIVE) + "s");
            }
        }
    }
}
