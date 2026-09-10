/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.ManagedIdentityCredential;

import com.microsoft.sqlserver.testframework.Constants;

import reactor.core.Exceptions;
import reactor.core.publisher.Mono;


/**
 * Pure unit tests for {@link SQLServerConnection} and related driver internals.
 *
 * These tests are Mockito/reflection based and require no live SQL Server instance or external test configuration.
 * Unlike {@code SQLServerConnectionTest}, this class does not extend {@code AbstractTest} and does not call
 * {@code setConnection()}, so it can run in a SQL-Server-free environment. Lives in package
 * {@code com.microsoft.sqlserver.jdbc} to reach package-private types and members.
 */
public class SQLServerConnectionUnitTest {

    SQLServerConnection mockConnection;
    Logger mockLogger;

    /*
     * Regression tests for issue #2999: a transient Managed Identity credential retrieval failure must not become
     * permanently cached. The azure-identity credential objects (ManagedIdentityCredential / DefaultAzureCredential)
     * are stored in a static credential cache and reused across all pooled connections. When a transient failure (e.g.
     * a Managed Identity endpoint outage that causes a reactive timeout) poisons the cached credential's internal
     * token state, every subsequent token request on that same instance keeps failing. The fix evicts the poisoned
     * credential on failure so a subsequent attempt can rebuild a fresh credential and recover. On success, the
     * credential must remain cached.
     */
    private static final String MI_TEST_RESOURCE = "https://database.windows.net/";
    private static final String MI_TEST_CLIENT_ID = "00000000-0000-0000-0000-000000000001";
    private static final String INTELLIJ_KEEPASS_PATH_ENV = "INTELLIJ_KEEPASS_PATH";
    private static final String ADDITIONALLY_ALLOWED_TENANTS_ENV = "ADDITIONALLY_ALLOWED_TENANTS";

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getCredentialCache() throws Exception {
        Field cacheField = SQLServerSecurityUtility.class.getDeclaredField("CREDENTIAL_CACHE");
        cacheField.setAccessible(true);
        return (Map<String, Object>) cacheField.get(null);
    }

    @AfterEach
    void clearCredentialCache() throws Exception {
        getCredentialCache().clear();
    }

    private static Object wrapCredential(Object tokenCredential) throws Exception {
        Class<?> credClass = Class.forName("com.microsoft.sqlserver.jdbc.SQLServerSecurityUtility$Credential");
        Constructor<?> ctor = credClass.getDeclaredConstructor(Object.class);
        ctor.setAccessible(true);
        return ctor.newInstance(tokenCredential);
    }

    private static Object unwrapCredential(Object credential) throws Exception {
        Field tokenCredentialField = credential.getClass().getDeclaredField("tokenCredential");
        tokenCredentialField.setAccessible(true);
        return tokenCredentialField.get(credential);
    }

    private static String managedIdentityCacheKey() throws SQLServerException {
        return Util.getHashedSecret(
                new String[] {MI_TEST_CLIENT_ID, ManagedIdentityCredential.class.getSimpleName()});
    }

    private static String defaultAzureCredentialCacheKey() throws SQLServerException {
        String intellijKeepassPath = System.getenv(INTELLIJ_KEEPASS_PATH_ENV);
        String additionallyAllowedTenantsValue = System.getenv(ADDITIONALLY_ALLOWED_TENANTS_ENV);
        String[] additionallyAllowedTenants = null;

        if (null != additionallyAllowedTenantsValue && !additionallyAllowedTenantsValue.isEmpty()) {
            additionallyAllowedTenants = additionallyAllowedTenantsValue.split(",");
        }

        int secretsLength = null == additionallyAllowedTenants ? 3 : additionallyAllowedTenants.length + 3;
        String[] secrets = new String[secretsLength];
        if (null != additionallyAllowedTenants && additionallyAllowedTenants.length != 0) {
            System.arraycopy(additionallyAllowedTenants, 0, secrets, 3, additionallyAllowedTenants.length);
        }

        secrets[0] = DefaultAzureCredential.class.getSimpleName();
        secrets[1] = MI_TEST_CLIENT_ID;
        secrets[2] = intellijKeepassPath;
        return Util.getHashedSecret(secrets);
    }

    private static void assertTokenAcquisitionError(SQLServerException exception) {
        assertEquals(SQLServerException.getErrString("R_ManagedIdentityTokenAcquisitionError"),
                exception.getMessage(), "The exception path must report a token acquisition error.");
        assertNotNull(exception.getCause(), "The exception path must preserve the underlying failure as its cause.");
    }

    private static void assertEmptyTokenAcquisitionFailure(SQLServerException exception) {
        assertEquals(SQLServerException.getErrString("R_ManagedIdentityTokenAcquisitionFail"), exception.getMessage(),
                "The empty-token path must report that no token was returned.");
        assertNull(exception.getCause(), "The empty-token path must not report an underlying request failure.");
    }

    @Test
    public void testManagedIdentityTransientFailureEvictsCachedCredential() throws Exception {
        Map<String, Object> cache = getCredentialCache();
        String key = managedIdentityCacheKey();

        ManagedIdentityCredential mockCredential = mock(ManagedIdentityCredential.class);
        when(mockCredential.getToken(any(TokenRequestContext.class)))
                .thenReturn(Mono.error(new RuntimeException("Simulated transient Managed Identity endpoint outage")));

        cache.put(key, wrapCredential(mockCredential));
        assertTrue(cache.containsKey(key), "Precondition: credential should be cached before the failing call");

        SQLServerException exception = assertThrows(SQLServerException.class, () -> SQLServerSecurityUtility
                .getManagedIdentityCredAuthToken(MI_TEST_RESOURCE, MI_TEST_CLIENT_ID, 5000L));

        assertTokenAcquisitionError(exception);
        assertFalse(cache.containsKey(key),
                "A credential poisoned by a transient failure must be evicted so a fresh one can be built.");
    }

    @Test
    public void testManagedIdentityEmptyTokenEvictsCachedCredential() throws Exception {
        Map<String, Object> cache = getCredentialCache();
        String key = managedIdentityCacheKey();

        ManagedIdentityCredential mockCredential = mock(ManagedIdentityCredential.class);
        when(mockCredential.getToken(any(TokenRequestContext.class))).thenReturn(Mono.empty());

        cache.put(key, wrapCredential(mockCredential));

        SQLServerException exception = assertThrows(SQLServerException.class, () -> SQLServerSecurityUtility
                .getManagedIdentityCredAuthToken(MI_TEST_RESOURCE, MI_TEST_CLIENT_ID, 5000L));

        assertEmptyTokenAcquisitionFailure(exception);
        assertFalse(cache.containsKey(key),
                "A credential that returns no token must be evicted so a fresh one can be built.");
    }

    @Test
    public void testManagedIdentityReactorTimeoutEvictsCachedCredential() throws Exception {
        Map<String, Object> cache = getCredentialCache();
        String key = managedIdentityCacheKey();

        ManagedIdentityCredential mockCredential = mock(ManagedIdentityCredential.class);
        when(mockCredential.getToken(any(TokenRequestContext.class))).thenReturn(Mono.never());

        cache.put(key, wrapCredential(mockCredential));

        SQLServerException exception = assertThrows(SQLServerException.class, () -> SQLServerSecurityUtility
                .getManagedIdentityCredAuthToken(MI_TEST_RESOURCE, MI_TEST_CLIENT_ID, 25L));

        assertTokenAcquisitionError(exception);
        Throwable unwrappedCause = Exceptions.unwrap(exception.getCause());
        assertTrue(unwrappedCause instanceof TimeoutException,
                "Expected Reactor to produce a TimeoutException, but got: " + unwrappedCause);
        assertFalse(cache.containsKey(key), "A credential that times out must be evicted from the cache.");
    }

    @Test
    public void testManagedIdentitySuccessfulTokenKeepsCachedCredential() throws Exception {
        Map<String, Object> cache = getCredentialCache();
        String key = managedIdentityCacheKey();

        AccessToken token = new AccessToken("dummy-token", OffsetDateTime.now().plus(Duration.ofHours(1)));
        ManagedIdentityCredential mockCredential = mock(ManagedIdentityCredential.class);
        when(mockCredential.getToken(any(TokenRequestContext.class))).thenReturn(Mono.just(token));

        cache.put(key, wrapCredential(mockCredential));

        SqlAuthenticationToken result = SQLServerSecurityUtility
                .getManagedIdentityCredAuthToken(MI_TEST_RESOURCE, MI_TEST_CLIENT_ID, 5000L);

        assertNotNull(result, "A valid token should be returned on success.");
        assertEquals("dummy-token", result.getAccessToken(), "Unexpected access token returned.");
        assertTrue(cache.containsKey(key), "A working credential must remain cached across successful calls.");
    }

    /*
     * Eviction must be identity-checked: a failing credential must only evict itself, never a healthy replacement that
     * another thread rebuilt under the same key while this thread was waiting on its (now failed) token request. Here a
     * failing credential is cached, then replaced by a healthy one before the failure is processed; the failing call
     * must not evict the healthy replacement.
     */
    @Test
    public void testManagedIdentityFailureDoesNotEvictHealthyReplacement() throws Exception {
        Map<String, Object> cache = getCredentialCache();
        String key = managedIdentityCacheKey();

        ManagedIdentityCredential failingCredential = mock(ManagedIdentityCredential.class);
        ManagedIdentityCredential healthyCredential = mock(ManagedIdentityCredential.class);
        AccessToken token = new AccessToken("healthy-token", OffsetDateTime.now().plus(Duration.ofHours(1)));
        when(healthyCredential.getToken(any(TokenRequestContext.class))).thenReturn(Mono.just(token));
        // When the failing credential's token request is invoked, simulate another thread having already replaced the
        // cached entry with a fresh, healthy credential before this failure is handled.
        when(failingCredential.getToken(any(TokenRequestContext.class))).thenAnswer(invocation -> {
            cache.put(key, wrapCredential(healthyCredential));
            return Mono.error(new RuntimeException("Simulated transient Managed Identity endpoint outage"));
        });

        cache.put(key, wrapCredential(failingCredential));

        SQLServerException exception = assertThrows(SQLServerException.class, () -> SQLServerSecurityUtility
                .getManagedIdentityCredAuthToken(MI_TEST_RESOURCE, MI_TEST_CLIENT_ID, 5000L));

        assertTokenAcquisitionError(exception);
        assertTrue(cache.containsKey(key),
                "The healthy replacement credential must remain cached; only the failing instance may be evicted.");
        assertSame(healthyCredential, unwrapCredential(cache.get(key)),
                "The failing call must not replace or evict the healthy credential.");
    }

    @Test
    public void testConcurrentManagedIdentityFailureDoesNotEvictSuccessfulCredential() throws Exception {
        Map<String, Object> cache = getCredentialCache();
        String key = managedIdentityCacheKey();

        ManagedIdentityCredential failingCredential = mock(ManagedIdentityCredential.class);
        ManagedIdentityCredential healthyCredential = mock(ManagedIdentityCredential.class);
        AccessToken healthyToken = new AccessToken("healthy-token", OffsetDateTime.now().plus(Duration.ofHours(1)));
        RuntimeException transientFailure = new RuntimeException("Simulated concurrent Managed Identity failure");
        CompletableFuture<AccessToken> failingTokenFuture = new CompletableFuture<>();
        CountDownLatch failingRequestStarted = new CountDownLatch(1);

        when(failingCredential.getToken(any(TokenRequestContext.class))).thenAnswer(invocation -> {
            failingRequestStarted.countDown();
            return Mono.fromFuture(failingTokenFuture);
        });
        when(healthyCredential.getToken(any(TokenRequestContext.class))).thenReturn(Mono.just(healthyToken));
        cache.put(key, wrapCredential(failingCredential));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<SQLServerException> failingCall = executor.submit(() -> assertThrows(SQLServerException.class,
                () -> SQLServerSecurityUtility.getManagedIdentityCredAuthToken(MI_TEST_RESOURCE, MI_TEST_CLIENT_ID,
                        10000L)));

        try {
            assertTrue(failingRequestStarted.await(5, TimeUnit.SECONDS),
                    "The failing token request did not start within the expected time.");

            cache.put(key, wrapCredential(healthyCredential));
            SqlAuthenticationToken successfulResult = SQLServerSecurityUtility
                    .getManagedIdentityCredAuthToken(MI_TEST_RESOURCE, MI_TEST_CLIENT_ID, 5000L);
            assertEquals("healthy-token", successfulResult.getAccessToken(),
                    "The concurrent caller should succeed with the replacement credential.");

            failingTokenFuture.completeExceptionally(transientFailure);
            SQLServerException failure = failingCall.get(5, TimeUnit.SECONDS);

            assertTokenAcquisitionError(failure);
            assertSame(healthyCredential, unwrapCredential(cache.get(key)),
                    "A concurrent failure must not evict the credential used by the successful caller.");
        } finally {
            failingTokenFuture.completeExceptionally(transientFailure);
            executor.shutdownNow();
        }
    }

    @Test
    public void testDefaultAzureCredentialTransientFailureEvictsCachedCredential() throws Exception {
        Map<String, Object> cache = getCredentialCache();
        String key = defaultAzureCredentialCacheKey();

        DefaultAzureCredential mockCredential = mock(DefaultAzureCredential.class);
        when(mockCredential.getToken(any(TokenRequestContext.class)))
                .thenReturn(Mono.error(new RuntimeException("Simulated transient Default Azure Credential failure")));

        cache.put(key, wrapCredential(mockCredential));

        SQLServerException exception = assertThrows(SQLServerException.class, () -> SQLServerSecurityUtility
                .getDefaultAzureCredAuthToken(MI_TEST_RESOURCE, MI_TEST_CLIENT_ID, 5000));

        assertTokenAcquisitionError(exception);
        assertFalse(cache.containsKey(key),
                "A Default Azure Credential poisoned by a transient failure must be evicted.");
    }

    @Test
    public void testDefaultAzureCredentialEmptyTokenEvictsCachedCredential() throws Exception {
        Map<String, Object> cache = getCredentialCache();
        String key = defaultAzureCredentialCacheKey();

        DefaultAzureCredential mockCredential = mock(DefaultAzureCredential.class);
        when(mockCredential.getToken(any(TokenRequestContext.class))).thenReturn(Mono.empty());

        cache.put(key, wrapCredential(mockCredential));

        SQLServerException exception = assertThrows(SQLServerException.class, () -> SQLServerSecurityUtility
                .getDefaultAzureCredAuthToken(MI_TEST_RESOURCE, MI_TEST_CLIENT_ID, 5000));

        assertEmptyTokenAcquisitionFailure(exception);
        assertFalse(cache.containsKey(key), "A Default Azure Credential that returns no token must be evicted.");
    }

    @Test
    public void testDefaultAzureCredentialSuccessfulTokenKeepsCachedCredential() throws Exception {
        Map<String, Object> cache = getCredentialCache();
        String key = defaultAzureCredentialCacheKey();

        AccessToken token = new AccessToken("dummy-dac-token", OffsetDateTime.now().plus(Duration.ofHours(1)));
        DefaultAzureCredential mockCredential = mock(DefaultAzureCredential.class);
        when(mockCredential.getToken(any(TokenRequestContext.class))).thenReturn(Mono.just(token));
        cache.put(key, wrapCredential(mockCredential));

        SqlAuthenticationToken result = SQLServerSecurityUtility
                .getDefaultAzureCredAuthToken(MI_TEST_RESOURCE, MI_TEST_CLIENT_ID, 5000);

        assertNotNull(result, "A valid token should be returned on success.");
        assertEquals("dummy-dac-token", result.getAccessToken(), "Unexpected access token returned.");
        assertSame(mockCredential, unwrapCredential(cache.get(key)),
                "A working Default Azure Credential must remain cached across successful calls.");
    }

    @Test
    public void testDefaultAzureCredentialFailureDoesNotEvictHealthyReplacement() throws Exception {
        Map<String, Object> cache = getCredentialCache();
        String key = defaultAzureCredentialCacheKey();

        DefaultAzureCredential failingCredential = mock(DefaultAzureCredential.class);
        DefaultAzureCredential healthyCredential = mock(DefaultAzureCredential.class);
        AccessToken token = new AccessToken("healthy-dac-token", OffsetDateTime.now().plus(Duration.ofHours(1)));
        when(healthyCredential.getToken(any(TokenRequestContext.class))).thenReturn(Mono.just(token));
        when(failingCredential.getToken(any(TokenRequestContext.class))).thenAnswer(invocation -> {
            cache.put(key, wrapCredential(healthyCredential));
            return Mono.error(new RuntimeException("Simulated transient Default Azure Credential failure"));
        });
        cache.put(key, wrapCredential(failingCredential));

        SQLServerException exception = assertThrows(SQLServerException.class, () -> SQLServerSecurityUtility
                .getDefaultAzureCredAuthToken(MI_TEST_RESOURCE, MI_TEST_CLIENT_ID, 5000));

        assertTokenAcquisitionError(exception);
        assertSame(healthyCredential, unwrapCredential(cache.get(key)),
                "The failing Default Azure Credential must not evict its healthy replacement.");
    }

    public Method mockedConnectionRecoveryCheck() throws Exception {
        mockConnection = spy(new SQLServerConnection("test"));
        mockLogger = mock(Logger.class);
        doReturn(true).when(mockLogger).isLoggable(Level.WARNING);
        doNothing().when(mockConnection).terminate(anyInt(), anyString());

        Method method = SQLServerConnection.class.getDeclaredMethod("connectionReconveryCheck", boolean.class,
                boolean.class, ServerPortPlaceHolder.class);
        method.setAccessible(true);
        return method;
    }

    @Test
    @Tag(Constants.CodeCov)
    void testConnectionRecoveryCheckThrowsWhenAllConditionsMet() throws Exception {
        Method method = mockedConnectionRecoveryCheck();
        method.invoke(mockConnection, true, false, null);
        verify(mockConnection, times(1)).terminate(eq(SQLServerException.DRIVER_ERROR_INVALID_TDS),
                eq(SQLServerException.getErrString("R_crClientNoRecoveryAckFromLogin")));
    }

    @Test
    @Tag(Constants.CodeCov)
    void testConnectionRecoveryCheckDoesNotThrowWhenNotReconnectRunning() throws Exception {
        Method method = mockedConnectionRecoveryCheck();
        method.invoke(mockConnection, false, false, null);
        verify(mockConnection, never()).terminate(anyInt(), anyString());
    }

    @Test
    @Tag(Constants.CodeCov)
    void testConnectionRecoveryCheckDoesNotThrowWhenRecoveryPossible() throws Exception {
        Method method = mockedConnectionRecoveryCheck();
        method.invoke(mockConnection, true, true, null);
        verify(mockConnection, never()).terminate(anyInt(), anyString());
    }

    @Test
    @Tag(Constants.CodeCov)
    void testConnectionRecoveryCheckDoesNotThrowWhenRoutingDetailsNotNull() throws Exception {
        Method method = mockedConnectionRecoveryCheck();
        ServerPortPlaceHolder routingDetails = mock(ServerPortPlaceHolder.class);
        method.setAccessible(true);
        method.invoke(mockConnection, true, false, routingDetails);
        verify(mockConnection, never()).terminate(anyInt(), anyString());
    }

    /**
     * Test generateEnclavePackage for coverage.
     * This test checks that the method can be called and returns a non-null result for dummy input.
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testGenerateEnclavePackager() throws Exception {
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);

        try (SQLServerConnection conn = ctor.newInstance("test")) {
            // Set enclaveProvider to a mock that returns a dummy byte array
            ISQLServerEnclaveProvider mockProvider = org.mockito.Mockito.mock(ISQLServerEnclaveProvider.class);
            byte[] dummyPackage = new byte[] { 1, 2, 3 };
            org.mockito.Mockito.when(mockProvider.getEnclavePackage(org.mockito.Mockito.anyString(),
                    org.mockito.ArgumentMatchers.<ArrayList<byte[]>>any())).thenReturn(dummyPackage);
            java.lang.reflect.Field enclaveProviderField = SQLServerConnection.class
                    .getDeclaredField("enclaveProvider");
            enclaveProviderField.setAccessible(true);
            enclaveProviderField.set(conn, mockProvider);

            ArrayList<byte[]> enclaveCEKs = new ArrayList<>();
            enclaveCEKs.add(new byte[] { 4, 5, 6 });
            byte[] result = conn.generateEnclavePackage("SELECT 1", enclaveCEKs);
            assertNotNull(result);
            assertArrayEquals(dummyPackage, result);
        }
    }

    /**
     * Covers both null and non-null enclaveProvider cases.
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testInvalidateEnclaveSessionCache() throws Exception {
        // Create SQLServerConnection instance via reflection
        java.lang.reflect.Constructor<SQLServerConnection> ctor = SQLServerConnection.class
                .getDeclaredConstructor(String.class);
        ctor.setAccessible(true);

        try (SQLServerConnection conn = ctor.newInstance("test")) {
            // Get the enclaveProvider field via reflection
            java.lang.reflect.Field enclaveProviderField = SQLServerConnection.class
                    .getDeclaredField("enclaveProvider");
            enclaveProviderField.setAccessible(true);

            // Case 1: enclaveProvider is null, should not throw
            enclaveProviderField.set(conn, null);
            try {
                conn.invalidateEnclaveSessionCache();
            } catch (Exception e) {
                fail("Should not throw when enclaveProvider is null: " + e.getMessage());
            }

            // Case 2: enclaveProvider is not null, should call
            // invalidateEnclaveSessionCache() on provider
            ISQLServerEnclaveProvider mockProvider = org.mockito.Mockito.mock(ISQLServerEnclaveProvider.class);
            enclaveProviderField.set(conn, mockProvider);
            conn.invalidateEnclaveSessionCache();
            // Verify that invalidateEnclaveSession() was called on the mock provider when
            // not null
            org.mockito.Mockito.verify(mockProvider).invalidateEnclaveSession();
        }
    }

    private void setConnectionField(SQLServerConnection conn, String fieldName, Object value) throws Exception {
        java.lang.reflect.Field field = SQLServerConnection.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(conn, value);
    }

    @Test
    @Tag(Constants.CodeCov)
    public void testConnectActiveDirectoryInteractiveTimeout() throws Exception {
        SQLServerConnection conn = new SQLServerConnection("test");
        setConnectionField(conn, "authenticationString", "ActiveDirectoryInteractive");
        Properties props = new Properties();
        props.setProperty("loginTimeout", "1");
        // connectInternal will throw, but we want to check the timeout is multiplied
        SQLServerConnection spyConn = spy(conn);
        doThrow(new SQLServerException("fail", null, 0, null)).when(spyConn).connectInternal(any(), any());
        assertThrows(SQLServerException.class, () -> spyConn.connect(props, null));
        // If you want to check the timeout value, you can expose it via reflection or add a getter for testing.
    }

    @Test
    @Tag(Constants.CodeCov)
    public void testConnectInvalidateEnclaveSessionCacheCalled() throws Exception {
        SQLServerConnection conn = spy(new SQLServerConnection("test"));
        doNothing().when(conn).invalidateEnclaveSessionCache();
        doThrow(new SQLServerException("fail", null, 0, null)).when(conn).connectInternal(any(), any());
        Properties props = new Properties();
        props.setProperty("loginTimeout", "1");
        assertThrows(SQLServerException.class, () -> conn.connect(props, null));
        verify(conn, atLeastOnce()).invalidateEnclaveSessionCache();
    }
}
