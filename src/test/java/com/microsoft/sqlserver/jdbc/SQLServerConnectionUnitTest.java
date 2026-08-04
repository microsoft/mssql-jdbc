/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ManagedIdentityCredential;

import com.microsoft.sqlserver.testframework.Constants;

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

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getCredentialCache() throws Exception {
        Field cacheField = SQLServerSecurityUtility.class.getDeclaredField("CREDENTIAL_CACHE");
        cacheField.setAccessible(true);
        return (Map<String, Object>) cacheField.get(null);
    }

    private static Object wrapCredential(Object tokenCredential) throws Exception {
        Class<?> credClass = Class.forName("com.microsoft.sqlserver.jdbc.SQLServerSecurityUtility$Credential");
        Constructor<?> ctor = credClass.getDeclaredConstructor(Object.class);
        ctor.setAccessible(true);
        return ctor.newInstance(tokenCredential);
    }

    private static String managedIdentityCacheKey() throws SQLServerException {
        return Util.getHashedSecret(
                new String[] {MI_TEST_CLIENT_ID, ManagedIdentityCredential.class.getSimpleName()});
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

        try {
            assertThrows(SQLServerException.class, () -> SQLServerSecurityUtility
                    .getManagedIdentityCredAuthToken(MI_TEST_RESOURCE, MI_TEST_CLIENT_ID, 5000L));

            assertFalse(cache.containsKey(key),
                    "A credential poisoned by a transient failure must be evicted so a fresh one can be built.");
        } finally {
            cache.remove(key);
        }
    }

    @Test
    public void testManagedIdentitySuccessfulTokenKeepsCachedCredential() throws Exception {
        Map<String, Object> cache = getCredentialCache();
        String key = managedIdentityCacheKey();

        AccessToken token = new AccessToken("dummy-token", OffsetDateTime.now().plus(Duration.ofHours(1)));
        ManagedIdentityCredential mockCredential = mock(ManagedIdentityCredential.class);
        when(mockCredential.getToken(any(TokenRequestContext.class))).thenReturn(Mono.just(token));

        cache.put(key, wrapCredential(mockCredential));

        try {
            SqlAuthenticationToken result = SQLServerSecurityUtility
                    .getManagedIdentityCredAuthToken(MI_TEST_RESOURCE, MI_TEST_CLIENT_ID, 5000L);

            assertNotNull(result, "A valid token should be returned on success.");
            assertEquals("dummy-token", result.getAccessToken(), "Unexpected access token returned.");
            assertTrue(cache.containsKey(key), "A working credential must remain cached across successful calls.");
        } finally {
            cache.remove(key);
        }
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
        AccessToken token = new AccessToken("healthy-token", OffsetDateTime.now().plus(Duration.ofHours(1)));
        // When the failing credential's token request is invoked, simulate another thread having already replaced the
        // cached entry with a fresh, healthy credential before this failure is handled.
        when(failingCredential.getToken(any(TokenRequestContext.class))).thenAnswer(invocation -> {
            ManagedIdentityCredential healthyCredential = mock(ManagedIdentityCredential.class);
            when(healthyCredential.getToken(any(TokenRequestContext.class))).thenReturn(Mono.just(token));
            cache.put(key, wrapCredential(healthyCredential));
            return Mono.error(new RuntimeException("Simulated transient Managed Identity endpoint outage"));
        });

        cache.put(key, wrapCredential(failingCredential));

        try {
            assertThrows(SQLServerException.class, () -> SQLServerSecurityUtility
                    .getManagedIdentityCredAuthToken(MI_TEST_RESOURCE, MI_TEST_CLIENT_ID, 5000L));

            assertTrue(cache.containsKey(key),
                    "The healthy replacement credential must remain cached; only the failing instance may be evicted.");
        } finally {
            cache.remove(key);
        }
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
