/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;

class OtelBootstrapTest {

    @Test
    void otlpHeadersIncludeBearerTokenAndPreserveOtherHeaders() {
        Properties props = new Properties();
        props.setProperty(SQLServerDriverStringProperty.OTEL_HEADERS.toString(), "x-api-key=abc,tenant=demo");
        props.setProperty(SQLServerDriverBooleanProperty.OTEL_USE_SQL_ACCESS_TOKEN.toString(), "true");

        SqlAuthenticationToken sqlAuthToken = new SqlAuthenticationToken("token123", System.currentTimeMillis() + 60000L);

        Map<String, String> headers = Arrays.stream(OtelBootstrap.otlpHeaders(props, sqlAuthToken))
                .collect(Collectors.toMap(pair -> pair[0], pair -> pair[1]));

        assertEquals("abc", headers.get("x-api-key"));
        assertEquals("demo", headers.get("tenant"));
        assertEquals("Bearer token123", headers.get("Authorization"));
        assertFalse(headers.containsKey("authorization"), "header keys should preserve the configured casing");
    }

    @Test
    void otlpHeadersHonorExplicitBearerPrefix() {
        Properties props = new Properties();
        props.setProperty(SQLServerDriverStringProperty.OTEL_BEARER_TOKEN.toString(), "Bearer token456");

        String[][] headers = OtelBootstrap.otlpHeaders(props, null);

        assertEquals(1, headers.length);
        assertEquals("Authorization", headers[0][0]);
        assertEquals("Bearer token456", headers[0][1]);
    }

    @Test
    void sqlAccessTokenTakesPrecedenceOverExplicitBearerToken() {
        Properties props = new Properties();
        props.setProperty(SQLServerDriverBooleanProperty.OTEL_USE_SQL_ACCESS_TOKEN.toString(), "true");
        props.setProperty(SQLServerDriverStringProperty.OTEL_BEARER_TOKEN.toString(), "Bearer token456");

        SqlAuthenticationToken sqlAuthToken = new SqlAuthenticationToken("token123", System.currentTimeMillis() + 60000L);

        String[][] headers = OtelBootstrap.otlpHeaders(props, sqlAuthToken);

        assertEquals(1, headers.length);
        assertEquals("Authorization", headers[0][0]);
        assertEquals("Bearer token123", headers[0][1]);
    }

    @Test
    void callbackProvidesBearerWhenNoSqlToken() {
        Properties props = new Properties();
        props.setProperty(SQLServerDriverStringProperty.OTEL_ACCESS_TOKEN_CALLBACK_CLASS.toString(),
                StubCallback.class.getName());

        String[][] headers = OtelBootstrap.otlpHeaders(props, null);

        assertEquals(1, headers.length);
        assertEquals("Authorization", headers[0][0]);
        assertEquals("Bearer cbToken", headers[0][1]);
    }

    @Test
    void callbackTakesPrecedenceOverExplicitBearerToken() {
        Properties props = new Properties();
        props.setProperty(SQLServerDriverStringProperty.OTEL_ACCESS_TOKEN_CALLBACK_CLASS.toString(),
                StubCallback.class.getName());
        props.setProperty(SQLServerDriverStringProperty.OTEL_BEARER_TOKEN.toString(), "staticToken");

        Map<String, String> headers = Arrays.stream(OtelBootstrap.otlpHeaders(props, null))
                .collect(Collectors.toMap(pair -> pair[0], pair -> pair[1]));

        assertEquals("Bearer cbToken", headers.get("Authorization"));
    }

    @Test
    void sqlAccessTokenTakesPrecedenceOverCallback() {
        Properties props = new Properties();
        props.setProperty(SQLServerDriverBooleanProperty.OTEL_USE_SQL_ACCESS_TOKEN.toString(), "true");
        props.setProperty(SQLServerDriverStringProperty.OTEL_ACCESS_TOKEN_CALLBACK_CLASS.toString(),
                StubCallback.class.getName());

        SqlAuthenticationToken sqlAuthToken = new SqlAuthenticationToken("sqlToken",
                System.currentTimeMillis() + 60000L);

        String[][] headers = OtelBootstrap.otlpHeaders(props, sqlAuthToken);

        assertEquals(1, headers.length);
        assertEquals("Bearer sqlToken", headers[0][1]);
    }

    @Test
    void callbackFailureFallsBackToExplicitBearerToken() {
        Properties props = new Properties();
        props.setProperty(SQLServerDriverStringProperty.OTEL_ACCESS_TOKEN_CALLBACK_CLASS.toString(),
                ThrowingCallback.class.getName());
        props.setProperty(SQLServerDriverStringProperty.OTEL_BEARER_TOKEN.toString(), "fallbackToken");

        String[][] headers = OtelBootstrap.otlpHeaders(props, null);

        assertEquals(1, headers.length);
        assertEquals("Bearer fallbackToken", headers[0][1]);
    }

    @Test
    void callbackBearerRefreshesWhenTokenExpires() throws Exception {
        resetCallbackTokenCache();
        CountingCallback.mintCount.set(0);
        CountingCallback.expiryOffsetMs = 0L; // already past the refresh skew -> re-mint on every export

        Properties props = new Properties();
        props.setProperty(SQLServerDriverStringProperty.OTEL_ACCESS_TOKEN_CALLBACK_CLASS.toString(),
                CountingCallback.class.getName());

        String first = OtelBootstrap.currentBearer(props);
        String second = OtelBootstrap.currentBearer(props);
        String third = OtelBootstrap.currentBearer(props);

        assertEquals("Bearer token-1", first);
        assertEquals("Bearer token-2", second);
        assertEquals("Bearer token-3", third);
        assertEquals(3, CountingCallback.mintCount.get(),
                "an expired callback token must be re-minted on every OTLP export");
    }

    @Test
    void callbackBearerIsCachedUntilNearExpiry() throws Exception {
        resetCallbackTokenCache();
        CountingCallback.mintCount.set(0);
        CountingCallback.expiryOffsetMs = 3_600_000L; // 1h ahead, well beyond the 5-min refresh skew

        Properties props = new Properties();
        props.setProperty(SQLServerDriverStringProperty.OTEL_ACCESS_TOKEN_CALLBACK_CLASS.toString(),
                CountingCallback.class.getName());

        String first = OtelBootstrap.currentBearer(props);
        String second = OtelBootstrap.currentBearer(props);
        String third = OtelBootstrap.currentBearer(props);

        assertEquals("Bearer token-1", first);
        assertEquals(first, second);
        assertEquals(first, third);
        assertEquals(1, CountingCallback.mintCount.get(),
                "a valid callback token must be reused, not re-minted, on each export");
    }

    /** Clears the static callback-token cache so rotation assertions start from a known state. */
    private static void resetCallbackTokenCache() throws Exception {
        Field field = OtelBootstrap.class.getDeclaredField("callbackToken");
        field.setAccessible(true);
        ((AtomicReference<?>) field.get(null)).set(null);
    }

    /** Stub callback returning a fixed token, loaded by name via otelAccessTokenCallbackClass. */
    public static class StubCallback implements SQLServerAccessTokenCallback {
        @Override
        public SqlAuthenticationToken getAccessToken(String spn, String stsurl) {
            return new SqlAuthenticationToken("cbToken", System.currentTimeMillis() + 60000L);
        }
    }

    /** Stub callback that fails, to verify resolution is non-fatal and falls through. */
    public static class ThrowingCallback implements SQLServerAccessTokenCallback {
        @Override
        public SqlAuthenticationToken getAccessToken(String spn, String stsurl) {
            throw new RuntimeException("simulated token acquisition failure");
        }
    }

    /** Callback that mints a fresh, changing token each call with a configurable expiry, for rotation tests. */
    public static class CountingCallback implements SQLServerAccessTokenCallback {
        static final AtomicInteger mintCount = new AtomicInteger(0);
        static long expiryOffsetMs = 0L;

        @Override
        public SqlAuthenticationToken getAccessToken(String spn, String stsurl) {
            int n = mintCount.incrementAndGet();
            return new SqlAuthenticationToken("token-" + n, System.currentTimeMillis() + expiryOffsetMs);
        }
    }
}