/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
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
}