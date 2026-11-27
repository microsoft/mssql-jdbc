/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.SQLServerAccessTokenCallback;
import com.microsoft.sqlserver.jdbc.SqlAuthenticationToken;


class OtlpConfigurationTest {
    static Properties properties(String endpoint) {
        Properties properties = new Properties();
        properties.setProperty("otelEndpoint", endpoint);
        return properties;
    }

    @Test
    void normalizesOnlyTheTerminalSignalPathAndPreservesUriEscaping() {
        for (String suffix : new String[] {"", "/", "/v1/metrics", "/v1/traces/"}) {
            OtlpConfiguration config = new OtlpConfiguration(
                    properties("https://collector.example/base%20path" + suffix));
            assertEquals("https://collector.example/base%20path/v1/traces", config.endpoint);
        }
        assertEquals("https://collector.example/v1/metrics/base/v1/traces",
                new OtlpConfiguration(properties("https://collector.example/v1/metrics/base")).endpoint);
    }

    @Test
    void endpointErrorsNeverEchoInputAndHttpRequiresNarrowOptIn() {
        for (String endpoint : new String[] {"", "https://user:SECRET@collector.example",
                "https://collector.example?SECRET", "https://collector.example#SECRET", "ftp://collector.example",
                "//collector.example", "https://bad host/SECRET", "http://localhost", "http://127.0.0.1",
                "http://[::1]", "https://collector.example:0"}) {
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> new OtlpConfiguration(properties(endpoint)));
            assertFalse(error.toString().contains("SECRET"));
            assertNull(error.getCause());
        }
        for (String host : new String[] {"localhost", "127.0.0.1", "[::1]"}) {
            Properties properties = properties("http://" + host + ":4318");
            properties.setProperty("otelAllowInsecureLocalEndpoint", "true");
            assertEquals("http://" + host + ":4318/v1/traces", new OtlpConfiguration(properties).endpoint);
        }
        Properties properties = properties("http://collector:4318");
        properties.setProperty("otelAllowInsecureLocalEndpoint", "true");
        assertThrows(IllegalArgumentException.class, () -> new OtlpConfiguration(properties));
        properties.setProperty("otelAllowInsecureDevelopmentEndpoint", "true");
        assertEquals("http://collector:4318/v1/traces", new OtlpConfiguration(properties).endpoint);
        properties.setProperty("otelEndpoint", "http://collector.example:4318");
        assertThrows(IllegalArgumentException.class, () -> new OtlpConfiguration(properties));
    }

    @Test
    void headersAreValidatedCaseInsensitivelyWithoutOverridesOrInjection() {
        for (String header : new String[] {"AUTHORIZATION=SECRET", "Host=x", "Content-Length=1", "Forwarded=x",
                "X-Forwarded-For=x", "x-ms-arm-resource-id=x", "Content-Type=x", "User-Agent=x", "Connection=x",
                "X-Test=a,X-TEST=b", "bad header=x", "X-Test=a\r\nSECRET", "X-Test=", "missing-equals"}) {
            Properties properties = properties("https://collector.example");
            properties.setProperty("otelHeaders", header);
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> new OtlpConfiguration(properties));
            assertFalse(error.toString().contains("SECRET"));
        }
        Properties properties = properties("https://collector.example");
        properties.setProperty("otelHeaders", "X-Tenant=one,x-extra=a=b");
        properties.setProperty("otelArmResourceId", "/subscriptions/test/resourceGroups/test");
        properties.setProperty("otelBearerToken", "STATIC");
        OtlpConfiguration config = new OtlpConfiguration(properties);
        properties.setProperty("otelBearerToken", "MUTATED");
        assertEquals("Bearer STATIC", config.get().get("Authorization"));
        assertEquals("one", config.get().get("x-tenant"));
        assertEquals("a=b", config.get().get("x-extra"));
        assertEquals("/subscriptions/test/resourceGroups/test", config.get().get("x-ms-arm-resource-id"));
    }

    @Test
    void callbackIsLazyRefreshesShortLivedTokensAndIsIsolatedPerInstance() {
        AtomicLong clock = new AtomicLong(10_000);
        Properties properties = callbackProperties();
        OtlpConfiguration first = new OtlpConfiguration(properties, clock::get);
        OtlpConfiguration second = new OtlpConfiguration(properties, clock::get);
        RefreshCallback.now = clock;
        RefreshCallback.fail = false;
        RefreshCallback.expired = false;
        RefreshCallback.constructed = 0;
        assertEquals(0, RefreshCallback.constructed);
        assertEquals("Bearer TOKEN-1-scope-tenant", first.get().get("Authorization"));
        assertEquals(1, RefreshCallback.constructed);
        clock.set(10_079);
        assertEquals("Bearer TOKEN-1-scope-tenant", first.get().get("Authorization"));
        clock.set(10_080);
        assertEquals("Bearer TOKEN-2-scope-tenant", first.get().get("Authorization"));
        assertEquals("Bearer TOKEN-1-scope-tenant", second.get().get("Authorization"));
        assertEquals(2, RefreshCallback.constructed);
        Properties otherTenant = callbackProperties();
        otherTenant.setProperty("otelTokenAuthority", "other");
        assertEquals("Bearer TOKEN-1-scope-other",
                new OtlpConfiguration(otherTenant, clock::get).get().get("Authorization"));
    }

    @Test
    void failedRefreshBacksOffAndNeverFallsBackToStaticOrExpiredToken() {
        AtomicLong clock = new AtomicLong(10_000);
        RefreshCallback.now = clock;
        RefreshCallback.fail = false;
        RefreshCallback.expired = false;
        OtlpConfiguration config = new OtlpConfiguration(callbackProperties(), clock::get);
        assertEquals("Bearer TOKEN-1-scope-tenant", config.get().get("Authorization"));
        clock.set(10_080);
        RefreshCallback.fail = true;
        IllegalStateException error = assertThrows(IllegalStateException.class, config::get);
        assertFalse(error.toString().contains("SECRET"));
        assertNull(error.getCause());
        RefreshCallback.fail = false;
        clock.set(10_101);
        assertThrows(IllegalStateException.class, config::get);
        clock.set(11_081);
        assertEquals("Bearer TOKEN-3-scope-tenant", config.get().get("Authorization"));
        clock.set(11_181);
        RefreshCallback.expired = true;
        try {
            assertThrows(IllegalStateException.class, config::get);
        } finally {
            RefreshCallback.expired = false;
        }
    }

    @Test
    void authConfigurationRequiresActualScopeAndAuthorityAndRejectsUnsafeTokens() {
        Properties properties = callbackProperties();
        properties.remove("otelTokenScope");
        assertThrows(IllegalArgumentException.class, () -> new OtlpConfiguration(properties));
        Properties invalid = properties("https://collector.example");
        for (String token : new String[] {"", "Bearer ", "secret\r\nx=y", "contains space"}) {
            invalid.setProperty("otelBearerToken", token);
            assertThrows(IllegalArgumentException.class, () -> new OtlpConfiguration(invalid));
        }
    }

    private static Properties callbackProperties() {
        Properties properties = properties("https://collector.example");
        properties.setProperty("otelAccessTokenCallbackClass", RefreshCallback.class.getName());
        properties.setProperty("otelTokenScope", "scope");
        properties.setProperty("otelTokenAuthority", "tenant");
        properties.setProperty("otelBearerToken", "STATIC-NOT-A-FALLBACK");
        return properties;
    }

    public static final class RefreshCallback implements SQLServerAccessTokenCallback {
        static AtomicLong now;
        static boolean fail;
        static boolean expired;
        static int constructed;
        private int calls;

        public RefreshCallback() {
            constructed++;
        }

        @Override
        public SqlAuthenticationToken getAccessToken(String scope, String authority) {
            calls++;
            if (fail) {
                throw new IllegalStateException("SECRET provider diagnostics");
            }
            return new SqlAuthenticationToken("TOKEN-" + calls + "-" + scope + "-" + authority,
                    now.get() + (expired ? -1 : 100));
        }
    }
}
