/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.baggage.Baggage;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import io.opentelemetry.context.Scope;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.ConnectionEventFixture;
import com.microsoft.sqlserver.jdbc.PerformanceActivity;
import com.microsoft.sqlserver.jdbc.PerformanceLogCallback;
import com.microsoft.sqlserver.jdbc.PerformanceLogEvent;
import com.microsoft.sqlserver.jdbc.SQLServerAccessTokenCallback;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.SqlAuthenticationToken;
import com.microsoft.sqlserver.jdbc.UserOtelTelemetryProvider;
import com.sun.net.httpserver.HttpServer;


class UserEndpointTelemetryTest {
    private static final Duration WAIT = Duration.ofSeconds(10);

    private UserOtelTelemetryProvider provider() {
        java.util.Iterator<UserOtelTelemetryProvider> providers = ServiceLoader.load(UserOtelTelemetryProvider.class)
                .iterator();
        assertTrue(providers.hasNext(), "Optional module must register its automatic provider");
        return providers.next();
    }

    private void flush() throws Exception {
        assertEquals(Boolean.TRUE, provider().getClass().getMethod("forceFlush", Duration.class).invoke(null, WAIT));
    }

    @AfterEach
    void cleanup() throws Exception {
        SQLServerDriver.unregisterPerformanceLogCallback();
        java.util.Iterator<UserOtelTelemetryProvider> providers = ServiceLoader.load(UserOtelTelemetryProvider.class)
                .iterator();
        if (providers.hasNext()) {
            providers.next().getClass().getMethod("shutdown").invoke(null);
        }
    }

    @Test
    void automaticallyRoutesRealCoreFailureAlongsideApplicationCallback() throws Exception {
        try (Collector collector = new Collector()) {
            AtomicInteger legacy = new AtomicInteger();
            SQLServerDriver.registerPerformanceLogCallback(new PerformanceLogCallback() {
                @Override
                public void publish(PerformanceActivity activity, int id, long duration, Exception exception) {
                    legacy.incrementAndGet();
                }

                @Override
                public void publish(PerformanceActivity activity, int id, int statement, long duration,
                        Exception exception) {}
            });
            Properties options = collector.options(2);
            options.setProperty("portNumber", "invalid");
            assertThrows(SQLException.class, () -> new SQLServerDriver().connect("jdbc:sqlserver://", options));
            flush();
            assertTrue(legacy.get() > 0);
            assertTrue(collector.body("/v1/traces").contains("mssql.driver.connection.open"));
            assertTrue(collector.body("/v1/metrics").contains("db.client.operation.duration"));
        }
    }

    @Test
    void levelsApplyToConnectionAndStatementRootsButNeverFilterMetrics() throws Exception {
        for (int level = 0; level <= 2; level++) {
            try (Collector collector = new Collector()) {
                PerformanceLogCallback callback = provider().create(collector.options(level));
                for (boolean statement : new boolean[] {false, true}) {
                    tree(callback, statement ? 10 : 1, statement, false, 100);
                    tree(callback, statement ? 20 : 2, statement, true, 100);
                }
                flush();
                String traces = collector.body("/v1/traces");
                if (level == 0) {
                    assertTrue(traces.isEmpty());
                } else {
                    assertTrue(traces.contains("mssql.driver.connection.open"));
                    assertTrue(traces.contains("mssql.driver.statement.execute"));
                    assertTrue(traces.contains("mssql.driver.error"));
                    assertEquals(level == 1 ? 1 : 2, occurrences(traces, "mssql.driver.connection.open"));
                    assertEquals(level == 1 ? 1 : 2, occurrences(traces, "mssql.driver.statement.execute"));
                }
                assertFalse(traces.contains("SECRET"));
                String metrics = collector.body("/v1/metrics");
                assertTrue(metrics.contains("db.client.operation.count"));
                for (String activity : new String[] {"connection.open", "statement.execute", "connection.dns",
                        "statement.first_response"}) {
                    assertEquals(2, collector.counter("db.client.operation.count", activity));
                    assertEquals(1, collector.counter("db.client.operation.error.count", activity));
                }
                for (String forbidden : new String[] {"SECRET", "db.query.text", "mssql.connection.guid",
                        "mssql.statement.update_count", "scope_id"}) {
                    assertFalse(metrics.contains(forbidden), forbidden);
                }
            } finally {
                cleanup();
            }
        }
    }

    @Test
    void strictDecimalThresholdsIncludeFailedRootsAndAbsentDiffersFromZero() throws Exception {
        for (boolean statement : new boolean[] {false, true}) {
            for (String threshold : new String[] {"absent", "0", "0.0000000105", "0.000000011"}) {
                try (Collector collector = new Collector()) {
                    Properties options = collector.options(2);
                    if (!"absent".equals(threshold)) {
                        options.setProperty(statement ? "otelStatementSpanDurationThreshold"
                                                      : "otelConnectionSpanDurationThreshold",
                                threshold);
                    }
                    PerformanceLogCallback callback = provider().create(options);
                    tree(callback, 100, statement, true, "absent".equals(threshold) || "0".equals(threshold) ? 0 : 11);
                    flush();
                    boolean expected = "absent".equals(threshold) || "0.0000000105".equals(threshold);
                    assertEquals(expected, !collector.body("/v1/traces").isEmpty(), threshold);
                    assertTrue(collector.body("/v1/metrics").contains("db.client.operation.error.count"));
                } finally {
                    cleanup();
                }
            }
        }
    }

    @Test
    void thresholdBoundariesArePerRootKindAndCombineWithFailureFiltering() throws Exception {
        for (int level : new int[] {1, 2}) {
            for (boolean statement : new boolean[] {false, true}) {
                try (Collector collector = new Collector()) {
                    Properties options = collector.options(level);
                    options.setProperty("otelConnectionSpanDurationThreshold", statement ? "100" : "0.000000010");
                    options.setProperty("otelStatementSpanDurationThreshold", statement ? "0.000000010" : "100");
                    PerformanceLogCallback callback = provider().create(options);
                    long id = 1;
                    for (boolean failed : new boolean[] {false, true}) {
                        for (long duration : new long[] {9, 10, 11}) {
                            tree(callback, id++, statement, failed, duration);
                        }
                    }
                    flush();
                    String rootName = statement ? "mssql.driver.statement.execute" : "mssql.driver.connection.open";
                    assertEquals(level == 1 ? 1 : 2, occurrences(collector.body("/v1/traces"), rootName));
                    String activity = statement ? "statement.execute" : "connection.open";
                    assertEquals(6, collector.counter("db.client.operation.count", activity));
                    assertEquals(3, collector.counter("db.client.operation.error.count", activity));
                } finally {
                    cleanup();
                }
            }
        }
    }

    @Test
    void coreNormalizedThresholdsReachTheSpiAndPreserveStrictNanosecondBoundaries() throws Exception {
        for (boolean statement : new boolean[] {false, true}) {
            try (Collector collector = new Collector()) {
                String name = statement ? "otelStatementSpanDurationThreshold" : "otelConnectionSpanDurationThreshold";
                long id = 100;
                for (String seconds : new String[] {"10", "0.000000001", "9223372036.854775807"}) {
                    Properties raw = collector.options(2);
                    raw.setProperty(name, seconds);
                    Properties normalized = ConnectionEventFixture.userOptions(raw);
                    assertEquals(new java.math.BigDecimal(seconds).stripTrailingZeros().toString(),
                            normalized.getProperty(name));
                    PerformanceLogCallback callback = provider().create(normalized);
                    assertNotNull(callback,
                            "Core-normalized threshold must be accepted: " + normalized.getProperty(name));
                    long nanos = new java.math.BigDecimal(seconds).scaleByPowerOfTen(9).longValueExact();
                    tree(callback, id++, statement, true, nanos);
                    if (nanos != Long.MAX_VALUE) {
                        tree(callback, id++, statement, true, nanos + 1);
                    }
                }
                flush();
                String root = statement ? "mssql.driver.statement.execute" : "mssql.driver.connection.open";
                assertEquals(2, occurrences(collector.body("/v1/traces"), root));
                String activity = statement ? "statement.execute" : "connection.open";
                assertEquals(5, collector.counter("db.client.operation.count", activity));
                assertEquals(5, collector.counter("db.client.operation.error.count", activity));
            } finally {
                cleanup();
            }
        }
    }

    @Test
    void normalizedThresholdDoesNotSilentlyDisableRealConnectionMetrics() throws Exception {
        try (Collector collector = new Collector()) {
            Properties options = collector.options(2);
            options.setProperty("otelConnectionSpanDurationThreshold", "10");
            options.setProperty("portNumber", "invalid");
            assertThrows(SQLException.class, () -> new SQLServerDriver().connect("jdbc:sqlserver://", options));
            flush();
            assertTrue(collector.spans().isEmpty());
            assertEquals(1, collector.counter("db.client.operation.count", "connection.open"));
            assertEquals(1, collector.counter("db.client.operation.error.count", "connection.open"));
        }
    }

    @Test
    void everyHttpMessageGetsAFreshCallbackTokenEvenBeforeExpiry() throws Exception {
        PerMessageTokenCallback.calls.set(0);
        try (Collector collector = new Collector()) {
            Properties options = collector.options(2);
            options.setProperty("otelAccessTokenCallback", PerMessageTokenCallback.class.getName());
            PerformanceLogCallback callback = provider().create(options);
            tree(callback, 1, false, false, 10);
            flush();
            tree(callback, 2, true, true, 10);
            flush();
            assertEquals(4, collector.requests.size(), "Two trace messages and two metric messages");
            java.util.Set<String> tokens = new java.util.HashSet<>();
            for (Request request : collector.requests) {
                assertEquals(1, request.authorizationCount, "Exactly one authorization header per message");
                assertTrue(request.authorization.startsWith("Bearer SECRET-message-"));
                assertFalse(request.body.contains("SECRET"));
                tokens.add(request.authorization);
            }
            assertEquals(collector.requests.size(), tokens.size());
            assertEquals(collector.requests.size(), PerMessageTokenCallback.calls.get());
        }
    }

    @Test
    void callbackFailureAfterSuccessNeverReusesAnUnexpiredTokenForEitherSignal() throws Exception {
        PerMessageTokenCallback.calls.set(0);
        PerMessageTokenCallback.fail = false;
        try (Collector collector = new Collector()) {
            Properties options = collector.options(2);
            options.setProperty("otelAccessTokenCallback", PerMessageTokenCallback.class.getName());
            PerformanceLogCallback callback = provider().create(options);
            tree(callback, 1, false, false, 10);
            flush();
            assertEquals(2, collector.requests.size());
            PerMessageTokenCallback.fail = true;
            tree(callback, 2, true, true, 10);
            // Flush waits for export attempts, not delivery: the SDK metric reader completes successfully even
            // when its exporter returns failure. Verify fail-closed authentication at the HTTP boundary instead.
            flush();
            assertEquals(2, collector.requests.size(), "Neither signal may use stale or anonymous credentials");
            assertEquals(3, PerMessageTokenCallback.calls.get(), "Failed callback is backed off across both signals");
        } finally {
            PerMessageTokenCallback.fail = false;
        }
    }

    @Test
    void invalidCallbackTokensFailClosedAndRecoverAfterBackoff() {
        AtomicLong clock = new AtomicLong(1000);
        Properties options = new Properties();
        options.setProperty("otelEndpoint", "http://127.0.0.1:4318");
        options.setProperty("otelAccessTokenCallback", InvalidTokenCallback.class.getName());
        for (String invalid : new String[] {"null", "expired", "empty", "injection", "throw"}) {
            try (OtlpConfiguration config = OtlpConfiguration.forUserEndpoint(options, getClass().getClassLoader(),
                    clock::get)) {
                InvalidTokenCallback.mode = invalid;
                IllegalStateException error = assertThrows(IllegalStateException.class, config::get);
                assertNull(error.getCause());
                assertFalse(error.toString().contains("SECRET"));
                InvalidTokenCallback.mode = "valid";
                assertThrows(IllegalStateException.class, config::get);
                clock.addAndGet(1000);
                assertEquals("Bearer SECRET", config.get().get("Authorization"));
            }
        }
    }

    @Test
    void refreshedHeadersRunOnExportWorkersAndNeverEnterPayload() throws Exception {
        TokenCallback.calls.set(0);
        try (Collector collector = new Collector()) {
            Properties options = collector.options(2);
            options.setProperty("otelAccessTokenCallback", TokenCallback.class.getName());
            PerformanceLogCallback callback = provider().create(options);
            tree(callback, 1, false, true, 10);
            flush();
            Thread.sleep(150); // Let the first short-lived credential expire before the next real HTTP batch.
            tree(callback, 2, true, true, 10);
            flush();
            assertTrue(collector.requests.size() >= 4);
            java.util.Set<String> tokens = new java.util.HashSet<>();
            for (Request request : collector.requests) {
                tokens.add(request.authorization);
                assertTrue(request.authorization.startsWith("Bearer SECRET-"));
                assertFalse(request.body.contains("SECRET"));
            }
            assertTrue(tokens.size() >= 2, "An expired credential must be replaced");
            assertTrue(TokenCallback.calls.get() >= 2);
        }
    }

    @Test
    void headersAreIsolatedByEndpointCallbackAndCapturedLoaderForBothSignals() throws Exception {
        IsolatedTokenCallback.instances.set(0);
        ClassLoader original = Thread.currentThread().getContextClassLoader();
        try (Collector first = new Collector(); Collector second = new Collector()) {
            Properties authenticated = first.options(2);
            authenticated.setProperty("otelAccessTokenCallback", IsolatedTokenCallback.class.getName());
            Thread.currentThread().setContextClassLoader(new ClassLoader(original) {});
            PerformanceLogCallback loaderOne = provider().create(authenticated);
            Thread.currentThread().setContextClassLoader(new ClassLoader(original) {});
            PerformanceLogCallback loaderTwo = provider().create(authenticated);
            PerformanceLogCallback anonymous = provider().create(first.options(2));
            Properties other = second.options(2);
            other.setProperty("otelAccessTokenCallback", IsolatedTokenCallback.class.getName());
            PerformanceLogCallback otherEndpoint = provider().create(other);
            tree(loaderOne, 1, false, false, 10);
            tree(loaderTwo, 1, false, false, 10);
            tree(anonymous, 1, false, false, 10);
            tree(otherEndpoint, 1, false, false, 10);
            flush();
            assertEquals(3, IsolatedTokenCallback.instances.get(),
                    "Endpoint and loader changes require separate credentials");
            java.util.Set<String> firstTokens = new java.util.HashSet<>();
            java.util.Set<String> secondTokens = new java.util.HashSet<>();
            for (String path : new String[] {"/v1/traces", "/v1/metrics"}) {
                firstTokens.clear();
                secondTokens.clear();
                first.requests.stream().filter(r -> path.equals(r.path)).forEach(r -> firstTokens.add(r.authorization));
                second.requests.stream().filter(r -> path.equals(r.path))
                        .forEach(r -> secondTokens.add(r.authorization));
                assertEquals(3, firstTokens.size());
                assertTrue(firstTokens.remove(null), "Anonymous routes must never inherit authorization");
                assertEquals(1, secondTokens.size());
                assertFalse(secondTokens.contains(null));
                assertTrue(Collections.disjoint(firstTokens, secondTokens),
                        "Endpoint credentials must not cross routes");
            }
            assertTrue(first.requests.stream().noneMatch(r -> r.body.contains("SECRET")));
            assertTrue(second.requests.stream().noneMatch(r -> r.body.contains("SECRET")));
        } finally {
            Thread.currentThread().setContextClassLoader(original);
        }
    }

    @Test
    void tokenExpiryAndFailureBackoffAreFailClosed() {
        AtomicLong clock = new AtomicLong(1000);
        Properties options = new Properties();
        options.setProperty("otelEndpoint", "http://127.0.0.1:4318");
        options.setProperty("otelAccessTokenCallback", ControlledTokenCallback.class.getName());
        ControlledTokenCallback.calls.set(0);
        ControlledTokenCallback.expires.set(2000);
        try (OtlpConfiguration config = OtlpConfiguration.forUserEndpoint(options, getClass().getClassLoader(),
                clock::get)) {
            assertEquals("Bearer SECRET", config.get().get("Authorization"));
            assertEquals("Bearer SECRET", config.get().get("Authorization"));
            assertEquals(2, ControlledTokenCallback.calls.get());
            clock.set(2000);
            assertThrows(IllegalStateException.class, config::get);
            ControlledTokenCallback.expires.set(5000);
            assertThrows(IllegalStateException.class, config::get);
            assertEquals(3, ControlledTokenCallback.calls.get());
            clock.set(3000);
            assertEquals("Bearer SECRET", config.get().get("Authorization"));
        }
    }

    @Test
    void capturesCallerContextWithoutInstallingAGlobalSdk() throws Exception {
        try (Collector collector = new Collector()) {
            PerformanceLogCallback callback = provider().create(collector.options(2));
            SpanContext parent = SpanContext.create("1234567890abcdef1234567890abcdef", "1234567890abcdef",
                    TraceFlags.getSampled(), TraceState.getDefault());
            try (Scope ignored = Span.wrap(parent).makeCurrent()) {
                tree(callback, 1, false, false, 123456789);
                assertEquals(parent, Span.current().getSpanContext());
            }
            flush();
            byte[] traceId = new byte[] {0x12, 0x34, 0x56, 0x78, (byte) 0x90, (byte) 0xab, (byte) 0xcd, (byte) 0xef,
                    0x12, 0x34, 0x56, 0x78, (byte) 0x90, (byte) 0xab, (byte) 0xcd, (byte) 0xef};
            assertTrue(collector.requests.stream().filter(r -> "/v1/traces".equals(r.path))
                    .anyMatch(r -> contains(r.bytes, traceId)));
            assertTrue(collector.requests.stream().allMatch(r -> r.authorization == null));
        }
    }

    @Test
    void callbacksStayNonblockingWhenTokenProviderIsBlockedAndFailureIsIsolated() throws Exception {
        BlockingTokenCallback.entered = new java.util.concurrent.CountDownLatch(1);
        BlockingTokenCallback.release = new java.util.concurrent.CountDownLatch(1);
        try (Collector collector = new Collector()) {
            Properties options = collector.options(2);
            options.setProperty("otelAccessTokenCallback", BlockingTokenCallback.class.getName());
            PerformanceLogCallback callback = provider().create(options);
            tree(callback, 1, false, true, 10);
            // Explicit flush starts export on a worker, not on this test/JDBC thread.
            java.util.concurrent.CompletableFuture<Void> flushing = java.util.concurrent.CompletableFuture
                    .runAsync(() -> {
                        try {
                            flush();
                        } catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            assertTrue(BlockingTokenCallback.entered.await(5, TimeUnit.SECONDS));
            org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(Duration.ofSeconds(1), () -> {
                for (int i = 2; i < 1000; i++) {
                    tree(callback, i, true, true, 10);
                }
            });
            BlockingTokenCallback.release.countDown();
            flushing.get(15, TimeUnit.SECONDS);
            assertTrue(collector.requests.isEmpty(), "Token failures must not send unauthenticated requests");
        } finally {
            BlockingTokenCallback.release.countDown();
        }
    }

    @Test
    void sharedTransportDoesNotMixConnectionFiltersOrCollidingRootIds() throws Exception {
        try (Collector collector = new Collector()) {
            PerformanceLogCallback metricsOnly = provider().create(collector.options(0));
            PerformanceLogCallback traces = provider().create(collector.options(2));
            tree(metricsOnly, 1, false, true, 10);
            tree(traces, 1, false, false, 10);
            tree(traces, 1, true, false, 10);
            flush();
            assertEquals(1, occurrences(collector.body("/v1/traces"), "mssql.driver.connection.open"));
            assertEquals(1, occurrences(collector.body("/v1/traces"), "mssql.driver.statement.execute"));
        }
    }

    @Test
    void failuresHaveOriginOnlyEventsAndAllowlistedMetadata() throws Exception {
        try (Collector collector = new Collector()) {
            PerformanceLogCallback callback = provider().create(collector.options(2));
            Map<String, Object> metadata = new java.util.LinkedHashMap<>();
            metadata.put("mssql.connection.guid", "12345678-1234-1234-1234-123456789abc");
            metadata.put("mssql.connection.encrypt", "strict");
            metadata.put("mssql.connection.origin", "datasource");
            metadata.put("mssql.authentication.method", "sql_password");
            metadata.put("password", "SECRET-password");
            metadata.put("server.address", "SECRET-host");
            metadata.put("db.query.text", "SECRET-query");
            long start = 1700000000000000000L;
            for (PerformanceLogEvent.Type type : PerformanceLogEvent.Type.values()) {
                if (type == PerformanceLogEvent.Type.START) {
                    callback.publish(ConnectionEventFixture.event(type, 1, 0, 1, PerformanceActivity.CONNECTION, start,
                            100, null, "dns", false, metadata));
                }
                callback.publish(ConnectionEventFixture.event(type, 2, 1, 1, PerformanceActivity.DNS, start, 100,
                        type == PerformanceLogEvent.Type.END ? new java.net.UnknownHostException("SECRET") : null,
                        "dns", true, metadata));
                if (type == PerformanceLogEvent.Type.END) {
                    callback.publish(ConnectionEventFixture
                            .event(type, 1, 0, 1, PerformanceActivity.CONNECTION, start, 100,
                                    new java.net.UnknownHostException("SECRET"), "dns", false, metadata)
                            .withoutException());
                }
            }
            tree(callback, 20, true, true, 100);
            tree(callback, 30, true, false, 100);
            flush();
            String traces = collector.body("/v1/traces");
            assertEquals(2, occurrences(traces, "mssql.driver.error"), "One event per origin, not per failed ancestor");
            assertTrue(traces.contains("mssql.driver.connection.dns"));
            assertTrue(traces.contains("mssql.driver.statement.first_response"));
            for (String safe : new String[] {"mssql.connection.guid", "mssql.connection.encrypt", "strict",
                    "datasource", "sql_password", "name_resolution", "java.net.UnknownHostException",
                    "mssql.statement.outcome", "mssql.statement.type", "mssql.statement.operation"}) {
                assertTrue(traces.contains(safe), safe);
            }
            assertFalse(traces.contains("SECRET"));
            List<Wire> spans = collector.spans();
            Wire root = spans.stream().filter(s -> "mssql.driver.connection.open".equals(s.text(5))).findFirst().get();
            assertEquals("name_resolution", root.attribute("mssql.error.category"));
            assertEquals("java.net.UnknownHostException", root.attribute("error.type"));
            assertEquals(0, root.children(11).size());
            assertEquals(2, root.children(15).get(0).number(3)); // ERROR, even after withoutException().
            Wire success = spans.stream().filter(s -> "success".equals(s.attribute("mssql.statement.outcome")))
                    .findFirst().get();
            assertEquals(0, success.children(11).size());
            assertEquals(0, success.children(15).get(0).number(3));
        }
    }

    @Test
    void levelOneUsesCapturedFailureNotInferredOutcome() throws Exception {
        try (Collector collector = new Collector()) {
            PerformanceLogCallback callback = provider().create(collector.options(1));
            for (boolean statement : new boolean[] {false, true}) {
                for (boolean failed : new boolean[] {false, true}) {
                    long id = (statement ? 100 : 10) + (failed ? 1 : 0);
                    PerformanceActivity activity = statement ? PerformanceActivity.STATEMENT_INVOCATION
                                                             : PerformanceActivity.CONNECTION;
                    for (PerformanceLogEvent.Type type : PerformanceLogEvent.Type.values()) {
                        PerformanceLogEvent root = ConnectionEventFixture.userEvent(type, id, 0, id, activity,
                                statement, 100,
                                type == PerformanceLogEvent.Type.END && failed ? new SQLException("SECRET") : null);
                        // Failure-looking metadata must not substitute for an actual captured failure.
                        if (!failed) {
                            root = ConnectionEventFixture.withUserAttributes(root, Collections.singletonMap(
                                    statement ? "mssql.statement.outcome" : "mssql.connection.outcome", "failure"));
                        }
                        callback.publish(root.withoutException());
                    }
                }
            }
            flush();
            assertEquals(1, occurrences(collector.body("/v1/traces"), "mssql.driver.connection.open"));
            assertEquals(1, occurrences(collector.body("/v1/traces"), "mssql.driver.statement.execute"));
            for (String activity : new String[] {"connection.open", "statement.execute"}) {
                assertEquals(2, collector.counter("db.client.operation.count", activity));
                // Shared metrics honor the explicit outcome schema; trace admission still uses captured failures.
                assertEquals(2, collector.counter("db.client.operation.error.count", activity));
            }
        }
    }

    @Test
    void invalidNegativeThresholdsAreRejectedAndUnsampledParentsSuppressEntireStatementTrees() throws Exception {
        try (Collector collector = new Collector()) {
            for (String name : new String[] {"otelConnectionSpanDurationThreshold",
                    "otelStatementSpanDurationThreshold"}) {
                Properties options = collector.options(2);
                for (String value : new String[] {"-0.1", "-1E-9", "9223372036.854775808", "1E+100", "NaN",
                        "1E+2147483647"}) {
                    options.setProperty(name, value);
                    assertNull(provider().create(options), value);
                }
            }
            Properties defaults = collector.options(0);
            defaults.remove("otelLogLevel");
            tree(provider().create(defaults), 1, false, true, 100);
            PerformanceLogCallback callback = provider().create(collector.options(2));
            SpanContext parent = SpanContext.create("1234567890abcdef1234567890abcdef", "1234567890abcdef",
                    TraceFlags.getDefault(), TraceState.getDefault());
            try (Scope ignored = Span.wrap(parent).makeCurrent()) {
                tree(callback, 10, true, false, 100);
                tree(callback, 20, true, true, 100);
            }
            flush();
            assertTrue(collector.spans().isEmpty(), "Even failed statement roots must honor an unsampled parent");
            assertEquals(2, collector.counter("db.client.operation.count", "statement.execute"));
            assertEquals(1, collector.counter("db.client.operation.error.count", "statement.execute"));
        }
    }

    @Test
    void sampledAndInvalidParentsPreserveCompleteStatementHierarchy() throws Exception {
        for (boolean valid : new boolean[] {false, true}) {
            try (Collector collector = new Collector()) {
                PerformanceLogCallback callback = provider().create(collector.options(2));
                SpanContext parent = valid ? SpanContext.create("1234567890abcdef1234567890abcdef", "1234567890abcdef",
                        TraceFlags.getSampled(), TraceState.getDefault()) : SpanContext.getInvalid();
                try (Scope ignored = Span.wrap(parent).makeCurrent()) {
                    tree(callback, 1, true, true, 100);
                }
                flush();
                assertEquals(2, collector.spans().size());
                Wire root = collector.spans().stream().filter(s -> s.text(5).endsWith("execute")).findFirst().get();
                Wire child = collector.spans().stream().filter(s -> s.text(5).endsWith("first_response")).findFirst()
                        .get();
                assertArrayEquals(root.bytes(1), child.bytes(1));
                assertArrayEquals(root.bytes(2), child.bytes(4));
                if (valid) {
                    assertArrayEquals(hex(parent.getTraceId()), root.bytes(1));
                    assertArrayEquals(hex(parent.getSpanId()), root.bytes(4));
                } else {
                    assertEquals(0, root.bytes(4).length);
                }
            } finally {
                cleanup();
            }
        }
    }

    @Test
    void ownedSdkExportsVersionedScopesAndAllFourBoundedMetricsAtLevelZero() throws Exception {
        try (Collector collector = new Collector()) {
            PerformanceLogCallback callback = provider().create(collector.options(0));
            for (boolean statement : new boolean[] {false, true}) {
                PerformanceActivity activity = statement ? PerformanceActivity.STATEMENT_INVOCATION
                                                         : PerformanceActivity.CONNECTION;
                for (boolean failed : new boolean[] {false, true}) {
                    PerformanceLogEvent end = ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.END,
                            failed ? 2 : 1, 0, failed ? 2 : 1, activity, statement, 100000000,
                            failed ? new SQLException("SECRET") : null);
                    Map<String, Object> metadata = new java.util.LinkedHashMap<>(end.getAttributes());
                    metadata.put(statement ? "mssql.statement.attempt_count" : "mssql.connection.attempt_count", 3L);
                    metadata.put("mssql.retry.reason", statement ? "configurable_retry" : "redirect");
                    metadata.put("mssql.authentication.method", "sql_password");
                    metadata.put("db.query.text", "SECRET-query");
                    callback.publish(ConnectionEventFixture.withUserAttributes(end, metadata));
                }
            }
            flush();
            assertTrue(collector.spans().isEmpty());
            java.util.Set<String> names = new java.util.HashSet<>();
            for (Wire metric : collector.metrics()) {
                names.add(metric.text(1));
            }
            assertEquals(new java.util.HashSet<>(
                    java.util.Arrays.asList("db.client.operation.count", "db.client.operation.error.count",
                            "db.client.operation.retry.count", "db.client.operation.duration")),
                    names);
            for (String activity : new String[] {"connection.open", "statement.execute"}) {
                assertEquals(2, collector.counter("db.client.operation.count", activity));
                assertEquals(1, collector.counter("db.client.operation.error.count", activity));
                assertEquals(4, collector.counter("db.client.operation.retry.count", activity));
            }
            double[] expected = {0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1,
                    2.5, 5, 10, 30};
            for (Wire metric : collector.metrics()) {
                if (!"db.client.operation.duration".equals(metric.text(1))) {
                    continue;
                }
                List<Wire> points = metric.children(9).get(0).children(1);
                assertEquals(4, points.size(), "Both outcomes and root kinds retain separate histogram series");
                for (Wire point : points) {
                    assertArrayEquals(expected, point.doubles(7));
                    assertEquals(1, point.number(4));
                    assertEquals(0.1, Double.longBitsToDouble(point.number(5)), 0.000000001);
                    long[] buckets = point.longs(6);
                    assertEquals(18, buckets.length);
                    assertEquals(1, buckets[9], "The p95 input falls in the 100ms bucket, not a coarse default bucket");
                    assertEquals("1.0", point.attribute(9, "mssql.telemetry.schema.version"));
                    String kind = point.attribute(9, "mssql.operation.kind");
                    assertTrue("connection".equals(kind) || "statement".equals(kind));
                    assertEquals("connection".equals(kind) ? "sql_password" : "statement", point.attribute(9,
                            "connection".equals(kind) ? "mssql.connection.auth_method" : "mssql.statement.type"));
                    assertTrue(java.util.Arrays.asList("success", "failure")
                            .contains(point.attribute(9, "mssql.operation.outcome")));
                }
            }
            assertFalse(collector.body("/v1/metrics").contains("SECRET"));
            assertScopes(collector, "/v1/metrics");
        }
    }

    private static void assertScopes(Collector collector, String path) {
        String version = SQLServerDriver.class.getPackage().getImplementationVersion();
        if (version == null || version.isEmpty()) {
            SQLServerDriver driver = new SQLServerDriver();
            version = driver.getMajorVersion() + "." + driver.getMinorVersion();
        }
        int count = 0;
        for (Request request : collector.requests) {
            if (path.equals(request.path)) {
                for (Wire resource : new Wire(request.bytes).children(1)) {
                    for (Wire scope : resource.children(2)) {
                        Wire identity = scope.children(1).get(0);
                        assertEquals("com.microsoft.sqlserver.jdbc", identity.text(1));
                        assertEquals(version, identity.text(2));
                        count++;
                    }
                }
            }
        }
        assertTrue(count > 0);
    }

    @Test
    void retryMetricCountsOnlyAdditionalAttemptsOnRootsNotAttemptScopesOrDecisions() throws Exception {
        try (Collector collector = new Collector()) {
            PerformanceLogCallback callback = provider().create(collector.options(0));
            for (boolean statement : new boolean[] {false, true}) {
                long id = statement ? 10 : 1;
                PerformanceActivity root = statement ? PerformanceActivity.STATEMENT_INVOCATION
                                                     : PerformanceActivity.CONNECTION;
                PerformanceActivity attempt = statement ? PerformanceActivity.STATEMENT_ATTEMPT
                                                        : PerformanceActivity.CONNECTION_ATTEMPT;
                Map<String, Object> metadata = new java.util.LinkedHashMap<>();
                metadata.put(statement ? "mssql.statement.attempt_count" : "mssql.connection.attempt_count", 3L);
                metadata.put("mssql.retry.reason", "SECRET-unregistered-reason");
                callback.publish(ConnectionEventFixture.withUserAttributes(ConnectionEventFixture
                        .userEvent(PerformanceLogEvent.Type.END, id, 0, id, root, statement, 100, null), metadata));
                for (int i = 1; i <= 3; i++) {
                    callback.publish(ConnectionEventFixture.withUserAttributes(ConnectionEventFixture.userEvent(
                            PerformanceLogEvent.Type.END, id + i, id, id, attempt, statement, 10, null), metadata));
                }
                // A scheduled retry that never began another attempt is not a retry-count observation.
                metadata.put(statement ? "mssql.statement.attempt_count" : "mssql.connection.attempt_count", 1L);
                metadata.put(statement ? "mssql.statement.retry_count" : "mssql.connection.retry_count", 99L);
                callback.publish(ConnectionEventFixture.withUserAttributes(ConnectionEventFixture.userEvent(
                        PerformanceLogEvent.Type.END, id + 100, 0, id + 100, root, statement, 100, null), metadata));
            }
            flush();
            for (String activity : new String[] {"connection.open", "statement.execute"}) {
                assertEquals(2, collector.counter("db.client.operation.retry.count", activity));
            }
            for (String activity : new String[] {"connection.attempt", "statement.attempt"}) {
                assertEquals(3, collector.counter("db.client.operation.count", activity));
                assertEquals(0, collector.counter("db.client.operation.retry.count", activity));
            }
            for (Wire metric : collector.metrics()) {
                if ("db.client.operation.retry.count".equals(metric.text(1))) {
                    for (Wire point : metric.children(7).get(0).children(1)) {
                        assertEquals("not_applicable", point.attribute(7, "mssql.retry.reason"));
                    }
                }
            }
            assertFalse(collector.body("/v1/metrics").contains("SECRET"));
        }
    }

    @Test
    void sqlIsCapturedOnlyOnRootEndMaskedBeforeAdmissionAndNeverTrustedFromAttributes() throws Exception {
        try (Collector collector = new Collector()) {
            PerformanceLogCallback callback = provider().create(collector.options(2));
            Object owner = field(callback, "owner");
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            blockWorker(owner, entered, release);
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                long id = 0;
                for (String sql : new String[] {"SELECT N'SECRET-literal', 42 /* SECRET-comment */",
                        "SELECT 'SECRET-malformed", "SELECT /* SECRET-unclosed-comment",
                        String.join("", Collections.nCopies(3000, "SECRET")), null}) {
                    id++;
                    PerformanceLogEvent start = ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.START, id, 0,
                            id, PerformanceActivity.STATEMENT_INVOCATION, true, 0, null);
                    publishSql(callback, start.withMaskedQueryText("SECRET-injected-start"), "SELECT 'SECRET-start'");
                    PerformanceLogEvent child = ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.START,
                            id + 100, id, id, PerformanceActivity.STATEMENT_FIRST_SERVER_RESPONSE, true, 0, null);
                    publishSql(callback, child.withMaskedQueryText("SECRET-injected-child"), "SELECT 'SECRET-child'");
                    child = ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.END, id + 100, id, id,
                            PerformanceActivity.STATEMENT_FIRST_SERVER_RESPONSE, true, 100,
                            new SQLException("SECRET-error"));
                    publishSql(callback, child.withMaskedQueryText("SECRET-injected-child"), "SELECT 'SECRET-child'");
                    PerformanceLogEvent end = ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.END, id, 0, id,
                            PerformanceActivity.STATEMENT_INVOCATION, true, 100, new SQLException("SECRET-error"));
                    publishSql(callback, end.withMaskedQueryText("SECRET-injected-root"), sql);
                }
                for (Object queued : (java.util.Queue<?>) field(owner, "queue")) {
                    for (java.lang.reflect.Field capture : queued.getClass().getDeclaredFields()) {
                        capture.setAccessible(true);
                        Object value = capture.get(queued);
                        if (value instanceof PerformanceLogEvent) {
                            PerformanceLogEvent event = (PerformanceLogEvent) value;
                            assertNull(event.getException());
                            assertFalse(event.getAttributes().toString().contains("SECRET"),
                                    "Never enqueue caller SQL attributes");
                        } else if (value instanceof String) {
                            assertFalse(((String) value).contains("SECRET"));
                        }
                    }
                }
            } finally {
                release.countDown();
            }
            flush();
            List<Wire> spans = collector.spans();
            assertEquals(10, spans.size());
            assertEquals(1, spans.stream().filter(s -> s.attribute("db.query.text") != null).count());
            Wire captured = spans.stream().filter(s -> s.attribute("db.query.text") != null).findFirst().get();
            assertEquals("mssql.driver.statement.execute", captured.text(5));
            assertEquals("SELECT ?, ?", captured.attribute("db.query.text"));
            assertFalse(collector.body("/v1/traces").contains("SECRET"));
            assertFalse(collector.body("/v1/metrics").contains("db.query.text"));
            assertScopes(collector, "/v1/traces");
        }
    }

    @SuppressWarnings("unchecked")
    private static void publishSql(PerformanceLogCallback callback, PerformanceLogEvent event,
            String sql) throws Exception {
        java.lang.reflect.Field field = Class.forName("com.microsoft.sqlserver.jdbc.PerformanceLog")
                .getDeclaredField("currentUserSql");
        field.setAccessible(true);
        ThreadLocal<String> current = (ThreadLocal<String>) field.get(null);
        current.set(sql);
        try {
            callback.publish(event);
        } finally {
            current.remove();
        }
    }

    @Test
    void connectionAndStatementDiagnosticsReplayOnRootsWithOriginalTimestampsAndAllowlists() throws Exception {
        try (Collector collector = new Collector()) {
            PerformanceLogCallback callback = provider().create(collector.options(2));
            long start = 1700000000000000000L;
            for (boolean statement : new boolean[] {false, true}) {
                PerformanceActivity activity = statement ? PerformanceActivity.STATEMENT_INVOCATION
                                                         : PerformanceActivity.CONNECTION;
                long id = statement ? 2 : 1;
                callback.publish(ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.START, id, 0, id, activity,
                        statement, 0, null));
                Map<String, Object> metadata = new java.util.LinkedHashMap<>();
                metadata.put("mssql.error.retry_decision", "retry_scheduled");
                metadata.put("mssql.retry.delay", 0.5);
                metadata.put("mssql.timeout.kind", statement ? "query" : "login_budget");
                metadata.put("mssql.timeout.phase", statement ? "first_response" : "dns");
                metadata.put("password", "SECRET");
                List<Map<String, Object>> diagnostics = new ArrayList<>();
                String retry = statement ? "mssql.driver.statement.retry_decision"
                                         : "mssql.driver.connection.retry_decision";
                diagnostics.add(diagnostic(retry, start + 20, metadata));
                diagnostics.add(diagnostic("mssql.driver.timeout", start + 30, metadata));
                diagnostics.add(diagnostic("mssql.driver.SECRET", start + 40, metadata));
                diagnostics.add(diagnostic("mssql.driver.timeout", start + 101, metadata));
                PerformanceLogEvent end = ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.END, id, 0, id,
                        activity, statement, 100, null);
                callback.publish(withDiagnostics(end, diagnostics));
            }
            flush();
            assertEquals(2, collector.spans().size());
            for (Wire root : collector.spans()) {
                List<Wire> events = root.children(11);
                assertEquals(2, events.size());
                assertEquals(start + 20, events.get(0).number(1));
                assertEquals("retry_scheduled", events.get(0).attribute(3, "mssql.error.retry_decision"));
                assertEquals(start + 30, events.get(1).number(1));
                assertEquals("mssql.driver.timeout", events.get(1).text(2));
                assertEquals(root.text(5).contains("statement") ? "query" : "login_budget",
                        events.get(1).attribute(3, "mssql.timeout.kind"));
            }
            assertFalse(collector.body("/v1/traces").contains("SECRET"));
        }
    }

    private static Map<String, Object> diagnostic(String name, long timestamp, Map<String, Object> attributes) {
        Map<String, Object> diagnostic = new java.util.LinkedHashMap<>();
        diagnostic.put("name", name);
        diagnostic.put("timestamp", timestamp);
        diagnostic.put("attributes", attributes);
        return diagnostic;
    }

    private static PerformanceLogEvent withDiagnostics(PerformanceLogEvent event,
            List<Map<String, Object>> diagnostics) throws Exception {
        // The existing fixture overload predates statement IDs; preserve the real statement identity here.
        Class<?>[] types = {PerformanceLogEvent.Type.class, long.class, long.class, long.class, int.class, int.class,
                PerformanceActivity.class, long.class, long.class, long.class, Exception.class, String.class, Map.class,
                Map.class, List.class};
        java.lang.reflect.Constructor<PerformanceLogEvent> constructor = PerformanceLogEvent.class
                .getDeclaredConstructor(types);
        constructor.setAccessible(true);
        Object[] arguments = {event.getType(), event.getScopeId(), event.getParentScopeId(), event.getRootScopeId(),
                event.getConnectionId(), event.getStatementId(), event.getActivity(), event.getStartEpochNanos(),
                event.getEndEpochNanos(), event.getDurationNanos(), event.getException(), event.getFailurePhase(),
                event.getAttributes(), event.getErrorAttributes(), diagnostics};
        return constructor.newInstance(arguments);
    }

    @Test
    void queuedRootCapturesOnlySpanIdentityAndNoCallerContext() throws Exception {
        try (Collector collector = new Collector()) {
            PerformanceLogCallback callback = provider().create(collector.options(2));
            Object owner = field(callback, "owner");
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            blockWorker(owner, entered, release);
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                ContextKey<Object> secret = ContextKey.named("SECRET");
                SpanContext identity = SpanContext.create("1234567890abcdef1234567890abcdef", "1234567890abcdef",
                        TraceFlags.getSampled(), TraceState.getDefault());
                Context caller = Baggage.builder().put("SECRET", "SECRET").build().storeInContext(Context.root())
                        .with(secret, new Object()).with(Span.wrap(identity));
                try (Scope ignored = caller.makeCurrent()) {
                    callback.publish(ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.START, 1, 0, 1,
                            PerformanceActivity.CONNECTION, false, 0, null));
                }
                java.util.Queue<?> queue = (java.util.Queue<?>) field(owner, "queue");
                Object queued = queue.peek();
                assertNotNull(queued);
                for (java.lang.reflect.Field capture : queued.getClass().getDeclaredFields()) {
                    capture.setAccessible(true);
                    assertFalse(capture.get(queued) instanceof Context, "Do not retain caller Context/baggage");
                    if (capture.get(queued) instanceof PerformanceLogEvent) {
                        assertNull(((PerformanceLogEvent) capture.get(queued)).getException());
                    }
                }
            } finally {
                release.countDown();
            }
            flush();
            Map<?, ?> trees = (Map<?, ?>) field(owner, "trees");
            assertEquals(1, trees.size());
            assertInstanceOf(SpanContext.class, field(trees.values().iterator().next(), "parent"));
        }
    }

    @Test
    void incompleteOrOversizedTreesAreDroppedWholeButDoNotSuppressMetrics() throws Exception {
        try (Collector collector = new Collector()) {
            PerformanceLogCallback callback = provider().create(collector.options(2));
            for (int size : new int[] {1, 513}) {
                long root = size * 10000L;
                callback.publish(ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.START, root, 0, root,
                        PerformanceActivity.CONNECTION, false, 0, null));
                for (int i = 1; i <= size; i++) {
                    callback.publish(ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.START, root + i, root,
                            root, PerformanceActivity.DNS, false, 0, null));
                    if (size != 1) {
                        callback.publish(ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.END, root + i, root,
                                root, PerformanceActivity.DNS, false, 100, null));
                    }
                }
                callback.publish(ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.END, root, 0, root,
                        PerformanceActivity.CONNECTION, false, 100, null));
            }
            flush();
            assertTrue(collector.spans().isEmpty());
            assertEquals(2, collector.counter("db.client.operation.count", "connection.open"));
            assertEquals(513, collector.counter("db.client.operation.count", "connection.dns"));
        }
    }

    @Test
    void queueLossInvalidatesPendingAndQueuedTreesButMetricsAndFreshTreesSurvive() throws Exception {
        try (Collector collector = new Collector()) {
            PerformanceLogCallback callback = provider().create(collector.options(2));
            Object owner = field(callback, "owner");
            callback.publish(ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.START, 1, 0, 1,
                    PerformanceActivity.CONNECTION, false, 0, null));
            flush();
            CountDownLatch entered = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            blockWorker(owner, entered, release);
            try {
                assertTrue(entered.await(5, TimeUnit.SECONDS));
                tree(callback, 10, true, false, 100); // A queued complete tree predating the loss must also go.
                while (((AtomicInteger) field(owner, "queued")).get() < 8192) {
                    assertTrue(offer(owner, () -> {}));
                }
                callback.publish(ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.START, 2, 1, 1,
                        PerformanceActivity.DNS, false, 0, null)); // Rejected boundary.
                assertEquals(8192, ((AtomicInteger) field(owner, "queued")).get());
            } finally {
                release.countDown();
            }
            // A marker admitted after the backlog drains makes this deterministic without sleeps.
            Thread worker = (Thread) field(owner, "worker");
            long deadline = System.nanoTime() + WAIT.toNanos();
            while (((AtomicInteger) field(owner, "queued")).get() != 0 && System.nanoTime() < deadline) {
                Thread.yield();
            }
            assertTrue(worker.isAlive());
            callback.publish(ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.END, 1, 0, 1,
                    PerformanceActivity.CONNECTION, false, 100, null));
            flush();
            assertTrue(collector.body("/v1/traces").isEmpty(),
                    "Never silently export a partial tree after admission loss");
            assertTrue(collector.body("/v1/metrics").contains("db.client.operation.count"));
            tree(callback, 20, true, false, 100);
            flush();
            assertEquals(1, occurrences(collector.body("/v1/traces"), "mssql.driver.statement.execute"));
            UserEndpointTelemetryProvider.shutdown();
            worker.join(5000);
            assertFalse(worker.isAlive());
            assertFalse(offer(owner, () -> fail("Closed registry accepted work")));
            assertEquals(0, ((AtomicInteger) field(owner, "queued")).get());
        }
    }

    private static Object field(Object target, String name) throws Exception {
        java.lang.reflect.Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    @Test
    void exportBackpressureAdmitsOnlyWholeTrees() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        List<Integer> batches = new java.util.concurrent.CopyOnWriteArrayList<>();
        io.opentelemetry.sdk.trace.export.SpanExporter delegate = new io.opentelemetry.sdk.trace.export.SpanExporter() {
            @Override
            public io.opentelemetry.sdk.common.CompletableResultCode export(
                    java.util.Collection<io.opentelemetry.sdk.trace.data.SpanData> spans) {
                assertEquals("mssql-user-otel-export", Thread.currentThread().getName());
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                batches.add(spans.size());
                return io.opentelemetry.sdk.common.CompletableResultCode.ofSuccess();
            }

            @Override
            public io.opentelemetry.sdk.common.CompletableResultCode flush() {
                return io.opentelemetry.sdk.common.CompletableResultCode.ofSuccess();
            }

            @Override
            public io.opentelemetry.sdk.common.CompletableResultCode shutdown() {
                return io.opentelemetry.sdk.common.CompletableResultCode.ofSuccess();
            }
        };
        Class<?> type = Class.forName(UserEndpointTelemetryProvider.class.getName() + "$TreeExporter");
        java.lang.reflect.Constructor<?> constructor = type
                .getDeclaredConstructor(io.opentelemetry.sdk.trace.export.SpanExporter.class);
        constructor.setAccessible(true);
        Object exporter = constructor.newInstance(delegate);
        java.lang.reflect.Method offer = type.getDeclaredMethod("offer", List.class);
        java.lang.reflect.Method flush = type.getDeclaredMethod("flush", long.class);
        java.lang.reflect.Method close = type.getDeclaredMethod("close");
        offer.setAccessible(true);
        flush.setAccessible(true);
        close.setAccessible(true);
        try {
            offer.invoke(exporter, Collections.nCopies(512, null));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            for (int i = 0; i < 10; i++) {
                offer.invoke(exporter, Collections.nCopies(512, null));
            }
            assertEquals(3, ((java.util.concurrent.ThreadPoolExecutor) field(exporter, "worker")).getQueue().size());
            release.countDown();
            assertEquals(Boolean.TRUE, flush.invoke(exporter, System.nanoTime() + WAIT.toNanos()));
            assertEquals(java.util.Arrays.asList(512, 512, 512, 512), batches);
            assertEquals(2048, ((java.util.concurrent.Semaphore) field(exporter, "spans")).availablePermits());
        } finally {
            release.countDown();
            close.invoke(exporter);
        }
    }

    private static boolean offer(Object owner, Runnable task) throws Exception {
        java.lang.reflect.Method method = owner.getClass().getDeclaredMethod("offer", Runnable.class);
        method.setAccessible(true);
        return (Boolean) method.invoke(owner, task);
    }

    private static void blockWorker(Object owner, CountDownLatch entered, CountDownLatch release) throws Exception {
        assertTrue(offer(owner, () -> {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }

    private static byte[] hex(String value) {
        byte[] bytes = new byte[value.length() / 2];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = (byte) Integer.parseInt(value.substring(i * 2, i * 2 + 2), 16);
        }
        return bytes;
    }

    // Minimal OTLP protobuf reader: assertions inspect actual spans/events/status rather than binary substring counts.
    static final class Wire {
        final Map<Integer, List<Object>> fields = new java.util.LinkedHashMap<>();

        Wire(byte[] bytes) {
            java.nio.ByteBuffer input = java.nio.ByteBuffer.wrap(bytes).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            while (input.hasRemaining()) {
                long tag = varint(input);
                int kind = (int) (tag & 7);
                Object value;
                switch (kind) {
                    case 0:
                        value = varint(input);
                        break;
                    case 1:
                        value = input.getLong();
                        break;
                    case 2:
                        byte[] data = new byte[(int) varint(input)];
                        input.get(data);
                        value = data;
                        break;
                    case 5:
                        value = (long) input.getInt();
                        break;
                    default:
                        throw new AssertionError("Unexpected protobuf wire type " + kind);
                }
                fields.computeIfAbsent((int) (tag >>> 3), key -> new ArrayList<>()).add(value);
            }
        }

        static long varint(java.nio.ByteBuffer input) {
            long value = 0;
            for (int shift = 0; shift < 64; shift += 7) {
                int next = input.get() & 255;
                value |= (long) (next & 127) << shift;
                if ((next & 128) == 0) {
                    return value;
                }
            }
            throw new AssertionError("Invalid protobuf varint");
        }

        List<Wire> children(int tag) {
            List<Wire> values = new ArrayList<>();
            for (Object value : fields.getOrDefault(tag, Collections.emptyList())) {
                values.add(new Wire((byte[]) value));
            }
            return values;
        }

        byte[] bytes(int tag) {
            return (byte[]) fields.getOrDefault(tag, Collections.singletonList(new byte[0])).get(0);
        }

        String text(int tag) {
            return new String(bytes(tag), StandardCharsets.UTF_8);
        }

        long number(int tag) {
            return (Long) fields.getOrDefault(tag, Collections.singletonList(0L)).get(0);
        }

        double[] doubles(int tag) {
            List<Object> encoded = fields.getOrDefault(tag, Collections.emptyList());
            if (encoded.size() == 1 && encoded.get(0) instanceof byte[]) {
                java.nio.ByteBuffer packed = java.nio.ByteBuffer.wrap((byte[]) encoded.get(0))
                        .order(java.nio.ByteOrder.LITTLE_ENDIAN);
                double[] values = new double[packed.remaining() / 8];
                for (int i = 0; i < values.length; i++) {
                    values[i] = packed.getDouble();
                }
                return values;
            }
            double[] values = new double[encoded.size()];
            for (int i = 0; i < values.length; i++) {
                values[i] = Double.longBitsToDouble((Long) encoded.get(i));
            }
            return values;
        }

        long[] longs(int tag) {
            List<Object> encoded = fields.getOrDefault(tag, Collections.emptyList());
            if (encoded.size() == 1 && encoded.get(0) instanceof byte[]) {
                // OTLP HistogramDataPoint.bucket_counts is packed fixed64, not packed varint.
                java.nio.ByteBuffer packed = java.nio.ByteBuffer.wrap((byte[]) encoded.get(0))
                        .order(java.nio.ByteOrder.LITTLE_ENDIAN);
                long[] result = new long[packed.remaining() / 8];
                for (int i = 0; i < result.length; i++) {
                    result[i] = packed.getLong();
                }
                return result;
            }
            long[] result = new long[encoded.size()];
            for (int i = 0; i < result.length; i++) {
                result[i] = (Long) encoded.get(i);
            }
            return result;
        }

        String attribute(String name) {
            return attribute(9, name);
        }

        String attribute(int tag, String name) {
            for (Wire attribute : children(tag)) {
                if (name.equals(attribute.text(1))) {
                    return attribute.children(2).get(0).text(1);
                }
            }
            return null;
        }
    }

    private static int occurrences(String text, String value) {
        return (text.length() - text.replace(value, "").length()) / value.length();
    }

    private static boolean contains(byte[] body, byte[] sequence) {
        outer : for (int i = 0; i <= body.length - sequence.length; i++) {
            for (int j = 0; j < sequence.length; j++) {
                if (body[i + j] != sequence[j]) {
                    continue outer;
                }
            }
            return true;
        }
        return false;
    }

    public static final class ControlledTokenCallback implements SQLServerAccessTokenCallback {
        static final AtomicInteger calls = new AtomicInteger();
        static final AtomicLong expires = new AtomicLong();

        @Override
        public SqlAuthenticationToken getAccessToken(String endpoint, String authority) {
            calls.incrementAndGet();
            return new SqlAuthenticationToken("SECRET", expires.get());
        }
    }

    public static final class IsolatedTokenCallback implements SQLServerAccessTokenCallback {
        static final AtomicInteger instances = new AtomicInteger();
        final int instance = instances.incrementAndGet();

        public IsolatedTokenCallback() {
            assertTrue(Thread.currentThread().getName().startsWith("BatchSpanProcessor_")
                    || Thread.currentThread().getName().startsWith("mssql-user-otel-")
                    || Thread.currentThread().getName().startsWith("PeriodicMetricReader"));
        }

        @Override
        public SqlAuthenticationToken getAccessToken(String endpoint, String authority) {
            return new SqlAuthenticationToken("SECRET-instance-" + instance, System.currentTimeMillis() + 60000);
        }
    }

    public static final class BlockingTokenCallback implements SQLServerAccessTokenCallback {
        static java.util.concurrent.CountDownLatch entered;
        static java.util.concurrent.CountDownLatch release;

        @Override
        public SqlAuthenticationToken getAccessToken(String endpoint, String authority) {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException("SECRET");
        }
    }

    static void tree(PerformanceLogCallback callback, long id, boolean statement, boolean failed,
            long duration) throws Exception {
        PerformanceActivity root = statement ? PerformanceActivity.STATEMENT_INVOCATION
                                             : PerformanceActivity.CONNECTION;
        callback.publish(
                ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.START, id, 0, id, root, statement, 0, null));
        callback.publish(ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.START, id + 1000, id, id,
                statement ? PerformanceActivity.STATEMENT_FIRST_SERVER_RESPONSE : PerformanceActivity.DNS, statement, 0,
                null));
        callback.publish(ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.END, id + 1000, id, id,
                statement ? PerformanceActivity.STATEMENT_FIRST_SERVER_RESPONSE : PerformanceActivity.DNS, statement,
                duration, failed ? new SQLException("SECRET") : null));
        callback.publish(ConnectionEventFixture.userEvent(PerformanceLogEvent.Type.END, id, 0, id, root, statement,
                duration, failed ? new SQLException("SECRET") : null));
    }

    public static final class TokenCallback implements SQLServerAccessTokenCallback {
        static final AtomicInteger calls = new AtomicInteger();

        @Override
        public SqlAuthenticationToken getAccessToken(String endpoint, String authority) {
            assertNotEquals("main", Thread.currentThread().getName());
            assertTrue(endpoint.startsWith("http://127.0.0.1:"));
            assertEquals("", authority);
            return new SqlAuthenticationToken("SECRET-" + calls.incrementAndGet(), System.currentTimeMillis() + 100);
        }
    }

    public static final class PerMessageTokenCallback implements SQLServerAccessTokenCallback {
        static final AtomicInteger calls = new AtomicInteger();
        static volatile boolean fail;

        @Override
        public SqlAuthenticationToken getAccessToken(String endpoint, String authority) {
            assertTrue(Thread.currentThread().getName().startsWith("mssql-user-otel-")
                    || Thread.currentThread().getName().startsWith("PeriodicMetricReader"));
            assertEquals("", authority);
            int call = calls.incrementAndGet();
            if (fail) {
                throw new IllegalStateException("SECRET");
            }
            return new SqlAuthenticationToken("SECRET-message-" + call, System.currentTimeMillis() + 3600000);
        }
    }

    public static final class InvalidTokenCallback implements SQLServerAccessTokenCallback {
        static String mode;

        @Override
        public SqlAuthenticationToken getAccessToken(String endpoint, String authority) {
            switch (mode) {
                case "null":
                    return null;
                case "expired":
                    return new SqlAuthenticationToken("SECRET", 0);
                case "empty":
                    return new SqlAuthenticationToken("", Long.MAX_VALUE);
                case "injection":
                    return new SqlAuthenticationToken("SECRET\r\nInjected: value", Long.MAX_VALUE);
                case "throw":
                    throw new IllegalStateException("SECRET");
                default:
                    return new SqlAuthenticationToken("SECRET", Long.MAX_VALUE);
            }
        }
    }

    static final class Request {
        String path;
        String authorization;
        int authorizationCount;
        String body;
        byte[] bytes;
    }

    static final class Collector implements AutoCloseable {
        final BlockingQueue<Request> requests = new LinkedBlockingQueue<>();
        final HttpServer server;

        Collector() throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                try {
                    Request request = new Request();
                    request.path = exchange.getRequestURI().getPath();
                    request.authorization = exchange.getRequestHeaders().getFirst("Authorization");
                    request.authorizationCount = exchange.getRequestHeaders()
                            .getOrDefault("Authorization", Collections.emptyList()).size();
                    ByteArrayOutputStream body = new ByteArrayOutputStream();
                    byte[] buffer = new byte[4096];
                    int read;
                    while ((read = exchange.getRequestBody().read(buffer)) != -1) {
                        body.write(buffer, 0, read);
                    }
                    request.bytes = body.toByteArray();
                    request.body = new String(request.bytes, StandardCharsets.UTF_8);
                    requests.add(request);
                    exchange.getResponseHeaders().set("Content-Type", "application/x-protobuf");
                    exchange.sendResponseHeaders(200, -1);
                } finally {
                    exchange.close();
                }
            });
            server.start();
        }

        Properties options(int level) {
            Properties options = new Properties();
            options.setProperty("otelEndpoint", "http://127.0.0.1:" + server.getAddress().getPort());
            options.setProperty("otelLogLevel", Integer.toString(level));
            return options;
        }

        String body(String path) {
            StringBuilder body = new StringBuilder();
            for (Request request : requests) {
                if (path.equals(request.path)) {
                    body.append(request.body);
                }
            }
            return body.toString();
        }

        List<Wire> spans() {
            List<Wire> spans = new ArrayList<>();
            for (Request request : requests) {
                if ("/v1/traces".equals(request.path)) {
                    for (Wire resource : new Wire(request.bytes).children(1)) {
                        for (Wire scope : resource.children(2)) {
                            spans.addAll(scope.children(2));
                        }
                    }
                }
            }
            return spans;
        }

        long counter(String name, String activity) {
            long result = 0;
            for (Request request : requests) {
                if (!"/v1/metrics".equals(request.path)) {
                    continue;
                }
                long current = 0;
                for (Wire resource : new Wire(request.bytes).children(1)) {
                    for (Wire scope : resource.children(2)) {
                        for (Wire metric : scope.children(2)) {
                            if (!name.equals(metric.text(1))) {
                                continue;
                            }
                            for (Wire sum : metric.children(7)) {
                                for (Wire point : sum.children(1)) {
                                    if (activity.equals(point.attribute(7, "mssql.performance.activity"))) {
                                        current += point.number(6);
                                    }
                                }
                            }
                        }
                    }
                }
                result = current; // Cumulative export: sum outcomes within the newest HTTP snapshot, not across flushes.
            }
            return result;
        }

        List<Wire> metrics() {
            List<Wire> metrics = new ArrayList<>();
            for (Request request : requests) {
                if ("/v1/metrics".equals(request.path)) {
                    for (Wire resource : new Wire(request.bytes).children(1)) {
                        for (Wire scope : resource.children(2)) {
                            metrics.addAll(scope.children(2));
                        }
                    }
                }
            }
            return metrics;
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
