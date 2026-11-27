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
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.ConnectionEventFixture;
import com.microsoft.sqlserver.jdbc.PerformanceActivity;
import com.microsoft.sqlserver.jdbc.PerformanceLogEvent;
import com.microsoft.sqlserver.jdbc.SQLServerAccessTokenCallback;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.SqlAuthenticationToken;
import com.sun.net.httpserver.HttpServer;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;


class OtlpConnectionTelemetryTest {
    private static final Duration WAIT = Duration.ofSeconds(10);

    @Test
    void closeDrainsAdapterThenFlushesOwnedTransportAndRejectsInvalidWaits() throws Exception {
        try (Collector collector = new Collector()) {
            OtlpConnectionTelemetry telemetry = OtlpConnectionTelemetry.create(collector.properties());
            try {
                assertThrows(IllegalArgumentException.class, () -> telemetry.forceFlush(Duration.ofSeconds(-1)));
                assertThrows(IllegalArgumentException.class,
                        () -> telemetry.forceFlush(Duration.ofSeconds(Long.MAX_VALUE)));
                assertTrue(telemetry.awaitIdle(Duration.ZERO));
                publishFailure(telemetry, 400);
            } finally {
                telemetry.close();
            }
            assertNotNull(collector.requests.poll(10, TimeUnit.SECONDS));
            assertFalse(telemetry.forceFlush(WAIT));
            assertTrue(telemetry.awaitIdle(WAIT));
        }
    }

    @Test
    void realFailedConnectionExportsOnlyTracesWithHeadersAndMinimalResource() throws Exception {
        try (Collector collector = new Collector()) {
            Properties properties = collector.properties();
            properties.setProperty("otelServiceName", "test-connection-service");
            properties.setProperty("otelBearerToken", "HEADER-ONLY-TOKEN");
            properties.setProperty("otelArmResourceId", "/HEADER-ONLY-ARM");
            properties.setProperty("otelDiscoveredArmRegion", "NOT-A-RESOURCE-ATTRIBUTE");
            properties.setProperty("otelHeaders", "X-Test=custom");
            InMemorySpanExporter applicationExporter = InMemorySpanExporter.create();
            GlobalOpenTelemetry.resetForTest();
            try (SdkTracerProvider applicationProvider = SdkTracerProvider.builder()
                    .addSpanProcessor(SimpleSpanProcessor.create(applicationExporter)).build();
                    OtlpConnectionTelemetry telemetry = OtlpConnectionTelemetry.create(properties);
                    OtlpConnectionTelemetry independent = OtlpConnectionTelemetry.create(collector.properties())) {
                GlobalOpenTelemetry.set(OpenTelemetrySdk.builder().setTracerProvider(applicationProvider).build());
                OpenTelemetry global = GlobalOpenTelemetry.get();
                try (OtlpConnectionTelemetry afterGlobal = OtlpConnectionTelemetry.create(collector.properties())) {
                    assertNotSame(telemetry.getCallback(), afterGlobal.getCallback());
                    assertSame(global, GlobalOpenTelemetry.get());
                }
                assertSame(global, GlobalOpenTelemetry.get());
                assertNotSame(telemetry.getCallback(), independent.getCallback());
                successAndLegacyEvents(telemetry.getCallback());
                assertTrue(telemetry.awaitIdle(WAIT));
                assertTrue(telemetry.forceFlush(WAIT));
                assertTrue(collector.requests.isEmpty());
                SQLServerDriver.registerPerformanceLogCallback(telemetry.getCallback());
                try {
                    failConnection();
                } finally {
                    SQLServerDriver.unregisterPerformanceLogCallback();
                }
                assertTrue(telemetry.awaitIdle(WAIT));
                assertTrue(telemetry.forceFlush(WAIT));
                Request request = collector.requests.poll(10, TimeUnit.SECONDS);
                assertNotNull(request);
                assertEquals("/v1/traces", request.path);
                assertEquals("Bearer HEADER-ONLY-TOKEN", request.authorization);
                assertEquals("/HEADER-ONLY-ARM", request.arm);
                assertEquals("custom", request.custom);
                assertEquals("application/x-protobuf", request.contentType);
                assertTrue(request.body.contains("mssql.driver.connection.open"));
                assertTrue(request.body.contains("test-connection-service"));
                for (String excluded : new String[] {"HEADER-ONLY", "NOT-A-RESOURCE", "telemetry.sdk", "host.name",
                        "db.query.text", "successful-open", "connection.failures", "v1/metrics"}) {
                    assertFalse(request.body.contains(excluded), excluded);
                }
                for (Request remaining : collector.requests) {
                    assertEquals("/v1/traces", remaining.path);
                }
                assertSame(global, GlobalOpenTelemetry.get());
                global.getTracer("application").spanBuilder("still-application-owned").startSpan().end();
                assertEquals(1, applicationExporter.getFinishedSpanItems().size());
            } finally {
                GlobalOpenTelemetry.resetForTest();
            }
        }
    }

    @Test
    void supplierFailureSendsNothingAndExporterWorkerSurvives() throws Exception {
        WorkerCallback.fail = true;
        WorkerCallback.thread = null;
        try (Collector collector = new Collector()) {
            Properties properties = collector.properties();
            properties.setProperty("otelAccessTokenCallbackClass", WorkerCallback.class.getName());
            properties.setProperty("otelTokenScope", "actual-scope");
            properties.setProperty("otelTokenAuthority", "actual-authority");
            properties.setProperty("otelBearerToken", "NEVER-FALL-BACK");
            try (OtlpConnectionTelemetry telemetry = OtlpConnectionTelemetry.create(properties)) {
                assertNull(WorkerCallback.thread);
                publishFailure(telemetry, 100);
                assertTrue(telemetry.awaitIdle(WAIT));
                telemetry.forceFlush(WAIT);
                assertNotNull(WorkerCallback.thread);
                assertNotSame(Thread.currentThread(), WorkerCallback.thread);
                assertTrue(WorkerCallback.thread.getName().contains("BatchSpanProcessor"));
                assertTrue(collector.requests.isEmpty());
                WorkerCallback.fail = false;
                // Unit tests use a fake clock for exact backoff boundaries; this exercises the real transport clock.
                Thread.sleep(1100);
                publishFailure(telemetry, 200);
                assertTrue(telemetry.awaitIdle(WAIT));
                telemetry.forceFlush(WAIT);
                Request request = collector.requests.poll(10, TimeUnit.SECONDS);
                assertNotNull(request);
                assertEquals("Bearer LIVE-TOKEN", request.authorization);
            }
        } finally {
            WorkerCallback.fail = false;
        }
    }

    @Test
    void flushAndCloseRemainFiniteWithBlockedTokenProvider() throws Exception {
        BlockingCallback.entered = new CountDownLatch(1);
        BlockingCallback.release = new CountDownLatch(1);
        try (Collector collector = new Collector()) {
            Properties properties = collector.properties();
            properties.setProperty("otelAccessTokenCallbackClass", BlockingCallback.class.getName());
            properties.setProperty("otelTokenScope", "scope");
            properties.setProperty("otelTokenAuthority", "authority");
            OtlpConnectionTelemetry telemetry = OtlpConnectionTelemetry.create(properties);
            try {
                publishFailure(telemetry, 300);
                assertTrue(telemetry.awaitIdle(WAIT));
                assertTrue(BlockingCallback.entered.await(10, TimeUnit.SECONDS));
                assertTimeout(Duration.ofSeconds(1), () -> assertFalse(telemetry.forceFlush(Duration.ofMillis(20))));
                assertTimeout(Duration.ofSeconds(16), telemetry::close);
                assertTimeout(Duration.ofSeconds(1), telemetry::close);
            } finally {
                BlockingCallback.release.countDown();
                telemetry.close();
            }
        }
    }

    private static void failConnection() {
        Properties properties = new Properties();
        properties.setProperty("portNumber", "invalid");
        assertThrows(SQLException.class, () -> new SQLServerDriver().connect("jdbc:sqlserver://", properties));
    }

    private static void publishFailure(OtlpConnectionTelemetry telemetry, long id) {
        PerformanceLogEvent start = ConnectionEventFixture.event(PerformanceLogEvent.Type.START, id, 0, id,
                PerformanceActivity.CONNECTION, 1700000000000000000L, 0, null, null, false, Collections.emptyMap());
        PerformanceLogEvent end = ConnectionEventFixture.event(PerformanceLogEvent.Type.END, id, 0, id,
                PerformanceActivity.CONNECTION, 1700000000000000000L, 10, new SQLException("SECRET"), "configuration",
                true, Collections.emptyMap());
        telemetry.getCallback().publish(start);
        telemetry.getCallback().publish(end);
    }

    private static void successAndLegacyEvents(OpenTelemetryConnectionCallback callback) {
        callback.publish(ConnectionEventFixture.event(PerformanceLogEvent.Type.START, 1, 0, 1,
                PerformanceActivity.CONNECTION, 1700000000000000000L, 0, null, null, false, Collections.emptyMap()));
        callback.publish(ConnectionEventFixture.event(PerformanceLogEvent.Type.END, 1, 0, 1,
                PerformanceActivity.CONNECTION, 1700000000000000000L, 10, null, null, false, Collections.emptyMap()));
        callback.publish(PerformanceActivity.CONNECTION, 1, 10L, new SQLException("SECRET"));
        callback.publish(PerformanceActivity.STATEMENT_EXECUTE, 1, 2, 10L, new SQLException("SECRET SQL"));
    }

    public static final class WorkerCallback implements SQLServerAccessTokenCallback {
        static volatile boolean fail;
        static volatile Thread thread;

        @Override
        public SqlAuthenticationToken getAccessToken(String scope, String authority) {
            thread = Thread.currentThread();
            if (fail) {
                throw new IllegalStateException("SECRET callback failure");
            }
            if (!"actual-scope".equals(scope) || !"actual-authority".equals(authority)) {
                throw new IllegalStateException("Unexpected token parameters");
            }
            return new SqlAuthenticationToken("LIVE-TOKEN", System.currentTimeMillis() + 60_000);
        }
    }

    public static final class BlockingCallback implements SQLServerAccessTokenCallback {
        static CountDownLatch entered;
        static CountDownLatch release;

        @Override
        public SqlAuthenticationToken getAccessToken(String scope, String authority) {
            entered.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            return new SqlAuthenticationToken("LATE", System.currentTimeMillis() + 60_000);
        }
    }

    private static final class Request {
        String path;
        String authorization;
        String arm;
        String custom;
        String contentType;
        String body;
    }

    private static final class Collector implements AutoCloseable {
        final BlockingQueue<Request> requests = new LinkedBlockingQueue<>();
        final HttpServer server;

        Collector() throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                try {
                    Request request = new Request();
                    request.path = exchange.getRequestURI().getPath();
                    request.authorization = exchange.getRequestHeaders().getFirst("Authorization");
                    request.arm = exchange.getRequestHeaders().getFirst("x-ms-arm-resource-id");
                    request.custom = exchange.getRequestHeaders().getFirst("x-test");
                    request.contentType = exchange.getRequestHeaders().getFirst("Content-Type");
                    ByteArrayOutputStream body = new ByteArrayOutputStream();
                    byte[] buffer = new byte[4096];
                    int read;
                    while ((read = exchange.getRequestBody().read(buffer)) != -1) {
                        body.write(buffer, 0, read);
                    }
                    request.body = new String(body.toByteArray(), StandardCharsets.UTF_8);
                    requests.add(request);
                    exchange.getResponseHeaders().set("Content-Type", "application/x-protobuf");
                    exchange.sendResponseHeaders(200, -1);
                } finally {
                    exchange.close();
                }
            });
            server.start();
        }

        Properties properties() {
            Properties properties = OtlpConfigurationTest
                    .properties("http://127.0.0.1:" + server.getAddress().getPort());
            properties.setProperty("otelAllowInsecureLocalEndpoint", "true");
            return properties;
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
