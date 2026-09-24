/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import static org.junit.jupiter.api.Assertions.*;
import static com.microsoft.sqlserver.jdbc.PerformanceLogEvent.Type.*;

import java.net.UnknownHostException;
import java.net.SocketTimeoutException;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.auth.login.LoginException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.ConnectionEventFixture;
import com.microsoft.sqlserver.jdbc.PerformanceActivity;
import com.microsoft.sqlserver.jdbc.PerformanceLogEvent;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.trace.ReadWriteSpan;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;


class OpenTelemetryConnectionCallbackTest {
    private static final long EPOCH = 1700000000000000000L;
    private static final Duration WAIT = Duration.ofSeconds(10);
    private final InMemorySpanExporter exporter = InMemorySpanExporter.create();
    private final InMemoryMetricReader reader = InMemoryMetricReader.create();
    private OpenTelemetrySdk sdk;
    private OpenTelemetryConnectionCallback adapter;

    private OpenTelemetryConnectionCallback.Builder setup(Sampler sampler, SpanProcessor processor) {
        io.opentelemetry.sdk.trace.SdkTracerProviderBuilder traces = SdkTracerProvider.builder().setSampler(sampler);
        if (processor != null) {
            traces.addSpanProcessor(processor);
        }
        sdk = OpenTelemetrySdk.builder()
                .setTracerProvider(traces.addSpanProcessor(SimpleSpanProcessor.create(exporter)).build())
                .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(reader).build()).build();
        return OpenTelemetryConnectionCallback.builder(sdk);
    }

    @AfterEach
    void cleanup() {
        SQLServerDriver.unregisterPerformanceLogCallback();
        if (adapter != null) {
            adapter.close();
        }
        if (sdk != null) {
            sdk.close();
        }
    }

    private PerformanceLogEvent event(PerformanceLogEvent.Type type, long id, long parent, long root,
            PerformanceActivity activity, long offset, long duration, Exception failure, String phase, boolean origin) {
        Map<String, Object> attrs = new HashMap<>();
        // Deliberately try to leak identity onto every span; adapter enforces placement.
        attrs.put("mssql.connection.guid", "11111111-1111-1111-1111-111111111111");
        attrs.put("mssql.connection.client_connection_id", "22222222-2222-2222-2222-222222222222");
        attrs.put("mssql.telemetry.schema.version", "1.0");
        attrs.put("server.address", "SECRET.example");
        attrs.put("db.query.text", "SECRET SQL");
        attrs.put("mssql.driver.user_agent.original", "SECRET user agent");
        return ConnectionEventFixture.event(type, id, parent, root, activity, EPOCH + offset, duration, failure, phase,
                origin, attrs);
    }

    private void rootStart(long root) {
        adapter.publish(event(START, root, 0, root, PerformanceActivity.CONNECTION, 0, 0, null, null, false));
    }

    private void rootEnd(long root, Exception failure) {
        adapter.publish(event(END, root, 0, root, PerformanceActivity.CONNECTION, 0, 8000001, failure, "dns", true));
    }

    private void tree(long root, Exception failure, boolean login, boolean success) {
        rootStart(root);
        PerformanceActivity[] activities = login ? new PerformanceActivity[] {
                PerformanceActivity.CONNECTION_CONFIGURATION, PerformanceActivity.CONNECTION_ATTEMPT,
                PerformanceActivity.DNS, PerformanceActivity.SOCKET_CONNECT, PerformanceActivity.PRELOGIN,
                PerformanceActivity.TLS, PerformanceActivity.LOGIN_EXCHANGE}
                                                 : new PerformanceActivity[] {
                                                         PerformanceActivity.CONNECTION_CONFIGURATION,
                                                         PerformanceActivity.CONNECTION_ATTEMPT,
                                                         PerformanceActivity.DNS};
        for (int i = 0; i < activities.length; i++) {
            adapter.publish(event(START, root + i + 1, i < 2 ? root : root + 2, root, activities[i], (i + 1) * 100, 0,
                    null, null, false));
        }
        String phase = login ? "login" : "dns";
        for (int i = activities.length - 1; i >= 0; i--) {
            boolean failed = i == activities.length - 1 || i == 1;
            adapter.publish(event(END, root + i + 1, i < 2 ? root : root + 2, root, activities[i], (i + 1) * 100,
                    i == 1 ? 6000001 : 1001, failed ? failure : null, phase, i == activities.length - 1));
        }
        adapter.publish(event(END, root, 0, root, PerformanceActivity.CONNECTION, 0, 8000001, success ? null : failure,
                phase, false));
    }

    private List<SpanData> spans() throws Exception {
        assertTrue(adapter.awaitIdle(WAIT));
        return exporter.getFinishedSpanItems();
    }

    private void drainIngestion() throws Exception {
        assertTrue(adapter.awaitIngestion(WAIT));
    }

    private SpanData named(List<SpanData> spans, String suffix) {
        return spans.stream().filter(s -> s.getName().equals("mssql.driver.connection." + suffix)).findFirst().get();
    }

    @Test
    void publisherReturnsWhileIngestionClockIsBlocked() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        java.util.concurrent.atomic.AtomicBoolean first = new java.util.concurrent.atomic.AtomicBoolean(true);
        OpenTelemetryConnectionCallback.Builder builder = setup(Sampler.alwaysOn(), null);
        builder.nanoClock = () -> {
            if (!Thread.currentThread().getName().endsWith("-expiry") && first.getAndSet(false)) {
                entered.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            return System.nanoTime();
        };
        adapter = builder.build();
        ExecutorService publishers = Executors.newFixedThreadPool(2);
        try {
            Future<?> start = publishers.submit(() -> rootStart(1));
            assertTrue(entered.await(10, TimeUnit.SECONDS));
            Future<?> end = publishers.submit(() -> rootEnd(1, new UnknownHostException()));
            start.get(10, TimeUnit.SECONDS);
            end.get(10, TimeUnit.SECONDS);
            assertFalse(adapter.awaitIdle(Duration.ZERO));
            assertTrue(exporter.getFinishedSpanItems().isEmpty());
        } finally {
            release.countDown();
            publishers.shutdownNow();
        }
        assertEquals(1, spans().size());
    }

    @Test
    void queuedProjectionRetainsOnlyFailureFlagAndOriginalSpanContext() throws Exception {
        BlockingClock clock = new BlockingClock();
        io.opentelemetry.context.ContextKey<Object> key = io.opentelemetry.context.ContextKey.named("SECRET");
        SpanProcessor observer = new BlockingProcessor() {
            @Override
            public void onStart(Context parent, ReadWriteSpan span) {
                if (span.getName().startsWith("mssql.driver")) {
                    assertNull(parent.get(key));
                    assertTrue(io.opentelemetry.api.baggage.Baggage.fromContext(parent).isEmpty());
                }
            }
        };
        OpenTelemetryConnectionCallback.Builder builder = setup(Sampler.alwaysOn(), observer);
        builder.nanoClock = clock;
        adapter = builder.build();
        Span parent = sdk.getTracer("app").spanBuilder("app-parent").startSpan();
        Exception hostile = new SQLException("SECRET") {
            private static final long serialVersionUID = 1L;

            @Override
            public synchronized Throwable getCause() {
                throw new AssertionError("Producer must not traverse causes");
            }
        };
        PerformanceLogEvent original = ConnectionEventFixture.withException(ConnectionEventFixture.withoutMetadata(
                event(END, 1, 0, 1, PerformanceActivity.CONNECTION, 0, 100, null, null, false)), hostile);
        ExecutorService publisher = Executors.newSingleThreadExecutor();
        clock.arm();
        try {
            Context caller = io.opentelemetry.api.baggage.Baggage.builder().put("SECRET", "SECRET").build()
                    .storeInContext(Context.root().with(parent).with(key, new Object()));
            try (Scope ignored = caller.makeCurrent()) {
                rootStart(1);
            }
            assertTrue(clock.entered.await(10, TimeUnit.SECONDS));
            publisher.submit(() -> adapter.publish(original)).get(10, TimeUnit.SECONDS);
            Object envelope = ((java.util.Queue<?>) field(adapter, "ingress")).peek();
            assertNotNull(envelope);
            PerformanceLogEvent projected = (PerformanceLogEvent) field(envelope, "event");
            assertNotSame(original, projected);
            assertNull(projected.getException());
            assertTrue(projected.hasException());
            assertSame(hostile, original.getException());
            assertSame(original.getAttributes(), projected.getAttributes());
            assertSame(original.getDiagnosticEvents(), projected.getDiagnosticEvents());
            for (java.lang.reflect.Field member : envelope.getClass().getDeclaredFields()) {
                assertTrue(member.getType() == PerformanceLogEvent.class
                        || member.getType() == io.opentelemetry.api.trace.SpanContext.class
                        || member.getType() == long.class);
            }
        } finally {
            clock.release.countDown();
            publisher.shutdownNow();
        }
        SpanData root = named(spans(), "open");
        assertEquals(parent.getSpanContext().getSpanId(), root.getParentSpanId());
        assertEquals(parent.getSpanContext().getTraceId(), root.getTraceId());
        assertEquals(StatusCode.ERROR, root.getStatus().getStatusCode());
        parent.end();
    }

    @Test
    void eventOverflowInvalidatesPartialTreesAndDoesNotCountLostRoots() throws Exception {
        BlockingClock clock = new BlockingClock();
        OpenTelemetryConnectionCallback.Builder builder = setup(Sampler.alwaysOn(), null).eventQueueCapacity(1)
                .metricsEnabled(true);
        builder.nanoClock = clock;
        adapter = builder.build();
        rootStart(1);
        drainIngestion();
        clock.arm();
        try {
            adapter.publish(event(START, 2, 1, 1, PerformanceActivity.DNS, 10, 0, null, null, false));
            assertTrue(clock.entered.await(10, TimeUnit.SECONDS));
            adapter.publish(
                    event(END, 2, 1, 1, PerformanceActivity.DNS, 10, 20, new UnknownHostException(), "dns", true));
            rootEnd(1, new SocketTimeoutException()); // dropped END
            rootStart(20); // dropped START
            assertEquals(1, ((java.util.Queue<?>) field(adapter, "ingress")).size());
            assertEquals(2, adapter.droppedEventCount());
            assertFalse(adapter.awaitIdle(Duration.ZERO));
        } finally {
            clock.release.countDown();
        }
        drainIngestion();
        assertEquals(4, adapter.droppedEventCount()); // two rejected and two invalidated accepted boundaries
        assertEquals(1, adapter.droppedOpenCount());
        rootEnd(1, new SocketTimeoutException());
        drainIngestion();
        rootEnd(20, new SocketTimeoutException());
        assertTrue(spans().isEmpty());
        assertTrue(reader.collectAllMetrics().isEmpty());
        rootStart(30);
        drainIngestion();
        rootEnd(30, new SocketTimeoutException());
        assertEquals(1, spans().size());
        assertEquals(2, reader.collectAllMetrics().size());
        for (MetricData metric : reader.collectAllMetrics()) {
            assertEquals(1L, metric.getLongSumData().getPoints().iterator().next().getValue());
        }
    }

    @Test
    void contendedAdmissionDoesNotWaitAndInvalidatesPendingRoot() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).metricsEnabled(true).build();
        rootStart(1);
        drainIngestion();
        java.util.concurrent.locks.ReentrantLock admission = (java.util.concurrent.locks.ReentrantLock) field(adapter,
                "admission");
        ExecutorService publisher = Executors.newSingleThreadExecutor();
        admission.lock();
        try {
            publisher.submit(
                    () -> adapter.publish(event(START, 2, 1, 1, PerformanceActivity.DNS, 10, 0, null, null, false)))
                    .get(10, TimeUnit.SECONDS);
            assertEquals(1, adapter.droppedEventCount());
        } finally {
            admission.unlock();
            publisher.shutdownNow();
        }
        rootEnd(1, new SocketTimeoutException());
        assertTrue(spans().isEmpty());
        assertEquals(1, adapter.droppedOpenCount());
        assertTrue(reader.collectAllMetrics().isEmpty());
    }

    @Test
    void closeIsFiniteWhileIngestionBlockedAndReleasesAcceptedQueue() throws Exception {
        BlockingClock clock = new BlockingClock();
        OpenTelemetryConnectionCallback.Builder builder = setup(Sampler.alwaysOn(), null)
                .closeTimeout(Duration.ofMillis(10));
        builder.nanoClock = clock;
        adapter = builder.build();
        ExecutorService callers = Executors.newFixedThreadPool(3);
        clock.arm();
        try {
            rootStart(1);
            assertTrue(clock.entered.await(10, TimeUnit.SECONDS));
            rootEnd(1, new UnknownHostException());
            Future<?> first = callers.submit(() -> adapter.close());
            Future<?> second = callers.submit(() -> adapter.close());
            callers.submit(() -> {
                for (int i = 10; i < 30; i++) {
                    rootStart(i);
                    rootEnd(i, new UnknownHostException());
                }
            }).get(10, TimeUnit.SECONDS);
            first.get(10, TimeUnit.SECONDS);
            second.get(10, TimeUnit.SECONDS);
            assertTrue(((java.util.Queue<?>) field(adapter, "ingress")).isEmpty());
            assertEquals(0, adapter.pendingOpenCount());
            assertEquals(0, adapter.queuedOpenCount());
            assertFalse(adapter.awaitIdle(Duration.ZERO)); // clock still owns one in-flight snapshot
            assertTrue(adapter.droppedEventCount() > 0);
        } finally {
            clock.release.countDown();
            callers.shutdownNow();
        }
        assertTrue(spans().isEmpty());
        for (String name : new String[] {"ingestion", "worker"}) {
            Thread thread = (Thread) field(adapter, name);
            thread.join(WAIT.toMillis());
            assertFalse(thread.isAlive());
            assertSame(OpenTelemetryConnectionCallback.class.getClassLoader(), thread.getContextClassLoader());
        }
    }

    private static Object field(Object target, String name) throws Exception {
        java.lang.reflect.Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static final class BlockingClock implements java.util.function.LongSupplier {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final java.util.concurrent.atomic.AtomicBoolean armed = new java.util.concurrent.atomic.AtomicBoolean();

        void arm() {
            armed.set(true);
        }

        @Override
        public long getAsLong() {
            if (Thread.currentThread().getName().endsWith("-ingest") && armed.compareAndSet(true, false)) {
                entered.countDown();
                boolean interrupted = false;
                while (true) {
                    try {
                        release.await();
                        break;
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
            return System.nanoTime();
        }
    }

    @Test
    void successIncludingHandledRetryExportsNothing() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).metricsEnabled(true).build();
        tree(1, new SocketTimeoutException("SECRET"), false, true);
        assertTrue(spans().isEmpty());
        assertTrue(reader.collectAllMetrics().isEmpty());
        assertEquals(0, adapter.pendingOpenCount());
    }

    @Test
    void dnsTreeHasExactBoundariesParentsAndOneOriginEvent() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).build();
        tree(1, new UnknownHostException("SECRET"), false, false);
        List<SpanData> spans = spans();
        assertEquals(4, spans.size());
        verifyTree(spans, "dns", "name_resolution");
    }

    @Test
    void loginTreeHasExactBoundariesParentsAndOneOriginEvent() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).build();
        tree(1, new LoginException("SECRET user password"), true, false);
        List<SpanData> spans = spans();
        assertEquals(8, spans.size());
        verifyTree(spans, "login", "authentication");
    }

    private void verifyTree(List<SpanData> spans, String phase, String category) {
        SpanData root = named(spans, "open");
        SpanData attempt = named(spans, "attempt");
        assertEquals(EPOCH, root.getStartEpochNanos());
        assertEquals(8000001, root.getEndEpochNanos() - root.getStartEpochNanos());
        assertEquals(6000001, attempt.getEndEpochNanos() - attempt.getStartEpochNanos());
        assertEquals(SpanKind.CLIENT, root.getKind());
        assertEquals(StatusCode.ERROR, root.getStatus().getStatusCode());
        assertEquals(category, root.getAttributes().get(AttributeKey.stringKey("mssql.error.category")));
        assertEquals(phase, root.getAttributes().get(AttributeKey.stringKey("mssql.connection.failure_phase")));
        assertEquals(root.getSpanId(), attempt.getParentSpanId());
        assertEquals(attempt.getSpanId(), named(spans, phase).getParentSpanId());
        assertEquals(1, spans.stream().mapToInt(s -> s.getEvents().size()).sum());
        assertEquals("mssql.driver.error", named(spans, phase).getEvents().get(0).getName());
        assertEquals(named(spans, phase).getEndEpochNanos(), named(spans, phase).getEvents().get(0).getEpochNanos());
        assertEquals(3, named(spans, phase).getEvents().get(0).getAttributes().size());
        for (SpanData span : spans) {
            assertEquals(root.getTraceId(), span.getTraceId());
            assertEquals("com.microsoft.sqlserver.jdbc", span.getInstrumentationScopeInfo().getName());
            assertFalse(span.toString().contains("SECRET"));
            assertEquals("", span.getStatus().getDescription());
            assertEquals(span == root,
                    span.getAttributes().get(AttributeKey.stringKey("mssql.connection.guid")) != null);
            assertEquals(span == attempt,
                    span.getAttributes().get(AttributeKey.stringKey("mssql.connection.client_connection_id")) != null);
            if (span != root && span != attempt) {
                assertEquals(1001, span.getEndEpochNanos() - span.getStartEpochNanos());
            }
        }
        assertEquals(StatusCode.UNSET, named(spans, "configuration").getStatus().getStatusCode());
    }

    @Test
    void capturesApplicationParentAtStartNotEnd() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).build();
        Span parent = sdk.getTracer("app").spanBuilder("app-parent").startSpan();
        try (Scope scope = parent.makeCurrent()) {
            rootStart(1);
        }
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(() -> rootEnd(1, new UnknownHostException("SECRET"))).get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
        SpanData root = named(spans(), "open");
        assertEquals(parent.getSpanContext().getSpanId(), root.getParentSpanId());
        assertEquals(parent.getSpanContext().getTraceId(), root.getTraceId());
        parent.end();
    }

    @Test
    void independentConcurrentRoots() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).build();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 1; i <= 16; i++) {
                final long root = i * 10;
                futures.add(executor.submit(() -> tree(root, new UnknownHostException(), false, false)));
            }
            for (Future<?> future : futures) {
                future.get(10, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }
        List<SpanData> spans = spans();
        // Contending producers may deliberately lose admission; every exported tree must still be complete.
        assertEquals(0, spans.size() % 4);
        assertEquals(spans.size() / 4, spans.stream().map(SpanData::getTraceId).distinct().count());
        if (adapter.droppedEventCount() == 0) {
            assertEquals(64, spans.size());
        }
        assertEquals(0, adapter.pendingOpenCount());
    }

    @Test
    void replayHonorsUnsampledApplicationParentWithoutCreatingLivePhaseContext() throws Exception {
        adapter = setup(Sampler.parentBased(Sampler.alwaysOn()), null).metricsEnabled(true).build();
        io.opentelemetry.api.trace.SpanContext parent = io.opentelemetry.api.trace.SpanContext.createFromRemoteParent(
                "11111111111111111111111111111111", "2222222222222222",
                io.opentelemetry.api.trace.TraceFlags.getDefault(), io.opentelemetry.api.trace.TraceState.getDefault());
        try (Scope ignored = Context.root().with(Span.wrap(parent)).makeCurrent()) {
            rootStart(1);
            assertEquals(parent, Span.current().getSpanContext());
            assertTrue(exporter.getFinishedSpanItems().isEmpty());
            rootEnd(1, new UnknownHostException());
            assertEquals(parent, Span.current().getSpanContext());
        }
        assertTrue(spans().isEmpty());
        assertEquals(1L,
                reader.collectAllMetrics().stream()
                        .filter(metric -> metric.getName().equals("mssql.driver.connection.failure.count")).findFirst()
                        .get().getLongSumData().getPoints().iterator().next().getValue());
    }

    @Test
    void metricsAreOptionalAttributeFreeAndOncePerTerminalRoot() throws Exception {
        adapter = setup(Sampler.alwaysOff(), null).metricsEnabled(true).build();
        tree(1, new SocketTimeoutException("SECRET"), false, false);
        rootEnd(1, new SocketTimeoutException()); // duplicate END
        tree(20, new UnknownHostException(), false, false);
        tree(40, new SocketTimeoutException(), false, true);
        assertTrue(spans().isEmpty()); // honor application sampler without suppressing counters
        assertEquals(2, reader.collectAllMetrics().size());
        for (MetricData metric : reader.collectAllMetrics()) {
            assertTrue(metric.getName().equals("mssql.driver.connection.failure.count")
                    || metric.getName().equals("mssql.driver.connection.timeout.count"));
            assertEquals(1, metric.getLongSumData().getPoints().size());
            metric.getLongSumData().getPoints().forEach(p -> {
                assertTrue(p.getAttributes().isEmpty());
                assertEquals(metric.getName().contains("timeout") ? 1 : 2, p.getValue());
            });
        }
    }

    @Test
    void defaultMetricsDisabledAndLegacyCallbacksAreNoOps() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).build();
        adapter.publish(PerformanceActivity.CONNECTION, 1, 123L, new SQLException("SECRET"));
        adapter.publish(PerformanceActivity.STATEMENT_EXECUTE, 1, 2, 123L, new SQLException("SECRET"));
        assertTrue(spans().isEmpty());
        tree(1, new UnknownHostException(), false, false);
        assertEquals(4, spans().size());
        assertTrue(reader.collectAllMetrics().isEmpty());
    }

    @Test
    void truncatedTreeIsHonestAndDoesNotInventMissingParents() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).maxSpansPerOpen(2).build();
        tree(1, new UnknownHostException(), true, false);
        List<SpanData> spans = spans();
        assertEquals(2, spans.size());
        SpanData root = named(spans, "open");
        assertEquals(true, root.getAttributes().get(AttributeKey.booleanKey("mssql.telemetry.truncated")));
        assertEquals(6L, root.getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_span_count")));
        assertEquals(0L, root.getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_event_count")));
        assertEquals("login", root.getAttributes().get(AttributeKey.stringKey("mssql.connection.failure_phase")));
        assertEquals(root.getSpanId(), named(spans, "configuration").getParentSpanId());
        assertEquals(1, root.getEvents().size());
        assertEquals("login", root.getEvents().get(0).getAttributes().get(AttributeKey.stringKey("mssql.error.phase")));
    }

    @Test
    void rollingSpanLimitRetainsFinalAttemptAndFailureAncestors() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).maxSpansPerOpen(4).maxEventsPerOpen(1).build();
        rootStart(1);
        for (int attempt = 0; attempt < 20; attempt++) {
            long id = 10 + attempt * 3;
            adapter.publish(event(START, id, 1, 1, PerformanceActivity.CONNECTION_ATTEMPT, id, 0, null, null, false));
            adapter.publish(
                    event(START, id + 1, id, 1, PerformanceActivity.LOGIN_EXCHANGE, id + 1, 0, null, null, false));
            adapter.publish(
                    event(START, id + 2, id + 1, 1, PerformanceActivity.TOKEN_REQUEST, id + 2, 0, null, null, false));
            adapter.publish(event(END, id + 2, id + 1, 1, PerformanceActivity.TOKEN_REQUEST, id + 2, 10,
                    new LoginException(), "token_acquisition", true));
            adapter.publish(event(END, id + 1, id, 1, PerformanceActivity.LOGIN_EXCHANGE, id + 1, 20,
                    new LoginException(), "token_acquisition", false));
            adapter.publish(event(END, id, 1, 1, PerformanceActivity.CONNECTION_ATTEMPT, id, 30, new LoginException(),
                    "token_acquisition", false));
        }
        adapter.publish(event(END, 1, 0, 1, PerformanceActivity.CONNECTION, 0, 1000, new LoginException(),
                "token_acquisition", false));
        List<SpanData> result = spans();
        assertEquals(4, result.size());
        SpanData root = named(result, "open");
        SpanData attempt = named(result, "attempt");
        SpanData login = named(result, "login");
        SpanData token = named(result, "token_acquisition");
        assertEquals(EPOCH + 67, attempt.getStartEpochNanos());
        assertEquals(root.getSpanId(), attempt.getParentSpanId());
        assertEquals(attempt.getSpanId(), login.getParentSpanId());
        assertEquals(login.getSpanId(), token.getParentSpanId());
        assertEquals(1, token.getEvents().size());
        assertTrue(root.getEvents().isEmpty());
        assertEquals(57L, root.getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_span_count")));
        assertEquals(19L, root.getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_event_count")));
    }

    @Test
    void tinyCapPreservesErrorOnNearestRetainedAncestorWithoutDuplicateRoot() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).maxSpansPerOpen(2).build();
        rootStart(1);
        adapter.publish(event(START, 2, 1, 1, PerformanceActivity.CONNECTION_ATTEMPT, 10, 0, null, null, false));
        adapter.publish(event(START, 3, 2, 1, PerformanceActivity.LOGIN_EXCHANGE, 20, 0, null, null, false));
        adapter.publish(event(START, 4, 3, 1, PerformanceActivity.TOKEN_REQUEST, 30, 0, null, null, false));
        adapter.publish(event(END, 4, 3, 1, PerformanceActivity.TOKEN_REQUEST, 30, 10, new LoginException(),
                "token_acquisition", true));
        adapter.publish(event(END, 3, 2, 1, PerformanceActivity.LOGIN_EXCHANGE, 20, 30, new LoginException(),
                "token_acquisition", false));
        adapter.publish(event(END, 2, 1, 1, PerformanceActivity.CONNECTION_ATTEMPT, 10, 50, new LoginException(),
                "token_acquisition", false));
        adapter.publish(event(END, 1, 0, 1, PerformanceActivity.CONNECTION, 0, 100, new LoginException(),
                "token_acquisition", false));
        List<SpanData> result = spans();
        assertEquals(2, result.size());
        assertTrue(named(result, "open").getEvents().isEmpty());
        assertEquals(1, named(result, "attempt").getEvents().size());
        assertEquals("token_acquisition", named(result, "attempt").getEvents().get(0).getAttributes()
                .get(AttributeKey.stringKey("mssql.error.phase")));
        assertEquals(EPOCH + 40, named(result, "attempt").getEvents().get(0).getEpochNanos());
    }

    @Test
    void finalFailureAfterManySuccessfulPhasesRetainsWholeChain() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).maxSpansPerOpen(4).build();
        rootStart(1);
        for (int i = 2; i < 200; i++) {
            adapter.publish(event(START, i, 1, 1, PerformanceActivity.DNS, i, 0, null, null, false));
            adapter.publish(event(END, i, 1, 1, PerformanceActivity.DNS, i, 1, null, null, false));
        }
        adapter.publish(event(START, 200, 1, 1, PerformanceActivity.CONNECTION_ATTEMPT, 200, 0, null, null, false));
        adapter.publish(event(START, 201, 200, 1, PerformanceActivity.LOGIN_EXCHANGE, 201, 0, null, null, false));
        adapter.publish(event(START, 202, 201, 1, PerformanceActivity.TOKEN_REQUEST, 202, 0, null, null, false));
        adapter.publish(event(END, 202, 201, 1, PerformanceActivity.TOKEN_REQUEST, 202, 10, new LoginException(),
                "token_acquisition", true));
        adapter.publish(event(END, 201, 200, 1, PerformanceActivity.LOGIN_EXCHANGE, 201, 20, new LoginException(),
                "token_acquisition", false));
        adapter.publish(event(END, 200, 1, 1, PerformanceActivity.CONNECTION_ATTEMPT, 200, 30, new LoginException(),
                "token_acquisition", false));
        adapter.publish(event(END, 1, 0, 1, PerformanceActivity.CONNECTION, 0, 1000, new LoginException(),
                "token_acquisition", false));
        List<SpanData> result = spans();
        assertEquals(4, result.size());
        SpanData root = named(result, "open");
        SpanData attempt = named(result, "attempt");
        SpanData login = named(result, "login");
        SpanData token = named(result, "token_acquisition");
        assertEquals(EPOCH + 200, attempt.getStartEpochNanos());
        assertEquals(root.getSpanId(), attempt.getParentSpanId());
        assertEquals(attempt.getSpanId(), login.getParentSpanId());
        assertEquals(login.getSpanId(), token.getParentSpanId());
        assertEquals(1, token.getEvents().size());
        assertTrue(root.getEvents().isEmpty());
        assertEquals(198L, root.getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_span_count")));
        assertEquals(0L, root.getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_event_count")));
    }

    @Test
    void endRecoversBoundedMissingAncestryWhenEarlierActiveBranchCompletes() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).maxSpansPerOpen(4).build();
        rootStart(1);
        // Three concurrent scopes temporarily consume the entire span budget.
        for (int i = 2; i <= 4; i++) {
            adapter.publish(event(START, i, 1, 1, PerformanceActivity.DNS, i, 0, null, null, false));
        }
        adapter.publish(event(START, 5, 1, 1, PerformanceActivity.CONNECTION_ATTEMPT, 10, 0, null, null, false));
        adapter.publish(event(START, 6, 5, 1, PerformanceActivity.LOGIN_EXCHANGE, 20, 0, null, null, false));
        adapter.publish(event(START, 7, 6, 1, PerformanceActivity.TOKEN_REQUEST, 30, 0, null, null, false));
        for (int i = 2; i <= 4; i++) {
            adapter.publish(event(END, i, 1, 1, PerformanceActivity.DNS, i, 30, null, null, false));
        }
        adapter.publish(event(END, 7, 6, 1, PerformanceActivity.TOKEN_REQUEST, 30, 10, new LoginException(),
                "token_acquisition", true));
        adapter.publish(event(END, 6, 5, 1, PerformanceActivity.LOGIN_EXCHANGE, 20, 30, new LoginException(),
                "token_acquisition", false));
        adapter.publish(event(END, 5, 1, 1, PerformanceActivity.CONNECTION_ATTEMPT, 10, 50, new LoginException(),
                "token_acquisition", false));
        adapter.publish(event(END, 1, 0, 1, PerformanceActivity.CONNECTION, 0, 100, new LoginException(),
                "token_acquisition", false));
        List<SpanData> result = spans();
        assertEquals(4, result.size());
        assertEquals(named(result, "open").getSpanId(), named(result, "attempt").getParentSpanId());
        assertEquals(named(result, "attempt").getSpanId(), named(result, "login").getParentSpanId());
        assertEquals(named(result, "login").getSpanId(), named(result, "token_acquisition").getParentSpanId());
        assertEquals(1, named(result, "token_acquisition").getEvents().size());
        assertTrue(named(result, "open").getEvents().isEmpty());
        assertEquals(3L,
                named(result, "open").getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_span_count")));
    }

    @Test
    void endWithoutRetainedStartUsesRealBoundaryAndRootOnlyFallbackIsMarked() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).maxSpansPerOpen(2).build();
        rootStart(1);
        // Core END includes the original START timestamp and parent even if START was not observed.
        adapter.publish(event(END, 2, 1, 1, PerformanceActivity.DNS, 10, 20, new UnknownHostException(), "dns", true));
        adapter.publish(
                event(END, 1, 0, 1, PerformanceActivity.CONNECTION, 0, 100, new UnknownHostException(), "dns", false));
        List<SpanData> result = spans();
        assertEquals(2, result.size());
        assertEquals(EPOCH + 10, named(result, "dns").getStartEpochNanos());
        assertEquals(EPOCH + 30, named(result, "dns").getEndEpochNanos());
        assertEquals(1, named(result, "dns").getEvents().size());
        adapter.close();
        exporter.reset();
        adapter = OpenTelemetryConnectionCallback.builder(sdk).maxSpansPerOpen(1).build();
        rootStart(10);
        adapter.publish(
                event(END, 11, 10, 10, PerformanceActivity.DNS, 10, 20, new UnknownHostException(), "dns", true));
        adapter.publish(event(END, 10, 0, 10, PerformanceActivity.CONNECTION, 0, 100, new UnknownHostException(), "dns",
                false));
        SpanData root = named(spans(), "open");
        assertEquals(1, root.getEvents().size());
        assertEquals("dns", root.getEvents().get(0).getAttributes().get(AttributeKey.stringKey("mssql.error.phase")));
        assertEquals(true, root.getAttributes().get(AttributeKey.booleanKey("mssql.telemetry.truncated")));
        assertEquals(1L, root.getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_span_count")));
    }

    @Test
    void rootOnlyCapCountsRejectedStartsOnceAfterAllEndsUnwind() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).maxSpansPerOpen(1).maxEventsPerOpen(1).build();
        rootStart(1);
        adapter.publish(event(START, 2, 1, 1, PerformanceActivity.CONNECTION_ATTEMPT, 10, 0, null, null, false));
        adapter.publish(event(START, 3, 2, 1, PerformanceActivity.LOGIN_EXCHANGE, 20, 0, null, null, false));
        adapter.publish(event(START, 4, 3, 1, PerformanceActivity.TOKEN_REQUEST, 30, 0, null, null, false));
        adapter.publish(event(END, 4, 3, 1, PerformanceActivity.TOKEN_REQUEST, 30, 10, new LoginException("SECRET"),
                "token_acquisition", true));
        adapter.publish(event(END, 3, 2, 1, PerformanceActivity.LOGIN_EXCHANGE, 20, 30, new LoginException("SECRET"),
                "token_acquisition", false));
        adapter.publish(event(END, 2, 1, 1, PerformanceActivity.CONNECTION_ATTEMPT, 10, 50,
                new LoginException("SECRET"), "token_acquisition", false));
        adapter.publish(event(END, 1, 0, 1, PerformanceActivity.CONNECTION, 0, 100, new LoginException("SECRET"),
                "token_acquisition", false));
        List<SpanData> result = spans();
        assertEquals(1, result.size());
        SpanData root = named(result, "open");
        assertEquals(3L, root.getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_span_count")));
        assertEquals(true, root.getAttributes().get(AttributeKey.booleanKey("mssql.telemetry.truncated")));
        assertEquals(0L, root.getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_event_count")));
        assertEquals(StatusCode.ERROR, root.getStatus().getStatusCode());
        assertEquals(1, root.getEvents().size());
        assertEquals("mssql.driver.error", root.getEvents().get(0).getName());
        assertEquals("token_acquisition",
                root.getEvents().get(0).getAttributes().get(AttributeKey.stringKey("mssql.error.phase")));
        assertEquals(EPOCH + 40, root.getEvents().get(0).getEpochNanos());
        assertFalse(result.toString().contains("SECRET"));
        assertEquals(0, adapter.pendingOpenCount());
    }

    @Test
    void rootOnlyCapPreservesTerminalErrorAfterManyMissingStarts() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).maxSpansPerOpen(1).maxEventsPerOpen(1).build();
        rootStart(1);
        for (int i = 2; i <= 10000; i++) {
            adapter.publish(event(START, i, i - 1, 1, PerformanceActivity.DNS, i, 0, null, null, false));
            if (i % 100 == 0) {
                drainIngestion();
            }
        }
        drainIngestion();
        java.lang.reflect.Field pending = OpenTelemetryConnectionCallback.class.getDeclaredField("pending");
        pending.setAccessible(true);
        Object open = ((Map<?, ?>) pending.get(adapter)).get(1L);
        for (String field : new String[] {"nodes", "missing"}) {
            java.lang.reflect.Field records = open.getClass().getDeclaredField(field);
            records.setAccessible(true);
            assertEquals(1, ((Map<?, ?>) records.get(open)).size());
        }
        for (int i = 10000; i >= 2; i--) {
            adapter.publish(event(END, i, i - 1, 1, PerformanceActivity.DNS, i, 20000 - 2 * i,
                    new UnknownHostException("SECRET"), "dns", i == 10000));
            if (i % 100 == 0) {
                drainIngestion();
            }
        }
        adapter.publish(event(END, 1, 0, 1, PerformanceActivity.CONNECTION, 0, 20000, new UnknownHostException(), "dns",
                false));
        SpanData root = named(spans(), "open");
        assertEquals(1, root.getEvents().size());
        assertEquals("dns", root.getEvents().get(0).getAttributes().get(AttributeKey.stringKey("mssql.error.phase")));
        assertEquals(9999L, root.getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_span_count")));
    }

    @Test
    void partialMetricInitializationRetainsDeltasAndRetriesWithoutAnotherTree() throws Exception {
        setup(Sampler.alwaysOn(), null);
        java.util.concurrent.atomic.AtomicBoolean fail = new java.util.concurrent.atomic.AtomicBoolean(true);
        io.opentelemetry.api.metrics.Meter meter = sdk.getMeter("com.microsoft.sqlserver.jdbc");
        io.opentelemetry.api.metrics.Meter wrappedMeter = proxy(io.opentelemetry.api.metrics.Meter.class, meter,
                (method, args) -> {
                    if ("counterBuilder".equals(method.getName())
                            && "mssql.driver.connection.timeout.count".equals(args[0])) {
                        io.opentelemetry.api.metrics.LongCounterBuilder builder = meter
                                .counterBuilder((String) args[0]);
                        return proxy(io.opentelemetry.api.metrics.LongCounterBuilder.class, builder, (m, a) -> {
                            if ("build".equals(m.getName()) && fail.getAndSet(false)) {
                                throw new IllegalStateException("SECRET metric initialization");
                            }
                            return m.invoke(builder, a);
                        });
                    }
                    return method.invoke(meter, args);
                });
        io.opentelemetry.api.OpenTelemetry wrapped = proxy(io.opentelemetry.api.OpenTelemetry.class, sdk,
                (method, args) -> "getMeter".equals(method.getName()) ? wrappedMeter : method.invoke(sdk, args));
        adapter = OpenTelemetryConnectionCallback.builder(wrapped).metricsEnabled(true).build();
        tree(1, new SocketTimeoutException(), false, false);
        assertTrue(adapter.awaitIdle(WAIT));
        assertEquals(2, reader.collectAllMetrics().size());
        reader.collectAllMetrics()
                .forEach(metric -> assertEquals(1L, metric.getLongSumData().getPoints().iterator().next().getValue()));
        tree(20, new SocketTimeoutException(), false, false);
        assertEquals(8, spans().size()); // metric failures must not suppress traces
        assertEquals(2, reader.collectAllMetrics().size());
        for (MetricData metric : reader.collectAllMetrics()) {
            assertEquals(2, metric.getLongSumData().getPoints().iterator().next().getValue());
        }
    }

    @Test
    void meterAcquisitionFailureDoesNotDropSpansAndRecoversOffPublisherThread() throws Exception {
        setup(Sampler.alwaysOn(), null);
        Thread publisher = Thread.currentThread();
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        io.opentelemetry.api.OpenTelemetry wrapped = proxy(io.opentelemetry.api.OpenTelemetry.class, sdk,
                (method, args) -> {
                    assertNotSame(publisher, Thread.currentThread());
                    if ("getMeter".equals(method.getName()) && calls.getAndIncrement() == 0) {
                        throw new IllegalStateException("SECRET meter failure");
                    }
                    return method.invoke(sdk, args);
                });
        adapter = OpenTelemetryConnectionCallback.builder(wrapped).metricsEnabled(true).build();
        rootStart(1);
        assertEquals(0, calls.get());
        rootEnd(1, new SocketTimeoutException());
        assertTrue(adapter.awaitIdle(WAIT));
        rootStart(10);
        rootEnd(10, new SocketTimeoutException());
        assertEquals(2, spans().size());
        assertEquals(0, adapter.droppedOpenCount());
        assertEquals(2, reader.collectAllMetrics().size());
        reader.collectAllMetrics()
                .forEach(metric -> assertEquals(2L, metric.getLongSumData().getPoints().iterator().next().getValue()));
    }

    @Test
    void twoAndThreeSpanLimitsKeepFinalTlsOrLoginFailureAfterRetries() throws Exception {
        setup(Sampler.alwaysOn(), null);
        for (int limit : new int[] {2, 3}) {
            for (PerformanceActivity activity : new PerformanceActivity[] {PerformanceActivity.TLS,
                    PerformanceActivity.LOGIN_EXCHANGE}) {
                adapter = OpenTelemetryConnectionCallback.builder(sdk).maxSpansPerOpen(limit).maxEventsPerOpen(1)
                        .build();
                String phase = activity == PerformanceActivity.TLS ? "tls" : "login";
                rootStart(1);
                for (int i = 0; i < 10; i++) {
                    long id = 10 + i * 2;
                    adapter.publish(
                            event(START, id, 1, 1, PerformanceActivity.CONNECTION_ATTEMPT, id, 0, null, null, false));
                    adapter.publish(event(START, id + 1, id, 1, activity, id + 1, 0, null, null, false));
                    adapter.publish(
                            event(END, id + 1, id, 1, activity, id + 1, 1, new SocketTimeoutException(), phase, true));
                    adapter.publish(event(END, id, 1, 1, PerformanceActivity.CONNECTION_ATTEMPT, id, 2,
                            new SocketTimeoutException(), phase, false));
                }
                adapter.publish(event(END, 1, 0, 1, PerformanceActivity.CONNECTION, 0, 100,
                        new SocketTimeoutException(), phase, false));
                List<SpanData> result = spans();
                assertEquals(limit, result.size());
                SpanData root = named(result, "open");
                SpanData attempt = named(result, "attempt");
                SpanData owner = limit == 3 ? named(result, phase) : attempt;
                assertEquals(EPOCH + 28, attempt.getStartEpochNanos());
                assertEquals(root.getSpanId(), attempt.getParentSpanId());
                assertEquals(root.getTraceId(), owner.getTraceId());
                if (limit == 3) {
                    assertEquals(attempt.getSpanId(), owner.getParentSpanId());
                }
                assertEquals("11111111-1111-1111-1111-111111111111",
                        root.getAttributes().get(AttributeKey.stringKey("mssql.connection.guid")));
                assertEquals("22222222-2222-2222-2222-222222222222",
                        attempt.getAttributes().get(AttributeKey.stringKey("mssql.connection.client_connection_id")));
                assertEquals(1, result.stream().mapToInt(span -> span.getEvents().size()).sum());
                assertEquals(EPOCH + 30, owner.getEvents().get(0).getEpochNanos());
                assertEquals(phase,
                        owner.getEvents().get(0).getAttributes().get(AttributeKey.stringKey("mssql.error.phase")));
                assertEquals(true, root.getAttributes().get(AttributeKey.booleanKey("mssql.telemetry.truncated")));
                assertEquals(21L - limit,
                        root.getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_span_count")));
                adapter.close();
                exporter.reset();
            }
        }
    }

    @Test
    void countersIncludeQueueDroppedFailuresWithoutBlockingPublish() throws Exception {
        BlockingProcessor blocker = new BlockingProcessor();
        adapter = setup(Sampler.alwaysOn(), blocker).metricsEnabled(true).queueCapacity(1).build();
        ExecutorService publisher = Executors.newSingleThreadExecutor();
        try {
            rootStart(1);
            rootEnd(1, new UnknownHostException());
            assertTrue(blocker.entered.await(10, TimeUnit.SECONDS));
            publisher.submit(() -> {
                for (int i = 2; i <= 101; i++) {
                    rootStart(i);
                    rootEnd(i, new SocketTimeoutException());
                    rootEnd(i, new SocketTimeoutException()); // duplicate END is not a new observation
                }
            }).get(10, TimeUnit.SECONDS);
            drainIngestion();
            assertEquals(1, adapter.queuedOpenCount());
            assertEquals(99, adapter.droppedOpenCount());
        } finally {
            blocker.release.countDown();
            publisher.shutdownNow();
        }
        assertEquals(2, spans().size());
        for (MetricData metric : reader.collectAllMetrics()) {
            assertEquals(metric.getName().contains("timeout") ? 100L : 101L,
                    metric.getLongSumData().getPoints().iterator().next().getValue());
        }
        assertEquals(2, reader.collectAllMetrics().size());
    }

    @Test
    void evictedAndUnobservedRootEndsAreNotCounted() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).metricsEnabled(true).maxPendingOpens(1).build();
        rootStart(1);
        rootStart(2);
        rootEnd(1, new SocketTimeoutException());
        rootEnd(99, new SocketTimeoutException());
        rootEnd(2, new SocketTimeoutException());
        assertEquals(1, spans().size());
        assertEquals(2, reader.collectAllMetrics().size());
        reader.collectAllMetrics()
                .forEach(metric -> assertEquals(1L, metric.getLongSumData().getPoints().iterator().next().getValue()));
    }

    @Test
    void tracerAcquisitionFailureDoesNotSuppressObservedFailureCounter() throws Exception {
        setup(Sampler.alwaysOn(), null);
        io.opentelemetry.api.OpenTelemetry wrapped = proxy(io.opentelemetry.api.OpenTelemetry.class, sdk,
                (method, args) -> {
                    if ("getTracer".equals(method.getName())) {
                        throw new IllegalStateException("SECRET tracer failure");
                    }
                    return method.invoke(sdk, args);
                });
        adapter = OpenTelemetryConnectionCallback.builder(wrapped).metricsEnabled(true).build();
        rootStart(1);
        rootEnd(1, new SocketTimeoutException());
        assertTrue(spans().isEmpty());
        assertEquals(1, adapter.droppedOpenCount());
        assertEquals(2, reader.collectAllMetrics().size());
        reader.collectAllMetrics()
                .forEach(metric -> assertEquals(1L, metric.getLongSumData().getPoints().iterator().next().getValue()));
    }

    @Test
    void throwingCounterAddIsNotReplayedAndDoesNotSuppressTimeoutCounter() throws Exception {
        setup(Sampler.alwaysOn(), null);
        java.util.concurrent.atomic.AtomicBoolean fail = new java.util.concurrent.atomic.AtomicBoolean(true);
        io.opentelemetry.api.metrics.Meter meter = sdk.getMeter("com.microsoft.sqlserver.jdbc");
        io.opentelemetry.api.metrics.Meter wrappedMeter = proxy(io.opentelemetry.api.metrics.Meter.class, meter,
                (method, args) -> {
                    if ("counterBuilder".equals(method.getName())
                            && "mssql.driver.connection.failure.count".equals(args[0])) {
                        io.opentelemetry.api.metrics.LongCounterBuilder builder = meter
                                .counterBuilder((String) args[0]);
                        return proxy(io.opentelemetry.api.metrics.LongCounterBuilder.class, builder, (m, a) -> {
                            if ("build".equals(m.getName())) {
                                io.opentelemetry.api.metrics.LongCounter counter = builder.build();
                                return proxy(io.opentelemetry.api.metrics.LongCounter.class, counter, (add, delta) -> {
                                    Object result = add.invoke(counter, delta);
                                    if ("add".equals(add.getName()) && fail.getAndSet(false)) {
                                        throw new IllegalStateException("SECRET after recording");
                                    }
                                    return result;
                                });
                            }
                            return m.invoke(builder, a);
                        });
                    }
                    return method.invoke(meter, args);
                });
        io.opentelemetry.api.OpenTelemetry wrapped = proxy(io.opentelemetry.api.OpenTelemetry.class, sdk,
                (method, args) -> "getMeter".equals(method.getName()) ? wrappedMeter : method.invoke(sdk, args));
        adapter = OpenTelemetryConnectionCallback.builder(wrapped).metricsEnabled(true).build();
        rootStart(1);
        rootEnd(1, new SocketTimeoutException());
        assertTrue(adapter.awaitIdle(WAIT));
        rootStart(2);
        rootEnd(2, new SocketTimeoutException());
        assertEquals(2, spans().size());
        assertEquals(2, reader.collectAllMetrics().size());
        reader.collectAllMetrics()
                .forEach(metric -> assertEquals(2L, metric.getLongSumData().getPoints().iterator().next().getValue()));
    }

    @Test
    void persistentMetricInitializationFailureHasBoundedClose() throws Exception {
        setup(Sampler.alwaysOn(), null);
        CountDownLatch attempted = new CountDownLatch(1);
        io.opentelemetry.api.OpenTelemetry wrapped = proxy(io.opentelemetry.api.OpenTelemetry.class, sdk,
                (method, args) -> {
                    if ("getMeter".equals(method.getName())) {
                        attempted.countDown();
                        throw new IllegalStateException("SECRET persistent SDK failure");
                    }
                    return method.invoke(sdk, args);
                });
        adapter = OpenTelemetryConnectionCallback.builder(wrapped).metricsEnabled(true)
                .closeTimeout(Duration.ofMillis(10)).build();
        rootStart(1);
        rootEnd(1, new SocketTimeoutException());
        assertTrue(attempted.await(10, TimeUnit.SECONDS));
        assertFalse(adapter.awaitIdle(Duration.ZERO));
        ExecutorService closer = Executors.newSingleThreadExecutor();
        try {
            closer.submit(() -> adapter.close()).get(10, TimeUnit.SECONDS);
        } finally {
            closer.shutdownNow();
        }
        assertTrue(adapter.awaitIdle(WAIT));
        assertEquals(0, adapter.pendingOpenCount());
        assertEquals(0, adapter.queuedOpenCount());
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, T delegate, ReflectiveCall call) {
        return (T) java.lang.reflect.Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[] {type},
                (object, method, args) -> {
                    try {
                        return call.invoke(method, args);
                    } catch (java.lang.reflect.InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    private interface ReflectiveCall {
        Object invoke(java.lang.reflect.Method method, Object[] args) throws Throwable;
    }

    private Map<String, Object> diagnostic(String name, long timestamp, Map<String, Object> attributes) {
        Map<String, Object> result = new HashMap<>();
        result.put("name", name);
        result.put("timestamp", timestamp);
        result.put("attributes", attributes);
        return result;
    }

    @Test
    void coreEndDiagnosticsAreConsumedOnceBeforeRootIsQueued() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).build();
        rootStart(1);
        adapter.publish(event(START, 2, 1, 1, PerformanceActivity.TOKEN_REQUEST, 10, 0, null, null, false));
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("mssql.authentication.method", "access_token_callback");
        attrs.put("mssql.authentication.token_source", "callback");
        attrs.put("mssql.error.message", "SECRET");
        attrs.put("mssql.timeout.kind", "token_request");
        attrs.put("mssql.timeout.phase", "token_acquisition");
        attrs.put("mssql.timeout.value", 0.25);
        List<Map<String, Object>> diagnostics = new ArrayList<>();
        diagnostics.add(diagnostic("mssql.driver.authentication", EPOCH + 15, attrs));
        diagnostics.add(diagnostic("mssql.driver.timeout", EPOCH + 30, attrs));
        diagnostics.add(diagnostic("mssql.driver.timeout", EPOCH + 500, attrs)); // outside boundary
        PerformanceLogEvent end = ConnectionEventFixture.withDiagnostics(event(END, 2, 1, 1,
                PerformanceActivity.TOKEN_REQUEST, 10, 20, new SocketTimeoutException(), "token_acquisition", true),
                diagnostics);
        adapter.publish(end);
        adapter.publish(end);
        diagnostics = new ArrayList<>();
        diagnostics
                .add(diagnostic("mssql.driver.retry", EPOCH + 40, Collections.singletonMap("mssql.retry.delay", 0.5)));
        diagnostics.add(diagnostic("mssql.driver.redirect", EPOCH + 50,
                Collections.singletonMap("mssql.connection.redirect.type", "tds_routing")));
        adapter.publish(ConnectionEventFixture.withDiagnostics(event(END, 1, 0, 1, PerformanceActivity.CONNECTION, 0,
                100, new SocketTimeoutException(), "token_acquisition", false), diagnostics));
        List<SpanData> result = spans();
        SpanData token = named(result, "token_acquisition");
        assertEquals(3, token.getEvents().size());
        assertEquals("mssql.driver.authentication", token.getEvents().get(0).getName());
        assertEquals("mssql.driver.timeout", token.getEvents().get(1).getName());
        assertEquals("mssql.driver.error", token.getEvents().get(2).getName());
        assertEquals(EPOCH + 15, token.getEvents().get(0).getEpochNanos());
        assertEquals(2, named(result, "open").getEvents().size());
        assertFalse(result.toString().contains("SECRET"));
    }

    @Test
    void coreEndDiagnosticOverflowReservesErrorEvenWithRootOnlyBudget() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).maxSpansPerOpen(1).maxEventsPerOpen(1).build();
        rootStart(1);
        PerformanceLogEvent end = event(END, 2, 1, 1, PerformanceActivity.TOKEN_REQUEST, 10, 20,
                new SocketTimeoutException(), "token_acquisition", true);
        List<Map<String, Object>> diagnostics = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            diagnostics.add(diagnostic("mssql.driver.timeout", EPOCH + 20,
                    Collections.singletonMap("mssql.timeout.phase", "token_acquisition")));
        }
        adapter.publish(ConnectionEventFixture.withDiagnostics(end, diagnostics));
        adapter.publish(event(END, 1, 0, 1, PerformanceActivity.CONNECTION, 0, 100, new SocketTimeoutException(),
                "token_acquisition", false));
        SpanData root = named(spans(), "open");
        assertEquals(1, root.getEvents().size());
        assertEquals("mssql.driver.error", root.getEvents().get(0).getName());
        assertEquals("token_acquisition",
                root.getEvents().get(0).getAttributes().get(AttributeKey.stringKey("mssql.error.phase")));
        assertEquals(true, root.getAttributes().get(AttributeKey.booleanKey("mssql.telemetry.truncated")));
    }

    @Test
    void rootBatchRoutesAuthAndTimeoutToMatchingAttemptAndReportsCoreTruncation() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).build();
        rootStart(1);
        for (int i = 2; i <= 3; i++) {
            Map<String, Object> attrs = Collections.singletonMap("mssql.connection.attempt", (long) i);
            adapter.publish(ConnectionEventFixture.event(START, i, 1, 1, PerformanceActivity.TOKEN_REQUEST,
                    EPOCH + i * 10, 0, null, null, false, attrs));
            adapter.publish(
                    ConnectionEventFixture.event(END, i, 1, 1, PerformanceActivity.TOKEN_REQUEST, EPOCH + i * 10, 9,
                            i == 3 ? new SocketTimeoutException() : null, "token_acquisition", i == 3, attrs));
        }
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("mssql.connection.attempt", 3L);
        attrs.put("mssql.authentication.method", "access_token_callback");
        attrs.put("mssql.authentication.token_source", "callback");
        attrs.put("mssql.timeout.phase", "token_acquisition");
        attrs.put("mssql.error.retry_decision", "budget_exhausted");
        List<Map<String, Object>> diagnostics = new ArrayList<>();
        diagnostics.add(diagnostic("mssql.driver.authentication", EPOCH + 31, attrs));
        diagnostics.add(diagnostic("mssql.driver.timeout", EPOCH + 38, attrs));
        diagnostics.add(diagnostic("mssql.driver.connection.retry_decision", EPOCH + 40, attrs));
        adapter.publish(ConnectionEventFixture.withDiagnostics(ConnectionEventFixture.event(END, 1, 0, 1,
                PerformanceActivity.CONNECTION, EPOCH, 100, new SocketTimeoutException(), "token_acquisition", false,
                Collections.singletonMap("mssql.connection.diagnostic_events_dropped", 5L)), diagnostics));
        List<SpanData> result = spans();
        SpanData token = result.stream().filter(span -> span.getStartEpochNanos() == EPOCH + 30).findFirst().get();
        assertEquals(3, token.getEvents().size());
        assertEquals(0, result.stream().filter(span -> span.getStartEpochNanos() == EPOCH + 20).findFirst().get()
                .getEvents().size());
        SpanData root = named(result, "open");
        assertEquals(1, root.getEvents().size());
        assertEquals("budget_exhausted",
                root.getEvents().get(0).getAttributes().get(AttributeKey.stringKey("mssql.error.retry_decision")));
        assertEquals(5L, root.getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_event_count")));
        assertEquals(true, root.getAttributes().get(AttributeKey.booleanKey("mssql.telemetry.truncated")));
    }

    @Test
    void diagnosticReplayFiltersPayloadAndKeepsOriginErrorReserved() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).maxEventsPerOpen(2).build();
        rootStart(1);
        adapter.publish(event(START, 2, 1, 1, PerformanceActivity.TOKEN_REQUEST, 10, 0, null, null, false));
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("mssql.authentication.method", "access_token_callback");
        attrs.put("mssql.authentication.token_source", "callback");
        attrs.put("mssql.error.message", "SECRET");
        attrs.put("server.address", "SECRET");
        drainIngestion();
        adapter.recordDiagnostic(1, 2, "mssql.driver.authentication", EPOCH + 20, attrs);
        adapter.publish(event(END, 2, 1, 1, PerformanceActivity.TOKEN_REQUEST, 10, 30, new SocketTimeoutException(),
                "token_acquisition", true));
        attrs.put("mssql.timeout.phase", "token_acquisition");
        attrs.put("mssql.timeout.kind", "token_request");
        attrs.put("mssql.timeout.value", 2.5);
        drainIngestion();
        adapter.recordDiagnostic(1, 2, "mssql.driver.timeout", EPOCH + 40, attrs);
        adapter.recordDiagnostic(1, 2, "mssql.driver.SECRET", EPOCH + 41, attrs);
        adapter.publish(event(END, 1, 0, 1, PerformanceActivity.CONNECTION, 0, 100, new SocketTimeoutException(),
                "token_acquisition", false));
        List<SpanData> result = spans();
        SpanData token = named(result, "token_acquisition");
        assertEquals(2, token.getEvents().size());
        assertEquals("mssql.driver.error", token.getEvents().get(0).getName());
        assertEquals("mssql.driver.timeout", token.getEvents().get(1).getName());
        assertEquals(EPOCH + 40, token.getEvents().get(1).getEpochNanos());
        assertEquals(2.5, token.getEvents().get(1).getAttributes().get(AttributeKey.doubleKey("mssql.timeout.value")));
        assertFalse(result.toString().contains("SECRET"));
        assertTrue(named(result, "open").getEvents().isEmpty());
        assertEquals(1L,
                named(result, "open").getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_event_count")));
    }

    @Test
    void diagnosticOnlyOverflowCannotDisplaceFinalError() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).maxEventsPerOpen(1).build();
        rootStart(1);
        adapter.publish(event(START, 2, 1, 1, PerformanceActivity.DNS, 10, 0, null, null, false));
        adapter.publish(event(END, 2, 1, 1, PerformanceActivity.DNS, 10, 20, new UnknownHostException(), "dns", true));
        drainIngestion();
        adapter.recordDiagnostic(1, 1, "mssql.driver.retry", EPOCH + 40, Collections.emptyMap());
        adapter.publish(
                event(END, 1, 0, 1, PerformanceActivity.CONNECTION, 0, 100, new UnknownHostException(), "dns", false));
        List<SpanData> result = spans();
        assertEquals(1, named(result, "dns").getEvents().size());
        assertTrue(named(result, "open").getEvents().isEmpty());
        assertEquals(1L,
                named(result, "open").getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_event_count")));
    }

    @Test
    void diagnosticDecisionsBelongToRootAndSuccessfulOpensStillExportNothing() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).build();
        for (long root : new long[] {1, 10}) {
            rootStart(root);
            adapter.publish(
                    event(START, root + 1, root, root, PerformanceActivity.LOGIN_EXCHANGE, 10, 0, null, null, false));
            drainIngestion();
            adapter.recordDiagnostic(root, root + 1, "mssql.driver.retry", EPOCH + 20,
                    Collections.singletonMap("mssql.retry.delay", 0.5));
            adapter.recordDiagnostic(root, root + 1, "mssql.driver.redirect", EPOCH + 21,
                    Collections.singletonMap("mssql.connection.redirect.type", "tds_routing"));
            adapter.publish(
                    event(END, root + 1, root, root, PerformanceActivity.LOGIN_EXCHANGE, 10, 20, null, null, false));
            rootEnd(root, root == 1 ? null : new UnknownHostException());
        }
        List<SpanData> result = spans();
        assertEquals(2, result.size());
        SpanData root = named(result, "open");
        assertEquals(3, root.getEvents().size());
        assertEquals("mssql.driver.retry", root.getEvents().get(0).getName());
        assertEquals("mssql.driver.redirect", root.getEvents().get(1).getName());
        assertTrue(named(result, "login").getEvents().isEmpty());
    }

    @Test
    void endMetadataPropagatesAttemptAndAuthenticationWithoutRepeatingRootSettings() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).build();
        adapter.publish(ConnectionEventFixture.event(START, 1, 0, 1, PerformanceActivity.CONNECTION, EPOCH, 0, null,
                null, false, Collections.singletonMap("mssql.authentication.method", "unknown")));
        adapter.publish(event(START, 2, 1, 1, PerformanceActivity.CONNECTION_ATTEMPT, 10, 0, null, null, false));
        adapter.publish(event(START, 3, 2, 1, PerformanceActivity.LOGIN_EXCHANGE, 20, 0, null, null, false));
        adapter.publish(event(START, 4, 3, 1, PerformanceActivity.TOKEN_REQUEST, 30, 0, null, null, false));
        adapter.publish(ConnectionEventFixture.event(END, 4, 3, 1, PerformanceActivity.TOKEN_REQUEST, EPOCH + 30, 10,
                new LoginException(), "token_acquisition", true,
                Collections.singletonMap("mssql.authentication.token_source", "callback")));
        adapter.publish(event(END, 3, 2, 1, PerformanceActivity.LOGIN_EXCHANGE, 20, 30, new LoginException(),
                "token_acquisition", false));
        Map<String, Object> attemptAttrs = new HashMap<>();
        attemptAttrs.put("mssql.connection.attempt", 3L);
        attemptAttrs.put("mssql.connection.attempt_reason", "retry");
        attemptAttrs.put("mssql.connection.attempt_outcome", "failure");
        adapter.publish(ConnectionEventFixture.event(END, 2, 1, 1, PerformanceActivity.CONNECTION_ATTEMPT, EPOCH + 10,
                50, new LoginException(), "token_acquisition", false, attemptAttrs));
        Map<String, Object> rootAttrs = new HashMap<>();
        rootAttrs.put("mssql.authentication.method", "access_token_callback");
        rootAttrs.put("mssql.connection.retry_count", 2L);
        rootAttrs.put("mssql.connection.attempt_count", 3L);
        adapter.publish(ConnectionEventFixture.event(END, 1, 0, 1, PerformanceActivity.CONNECTION, EPOCH, 100,
                new LoginException(), "token_acquisition", false, rootAttrs));
        List<SpanData> result = spans();
        assertEquals(2L,
                named(result, "open").getAttributes().get(AttributeKey.longKey("mssql.connection.retry_count")));
        assertEquals("retry", named(result, "attempt").getAttributes()
                .get(AttributeKey.stringKey("mssql.connection.attempt_reason")));
        assertEquals("failure", named(result, "attempt").getAttributes()
                .get(AttributeKey.stringKey("mssql.connection.attempt_outcome")));
        for (String phase : new String[] {"attempt", "login", "token_acquisition"}) {
            assertEquals(3L,
                    named(result, phase).getAttributes().get(AttributeKey.longKey("mssql.connection.attempt")));
        }
        for (String phase : new String[] {"login", "token_acquisition"}) {
            assertEquals("access_token_callback",
                    named(result, phase).getAttributes().get(AttributeKey.stringKey("mssql.authentication.method")));
        }
        assertNull(named(result, "attempt").getAttributes().get(AttributeKey.stringKey("mssql.authentication.method")));
        assertEquals("callback", named(result, "token_acquisition").getAttributes()
                .get(AttributeKey.stringKey("mssql.authentication.token_source")));
    }

    @Test
    void pendingLimitExpirationAndCloseReleaseState() throws Exception {
        AtomicLong clock = new AtomicLong();
        OpenTelemetryConnectionCallback.Builder builder = setup(Sampler.alwaysOn(), null).maxPendingOpens(1)
                .maxOpenAge(Duration.ofSeconds(1));
        builder.nanoClock = clock::get;
        adapter = builder.build();
        rootStart(1);
        rootStart(20); // evicts oldest without retaining a tombstone
        drainIngestion();
        assertEquals(1, adapter.pendingOpenCount());
        assertEquals(1, adapter.droppedOpenCount());
        clock.set(TimeUnit.SECONDS.toNanos(2));
        adapter.expirePending();
        assertEquals(0, adapter.pendingOpenCount());
        assertEquals(2, adapter.droppedOpenCount());
        rootStart(40);
        adapter.close();
        rootEnd(40, new UnknownHostException());
        rootStart(60);
        assertEquals(0, adapter.pendingOpenCount());
        assertTrue(spans().isEmpty());
    }

    @Test
    void queueIsBoundedAndPublishNeverInvokesSdk() throws Exception {
        BlockingProcessor blocker = new BlockingProcessor();
        adapter = setup(Sampler.alwaysOn(), blocker).queueCapacity(1).build();
        try {
            rootStart(1);
            rootEnd(1, new UnknownHostException());
            assertTrue(blocker.entered.await(10, TimeUnit.SECONDS));
            rootStart(20);
            rootEnd(20, new UnknownHostException());
            rootStart(40);
            rootEnd(40, new UnknownHostException());
            drainIngestion();
            assertEquals(1, adapter.queuedOpenCount());
            assertEquals(1, adapter.droppedOpenCount());
        } finally {
            blocker.release.countDown();
        }
        assertEquals(2, spans().size());
    }

    @Test
    void closeDrainsWithoutClosingApplicationSdk() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).build();
        tree(1, new UnknownHostException(), false, false);
        adapter.close();
        assertEquals(4, exporter.getFinishedSpanItems().size());
        sdk.getTracer("app").spanBuilder("still-alive").startSpan().end();
        assertEquals(5, exporter.getFinishedSpanItems().size());
        adapter.close();
    }

    @Test
    void boundedCloseDiscardsQueueEvenWhenSdkIsBlocked() throws Exception {
        BlockingProcessor blocker = new BlockingProcessor();
        adapter = setup(Sampler.alwaysOn(), blocker).closeTimeout(Duration.ofMillis(10)).build();
        try {
            rootStart(1);
            rootEnd(1, new UnknownHostException());
            assertTrue(blocker.entered.await(10, TimeUnit.SECONDS));
            rootStart(20);
            rootEnd(20, new UnknownHostException());
            adapter.close();
            assertEquals(0, adapter.queuedOpenCount());
            assertEquals(0, adapter.pendingOpenCount());
        } finally {
            blocker.release.countDown();
        }
        assertTrue(adapter.awaitIdle(WAIT));
    }

    @Test
    void coreExceptionIdentitySurvivesAdapterAndWorkerFailure() throws Exception {
        SpanProcessor broken = new BlockingProcessor() {
            @Override
            public void onStart(Context parent, ReadWriteSpan span) {
                throw new IllegalStateException("SECRET SDK failure");
            }
        };
        adapter = setup(Sampler.alwaysOn(), broken).build();
        SQLServerDriver.registerPerformanceLogCallback(adapter);
        SQLException original = new SQLException("SECRET original");
        assertSame(original, assertThrows(SQLException.class, () -> ConnectionEventFixture.failThroughCore(original)));
        assertTrue(adapter.awaitIdle(WAIT));
        assertEquals(0, adapter.pendingOpenCount());
    }

    @Test
    void rejectsUnsafeValuesEvenForAllowedKeys() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).build();
        rootStart(1);
        Map<String, Object> attrs = new HashMap<>();
        attrs.put("mssql.connection.guid", "SECRET");
        attrs.put("mssql.connection.origin", "SECRET");
        attrs.put("mssql.authentication.method", "SECRET");
        attrs.put("mssql.connection.encrypt", "SECRET");
        attrs.put("mssql.connection.login_timeout", Double.NaN);
        attrs.put("mssql.connection.socket_timeout", -1.0);
        attrs.put("mssql.connection.attempt_count", "SECRET");
        PerformanceLogEvent end = ConnectionEventFixture.event(END, 1, 0, 1, PerformanceActivity.CONNECTION, EPOCH,
                1234, new UnknownHostException(), "dns", true, attrs);
        Map<String, Object> errors = new HashMap<>(end.getErrorAttributes());
        errors.put("mssql.error.code", "jdbc:R_SECRET");
        errors.put("exception.type", "com.SECRET.CustomException");
        errors.put("mssql.error.message", "SECRET password");
        errors.put("exception.stacktrace", "SECRET");
        errors.put("mssql.error.sql_state", "SECRET");
        adapter.publish(ConnectionEventFixture.withErrorAttributes(end, errors));
        SpanData root = named(spans(), "open");
        assertFalse(root.toString().contains("SECRET"));
        assertNull(root.getAttributes().get(AttributeKey.doubleKey("mssql.connection.login_timeout")));
        assertNull(root.getAttributes().get(AttributeKey.doubleKey("mssql.connection.socket_timeout")));
        assertEquals(3, root.getEvents().get(0).getAttributes().size());
    }

    @Test
    void userAgentRequiresExactExplicitApprovalAndOnlyAppearsOnRoot() throws Exception {
        String approved = "1|MS-JDBC|13.6.0.0|amd64|Windows|Windows 11 10.0|OpenJDK 64-Bit Server VM 21.0.4";
        adapter = setup(Sampler.alwaysOn(), null).approvedUserAgent(approved).build();
        Map<String, Object> attrs = Collections.singletonMap("mssql.driver.user_agent.original", approved);
        rootStart(1);
        adapter.publish(ConnectionEventFixture.event(START, 2, 1, 1, PerformanceActivity.CONNECTION_CONFIGURATION,
                EPOCH, 0, null, null, false, attrs));
        adapter.publish(ConnectionEventFixture.event(END, 2, 1, 1, PerformanceActivity.CONNECTION_CONFIGURATION, EPOCH,
                1234, new SQLException(), "configuration", true, attrs));
        adapter.publish(ConnectionEventFixture.event(END, 1, 0, 1, PerformanceActivity.CONNECTION, EPOCH, 2000,
                new SQLException(), "configuration", false, attrs));
        List<SpanData> result = spans();
        assertEquals(approved,
                named(result, "open").getAttributes().get(AttributeKey.stringKey("mssql.driver.user_agent.original")));
        assertNull(named(result, "configuration").getAttributes()
                .get(AttributeKey.stringKey("mssql.driver.user_agent.original")));
        assertThrows(IllegalArgumentException.class,
                () -> OpenTelemetryConnectionCallback.builder(sdk).approvedUserAgent("SECRET"));
        rootStart(20);
        rootEnd(20, new UnknownHostException());
        assertFalse(spans().toString().contains("SECRET"));
    }

    @Test
    void eventCapAndDuplicateBoundariesDoNotDuplicateFailure() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).maxEventsPerOpen(1).build();
        rootStart(1);
        rootStart(1);
        for (int i = 2; i <= 3; i++) {
            PerformanceLogEvent start = event(START, i, 1, 1, PerformanceActivity.DNS, i, 0, null, null, false);
            PerformanceLogEvent end = event(END, i, 1, 1, PerformanceActivity.DNS, i, 20, new UnknownHostException(),
                    "dns", true);
            adapter.publish(start);
            adapter.publish(start);
            adapter.publish(end);
            adapter.publish(end);
        }
        adapter.publish(
                event(END, 1, 0, 1, PerformanceActivity.CONNECTION, 0, 100, new UnknownHostException(), "dns", false));
        List<SpanData> result = spans();
        assertEquals(3, result.size());
        assertEquals(1, result.stream().mapToInt(s -> s.getEvents().size()).sum());
        assertEquals(1L,
                named(result, "open").getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_event_count")));
        // Retain the latest origin, not an intermediate retry's event.
        assertEquals(EPOCH + 23,
                result.stream().flatMap(s -> s.getEvents().stream()).findFirst().get().getEpochNanos());
    }

    @Test
    void orphanAndIncompleteChildrenAreNotInvented() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).build();
        rootEnd(1, new UnknownHostException()); // no captured entry context
        rootStart(20);
        adapter.publish(event(START, 21, 999, 20, PerformanceActivity.DNS, 10, 0, null, null, false));
        adapter.publish(
                event(END, 21, 999, 20, PerformanceActivity.DNS, 10, 50, new UnknownHostException(), "dns", true));
        adapter.publish(event(START, 22, 20, 20, PerformanceActivity.TLS, 10, 0, null, null, false));
        rootEnd(20, new UnknownHostException());
        List<SpanData> result = spans();
        assertEquals(1, result.size());
        assertEquals(true,
                named(result, "open").getAttributes().get(AttributeKey.booleanKey("mssql.telemetry.truncated")));
    }

    @Test
    void explicitRegistrationObservesRealDriverConfigurationFailure() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).approvedUserAgent(ConnectionEventFixture.driverUserAgent()).build();
        SQLServerDriver.registerPerformanceLogCallback(adapter);
        java.util.Properties properties = new java.util.Properties();
        properties.setProperty("portNumber", "invalid");
        assertThrows(SQLException.class, () -> new SQLServerDriver().connect("jdbc:sqlserver://", properties));
        List<SpanData> result = spans();
        assertEquals("configuration",
                named(result, "open").getAttributes().get(AttributeKey.stringKey("mssql.connection.failure_phase")));
        assertEquals(ConnectionEventFixture.driverUserAgent(),
                named(result, "open").getAttributes().get(AttributeKey.stringKey("mssql.driver.user_agent.original")));
        assertEquals(0L,
                named(result, "open").getAttributes().get(AttributeKey.longKey("mssql.connection.attempt_count")));
        assertEquals(1L, result.stream().flatMap(s -> s.getEvents().stream())
                .filter(e -> "mssql.driver.error".equals(e.getName())).count());
        assertEquals(1L, result.stream().flatMap(s -> s.getEvents().stream())
                .filter(e -> "mssql.driver.connection.retry_decision".equals(e.getName())).count());
    }

    @Test
    void realDriverDnsFailureExportsRootSettingsAndActualAttemptCount() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).approvedUserAgent(ConnectionEventFixture.driverUserAgent()).build();
        SQLServerDriver.registerPerformanceLogCallback(adapter);
        java.util.Properties properties = new java.util.Properties();
        // Invalid IPv6 syntax exercises the real resolver without contacting an external DNS server.
        properties.setProperty("serverName", "invalid:literal");
        properties.setProperty("portNumber", "1433");
        properties.setProperty("encrypt", "false");
        properties.setProperty("trustServerCertificate", "false");
        properties.setProperty("transparentNetworkIPResolution", "false");
        properties.setProperty("connectRetryCount", "0");
        properties.setProperty("loginTimeout", "3");
        properties.setProperty("socketTimeout", "1500");
        properties.setProperty("password", "SECRET");
        assertThrows(SQLException.class, () -> new SQLServerDriver().connect("jdbc:sqlserver://", properties));
        List<SpanData> result = spans();
        SpanData root = named(result, "open");
        SpanData attempt = named(result, "attempt");
        assertEquals(4, result.size());
        assertEquals(attempt.getSpanId(), named(result, "dns").getParentSpanId());
        assertFalse(result.stream().anyMatch(span -> span.getName().equals("mssql.driver.connection.socket_connect")));
        assertEquals("name_resolution", root.getAttributes().get(AttributeKey.stringKey("mssql.error.category")));
        assertEquals("dns", root.getAttributes().get(AttributeKey.stringKey("mssql.connection.failure_phase")));
        assertEquals(ConnectionEventFixture.driverUserAgent(),
                root.getAttributes().get(AttributeKey.stringKey("mssql.driver.user_agent.original")));
        assertEquals(1L, root.getAttributes().get(AttributeKey.longKey("mssql.connection.attempt_count")));
        assertEquals(1L, attempt.getAttributes().get(AttributeKey.longKey("mssql.connection.attempt")));
        assertNull(attempt.getAttributes().get(AttributeKey.stringKey("mssql.connection.client_connection_id")));
        assertEquals("false", root.getAttributes().get(AttributeKey.stringKey("mssql.connection.encrypt")));
        assertEquals(false,
                root.getAttributes().get(AttributeKey.booleanKey("mssql.connection.trust_server_certificate")));
        assertEquals(3.0, root.getAttributes().get(AttributeKey.doubleKey("mssql.connection.login_timeout")));
        assertEquals(1.5, root.getAttributes().get(AttributeKey.doubleKey("mssql.connection.socket_timeout")));
        assertEquals(0L, root.getAttributes().get(AttributeKey.longKey("mssql.connection.connect_retry_count")));
        assertEquals(root.getSpanId(), attempt.getParentSpanId());
        assertEquals(1, result.stream().flatMap(s -> s.getEvents().stream())
                .filter(e -> "mssql.driver.error".equals(e.getName())).count());
        assertEquals(1, named(result, "dns").getEvents().stream().filter(e -> "mssql.driver.error".equals(e.getName()))
                .count());
        for (SpanData span : result) {
            assertFalse(span.toString().contains("SECRET"));
            assertFalse(span.toString().contains("invalid:literal"));
            if (span != root) {
                assertNull(span.getAttributes().get(AttributeKey.stringKey("mssql.driver.user_agent.original")));
                assertNull(span.getAttributes().get(AttributeKey.stringKey("mssql.connection.guid")));
                assertNull(span.getAttributes().get(AttributeKey.longKey("mssql.connection.attempt_count")));
                assertNull(span.getAttributes().get(AttributeKey.stringKey("mssql.connection.encrypt")));
                assertNull(span.getAttributes().get(AttributeKey.doubleKey("mssql.connection.socket_timeout")));
            }
            if (span == root || span.getName().endsWith(".configuration")) {
                assertNull(span.getAttributes().get(AttributeKey.longKey("mssql.connection.attempt")));
            } else {
                assertEquals(1L, span.getAttributes().get(AttributeKey.longKey("mssql.connection.attempt")));
            }
        }
        assertEquals(0, adapter.pendingOpenCount());
        assertEquals(0, adapter.droppedOpenCount());
    }

    @Test
    void builderRejectsUnboundedOrInvalidLimits() {
        OpenTelemetryConnectionCallback.Builder builder = setup(Sampler.alwaysOn(), null);
        assertThrows(IllegalArgumentException.class, () -> builder.maxSpansPerOpen(0));
        assertThrows(IllegalArgumentException.class, () -> builder.maxEventsPerOpen(0));
        assertThrows(IllegalArgumentException.class, () -> builder.maxPendingOpens(0));
        assertThrows(IllegalArgumentException.class, () -> builder.queueCapacity(0));
        assertThrows(IllegalArgumentException.class, () -> builder.eventQueueCapacity(0));
        assertThrows(IllegalArgumentException.class, () -> builder.maxOpenAge(Duration.ZERO));
        assertThrows(IllegalArgumentException.class, () -> builder.closeTimeout(Duration.ofSeconds(-1)));
    }

    @Test
    void missingMetadataIsOmittedRatherThanInvented() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).build();
        adapter.publish(ConnectionEventFixture
                .withoutMetadata(event(START, 1, 0, 1, PerformanceActivity.CONNECTION, 0, 0, null, null, false)));
        adapter.publish(ConnectionEventFixture.withoutMetadata(
                event(END, 1, 0, 1, PerformanceActivity.CONNECTION, 0, 100, new SQLException("SECRET"), null, false)));
        SpanData root = named(spans(), "open");
        assertEquals(EPOCH, root.getStartEpochNanos());
        assertEquals(EPOCH + 100, root.getEndEpochNanos());
        assertEquals(StatusCode.ERROR, root.getStatus().getStatusCode());
        assertEquals("unknown", root.getAttributes().get(AttributeKey.stringKey("mssql.connection.failure_phase")));
        assertNull(root.getAttributes().get(AttributeKey.stringKey("mssql.connection.guid")));
        assertNull(root.getAttributes().get(AttributeKey.stringKey("mssql.connection.client_connection_id")));
        assertNull(root.getAttributes().get(AttributeKey.stringKey("mssql.driver.user_agent.original")));
        assertNull(root.getAttributes().get(AttributeKey.longKey("mssql.connection.attempt_count")));
        assertTrue(root.getEvents().isEmpty());
        assertFalse(root.toString().contains("SECRET"));
    }

    @Test
    void constructorDoesNotRegisterAndCloseDoesNotUnregisterOtherCallback() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).build();
        assertThrows(SQLException.class, () -> ConnectionEventFixture.failThroughCore(new SQLException()));
        assertTrue(spans().isEmpty());
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        SQLServerDriver.registerPerformanceLogCallback(new com.microsoft.sqlserver.jdbc.PerformanceLogCallback() {
            @Override
            public void publish(PerformanceLogEvent event) {
                calls.incrementAndGet();
            }

            @Override
            public void publish(PerformanceActivity activity, int connectionId, long duration, Exception exception) {}

            @Override
            public void publish(PerformanceActivity activity, int connectionId, int statementId, long duration,
                    Exception exception) {}
        });
        adapter.close();
        assertThrows(SQLException.class, () -> ConnectionEventFixture.failThroughCore(new SQLException()));
        assertEquals(2, calls.get());
    }

    @Test
    void workerContinuesAfterSdkFailureAndDoesNotCallSdkOnPublisher() throws Exception {
        java.util.concurrent.atomic.AtomicInteger calls = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicReference<Thread> sdkThread = new java.util.concurrent.atomic.AtomicReference<>();
        SpanProcessor intermittent = new BlockingProcessor() {
            @Override
            public void onStart(Context parent, ReadWriteSpan span) {
                sdkThread.set(Thread.currentThread());
                if (calls.getAndIncrement() == 0) {
                    throw new IllegalStateException("SECRET");
                }
            }
        };
        adapter = setup(Sampler.alwaysOn(), intermittent).build();
        rootStart(1);
        rootEnd(1, new UnknownHostException());
        assertTrue(adapter.awaitIdle(WAIT));
        assertEquals(1, adapter.droppedOpenCount());
        rootStart(20);
        rootEnd(20, new UnknownHostException());
        assertEquals(1, spans().size());
        assertNotSame(Thread.currentThread(), sdkThread.get());
    }

    @Test
    void expirationRunsAutomaticallyEvenWhenExporterIsBlocked() throws Exception {
        BlockingProcessor blocker = new BlockingProcessor();
        AtomicLong clock = new AtomicLong();
        OpenTelemetryConnectionCallback.Builder builder = setup(Sampler.alwaysOn(), blocker);
        builder.maxOpenAge(Duration.ofMillis(50));
        builder.nanoClock = clock::get;
        adapter = builder.build();
        try {
            rootStart(1);
            rootEnd(1, new UnknownHostException());
            assertTrue(blocker.entered.await(10, TimeUnit.SECONDS));
            rootStart(20);
            drainIngestion();
            assertEquals(1, adapter.pendingOpenCount());
            clock.set(TimeUnit.SECONDS.toNanos(1));
            long deadline = System.nanoTime() + WAIT.toNanos();
            while (adapter.pendingOpenCount() != 0 && System.nanoTime() < deadline) {
                Thread.sleep(10);
            }
            assertEquals(0, adapter.pendingOpenCount());
            assertEquals(1, adapter.droppedOpenCount());
            assertFalse(adapter.awaitIdle(Duration.ofMillis(10)));
        } finally {
            blocker.release.countDown();
        }
        assertEquals(1, spans().size());
    }

    @Test
    void mismatchedEndDoesNotOverwriteStartIdentityOrCreateParent() throws Exception {
        adapter = setup(Sampler.alwaysOn(), null).build();
        rootStart(1);
        adapter.publish(event(START, 2, 1, 1, PerformanceActivity.DNS, 10, 0, null, null, false));
        adapter.publish(
                event(END, 2, 999, 1, PerformanceActivity.DNS, 10, 50, new UnknownHostException(), "dns", true));
        adapter.publish(event(START, 3, 2, 1, PerformanceActivity.SOCKET_CONNECT, 20, 0, null, null, false));
        adapter.publish(event(END, 3, 2, 1, PerformanceActivity.SOCKET_CONNECT, 20, 30, null, null, false));
        rootEnd(1, new UnknownHostException());
        List<SpanData> result = spans();
        assertEquals(1, result.size());
        assertEquals(2L,
                named(result, "open").getAttributes().get(AttributeKey.longKey("mssql.telemetry.dropped_span_count")));
    }

    private static class BlockingProcessor implements SpanProcessor {
        final CountDownLatch entered = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        public void onStart(Context parent, ReadWriteSpan span) {
            entered.countDown();
            boolean interrupted = false;
            while (true) {
                try {
                    if (!release.await(10, TimeUnit.SECONDS)) {
                        throw new AssertionError("SDK test gate not released");
                    }
                    break;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public boolean isStartRequired() {
            return true;
        }

        @Override
        public void onEnd(ReadableSpan span) {}

        @Override
        public boolean isEndRequired() {
            return false;
        }

        @Override
        public CompletableResultCode shutdown() {
            return CompletableResultCode.ofSuccess();
        }

        @Override
        public CompletableResultCode forceFlush() {
            return CompletableResultCode.ofSuccess();
        }
    }
}
