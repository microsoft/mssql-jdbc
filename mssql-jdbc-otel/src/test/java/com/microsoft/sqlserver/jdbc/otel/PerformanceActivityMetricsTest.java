/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.PerformanceActivity;
import com.microsoft.sqlserver.jdbc.PerformanceLogCallback;
import com.microsoft.sqlserver.jdbc.PerformanceLogEvent;
import com.microsoft.sqlserver.jdbc.StatementType;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.MetricDataType;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;


class PerformanceActivityMetricsTest {
    private static final String SCOPE = "com.microsoft.sqlserver.jdbc";
    private static final String COUNT = "db.client.operation.count";
    private static final String DURATION = "db.client.operation.duration";
    private static final String ERROR = "db.client.operation.error.count";
    private static final String RETRY = "db.client.operation.retry.count";
    private static final String OUTCOME = "mssql.operation.outcome";
    private static final String PROTOCOL = "mssql.statement.protocol_operation";
    private static final List<Double> BUCKETS = Arrays.asList(0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01,
            0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0);
    private final InMemoryMetricReader reader = InMemoryMetricReader.create();
    private final InMemorySpanExporter spans = InMemorySpanExporter.create();
    private final OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(reader).build())
            .setTracerProvider(SdkTracerProvider.builder().addSpanProcessor(SimpleSpanProcessor.create(spans)).build())
            .build();
    private final PerformanceActivityMetrics metrics = new PerformanceActivityMetrics(sdk, SCOPE);
    private final PerformanceLogCallback callback = callback(null);

    @AfterEach
    void close() {
        sdk.close();
    }

    @Test
    void exactActivityCatalogAndConditionalDimensions() throws Exception {
        Map<PerformanceActivity, String> catalog = new LinkedHashMap<>();
        catalog.put(PerformanceActivity.CONNECTION, "connection.open");
        catalog.put(PerformanceActivity.PRELOGIN, "connection.prelogin_legacy");
        catalog.put(PerformanceActivity.CONNECTION_CONFIGURATION, "connection.configuration");
        catalog.put(PerformanceActivity.CONNECTION_ATTEMPT, "connection.attempt");
        catalog.put(PerformanceActivity.INSTANCE_DISCOVERY, "connection.instance_discovery");
        catalog.put(PerformanceActivity.DNS, "connection.dns");
        catalog.put(PerformanceActivity.SOCKET_CONNECT, "connection.socket_connect");
        catalog.put(PerformanceActivity.TLS, "connection.tls");
        catalog.put(PerformanceActivity.LOGIN_EXCHANGE, "connection.login");
        catalog.put(PerformanceActivity.TOKEN_REQUEST, "connection.token_request");
        catalog.put(PerformanceActivity.CONNECTION_INITIALIZE, "connection.initialize");
        catalog.put(PerformanceActivity.CONNECTION_REDIRECT, "connection.redirect");
        catalog.put(PerformanceActivity.STATEMENT_INVOCATION, "statement.execute");
        catalog.put(PerformanceActivity.STATEMENT_ATTEMPT, "statement.attempt");
        catalog.put(PerformanceActivity.STATEMENT_REQUEST_BUILD, "statement.request_build");
        catalog.put(PerformanceActivity.STATEMENT_FIRST_SERVER_RESPONSE, "statement.first_response");
        catalog.put(PerformanceActivity.STATEMENT_PREPARE, "statement.server_call.prepare");
        catalog.put(PerformanceActivity.STATEMENT_PREPEXEC, "statement.server_call.prepexec");
        catalog.put(PerformanceActivity.STATEMENT_EXECUTE, "statement.server_call.execute");
        assertEquals(PerformanceActivity.values().length - 2, catalog.size());
        for (PerformanceActivity activity : PerformanceActivity.values()) {
            Map<String, Object> metadata = values("mssql.authentication.method", "sql_password", "mssql.statement.type",
                    "prepared_statement", PROTOCOL, "sp_execute");
            // Kind must follow the activity, not the availability of an identifier or thread-local callback.
            metrics.record(event(activity, metadata, null, 1_000_000_000L), callback);
        }
        Collection<MetricData> data = reader.collectAllMetrics();
        assertEquals(2, data.size());
        MetricData count = metric(data, COUNT);
        assertEquals(catalog.size(), count.getLongSumData().getPoints().size());
        for (Map.Entry<PerformanceActivity, String> entry : catalog.entrySet()) {
            boolean statement = entry.getValue().startsWith("statement.");
            AttributesBuilderFixture expected = new AttributesBuilderFixture(entry.getValue(), statement);
            if (statement) {
                expected.put("mssql.statement.type", "prepared_statement");
                if (entry.getValue().startsWith("statement.server_call.")) {
                    expected.put(PROTOCOL, "sp_execute");
                }
            } else {
                expected.put("mssql.connection.auth_method", "sql_password");
            }
            Attributes attributes = expected.put(OUTCOME, "success").build();
            assertEquals(1, count.getLongSumData().getPoints().stream()
                    .filter(point -> point.getAttributes().equals(attributes)).count(), entry.getValue());
            HistogramPointData duration = metric(data, DURATION).getHistogramData().getPoints().stream()
                    .filter(point -> point.getAttributes().equals(attributes)).findFirst()
                    .orElseThrow(AssertionError::new);
            assertEquals(1, duration.getCount());
            assertEquals(1.0, duration.getSum());
        }
    }

    @Test
    void exactInstrumentsUnitsBucketsAndOutcomeAccounting() throws Exception {
        for (PerformanceActivity root : Arrays.asList(PerformanceActivity.CONNECTION,
                PerformanceActivity.STATEMENT_INVOCATION)) {
            String prefix = root == PerformanceActivity.CONNECTION ? "mssql.connection." : "mssql.statement.";
            for (String outcome : Arrays.asList("success", "failure", "timeout", "canceled")) {
                metrics.record(event(root,
                        values(prefix + "outcome", outcome, prefix + "attempt_count", 3L, prefix + "retry_count", 99L),
                        null, 250_000_000L), callback);
            }
        }
        Collection<MetricData> data = reader.collectAllMetrics();
        assertEquals(4, data.size());
        String[] names = {COUNT, DURATION, ERROR, RETRY};
        String[] units = {"{operation}", "s", "{error}", "{retry}"};
        for (int i = 0; i < names.length; i++) {
            MetricData metric = metric(data, names[i]);
            assertEquals(units[i], metric.getUnit());
            assertEquals(SCOPE, metric.getInstrumentationScopeInfo().getName());
            assertEquals(i == 1 ? MetricDataType.HISTOGRAM : MetricDataType.LONG_SUM, metric.getType());
            if (i != 1) {
                assertTrue(metric.getLongSumData().isMonotonic());
            }
        }
        assertEquals(8, total(metric(data, COUNT)));
        assertEquals(6, total(metric(data, ERROR)));
        assertEquals(16, total(metric(data, RETRY)));
        assertEquals(8, metric(data, COUNT).getLongSumData().getPoints().size());
        assertEquals(2, metric(data, ERROR).getLongSumData().getPoints().size());
        assertEquals(2, metric(data, RETRY).getLongSumData().getPoints().size());
        for (HistogramPointData point : metric(data, DURATION).getHistogramData().getPoints()) {
            assertEquals(BUCKETS, point.getBoundaries());
            assertEquals(1, point.getCount());
            assertEquals(0.25, point.getSum());
            assertEquals(1L, point.getCounts().get(10).longValue());
            assertEquals(1, point.getCounts().stream().mapToLong(Long::longValue).sum());
            assertEquals(1,
                    metric(data, COUNT).getLongSumData().getPoints().stream()
                            .filter(count -> count.getAttributes().equals(point.getAttributes())).findFirst()
                            .orElseThrow(AssertionError::new).getValue());
        }
        for (String name : Arrays.asList(ERROR, RETRY)) {
            metric(data, name).getLongSumData().getPoints().forEach(point -> {
                boolean statement = "statement"
                        .equals(point.getAttributes().get(AttributeKey.stringKey("mssql.operation.kind")));
                AttributesBuilderFixture expected = new AttributesBuilderFixture(
                        statement ? "statement.execute" : "connection.open", statement);
                expected.put(statement ? "mssql.statement.type" : "mssql.connection.auth_method",
                        statement ? "not_applicable" : "not_recorded");
                if (name.equals(RETRY)) {
                    expected.put("mssql.retry.reason", "not_applicable");
                }
                assertEquals(expected.build(), point.getAttributes());
            });
        }
    }

    @Test
    void retriesUseOnlyRootAttemptsActuallyBegun() throws Exception {
        for (PerformanceActivity activity : Arrays.asList(PerformanceActivity.CONNECTION,
                PerformanceActivity.STATEMENT_INVOCATION, PerformanceActivity.CONNECTION_ATTEMPT,
                PerformanceActivity.STATEMENT_ATTEMPT)) {
            String prefix = activity.name().startsWith("STATEMENT") ? "mssql.statement." : "mssql.connection.";
            for (Object attempts : Arrays.asList(-1L, Long.MIN_VALUE, 0L, 1L, 4L, "SECRET")) {
                metrics.record(event(activity, values(prefix + "attempt_count", attempts, prefix + "retry_count", 100L),
                        null, 0), callback);
            }
            metrics.record(event(activity, values(prefix + "retry_count", 100L), null, 0), callback);
        }
        assertEquals(6, total(metric(reader.collectAllMetrics(), RETRY)));
        assertEquals(2, metric(reader.collectAllMetrics(), RETRY).getLongSumData().getPoints().size());
    }

    @Test
    void retryReasonIsAClosedRegistryAndOnlyOnRetryCounter() throws Exception {
        String[] reasons = {"retry", "redirect", "failover", "configurable_retry", "invalid_handle_retry",
                "enclave_retry", "SECRET customer retry", "initial"};
        for (String reason : reasons) {
            metrics.record(
                    event(PerformanceActivity.CONNECTION,
                            values("mssql.connection.attempt_count", 2L, "mssql.retry.reason", reason), null, 1),
                    callback);
        }
        MetricData retry = metric(reader.collectAllMetrics(), RETRY);
        assertEquals(8, total(retry));
        assertEquals(7, retry.getLongSumData().getPoints().size());
        for (LongPointData point : retry.getLongSumData().getPoints()) {
            String reason = point.getAttributes().get(AttributeKey.stringKey("mssql.retry.reason"));
            assertTrue(Arrays.asList("retry", "redirect", "failover", "configurable_retry", "invalid_handle_retry",
                    "enclave_retry", "not_applicable").contains(reason));
            assertEquals("not_applicable".equals(reason) ? 2 : 1, point.getValue());
        }
        assertNull(metric(reader.collectAllMetrics(), COUNT).getLongSumData().getPoints().iterator().next()
                .getAttributes().get(AttributeKey.stringKey("mssql.retry.reason")));
    }

    @Test
    void boundedMetadataAndCallbackFallbacks() throws Exception {
        String[] protocols = {"direct_sql", "direct_sql_batch", "sp_executesql", "sp_prepexec", "sp_prepare",
                "sp_execute", "prepared_batch", "cursor_open", "cursor_prepexec", "cursor_execute", "bulk_copy"};
        for (String protocol : protocols) {
            metrics.record(event(PerformanceActivity.STATEMENT_EXECUTE, values(PROTOCOL, protocol), null, 1), callback);
        }
        metrics.record(event(PerformanceActivity.STATEMENT_EXECUTE, values(PROTOCOL, "SECRET"), null, 1), callback);
        MetricData count = metric(reader.collectAllMetrics(), COUNT);
        assertEquals(12, count.getLongSumData().getPoints().size());
        count.getLongSumData().getPoints()
                .forEach(point -> assertTrue(Arrays
                        .asList("direct_sql", "direct_sql_batch", "sp_executesql", "sp_prepexec", "sp_prepare",
                                "sp_execute", "prepared_batch", "cursor_open", "cursor_prepexec", "cursor_execute",
                                "bulk_copy", "not_applicable")
                        .contains(point.getAttributes().get(AttributeKey.stringKey(PROTOCOL)))));
        for (StatementType type : StatementType.values()) {
            metrics.record(event(PerformanceActivity.STATEMENT_INVOCATION, Collections.emptyMap(), null, 1),
                    callback(type));
            String expectedType = type == StatementType.STATEMENT ? "statement"
                                                                  : type == StatementType.PREPARED_STATEMENT ? "prepared_statement"
                                                                                                             : "callable_statement";
            assertTrue(metric(reader.collectAllMetrics(), COUNT).getLongSumData().getPoints().stream()
                    .anyMatch(point -> expectedType
                            .equals(point.getAttributes().get(AttributeKey.stringKey("mssql.statement.type")))));
        }
        metrics.record(
                event(PerformanceActivity.STATEMENT_INVOCATION, values("mssql.statement.type", "SECRET"), null, 1),
                callback);
        String[] authMethods = {"sql_password", "entra_password", "entra_integrated", "managed_identity",
                "service_principal_secret", "service_principal_certificate", "interactive", "default_credential",
                "access_token_callback", "access_token", "integrated_kerberos", "integrated_ntlm", "integrated_native",
                "unknown", "SECRET"};
        for (String auth : authMethods) {
            metrics.record(event(PerformanceActivity.CONNECTION, values("mssql.authentication.method", auth), null, 1),
                    callback);
            String expectedAuth = "SECRET".equals(auth) ? "not_recorded" : auth;
            assertTrue(metric(reader.collectAllMetrics(), COUNT).getLongSumData().getPoints().stream()
                    .anyMatch(point -> expectedAuth.equals(
                            point.getAttributes().get(AttributeKey.stringKey("mssql.connection.auth_method")))));
        }
        count = metric(reader.collectAllMetrics(), COUNT);
        assertEquals(31, count.getLongSumData().getPoints().size());
        assertTrue(count.getLongSumData().getPoints().stream().anyMatch(point -> "not_recorded"
                .equals(point.getAttributes().get(AttributeKey.stringKey("mssql.connection.auth_method")))));
    }

    @Test
    void privateValuesNeverChangeSeriesOrEnterMetricAttributes() throws Exception {
        for (int i = 0; i < 1000; i++) {
            String secret = "SECRET" + i;
            Map<String, Object> metadata = values("mssql.connection.guid", secret, "db.query.text", secret,
                    "server.address", secret, "error.type", secret, "mssql.error.category", secret,
                    "mssql.statement.type", secret, PROTOCOL, secret, "mssql.authentication.method", secret,
                    "mssql.retry.reason", secret, "mssql.telemetry.schema.version", secret,
                    "mssql.connection.attempt_count", 2L);
            metrics.record(event(PerformanceActivity.CONNECTION, metadata, new Exception(secret), 1), callback);
        }
        for (MetricData metric : reader.collectAllMetrics()) {
            assertFalse(metric.toString().contains("SECRET"));
            if (metric.getType() == MetricDataType.HISTOGRAM) {
                assertEquals(1, metric.getHistogramData().getPoints().size());
            } else {
                assertEquals(1, metric.getLongSumData().getPoints().size());
            }
        }
    }

    @Test
    void fallbackOutcomesUseClassificationWithoutInspectingExceptions() throws Exception {
        Exception hostile = new Exception() {
            private static final long serialVersionUID = 1L;

            @Override
            public String getMessage() {
                throw new AssertionError("Exception must not be inspected");
            }
        };
        for (String category : Arrays.asList("timeout", "canceled", "SECRET")) {
            metrics.record(event(PerformanceActivity.DNS, values("mssql.error.category", category), hostile, 1)
                    .withoutException(), callback);
        }
        metrics.record(event(PerformanceActivity.DNS, Collections.emptyMap(), null, 1), callback);
        assertEquals(4, metric(reader.collectAllMetrics(), COUNT).getLongSumData().getPoints().size());
        assertEquals(3, total(metric(reader.collectAllMetrics(), ERROR)));
    }

    @Test
    void attemptOutcomesAndChildCloseBoundariesAreNotRelabeledByRootFailure() throws Exception {
        for (PerformanceActivity attempt : Arrays.asList(PerformanceActivity.CONNECTION_ATTEMPT,
                PerformanceActivity.STATEMENT_ATTEMPT)) {
            String prefix = attempt == PerformanceActivity.CONNECTION_ATTEMPT ? "mssql.connection."
                                                                              : "mssql.statement.";
            for (String outcome : Arrays.asList("success", "failure", "timeout", "canceled")) {
                metrics.record(event(attempt, values(prefix + "attempt_outcome", outcome), null, 0), callback);
            }
        }
        metrics.record(event(PerformanceActivity.DNS, Collections.emptyMap(), null, 0), callback);
        metrics.record(event(PerformanceActivity.CONNECTION, values("mssql.connection.outcome", "failure"),
                new Exception("SECRET"), 0), callback);
        Collection<MetricData> data = reader.collectAllMetrics();
        assertEquals(10, total(metric(data, COUNT)));
        assertEquals(7, total(metric(data, ERROR)));
        assertTrue(metric(data, COUNT).getLongSumData().getPoints().stream()
                .anyMatch(point -> "connection.dns"
                        .equals(point.getAttributes().get(AttributeKey.stringKey("mssql.performance.activity")))
                        && "success".equals(point.getAttributes().get(AttributeKey.stringKey(OUTCOME)))));
        assertFalse(data.stream().anyMatch(metric -> RETRY.equals(metric.getName())));
    }

    @Test
    void invalidBoundariesAndLegacyWrappersAreIgnored() throws Exception {
        metrics.record(event(PerformanceActivity.CONNECTION, Collections.emptyMap(), null, -1), callback);
        metrics.record(
                event(PerformanceLogEvent.Type.START, PerformanceActivity.CONNECTION, Collections.emptyMap(), null, 0),
                callback);
        metrics.record(event(PerformanceActivity.LOGIN, Collections.emptyMap(), null, 1), callback);
        metrics.record(event(PerformanceActivity.TOKEN_ACQUISITION, Collections.emptyMap(), null, 1), callback);
        assertTrue(reader.collectAllMetrics().isEmpty());
    }

    @Test
    void sharedInstrumentsSupportConcurrentRecording() throws Exception {
        PerformanceLogEvent event = event(PerformanceActivity.CONNECTION, Collections.emptyMap(), null, 1);
        ExecutorService workers = Executors.newFixedThreadPool(4);
        try {
            List<Future<?>> tasks = new ArrayList<>();
            for (int i = 0; i < 4; i++) {
                tasks.add(workers.submit(() -> {
                    for (int j = 0; j < 250; j++) {
                        metrics.record(event, callback);
                    }
                }));
            }
            for (Future<?> task : tasks) {
                task.get(10, TimeUnit.SECONDS);
            }
            assertEquals(1000, total(metric(reader.collectAllMetrics(), COUNT)));
            assertEquals(1000, metric(reader.collectAllMetrics(), DURATION).getHistogramData().getPoints().iterator()
                    .next().getCount());
        } finally {
            workers.shutdownNow();
        }
    }

    @Test
    void lazyInitializationFailureDoesNotSuppressSpanAdmissionAndCanRecover() throws Exception {
        AtomicInteger meterCalls = new AtomicInteger();
        OpenTelemetry wrapped = (OpenTelemetry) Proxy.newProxyInstance(OpenTelemetry.class.getClassLoader(),
                new Class<?>[] {OpenTelemetry.class}, (proxy, method, args) -> {
                    if ("getMeter".equals(method.getName()) && meterCalls.incrementAndGet() == 1) {
                        throw new IllegalStateException("SECRET SDK initialization failure");
                    }
                    try {
                        return method.invoke(sdk, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        try (OpenTelemetryConnectionCallback adapter = OpenTelemetryConnectionCallback.builder(wrapped)
                .performanceMetricsEnabled(true).build()) {
            assertEquals(0, meterCalls.get());
            adapter.publish(event(PerformanceLogEvent.Type.START, PerformanceActivity.CONNECTION,
                    Collections.emptyMap(), null, 0));
            PerformanceLogEvent failure = event(PerformanceActivity.CONNECTION,
                    values("mssql.connection.outcome", "failure", "mssql.error.category", "network_connectivity"),
                    new Exception("SECRET"), 1);
            assertDoesNotThrow(() -> adapter.publish(failure));
            assertTrue(adapter.awaitIdle(Duration.ofSeconds(10)));
            assertEquals(1, spans.getFinishedSpanItems().size());
            assertTrue(reader.collectAllMetrics().isEmpty());
            assertDoesNotThrow(
                    () -> adapter.publish(event(PerformanceActivity.CONNECTION, Collections.emptyMap(), null, 1)));
            assertEquals(1, total(metric(reader.collectAllMetrics(), COUNT)));
            assertEquals(2, meterCalls.get());
        }
    }

    private static PerformanceLogCallback callback(StatementType type) {
        return new PerformanceLogCallback() {
            @Override
            public void publish(PerformanceActivity activity, int connection, long duration, Exception exception) {}

            @Override
            public void publish(PerformanceActivity activity, int connection, int statement, long duration,
                    Exception exception) {}

            @Override
            public StatementType getCurrentStatementType() {
                return type;
            }

            @Override
            public String getCurrentUserSql() {
                throw new AssertionError("Metrics must not request SQL");
            }
        };
    }

    private static PerformanceLogEvent event(PerformanceActivity activity, Map<String, Object> metadata,
            Exception failure, long duration) throws Exception {
        return event(PerformanceLogEvent.Type.END, activity, metadata, failure, duration);
    }

    private static PerformanceLogEvent event(PerformanceLogEvent.Type type, PerformanceActivity activity,
            Map<String, Object> metadata, Exception failure, long duration) throws Exception {
        Constructor<PerformanceLogEvent> constructor = PerformanceLogEvent.class.getDeclaredConstructor(
                PerformanceLogEvent.Type.class, long.class, long.class, long.class, int.class, int.class,
                PerformanceActivity.class, long.class, long.class, long.class, Exception.class, String.class, Map.class,
                Map.class, List.class);
        constructor.setAccessible(true);
        return constructor.newInstance(type, 1L, 0L, 1L, 123, 0, activity, 1_700_000_000_000_000_000L,
                1_700_000_000_000_000_000L + duration, duration, failure, failure == null ? null : "dns", metadata,
                Collections.emptyMap(), Collections.emptyList());
    }

    private static Map<String, Object> values(Object... pairs) {
        Map<String, Object> result = new HashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            result.put((String) pairs[i], pairs[i + 1]);
        }
        return result;
    }

    private static MetricData metric(Collection<MetricData> data, String name) {
        return data.stream().filter(value -> name.equals(value.getName())).findFirst().orElseThrow(AssertionError::new);
    }

    private static long total(MetricData metric) {
        return metric.getLongSumData().getPoints().stream().mapToLong(LongPointData::getValue).sum();
    }

    private static final class AttributesBuilderFixture {
        private final io.opentelemetry.api.common.AttributesBuilder builder = Attributes.builder();

        AttributesBuilderFixture(String activity, boolean statement) {
            builder.put("mssql.performance.activity", activity)
                    .put("mssql.operation.kind", statement ? "statement" : "connection")
                    .put("mssql.telemetry.schema.version", "1.0");
        }

        AttributesBuilderFixture put(String key, String value) {
            builder.put(key, value);
            return this;
        }

        Attributes build() {
            return builder.build();
        }
    }
}
