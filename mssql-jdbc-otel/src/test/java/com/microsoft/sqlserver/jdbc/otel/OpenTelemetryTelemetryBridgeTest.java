package com.microsoft.sqlserver.jdbc.otel;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.PerformanceActivity;
import com.microsoft.sqlserver.jdbc.StatementType;
import com.microsoft.sqlserver.jdbc.TelemetryEvent;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.PointData;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;

class OpenTelemetryTelemetryBridgeTest {

    @Test
    void publishesSpanAndAttributes() throws Exception {
        InMemorySpanExporter spanExporter = InMemorySpanExporter.create();
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(SimpleSpanProcessor.create(spanExporter))
                .build();

        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();

        OpenTelemetryTelemetryBridge bridge = new OpenTelemetryTelemetryBridge(openTelemetry);
        TelemetryEvent event = new TelemetryEvent(PerformanceActivity.STATEMENT_EXECUTE, 42, 7, 25L,
                new IllegalStateException("boom"), "SELECT 1", StatementType.PREPARED_STATEMENT,
                Collections.singletonMap("Authorization", "Bearer token"), "Bearer", "mssql.jdbc.query",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "corr-42", "ms",
                Collections.singletonMap("custom.attribute", "custom-value"));

        bridge.publish(event);

        List<SpanData> finishedSpans = spanExporter.getFinishedSpanItems();
        assertEquals(1, finishedSpans.size());

        SpanData span = finishedSpans.get(0);
        assertEquals("mssql.jdbc.query", span.getName());
        assertEquals(PerformanceActivity.STATEMENT_EXECUTE.name(),
                span.getAttributes().get(AttributeKey.stringKey("mssql.jdbc.activity")));
        assertEquals(42L, span.getAttributes().get(AttributeKey.longKey("mssql.jdbc.connection.id")));
        assertEquals(7L, span.getAttributes().get(AttributeKey.longKey("mssql.jdbc.statement.id")));
        assertEquals(25D, span.getAttributes().get(AttributeKey.doubleKey("mssql.jdbc.measurement.value")));
        assertEquals("ms", span.getAttributes().get(AttributeKey.stringKey("mssql.jdbc.measurement.unit")));
        assertEquals("Bearer", span.getAttributes().get(AttributeKey.stringKey("mssql.jdbc.auth.scheme")));
        assertEquals("custom-value", span.getAttributes().get(AttributeKey.stringKey("custom.attribute")));
        assertTrue(span.getAttributes().get(AttributeKey.stringKey("mssql.jdbc.correlation.id")).contains("corr-42"));
    }

    @Test
    void metricsCarryOnlyLowCardinalityAttributes() throws Exception {
        InMemoryMetricReader metricReader = InMemoryMetricReader.create();
        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .registerMetricReader(metricReader)
                .build();

        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .build();

        OpenTelemetryTelemetryBridge bridge = new OpenTelemetryTelemetryBridge(openTelemetry);
        TelemetryEvent event = new TelemetryEvent(PerformanceActivity.STATEMENT_EXECUTE, 42, 7, 25L,
                null, "SELECT 1", StatementType.PREPARED_STATEMENT,
                Collections.singletonMap("Authorization", "Bearer token"), "Bearer", "mssql.jdbc.query",
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "corr-42", "ms",
                Collections.singletonMap("mssql.jdbc.otel.mode", "PAAS"));

        bridge.publish(event);

        List<MetricData> metrics = List.copyOf(metricReader.collectAllMetrics());
        assertFalse(metrics.isEmpty(), "expected at least one metric to be exported");

        for (MetricData metric : metrics) {
            for (PointData point : metric.getData().getPoints()) {
                Attributes attrs = point.getAttributes();

                // Low-cardinality dimensions are present.
                assertEquals(PerformanceActivity.STATEMENT_EXECUTE.name(),
                        attrs.get(AttributeKey.stringKey("mssql.jdbc.activity")),
                        metric.getName() + " should keep the bounded activity dimension");
                assertEquals(StatementType.PREPARED_STATEMENT.name(),
                        attrs.get(AttributeKey.stringKey("mssql.jdbc.statement.type")),
                        metric.getName() + " should keep the bounded statement type dimension");

                // High-cardinality dimensions are excluded so series count stays bounded.
                assertNullAttribute(attrs, AttributeKey.longKey("mssql.jdbc.connection.id"), metric.getName());
                assertNullAttribute(attrs, AttributeKey.longKey("mssql.jdbc.statement.id"), metric.getName());
                assertNullAttribute(attrs, AttributeKey.doubleKey("mssql.jdbc.measurement.value"), metric.getName());
                assertNullAttribute(attrs, AttributeKey.stringKey("mssql.jdbc.sql"), metric.getName());
                assertNullAttribute(attrs, AttributeKey.stringKey("mssql.jdbc.correlation.id"), metric.getName());
                assertNullAttribute(attrs, AttributeKey.stringKey("mssql.jdbc.trace.parent"), metric.getName());
            }
        }
    }

    private static void assertNullAttribute(Attributes attrs, AttributeKey<?> key, String metricName) {
        assertTrue(attrs.get(key) == null,
                metricName + " must not carry high-cardinality attribute '" + key.getKey() + "'");
    }
}
