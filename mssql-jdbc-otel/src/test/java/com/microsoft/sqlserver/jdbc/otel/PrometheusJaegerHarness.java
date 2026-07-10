package com.microsoft.sqlserver.jdbc.otel;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import com.microsoft.sqlserver.jdbc.PerformanceActivity;
import com.microsoft.sqlserver.jdbc.StatementType;
import com.microsoft.sqlserver.jdbc.TelemetryEvent;

import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

public final class PrometheusJaegerHarness {
    private PrometheusJaegerHarness() {}

    public static void main(String[] args) throws Exception {
        AtomicLong publishedEvents = new AtomicLong();
        AtomicLong totalDurationMs = new AtomicLong();
        AtomicLong errorCount = new AtomicLong();
        HttpServer prometheusServer = HttpServer.create(new InetSocketAddress("127.0.0.1", 9090), 0);
        int prometheusPort = prometheusServer.getAddress().getPort();
        prometheusServer.createContext("/metrics", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                long count = publishedEvents.get();
                long durationTotal = totalDurationMs.get();
                double averageDuration = count == 0 ? 0.0d : durationTotal / (double) count;
                String body = "# TYPE mssql_jdbc_operation_count counter\n"
                        + "mssql_jdbc_operation_count{activity=\"LOAD_TEST\"} " + count + "\n"
                        + "# TYPE mssql_jdbc_operation_duration_ms gauge\n"
                        + "mssql_jdbc_operation_duration_ms{activity=\"LOAD_TEST\"} "
                        + String.format(Locale.US, "%.2f", averageDuration) + "\n"
                        + "# TYPE mssql_jdbc_operation_errors_total counter\n"
                        + "mssql_jdbc_operation_errors_total{activity=\"LOAD_TEST\"} " + errorCount.get() + "\n";
                byte[] payload = body.getBytes(StandardCharsets.UTF_8);
                exchange.getResponseHeaders().add("Content-Type", "text/plain; version=0.0.4");
                exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                exchange.sendResponseHeaders(200, payload.length);
                try (OutputStream outputStream = exchange.getResponseBody()) {
                    outputStream.write(payload);
                }
            }
        });
        prometheusServer.setExecutor(Executors.newSingleThreadExecutor());
        prometheusServer.start();
        System.out.println("PROMETHEUS_ENDPOINT=http://127.0.0.1:" + prometheusPort + "/metrics");

        SdkMeterProvider meterProvider = SdkMeterProvider.builder().build();

        OtlpHttpSpanExporter exporter = OtlpHttpSpanExporter.builder()
                .setEndpoint("http://127.0.0.1:4318/v1/traces")
                .build();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
                .build();

        OpenTelemetrySdk openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setMeterProvider(meterProvider)
                .build();

        OpenTelemetryTelemetryBridge bridge = new OpenTelemetryTelemetryBridge(openTelemetry);

        for (int i = 0; i < 250; i++) {
            PerformanceActivity activity;
            if (i % 5 == 0) {
                activity = PerformanceActivity.CONNECTION;
            } else if (i % 5 == 1) {
                activity = PerformanceActivity.LOGIN;
            } else if (i % 5 == 2) {
                activity = PerformanceActivity.STATEMENT_PREPARE;
            } else if (i % 5 == 3) {
                activity = PerformanceActivity.STATEMENT_PREPEXEC;
            } else {
                activity = PerformanceActivity.STATEMENT_EXECUTE;
            }

            StatementType statementType;
            if (i % 3 == 0) {
                statementType = StatementType.STATEMENT;
            } else if (i % 3 == 1) {
                statementType = StatementType.PREPARED_STATEMENT;
            } else {
                statementType = StatementType.CALLABLE_STATEMENT;
            }

            String sql = i % 2 == 0 ? "SELECT 1" : "SELECT * FROM sys.objects WHERE 1 = 1";
            long duration = 25L + (i % 40);
            TelemetryEvent event = new TelemetryEvent(activity, 100 + i, 1000 + i, duration,
                    i % 19 == 0 ? new IllegalStateException("load-test-sample") : null, sql, statementType,
                    Collections.singletonMap("Authorization", "Bearer token"), "Bearer", "mssql.jdbc.query",
                    "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "corr-" + i);
            bridge.publish(event);
            publishedEvents.incrementAndGet();
            totalDurationMs.addAndGet(duration);
            if (event.getException() != null) {
                errorCount.incrementAndGet();
            }
        }

        tracerProvider.forceFlush();
        meterProvider.forceFlush();

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest metricsRequest = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + prometheusPort + "/metrics"))
                .GET()
                .build();
        HttpResponse<String> metricsResponse = client.send(metricsRequest, HttpResponse.BodyHandlers.ofString());
        System.out.println("PROMETHEUS_STATUS=" + metricsResponse.statusCode());
        System.out.println(metricsResponse.body().contains("mssql_jdbc_operation_count") ? "PROMETHEUS_METRIC_PRESENT"
                : "PROMETHEUS_METRIC_MISSING");
        System.out.println("POSTED_EVENTS=" + publishedEvents.get());
        System.out.println("Harness is running. Press Ctrl+C to stop.");

        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
        } finally {
            prometheusServer.stop(0);
            tracerProvider.close();
            meterProvider.close();
            openTelemetry.close();
        }
    }
}
