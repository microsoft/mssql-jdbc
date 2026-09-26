/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import java.time.Duration;
import java.util.Collection;
import java.util.Objects;
import java.util.Properties;
import java.util.ServiceConfigurationError;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.TracerProvider;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;


/**
 * Optional, owned OTLP/HTTP transport for the failure-only connection callback. No global SDK, meter provider,
 * shutdown hook, JDBC discovery, or callback registration is created. Register {@link #getCallback()} explicitly
 * with {@code SQLServerDriver.registerPerformanceLogCallback} before the first SQL call, and unregister before close.
 * Successes and legacy statement callbacks are ignored by the existing adapter; metrics remain disabled.
 *
 * <p>
 * Configuration is programmatic (not a JDBC URL). Supported properties:
 * <ul>
 * <li>{@code otelEndpoint}: required HTTPS endpoint; base paths and terminal /v1/metrics or /v1/traces are normalized
 * to /v1/traces. Userinfo, query and fragment are rejected. Default JVM TLS trust is used, never JDBC trust settings.</li>
 * <li>{@code otelAllowInsecureLocalEndpoint=true}: permits HTTP only for localhost, 127.0.0.1 or [::1].</li>
 * <li>{@code otelAllowInsecureDevelopmentEndpoint=true}: permits HTTP for a single-label container DNS hostname
 * only. This is NOT a security boundary or private-address verification; use only on an isolated, trusted local
 * development network. Bearers and telemetry travel in cleartext. Never enable for production.</li>
 * <li>{@code otelServiceName}: defaults to mssql-jdbc-connection-demo; the only resource attribute.</li>
 * <li>{@code otelArmResourceId}: optional x-ms-arm-resource-id request header, never a span/resource attribute.</li>
 * <li>{@code otelHeaders}: optional comma-separated name=value pairs (no escaping); validated case-insensitively,
 * with duplicate, authentication, forwarding, transport and reserved header overrides rejected.</li>
 * <li>{@code otelAccessTokenCallbackClass}: public no-arg SQLServerAccessTokenCallback provider; requires actual
 * {@code otelTokenScope} and {@code otelTokenAuthority} values. They are passed unchanged, not inferred from endpoint.</li>
 * <li>{@code otelBearerToken}: optional raw or Bearer-prefixed static token supplied by the caller, for example from
 * an environment variable. No environment is read here. A configured callback takes precedence, with no fallback to
 * this token on failure. Static tokens cannot be refreshed or expiry-checked; use a callback for expiring tokens.</li>
 * <li>{@code otelApprovedUserAgent}: optional privacy-approved version-1 driver value; only an exact match from a
 * driver event may be exported. No runtime UA is inferred and this is not an HTTP User-Agent override.</li>
 * </ul>
 *
 * <p>
 * Token providers run only on the batch export worker and must implement their own network deadlines. Refresh is
 * 20% of remaining lifetime early (at most 60 seconds); failures drop batches and back off one second. There is no
 * SQL-token reuse or shared credential cache. A stuck provider cannot be forcibly terminated, but waits on this
 * object remain finite. The adapter and batch processor have independent bounded, lossy queues.
 *
 * <p>
 * OTel 1.41's default HTTP sender follows redirects; this public exporter API cannot disable them. OkHttp removes
 * Authorization across origins, but custom/ARM headers can still follow redirects, and same-origin Authorization
 * is retained. Configure a trusted final collector URL without redirects. The upstream exporter also owns transport
 * error logging (including collector response diagnostics); configure its JUL logging policy at the application
 * boundary if the collector may return sensitive text. This module does not change process-wide logging settings.
 */
public final class OtlpConnectionTelemetry implements AutoCloseable {
    private static final Duration EXPORT_TIMEOUT = Duration.ofSeconds(3);
    private static final Duration WAIT_BUDGET = Duration.ofSeconds(5);
    private final OtlpConfiguration configuration;
    private final SdkTracerProvider provider;
    private final OpenTelemetryConnectionCallback callback;
    private final AtomicBoolean closed = new AtomicBoolean();

    private OtlpConnectionTelemetry(OtlpConfiguration configuration, SdkTracerProvider provider,
            OpenTelemetryConnectionCallback callback) {
        this.configuration = configuration;
        this.provider = provider;
        this.callback = callback;
    }

    /**
     * Creates an independent traces-only pipeline with a 2048-span batch queue, batches of 128, 200ms schedule,
     * 3-second connect/export timeouts and a 5-second processor export wait. No token is acquired at construction.
     *
     * @param configuration
     *        programmatic properties described on this class; read once, not retained or subsequently mutated
     * @return an owned, unregistered telemetry pipeline
     * @throws IllegalArgumentException
     *         if configuration is invalid (errors do not include configuration values)
     */
    public static OtlpConnectionTelemetry create(Properties configuration) {
        OtlpConfiguration config = new OtlpConfiguration(configuration);
        SpanExporter exporter = null;
        BatchSpanProcessor processor = null;
        SdkTracerProvider provider = null;
        try {
            exporter = new SafeExporter(OtlpHttpSpanExporter.builder().setEndpoint(config.endpoint).setHeaders(config)
                    .setConnectTimeout(EXPORT_TIMEOUT).setTimeout(EXPORT_TIMEOUT).setRetryPolicy(null)
                    .setMeterProvider(MeterProvider.noop()).build());
            Resource resource = Resource.create(Attributes.builder().put("service.name", config.serviceName).build());
            processor = BatchSpanProcessor.builder(exporter).setMaxQueueSize(2048).setMaxExportBatchSize(128)
                    .setScheduleDelay(Duration.ofMillis(200)).setExporterTimeout(WAIT_BUDGET).build();
            provider = SdkTracerProvider.builder().setResource(Resource.empty().merge(resource))
                    .addSpanProcessor(processor).build();
            final SdkTracerProvider ownedProvider = provider;
            // OpenTelemetrySdk.builder().build() also creates default meter/logger SDKs. Use only the trace SDK.
            OpenTelemetry tracesOnly = new OpenTelemetry() {
                @Override
                public TracerProvider getTracerProvider() {
                    return ownedProvider;
                }

                @Override
                public ContextPropagators getPropagators() {
                    return ContextPropagators.noop();
                }
            };
            OpenTelemetryConnectionCallback.Builder builder = OpenTelemetryConnectionCallback.builder(tracesOnly)
                    .metricsEnabled(false).closeTimeout(WAIT_BUDGET);
            if (config.approvedUserAgent != null) {
                builder.approvedUserAgent(config.approvedUserAgent);
            }
            return new OtlpConnectionTelemetry(config, provider, builder.build());
        } catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
            config.close();
            if (provider != null) {
                provider.shutdown();
            } else if (processor != null) {
                processor.shutdown();
            } else if (exporter != null) {
                exporter.shutdown();
            }
            throw new IllegalStateException("Unable to create telemetry transport");
        }
    }

    /** @return the failure-only callback, to register explicitly before SQL calls and unregister before close */
    public OpenTelemetryConnectionCallback getCallback() {
        return callback;
    }

    /**
     * Waits for adapter ingestion and SDK handoff, not network export. Call forceFlush afterwards if needed.
     *
     * @param timeout
     *        nonnegative wait budget
     * @return whether the adapter is idle
     * @throws InterruptedException
     *         if interrupted while waiting
     */
    public boolean awaitIdle(Duration timeout) throws InterruptedException {
        return callback.awaitIdle(timeout);
    }

    /**
     * Bounded wait for the owned trace processor to flush. Does not drain the adapter or flush another SDK.
     * A completed SDK flush is not a delivery acknowledgement: batches may have been dropped or export may fail.
     *
     * @param timeout
     *        nonnegative wait budget within the nanosecond range
     * @return whether the SDK flush completed successfully within the budget; false after close
     */
    public boolean forceFlush(Duration timeout) {
        long nanos = nanos(timeout);
        return !closed.get() && waitFor(provider.forceFlush(), nanos);
    }

    private static long nanos(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        try {
            long value = timeout.toNanos();
            if (value < 0) {
                throw new IllegalArgumentException("Invalid telemetry wait duration");
            }
            return value;
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Invalid telemetry wait duration");
        }
    }

    private static boolean waitFor(CompletableResultCode result, long nanos) {
        if (nanos > 0) {
            result.join(nanos, TimeUnit.NANOSECONDS);
        }
        return result.isDone() && result.isSuccess();
    }

    /**
     * Idempotently stops/drains the adapter, then flushes and shuts down the owned tracer provider. Each stage waits
     * at most five seconds (15 seconds total, excluding scheduling). Does not unregister callbacks: the caller must
     * do that first. Timed-out work is lossy. A blocked application token provider may retain one daemon worker until
     * it returns; after close its result cannot start a new authenticated request.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                callback.close();
                waitFor(provider.forceFlush(), WAIT_BUDGET.toNanos());
            } finally {
                configuration.close();
                waitFor(provider.shutdown(), WAIT_BUDGET.toNanos());
            }
        }
    }

    /** A supplier exception escapes the 1.41 HTTP sender before enqueue. Convert it to a failed batch, not a log. */
    private static final class SafeExporter implements SpanExporter {
        private final SpanExporter delegate;

        SafeExporter(SpanExporter delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableResultCode export(Collection<SpanData> spans) {
            try {
                return delegate.export(spans);
            } catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
                return CompletableResultCode.ofFailure();
            }
        }

        @Override
        public CompletableResultCode flush() {
            return delegate.flush();
        }

        @Override
        public CompletableResultCode shutdown() {
            return delegate.shutdown();
        }
    }
}
