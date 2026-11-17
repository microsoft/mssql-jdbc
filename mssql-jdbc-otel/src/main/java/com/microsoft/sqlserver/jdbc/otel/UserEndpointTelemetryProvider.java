/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.ServiceConfigurationError;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.microsoft.sqlserver.jdbc.PerformanceActivity;
import com.microsoft.sqlserver.jdbc.PerformanceLogCallback;
import com.microsoft.sqlserver.jdbc.PerformanceLogEvent;
import com.microsoft.sqlserver.jdbc.UserOtelTelemetryProvider;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanBuilder;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.ReadableSpan;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import io.opentelemetry.sdk.trace.samplers.Sampler;


/**
 * Automatic user-owned OTLP/HTTP routing, independent of the global SDK and the legacy performance callback.
 * SDK construction, tree filtering, token providers and export run exclusively on daemon workers. Producer callbacks
 * only capture exception-free immutable events and the caller's SpanContext, and offer them to a bounded lossy queue.
 * Admission loss invalidates pending and queued older trees; incomplete trees are never replayed as complete traces.
 * Closing a SQL connection does not close a shared transport. Idle transports expire after five minutes.
 */
public final class UserEndpointTelemetryProvider implements UserOtelTelemetryProvider {
    private static final int MAX_QUEUE = 8192;
    private static final int MAX_ENDPOINTS = 32;
    private static final int MAX_TREES = 256;
    private static final int MAX_TREE_EVENTS = 512;
    private static final long IDLE_NANOS = TimeUnit.MINUTES.toNanos(5);
    private static final long TREE_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static final Duration EXPORT_TIMEOUT = Duration.ofSeconds(3);
    private static final AtomicLong ROUTES = new AtomicLong();
    private static volatile Registry registry;

    /** Public no-op constructor for ServiceLoader; no SDK or background work is initialized here. */
    public UserEndpointTelemetryProvider() {}

    @Override
    public PerformanceLogCallback create(Properties options) {
        try {
            Options route = new Options(options);
            Registry current = registry;
            if (current == null || current.closed) {
                synchronized (UserEndpointTelemetryProvider.class) {
                    current = registry;
                    if (current == null || current.closed) {
                        current = new Registry();
                        registry = current;
                    }
                }
            }
            return new Callback(current, route);
        } catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
            return null;
        }
    }

    /**
     * Drains queued events and requests bounded SDK flushes. Completion is not a collector delivery acknowledgement.
     * This explicit application-lifecycle method may wait; JDBC callbacks never do.
    *
     * @param timeout
     *        total nonnegative wait budget
     * @return whether the drain and SDK flush completed within the budget
     */
    public static boolean forceFlush(Duration timeout) {
        Objects.requireNonNull(timeout, "timeout");
        long nanos = timeout.toNanos();
        if (nanos < 0) {
            throw new IllegalArgumentException("Invalid telemetry wait duration");
        }
        Registry current = registry;
        if (current == null) {
            return true;
        }
        CompletableFuture<Boolean> completion = new CompletableFuture<>();
        long deadline = System.nanoTime() + nanos;
        if (!current.offer(() -> {
            boolean success = true;
            for (Pipeline pipeline : current.endpoints.values()) {
                success &= pipeline.flush(deadline);
            }
            completion.complete(success);
        })) {
            return false;
        }
        try {
            return completion.get(nanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException e) {
            return false;
        }
    }

    /**
     * Stops accepting events, discards pending trees, and asynchronously shuts down owned SDKs and credentials.
     * Safe to call repeatedly. Existing callbacks become no-ops; new connections can start a fresh registry.
     */
    public static synchronized void shutdown() {
        Registry current = registry;
        registry = null;
        if (current != null) {
            current.admission.lock();
            try {
                current.closed = true;
            } finally {
                current.admission.unlock();
            }
            current.worker.interrupt();
        }
    }

    private static final class Options {
        final Key key;
        final int level;
        final BigDecimal connectionThreshold;
        final BigDecimal statementThreshold;
        final long id = ROUTES.incrementAndGet();

        Options(Properties options) {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            key = new Key(options.getProperty("otelEndpoint"), options.getProperty("otelAccessTokenCallback"),
                    loader == null ? UserEndpointTelemetryProvider.class.getClassLoader() : loader);
            level = Integer.parseInt(options.getProperty("otelLogLevel", "0"));
            if (level < 0 || level > 2 || key.endpoint == null) {
                throw new IllegalArgumentException();
            }
            connectionThreshold = threshold(options, "otelConnectionSpanDurationThreshold");
            statementThreshold = threshold(options, "otelStatementSpanDurationThreshold");
        }

        private static BigDecimal threshold(Properties options, String key) {
            String value = options.getProperty(key);
            if (value == null) {
                return null;
            }
            // Core normalization uses stripTrailingZeros().toString(), which can produce scientific notation.
            BigDecimal seconds = new BigDecimal(value.trim());
            BigDecimal nanos = seconds.scaleByPowerOfTen(9);
            if (seconds.signum() < 0 || nanos.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) > 0) {
                throw new IllegalArgumentException();
            }
            return nanos;
        }

        boolean traces(PerformanceLogEvent root) {
            BigDecimal threshold = root.getStatementId() == 0 ? connectionThreshold : statementThreshold;
            return level != 0 && (level == 2 || root.hasException())
                    && (threshold == null || BigDecimal.valueOf(root.getDurationNanos()).compareTo(threshold) > 0);
        }
    }

    private static final class Key {
        final String endpoint;
        final String callback;
        final ClassLoader loader;

        Key(String endpoint, String callback, ClassLoader loader) {
            this.endpoint = endpoint;
            this.callback = callback;
            this.loader = loader;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Key)) {
                return false;
            }
            Key key = (Key) other;
            return loader == key.loader && Objects.equals(endpoint, key.endpoint)
                    && Objects.equals(callback, key.callback);
        }

        @Override
        public int hashCode() {
            return Objects.hash(endpoint, callback, System.identityHashCode(loader));
        }
    }

    private static final class Callback implements PerformanceLogCallback {
        final Registry owner;
        final Options options;

        Callback(Registry owner, Options options) {
            this.owner = owner;
            this.options = options;
        }

        @Override
        public void publish(PerformanceLogEvent event) {
            if (owner.closed || event == null) {
                return;
            }
            try {
                long epoch = owner.lossEpoch.get();
                PerformanceLogEvent snapshot = event.withoutException();
                boolean rootStart = event.getParentScopeId() == 0 && event.getType() == PerformanceLogEvent.Type.START;
                SpanContext parent = rootStart ? Span.current().getSpanContext() : SpanContext.getInvalid();
                owner.offer(() -> owner.accept(options, snapshot, parent, epoch));
            } catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
                owner.lossEpoch.incrementAndGet();
                // Optional telemetry must never affect the SQL operation or log event/configuration values.
            }
        }

        @Override
        public void publish(PerformanceActivity activity, int connectionId, long duration, Exception exception) {}

        @Override
        public void publish(PerformanceActivity activity, int connectionId, int statementId, long duration,
                Exception exception) {}
    }

    private static final class Registry implements Runnable {
        final ConcurrentLinkedQueue<Runnable> queue = new ConcurrentLinkedQueue<>();
        final AtomicInteger queued = new AtomicInteger();
        final ReentrantLock admission = new ReentrantLock();
        final AtomicLong lossEpoch = new AtomicLong();
        // Only the ingestion worker reads/writes the registry, SDK references and buffered trees.
        final Map<Key, Pipeline> endpoints = new LinkedHashMap<>();
        final Map<String, Tree> trees = new LinkedHashMap<>();
        final Thread worker;
        volatile boolean closed;
        long observedEpoch;

        Registry() {
            worker = new Thread(this, "mssql-user-otel-ingest");
            worker.setDaemon(true);
            worker.setContextClassLoader(UserEndpointTelemetryProvider.class.getClassLoader());
            worker.start();
        }

        boolean offer(Runnable message) {
            // Producers never wait, and shutdown cannot race a reservation or leave uncounted queued work.
            if (!admission.tryLock()) {
                lossEpoch.incrementAndGet();
                return false;
            }
            try {
                if (closed) {
                    return false;
                }
                if (queued.get() >= MAX_QUEUE) {
                    lossEpoch.incrementAndGet();
                    return false;
                }
                queued.incrementAndGet();
                queue.offer(message);
                return true;
            } finally {
                admission.unlock();
            }
        }

        @Override
        public void run() {
            try {
                while (!closed) {
                    Runnable next = queue.poll();
                    if (next != null) {
                        queued.decrementAndGet();
                        try {
                            next.run();
                        } catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
                            lossEpoch.incrementAndGet();
                            // Drop instrumentation failures without retaining or logging their diagnostics.
                        }
                    } else {
                        expire();
                        try {
                            Thread.sleep(10);
                        } catch (InterruptedException e) {
                            if (closed) {
                                break;
                            }
                        }
                    }
                }
            } finally {
                admission.lock();
                try {
                    closed = true;
                    queue.clear();
                    queued.set(0);
                } finally {
                    admission.unlock();
                }
                trees.clear();
                for (Pipeline pipeline : endpoints.values()) {
                    pipeline.close();
                }
                endpoints.clear();
            }
        }

        void accept(Options options, PerformanceLogEvent event, SpanContext parent, long epoch) {
            Pipeline pipeline = endpoints.get(options.key);
            if (pipeline == null) {
                expire();
                if (endpoints.size() >= MAX_ENDPOINTS) {
                    return; // Bound retained endpoint/callback/class-loader state; never evict an active exporter.
                }
                pipeline = new Pipeline(options.key);
                endpoints.put(options.key, pipeline);
            }
            pipeline.lastUsed = System.nanoTime();
            if (event.getType() == PerformanceLogEvent.Type.END) {
                pipeline.metrics(event);
            }
            if (options.level == 0) {
                return;
            }
            long currentEpoch = lossEpoch.get();
            if (observedEpoch != currentEpoch) {
                trees.clear();
                observedEpoch = currentEpoch;
            }
            if (epoch != currentEpoch) {
                return; // Metrics above are independent of span admission/tree completeness.
            }
            // Connection and statement scope allocators are separate; neither IDs nor callbacks are global tree keys.
            String key = options.id + ":" + (event.getStatementId() == 0 ? "c:" : "s:") + event.getRootScopeId();
            if (event.getType() == PerformanceLogEvent.Type.START && event.getParentScopeId() == 0) {
                if (trees.size() < MAX_TREES) {
                    trees.put(key, new Tree(parent));
                }
            }
            Tree tree = trees.get(key);
            if (tree == null) {
                return;
            }
            if (event.getType() == PerformanceLogEvent.Type.START) {
                if (tree.starts.size() >= MAX_TREE_EVENTS || tree.starts.containsKey(event.getScopeId())
                        || (event.getParentScopeId() != 0 && (!tree.starts.containsKey(event.getParentScopeId())
                                || tree.ended.contains(event.getParentScopeId())))) {
                    tree.overflow = true;
                } else {
                    tree.starts.put(event.getScopeId(), event);
                }
            }
            if (event.getType() == PerformanceLogEvent.Type.END) {
                PerformanceLogEvent start = tree.starts.get(event.getScopeId());
                if (start == null || start.getParentScopeId() != event.getParentScopeId()
                        || start.getStartEpochNanos() != event.getStartEpochNanos()
                        || start.getActivity() != event.getActivity() || !tree.ended.add(event.getScopeId())) {
                    tree.overflow = true;
                }
                if (tree.events.size() < MAX_TREE_EVENTS) {
                    tree.events.add(event);
                } else {
                    tree.overflow = true;
                }
                if (event.getParentScopeId() == 0) {
                    trees.remove(key);
                    if (!tree.overflow && tree.starts.size() == tree.events.size() && epoch == lossEpoch.get()
                            && options.traces(event)) {
                        pipeline.traces(tree, event);
                    }
                }
            }
        }

        void expire() {
            long now = System.nanoTime();
            trees.values().removeIf(tree -> now - tree.created > TREE_NANOS);
            Iterator<Pipeline> iterator = endpoints.values().iterator();
            while (iterator.hasNext()) {
                Pipeline pipeline = iterator.next();
                if (now - pipeline.lastUsed > IDLE_NANOS) {
                    iterator.remove();
                    pipeline.close();
                }
            }
        }
    }

    private static final class Tree {
        final SpanContext parent;
        final long created = System.nanoTime();
        final List<PerformanceLogEvent> events = new ArrayList<>();
        final Map<Long, PerformanceLogEvent> starts = new LinkedHashMap<>();
        final Set<Long> ended = new HashSet<>();
        boolean overflow;

        Tree(SpanContext parent) {
            this.parent = parent;
        }
    }

    private static final class Pipeline {
        final OtlpConfiguration configuration;
        final SdkTracerProvider traces;
        final TreeExporter exports;
        final SdkMeterProvider metrics;
        final Tracer tracer;
        final LongCounter count;
        final LongCounter errors;
        final DoubleHistogram duration;
        long lastUsed = System.nanoTime();

        Pipeline(Key key) {
            Properties options = new Properties();
            options.setProperty("otelEndpoint", key.endpoint);
            if (key.callback != null) {
                options.setProperty("otelAccessTokenCallback", key.callback);
            }
            configuration = OtlpConfiguration.forUserEndpoint(options, key.loader, System::currentTimeMillis);
            SdkTracerProvider traceProvider = null;
            SdkMeterProvider meterProvider = null;
            SpanExporter spanExporter = null;
            MetricExporter metricExporter = null;
            try {
                Resource resource = Resource
                        .create(Attributes.of(AttributeKey.stringKey("service.name"), "mssql-jdbc"));
                spanExporter = new SafeSpans(OtlpHttpSpanExporter.builder().setEndpoint(configuration.endpoint)
                        .setHeaders(configuration).setConnectTimeout(EXPORT_TIMEOUT).setTimeout(EXPORT_TIMEOUT)
                        .setRetryPolicy(null).setMeterProvider(MeterProvider.noop()).build());
                // Admission must be atomic for a whole tree. A BatchSpanProcessor drops individual spans at capacity.
                traceProvider = SdkTracerProvider.builder().setResource(resource).setSampler(Sampler.alwaysOn())
                        .build();
                metricExporter = new SafeMetrics(OtlpHttpMetricExporter.builder()
                        .setEndpoint(configuration.metricsEndpoint).setHeaders(configuration)
                        .setConnectTimeout(EXPORT_TIMEOUT).setTimeout(EXPORT_TIMEOUT).setRetryPolicy(null).build());
                meterProvider = SdkMeterProvider.builder().setResource(resource).registerMetricReader(
                        PeriodicMetricReader.builder(metricExporter).setInterval(Duration.ofSeconds(60)).build())
                        .build();
                traces = traceProvider;
                exports = new TreeExporter(spanExporter);
                metrics = meterProvider;
                tracer = traces.get("com.microsoft.sqlserver.jdbc.user-endpoint");
                Meter meter = metrics.get("com.microsoft.sqlserver.jdbc.user-endpoint");
                count = meter.counterBuilder("db.client.operation.count").setUnit("{operation}").build();
                errors = meter.counterBuilder("db.client.operation.error.count").setUnit("{error}").build();
                duration = meter.histogramBuilder("db.client.operation.duration").setUnit("s").build();
            } catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
                configuration.close();
                if (traceProvider != null) {
                    traceProvider.shutdown();
                }
                if (spanExporter != null) {
                    spanExporter.shutdown();
                }
                if (meterProvider != null) {
                    meterProvider.shutdown();
                } else if (metricExporter != null) {
                    metricExporter.shutdown();
                }
                throw new IllegalStateException("Telemetry transport unavailable");
            }
        }

        void metrics(PerformanceLogEvent event) {
            Attributes labels = Attributes.builder().put("mssql.activity", event.getActivity().name())
                    .put("mssql.activity.type", event.getStatementId() == 0 ? "connection" : "statement").build();
            count.add(1, labels, Context.root());
            duration.record(Math.max(0, event.getDurationNanos()) / 1_000_000_000.0, labels, Context.root());
            if (event.hasException()) {
                errors.add(1, labels, Context.root());
            }
        }

        void traces(Tree tree, PerformanceLogEvent root) {
            // Validate closure before creating any SDK spans, not after a partial replay has already escaped.
            Set<Long> available = new HashSet<>();
            available.add(root.getScopeId());
            for (int i = tree.events.size() - 2; i >= 0; i--) {
                PerformanceLogEvent event = tree.events.get(i);
                if (!available.contains(event.getParentScopeId()) || !available.add(event.getScopeId())) {
                    return;
                }
            }
            Map<Long, Span> spans = new LinkedHashMap<>();
            // END order is children first. Build in reverse so parent context exists before its descendants.
            List<PerformanceLogEvent> ordered = new ArrayList<>(tree.events);
            Collections.reverse(ordered);
            boolean statement = root.getStatementId() != 0;
            String prefix = statement ? "mssql.driver.statement." : "mssql.driver.connection.";
            for (PerformanceLogEvent event : ordered) {
                boolean isRoot = event.getParentScopeId() == 0;
                Span parentSpan = spans.get(event.getParentScopeId());
                Context parent = Context.root().with(isRoot ? Span.wrap(tree.parent) : parentSpan);
                String phase;
                if (isRoot) {
                    phase = statement ? "execute" : "open";
                } else if (statement) {
                    phase = StatementAttributePolicy.phase(event.getPhase());
                } else {
                    phase = "attempt".equals(event.getPhase()) ? "attempt"
                                                               : ConnectionAttributePolicy.phase(event.getPhase());
                }
                Map<String, Object> metadata = event.getAttributes();
                // This automatic route never opts into SQL capture, including masked SQL supplied by another adapter.
                if (statement && metadata.containsKey("db.query.text")) {
                    metadata = new LinkedHashMap<>(metadata);
                    metadata.remove("db.query.text");
                }
                Attributes attributes;
                if (statement) {
                    attributes = StatementAttributePolicy.span(metadata, isRoot,
                            event.getActivity() == PerformanceActivity.STATEMENT_ATTEMPT, phase);
                } else {
                    attributes = ConnectionAttributePolicy.span(metadata, isRoot,
                            event.getActivity() == PerformanceActivity.CONNECTION_ATTEMPT, phase, null);
                }
                SpanBuilder builder = tracer.spanBuilder(prefix + phase).setParent(parent)
                        .setSpanKind(isRoot ? SpanKind.CLIENT : SpanKind.INTERNAL)
                        .setStartTimestamp(event.getStartEpochNanos(), TimeUnit.NANOSECONDS)
                        .setAllAttributes(attributes).setAttribute("mssql.activity", event.getActivity().name());
                Span span = builder.startSpan();
                if (event.hasException()) {
                    span.setStatus(StatusCode.ERROR);
                    span.setAttribute("mssql.error.category",
                            statement ? StatementAttributePolicy.category(metadata.get("mssql.error.category"))
                                      : ConnectionAttributePolicy.category(metadata.get("mssql.error.category")));
                    span.setAttribute("error.type",
                            statement ? StatementAttributePolicy.errorType(metadata.get("error.type"))
                                      : ConnectionAttributePolicy.errorType(metadata.get("error.type")));
                    if (!event.getErrorAttributes().isEmpty()) {
                        // Only the origin carries detailed error evidence; enclosing failures retain summaries.
                        span.addEvent("mssql.driver.error",
                                statement ? StatementAttributePolicy.error(event.getErrorAttributes())
                                          : ConnectionAttributePolicy.error(event.getErrorAttributes()),
                                event.getEndEpochNanos(), TimeUnit.NANOSECONDS);
                    }
                }
                spans.put(event.getScopeId(), span);
            }
            List<SpanData> completed = new ArrayList<>(tree.events.size());
            for (PerformanceLogEvent event : tree.events) {
                Span span = spans.get(event.getScopeId());
                if (span != null) {
                    span.end(event.getEndEpochNanos(), TimeUnit.NANOSECONDS);
                    completed.add(((ReadableSpan) span).toSpanData());
                }
            }
            exports.offer(completed);
        }

        boolean flush(long deadline) {
            boolean result = exports.flush(deadline);
            return await(metrics.forceFlush(), deadline) && result;
        }

        void close() {
            configuration.close(); // Does not wait for a potentially blocked user token callback.
            exports.close();
            traces.shutdown();
            metrics.shutdown();
        }
    }

    private static final class TreeExporter {
        final SpanExporter delegate;
        final Semaphore spans = new Semaphore(2048);
        final ThreadPoolExecutor worker = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(64), task -> {
                    Thread thread = new Thread(task, "mssql-user-otel-export");
                    thread.setDaemon(true);
                    thread.setContextClassLoader(UserEndpointTelemetryProvider.class.getClassLoader());
                    return thread;
                });

        TreeExporter(SpanExporter delegate) {
            this.delegate = delegate;
        }

        void offer(List<SpanData> tree) {
            if (!spans.tryAcquire(tree.size())) {
                return; // Drop the entire tree, never its root or a suffix of children.
            }
            try {
                worker.execute(() -> {
                    try {
                        delegate.export(tree).join(EXPORT_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS);
                    } catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
                        // Keep optional transport diagnostics, including callback exceptions, off application logs.
                    } finally {
                        spans.release(tree.size());
                    }
                });
            } catch (RejectedExecutionException e) {
                spans.release(tree.size());
            }
        }

        boolean flush(long deadline) {
            CompletableResultCode completion = new CompletableResultCode();
            try {
                worker.execute(() -> {
                    try {
                        if (await(delegate.flush(), deadline)) {
                            completion.succeed();
                        } else {
                            completion.fail();
                        }
                    } catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
                        completion.fail();
                    }
                });
            } catch (RejectedExecutionException e) {
                return false;
            }
            return await(completion, deadline);
        }

        void close() {
            worker.shutdownNow();
            delegate.shutdown();
        }
    }

    private static boolean await(CompletableResultCode result, long deadline) {
        long remaining = deadline - System.nanoTime();
        if (remaining > 0) {
            result.join(remaining, TimeUnit.NANOSECONDS);
        }
        return result.isDone() && result.isSuccess();
    }

    private static final class SafeSpans implements SpanExporter {
        final SpanExporter delegate;

        SafeSpans(SpanExporter delegate) {
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

    private static final class SafeMetrics implements MetricExporter {
        final MetricExporter delegate;

        SafeMetrics(MetricExporter delegate) {
            this.delegate = delegate;
        }

        @Override
        public CompletableResultCode export(Collection<MetricData> metrics) {
            try {
                return delegate.export(metrics);
            } catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
                return CompletableResultCode.ofFailure();
            }
        }

        @Override
        public io.opentelemetry.sdk.metrics.data.AggregationTemporality getAggregationTemporality(
                io.opentelemetry.sdk.metrics.InstrumentType type) {
            return delegate.getAggregationTemporality(type);
        }

        @Override
        public io.opentelemetry.sdk.metrics.Aggregation getDefaultAggregation(
                io.opentelemetry.sdk.metrics.InstrumentType type) {
            return delegate.getDefaultAggregation(type);
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
