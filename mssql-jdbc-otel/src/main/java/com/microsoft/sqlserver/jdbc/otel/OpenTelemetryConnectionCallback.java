/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceConfigurationError;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;

import com.microsoft.sqlserver.jdbc.PerformanceActivity;
import com.microsoft.sqlserver.jdbc.PerformanceLogCallback;
import com.microsoft.sqlserver.jdbc.PerformanceLogEvent;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;


/**
 * Optional failure-only connection telemetry. Construction does not register a callback: explicitly call
 * {@code SQLServerDriver.registerPerformanceLogCallback(adapter)} and unregister it before closing the adapter.
 * Legacy performance callbacks are deliberately ignored. Only sanitized scalar snapshots are retained, never
 * exceptions, connections, SQL text or credentials. The application owns the supplied OpenTelemetry instance,
 * sampling, processors, exporters and their flush/shutdown lifecycle; this adapter never closes that instance.
 *
 * <p>
 * JDBC callbacks only capture the root caller's SpanContext, project immutable exception-free events and attempt
 * nonblocking admission to a bounded event queue. An ingestion daemon constructs trees and applies attribute policy;
 * a separate export daemon hands completed failed trees to the application's SDK with original timestamps.
 * No SDK span/metric operation or tree processing runs on a JDBC callback thread. Admission contention or event
 * overflow advances a loss epoch: queued older events and pending partial trees are conservatively discarded.
 * Consequently unrelated concurrent opens may also be lost, but missing boundaries do not fabricate complete trees.
 * Complete-tree queue overload drops whole trees. Per-open limits evict completed older spans before admitting new branches.
 * Optional counters use two constant-space pending deltas independent of the span queue; SDK instrument construction
 * failures are retried at most once per 100 milliseconds while spans continue to be processed.
 * Active retained ancestors are never evicted to admit descendants. If a path cannot fit, its error is attached to
 * the nearest retained owner (or root) with the original phase and timestamp. Missing-scope records are also bounded;
 * once their ancestry expires, only root fallback and recent-boundary deduplication can be guaranteed.
 * A constant-space count of forgotten unfinished scopes prevents double-counting drops on well-formed END unwinds.
 * After identity eviction, an unobserved or duplicate END cannot be distinguished from one of those forgotten scopes.
 * Each open retains at most {@code maxSpansPerOpen} span records, the same number of missing-scope records, and
 * {@code maxEventsPerOpen} events. Each record contains only a fixed allowlist of bounded scalars. Queued opens
 * release missing-scope records. The pending, queued and single in-flight open budgets are independent.
 * Native SDK span IDs are allocated during delayed replay, not at phase START; this is not live phase context
 * propagation into identity-provider instrumentation. No application thread-local context is changed.
 * A separate daemon expires incomplete opens even if an application processor blocks the export worker.
 */
public final class OpenTelemetryConnectionCallback implements PerformanceLogCallback, AutoCloseable {
    private static final String SCOPE = "com.microsoft.sqlserver.jdbc";
    private final Object lock = new Object();
    // Producers only try this lock; no consumer processing or application hook runs while holding it.
    private final ReentrantLock admission = new ReentrantLock();
    private final ConcurrentLinkedQueue<Envelope> ingress = new ConcurrentLinkedQueue<>();
    private final Semaphore eventSlots;
    private final AtomicLong ingressOutstanding = new AtomicLong();
    private final AtomicLong lossEpoch = new AtomicLong();
    private final AtomicLong droppedEvents = new AtomicLong();
    private final OpenTelemetry telemetry;
    private final int maxPendingOpens;
    private final int maxSpansPerOpen;
    private final int maxEventsPerOpen;
    private final int queueCapacity;
    private final long maxOpenAgeNanos;
    private final long closeTimeoutNanos;
    private final LongSupplier nanoClock;
    private final String approvedUserAgent;
    private final boolean metricsEnabled;
    private final Map<Long, Open> pending = new LinkedHashMap<>();
    private final Deque<Open> queue = new ArrayDeque<>();
    private final Thread worker;
    private final Thread ingestion;
    private final ScheduledExecutorService expiry;
    private volatile boolean closed;
    private volatile boolean ingestionDone;
    private boolean active;
    private long observedEpoch;
    private volatile boolean abort;
    private long droppedOpens;
    // Lock-protected constant-space deltas, independent of span queue capacity and application sampling.
    private long pendingFailures;
    private long pendingTimeouts;
    // Worker-confined SDK objects, initialized only for the first failed open.
    private Tracer tracer;
    private Counters counters;

    private OpenTelemetryConnectionCallback(Builder builder) {
        telemetry = builder.telemetry;
        maxPendingOpens = builder.maxPendingOpens;
        maxSpansPerOpen = builder.maxSpansPerOpen;
        maxEventsPerOpen = builder.maxEventsPerOpen;
        queueCapacity = builder.queueCapacity;
        eventSlots = new Semaphore(builder.eventQueueCapacity);
        maxOpenAgeNanos = builder.maxOpenAge.toNanos();
        closeTimeoutNanos = builder.closeTimeout.toNanos();
        nanoClock = builder.nanoClock;
        approvedUserAgent = builder.approvedUserAgent;
        metricsEnabled = builder.metricsEnabled;
        worker = daemon(this::work, "mssql-jdbc-otel-export");
        ingestion = daemon(this::ingest, "mssql-jdbc-otel-ingest");
        expiry = Executors.newSingleThreadScheduledExecutor(task -> daemon(task, "mssql-jdbc-otel-expiry"));
        long interval = Math.min(maxOpenAgeNanos, TimeUnit.SECONDS.toNanos(1));
        expiry.scheduleWithFixedDelay(this::expirePending, interval, interval, TimeUnit.NANOSECONDS);
        worker.start();
        ingestion.start();
    }

    private static Thread daemon(Runnable task, String name) {
        Thread thread = new Thread(task, name);
        thread.setDaemon(true);
        // Do not retain the constructing application's context class loader on a long-lived worker.
        thread.setContextClassLoader(OpenTelemetryConnectionCallback.class.getClassLoader());
        return thread;
    }

    /**
     * Creates an unregistered builder without consulting the global SDK.
     * 
     * @param telemetry
     *        application-owned OpenTelemetry API instance
     * @return a builder with finite defaults and metrics disabled
     */
    public static Builder builder(OpenTelemetry telemetry) {
        return new Builder(telemetry);
    }

    /** Bounded retention configuration. All capacities must be positive. */
    public static final class Builder {
        private final OpenTelemetry telemetry;
        private int maxPendingOpens = 256;
        private int maxSpansPerOpen = 128;
        private int maxEventsPerOpen = 256;
        private int queueCapacity = 64;
        private int eventQueueCapacity = 4096;
        private Duration maxOpenAge = Duration.ofMinutes(5);
        private Duration closeTimeout = Duration.ofSeconds(5);
        private boolean metricsEnabled;
        private String approvedUserAgent;
        LongSupplier nanoClock = System::nanoTime;

        private Builder(OpenTelemetry telemetry) {
            this.telemetry = Objects.requireNonNull(telemetry, "telemetry");
        }

        /**
         * @param value
         *        maximum incomplete opens (default 256); oldest is evicted on overflow
         * @return this builder
         */
        public Builder maxPendingOpens(int value) {
            maxPendingOpens = positive(value);
            return this;
        }

        /**
         * @param value
         *        maximum spans per open including root (default 128)
         * @return this builder
         */
        public Builder maxSpansPerOpen(int value) {
            maxSpansPerOpen = positive(value);
            return this;
        }

        /**
         * @param value
         *        maximum events per open (default 256); latest origin error is reserved
         * @return this builder
         */
        public Builder maxEventsPerOpen(int value) {
            maxEventsPerOpen = positive(value);
            return this;
        }

        /**
         * @param value
         *        maximum queued trees, excluding the one in flight (default 64)
         * @return this builder
         */
        public Builder queueCapacity(int value) {
            queueCapacity = positive(value);
            return this;
        }

        /**
         * Sets the raw boundary queue limit, separate from the completed-tree {@link #queueCapacity(int)} limit.
         * Admission never waits: overflow or concurrent-producer contention can discard events and invalidate
         * unrelated pending trees. There is no lossless-delivery guarantee.
         *
         * @param value
         *        maximum queued event snapshots, excluding one in-flight ingestion event (default 4096)
         * @return this builder
         */
        public Builder eventQueueCapacity(int value) {
            eventQueueCapacity = positive(value);
            return this;
        }

        /**
         * @param value
         *        positive retention age (default five minutes)
         * @return this builder
         */
        public Builder maxOpenAge(Duration value) {
            validateDuration(value, false);
            maxOpenAge = value;
            return this;
        }

        /**
         * @param value
         *        nonnegative close wait budget (default five seconds)
         * @return this builder
         */
        public Builder closeTimeout(Duration value) {
            validateDuration(value, true);
            closeTimeout = value;
            return this;
        }

        /**
         * Enables attribute-free counters for accepted terminal failed root ENDs, independently of span queue drops,
         * trace SDK failures and application sampling. Two bounded pending deltas are drained by the worker, not by
         * JDBC callback threads. Transactional instrument initialization is retried with a bounded frequency without
         * losing pending deltas. These are not total driver failure counts: roots expired/evicted before END and
         * roots with no observed START or invalidated by event loss are excluded. Close timeout, numeric saturation at Long.MAX_VALUE, or SDK
         * recording failures can undercount. A throwing add is not retried because it may already have recorded the
         * delta. Metrics failures never suppress spans. The supplied SDK remains application-owned.
         * 
         * @param value
         *        whether to enable these observed-root counters (default false)
         * @return this builder
         */
        public Builder metricsEnabled(boolean value) {
            metricsEnabled = value;
            return this;
        }

        /**
         * Opts in to one exact, privacy-reviewed driver user agent. No runtime text is discovered or fabricated.
         * 
         * @param value
         *        approved version-1 seven-field driver value, at most 512 ASCII characters
         * @return this builder
         */
        public Builder approvedUserAgent(String value) {
            if (!ConnectionAttributePolicy.validUserAgent(value)) {
                throw new IllegalArgumentException("Expected a bounded version-1 driver user agent");
            }
            approvedUserAgent = value;
            return this;
        }

        /** @return an unregistered adapter; close it when no longer needed */
        public OpenTelemetryConnectionCallback build() {
            return new OpenTelemetryConnectionCallback(this);
        }

        private static int positive(int value) {
            if (value <= 0) {
                throw new IllegalArgumentException("Capacity must be positive");
            }
            return value;
        }
    }

    private static long validateDuration(Duration value, boolean allowZero) {
        Objects.requireNonNull(value, "duration");
        try {
            long nanos = value.toNanos();
            if (nanos < 0 || (!allowZero && nanos == 0)) {
                throw new IllegalArgumentException("Invalid duration");
            }
            return nanos;
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Duration exceeds nanosecond range", e);
        }
    }

    @Override
    public void publish(PerformanceActivity activity, int connectionId, long duration, Exception exception) {}

    @Override
    public void publish(PerformanceActivity activity, int connectionId, int statementId, long duration,
            Exception exception) {}

    @Override
    public void publish(PerformanceLogEvent event) {
        if (event == null || event.getPhase() == null || event.getScopeId() <= 0 || event.getRootScopeId() <= 0) {
            return;
        }
        if (closed) {
            droppedEvents.incrementAndGet();
            return;
        }
        SpanContext parent = event.getType() == PerformanceLogEvent.Type.START && isRoot(event) ? Span.current()
                .getSpanContext() : null;
        PerformanceLogEvent snapshot = event.withoutException();
        if (!admission.tryLock()) {
            loseEvent();
            return;
        }
        try {
            if (closed || !eventSlots.tryAcquire()) {
                loseEvent();
                return;
            }
            ingressOutstanding.incrementAndGet();
            ingress.offer(new Envelope(snapshot, parent, lossEpoch.get()));
        } finally {
            admission.unlock();
        }
        LockSupport.unpark(ingestion);
    }

    private static boolean isRoot(PerformanceLogEvent event) {
        return event.getActivity() == PerformanceActivity.CONNECTION && event.getScopeId() == event.getRootScopeId()
                && event.getParentScopeId() == 0;
    }

    private void loseEvent() {
        lossEpoch.incrementAndGet();
        droppedEvents.incrementAndGet();
        LockSupport.unpark(ingestion);
    }

    private void ingest() {
        try {
            while (!abort) {
                Envelope envelope = ingress.poll();
                if (envelope == null) {
                    synchronized (lock) {
                        invalidateLostTrees();
                        lock.notifyAll();
                    }
                    if (closed && ingressOutstanding.get() == 0) {
                        return;
                    }
                    LockSupport.park(this);
                    continue;
                }
                eventSlots.release();
                try {
                    // Keep user-injected clocks outside every lock, including during expiry and shutdown.
                    long now = nanoClock.getAsLong();
                    process(envelope, now);
                } catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
                    loseEvent();
                    synchronized (lock) {
                        invalidateLostTrees();
                    }
                } finally {
                    ingressOutstanding.decrementAndGet();
                    synchronized (lock) {
                        lock.notifyAll();
                    }
                }
            }
        } finally {
            synchronized (lock) {
                droppedOpens += pending.size();
                pending.clear();
                ingestionDone = true;
                lock.notifyAll();
            }
        }
    }

    private void invalidateLostTrees() {
        long epoch = lossEpoch.get();
        if (observedEpoch != epoch) {
            droppedOpens += pending.size();
            pending.clear();
            observedEpoch = epoch;
        }
    }

    private void process(Envelope envelope, long now) {
        PerformanceLogEvent event = envelope.event;
        synchronized (lock) {
            invalidateLostTrees();
            if (abort || envelope.epoch != observedEpoch) {
                droppedEvents.incrementAndGet();
                return;
            }
            expireLocked(now);
            long rootId = event.getRootScopeId();
            boolean root = isRoot(event);
            Open open = pending.get(rootId);
            if (event.getType() == PerformanceLogEvent.Type.START) {
                if (root && open == null) {
                    if (pending.size() == maxPendingOpens) {
                        Iterator<Open> iterator = pending.values().iterator();
                        iterator.next();
                        iterator.remove();
                        droppedOpens++;
                    }
                    open = new Open(rootId, now, Context.root().with(Span.wrap(envelope.parent)));
                    pending.put(rootId, open);
                }
                if (open != null && !open.nodes.containsKey(event.getScopeId())
                        && !open.missing.containsKey(event.getScopeId())) {
                    Node node = new Node(event, root, approvedUserAgent);
                    Node parent = open.nodes.get(node.parent);
                    if (root || (parent != null && !parent.ended && makeRoom(open))) {
                        inherit(node, parent, open.nodes.get(open.id));
                        open.nodes.put(node.id, node);
                    } else {
                        open.droppedSpans++;
                        rememberMissing(open, node);
                    }
                }
            } else if (open != null) {
                Node node = open.nodes.get(event.getScopeId());
                if (node == null) {
                    Node missing = open.missing.get(event.getScopeId());
                    if ((missing == null || (!missing.ended && missing.matches(event)))
                            && event.getEndEpochNanos() >= event.getStartEpochNanos()) {
                        if (missing == null) {
                            missing = new Node(event, false, approvedUserAgent);
                            // An evicted unfinished record was already counted at START. Its END must not count
                            // again. Only an END with no outstanding forgotten boundary adds a new dropped scope.
                            if (open.forgottenUnendedSpans != 0) {
                                open.forgottenUnendedSpans--;
                            } else {
                                open.droppedSpans++;
                            }
                            rememberMissing(open, missing);
                        }
                        // END carries the real START identity. Reclaim completed branches before falling back to
                        // an ancestor event; never invent a completed/successful boundary for a missing ancestor.
                        restoreAncestry(open, missing);
                        missing.finish(event, false, approvedUserAgent);
                        long target = open.nodes.containsKey(missing.id) ? missing.id : owner(open, missing.parent);
                        retainDiagnostics(open, target, event);
                        retainError(open, target, event);
                    }
                } else if (!node.ended && node.matches(event)) {
                    node.finish(event, root, approvedUserAgent);
                    retainDiagnostics(open, node.id, event);
                    retainError(open, node.id, event);
                    if (root) {
                        // Loss may occur while policy/diagnostic processing runs. Do not commit that partial tree.
                        if (envelope.epoch != lossEpoch.get()) {
                            invalidateLostTrees();
                            droppedEvents.incrementAndGet();
                            return;
                        }
                        pending.remove(rootId);
                        if (node.failed) {
                            if (metricsEnabled) {
                                pendingFailures = saturatedAdd(pendingFailures, 1);
                                if ("timeout".equals(
                                        node.attributes.get(AttributeKey.stringKey("mssql.connection.outcome")))) {
                                    pendingTimeouts = saturatedAdd(pendingTimeouts, 1);
                                }
                                lock.notifyAll();
                            }
                            prune(open);
                            if (queue.size() < queueCapacity) {
                                queue.addLast(open);
                                lock.notifyAll();
                            } else {
                                droppedOpens++;
                            }
                        }
                    }
                }
            }
        }
    }

    private void restoreAncestry(Open open, Node leaf) {
        Deque<Node> path = new ArrayDeque<>();
        Node node = leaf;
        while (node != null && !node.ended && path.size() < maxSpansPerOpen && !path.contains(node)) {
            path.addFirst(node);
            Node parent = open.nodes.get(node.parent);
            if (parent != null) {
                if (parent.ended) {
                    return;
                }
                for (Node missing : path) {
                    // Newly restored ancestors remain active and therefore cannot be evicted by makeRoom.
                    if (!makeRoom(open)) {
                        return;
                    }
                    open.missing.remove(missing.id);
                    inherit(missing, open.nodes.get(missing.parent), open.nodes.get(open.id));
                    open.nodes.put(missing.id, missing);
                    open.droppedSpans--;
                }
                return;
            }
            node = open.missing.get(node.parent);
        }
    }

    // Evict completed leaves first: this preserves every active retained ancestor and native parent closure.
    private boolean makeRoom(Open open) {
        if (open.nodes.size() < maxSpansPerOpen) {
            return true;
        }
        for (Node candidate : open.nodes.values()) {
            if (candidate.id != open.id && candidate.ended) {
                boolean hasChild = false;
                for (Node node : open.nodes.values()) {
                    if (node.parent == candidate.id) {
                        hasChild = true;
                        break;
                    }
                }
                if (!hasChild) {
                    open.nodes.remove(candidate.id);
                    relocateEvents(open, candidate.id, owner(open, candidate.parent));
                    rememberMissing(open, candidate);
                    open.droppedSpans++;
                    return true;
                }
            }
        }
        return false;
    }

    private void rememberMissing(Open open, Node node) {
        node.owner = owner(open, node.parent);
        if (open.missing.size() == maxSpansPerOpen) {
            Iterator<Node> iterator = open.missing.values().iterator();
            Node forgotten = iterator.next();
            if (!forgotten.ended) {
                open.forgottenUnendedSpans = saturatedAdd(open.forgottenUnendedSpans, 1);
            }
            iterator.remove();
        }
        open.missing.put(node.id, node);
    }

    private static long owner(Open open, long id) {
        // Missing records store a flattened retained owner. Bound traversal even for malformed cyclic input.
        for (int i = 0; i <= open.missing.size(); i++) {
            if (open.nodes.containsKey(id)) {
                return id;
            }
            Node missing = open.missing.get(id);
            if (missing == null || missing.owner == id) {
                break;
            }
            id = missing.owner;
        }
        return open.id;
    }

    private static void relocateEvents(Open open, long from, long to) {
        for (RecordedEvent event : open.events) {
            if (event.owner == from) {
                event.owner = to;
            }
        }
    }

    private void retainError(Open open, long owner, PerformanceLogEvent event) {
        if (!event.getErrorAttributes().isEmpty()) {
            retainEvent(open,
                    new RecordedEvent(owner, "mssql.driver.error", event.getEndEpochNanos(),
                            ConnectionAttributePolicy.error(event.getErrorAttributes()),
                            ConnectionAttributePolicy.errorType(event.getAttributes().get("error.type"))));
        }
    }

    private void retainEvent(Open open, RecordedEvent event) {
        if (open.events.size() == maxEventsPerOpen) {
            RecordedEvent victim = null;
            // Diagnostics cannot evict origin errors. A new error first replaces a diagnostic, then the oldest error.
            for (RecordedEvent existing : open.events) {
                if (!existing.isError()) {
                    victim = existing;
                    break;
                }
            }
            if (victim == null && event.isError()) {
                victim = open.events.peekFirst();
            }
            if (open.droppedEvents != Long.MAX_VALUE) {
                open.droppedEvents++;
            }
            if (victim == null) {
                return;
            }
            open.events.remove(victim);
        }
        open.events.addLast(event);
    }

    @SuppressWarnings("unchecked")
    private void retainDiagnostics(Open open, long target, PerformanceLogEvent boundary) {
        List<Map<String, Object>> diagnostics = boundary.getDiagnosticEvents();
        if (boundary.getScopeId() == open.id) {
            Long dropped = open.nodes.get(open.id).attributes
                    .get(AttributeKey.longKey("mssql.connection.diagnostic_events_dropped"));
            if (dropped != null) {
                open.droppedEvents += Math.min(dropped, Long.MAX_VALUE - open.droppedEvents);
            }
        }
        // Core already bounds and freezes its list. Also bound adapter work for synthetic/excessive input.
        int first = Math.max(0, diagnostics.size() - maxEventsPerOpen);
        open.droppedEvents += Math.min(first, Long.MAX_VALUE - open.droppedEvents);
        for (int i = first; i < diagnostics.size(); i++) {
            Map<String, Object> diagnostic = diagnostics.get(i);
            if (diagnostic == null) {
                continue;
            }
            Object name = diagnostic.get("name");
            Object timestamp = diagnostic.get("timestamp");
            Object attributes = diagnostic.get("attributes");
            if (name instanceof String && timestamp instanceof Long && attributes instanceof Map
                    && (Long) timestamp >= boundary.getStartEpochNanos()
                    && (Long) timestamp <= boundary.getEndEpochNanos()) {
                recordDiagnostic(open, target, (String) name, (Long) timestamp, (Map<String, Object>) attributes);
            }
        }
    }

    // Package-private seam for isolated policy tests. Production consumes immutable diagnostics on accepted ENDs.
    void recordDiagnostic(long rootId, long scopeId, String name, long epochNanos, Map<String, Object> attributes) {
        synchronized (lock) {
            Open open = pending.get(rootId);
            if (!closed && open != null && epochNanos >= open.nodes.get(rootId).start) {
                recordDiagnostic(open, scopeId, name, epochNanos, attributes);
            }
        }
    }

    private void recordDiagnostic(Open open, long scopeId, String name, long epochNanos,
            Map<String, Object> attributes) {
        Attributes safe = ConnectionAttributePolicy.diagnostic(name, attributes);
        if (safe == null) {
            return;
        }
        long target = diagnosticOwner(open, scopeId, name, epochNanos, attributes);
        retainEvent(open, new RecordedEvent(target, name, epochNanos, safe));
    }

    private static long diagnosticOwner(Open open, long scopeId, String name, long epochNanos,
            Map<String, Object> attributes) {
        if ("mssql.driver.retry".equals(name) || "mssql.driver.redirect".equals(name)
                || "mssql.driver.connection.retry_decision".equals(name)) {
            return open.id;
        }
        if (scopeId != open.id) {
            return owner(open, scopeId);
        }
        // Current core batches diagnostics on root END without scope IDs. Match only retained real boundaries,
        // using attempt and timestamp; prefer token acquisition over its enclosing login scope. If unavailable,
        // keep the original diagnostic on root, not on an unrelated retry or a fabricated child.
        Node match = null;
        Object attempt = attributes.get("mssql.connection.attempt");
        Object phase = attributes.get("mssql.timeout.phase");
        for (Node node : open.nodes.values()) {
            boolean phaseMatch = "mssql.driver.authentication".equals(name) ? "login".equals(node.phase)
                    || "token_acquisition".equals(node.phase) : node.phase.equals(phase);
            if (!phaseMatch || !node.ended || epochNanos < node.start || epochNanos > node.end) {
                continue;
            }
            if (attempt != null) {
                Long nodeAttempt = node.attributes.get(AttributeKey.longKey("mssql.connection.attempt"));
                if (!(attempt instanceof Long || attempt instanceof Integer) || nodeAttempt == null
                        || nodeAttempt.longValue() != ((Number) attempt).longValue()) {
                    continue;
                }
            }
            if (match == null || ("token_acquisition".equals(node.phase) && !"token_acquisition".equals(match.phase))
                    || (node.phase.equals(match.phase) && node.start > match.start)) {
                match = node;
            }
        }
        return match == null ? open.id : match.id;
    }

    private static void inherit(Node node, Node parent, Node root) {
        if (node.id == (root == null ? node.id : root.id)) {
            return;
        }
        AttributeKey<Long> attempt = AttributeKey.longKey("mssql.connection.attempt");
        if (parent != null && node.attributes.get(attempt) == null && parent.attributes.get(attempt) != null) {
            node.attributes = node.attributes.toBuilder().put(attempt, parent.attributes.get(attempt)).build();
        }
        AttributeKey<String> method = AttributeKey.stringKey("mssql.authentication.method");
        if (root != null && ("login".equals(node.phase) || "token_acquisition".equals(node.phase))
                && (node.attributes.get(method) == null || "unknown".equals(node.attributes.get(method)))
                && root.attributes.get(method) != null) {
            node.attributes = node.attributes.toBuilder().put(method, root.attributes.get(method)).build();
        }
    }

    // START order guarantees parents precede children. Removing an incomplete parent also removes its descendants.
    private void prune(Open open) {
        Iterator<Node> iterator = open.nodes.values().iterator();
        while (iterator.hasNext()) {
            Node node = iterator.next();
            if (!node.ended || (node.id != open.id && !open.nodes.containsKey(node.parent))) {
                iterator.remove();
                open.droppedSpans++;
                rememberMissing(open, node);
                relocateEvents(open, node.id, owner(open, node.parent));
            } else {
                inherit(node, open.nodes.get(node.parent), open.nodes.get(open.id));
            }
        }
        open.missing.clear();
    }

    /** Removes expired incomplete trees. Also performed automatically, at least once per second. */
    public void expirePending() {
        long now = nanoClock.getAsLong();
        synchronized (lock) {
            expireLocked(now);
        }
    }

    private void expireLocked(long now) {
        Iterator<Open> iterator = pending.values().iterator();
        while (iterator.hasNext()) {
            if (now - iterator.next().created >= maxOpenAgeNanos) {
                iterator.remove();
                droppedOpens++;
            }
        }
    }

    /** @return incomplete opens already processed by ingestion; excludes queued raw events */
    public int pendingOpenCount() {
        synchronized (lock) {
            return pending.size();
        }
    }

    /** @return trees waiting for the worker, excluding its in-flight tree */
    public int queuedOpenCount() {
        synchronized (lock) {
            return queue.size();
        }
    }

    /** @return opens discarded due to event loss, retention/queue limits, close, or SDK handoff failure */
    public long droppedOpenCount() {
        synchronized (lock) {
            return droppedOpens;
        }
    }

    /**
     * Returns raw boundaries rejected on admission (including after close) or discarded after acceptance due to
     * loss discontinuity, ingestion failure or close timeout. This is distinct from per-tree diagnostic event
     * truncation and {@link #droppedOpenCount()}; loss may invalidate multiple otherwise unaffected opens.
     *
     * @return number of discarded raw event snapshots
     */
    public long droppedEventCount() {
        return droppedEvents.get();
    }

    // Test seam: wait for ingestion without requiring an application-blocked SDK worker to drain.
    boolean awaitIngestion(Duration timeout) throws InterruptedException {
        return awaitIdle(timeout, true);
    }

    /**
     * Waits for accepted raw events, in-flight ingestion, SDK handoff of completed trees and pending counter deltas;
     * incomplete opens do not prevent idleness. Concurrent publishers may add work after idleness is observed.
     * Does not force-flush application-owned processors or exporters.
     * 
     * @param timeout
     *        nonnegative wait budget
     * @return true if the worker and queue are idle
     * @throws InterruptedException
     *         if interrupted while waiting
     */
    public boolean awaitIdle(Duration timeout) throws InterruptedException {
        return awaitIdle(timeout, false);
    }

    private boolean awaitIdle(Duration timeout, boolean ingestionOnly) throws InterruptedException {
        long budget = validateDuration(timeout, true);
        long start = System.nanoTime();
        synchronized (lock) {
            while (ingressOutstanding.get() != 0 || (closed && !ingestionDone)
                    || (!ingestionDone && observedEpoch != lossEpoch.get())
                    || (!ingestionOnly && (active || !queue.isEmpty() || pendingFailures != 0))) {
                long remaining = budget - (System.nanoTime() - start);
                if (remaining <= 0 || Thread.currentThread() == worker || Thread.currentThread() == ingestion) {
                    return false;
                }
                TimeUnit.NANOSECONDS.timedWait(lock, remaining);
            }
            return true;
        }
    }

    /**
     * Rejects new events, drains accepted raw events, completed trees and counter deltas within the configured budget,
     * then releases incomplete opens. On timeout, clears both queues and pending deltas and interrupts the workers.
     * A misbehaving application processor cannot be forcibly stopped; it can retain at most one bounded in-flight tree
     * until it returns. A blocked ingestion clock can retain one exception-free event until it returns.
     * Does not unregister another callback or flush/close any application SDK resource. Idempotent.
     */
    @Override
    public void close() {
        admission.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;
        } finally {
            admission.unlock();
        }
        expiry.shutdownNow();
        LockSupport.unpark(ingestion);
        boolean drained = false;
        try {
            drained = awaitIdle(Duration.ofNanos(closeTimeoutNanos));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (!drained) {
                abort = true;
                while (ingress.poll() != null) {
                    eventSlots.release();
                    droppedEvents.incrementAndGet();
                    ingressOutstanding.decrementAndGet();
                }
                synchronized (lock) {
                    droppedOpens += pending.size();
                    pending.clear();
                    droppedOpens += queue.size();
                    queue.clear();
                    pendingFailures = 0;
                    pendingTimeouts = 0;
                    lock.notifyAll();
                }
                ingestion.interrupt();
                LockSupport.unpark(ingestion);
                worker.interrupt();
            }
        }
    }

    private void work() {
        long metricRetryAt = 0;
        while (true) {
            Open open;
            long failures;
            long timeouts;
            synchronized (lock) {
                while (queue.isEmpty() && !abort) {
                    if (pendingFailures != 0 && (metricRetryAt == 0 || System.nanoTime() - metricRetryAt >= 0)) {
                        break;
                    }
                    if (ingestionDone && pendingFailures == 0) {
                        return;
                    }
                    try {
                        if (pendingFailures != 0) {
                            TimeUnit.NANOSECONDS.timedWait(lock, Math.max(1, metricRetryAt - System.nanoTime()));
                        } else {
                            lock.wait();
                        }
                    } catch (InterruptedException e) {
                        if (abort) {
                            return;
                        }
                    }
                }
                if (abort) {
                    return;
                }
                failures = 0;
                timeouts = 0;
                if (metricRetryAt == 0 || System.nanoTime() - metricRetryAt >= 0) {
                    failures = pendingFailures;
                    timeouts = pendingTimeouts;
                    pendingFailures = 0;
                    pendingTimeouts = 0;
                }
                open = queue.pollFirst();
                active = true;
            }
            try {
                if (failures != 0) {
                    if (recordMetrics(failures, timeouts)) {
                        metricRetryAt = 0;
                    } else {
                        metricRetryAt = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100);
                        synchronized (lock) {
                            if (!abort) {
                                pendingFailures = saturatedAdd(pendingFailures, failures);
                                pendingTimeouts = saturatedAdd(pendingTimeouts, timeouts);
                            }
                        }
                    }
                }
                if (open != null) {
                    export(open);
                }
            } catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
                // Never surface SDK/exporter failures or their potentially sensitive messages to JDBC callers.
                synchronized (lock) {
                    droppedOpens++;
                }
            } finally {
                synchronized (lock) {
                    active = false;
                    lock.notifyAll();
                }
            }
        }
    }

    private static long saturatedAdd(long current, long delta) {
        return current + Math.min(delta, Long.MAX_VALUE - current);
    }

    // false means instrument construction failed before any add; only that case is safe to retry.
    private boolean recordMetrics(long failures, long timeouts) {
        if (counters == null) {
            try {
                LongCounter failureCounter = telemetry.getMeter(SCOPE)
                        .counterBuilder("mssql.driver.connection.failure.count").build();
                LongCounter timeoutCounter = telemetry.getMeter(SCOPE)
                        .counterBuilder("mssql.driver.connection.timeout.count").build();
                counters = new Counters(failureCounter, timeoutCounter);
            } catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
                return false;
            }
        }
        Counters current = counters;
        try {
            current.failures.add(failures);
        } catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
            // Do not retry an ambiguous recording result, but allow the independent timeout count to proceed.
            counters = null;
        }
        if (timeouts != 0) {
            try {
                current.timeouts.add(timeouts);
            } catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
                counters = null;
            }
        }
        return true;
    }

    private void export(Open open) {
        if (tracer == null) {
            tracer = telemetry.getTracer(SCOPE);
        }
        Map<Long, Span> spans = new LinkedHashMap<>();
        try {
            for (Node node : open.nodes.values()) {
                if (abort) {
                    break;
                }
                boolean root = node.id == open.id;
                Context parent = root ? open.parent : Context.root().with(spans.get(node.parent));
                Span span = tracer.spanBuilder("mssql.driver.connection." + node.phase).setParent(parent)
                        .setSpanKind(root ? SpanKind.CLIENT : SpanKind.INTERNAL)
                        .setStartTimestamp(node.start, TimeUnit.NANOSECONDS).setAllAttributes(node.attributes)
                        .startSpan();
                spans.put(node.id, span);
                if (node.failed) {
                    span.setStatus(StatusCode.ERROR);
                }
                if (root && (open.droppedSpans != 0 || open.droppedEvents != 0)) {
                    span.setAttribute("mssql.telemetry.truncated", true);
                    span.setAttribute("mssql.telemetry.dropped_span_count", open.droppedSpans);
                    span.setAttribute("mssql.telemetry.dropped_event_count", open.droppedEvents);
                }
                for (RecordedEvent event : open.events) {
                    if (event.owner == node.id) {
                        if (event.isError()) {
                            span.setStatus(StatusCode.ERROR);
                            if (!root) {
                                span.setAttribute("mssql.error.category",
                                        event.attributes.get(AttributeKey.stringKey("mssql.error.category")));
                                span.setAttribute("error.type", event.errorType);
                            }
                        }
                        span.addEvent(event.name, event.attributes, event.epochNanos, TimeUnit.NANOSECONDS);
                    }
                }
            }
        } finally {
            // End children first. Even a throwing processor must not strand the other spans in this tree.
            Deque<Node> reverse = new ArrayDeque<>(open.nodes.values());
            Throwable failure = null;
            while (!reverse.isEmpty()) {
                Node node = reverse.removeLast();
                Span span = spans.get(node.id);
                if (span != null) {
                    try {
                        span.end(node.end, TimeUnit.NANOSECONDS);
                    } catch (RuntimeException | LinkageError | ServiceConfigurationError e) {
                        failure = e;
                    }
                }
            }
            if (failure instanceof RuntimeException) {
                throw (RuntimeException) failure;
            } else if (failure instanceof Error) {
                throw (Error) failure;
            }
        }
    }

    private static final class Counters {
        final LongCounter failures;
        final LongCounter timeouts;

        Counters(LongCounter failures, LongCounter timeouts) {
            this.failures = Objects.requireNonNull(failures, "failures");
            this.timeouts = Objects.requireNonNull(timeouts, "timeouts");
        }
    }

    private static final class Envelope {
        final PerformanceLogEvent event;
        final SpanContext parent;
        final long epoch;

        Envelope(PerformanceLogEvent event, SpanContext parent, long epoch) {
            this.event = event;
            this.parent = parent;
            this.epoch = epoch;
        }
    }

    private static final class RecordedEvent {
        long owner;
        final String name;
        final long epochNanos;
        final Attributes attributes;
        final String errorType;

        RecordedEvent(long owner, String name, long epochNanos, Attributes attributes) {
            this(owner, name, epochNanos, attributes, null);
        }

        RecordedEvent(long owner, String name, long epochNanos, Attributes attributes, String errorType) {
            this.owner = owner;
            this.name = name;
            this.epochNanos = epochNanos;
            this.attributes = attributes;
            this.errorType = errorType;
        }

        boolean isError() {
            return "mssql.driver.error".equals(name);
        }
    }

    private static final class Open {
        final long id;
        final long created;
        final Context parent;
        final Map<Long, Node> nodes = new LinkedHashMap<>();
        final Map<Long, Node> missing = new LinkedHashMap<>();
        final Deque<RecordedEvent> events = new ArrayDeque<>();
        long forgottenUnendedSpans;
        long droppedSpans;
        long droppedEvents;

        Open(long id, long created, Context parent) {
            this.id = id;
            this.created = created;
            this.parent = parent;
        }
    }

    private static final class Node {
        final long id;
        final long parent;
        final long start;
        final PerformanceActivity activity;
        final String phase;
        long end;
        boolean ended;
        boolean failed;
        long owner;
        Attributes attributes;

        Node(PerformanceLogEvent event, boolean root, String approvedUserAgent) {
            id = event.getScopeId();
            parent = event.getParentScopeId();
            start = event.getStartEpochNanos();
            activity = event.getActivity();
            phase = root ? "open" : event.getPhase();
            attributes = ConnectionAttributePolicy.span(event.getAttributes(), root,
                    activity == PerformanceActivity.CONNECTION_ATTEMPT, phase, approvedUserAgent);
        }

        boolean matches(PerformanceLogEvent event) {
            return activity == event.getActivity() && parent == event.getParentScopeId()
                    && start == event.getStartEpochNanos() && event.getEndEpochNanos() >= start;
        }

        void finish(PerformanceLogEvent event, boolean root, String approvedUserAgent) {
            end = event.getEndEpochNanos();
            ended = true;
            // A phase label alone is not evidence that this particular boundary failed.
            Object outcome = event.getAttributes().get("mssql.connection.outcome");
            Object attemptOutcome = event.getAttributes().get("mssql.connection.attempt_outcome");
            failed = event.hasException() || event.getAttributes().containsKey("mssql.error.category")
                    || "failure".equals(attemptOutcome) || "timeout".equals(attemptOutcome)
                    || "canceled".equals(attemptOutcome) || "failure".equals(outcome) || "timeout".equals(outcome)
                    || "canceled".equals(outcome);
            // A successful root may contain handled failures. Its terminal outcome is authoritative.
            if (root && "success".equals(event.getAttributes().get("mssql.connection.outcome"))) {
                failed = false;
            }
            attributes = attributes.toBuilder().putAll(ConnectionAttributePolicy.span(event.getAttributes(), root,
                    activity == PerformanceActivity.CONNECTION_ATTEMPT, phase, approvedUserAgent)).build();
            if (failed) {
                attributes = attributes.toBuilder()
                        .put("mssql.error.category",
                                ConnectionAttributePolicy.category(event.getAttributes().get("mssql.error.category")))
                        .put("error.type", ConnectionAttributePolicy.errorType(event.getAttributes().get("error.type")))
                        .build();
                if (root) {
                    String category = attributes.get(AttributeKey.stringKey("mssql.error.category"));
                    String terminalOutcome = "failure";
                    if ("timeout".equals(category) || "canceled".equals(category)) {
                        terminalOutcome = category;
                    }
                    if ("timeout".equals(outcome) || "canceled".equals(outcome) || "failure".equals(outcome)) {
                        terminalOutcome = (String) outcome;
                    }
                    attributes = attributes.toBuilder()
                            .put("mssql.connection.failure_phase",
                                    ConnectionAttributePolicy.phase(event.getFailurePhase()))
                            .put("mssql.connection.outcome", terminalOutcome).build();
                }
            }
        }
    }
}
