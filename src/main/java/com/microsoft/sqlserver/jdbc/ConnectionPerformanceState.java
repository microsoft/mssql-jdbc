/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;


/**
 * Driver-owned, thread-confined phase stacks, matched by connection identity, not connection equals(). No global
 * connection registry or callback-owned phase state. Roots remove every reference on exit. Worker threads must open
 * their own scopes; an unrelated connection or thread cannot inherit or overwrite another operation's phase.
 */
final class ConnectionPerformanceState {
    private static final AtomicLong IDS = new AtomicLong();
    private static final ThreadLocal<List<Node>> ACTIVE = new ThreadLocal<>();
    private static final int MAX_DIAGNOSTIC_EVENTS = 128;

    enum RetryDecision {
        RETRY_SCHEDULED, NOT_RETRYABLE, LIMIT_REACHED, BUDGET_EXHAUSTED
    }

    private ConnectionPerformanceState() {}

    static Node current(SQLServerConnection con) {
        List<Node> nodes = ACTIVE.get();
        if (nodes != null) {
            for (int i = nodes.size() - 1; i >= 0; i--) {
                if (nodes.get(i).con == con) {
                    return nodes.get(i);
                }
            }
        }
        return null;
    }

    static Node enter(SQLServerConnection con, PerformanceActivity activity) {
        Node parent = activity == PerformanceActivity.CONNECTION ? null : current(con);
        Node node = new Node(con, activity, parent);
        // New work means a previous sibling's failure was handled. Do not contaminate a later failure or retry.
        for (Node ancestor = parent; ancestor != null; ancestor = ancestor.parent) {
            ancestor.pending = null;
        }
        List<Node> nodes = ACTIVE.get();
        if (nodes == null) {
            nodes = new ArrayList<>();
            ACTIVE.set(nodes);
        }
        nodes.add(node);
        return node;
    }

    static void exit(Node node) {
        if (!node.isOwner()) {
            return;
        }
        List<Node> nodes = ACTIVE.get();
        if (nodes != null) {
            if (node == node.root) {
                // Also clear abandoned children on exceptional root exit.
                nodes.removeIf(n -> n.root == node);
            } else {
                nodes.remove(node);
            }
            if (nodes.isEmpty()) {
                ACTIVE.remove();
            }
        }
    }

    static void settings(SQLServerConnection con, Properties validated) {
        Node node = current(con);
        if (node != null && validated != null && node.root.activity == PerformanceActivity.CONNECTION) {
            Map<String, Object> attrs = node.root.attributes;
            enumSetting(attrs, validated, "encrypt", "mssql.connection.encrypt", "false", "true", "strict");
            booleanSetting(attrs, validated, "trustServerCertificate", "mssql.connection.trust_server_certificate");
            if ("strict".equals(attrs.get("mssql.connection.encrypt"))) {
                attrs.put("mssql.connection.trust_server_certificate", false);
            }
            String intent = validated.getProperty("applicationIntent");
            if ("ReadOnly".equalsIgnoreCase(intent) || "ReadWrite".equalsIgnoreCase(intent)) {
                attrs.put("mssql.connection.application_intent",
                        "ReadOnly".equalsIgnoreCase(intent) ? "read_only" : "read_write");
            }
            booleanSetting(attrs, validated, "multiSubnetFailover", "mssql.connection.multi_subnet_failover");
            booleanSetting(attrs, validated, "transparentNetworkIPResolution",
                    "mssql.connection.transparent_network_ip_resolution");
            numberSetting(attrs, validated, "loginTimeout", "mssql.connection.login_timeout", 1, Integer.MAX_VALUE);
                numberSetting(attrs, validated, "socketTimeout", "mssql.connection.socket_timeout", 1000, Integer.MAX_VALUE);
            numberSetting(attrs, validated, "connectRetryCount", "mssql.connection.connect_retry_count", 0, 255);
            numberSetting(attrs, validated, "connectRetryInterval", "mssql.connection.connect_retry_interval", 1, 60);
            // No inspection of token, password, user, callback class, or endpoint values.
            String auth = validated.getProperty("authentication");
            String[] inputs = {"SqlPassword", "ActiveDirectoryPassword", "ActiveDirectoryIntegrated",
                    "ActiveDirectoryManagedIdentity", "ActiveDirectoryMSI", "ActiveDirectoryServicePrincipal",
                    "ActiveDirectoryServicePrincipalCertificate", "ActiveDirectoryInteractive",
                    "ActiveDirectoryDefault"};
            String[] outputs = {"sql_password", "entra_password", "entra_integrated", "managed_identity",
                    "managed_identity", "service_principal_secret", "service_principal_certificate", "interactive",
                    "default_credential"};
            for (int i = 0; i < inputs.length; i++) {
                if (inputs[i].equalsIgnoreCase(auth)) {
                    attrs.put("mssql.authentication.method", outputs[i]);
                    break;
                }
            }
            if ("NotSpecified".equalsIgnoreCase(auth)) {
                attrs.put("mssql.authentication.method", "true".equals(validated.getProperty("tokenCallback"))
                        ? "access_token_callback" : "true".equals(validated.getProperty("suppliedToken"))
                                ? "access_token" : "sql_password");
            }
            if ("true".equalsIgnoreCase(validated.getProperty("integratedSecurity"))) {
                String scheme = validated.getProperty("authenticationScheme");
                if ("JavaKerberos".equalsIgnoreCase(scheme)) {
                    attrs.put("mssql.authentication.method", "integrated_kerberos");
                } else if ("NTLM".equalsIgnoreCase(scheme)) {
                    attrs.put("mssql.authentication.method", "integrated_ntlm");
                } else if ("NativeAuthentication".equalsIgnoreCase(scheme)) {
                    attrs.put("mssql.authentication.method", "integrated_native");
                }
            }
        }
    }

    private static void enumSetting(Map<String, Object> attrs, Properties settings, String property, String key,
            String... allowed) {
        for (String value : allowed) {
            if (value.equalsIgnoreCase(settings.getProperty(property))) {
                attrs.put(key, value);
                break;
            }
        }
    }

    private static void booleanSetting(Map<String, Object> attrs, Properties settings, String property, String key) {
        String value = settings.getProperty(property);
        if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
            attrs.put(key, Boolean.valueOf(value));
        }
    }

    private static void numberSetting(Map<String, Object> attrs, Properties settings, String property, String key,
            int divisor, int max) {
        String value = settings.getProperty(property);
        if (value != null) {
            try {
                long number = Long.parseLong(value);
                if (number >= 0 && number <= max) {
                    if (divisor == 0) {
                        attrs.put(key, number);
                    } else {
                        attrs.put(key, (double) number / divisor);
                    }
                }
            } catch (NumberFormatException ignored) {
                // Invalid input is not telemetry. The connection parser owns validation and its error.
            }
        }
    }

    static final class Failure {
        final Exception exception;
        final long origin;
        final ConnectionTelemetryError error;

        Failure(Exception exception, Node origin, String resourceKey) {
            this(exception, origin, resourceKey, origin.activity.connectionPhase(), false);
        }

        Failure(Exception exception, Node origin, String resourceKey, String phase, boolean callback) {
            this.exception = exception;
            this.origin = origin.id;
            error = ConnectionTelemetryError.classify(exception, phase, resourceKey, callback);
        }
    }

    static void retry(SQLServerConnection con, RetryDecision decision, boolean failover, long delayMillis) {
        Node node = current(con);
        if (node == null) {
            return;
        }
        Node root = node.root;
        Map<String, Object> attrs = new LinkedHashMap<>();
        attrs.put("mssql.connection.attempt", root.attemptCount);
        attrs.put("mssql.error.retry_decision", decision.name().toLowerCase(java.util.Locale.ROOT));
        if (decision == RetryDecision.RETRY_SCHEDULED) {
            root.nextAttemptReason = failover ? "failover" : "retry";
            attrs.put("mssql.retry.attempt", root.attemptCount + 1);
            attrs.put("mssql.retry.delay", Math.max(0, delayMillis) / 1000.0);
        } else if (decision == RetryDecision.BUDGET_EXHAUSTED) {
            root.attributes.put("mssql.connection.budget_exhausted", true);
        }
        if (root.pending != null) {
            attrs.put("error.type", root.pending.error.errorType);
        }
        root.diagnostic(decision == RetryDecision.RETRY_SCHEDULED ? "mssql.driver.retry"
                                      : "mssql.driver.connection.retry_decision",
            attrs);
    }

    static void redirect(SQLServerConnection con, boolean enhanced) {
        Node node = current(con);
        if (node != null) {
            node.attributes.put("mssql.connection.attempt_outcome", "redirect");
            node.root.nextAttemptReason = "redirect";
            Map<String, Object> attrs = new LinkedHashMap<>();
            attrs.put("mssql.connection.attempt", node.root.attemptCount);
            attrs.put("mssql.connection.redirect.type", enhanced ? "enhanced_routing" : "tds_routing");
            node.root.diagnostic("mssql.driver.redirect", attrs);
        }
    }

    static void authentication(SQLServerConnection con, String source) {
        Node node = current(con);
        if (node != null && ("callback".equals(source) || "msal".equals(source) || "native".equals(source)
                || "managed_identity".equals(source) || "default_credential".equals(source))) {
            node.attributes.put("mssql.authentication.token_source", source);
            Map<String, Object> attrs = new LinkedHashMap<>();
            attrs.put("mssql.connection.attempt", node.root.attemptCount);
            attrs.put("mssql.authentication.token_source", source);
            attrs.put("mssql.authentication.method", node.root.attributes.get("mssql.authentication.method"));
            node.root.diagnostic("mssql.driver.authentication", attrs);
        }
    }

    static final class Node {
        final SQLServerConnection con;
        final PerformanceActivity activity;
        final Node parent;
        final Node root;
        final Thread owner = Thread.currentThread();
        final long id = IDS.incrementAndGet();
        final long startNanos = System.nanoTime();
        final long startEpochNanos;
        final Map<String, Object> attributes = new LinkedHashMap<>();
        Failure pending;
        Failure failure;
        UUID clientConnectionId;
        long attemptCount;
        long retryCount;
        long redirectCount;
        String nextAttemptReason = "initial";
        List<Map<String, Object>> diagnosticEvents;
        long droppedEvents;
        PerformanceLog.Scope scope;

        Node(SQLServerConnection con, PerformanceActivity activity, Node parent) {
            this.con = con;
            this.activity = activity;
            this.parent = parent;
            root = parent == null ? this : parent.root;
            startEpochNanos = parent == null ? System.currentTimeMillis() * 1000000L
                                             : root.startEpochNanos + (startNanos - root.startNanos);
            attributes.put("db.system.name", "microsoft.sql_server");
            if (activity == PerformanceActivity.CONNECTION) {
                attributes.put("mssql.connection.guid", UUID.randomUUID().toString());
                attributes.put("mssql.telemetry.schema.version", "1.0");
                attributes.put("mssql.authentication.method", "unknown");
                // Reuse the driver's value; the optional adapter requires exact approval before exporting it.
                attributes.put("mssql.driver.user_agent.original", SQLServerConnection.userAgentStr);
            } else if (activity == PerformanceActivity.CONNECTION_ATTEMPT
                    && root.activity == PerformanceActivity.CONNECTION) {
                // Count starts, even if an attempt fails before allocating its TDS client connection ID.
                attributes.put("mssql.connection.attempt", ++root.attemptCount);
                String reason = root.nextAttemptReason;
                attributes.put("mssql.connection.attempt_reason", reason);
                if ("redirect".equals(reason)) {
                    root.redirectCount++;
                } else if (root.attemptCount > 1) {
                    root.retryCount++;
                }
                root.nextAttemptReason = "retry";
            } else if (parent != null && parent.attributes.containsKey("mssql.connection.attempt")) {
                attributes.put("mssql.connection.attempt", parent.attributes.get("mssql.connection.attempt"));
            }
            if (activity == PerformanceActivity.LOGIN_EXCHANGE || activity == PerformanceActivity.TOKEN_REQUEST) {
                Object method = root.attributes.get("mssql.authentication.method");
                if (method != null) {
                    attributes.put("mssql.authentication.method", method);
                }
            }
        }

        boolean isOwner() {
            return owner == Thread.currentThread();
        }

        void fail(Exception exception, String resourceKey) {
            fail(exception, resourceKey, activity.connectionPhase(), false);
        }

        void fail(Exception exception, String resourceKey, String phase, boolean callback) {
            if (exception != null && isOwner() && failure == null) {
                failure = pending == null ? new Failure(exception, this, resourceKey, phase, callback) : pending;
                for (Node ancestor = parent; ancestor != null; ancestor = ancestor.parent) {
                    ancestor.pending = failure;
                }
                if (failure.origin == id && "timeout".equals(failure.error.category)) {
                    Map<String, Object> attrs = new LinkedHashMap<>();
                    attrs.put("mssql.connection.attempt", root.attemptCount);
                    attrs.put("mssql.timeout.phase", failure.error.phase);
                        attrs.put("mssql.timeout.kind", timeoutKind(resourceKey, failure.error));
                    attrs.put("error.type", failure.error.errorType);
                    root.diagnostic("mssql.driver.timeout", attrs);
                }
            }
        }

        private void diagnostic(String name, Map<String, Object> attrs) {
            if (diagnosticEvents == null) {
                diagnosticEvents = new ArrayList<>();
            }
            if (diagnosticEvents.size() == MAX_DIAGNOSTIC_EVENTS) {
                droppedEvents++;
                return;
            }
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("name", name);
            event.put("timestamp", startEpochNanos + Math.max(0, System.nanoTime() - startNanos));
            event.put("attributes", Collections.unmodifiableMap(new LinkedHashMap<>(attrs)));
            diagnosticEvents.add(Collections.unmodifiableMap(event));
        }

        private static String timeoutKind(String resourceKey, ConnectionTelemetryError error) {
            if ("R_timedOutBeforeRouting".equals(resourceKey)) {
                return "routing_budget";
            }
            if ("R_connectionTimedOut".equals(resourceKey) && "socket_connect".equals(error.phase)) {
                return "socket_selection";
            }
            if ("java.net.SocketTimeoutException".equals(error.errorType)) {
                return "socket_connect".equals(error.phase) ? "socket_connect" : "socket_read";
            }
            return "unknown";
        }

        PerformanceLogEvent event(boolean end) {
            long duration = end ? Math.max(0, System.nanoTime() - startNanos) : 0;
            Map<String, Object> attrs = new LinkedHashMap<>(attributes);
            Map<String, Object> errors = Collections.emptyMap();
            Failure terminal = end ? failure : null;
            if (end && activity == PerformanceActivity.CONNECTION_ATTEMPT) {
                attrs.put("mssql.connection.attempt_outcome",
                    terminal == null ? attributes.getOrDefault("mssql.connection.attempt_outcome", "success")
                             : outcome(terminal));
            }
            if (end && activity == PerformanceActivity.CONNECTION_ATTEMPT && clientConnectionId != null) {
                attrs.put("mssql.connection.client_connection_id", clientConnectionId.toString());
            }
            if (end && activity == PerformanceActivity.CONNECTION) {
                attrs.put("mssql.connection.attempt_count", attemptCount);
                attrs.put("mssql.connection.retry_count", retryCount);
                attrs.put("mssql.connection.redirect_count", redirectCount);
                attrs.put("mssql.connection.outcome", outcome(terminal));
                if (droppedEvents > 0) {
                    attrs.put("mssql.connection.diagnostic_events_dropped", droppedEvents);
                }
            }
            if (terminal != null) {
                attrs.put("mssql.error.category", terminal.error.category);
                attrs.put("error.type", terminal.error.errorType);
                if (activity == PerformanceActivity.CONNECTION) {
                    attrs.put("mssql.connection.failure_phase", terminal.error.phase);
                }
                if (terminal.origin == id) {
                    errors = terminal.error.attributes;
                }
            }
            return new PerformanceLogEvent(end ? PerformanceLogEvent.Type.END : PerformanceLogEvent.Type.START, id,
                    parent == null ? 0 : parent.id, root.id, con == null ? 0 : con.getConnectionID(), activity,
                    startEpochNanos, end ? startEpochNanos + duration : 0, duration,
                    terminal == null ? null : terminal.exception, terminal == null ? null : terminal.error.phase, attrs,
                    errors, end && activity == PerformanceActivity.CONNECTION
                            && diagnosticEvents != null ? diagnosticEvents : Collections.emptyList());
        }

        private String outcome(Failure terminal) {
            String category = terminal == null ? "success" : terminal.error.category;
            return "success".equals(category) || "timeout".equals(category) || "canceled".equals(category) ? category
                                                                                                       : "failure";
        }
    }
}
