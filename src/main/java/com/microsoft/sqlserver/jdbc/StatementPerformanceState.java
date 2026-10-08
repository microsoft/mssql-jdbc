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
import java.util.concurrent.atomic.AtomicLong;


/** Driver-owned, thread-confined statement execution lifecycle state. */
final class StatementPerformanceState {
    private static final AtomicLong IDS = new AtomicLong();
    private static final ThreadLocal<List<Node>> ACTIVE = new ThreadLocal<>();
    private static final int MAX_DIAGNOSTIC_EVENTS = 64;

    private StatementPerformanceState() {}

    static Node current(SQLServerStatement statement) {
        List<Node> nodes = ACTIVE.get();
        if (nodes != null) {
            for (int i = nodes.size() - 1; i >= 0; i--) {
                if (nodes.get(i).statement == statement) {
                    return nodes.get(i);
                }
            }
        }
        return null;
    }

    static Node enter(SQLServerStatement statement, PerformanceActivity activity, String userSql) {
        Node parent = activity == PerformanceActivity.STATEMENT_INVOCATION ? null : current(statement);
        Node node = new Node(statement, activity, parent, userSql);
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
                nodes.removeIf(candidate -> candidate.root == node);
            } else {
                nodes.remove(node);
            }
            if (nodes.isEmpty()) {
                ACTIVE.remove();
            }
        }
    }

    static void retry(SQLServerStatement statement, long delayMillis) {
        Node node = current(statement);
        if (node == null) {
            return;
        }
        Node root = node.root;
        root.nextAttemptReason = "configurable_retry";
        Map<String, Object> attributes = new LinkedHashMap<>();
        attributes.put("mssql.statement.attempt", root.attemptCount);
        attributes.put("mssql.retry.attempt", root.attemptCount + 1);
        attributes.put("mssql.retry.delay", Math.max(0, delayMillis) / 1000.0);
        attributes.put("mssql.error.retry_decision", "retry_scheduled");
        if (root.pending != null) {
            attributes.put("error.type", root.pending.error.errorType);
        }
        root.diagnostic("mssql.driver.statement.retry_decision", attributes);
    }

    static final class Failure {
        final Exception exception;
        final long origin;
        final StatementTelemetryError error;

        Failure(Exception exception, Node origin, String resourceKey) {
            this.exception = exception;
            this.origin = origin.id;
            error = StatementTelemetryError.classify(exception, origin.activity.statementPhase(), resourceKey);
        }
    }

    static final class Node {
        final SQLServerStatement statement;
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
        long attemptCount;
        long retryCount;
        String nextAttemptReason = "initial";
        List<Map<String, Object>> diagnosticEvents;
        PerformanceLog.Scope scope;

        Node(SQLServerStatement statement, PerformanceActivity activity, Node parent, String userSql) {
            this.statement = statement;
            this.activity = activity;
            this.parent = parent;
            root = parent == null ? this : parent.root;
            startEpochNanos = parent == null ? System.currentTimeMillis() * 1000000L
                                             : root.startEpochNanos + (startNanos - root.startNanos);
            attributes.put("db.system.name", "microsoft.sql_server");
            if (activity == PerformanceActivity.STATEMENT_INVOCATION) {
                attributes.put("mssql.telemetry.schema.version", "1.0");
                String guid = statement.connection.getTelemetryConnectionGuid();
                if (guid != null) {
                    attributes.put("mssql.connection.guid", guid);
                }
                attributes.put("mssql.statement.type", statement instanceof SQLServerCallableStatement
                        ? "callable_statement" : statement instanceof SQLServerPreparedStatement
                                ? "prepared_statement" : "statement");
                attributes.put("mssql.statement.api", api(statement.executeMethod));
                attributes.put("mssql.statement.operation", operation(userSql));
                attributes.put("mssql.statement.query_timeout", (double) Math.max(0, statement.queryTimeout));
                attributes.put("mssql.statement.response_buffering",
                        statement.getIsResponseBufferingAdaptive() ? "adaptive" : "full");
                if (statement.inOutParam != null) {
                    attributes.put("mssql.statement.parameter_count", (long) statement.inOutParam.length);
                }
                if (statement.executeMethod == SQLServerStatement.EXECUTE_BATCH) {
                    attributes.put("mssql.statement.batch_size", (long) statement.statementBatchSize());
                }
            } else if (activity == PerformanceActivity.STATEMENT_ATTEMPT) {
                attributes.put("mssql.statement.attempt", ++root.attemptCount);
                attributes.put("mssql.statement.attempt_reason", root.nextAttemptReason);
                if (root.attemptCount > 1) {
                    root.retryCount++;
                }
                root.nextAttemptReason = "configurable_retry";
            } else if (parent != null && parent.attributes.containsKey("mssql.statement.attempt")) {
                attributes.put("mssql.statement.attempt", parent.attributes.get("mssql.statement.attempt"));
            }
            if ("server_call".equals(activity.statementPhase())) {
                attributes.put("mssql.statement.protocol", statement.statementProtocol());
                attributes.put("mssql.statement.protocol_operation", statement.statementProtocolOperation(activity));
            }
            if (activity == PerformanceActivity.STATEMENT_FIRST_SERVER_RESPONSE) {
                attributes.put("mssql.statement.response_buffering",
                        statement.getIsResponseBufferingAdaptive() ? "adaptive" : "full");
            }
        }

        boolean isOwner() {
            return owner == Thread.currentThread();
        }

        void fail(Exception exception, String resourceKey) {
            if (exception != null && isOwner() && failure == null) {
                failure = pending == null ? new Failure(exception, this, resourceKey) : pending;
                for (Node ancestor = parent; ancestor != null; ancestor = ancestor.parent) {
                    ancestor.pending = failure;
                }
                if (failure.origin == id && "timeout".equals(failure.error.category)) {
                    Map<String, Object> timeout = new LinkedHashMap<>();
                    timeout.put("mssql.statement.attempt", root.attemptCount);
                    timeout.put("mssql.timeout.phase", failure.error.phase);
                    timeout.put("mssql.timeout.kind", "first_response".equals(failure.error.phase)
                            ? "socket_read" : "query");
                    timeout.put("mssql.timeout.value", (double) Math.max(0, statement.queryTimeout));
                    timeout.put("error.type", failure.error.errorType);
                    root.diagnostic("mssql.driver.timeout", timeout);
                }
            }
        }

        private void diagnostic(String name, Map<String, Object> values) {
            if (diagnosticEvents == null) {
                diagnosticEvents = new ArrayList<>();
            }
            if (diagnosticEvents.size() == MAX_DIAGNOSTIC_EVENTS) {
                return;
            }
            Map<String, Object> event = new LinkedHashMap<>();
            event.put("name", name);
            event.put("timestamp", startEpochNanos + Math.max(0, System.nanoTime() - startNanos));
            event.put("attributes", Collections.unmodifiableMap(new LinkedHashMap<>(values)));
            diagnosticEvents.add(Collections.unmodifiableMap(event));
        }

        PerformanceLogEvent event(boolean end) {
            long duration = end ? Math.max(0, System.nanoTime() - startNanos) : 0;
            Map<String, Object> values = new LinkedHashMap<>(attributes);
            Map<String, Object> errors = Collections.emptyMap();
            Failure terminal = end ? failure : null;
            if (end && activity == PerformanceActivity.STATEMENT_ATTEMPT) {
                values.put("mssql.statement.attempt_outcome", outcome(terminal));
            }
            if (end && activity == PerformanceActivity.STATEMENT_INVOCATION) {
                values.put("mssql.statement.attempt_count", attemptCount);
                values.put("mssql.statement.retry_count", retryCount);
                values.put("mssql.statement.outcome", outcome(terminal));
                values.put("mssql.statement.result_kind", statement.resultSet != null ? "result_set"
                        : statement.updateCount != -1 ? "update_count" : "none");
                if (statement.updateCount >= 0) {
                    values.put("mssql.statement.update_count", statement.updateCount);
                }
            }
            if (terminal != null) {
                values.put("mssql.error.category", terminal.error.category);
                values.put("error.type", terminal.error.errorType);
                if (activity == PerformanceActivity.STATEMENT_INVOCATION) {
                    values.put("mssql.statement.failure_phase", terminal.error.phase);
                }
                if (terminal.origin == id) {
                    errors = terminal.error.attributes;
                }
            }
            return new PerformanceLogEvent(end ? PerformanceLogEvent.Type.END : PerformanceLogEvent.Type.START, id,
                    parent == null ? 0 : parent.id, root.id, statement.connection.getConnectionID(),
                    statement.getStatementID(), activity, startEpochNanos, end ? startEpochNanos + duration : 0,
                    duration, terminal == null ? null : terminal.exception,
                    terminal == null ? null : terminal.error.phase, values, errors,
                    end && activity == PerformanceActivity.STATEMENT_INVOCATION && diagnosticEvents != null
                            ? diagnosticEvents : Collections.emptyList());
        }

        private static String outcome(Failure failure) {
            if (failure == null) {
                return "success";
            }
            return "timeout".equals(failure.error.category) || "canceled".equals(failure.error.category)
                    ? failure.error.category : "failure";
        }

        private static String api(int method) {
            switch (method) {
                case SQLServerStatement.EXECUTE_QUERY:
                    return "execute_query";
                case SQLServerStatement.EXECUTE_UPDATE:
                    return "execute_update";
                case SQLServerStatement.EXECUTE_BATCH:
                    return "execute_batch";
                case SQLServerStatement.EXECUTE:
                    return "execute";
                default:
                    return "internal";
            }
        }

        private static String operation(String sql) {
            if (sql == null) {
                return "unknown";
            }
            String value = sql.trim().toLowerCase(java.util.Locale.ROOT);
            String[] known = {"select", "insert", "update", "delete", "merge", "call", "exec", "create", "alter", "drop"};
            for (String candidate : known) {
                if (value.startsWith(candidate + " ") || value.equals(candidate) || value.startsWith(candidate + "(")) {
                    if ("exec".equals(candidate)) {
                        return "call";
                    }
                    if ("create".equals(candidate) || "alter".equals(candidate) || "drop".equals(candidate)) {
                        return "ddl";
                    }
                    return candidate;
                }
            }
            return "unknown";
        }
    }
}
