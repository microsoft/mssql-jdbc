/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;


/** Strict statement telemetry allowlist. SQL text, parameter values and handles are never inspected or copied. */
final class StatementAttributePolicy {
    private static final Set<String> PHASES = values("request_build", "server_call", "first_response", "attempt",
            "unknown");
    private static final Set<String> CATEGORIES = values("query_syntax_semantics", "constraint_violation",
            "authorization", "concurrency_conflict", "server_availability", "resource_throttling",
            "database_error", "timeout", "canceled", "network_connectivity", "connection_lifecycle",
            "connection_recovery", "configuration", "parameter_validation", "result_contract", "batch_operation",
            "encryption_security", "data_conversion", "protocol_error", "client_resource_exhaustion",
            "internal_error", "unknown");
    private static final Set<String> EXCEPTION_TYPES = values("java.sql.SQLTimeoutException",
            "java.net.SocketTimeoutException", "java.net.SocketException",
            "com.microsoft.sqlserver.jdbc.SQLServerException");

    private StatementAttributePolicy() {}

    private static Set<String> values(String... values) {
        return new HashSet<>(Arrays.asList(values));
    }

    static String category(Object value) {
        return CATEGORIES.contains(value) ? (String) value : "unknown";
    }

    static String phase(Object value) {
        return PHASES.contains(value) ? (String) value : "unknown";
    }

    static String errorType(Object value) {
        String text = value instanceof String ? (String) value : "unknown";
        if (EXCEPTION_TYPES.contains(text) || text.matches("sqlserver\\.[0-9]{1,10}")
            || text.matches("jdbc\\.R_[A-Za-z0-9_]{1,96}")) {
            return text;
        }
        return "unknown";
    }

    static Attributes span(Map<String, Object> input, boolean root, boolean attempt, String phase) {
        AttributesBuilder output = Attributes.builder().put("db.system.name", "microsoft.sql_server");
        if (input.containsKey("mssql.error.category")) {
            output.put("mssql.error.category", category(input.get("mssql.error.category")));
        }
        if (input.containsKey("error.type")) {
            output.put("error.type", errorType(input.get("error.type")));
        }
        integer(input, output, "mssql.statement.attempt", Integer.MAX_VALUE);
        if (attempt) {
            enumeration(input, output, "mssql.statement.attempt_reason", "initial", "configurable_retry",
                    "invalid_handle_retry", "enclave_retry");
            enumeration(input, output, "mssql.statement.attempt_outcome", "success", "failure", "timeout",
                    "canceled");
        }
        if ("server_call".equals(phase)) {
            enumeration(input, output, "mssql.statement.protocol", "sql_batch", "rpc", "bulk_copy");
            enumeration(input, output, "mssql.statement.protocol_operation", "direct_sql", "direct_sql_batch",
                    "sp_executesql", "sp_prepexec", "sp_prepare", "sp_execute", "prepared_batch", "cursor_open",
                    "cursor_prepexec", "cursor_execute", "bulk_copy");
        }
        if ("first_response".equals(phase)) {
            enumeration(input, output, "mssql.statement.response_buffering", "adaptive", "full");
        }
        if (root) {
            enumeration(input, output, "mssql.telemetry.schema.version", "1.0");
            uuid(input, output, "mssql.connection.guid");
            enumeration(input, output, "mssql.statement.type", "statement", "prepared_statement",
                    "callable_statement");
            enumeration(input, output, "mssql.statement.api", "execute_query", "execute_update", "execute",
                    "execute_batch", "execute_large_batch", "internal");
            enumeration(input, output, "mssql.statement.operation", "select", "insert", "update", "delete",
                    "merge", "call", "ddl", "unknown");
            enumeration(input, output, "mssql.statement.outcome", "success", "failure", "timeout", "canceled");
            enumeration(input, output, "mssql.statement.result_kind", "result_set", "update_count", "multiple",
                    "none");
            enumeration(input, output, "mssql.statement.response_buffering", "adaptive", "full");
            if (input.containsKey("mssql.statement.failure_phase")) {
                output.put("mssql.statement.failure_phase", phase(input.get("mssql.statement.failure_phase")));
            }
            seconds(input, output, "mssql.statement.query_timeout");
            integer(input, output, "mssql.statement.parameter_count", Integer.MAX_VALUE);
            integer(input, output, "mssql.statement.batch_size", Integer.MAX_VALUE);
            integer(input, output, "mssql.statement.attempt_count", Integer.MAX_VALUE);
            integer(input, output, "mssql.statement.retry_count", Integer.MAX_VALUE);
            integer(input, output, "mssql.statement.update_count", Long.MAX_VALUE);
            Object query = input.get("db.query.text");
            if (query instanceof String && !((String) query).isEmpty() && ((String) query).length() <= 4096) {
                output.put("db.query.text", (String) query);
            }
        }
        return output.build();
    }

    static Attributes error(Map<String, Object> input) {
        AttributesBuilder output = Attributes.builder();
        output.put("mssql.error.category", category(input.get("mssql.error.category")));
        output.put("mssql.error.phase", phase(input.get("mssql.error.phase")));
        enumeration(input, output, "mssql.error.source", "driver", "sql_server", "jvm", "os", "key_provider",
                "callback", "unknown");
        Object code = input.get("mssql.error.code");
        if (code instanceof String && (((String) code).matches("sqlserver:[0-9]{1,10}")
                || ((String) code).matches("jdbc:R_[A-Za-z0-9_]{1,96}"))) {
            output.put("mssql.error.code", (String) code);
        }
        Object type = input.get("exception.type");
        String safeType = errorType(type);
        if (!"unknown".equals(safeType)) {
            output.put("exception.type", safeType);
        }
        Object state = input.get("mssql.error.sql_state");
        if (state instanceof String && ((String) state).matches("[A-Z0-9]{5}")) {
            output.put("mssql.error.sql_state", (String) state);
        }
        integer(input, output, "mssql.error.driver_code", Integer.MAX_VALUE);
        integer(input, output, "mssql.error.server_state", 255);
        integer(input, output, "mssql.error.server_severity", 25);
        return output.build();
    }

    static Attributes diagnostic(String name, Map<String, Object> input) {
        AttributesBuilder output = Attributes.builder();
        if ("mssql.driver.statement.retry_decision".equals(name)) {
            integer(input, output, "mssql.statement.attempt", Integer.MAX_VALUE);
            integer(input, output, "mssql.retry.attempt", Integer.MAX_VALUE);
            seconds(input, output, "mssql.retry.delay");
            enumeration(input, output, "mssql.error.retry_decision", "retry_scheduled", "not_retryable",
                    "limit_reached", "timeout", "canceled");
            if (input.containsKey("error.type")) {
                output.put("error.type", errorType(input.get("error.type")));
            }
        } else if ("mssql.driver.timeout".equals(name)) {
            output.put("mssql.timeout.phase", phase(input.get("mssql.timeout.phase")));
            enumeration(input, output, "mssql.timeout.kind", "query", "socket_read", "cancel_ack", "unknown");
            seconds(input, output, "mssql.timeout.value");
            integer(input, output, "mssql.statement.attempt", Integer.MAX_VALUE);
            if (input.containsKey("error.type")) {
                output.put("error.type", errorType(input.get("error.type")));
            }
        } else {
            return null;
        }
        return output.build();
    }

    private static void enumeration(Map<String, Object> input, AttributesBuilder output, String key,
            String... allowed) {
        Object value = input.get(key);
        if (Arrays.asList(allowed).contains(value)) {
            output.put(key, (String) value);
        }
    }

    private static void seconds(Map<String, Object> input, AttributesBuilder output, String key) {
        Object value = input.get(key);
        if (value instanceof Number) {
            double number = ((Number) value).doubleValue();
            if (Double.isFinite(number) && number >= 0 && number <= Integer.MAX_VALUE) {
                output.put(key, number);
            }
        }
    }

    private static void integer(Map<String, Object> input, AttributesBuilder output, String key, long max) {
        Object value = input.get(key);
        if (value instanceof Long || value instanceof Integer) {
            long number = ((Number) value).longValue();
            if (number >= 0 && number <= max) {
                output.put(key, number);
            }
        }
    }

    private static void uuid(Map<String, Object> input, AttributesBuilder output, String key) {
        Object value = input.get(key);
        if (value instanceof String && ((String) value)
                .matches("[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}")) {
            output.put(key, (String) value);
        }
    }
}
