/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import java.util.Arrays;
import java.util.Map;

import com.microsoft.sqlserver.jdbc.PerformanceActivity;
import com.microsoft.sqlserver.jdbc.PerformanceLogCallback;
import com.microsoft.sqlserver.jdbc.PerformanceLogEvent;
import com.microsoft.sqlserver.jdbc.StatementType;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.DoubleHistogram;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.Meter;


/** Records bounded all-operation metrics from the same completed lifecycle boundaries used to assemble spans. */
final class PerformanceActivityMetrics {
    private static final String SCHEMA_VERSION = "1.0";
    private static final java.util.List<Double> DURATION_BUCKETS_SECONDS = Arrays.asList(0.0001, 0.00025, 0.0005,
            0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0);

    private final LongCounter operations;
    private final DoubleHistogram durations;
    private final LongCounter errors;
    private final LongCounter retries;

    PerformanceActivityMetrics(OpenTelemetry telemetry, String scope) {
        Meter meter = telemetry.getMeter(scope);
        operations = meter.counterBuilder("db.client.operation.count").setUnit("{operation}")
                .setDescription("Completed JDBC driver performance activities").build();
        durations = meter.histogramBuilder("db.client.operation.duration").setUnit("s")
                .setDescription("JDBC driver performance activity duration")
                .setExplicitBucketBoundariesAdvice(DURATION_BUCKETS_SECONDS).build();
        errors = meter.counterBuilder("db.client.operation.error.count").setUnit("{error}")
                .setDescription("Completed JDBC driver performance activities with a terminal error").build();
        retries = meter.counterBuilder("db.client.operation.retry.count").setUnit("{retry}")
                .setDescription("Additional JDBC connection or statement attempts begun").build();
    }

    void record(PerformanceLogEvent event, PerformanceLogCallback callback) {
        if (event.getType() != PerformanceLogEvent.Type.END || event.getDurationNanos() < 0) {
            return;
        }
        String activity = activity(event.getActivity());
        if (activity == null) {
            return;
        }
        boolean statement = event.getStatementId() != 0
                || event.getActivity() == PerformanceActivity.STATEMENT_INVOCATION;
        String outcome = outcome(event, statement);
        AttributesBuilder attributes = Attributes.builder()
                .put("mssql.performance.activity", activity)
                .put("mssql.operation.kind", statement ? "statement" : "connection")
                .put("mssql.operation.outcome", outcome)
                .put("mssql.telemetry.schema.version", SCHEMA_VERSION);

        if (statement) {
            attributes.put("mssql.statement.type", statementType(callback.getCurrentStatementType()));
            Object protocol = event.getAttributes().get("mssql.statement.protocol_operation");
            String protocolValue = protocolOperation(protocol);
            if (protocolValue != null) {
                attributes.put("mssql.statement.protocol_operation", protocolValue);
            }
        } else {
            attributes.put("mssql.connection.auth_method", authMethod(event.getAttributes()));
        }

        Attributes dimensions = attributes.build();
        operations.add(1, dimensions);
        durations.record(event.getDurationNanos() / 1_000_000_000.0, dimensions);
        if (!"success".equals(outcome)) {
            errors.add(1, dimensions);
        }

        if (event.getActivity() == PerformanceActivity.CONNECTION
                || event.getActivity() == PerformanceActivity.STATEMENT_INVOCATION) {
            Object value = event.getAttributes().get(statement ? "mssql.statement.retry_count"
                                                                : "mssql.connection.retry_count");
            if (value instanceof Number) {
                long count = ((Number) value).longValue();
                if (count > 0) {
                    retries.add(count, dimensions);
                }
            }
        }
    }

    private static String outcome(PerformanceLogEvent event, boolean statement) {
        Map<String, Object> values = event.getAttributes();
        Object value = values.get(statement ? "mssql.statement.outcome" : "mssql.connection.outcome");
        if (value == null) {
            value = values.get(statement ? "mssql.statement.attempt_outcome"
                                         : "mssql.connection.attempt_outcome");
        }
        if ("success".equals(value) || "failure".equals(value) || "timeout".equals(value)
                || "canceled".equals(value)) {
            return (String) value;
        }
        Object category = values.get("mssql.error.category");
        if ("timeout".equals(category) || "canceled".equals(category)) {
            return (String) category;
        }
        return event.hasException() || category != null ? "failure" : "success";
    }

    private static String statementType(StatementType value) {
        if (value == StatementType.PREPARED_STATEMENT) {
            return "prepared_statement";
        }
        if (value == StatementType.CALLABLE_STATEMENT) {
            return "callable_statement";
        }
        return value == StatementType.STATEMENT ? "statement" : "not_applicable";
    }

    private static String protocolOperation(Object value) {
        if (value instanceof String && Arrays.asList("direct_sql", "direct_sql_batch", "sp_executesql",
                "sp_prepexec", "sp_prepare", "sp_execute", "prepared_batch", "cursor_open",
                "cursor_prepexec", "cursor_execute", "bulk_copy").contains(value)) {
            return (String) value;
        }
        return null;
    }

    private static String authMethod(Map<String, Object> values) {
        Object value = values.get("mssql.authentication.method");
        if (value instanceof String && Arrays.asList("sql_password", "entra_password", "entra_integrated",
                "managed_identity", "service_principal_secret", "service_principal_certificate", "interactive",
                "default_credential", "access_token_callback", "access_token", "integrated_kerberos",
                "integrated_ntlm", "integrated_native", "unknown").contains(value)) {
            return (String) value;
        }
        return "not_recorded";
    }

    private static String activity(PerformanceActivity value) {
        switch (value) {
            case CONNECTION:
                return "connection.open";
            case PRELOGIN:
                return "connection.prelogin";
            case CONNECTION_CONFIGURATION:
                return "connection.configuration";
            case CONNECTION_ATTEMPT:
                return "connection.attempt";
            case INSTANCE_DISCOVERY:
                return "connection.instance_discovery";
            case DNS:
                return "connection.dns";
            case SOCKET_CONNECT:
                return "connection.socket_connect";
            case TLS:
                return "connection.tls";
            case LOGIN_EXCHANGE:
                return "connection.login";
            case TOKEN_REQUEST:
                return "connection.token_request";
            case CONNECTION_INITIALIZE:
                return "connection.initialize";
            case CONNECTION_REDIRECT:
                return "connection.redirect";
            case STATEMENT_INVOCATION:
                return "statement.execute";
            case STATEMENT_ATTEMPT:
                return "statement.attempt";
            case STATEMENT_REQUEST_BUILD:
                return "statement.request_build";
            case STATEMENT_FIRST_SERVER_RESPONSE:
                return "statement.first_response";
            case STATEMENT_PREPARE:
                return "statement.server_call.prepare";
            case STATEMENT_PREPEXEC:
                return "statement.server_call.prepexec";
            case STATEMENT_EXECUTE:
                return "statement.server_call.execute";
            default:
                return null;
        }
    }
}