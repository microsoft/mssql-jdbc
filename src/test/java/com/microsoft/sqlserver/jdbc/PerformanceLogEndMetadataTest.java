/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.net.SocketTimeoutException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


class PerformanceLogEndMetadataTest {
    private final PerformanceLogLifecycleTest.Collector collector = new PerformanceLogLifecycleTest.Collector();
    private SQLServerConnection connection;

    @BeforeEach
    void setup() throws Exception {
        connection = new SQLServerConnection("test");
        PerformanceLog.registerCallback(collector);
    }

    @AfterEach
    void cleanup() {
        PerformanceLog.unregisterCallback();
    }

    @Test
    void connectionChildrenInheritOnlyValidatedAuthenticationAtEnd() {
        Properties settings = new Properties();
        settings.setProperty("authentication", "SqlPassword");
        settings.setProperty("password", "SECRET");
        try (PerformanceLog.Scope root = connectionScope(PerformanceActivity.CONNECTION)) {
            try (PerformanceLog.Scope configuration = connectionScope(PerformanceActivity.CONNECTION_CONFIGURATION)) {
                PerformanceLog.setConnectionSettings(connection, settings);
            }
            try (PerformanceLog.Scope attempt = connectionScope(PerformanceActivity.CONNECTION_ATTEMPT);
                    PerformanceLog.Scope dns = connectionScope(PerformanceActivity.DNS)) {}
        }
        for (PerformanceLogEvent event : collector.ends) {
            assertEquals("sql_password", event.getAttributes().get("mssql.authentication.method"));
            assertFalse(event.getAttributes().toString().contains("SECRET"));
        }
        assertFalse(collector.starts.get(1).getAttributes().containsKey("mssql.authentication.method"));
        assertEquals("unknown", collector.starts.get(0).getAttributes().get("mssql.authentication.method"));
    }

    @Test
    void statementChildrenKeepTypeProtocolAndTerminalOutcome() throws Exception {
        SQLServerStatement statement = statement();
        // Simulate the selected direct-query path; construction alone has not selected a protocol operation.
        statement.executedSqlDirectly = true;
        statement.queryTimeout = 7;
        try (PerformanceLog.Scope root = PerformanceLog.createStatementInvocationScope(statement, "select 123")) {
            try (PerformanceLog.Scope attempt = PerformanceLog.createStatementAttemptScope(statement, "select 123")) {
                try (PerformanceLog.Scope call = statementScope(statement, PerformanceActivity.STATEMENT_EXECUTE);
                        PerformanceLog.Scope response = statementScope(statement,
                                PerformanceActivity.STATEMENT_FIRST_SERVER_RESPONSE)) {
                    SQLTimeoutException failure = new SQLTimeoutException("SECRET");
                    response.setException(failure);
                    call.setException(failure);
                    attempt.setException(failure);
                    root.setException(failure);
                }
            }
        }
        for (PerformanceLogEvent event : collector.ends) {
            assertEquals("statement", event.getAttributes().get("mssql.statement.type"));
            assertEquals("timeout", event.getAttributes().get("mssql.error.category"));
            assertFalse(event.getAttributes().toString().contains("SECRET"));
        }
        assertEquals("direct_sql", collector.ends.get(1).getAttributes().get("mssql.statement.protocol_operation"));
        PerformanceLogEvent root = lastEnd();
        assertEquals("timeout", root.getAttributes().get("mssql.statement.outcome"));
        assertEquals(1L, root.getAttributes().get("mssql.statement.attempt_count"));
        assertEquals(0L, root.getAttributes().get("mssql.statement.retry_count"));
        assertDiagnostic(root, "mssql.driver.timeout", "mssql.timeout.value", 7.0);
        assertRootOnlyDiagnostics();
    }

    @Test
    void connectionSingleBegunReasonIgnoresLaterUnstartedFailover() {
        try (PerformanceLog.Scope root = connectionScope(PerformanceActivity.CONNECTION)) {
            attempt();
            ConnectionPerformanceState.retry(connection, ConnectionPerformanceState.RetryDecision.RETRY_SCHEDULED,
                    false, 100);
            attempt();
            ConnectionPerformanceState.retry(connection, ConnectionPerformanceState.RetryDecision.RETRY_SCHEDULED, true,
                    200);
        }
        assertEquals(2L, lastEnd().getAttributes().get("mssql.connection.attempt_count"));
        assertEquals(1L, lastEnd().getAttributes().get("mssql.connection.retry_count"));
        assertEquals("retry", lastEnd().getAttributes().get("mssql.retry.reason"));
        assertEquals(2, lastEnd().getDiagnosticEvents().size());
        assertRootOnlyDiagnostics();
    }

    @Test
    void connectionRedirectAndFailoverAreActualBegunReasons() {
        for (boolean failover : new boolean[] {false, true}) {
            try (PerformanceLog.Scope root = connectionScope(PerformanceActivity.CONNECTION)) {
                try (PerformanceLog.Scope attempt = connectionScope(PerformanceActivity.CONNECTION_ATTEMPT)) {
                    if (!failover) {
                        ConnectionPerformanceState.redirect(connection, false);
                    }
                }
                if (failover) {
                    ConnectionPerformanceState.retry(connection,
                            ConnectionPerformanceState.RetryDecision.RETRY_SCHEDULED, true, 0);
                }
                attempt();
            }
            assertEquals(failover ? "failover" : "redirect", lastEnd().getAttributes().get("mssql.retry.reason"));
            assertEquals(2L, lastEnd().getAttributes().get("mssql.connection.attempt_count"));
        }
    }

    @Test
    void mixedBegunReasonsHaveNoSingleRootReason() {
        try (PerformanceLog.Scope root = connectionScope(PerformanceActivity.CONNECTION)) {
            try (PerformanceLog.Scope attempt = connectionScope(PerformanceActivity.CONNECTION_ATTEMPT)) {
                ConnectionPerformanceState.redirect(connection, false);
            }
            attempt();
            ConnectionPerformanceState.retry(connection, ConnectionPerformanceState.RetryDecision.RETRY_SCHEDULED, true,
                    0);
            attempt();
        }
        assertEquals(3L, lastEnd().getAttributes().get("mssql.connection.attempt_count"));
        assertFalse(lastEnd().getAttributes().containsKey("mssql.retry.reason"));
    }

    @Test
    void schedulingWithoutBeginningAnotherAttemptNeverAttributesRootRetry() throws Exception {
        try (PerformanceLog.Scope root = connectionScope(PerformanceActivity.CONNECTION)) {
            attempt();
            ConnectionPerformanceState.retry(connection, ConnectionPerformanceState.RetryDecision.RETRY_SCHEDULED, true,
                    0);
        }
        assertEquals(1L, lastEnd().getAttributes().get("mssql.connection.attempt_count"));
        assertEquals(0L, lastEnd().getAttributes().get("mssql.connection.retry_count"));
        assertFalse(lastEnd().getAttributes().containsKey("mssql.retry.reason"));
        SQLServerStatement statement = statement();
        try (PerformanceLog.Scope root = PerformanceLog.createStatementInvocationScope(statement, "select 1")) {
            try (PerformanceLog.Scope attempt = PerformanceLog.createStatementAttemptScope(statement, "select 1")) {}
            PerformanceLog.recordStatementRetry(statement, 100);
        }
        assertEquals(1L, lastEnd().getAttributes().get("mssql.statement.attempt_count"));
        assertEquals(0L, lastEnd().getAttributes().get("mssql.statement.retry_count"));
        assertFalse(lastEnd().getAttributes().containsKey("mssql.retry.reason"));
        assertEquals(1, lastEnd().getDiagnosticEvents().size());
    }

    @Test
    void statementRetriesExposeReasonAndImmutableRootDecisionsAfterRecovery() throws Exception {
        SQLServerStatement statement = statement();
        try (PerformanceLog.Scope root = PerformanceLog.createStatementInvocationScope(statement, "select 1")) {
            try (PerformanceLog.Scope attempt = PerformanceLog.createStatementAttemptScope(statement, "select 1")) {
                attempt.setException(new SQLException("SECRET"));
            }
            PerformanceLog.recordStatementRetry(statement, 250);
            try (PerformanceLog.Scope attempt = PerformanceLog.createStatementAttemptScope(statement, "select 1")) {}
            PerformanceLog.recordStatementRetry(statement, 500);
            try (PerformanceLog.Scope attempt = PerformanceLog.createStatementAttemptScope(statement, "select 1")) {}
        }
        PerformanceLogEvent root = lastEnd();
        assertEquals(3L, root.getAttributes().get("mssql.statement.attempt_count"));
        assertEquals(2L, root.getAttributes().get("mssql.statement.retry_count"));
        assertEquals("configurable_retry", root.getAttributes().get("mssql.retry.reason"));
        assertEquals("success", root.getAttributes().get("mssql.statement.outcome"));
        assertNull(root.getException());
        assertEquals(2, root.getDiagnosticEvents().size());
        assertDiagnostic(root, "mssql.driver.statement.retry_decision", "mssql.retry.delay", 0.25);
        assertRootOnlyDiagnostics();
    }

    @Test
    void connectionTimeoutAndApprovedAuthenticationRemainRootOnlyDiagnostics() {
        Properties settings = new Properties();
        settings.setProperty("authentication", "NotSpecified");
        settings.setProperty("tokenCallback", "true");
        try (PerformanceLog.Scope root = connectionScope(PerformanceActivity.CONNECTION)) {
            PerformanceLog.setConnectionSettings(connection, settings);
            try (PerformanceLog.Scope attempt = connectionScope(PerformanceActivity.CONNECTION_ATTEMPT)) {
                try (PerformanceLog.Scope token = connectionScope(PerformanceActivity.TOKEN_REQUEST)) {
                    ConnectionPerformanceState.authentication(connection, "SECRET");
                    ConnectionPerformanceState.authentication(connection, "callback");
                    SocketTimeoutException failure = new SocketTimeoutException("SECRET");
                    token.setException(failure);
                    attempt.setException(failure);
                    root.setException(failure);
                }
            }
        }
        assertEquals(2, lastEnd().getDiagnosticEvents().size());
        assertDiagnostic(lastEnd(), "mssql.driver.authentication", "mssql.authentication.token_source", "callback");
        assertDiagnostic(lastEnd(), "mssql.driver.timeout", "mssql.timeout.kind", "socket_read");
        assertRootOnlyDiagnostics();
    }

    @Test
    void inheritedEndDimensionsRejectUnknownValuesWithoutCopyingOtherRootMetadata() throws Exception {
        try (PerformanceLog.Scope root = connectionScope(PerformanceActivity.CONNECTION)) {
            ConnectionPerformanceState.current(connection).attributes.put("mssql.authentication.method", "SECRET");
            try (PerformanceLog.Scope dns = connectionScope(PerformanceActivity.DNS)) {}
        }
        assertFalse(collector.ends.get(0).getAttributes().containsKey("mssql.authentication.method"));
        assertFalse(collector.ends.get(0).getAttributes().containsKey("mssql.connection.guid"));

        SQLServerStatement statement = statement();
        try (PerformanceLog.Scope root = PerformanceLog.createStatementInvocationScope(statement, "select 123")) {
            StatementPerformanceState.current(statement).attributes.put("mssql.statement.type", "SECRET");
            try (PerformanceLog.Scope attempt = PerformanceLog.createStatementAttemptScope(statement, "select 123")) {}
        }
        PerformanceLogEvent attempt = collector.ends.get(collector.ends.size() - 2);
        assertFalse(attempt.getAttributes().containsKey("mssql.statement.type"));
        assertFalse(attempt.getAttributes().containsKey("mssql.statement.query_timeout"));
    }

    @Test
    void retryReasonRegistriesCannotCrossOperationKinds() throws Exception {
        try (PerformanceLog.Scope root = connectionScope(PerformanceActivity.CONNECTION)) {
            attempt();
            ConnectionPerformanceState.current(connection).nextAttemptReason = "configurable_retry";
            attempt();
        }
        assertFalse(lastEnd().getAttributes().containsKey("mssql.retry.reason"));
        SQLServerStatement statement = statement();
        try (PerformanceLog.Scope root = PerformanceLog.createStatementInvocationScope(statement, "select 1")) {
            try (PerformanceLog.Scope attempt = PerformanceLog.createStatementAttemptScope(statement, "select 1")) {}
            StatementPerformanceState.current(statement).nextAttemptReason = "failover";
            try (PerformanceLog.Scope attempt = PerformanceLog.createStatementAttemptScope(statement, "select 1")) {}
        }
        assertFalse(lastEnd().getAttributes().containsKey("mssql.retry.reason"));
    }

    @Test
    void protocolEndMetadataRetainsTheObservedServerCallRatherThanLaterHandleState() throws Exception {
        SQLServerStatement statement = statement();
        statement.executedSqlDirectly = true;
        try (PerformanceLog.Scope root = PerformanceLog.createStatementInvocationScope(statement, "select 1");
                PerformanceLog.Scope attempt = PerformanceLog.createStatementAttemptScope(statement, "select 1");
                PerformanceLog.Scope call = statementScope(statement, PerformanceActivity.STATEMENT_EXECUTE)) {
            statement.executedSqlDirectly = false;
        }
        PerformanceLogEvent call = collector.ends.get(0);
        assertEquals("direct_sql", call.getAttributes().get("mssql.statement.protocol_operation"));
        assertEquals("sql_batch", call.getAttributes().get("mssql.statement.protocol"));
        assertFalse(lastEnd().getAttributes().containsKey("mssql.statement.protocol_operation"));
    }

    @Test
    void diagnosticSnapshotsDefensivelyFreezeDecisionMapsAndAsyncProjections() {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("mssql.retry.reason", "retry");
        Map<String, Object> decision = new LinkedHashMap<>();
        decision.put("name", "mssql.driver.retry");
        decision.put("timestamp", 20L);
        decision.put("attributes", values);
        List<Map<String, Object>> decisions = new ArrayList<>();
        decisions.add(decision);
        PerformanceLogEvent event = new PerformanceLogEvent(PerformanceLogEvent.Type.END, 1, 0, 1, 1,
                PerformanceActivity.CONNECTION, 10, 30, 20, new SQLException("SECRET"), "login", Collections.emptyMap(),
                Collections.emptyMap(), decisions);
        PerformanceLogEvent snapshot = event.withoutException();
        values.put("mssql.retry.reason", "SECRET");
        decision.put("name", "SECRET");
        decisions.clear();
        assertEquals("mssql.driver.retry", snapshot.getDiagnosticEvents().get(0).get("name"));
        assertDiagnostic(snapshot, "mssql.driver.retry", "mssql.retry.reason", "retry");
        assertNull(snapshot.getException());
        assertTrue(snapshot.hasException());
        assertSame(event.getDiagnosticEvents(), snapshot.getDiagnosticEvents());
        assertThrows(UnsupportedOperationException.class, () -> snapshot.getDiagnosticEvents().get(0).clear());
        assertThrows(UnsupportedOperationException.class,
                () -> ((Map<?, ?>) snapshot.getDiagnosticEvents().get(0).get("attributes")).clear());
    }

    @Test
    void rootDecisionStorageStaysBoundedAndNeverLeaksIntoStartsOrChildren() throws Exception {
        try (PerformanceLog.Scope root = connectionScope(PerformanceActivity.CONNECTION)) {
            attempt();
            for (int i = 0; i < 140; i++) {
                ConnectionPerformanceState.retry(connection, ConnectionPerformanceState.RetryDecision.RETRY_SCHEDULED,
                        false, i);
            }
        }
        assertEquals(128, lastEnd().getDiagnosticEvents().size());
        assertEquals(12L, lastEnd().getAttributes().get("mssql.connection.diagnostic_events_dropped"));
        assertFalse(lastEnd().getAttributes().containsKey("mssql.retry.reason"));
        SQLServerStatement statement = statement();
        try (PerformanceLog.Scope root = PerformanceLog.createStatementInvocationScope(statement, "select 1")) {
            try (PerformanceLog.Scope attempt = PerformanceLog.createStatementAttemptScope(statement, "select 1")) {}
            for (int i = 0; i < 70; i++) {
                PerformanceLog.recordStatementRetry(statement, i);
            }
        }
        assertEquals(64, lastEnd().getDiagnosticEvents().size());
        assertFalse(lastEnd().getAttributes().containsKey("mssql.retry.reason"));
        assertRootOnlyDiagnostics();
    }

    private PerformanceLog.Scope connectionScope(PerformanceActivity activity) {
        return PerformanceLog.createConnectionScope(connection, activity);
    }

    private void attempt() {
        try (PerformanceLog.Scope attempt = connectionScope(PerformanceActivity.CONNECTION_ATTEMPT)) {}
    }

    private SQLServerStatement statement() throws Exception {
        // Satisfy local statement-construction guards only; no socket, login or SQL execution occurs.
        Field state = SQLServerConnection.class.getDeclaredField("state");
        state.setAccessible(true);
        for (Object value : state.getType().getEnumConstants()) {
            if ("OPENED".equals(value.toString())) {
                state.set(connection, value);
            }
        }
        Field buffering = SQLServerConnection.class.getDeclaredField("responseBuffering");
        buffering.setAccessible(true);
        buffering.set(connection, "adaptive");
        return new SQLServerStatement(connection, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                SQLServerStatementColumnEncryptionSetting.USE_CONNECTION_SETTING);
    }

    private PerformanceLog.Scope statementScope(SQLServerStatement statement, PerformanceActivity activity) {
        return PerformanceLog.createScope(PerformanceLog.perfLoggerStatement, connection, statement.getStatementID(),
                statement, "select 123", activity);
    }

    private PerformanceLogEvent lastEnd() {
        return collector.ends.get(collector.ends.size() - 1);
    }

    private void assertDiagnostic(PerformanceLogEvent root, String name, String key, Object value) {
        assertTrue(root.getDiagnosticEvents().stream().anyMatch(event -> name.equals(event.get("name"))
                && value.equals(((Map<?, ?>) event.get("attributes")).get(key))));
    }

    private void assertRootOnlyDiagnostics() {
        assertTrue(collector.starts.stream().allMatch(event -> event.getDiagnosticEvents().isEmpty()));
        for (PerformanceLogEvent event : collector.ends) {
            if (event.getParentScopeId() != 0) {
                assertTrue(event.getDiagnosticEvents().isEmpty());
            }
            List<Map<String, Object>> diagnostics = event.getDiagnosticEvents();
            assertThrows(UnsupportedOperationException.class, diagnostics::clear);
            for (Map<String, Object> diagnostic : diagnostics) {
                long timestamp = (Long) diagnostic.get("timestamp");
                assertTrue(timestamp >= event.getStartEpochNanos() && timestamp <= event.getEndEpochNanos());
                assertFalse(diagnostic.toString().contains("SECRET"));
                assertThrows(UnsupportedOperationException.class, diagnostic::clear);
                assertThrows(UnsupportedOperationException.class,
                    () -> ((Map<?, ?>) diagnostic.get("attributes")).clear());
            }
        }
    }
}
