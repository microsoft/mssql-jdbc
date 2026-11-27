/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

class ConnectionTelemetryTest {
    @Test
    void decisionsAreBoundedImmutableAndCountOnlyStartedAttempts() throws Exception {
        PerformanceLogLifecycleTest.Collector c = new PerformanceLogLifecycleTest.Collector();
        PerformanceLog.registerCallback(c);
        SQLServerConnection con = new SQLServerConnection("test");
        try (PerformanceLog.Scope root = PerformanceLog.createConnectionScope(con, PerformanceActivity.CONNECTION)) {
            try (PerformanceLog.Scope attempt = PerformanceLog.createConnectionScope(con,
                    PerformanceActivity.CONNECTION_ATTEMPT)) {
                ConnectionPerformanceState.redirect(con, false);
            }
            try (PerformanceLog.Scope attempt = PerformanceLog.createConnectionScope(con,
                    PerformanceActivity.CONNECTION_ATTEMPT)) {
                attempt.setException(new UnknownHostException("SECRET"));
            }
            ConnectionPerformanceState.retry(con, ConnectionPerformanceState.RetryDecision.RETRY_SCHEDULED, false, 500);
            try (PerformanceLog.Scope attempt = PerformanceLog.createConnectionScope(con,
                    PerformanceActivity.CONNECTION_ATTEMPT)) {
                assertEquals("attempt", PerformanceLog.getConnectionPhase(con));
            }
            for (int i = 0; i < 130; i++) {
                ConnectionPerformanceState.retry(con, ConnectionPerformanceState.RetryDecision.LIMIT_REACHED, false, 0);
            }
        }
        PerformanceLogEvent root = c.ends.get(3);
        assertEquals("initial", c.starts.get(1).getAttributes().get("mssql.connection.attempt_reason"));
        assertEquals("redirect", c.starts.get(2).getAttributes().get("mssql.connection.attempt_reason"));
        assertEquals("retry", c.starts.get(3).getAttributes().get("mssql.connection.attempt_reason"));
        assertEquals("redirect", c.ends.get(0).getAttributes().get("mssql.connection.attempt_outcome"));
        assertEquals("failure", c.ends.get(1).getAttributes().get("mssql.connection.attempt_outcome"));
        assertEquals(3L, root.getAttributes().get("mssql.connection.attempt_count"));
        assertEquals(1L, root.getAttributes().get("mssql.connection.retry_count"));
        assertEquals(1L, root.getAttributes().get("mssql.connection.redirect_count"));
        assertEquals(128, root.getDiagnosticEvents().size());
        assertEquals(4L, root.getAttributes().get("mssql.connection.diagnostic_events_dropped"));
        assertNull(root.getException());
        assertThrows(UnsupportedOperationException.class, () -> root.getDiagnosticEvents().clear());
        for (Map<String, Object> event : root.getDiagnosticEvents()) {
            assertTrue((Long) event.get("timestamp") >= root.getStartEpochNanos());
            assertTrue((Long) event.get("timestamp") <= root.getEndEpochNanos());
            assertFalse(event.toString().contains("SECRET"));
            assertThrows(UnsupportedOperationException.class, () -> event.clear());
            assertThrows(UnsupportedOperationException.class, () -> ((Map<?, ?>) event.get("attributes")).clear());
        }
        assertTrue(c.starts.stream().allMatch(e -> e.getDiagnosticEvents().isEmpty()));
        assertTrue(c.ends.stream().filter(e -> e != root).allMatch(e -> e.getDiagnosticEvents().isEmpty()));
    }

    @AfterEach
    void cleanup() {
        PerformanceLog.unregisterCallback();
    }

    @Test
    void safeSettingsImmutableSnapshotsAndAttemptIdentity() throws Exception {
        PerformanceLogLifecycleTest.Collector c = new PerformanceLogLifecycleTest.Collector();
        PerformanceLog.registerCallback(c);
        SQLServerConnection con = new SQLServerConnection("test");
        Properties settings = new Properties();
        settings.setProperty("authentication", "SqlPassword");
        settings.setProperty("encrypt", "strict");
        settings.setProperty("trustServerCertificate", "true");
        settings.setProperty("applicationIntent", "ReadOnly");
        settings.setProperty("socketTimeout", "1500");
        settings.setProperty("loginTimeout", "30");
        settings.setProperty("connectRetryCount", "2");
        settings.setProperty("connectRetryInterval", "10");
        for (String key : new String[] {"password", "user", "serverName", "databaseName", "accessToken",
                "applicationName", "multiSubnetFailover", "trustStore", "hostNameInCertificate"}) {
            settings.setProperty(key, "SECRET");
        }
        UUID id = UUID.randomUUID();
        try (PerformanceLog.Scope root = PerformanceLog.createScope(PerformanceLogLifecycleTest.LOGGER, con,
                PerformanceActivity.CONNECTION)) {
            PerformanceLog.setConnectionSettings(con, settings);
            try (PerformanceLog.Scope attempt = PerformanceLog.createConnectionScope(con,
                    PerformanceActivity.CONNECTION_ATTEMPT)) {
                PerformanceLog.setAttemptClientConnectionId(con, id);
            }
        }
        Map<String, Object> attrs = c.ends.get(1).getAttributes();
        assertEquals("sql_password", attrs.get("mssql.authentication.method"));
        assertEquals("strict", attrs.get("mssql.connection.encrypt"));
        assertEquals(false, attrs.get("mssql.connection.trust_server_certificate"));
        assertEquals(1.5, attrs.get("mssql.connection.socket_timeout"));
        assertEquals(30.0, attrs.get("mssql.connection.login_timeout"));
        assertEquals(2L, attrs.get("mssql.connection.connect_retry_count"));
        assertFalse(attrs.containsKey("mssql.connection.multi_subnet_failover"));
        assertFalse(attrs.containsKey("mssql.connection.client_connection_id"));
        assertNotNull(attrs.get("mssql.connection.guid"));
        assertEquals(id.toString(), c.ends.get(0).getAttributes().get("mssql.connection.client_connection_id"));
        assertFalse(c.starts.get(1).getAttributes().containsKey("mssql.connection.client_connection_id"));
        assertFalse(c.starts.get(0).getAttributes().containsKey("mssql.connection.encrypt"));
        for (PerformanceLogEvent e : c.ends) {
            assertFalse(e.getAttributes().toString().contains("SECRET"));
            assertThrows(UnsupportedOperationException.class, () -> e.getAttributes().put("bad", "value"));
            assertThrows(UnsupportedOperationException.class, () -> e.getErrorAttributes().put("bad", "value"));
        }
    }

    @Test
    void recoveredAttemptDoesNotFailSuccessfulRootOrPoisonNextOpen() throws Exception {
        PerformanceLogLifecycleTest.Collector c = new PerformanceLogLifecycleTest.Collector();
        PerformanceLog.registerCallback(c);
        SQLServerConnection con = new SQLServerConnection("test");
        try (PerformanceLog.Scope root = PerformanceLog.createScope(PerformanceLogLifecycleTest.LOGGER, con,
                PerformanceActivity.CONNECTION)) {
            try (PerformanceLog.Scope attempt = PerformanceLog.createConnectionScope(con,
                    PerformanceActivity.CONNECTION_ATTEMPT)) {
                attempt.setException(new UnknownHostException());
            }
            try (PerformanceLog.Scope retry = PerformanceLog.createConnectionScope(con,
                    PerformanceActivity.CONNECTION_ATTEMPT)) {
                assertEquals("attempt", PerformanceLog.getConnectionPhase(con));
            }
        }
        PerformanceLogEvent end = c.ends.get(2);
        assertNull(end.getException());
        assertNull(end.getFailurePhase());
        assertTrue(end.getErrorAttributes().isEmpty());
        assertFalse(end.getAttributes().containsKey("mssql.error.category"));
        assertEquals("success", end.getAttributes().get("mssql.connection.outcome"));
        assertNull(PerformanceLog.getConnectionPhase(con));
        try (PerformanceLog.Scope root = PerformanceLog.createScope(PerformanceLogLifecycleTest.LOGGER, con,
                PerformanceActivity.CONNECTION)) {
            root.setException(new IllegalArgumentException());
        }
        assertEquals("unknown", c.ends.get(3).getAttributes().get("mssql.error.category"));
        assertNotEquals(c.ends.get(2).getRootScopeId(), c.ends.get(3).getRootScopeId());
    }
    @Test
    void authenticationChildrenCarryValidatedMethodAndCallbackOrigin() throws Exception {
        PerformanceLogLifecycleTest.Collector c = new PerformanceLogLifecycleTest.Collector();
        PerformanceLog.registerCallback(c);
        SQLServerConnection con = new SQLServerConnection("test");
        Properties settings = new Properties();
        settings.setProperty("authentication", "NotSpecified");
        settings.setProperty("tokenCallback", "true");
        RuntimeException failure = new RuntimeException("SECRET");
        try (PerformanceLog.Scope root = PerformanceLog.createConnectionScope(con, PerformanceActivity.CONNECTION)) {
            PerformanceLog.setConnectionSettings(con, settings);
            try (PerformanceLog.Scope attempt = PerformanceLog.createConnectionScope(con,
                    PerformanceActivity.CONNECTION_ATTEMPT)) {
                try (PerformanceLog.Scope login = PerformanceLog.createConnectionScope(con,
                        PerformanceActivity.LOGIN_EXCHANGE)) {
                    try (PerformanceLog.Scope token = PerformanceLog.createConnectionScope(con,
                            PerformanceActivity.TOKEN_REQUEST)) {
                        ConnectionPerformanceState.authentication(con, "callback");
                        PerformanceLog.recordTokenCallbackFailure(con, failure);
                        // The class-based callback wrapper must not overwrite invocation provenance.
                        PerformanceLog.recordConnectionFailure(con, failure, "R_InvalidAccessTokenCallbackClass");
                    }
                    login.setException(failure);
                }
                attempt.setException(failure);
            }
            root.setException(failure);
        }
        assertEquals("callback", c.ends.get(0).getErrorAttributes().get("mssql.error.source"));
        assertEquals("authentication", c.ends.get(0).getAttributes().get("mssql.error.category"));
        for (PerformanceLogEvent e : c.starts) {
            if ("login".equals(e.getPhase()) || "token_acquisition".equals(e.getPhase())) {
                assertEquals("access_token_callback", e.getAttributes().get("mssql.authentication.method"));
            }
        }
        assertEquals("callback", c.ends.get(0).getAttributes().get("mssql.authentication.token_source"));
        assertSame(failure, c.ends.get(3).getException());
        assertEquals("token_acquisition", c.ends.get(3).getFailurePhase());
        assertEquals(1, c.ends.stream().filter(e -> !e.getErrorAttributes().isEmpty()).count());
    }
}