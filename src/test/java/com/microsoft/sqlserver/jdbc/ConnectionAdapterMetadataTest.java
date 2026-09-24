/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;


class ConnectionAdapterMetadataTest {
    @AfterEach
    void cleanup() {
        PerformanceLog.unregisterCallback();
    }

    @Test
    void rootMetadataAndAttemptCountersAreActualAndScoped() throws Exception {
        PerformanceLogLifecycleTest.Collector collector = new PerformanceLogLifecycleTest.Collector();
        PerformanceLog.registerCallback(collector);
        SQLServerConnection connection = new SQLServerConnection("test");
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        try (PerformanceLog.Scope root = PerformanceLog.createConnectionScope(connection,
                PerformanceActivity.CONNECTION)) {
            for (UUID id : new UUID[] {first, second}) {
                try (PerformanceLog.Scope attempt = PerformanceLog.createConnectionScope(connection,
                        PerformanceActivity.CONNECTION_ATTEMPT)) {
                    PerformanceLog.setAttemptClientConnectionId(connection, id);
                    try (PerformanceLog.Scope dns = PerformanceLog.createConnectionScope(connection,
                            PerformanceActivity.DNS)) {
                        assertEquals("dns", PerformanceLog.getConnectionPhase(connection));
                    }
                }
            }
        }
        PerformanceLogEvent rootStart = collector.starts.get(0);
        PerformanceLogEvent rootEnd = collector.ends.get(4);
        assertEquals(SQLServerConnection.userAgentStr,
                rootStart.getAttributes().get("mssql.driver.user_agent.original"));
        assertEquals(SQLServerConnection.userAgentStr, rootEnd.getAttributes().get("mssql.driver.user_agent.original"));
        assertFalse(rootStart.getAttributes().containsKey("mssql.connection.attempt_count"));
        assertEquals(2L, rootEnd.getAttributes().get("mssql.connection.attempt_count"));
        assertEquals(1L, collector.starts.get(1).getAttributes().get("mssql.connection.attempt"));
        assertEquals(2L, collector.starts.get(3).getAttributes().get("mssql.connection.attempt"));
        assertEquals(1L, collector.ends.get(1).getAttributes().get("mssql.connection.attempt"));
        assertEquals(2L, collector.ends.get(3).getAttributes().get("mssql.connection.attempt"));
        assertEquals(first.toString(),
                collector.ends.get(1).getAttributes().get("mssql.connection.client_connection_id"));
        assertEquals(second.toString(),
                collector.ends.get(3).getAttributes().get("mssql.connection.client_connection_id"));
        for (PerformanceLogEvent event : collector.ends) {
            if (event != rootEnd) {
                assertFalse(event.getAttributes().containsKey("mssql.driver.user_agent.original"));
                assertFalse(event.getAttributes().containsKey("mssql.connection.guid"));
                assertFalse(event.getAttributes().containsKey("mssql.connection.attempt_count"));
            }
        }
        try (PerformanceLog.Scope next = PerformanceLog.createConnectionScope(connection,
                PerformanceActivity.CONNECTION)) {
            assertEquals("connection.open", PerformanceLog.getConnectionPhase(connection));
        }
        PerformanceLogEvent next = collector.ends.get(5);
        assertEquals(0L, next.getAttributes().get("mssql.connection.attempt_count"));
        assertNotEquals(rootEnd.getAttributes().get("mssql.connection.guid"),
                next.getAttributes().get("mssql.connection.guid"));
        assertNull(PerformanceLog.getConnectionPhase(connection));
    }

    @Test
    void settingsStayOnOwningRootAndDoNotMutatePublishedSnapshots() throws Exception {
        PerformanceLogLifecycleTest.Collector collector = new PerformanceLogLifecycleTest.Collector();
        PerformanceLog.registerCallback(collector);
        SQLServerConnection connection = new SQLServerConnection("test");
        Properties settings = new Properties();
        settings.setProperty("encrypt", "strict");
        settings.setProperty("trustServerCertificate", "true");
        settings.setProperty("password", "SECRET");
        try (PerformanceLog.Scope outer = PerformanceLog.createConnectionScope(connection,
                PerformanceActivity.CONNECTION)) {
            try (PerformanceLog.Scope configuration = PerformanceLog.createConnectionScope(connection,
                    PerformanceActivity.CONNECTION_CONFIGURATION)) {
                PerformanceLog.setConnectionSettings(connection, settings);
            }
            try (PerformanceLog.Scope inner = PerformanceLog.createConnectionScope(connection,
                    PerformanceActivity.CONNECTION)) {
                settings.setProperty("encrypt", "false");
                PerformanceLog.setConnectionSettings(connection, settings);
                settings.setProperty("encrypt", "SECRET");
            }
            assertEquals("connection.open", PerformanceLog.getConnectionPhase(connection));
            try (PerformanceLog.Scope attempt = PerformanceLog.createConnectionScope(connection,
                    PerformanceActivity.CONNECTION_ATTEMPT)) {
                assertEquals("attempt", PerformanceLog.getConnectionPhase(connection));
            }
        }
        PerformanceLogEvent inner = collector.ends.get(1);
        PerformanceLogEvent outer = collector.ends.get(3);
        assertEquals("false", inner.getAttributes().get("mssql.connection.encrypt"));
        assertEquals(true, inner.getAttributes().get("mssql.connection.trust_server_certificate"));
        assertEquals("strict", outer.getAttributes().get("mssql.connection.encrypt"));
        assertEquals(false, outer.getAttributes().get("mssql.connection.trust_server_certificate"));
        assertNotEquals(inner.getAttributes().get("mssql.connection.guid"),
                outer.getAttributes().get("mssql.connection.guid"));
        assertEquals(0L, inner.getAttributes().get("mssql.connection.attempt_count"));
        assertEquals(1L, outer.getAttributes().get("mssql.connection.attempt_count"));
        for (PerformanceLogEvent start : collector.starts) {
            assertFalse(start.getAttributes().containsKey("mssql.connection.encrypt"));
            assertFalse(start.getAttributes().containsKey("mssql.connection.attempt_count"));
        }
        for (PerformanceLogEvent end : collector.ends) {
            Map<String, Object> attributes = end.getAttributes();
            assertFalse(attributes.toString().contains("SECRET"));
            assertThrows(UnsupportedOperationException.class, () -> attributes.put("bad", "value"));
            if (end != inner && end != outer) {
                assertFalse(attributes.containsKey("mssql.connection.encrypt"));
                assertFalse(attributes.containsKey("mssql.connection.trust_server_certificate"));
                assertFalse(attributes.containsKey("mssql.connection.guid"));
                assertFalse(attributes.containsKey("mssql.driver.user_agent.original"));
            }
        }
        assertNull(PerformanceLog.getConnectionPhase(connection));
    }

    @Test
    void failedAttemptBeforeClientIdAllocationStillCounts() throws Exception {
        PerformanceLogLifecycleTest.Collector collector = new PerformanceLogLifecycleTest.Collector();
        PerformanceLog.registerCallback(collector);
        SQLServerConnection connection = new SQLServerConnection("test");
        Exception failure = new java.net.UnknownHostException();
        try (PerformanceLog.Scope root = PerformanceLog.createConnectionScope(connection,
                PerformanceActivity.CONNECTION)) {
            try (PerformanceLog.Scope attempt = PerformanceLog.createConnectionScope(connection,
                    PerformanceActivity.CONNECTION_ATTEMPT)) {
                attempt.setException(failure);
            }
            root.setException(failure);
        }
        assertEquals(1L, collector.ends.get(0).getAttributes().get("mssql.connection.attempt"));
        assertFalse(collector.ends.get(0).getAttributes().containsKey("mssql.connection.client_connection_id"));
        assertEquals(1L, collector.ends.get(1).getAttributes().get("mssql.connection.attempt_count"));
    }

    @Test
    void reentrantRootsKeepIndependentAttemptCounters() throws Exception {
        PerformanceLogLifecycleTest.Collector collector = new PerformanceLogLifecycleTest.Collector();
        PerformanceLog.registerCallback(collector);
        SQLServerConnection connection = new SQLServerConnection("test");
        try (PerformanceLog.Scope outer = PerformanceLog.createConnectionScope(connection,
                PerformanceActivity.CONNECTION)) {
            try (PerformanceLog.Scope attempt = PerformanceLog.createConnectionScope(connection,
                    PerformanceActivity.CONNECTION_ATTEMPT)) {
                assertEquals("attempt", PerformanceLog.getConnectionPhase(connection));
            }
            try (PerformanceLog.Scope inner = PerformanceLog.createConnectionScope(connection,
                    PerformanceActivity.CONNECTION);
                    PerformanceLog.Scope attempt = PerformanceLog.createConnectionScope(connection,
                            PerformanceActivity.CONNECTION_ATTEMPT)) {
                assertEquals("attempt", PerformanceLog.getConnectionPhase(connection));
            }
            try (PerformanceLog.Scope attempt = PerformanceLog.createConnectionScope(connection,
                    PerformanceActivity.CONNECTION_ATTEMPT)) {
                assertEquals("attempt", PerformanceLog.getConnectionPhase(connection));
            }
        }
        assertEquals(1L, collector.ends.get(0).getAttributes().get("mssql.connection.attempt"));
        assertEquals(1L, collector.ends.get(1).getAttributes().get("mssql.connection.attempt"));
        assertEquals(1L, collector.ends.get(2).getAttributes().get("mssql.connection.attempt_count"));
        assertEquals(2L, collector.ends.get(3).getAttributes().get("mssql.connection.attempt"));
        assertEquals(2L, collector.ends.get(4).getAttributes().get("mssql.connection.attempt_count"));
        assertNull(PerformanceLog.getConnectionPhase(connection));
    }
}
