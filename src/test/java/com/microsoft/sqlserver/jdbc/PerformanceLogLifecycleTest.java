/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.PerformanceLogEvent.Type;


class PerformanceLogLifecycleTest {
    static class Collector implements PerformanceLogCallback {
        final List<PerformanceLogEvent> starts = new ArrayList<>();
        final List<PerformanceLogEvent> ends = new ArrayList<>();
        final List<String> order = new ArrayList<>();
        final List<PerformanceActivity> published = new ArrayList<>();
        long duration;
        boolean nanos;

        @Override
        public void publish(PerformanceLogEvent event) throws Exception {
            switch (event.getType()) {
                case START:
                    starts.add(event);
                    order.add("start:" + event.getPhase());
                    break;
                case END:
                    ends.add(event);
                    order.add("end:" + event.getPhase());
                    break;
                default:
                    fail("Unexpected lifecycle event type");
            }
        }

        @Override
        public boolean useNanoseconds() {
            return nanos;
        }

        @Override
        public void publish(PerformanceActivity activity, int connectionId, long value, Exception exception) {
            published.add(activity);
            duration = value;
            order.add("publish:" + activity.name());
        }

        @Override
        public void publish(PerformanceActivity activity, int connectionId, int statementId, long value,
                Exception exception) {
            publish(activity, connectionId, value, exception);
        }
    }

    static final Logger LOGGER = Logger.getLogger("test.connection.lifecycle");
    static {
        LOGGER.setLevel(Level.OFF);
    }

    @AfterEach
    void cleanup() {
        PerformanceLog.unregisterCallback();
    }

    @Test
    void orderIdentityTimeAndLegacyCounts() throws Exception {
        Collector c = new Collector();
        PerformanceLog.registerCallback(c);
        SQLServerConnection con = new SQLServerConnection("test");
        try (PerformanceLog.Scope root = PerformanceLog.createScope(LOGGER, con, PerformanceActivity.CONNECTION)) {
            try (PerformanceLog.Scope broad = PerformanceLog.createScope(LOGGER, con, PerformanceActivity.LOGIN);
                    PerformanceLog.Scope dns = PerformanceLog.createConnectionScope(con, PerformanceActivity.DNS)) {
                assertEquals("dns", PerformanceLog.getConnectionPhase(con));
            }
            try (PerformanceLog.Scope prelogin = PerformanceLog.createScope(LOGGER, con,
                    PerformanceActivity.PRELOGIN)) {
                assertEquals("prelogin", PerformanceLog.getConnectionPhase(con));
            }
        }
        assertEquals(3, c.starts.size());
        assertEquals(3, c.ends.size());
        assertEquals(3, c.published.size());
        assertEquals(Arrays.asList("start:connection.open", "start:dns", "end:dns", "publish:LOGIN", "start:prelogin",
                "end:prelogin", "publish:PRELOGIN", "end:connection.open", "publish:CONNECTION"), c.order);
        PerformanceLogEvent root = c.starts.get(0);
        assertEquals(0, root.getParentScopeId());
        assertEquals(root.getScopeId(), root.getRootScopeId());
        assertEquals(con.getConnectionID(), root.getConnectionId());
        for (PerformanceLogEvent start : c.starts) {
            PerformanceLogEvent end = c.ends.stream().filter(e -> e.getScopeId() == start.getScopeId()).findFirst()
                    .get();
            assertEquals(Type.START, start.getType());
            assertEquals(Type.END, end.getType());
            assertNull(start.getException());
            assertNull(end.getException());
            assertEquals(start.getParentScopeId(), end.getParentScopeId());
            assertEquals(start.getRootScopeId(), end.getRootScopeId());
            assertEquals(start.getPhase(), end.getPhase());
            assertEquals(0, start.getEndEpochNanos());
            assertEquals(0, start.getDurationNanos());
            assertEquals(start.getStartEpochNanos(), end.getStartEpochNanos());
            assertTrue(end.getEndEpochNanos() >= end.getStartEpochNanos());
            assertEquals(end.getDurationNanos(), end.getEndEpochNanos() - end.getStartEpochNanos());
            assertEquals(root.getScopeId(), start.getRootScopeId());
            if (start != root) {
                assertEquals(root.getScopeId(), start.getParentScopeId());
            }
        }
        assertNull(PerformanceLog.getConnectionPhase(con));
    }

    @Test
    void asyncSnapshotSharesImmutableMetadataButNeverRetainsException() {
        Exception hostile = new RuntimeException("SECRET") {
            private static final long serialVersionUID = 1L;

            @Override
            public synchronized Throwable getCause() {
                throw new AssertionError("Snapshot must not inspect exceptions");
            }
        };
        for (Exception failure : new Exception[] {null, hostile}) {
            PerformanceLogEvent original = new PerformanceLogEvent(Type.END, 2, 1, 1, 7, PerformanceActivity.DNS, 10,
                    30, 20, failure, "dns", Collections.singletonMap("error.type", "unknown"), Collections.emptyMap(),
                    Collections.singletonList(Collections.singletonMap("name", "mssql.driver.retry")));
            PerformanceLogEvent snapshot = original.withoutException();
            assertSame(failure, original.getException());
            assertNull(snapshot.getException());
            assertEquals(failure != null, snapshot.hasException());
            assertEquals(original.hasException(), snapshot.hasException());
            assertSame(snapshot, snapshot.withoutException());
            assertSame(original.getAttributes(), snapshot.getAttributes());
            assertSame(original.getErrorAttributes(), snapshot.getErrorAttributes());
            assertSame(original.getDiagnosticEvents(), snapshot.getDiagnosticEvents());
            assertEquals(original.getType(), snapshot.getType());
            assertEquals(original.getScopeId(), snapshot.getScopeId());
            assertEquals(original.getParentScopeId(), snapshot.getParentScopeId());
            assertEquals(original.getRootScopeId(), snapshot.getRootScopeId());
            assertEquals(original.getConnectionId(), snapshot.getConnectionId());
            assertEquals(original.getActivity(), snapshot.getActivity());
            assertEquals(original.getPhase(), snapshot.getPhase());
            assertEquals(original.getFailurePhase(), snapshot.getFailurePhase());
            assertEquals(original.getStartEpochNanos(), snapshot.getStartEpochNanos());
            assertEquals(original.getEndEpochNanos(), snapshot.getEndEpochNanos());
            assertEquals(original.getDurationNanos(), snapshot.getDurationNanos());
            assertThrows(UnsupportedOperationException.class, () -> snapshot.getAttributes().clear());
            assertThrows(UnsupportedOperationException.class, () -> snapshot.getDiagnosticEvents().clear());
        }
    }

    @Test
    void explicitEventTypeDoesNotDependOnTimestampsOrException() {
        for (Type type : Type.values()) {
            PerformanceLogEvent event = new PerformanceLogEvent(type, 1, 0, 1, 0, PerformanceActivity.CONNECTION, 0, 0,
                    0, null, null, Collections.emptyMap(), Collections.emptyMap());
            assertEquals(type, event.getType());
            assertEquals(0, event.getEndEpochNanos());
            assertEquals(0, event.getDurationNanos());
            assertNull(event.getException());
        }
    }

    @Test
    void endTypeCoversSuccessAndOriginalFailureWithoutMutatingStart() throws Exception {
        Collector c = new Collector();
        PerformanceLog.registerCallback(c);
        SQLServerConnection con = new SQLServerConnection("test");
        Exception original = new UnknownHostException("SECRET");
        for (Exception failure : new Exception[] {null, original}) {
            try (PerformanceLog.Scope root = PerformanceLog.createScope(LOGGER, con, PerformanceActivity.CONNECTION)) {
                root.setException(failure);
            }
        }
        assertEquals(2, c.starts.size());
        assertEquals(2, c.ends.size());
        assertEquals(Arrays.asList("start:connection.open", "end:connection.open", "publish:CONNECTION",
                "start:connection.open", "end:connection.open", "publish:CONNECTION"), c.order);
        for (int i = 0; i < c.starts.size(); i++) {
            PerformanceLogEvent start = c.starts.get(i);
            PerformanceLogEvent end = c.ends.get(i);
            assertEquals(Type.START, start.getType());
            assertNull(start.getException());
            assertNull(start.getFailurePhase());
            assertEquals(0, start.getEndEpochNanos());
            assertEquals(0, start.getDurationNanos());
            assertEquals(Type.END, end.getType());
            assertEquals(start.getScopeId(), end.getScopeId());
            assertEquals(start.getRootScopeId(), end.getRootScopeId());
            assertEquals(start.getParentScopeId(), end.getParentScopeId());
        }
        assertNull(c.ends.get(0).getException());
        assertEquals("success", c.ends.get(0).getAttributes().get("mssql.connection.outcome"));
        assertSame(original, c.ends.get(1).getException());
        assertEquals("failure", c.ends.get(1).getAttributes().get("mssql.connection.outcome"));
        assertNull(PerformanceLog.getConnectionPhase(con));
    }

    @Test
    void legacyOnlyCallbackInheritsNoOpLifecycleMethod() throws Exception {
        List<PerformanceActivity> published = new ArrayList<>();
        PerformanceLog.registerCallback(new PerformanceLogCallback() {
            @Override
            public void publish(PerformanceActivity activity, int connectionId, long duration, Exception exception) {
                published.add(activity);
            }

            @Override
            public void publish(PerformanceActivity activity, int connectionId, int statementId, long duration,
                    Exception exception) {
                published.add(activity);
            }
        });
        SQLServerConnection con = new SQLServerConnection("test");
        try (PerformanceLog.Scope root = PerformanceLog.createScope(LOGGER, con, PerformanceActivity.CONNECTION);
                PerformanceLog.Scope dns = PerformanceLog.createConnectionScope(con, PerformanceActivity.DNS)) {
            assertNull(PerformanceLog.getConnectionPhase(con));
        }
        assertEquals(Collections.singletonList(PerformanceActivity.CONNECTION), published);
        assertNull(PerformanceLog.getConnectionPhase(con));
    }

    @Test
    void originSurvivesCauseLosingWrappersAndPhaseRestores() throws Exception {
        Collector c = new Collector();
        PerformanceLog.registerCallback(c);
        SQLServerConnection con = new SQLServerConnection("test");
        Exception original = new UnknownHostException("SECRET");
        try (PerformanceLog.Scope root = PerformanceLog.createScope(LOGGER, con, PerformanceActivity.CONNECTION)) {
            try (PerformanceLog.Scope attempt = PerformanceLog.createConnectionScope(con,
                    PerformanceActivity.CONNECTION_ATTEMPT)) {
                try (PerformanceLog.Scope dns = PerformanceLog.createConnectionScope(con, PerformanceActivity.DNS)) {
                    PerformanceLog.recordConnectionFailure(con, original);
                }
                assertEquals("attempt", PerformanceLog.getConnectionPhase(con));
                attempt.setException(new SQLException("SECRET lost cause"));
            }
            assertEquals("connection.open", PerformanceLog.getConnectionPhase(con));
            root.setException(new SQLException("SECRET outer wrapper"));
        }
        assertEquals(1, c.ends.stream().filter(e -> !e.getErrorAttributes().isEmpty()).count());
        PerformanceLogEvent root = c.ends.get(2);
        assertSame(original, root.getException());
        assertEquals("dns", root.getFailurePhase());
        assertEquals("name_resolution", root.getAttributes().get("mssql.error.category"));
        assertFalse(root.getAttributes().toString().contains("SECRET"));
    }

    @Test
    void callbackFailuresCloseOnceAndKeepLegacy() throws Exception {
        Collector c = new Collector() {
            @Override
            public void publish(PerformanceLogEvent event) throws Exception {
                super.publish(event);
                throw new Exception("SECRET callback failure");
            }
        };
        PerformanceLog.registerCallback(c);
        SQLServerConnection con = new SQLServerConnection("test");
        PerformanceLog.Scope root = PerformanceLog.createScope(LOGGER, con, PerformanceActivity.CONNECTION);
        root.close();
        root.close();
        assertEquals(1, c.starts.size());
        assertEquals(1, c.ends.size());
        assertEquals(1, c.published.size());
        assertEquals(Arrays.asList("start:connection.open", "end:connection.open", "publish:CONNECTION"), c.order);
        assertNull(c.ends.get(0).getException());
        assertNull(PerformanceLog.getConnectionPhase(con));
    }

    @Test
    void callbackSnapshotAndLegacyUnitsAndContext() throws Exception {
        Collector first = new Collector();
        first.nanos = true;
        PerformanceLog.registerCallback(first);
        SQLServerConnection con = new SQLServerConnection("test");
        Collector second = new Collector();
        try (PerformanceLog.Scope root = PerformanceLog.createScope(LOGGER, con, PerformanceActivity.CONNECTION)) {
            PerformanceLog.unregisterCallback();
            PerformanceLog.registerCallback(second);
        }
        assertEquals(1, first.starts.size());
        assertEquals(1, first.ends.size());
        assertEquals(Type.START, first.starts.get(0).getType());
        assertEquals(Type.END, first.ends.get(0).getType());
        assertEquals(first.starts.get(0).getScopeId(), first.ends.get(0).getScopeId());
        assertTrue(second.starts.isEmpty());
        assertTrue(second.ends.isEmpty());
        // Preserve the existing close-time callback selection and scope-time duration unit for publish.
        assertEquals(1, second.published.size());
        assertTrue(second.duration >= 0);
        assertNull(second.getCurrentApplicationName());
        assertNull(second.getCurrentUserSql());
        assertNull(second.getCurrentStatementType());
    }

    @Test
    void statementScopeUsesOnlyLegacyPublish() throws Exception {
        SQLServerConnection con = new SQLServerConnection("test");
        Exception original = new SQLException("statement failure");
        Collector c = new Collector() {
            @Override
            public void publish(PerformanceActivity activity, int connectionId, int statementId, long value,
                    Exception exception) {
                assertEquals(PerformanceActivity.STATEMENT_EXECUTE, activity);
                assertEquals(con.getConnectionID(), connectionId);
                assertEquals(42, statementId);
                assertSame(original, exception);
                super.publish(activity, connectionId, statementId, value, exception);
            }
        };
        PerformanceLog.registerCallback(c);
        try (PerformanceLog.Scope statement = PerformanceLog.createScope(LOGGER, con, 42, null, null,
                PerformanceActivity.STATEMENT_EXECUTE)) {
            statement.setException(original);
            assertTrue(c.published.isEmpty());
        }
        assertTrue(c.starts.isEmpty());
        assertTrue(c.ends.isEmpty());
        assertEquals(Collections.singletonList(PerformanceActivity.STATEMENT_EXECUTE), c.published);
        assertTrue(c.duration >= 0);
        assertNull(c.getCurrentApplicationName());
        assertNull(c.getCurrentUserSql());
        assertNull(c.getCurrentStatementType());
    }

    @Test
    void connectionAndThreadOwnership() throws Exception {
        PerformanceLog.registerCallback(new Collector());
        SQLServerConnection a = new SQLServerConnection("a");
        SQLServerConnection b = new SQLServerConnection("b");
        try (PerformanceLog.Scope root = PerformanceLog.createScope(LOGGER, a, PerformanceActivity.CONNECTION);
                PerformanceLog.Scope dns = PerformanceLog.createConnectionScope(a, PerformanceActivity.DNS);
                PerformanceLog.Scope other = PerformanceLog.createScope(LOGGER, b, PerformanceActivity.CONNECTION)) {
            assertEquals("dns", PerformanceLog.getConnectionPhase(a));
            assertEquals("connection.open", PerformanceLog.getConnectionPhase(b));
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread thread = new Thread(() -> {
                try {
                    assertNull(PerformanceLog.getConnectionPhase(a));
                    try (PerformanceLog.Scope independent = PerformanceLog.createScope(LOGGER, a,
                            PerformanceActivity.CONNECTION)) {
                        assertEquals("connection.open", PerformanceLog.getConnectionPhase(a));
                    }
                    assertNull(PerformanceLog.getConnectionPhase(a));
                } catch (Throwable e) {
                    failure.set(e);
                }
            });
            thread.start();
            thread.join();
            if (failure.get() != null) {
                throw new AssertionError(failure.get());
            }
            assertEquals("dns", PerformanceLog.getConnectionPhase(a));
        }
        assertNull(PerformanceLog.getConnectionPhase(a));
        assertNull(PerformanceLog.getConnectionPhase(b));
    }

    @Test
    void disabledLifecycleDoesNotCreateStateOrClassifyExceptions() throws Exception {
        SQLServerConnection con = new SQLServerConnection("test");
        Exception hostile = new RuntimeException() {
            private static final long serialVersionUID = 1L;

            @Override
            public synchronized Throwable getCause() {
                fail("Disabled instrumentation must not classify failures");
                return null;
            }
        };
        try (PerformanceLog.Scope root = PerformanceLog.createScope(LOGGER, con, PerformanceActivity.CONNECTION);
                PerformanceLog.Scope dns = PerformanceLog.createConnectionScope(con, PerformanceActivity.DNS)) {
            assertNull(ConnectionPerformanceState.current(con));
            dns.setException(hostile);
            PerformanceLog.recordConnectionFailure(con, hostile);
        }
    }

    @Test
    void connectionLoggingDoesNotReadRawMessageButLegacyCallbackKeepsException() throws Exception {
        Exception original = new SQLException("SECRET") {
            private static final long serialVersionUID = 1L;

            @Override
            public String getMessage() {
                fail("Connection performance logs must not read raw exception messages");
                return null;
            }
        };
        Collector c = new Collector() {
            @Override
            public void publish(PerformanceActivity activity, int id, long duration, Exception exception) {
                assertSame(original, exception);
                super.publish(activity, id, duration, exception);
            }
        };
        PerformanceLog.registerCallback(c);
        Logger logger = Logger.getLogger("test.connection.privacy");
        Level previous = logger.getLevel();
        try {
            logger.setLevel(Level.FINE);
            try (PerformanceLog.Scope scope = PerformanceLog.createScope(logger, new SQLServerConnection("test"),
                    PerformanceActivity.CONNECTION)) {
                scope.setException(original);
            }
            assertSame(original, c.ends.get(0).getException());
            assertEquals(1, c.published.size());
        } finally {
            logger.setLevel(previous);
        }
    }
}
