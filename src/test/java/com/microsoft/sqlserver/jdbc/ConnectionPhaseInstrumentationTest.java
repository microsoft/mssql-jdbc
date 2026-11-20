/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.io.DataInputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;


/** Exercises real driver entry points using only validation and loopback transports. */
class ConnectionPhaseInstrumentationTest {
    private final List<PerformanceLogEvent> events = new ArrayList<>();
    private final List<PerformanceActivity> legacy = new ArrayList<>();

    private void collect() {
        PerformanceLog.registerCallback(new PerformanceLogCallback() {
            @Override
            public void publish(PerformanceLogEvent event) {
                events.add(event);
            }

            @Override
            public void publish(PerformanceActivity activity, int id, long duration, Exception error) {
                legacy.add(activity);
            }

            @Override
            public void publish(PerformanceActivity activity, int id, int statementId, long duration, Exception error) {
                legacy.add(activity);
            }
        });
    }

    @AfterEach
    void cleanup() {
        PerformanceLog.unregisterCallback();
    }

    private PerformanceLogEvent end(String phase) {
        return events.stream().filter(e -> e.getType() == PerformanceLogEvent.Type.END && phase.equals(e.getPhase()))
                .reduce((a, b) -> b).orElseThrow(() -> new AssertionError("Missing phase: " + phase));
    }

    private void assertBalanced() {
        List<Long> stack = new ArrayList<>();
        for (PerformanceLogEvent event : events) {
            if (event.getType() == PerformanceLogEvent.Type.START) {
                assertEquals(stack.isEmpty() ? 0L : stack.get(stack.size() - 1).longValue(), event.getParentScopeId());
                stack.add(event.getScopeId());
            } else {
                assertEquals(event.getScopeId(), stack.remove(stack.size() - 1).longValue());
            }
        }
        assertTrue(stack.isEmpty());
        assertEquals(1,
                events.stream().filter(
                        e -> e.getType() == PerformanceLogEvent.Type.START && "connection.open".equals(e.getPhase()))
                        .count());
    }

    @Test
    void invalidPropertiesFailConfigurationWithoutNetworkOrSettingsLeak() throws Exception {
        collect();
        Properties properties = new Properties();
        properties.setProperty("encrypt", "SECRET-invalid-value");
        SQLServerConnection con = new SQLServerConnection("test");
        assertThrows(SQLServerException.class, () -> con.connect(properties, null));
        assertNotNull(end("configuration").getException());
        assertEquals("configuration", end("connection.open").getFailurePhase());
        assertFalse(end("connection.open").getAttributes().containsKey("mssql.connection.encrypt"));
        assertFalse(events.stream().anyMatch(e -> e.getAttributes().toString().contains("SECRET")));
        assertFalse(events.stream().anyMatch(e -> "attempt".equals(e.getPhase())));
        assertNull(PerformanceLog.getConnectionPhase(con));
        assertBalanced();
    }

    @Test
    void invalidLoginTimeoutIsConfigurationFailure() throws Exception {
        collect();
        Properties properties = new Properties();
        properties.setProperty("loginTimeout", "not-a-number");
        SQLServerConnection con = new SQLServerConnection("test");
        assertThrows(SQLServerException.class, () -> con.connect(properties, null));
        assertEquals("configuration", end("connection.open").getFailurePhase());
        assertNotNull(end("configuration").getException());
        assertBalanced();
    }

    @Test
    void malformedUrlFailsConfiguration() {
        collect();
        assertThrows(SQLServerException.class,
                () -> new SQLServerDriver().connect("jdbc:sqlserver://localhost;password={unterminated", null));
        assertEquals("configuration", end("connection.open").getFailurePhase());
        assertBalanced();
    }

    @Test
    void unrelatedUrlDoesNotOpenConnection() throws Exception {
        collect();
        assertNull(new SQLServerDriver().connect("jdbc:other://localhost", null));
        assertTrue(events.isEmpty());
        assertTrue(legacy.isEmpty());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void invalidLiteralCapturesOriginalDnsFailure(boolean browser) throws Exception {
        collect();
        Properties properties = loopbackProperties(1);
        // Invalid IPv6 syntax fails in the resolver without an external DNS query.
        properties.setProperty("serverName", "invalid:literal");
        if (browser) {
            properties.remove("portNumber");
            properties.setProperty("instanceName", "test");
        }
        SQLServerConnection con = new SQLServerConnection("test");
        assertThrows(SQLServerException.class, () -> con.connect(properties, null));
        assertTrue(end("dns").getException() instanceof UnknownHostException);
        assertSame(end("dns").getException(), end("attempt").getException());
        assertSame(end("dns").getException(), end("connection.open").getException());
        assertEquals("dns", end("connection.open").getFailurePhase());
        assertEquals("name_resolution", end("connection.open").getAttributes().get("mssql.error.category"));
        assertFalse(end("dns").getErrorAttributes().isEmpty());
        assertTrue(end("attempt").getErrorAttributes().isEmpty());
        assertTrue(end("connection.open").getErrorAttributes().isEmpty());
        if (browser) {
            assertEquals(end("instance_discovery").getScopeId(), end("dns").getParentScopeId());
        } else {
            assertEquals(end("attempt").getScopeId(), end("dns").getParentScopeId());
        }
        assertFalse(events.stream().anyMatch(e -> "socket_connect".equals(e.getPhase())));
        assertFalse(end("attempt").getAttributes().containsKey("mssql.connection.client_connection_id"));
        assertNull(PerformanceLog.getConnectionPhase(con));
        assertBalanced();
    }

    @Test
    void strictTlsHandshakeFailureIsTlsNotPrelogin() throws Exception {
        collect();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            server.setSoTimeout(10000);
            Future<?> peer = executor.submit(() -> {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(10000);
                    DataInputStream input = new DataInputStream(socket.getInputStream());
                    byte[] header = new byte[5];
                    input.readFully(header);
                    input.readFully(new byte[((header[3] & 255) << 8) | (header[4] & 255)]);
                    // Fatal TLS handshake_failure alert; no certificates or external authentication required.
                    socket.getOutputStream().write(new byte[] {21, 3, 3, 0, 2, 2, 40});
                    socket.getOutputStream().flush();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Properties properties = loopbackProperties(server.getLocalPort());
            properties.setProperty("encrypt", "strict");
            properties.setProperty("trustServerCertificate", "true");
            SQLServerConnection con = new SQLServerConnection("test");
            assertThrows(SQLServerException.class, () -> con.connect(properties, null));
            peer.get(15, TimeUnit.SECONDS);
            assertEquals("tls", end("connection.open").getFailurePhase());
            assertNotNull(end("tls").getException());
            assertNull(end("socket_connect").getException());
            assertEquals(false,
                    end("connection.open").getAttributes().get("mssql.connection.trust_server_certificate"));
            assertFalse(events.stream().anyMatch(e -> "prelogin".equals(e.getPhase())));
            assertBalanced();
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"login", "initialize", "success"})
    void loopbackLoginAndInitializationBoundaries(String outcome) throws Exception {
        collect();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            server.setSoTimeout(10000);
            Future<?> peer = executor.submit(() -> {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(10000);
                    readPacket(socket);
                    // VERSION + ENCRYPTION options, reporting no encryption support for a plaintext mock.
                    reply(socket, new byte[] {0, 0, 11, 0, 6, 1, 0, 17, 0, 1, (byte) 255, 16, 0, 0, 0, 0, 0, 2});
                    readPacket(socket);
                    if ("login".equals(outcome)) {
                        reply(socket, new byte[] {0}); // Invalid token in the actual LOGIN response.
                    } else {
                        // Minimal LOGINACK followed by DONE for TDS 7.4.
                        reply(socket, new byte[] {(byte) 0xad, 10, 0, 1, 0x74, 0, 0, 4, 0, 16, 0, 0, 1, (byte) 0xfd, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
                        readPacket(socket); // SET LOCK_TIMEOUT initialization, outside the login exchange.
                        reply(socket,
                                "initialize".equals(
                                        outcome) ? new byte[] {0}
                                                 : new byte[] {(byte) 0xfd, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Properties properties = loopbackProperties(server.getLocalPort());
            properties.setProperty("lockTimeout", "1");
            SQLServerConnection con = new SQLServerConnection("test");
            try {
                if ("success".equals(outcome)) {
                    assertSame(con, con.connect(properties, null));
                    assertNull(end("connection.open").getException());
                    assertEquals("success", end("connection.open").getAttributes().get("mssql.connection.outcome"));
                } else {
                    assertThrows(SQLServerException.class, () -> con.connect(properties, null));
                    assertEquals(outcome, end("connection.open").getFailurePhase());
                }
                peer.get(15, TimeUnit.SECONDS);
                if (!"login".equals(outcome)) {
                    assertNull(end("login").getException());
                    assertTrue(end("login").getEndEpochNanos() <= end("initialize").getStartEpochNanos());
                    assertNull(end("attempt").getException());
                    for (PerformanceLogEvent event : events) {
                        if ("initialize".equals(event.getPhase())) {
                            assertEquals(end("connection.open").getScopeId(), event.getParentScopeId());
                            assertTrue(end("attempt").getEndEpochNanos() <= event.getStartEpochNanos());
                        }
                    }
                }
                assertEquals("sql_password", end("connection.open").getAttributes().get("mssql.authentication.method"));
                assertNull(end("prelogin").getException());
                assertBalanced();
            } finally {
                con.close();
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));
        }
    }

    private static Properties loopbackProperties(int port) {
        Properties properties = new Properties();
        properties.setProperty("serverName", "127.0.0.1");
        properties.setProperty("portNumber", Integer.toString(port));
        properties.setProperty("encrypt", "false");
        properties.setProperty("transparentNetworkIPResolution", "false");
        properties.setProperty("connectRetryCount", "0");
        properties.setProperty("loginTimeout", "3");
        properties.setProperty("workstationID", "test");
        return properties;
    }

    @Test
    void socketReadTimeoutKeepsOriginalFailureAndKnownPhase() throws Exception {
        collect();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            server.setSoTimeout(10000);
            Future<?> peer = executor.submit(() -> {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(10000);
                    readPacket(socket);
                    // Do not answer PRELOGIN. Wait for the driver's socket read timeout to close the channel.
                    assertEquals(-1, socket.getInputStream().read());
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Properties properties = loopbackProperties(server.getLocalPort());
            properties.setProperty("socketTimeout", "250");
            try (SQLServerConnection con = new SQLServerConnection("test")) {
                assertThrows(SQLServerException.class, () -> con.connect(properties, null));
                peer.get(15, TimeUnit.SECONDS);
                assertTrue(end("prelogin").getException() instanceof java.net.SocketTimeoutException);
                assertSame(end("prelogin").getException(), end("connection.open").getException());
                assertEquals("prelogin", end("connection.open").getFailurePhase());
                assertEquals("timeout", end("connection.open").getAttributes().get("mssql.error.category"));
                java.util.Map<String, Object> timeout = end("connection.open").getDiagnosticEvents().stream()
                        .filter(e -> "mssql.driver.timeout".equals(e.get("name"))).findFirst().get();
                java.util.Map<?, ?> attributes = (java.util.Map<?, ?>) timeout.get("attributes");
                assertEquals("prelogin", attributes.get("mssql.timeout.phase"));
                assertEquals("socket_read", attributes.get("mssql.timeout.kind"));
                assertBalanced();
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));
        }
    }

    @Test
    void malformedEnhancedRoutingCapturesKnownResourceWithoutChangingProtocolError() throws Exception {
        collect();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            server.setSoTimeout(10000);
            Future<?> peer = executor.submit(() -> {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(10000);
                    readPacket(socket);
                    reply(socket, new byte[] {0, 0, 11, 0, 6, 1, 0, 17, 0, 1, (byte) 255, 16, 0, 0, 0, 0, 0, 2});
                    readPacket(socket);
                    // Enhanced routing ENVCHANGE: valid protocol/port/server, invalid zero-length database.
                    reply(socket,
                            new byte[] {(byte) 0xe3, 14, 0, 21, 9, 0, 0, (byte) 0x99, 5, 1, 0, 'x', 0, 0, 0, 0, 0});
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            try (SQLServerConnection con = new SQLServerConnection("test")) {
                SQLServerException failure = assertThrows(SQLServerException.class,
                        () -> con.connect(loopbackProperties(server.getLocalPort()), null));
                peer.get(15, TimeUnit.SECONDS);
                assertEquals(SQLServerException.getErrString("R_invalidEnhancedRoutingInfo"), failure.getMessage());
                assertSame(failure, end("login").getException());
                assertEquals("login", end("connection.open").getFailurePhase());
                assertEquals("routing_redirect", end("connection.open").getAttributes().get("mssql.error.category"));
                assertEquals("jdbc:R_invalidEnhancedRoutingInfo",
                        end("login").getErrorAttributes().get("mssql.error.code"));
                assertBalanced();
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"sql_password", "access_token", "callback_instance", "callback_class"})
    void implicitAuthenticationMethodUsesValidatedMechanismWithoutSecrets(String method) throws Exception {
        collect();
        Properties properties = loopbackProperties(1);
        properties.setProperty("serverName", "invalid:literal");
        if ("access_token".equals(method)) {
            properties.setProperty("accessToken", "SECRET-token");
        } else if ("callback_instance".equals(method)) {
            properties.put("accessTokenCallback", (SQLServerAccessTokenCallback) (spn, stsurl) -> {
                throw new AssertionError("DNS failure must happen before token acquisition");
            });
        } else if ("callback_class".equals(method)) {
            properties.setProperty("accessTokenCallbackClass", "SECRET-class");
        } else {
            properties.setProperty("user", "SECRET-user");
            properties.setProperty("password", "SECRET-password");
        }
        try (SQLServerConnection con = new SQLServerConnection("test")) {
            assertThrows(SQLServerException.class, () -> con.connect(properties, null));
            assertEquals(method.startsWith("callback_") ? "access_token_callback" : method,
                    end("connection.open").getAttributes().get("mssql.authentication.method"));
            assertEquals("dns", end("connection.open").getFailurePhase());
            assertFalse(events.stream().anyMatch(e -> e.getAttributes().toString().contains("SECRET")));
            assertFalse(events.stream().anyMatch(e -> "token_acquisition".equals(e.getPhase())));
            assertBalanced();
        }
    }

    @Test
    void recoveryDoesNotPublishPhysicalOpenButKeepsLegacyCallback() throws Exception {
        collect();
        SQLServerConnection con = new SQLServerConnection("test");
        // Recovery reuses the existing properties. A deliberately incomplete fixture fails before network I/O.
        assertThrows(Exception.class, () -> con.connect((Properties) null, null));
        assertTrue(events.isEmpty());
        assertEquals(1, legacy.stream().filter(a -> a == PerformanceActivity.CONNECTION).count());
        assertNull(PerformanceLog.getConnectionPhase(con));
    }

    private static void readPacket(Socket socket) throws Exception {
        DataInputStream input = new DataInputStream(socket.getInputStream());
        byte[] header = new byte[8];
        input.readFully(header);
        int length = ((header[2] & 255) << 8) | (header[3] & 255);
        input.readFully(new byte[length - 8]);
    }

    @Test
    void realRecoveryThreadSuppressesAllNewOpenPhases() throws Exception {
        collect();
        java.util.concurrent.atomic.AtomicBoolean recoveryGate = new java.util.concurrent.atomic.AtomicBoolean();
        SQLServerConnection con = new SQLServerConnection("test") {
            @Override
            java.sql.Connection connect(Properties props, SQLServerPooledConnection pooled) throws SQLServerException {
                if (getSessionRecovery().isReconnectRunning()) {
                    recoveryGate.set(true);
                    assertNull(props);
                }
                return super.connect(props, pooled);
            }
        };
        Properties properties = loopbackProperties(1);
        properties.setProperty("serverName", "invalid:literal");
        properties.setProperty("connectRetryCount", "1");
        properties.setProperty("connectRetryInterval", "1");
        assertThrows(SQLServerException.class, () -> con.connect(properties, null));
        events.clear();
        legacy.clear();
        con.getSessionRecovery().reconnect(new UninterruptableTDSCommand("test recovery") {
            private static final long serialVersionUID = 1L;

            @Override
            boolean doExecute() {
                return true;
            }
        });
        assertTrue(recoveryGate.get());
        assertNotNull(con.getSessionRecovery().getReconnectException());
        assertTrue(events.isEmpty(), "Recovery must not create physical-open roots or orphan phases");
        assertEquals(1, legacy.stream().filter(a -> a == PerformanceActivity.CONNECTION).count());
        assertNull(PerformanceLog.getConnectionPhase(con));
    }

    @ParameterizedTest
    @ValueSource(strings = {"false", "true"})
    void transportDnsFinishesBeforeSocketScope(String parallel) throws Exception {
        collect();
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            SQLServerConnection con = new SQLServerConnection("test");
            try (PerformanceLog.Scope root = PerformanceLog.createConnectionScope(con, PerformanceActivity.CONNECTION);
                    PerformanceLog.Scope attempt = PerformanceLog.createConnectionScope(con,
                            PerformanceActivity.CONNECTION_ATTEMPT);
                    Socket socket = new SocketFinder("test", con).findSocket("127.0.0.1", server.getLocalPort(), 1000,
                            Boolean.parseBoolean(parallel), false, false, 1000, "UsePlatformDefault")) {
                assertEquals("attempt", PerformanceLog.getConnectionPhase(con));
            }
            assertEquals(end("attempt").getScopeId(), end("dns").getParentScopeId());
            assertEquals(end("attempt").getScopeId(), end("socket_connect").getParentScopeId());
            assertTrue(end("dns").getEndEpochNanos() <= end("socket_connect").getStartEpochNanos());
            assertEquals(1L, end("dns").getAttributes().get("mssql.connection.attempt"));
            assertEquals(1L, end("socket_connect").getAttributes().get("mssql.connection.attempt"));
            assertBalanced();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void realRetryAndRedirectKeepOneRootAndCountStartedAttempts(boolean redirect) throws Exception {
        collect();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0, 2, InetAddress.getByName("127.0.0.1"))) {
            server.setSoTimeout(10000);
            Future<?> peer = executor.submit(() -> {
                try {
                    try (Socket first = server.accept()) {
                        first.setSoTimeout(10000);
                        readPacket(first);
                        if (redirect) {
                            preloginReply(first);
                            readPacket(first);
                            reply(first, routingReply(server.getLocalPort()));
                        }
                        // Otherwise close PRELOGIN without replying: a retryable transport failure.
                    }
                    try (Socket second = server.accept()) {
                        second.setSoTimeout(10000);
                        readPacket(second);
                        preloginReply(second);
                        readPacket(second);
                        reply(second, new byte[] {(byte) 0xad, 10, 0, 1, 0x74, 0, 0, 4, 0, 16, 0, 0, 1, (byte) 0xfd, 0,
                                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0});
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Properties properties = loopbackProperties(server.getLocalPort());
            properties.setProperty("connectRetryCount", "1");
            properties.setProperty("connectRetryInterval", "1");
            properties.setProperty("loginTimeout", "10");
            try (SQLServerConnection con = new SQLServerConnection("test")) {
                assertSame(con, con.connect(properties, null));
                peer.get(15, TimeUnit.SECONDS);
                PerformanceLogEvent root = end("connection.open");
                assertNull(root.getException());
                assertEquals(2L, root.getAttributes().get("mssql.connection.attempt_count"));
                assertEquals(redirect ? 0L : 1L, root.getAttributes().get("mssql.connection.retry_count"));
                assertEquals(redirect ? 1L : 0L, root.getAttributes().get("mssql.connection.redirect_count"));
                assertEquals(redirect ? "redirect" : "retry",
                        end("attempt").getAttributes().get("mssql.connection.attempt_reason"));
                PerformanceLogEvent first = events.stream()
                        .filter(e -> e.getType() == PerformanceLogEvent.Type.END && "attempt".equals(e.getPhase()))
                        .findFirst().get();
                assertEquals(redirect ? "redirect" : "failure",
                        first.getAttributes().get("mssql.connection.attempt_outcome"));
                assertTrue(root.getDiagnosticEvents().stream().anyMatch(
                        e -> (redirect ? "mssql.driver.redirect" : "mssql.driver.retry").equals(e.get("name"))));
                assertBalanced();
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));
        }
    }

    @Test
    void routingBudgetExhaustionKeepsRedirectPhaseAndNoPhantomAttempt() throws Exception {
        collect();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            server.setSoTimeout(10000);
            Future<?> peer = executor.submit(() -> {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(10000);
                    readPacket(socket);
                    preloginReply(socket);
                    readPacket(socket);
                    reply(socket, routingReply(server.getLocalPort()));
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            // Expire the budget at the real redirect boundary, without sleeping or racing the socket timeout.
            SQLServerConnection con = new SQLServerConnection("test");
            PerformanceLog.unregisterCallback();
            PerformanceLog.registerCallback(new PerformanceLogLifecycleTest.Collector() {
                @Override
                public void publish(PerformanceLogEvent event) throws Exception {
                    events.add(event);
                    if (event.getType() == PerformanceLogEvent.Type.START && "redirect".equals(event.getPhase())) {
                        java.lang.reflect.Field expiry = SQLServerConnection.class.getDeclaredField("timerExpire");
                        expiry.setAccessible(true);
                        expiry.setLong(con, System.currentTimeMillis() - 1);
                    }
                }
            });
            try {
                assertThrows(SQLServerException.class,
                        () -> con.connect(loopbackProperties(server.getLocalPort()), null));
                peer.get(15, TimeUnit.SECONDS);
                assertEquals("timeout", end("connection.open").getAttributes().get("mssql.error.category"));
                assertEquals("redirect", end("connection.open").getFailurePhase());
                assertEquals(1L, end("connection.open").getAttributes().get("mssql.connection.attempt_count"));
                assertEquals(0L, end("connection.open").getAttributes().get("mssql.connection.redirect_count"));
                assertEquals("timeout", end("attempt").getAttributes().get("mssql.connection.attempt_outcome"));
                assertTrue(end("connection.open").getDiagnosticEvents().stream().anyMatch(e -> "routing_budget"
                        .equals(((java.util.Map<?, ?>) e.get("attributes")).get("mssql.timeout.kind"))));
                assertBalanced();
            } finally {
                con.close();
            }
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void callbackInvocationAndInvalidClassKeepDistinctSourceEvidence(boolean invalidClass) throws Exception {
        collect();
        Properties properties = loopbackProperties(1);
        properties.setProperty("serverName", "invalid:literal");
        RuntimeException original = new RuntimeException("SECRET");
        if (invalidClass) {
            properties.setProperty("accessTokenCallbackClass", String.class.getName());
        } else {
            properties.put("accessTokenCallback", (SQLServerAccessTokenCallback) (spn, stsurl) -> {
                throw original;
            });
        }
        try (SQLServerConnection con = new SQLServerConnection("test")) {
            assertThrows(SQLServerException.class, () -> con.connect(properties, null));
            events.clear();
            try (PerformanceLog.Scope root = PerformanceLog.createConnectionScope(con, PerformanceActivity.CONNECTION);
                    PerformanceLog.Scope login = PerformanceLog.createConnectionScope(con,
                            PerformanceActivity.LOGIN_EXCHANGE)) {
                Exception failure = assertThrows(Exception.class,
                        () -> con.onFedAuthInfo(con.new SqlFedAuthInfo(), null));
                login.setException(failure);
                root.setException(failure);
            }
            assertEquals(invalidClass ? "configuration" : "authentication",
                    end("token_acquisition").getAttributes().get("mssql.error.category"));
            assertEquals(invalidClass ? "driver" : "callback",
                    end("token_acquisition").getErrorAttributes().get("mssql.error.source"));
            if (!invalidClass) {
                assertSame(original, end("connection.open").getException());
            }
            assertEquals("token_acquisition", end("connection.open").getFailurePhase());
            assertFalse(events.stream().anyMatch(e -> e.getErrorAttributes().toString().contains("SECRET")));
            assertBalanced();
        }
    }

    @Test
    void parallelSelectionTimeoutWithoutWorkerExceptionKeepsLiteralEvidence() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeFalse(Util.isIBM(), "Threaded selector regression");
        collect();
        SQLServerConnection con = new SQLServerConnection("test") {
            @Override
            InetAddress[] resolveAllAddresses(String host) throws UnknownHostException {
                InetAddress address = super.resolveAllAddresses("127.0.0.1")[0];
                return new InetAddress[] {address, address};
            }
        };
        // Direct SocketFinder invocation still requires the properties used by TCP error formatting.
        con.activeConnectionProperties = loopbackProperties(1);
        SocketFinder finder = new SocketFinder("test", con);
        java.lang.reflect.Field result = SocketFinder.class.getDeclaredField("result");
        result.setAccessible(true);
        // Deterministically model the parent deadline winning before any worker result.
        // Workers see FAILURE and never connect; exercise the actual plain-IOException source and wrapper.
        result.set(finder, SocketFinder.Result.FAILURE);
        try (PerformanceLog.Scope root = PerformanceLog.createConnectionScope(con, PerformanceActivity.CONNECTION);
                PerformanceLog.Scope attempt = PerformanceLog.createConnectionScope(con,
                        PerformanceActivity.CONNECTION_ATTEMPT)) {
            SQLServerException wrapper = assertThrows(SQLServerException.class,
                    () -> finder.findSocket("127.0.0.1", 1, 1, true, false, false, 1, "UsePlatformDefault"));
            attempt.setException(wrapper);
            root.setException(wrapper);
        }
        assertEquals(java.io.IOException.class, end("socket_connect").getException().getClass());
        assertEquals("jdbc:R_connectionTimedOut", end("socket_connect").getErrorAttributes().get("mssql.error.code"));
        assertEquals("timeout", end("connection.open").getAttributes().get("mssql.error.category"));
        assertEquals("socket_connect", end("connection.open").getFailurePhase());
        assertSame(end("socket_connect").getException(), end("connection.open").getException());
        assertTrue(end("connection.open").getDiagnosticEvents().stream().anyMatch(
                e -> "socket_selection".equals(((java.util.Map<?, ?>) e.get("attributes")).get("mssql.timeout.kind"))));
        assertBalanced();
    }

    private static void preloginReply(Socket socket) throws Exception {
        reply(socket, new byte[] {0, 0, 11, 0, 6, 1, 0, 17, 0, 1, (byte) 255, 16, 0, 0, 0, 0, 0, 2});
    }

    private static byte[] routingReply(int port) {
        byte[] server = "127.0.0.1".getBytes(java.nio.charset.StandardCharsets.UTF_16LE);
        java.nio.ByteBuffer payload = java.nio.ByteBuffer.allocate(3 + 1 + 2 + 5 + server.length + 2 + 13 + 13)
                .order(java.nio.ByteOrder.LITTLE_ENDIAN);
        payload.put((byte) 0xe3).putShort((short) (1 + 2 + 5 + server.length + 2));
        payload.put((byte) 20).putShort((short) (5 + server.length)).put((byte) 0).putShort((short) port);
        payload.putShort((short) (server.length / 2)).put(server).putShort((short) 0);
        payload.put(new byte[] {(byte) 0xad, 10, 0, 1, 0x74, 0, 0, 4, 0, 16, 0, 0, 1});
        payload.put((byte) 0xfd).putShort((short) 0).putShort((short) 0).putLong(0);
        return payload.array();
    }

    private static void reply(Socket socket, byte[] payload) throws Exception {
        int length = payload.length + 8;
        socket.getOutputStream().write(new byte[] {4, 1, (byte) (length >> 8), (byte) length, 0, 0, 1, 0});
        socket.getOutputStream().write(payload);
        socket.getOutputStream().flush();
    }

    @Test
    void loopbackPreloginFailureHasRealAttemptDnsSocketAndOnePrelogin() throws Exception {
        collect();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (ServerSocket server = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"))) {
            server.setSoTimeout(10000);
            Future<?> peer = executor.submit(() -> {
                try (Socket socket = server.accept()) {
                    socket.setSoTimeout(10000);
                    DataInputStream input = new DataInputStream(socket.getInputStream());
                    byte[] header = new byte[8];
                    input.readFully(header);
                    int length = ((header[2] & 255) << 8) | (header[3] & 255);
                    input.readFully(new byte[length - 8]);
                    // Complete but invalid PRELOGIN response: deterministic protocol failure, not a timeout.
                    socket.getOutputStream().write(new byte[] {4, 1, 0, 9, 0, 0, 1, 0, (byte) 255});
                    socket.getOutputStream().flush();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            });
            Properties properties = new Properties();
            properties.setProperty("serverName", "127.0.0.1");
            properties.setProperty("portNumber", Integer.toString(server.getLocalPort()));
            properties.setProperty("encrypt", "false");
            properties.setProperty("transparentNetworkIPResolution", "false");
            properties.setProperty("connectRetryCount", "0");
            properties.setProperty("loginTimeout", "3");
            properties.setProperty("workstationID", "test");
            SQLServerConnection con = new SQLServerConnection("test");
            assertThrows(SQLServerException.class, () -> con.connect(properties, null));
            peer.get(15, TimeUnit.SECONDS);
            assertNull(end("dns").getException());
            assertNull(end("socket_connect").getException());
            assertEquals("prelogin", end("connection.open").getFailurePhase());
            assertEquals("prelogin", end("attempt").getFailurePhase());
            assertNotNull(end("attempt").getAttributes().get("mssql.connection.client_connection_id"));
            assertFalse(end("connection.open").getAttributes().containsKey("mssql.connection.client_connection_id"));
            assertEquals("false", end("connection.open").getAttributes().get("mssql.connection.encrypt"));
            assertEquals(1, legacy.stream().filter(a -> a == PerformanceActivity.PRELOGIN).count());
            assertEquals(1, legacy.stream().filter(a -> a == PerformanceActivity.LOGIN).count());
            assertEquals(1,
                    events.stream().filter(
                            e -> e.getType() == PerformanceLogEvent.Type.START && "prelogin".equals(e.getPhase()))
                            .count());
            assertFalse(legacy.stream().anyMatch(PerformanceActivity::isLifecycleOnly));
            assertNull(PerformanceLog.getConnectionPhase(con));
            assertBalanced();
        } finally {
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(15, TimeUnit.SECONDS));
        }
    }
}
