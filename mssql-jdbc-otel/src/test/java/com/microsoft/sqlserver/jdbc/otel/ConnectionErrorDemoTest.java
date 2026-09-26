/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenCredential;
import com.azure.identity.AzureCliCredential;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.ManagedIdentityCredential;
import com.microsoft.sqlserver.jdbc.SQLServerAccessTokenCallback;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.SqlAuthenticationToken;
import com.sun.net.httpserver.HttpServer;

import reactor.core.publisher.Mono;


class ConnectionErrorDemoTest {
    @Test
    void rejectsInvalidEnvironmentBeforeConnectingWithoutLeakingValues() {
        String[][] invalid = {{"DEMO_SCENARIOS", "SECRET"}, {"DEMO_SCENARIOS", "config,config"},
                {"DEMO_SCENARIOS", "config,"}, {"DEMO_REPEAT", "1001"}, {"DEMO_REPEAT", "0"},
                {"DEMO_PAUSE_SECONDS", "61"}, {"DEMO_PAUSE_SECONDS", "-1"}, {"OTEL_AUTH_MODE", "SECRET"},
                {"OTEL_EXPORTER_OTLP_ENDPOINT", "https://SECRET@example.com"},
                {"OTEL_ALLOW_INSECURE_LOCAL_ENDPOINT", "SECRET"}};
        for (String[] entry : invalid) {
            Map<String, String> env = environment();
            env.put(entry[0], entry[1]);
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> ConnectionErrorDemo.run(env, (url, properties) -> {
                        fail("Validation must precede JDBC");
                        return null;
                    }));
            assertEquals("Invalid demo configuration", error.getMessage());
            assertNull(error.getCause());
        }
    }

    @Test
    void tokenModesRequireExplicitScopeAndStaticToken() {
        for (String mode : new String[] {"static", "azure_cli", "managed_identity", "default"}) {
            Map<String, String> env = environment();
            env.put("OTEL_AUTH_MODE", mode);
            assertThrows(IllegalArgumentException.class, () -> ConnectionErrorDemo.telemetryProperties(env));
            env.put("OTEL_ACCESS_TOKEN_SCOPE", "https://approved.example/.default");
            if ("static".equals(mode)) {
                assertThrows(IllegalArgumentException.class, () -> ConnectionErrorDemo.telemetryProperties(env));
                env.put("OTEL_BEARER_TOKEN", "STATIC-TOKEN");
            }
            Properties properties = ConnectionErrorDemo.telemetryProperties(env);
            assertEquals("https://approved.example/.default", properties.getProperty("otelTokenScope"));
            if (!"static".equals(mode)) {
                assertEquals(DemoTokenCallback.class.getName(), properties.getProperty("otelAccessTokenCallbackClass"));
                assertEquals("https://login.microsoftonline.com/", properties.getProperty("otelTokenAuthority"));
            }
            env.put("OTEL_TOKEN_AUTHORITY", "https://SECRET@authority.example");
            assertThrows(IllegalArgumentException.class, () -> ConnectionErrorDemo.telemetryProperties(env));
        }
    }

    @Test
    void tokenCallbackUsesExplicitScopePreservesExpiryAndSanitizesBoundedFailures() {
        Map<String, String> env = environment();
        env.put("OTEL_ACCESS_TOKEN_SCOPE", "https://approved.example/.default");
        env.put("AZURE_AUTHORITY_HOST", "https://login.microsoftonline.us/");
        OffsetDateTime expiry = OffsetDateTime.now().plusMinutes(5);
        AtomicInteger requests = new AtomicInteger();
        TokenCredential credential = context -> {
            assertEquals(java.util.Collections.singletonList(env.get("OTEL_ACCESS_TOKEN_SCOPE")), context.getScopes());
            requests.incrementAndGet();
            return Mono.just(new AccessToken("TEST-TOKEN", expiry));
        };
        DemoTokenCallback callback = new DemoTokenCallback(env, credential, Duration.ofSeconds(1));
        assertEquals(0, requests.get());
        SqlAuthenticationToken token = callback.getAccessToken(env.get("OTEL_ACCESS_TOKEN_SCOPE"),
                "https://login.microsoftonline.us/");
        assertEquals("TEST-TOKEN", token.getAccessToken());
        assertEquals(expiry.toInstant().toEpochMilli(), token.getExpiresOn().getTime());
        assertThrows(IllegalStateException.class, () -> callback.getAccessToken("unapproved", "unapproved"));
        assertEquals(1, requests.get());
        for (TokenCredential unavailable : new TokenCredential[] {context -> Mono.never(),
                context -> Mono.error(new IllegalStateException("SECRET")),
                context -> Mono.just(new AccessToken("EXPIRED", OffsetDateTime.now().minusMinutes(1))),
                context -> Mono.empty()}) {
            DemoTokenCallback failing = new DemoTokenCallback(env, unavailable, Duration.ofMillis(20));
            IllegalStateException failure = assertTimeout(Duration.ofSeconds(1),
                    () -> assertThrows(IllegalStateException.class, () -> failing
                            .getAccessToken(env.get("OTEL_ACCESS_TOKEN_SCOPE"), "https://login.microsoftonline.us/")));
            assertEquals("Telemetry authentication unavailable", failure.getMessage());
            assertNull(failure.getCause());
        }
    }

    @Test
    void callbackBuildsTheSelectedProviderWithoutAcquiringTokens() throws Exception {
        Map<String, String> env = environment();
        env.put("OTEL_ACCESS_TOKEN_SCOPE", "https://approved.example/.default");
        String[] modes = {"azure_cli", "managed_identity", "default"};
        Class<?>[] types = {AzureCliCredential.class, ManagedIdentityCredential.class, DefaultAzureCredential.class};
        java.lang.reflect.Field field = DemoTokenCallback.class.getDeclaredField("credential");
        field.setAccessible(true);
        for (int i = 0; i < modes.length; i++) {
            env.put("OTEL_AUTH_MODE", modes[i]);
            DemoTokenCallback callback = new DemoTokenCallback(env, null, Duration.ofSeconds(1));
            assertTrue(types[i].isInstance(field.get(callback)));
        }
    }

    @Test
    void remoteScenariosRequireExplicitInputsAndPreserveSqlLoginOverrides() throws Exception {
        for (String scenario : new String[] {"login", "success"}) {
            Map<String, String> env = environment();
            env.put("DEMO_SCENARIOS", scenario);
            assertThrows(IllegalArgumentException.class, () -> ConnectionErrorDemo.run(env, (url, properties) -> {
                fail("Missing remote inputs must not connect");
                return null;
            }));
        }
        try (Collector collector = new Collector()) {
            Map<String, String> env = collector.environment();
            env.put("DEMO_SCENARIOS", "login");
            env.put("JDBC_CONNECTION_STRING", "jdbc:sqlserver://example.invalid;authentication=SqlPassword");
            env.put("DEMO_LOGIN_USER", "deliberately-nonexistent");
            env.put("DEMO_LOGIN_PASSWORD", "synthetic-not-a-secret");
            AtomicInteger attempts = new AtomicInteger();
            // SQLExceptions without an actual authentication/login root are not accepted as login failures.
            assertThrows(IllegalStateException.class, () -> ConnectionErrorDemo.run(env, (url, properties) -> {
                attempts.incrementAndGet();
                assertEquals("deliberately-nonexistent", properties.getProperty("user"));
                assertEquals("synthetic-not-a-secret", properties.getProperty("password"));
                assertEquals("SqlPassword", properties.getProperty("authentication"));
                assertEquals("5", properties.getProperty("loginTimeout"));
                assertEquals("5000", properties.getProperty("socketTimeout"));
                throw new SQLException("SECRET wrong phase");
            }));
            assertEquals(1, attempts.get());
            assertThrows(IllegalStateException.class,
                    () -> ConnectionErrorDemo.run(env, (url, properties) -> connection(new AtomicInteger())));
        }
    }

    @Test
    void loginRejectsNonSqlAuthenticationBeforeConnecting() {
        for (String authentication : new String[] {"authentication=ActiveDirectoryDefault", "integratedSecurity=true",
                "authenticationScheme=NTLM", "accessTokenCallbackClass=SECRET"}) {
            Map<String, String> env = environment();
            env.put("DEMO_SCENARIOS", "login");
            env.put("JDBC_CONNECTION_STRING", "jdbc:sqlserver://example.invalid;" + authentication);
            env.put("DEMO_LOGIN_USER", "deliberately-nonexistent");
            env.put("DEMO_LOGIN_PASSWORD", "synthetic-not-a-secret");
            assertThrows(IllegalArgumentException.class, () -> ConnectionErrorDemo.run(env, (url, properties) -> {
                fail("Non-SQL authentication must not be overridden");
                return null;
            }));
        }
    }

    @Test
    void localFailuresReachRealOtlpReceiverWithExpectedRootsAndNoStatementOrMetricPayload() throws Exception {
        try (Collector collector = new Collector()) {
            Map<String, String> env = collector.environment();
            env.put("DEMO_REPEAT", "2");
            ConnectionErrorDemo.Result result = ConnectionErrorDemo.run(env, new SQLServerDriver()::connect);
            assertEquals(4, result.failedConnections);
            assertEquals(0, result.successfulConnections);
            assertEquals(2, result.configurationFailures);
            assertEquals(2, result.dnsFailures);
            assertTrue(result.drained);
            assertTrue(result.flushed);
            String payload = collector.payload();
            assertEquals(4, occurrences(payload, "mssql.driver.connection.open"));
            assertTrue(payload.contains("configuration"));
            assertTrue(payload.contains("name_resolution"));
            assertTrue(payload.contains("mssql.connection.failure_phase"));
            for (String excluded : new String[] {"synthetic-not-a-secret", "statement", "db.query.text",
                    "connection.failures", "exception.message", "exception.stacktrace"}) {
                assertFalse(payload.contains(excluded), excluded);
            }
        }
    }

    @Test
    void standaloneMainUsesEnvironmentOnlyAndEmitsFixedSummaryWithRealNetworkEvidence() throws Exception {
        try (Collector collector = new Collector()) {
            String output = runMain(collector.environment(), 0);
            assertEquals("demo status=completed failed=2 successful=0 configuration=1 dns=1 login=0 "
                    + "drained=true flushed=true delivery=unverified", output.trim());
            assertEquals(2, occurrences(collector.payload(), "mssql.driver.connection.open"));
        }
    }

    @Test
    void standaloneMainRejectsBadConfigurationAndUnexpectedSuccessScenarioWithoutDiagnostics() throws Exception {
        Map<String, String> env = environment();
        env.put("DEMO_SCENARIOS", "SECRET");
        assertEquals("demo status=invalid_configuration", runMain(env, 1).trim());
        env.put("DEMO_SCENARIOS", "success");
        env.put("JDBC_CONNECTION_STRING", "jdbc:sqlserver://;portNumber=SECRET");
        // The success scenario fails locally; no connection details may escape to stdout/stderr.
        try (Collector collector = new Collector()) {
            env.put("OTEL_EXPORTER_OTLP_ENDPOINT", collector.environment().get("OTEL_EXPORTER_OTLP_ENDPOINT"));
            env.put("OTEL_ALLOW_INSECURE_LOCAL_ENDPOINT", "true");
            assertEquals("demo status=failed", runMain(env, 1).trim());
        }
    }

    private static String runMain(Map<String, String> env, int expectedExit) throws Exception {
        String java = Paths.get(System.getProperty("java.home"), "bin", "java").toString();
        String classpath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
        ProcessBuilder builder = new ProcessBuilder(java, "-cp", classpath, ConnectionErrorDemo.class.getName());
        String systemRoot = builder.environment().get("SystemRoot");
        builder.environment().clear();
        if (systemRoot != null) {
            builder.environment().put("SystemRoot", systemRoot);
        }
        builder.environment().putAll(env);
        builder.redirectErrorStream(true);
        Process process = builder.start();
        try {
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "Standalone demo must have a finite lifecycle");
            assertEquals(expectedExit, process.exitValue());
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int read;
            while ((read = process.getInputStream().read(buffer)) != -1) {
                output.write(buffer, 0, read);
            }
            return new String(output.toByteArray(), StandardCharsets.UTF_8);
        } finally {
            process.destroyForcibly();
        }
    }

    @Test
    void successOnlyOpensAndClosesWithoutSqlOrExportAndUnexpectedOutcomesFail() throws Exception {
        try (Collector collector = new Collector()) {
            Map<String, String> env = collector.environment();
            env.put("DEMO_SCENARIOS", "success");
            env.put("JDBC_CONNECTION_STRING", "jdbc:sqlserver://example.invalid");
            AtomicInteger closed = new AtomicInteger();
            ConnectionErrorDemo.Result result = ConnectionErrorDemo.run(env, (url, properties) -> connection(closed));
            assertEquals(1, closed.get());
            assertEquals(1, result.successfulConnections);
            assertEquals(0, result.failedConnections);
            assertTrue(collector.requests.isEmpty());
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> ConnectionErrorDemo.run(env, (url, properties) -> {
                        throw new SQLException("SECRET");
                    }));
            assertEquals("Unexpected demo scenario outcome", failure.getMessage());
            assertNull(failure.getCause());
            env.put("DEMO_SCENARIOS", "config");
            assertThrows(IllegalStateException.class,
                    () -> ConnectionErrorDemo.run(env, (url, properties) -> connection(closed)));
        }
    }

    @Test
    void authenticationCallbackRunsOnExporterWorkerAndFailureDropsExports() throws Exception {
        try (Collector collector = new Collector()) {
            Map<String, String> env = collector.environment();
            env.put("DEMO_SCENARIOS", "config");
            env.put("OTEL_AUTH_MODE", "azure_cli");
            env.put("OTEL_ACCESS_TOKEN_SCOPE", "https://approved.example/.default");
            TestTokenCallback.thread = null;
            TestTokenCallback.fail = true;
            ConnectionErrorDemo.run(env, new SQLServerDriver()::connect, TestTokenCallback.class);
            assertNotNull(TestTokenCallback.thread);
            assertTrue(TestTokenCallback.thread.getName().contains("BatchSpanProcessor"));
            assertNotSame(Thread.currentThread(), TestTokenCallback.thread);
            assertTrue(collector.requests.isEmpty());
            TestTokenCallback.fail = false;
            ConnectionErrorDemo.run(env, new SQLServerDriver()::connect, TestTokenCallback.class);
            assertTrue(collector.payload().contains("mssql.driver.connection.open"));
        }
    }

    @Test
    void failedRegistrationDoesNotUnregisterAnotherOwnersCallback() throws Exception {
        try (Collector collector = new Collector(); OtlpConnectionTelemetry owner = OtlpConnectionTelemetry
                .create(ConnectionErrorDemo.telemetryProperties(collector.environment()))) {
            SQLServerDriver.registerPerformanceLogCallback(owner.getCallback());
            try {
                assertThrows(IllegalStateException.class,
                        () -> ConnectionErrorDemo.run(collector.environment(), new SQLServerDriver()::connect));
                assertThrows(IllegalStateException.class,
                        () -> SQLServerDriver.registerPerformanceLogCallback(owner.getCallback()));
            } finally {
                SQLServerDriver.unregisterPerformanceLogCallback();
            }
        }
    }

    @Test
    void discoveryIsExplicitBoundedAndBeforeCallbackRegistration() throws Exception {
        Map<String, String> env = environment();
        env.remove("OTEL_EXPORTER_OTLP_ENDPOINT");
        env.put("DEMO_SCENARIOS", "success");
        env.put("JDBC_CONNECTION_STRING", "jdbc:sqlserver://example.invalid");
        env.put("DEMO_DISCOVERY_CONNECTION_STRING", "jdbc:sqlserver://discovery.invalid");
        AtomicInteger opens = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        assertThrows(IllegalArgumentException.class, () -> ConnectionErrorDemo.run(env, (url, properties) -> {
            fail("Missing discovery host approval must not connect");
            return null;
        }));
        env.put("OTEL_DISCOVERY_ALLOWED_HOST", "collector.example");
        Properties ownerProperties = new Properties();
        ownerProperties.setProperty("otelEndpoint", "https://collector.example");
        try (OtlpConnectionTelemetry probe = OtlpConnectionTelemetry.create(ownerProperties)) {
            ConnectionErrorDemo.Result result = ConnectionErrorDemo.run(env, (url, properties) -> {
                if (opens.incrementAndGet() == 1) {
                    assertEquals(env.get("DEMO_DISCOVERY_CONNECTION_STRING"), url);
                    assertEquals("5", properties.getProperty("loginTimeout"));
                    assertEquals("5000", properties.getProperty("socketTimeout"));
                    // This succeeds only if the demo has not registered telemetry during discovery.
                    SQLServerDriver.registerPerformanceLogCallback(probe.getCallback());
                    SQLServerDriver.unregisterPerformanceLogCallback();
                    return discoveryConnection(closes);
                }
                return connection(closes);
            });
            assertEquals(1, result.successfulConnections);
        }
        assertEquals(2, opens.get());
        assertEquals(4, closes.get()); // result set, statement, preflight connection, successful connection
    }

    @Test
    void discoveryRequiresExactApprovedHttpsHost() {
        Properties properties = new Properties();
        properties.setProperty("otelEndpoint", "https://approved.example/v1/metrics");
        ConnectionErrorDemo.approveDiscovery(properties, "approved.example");
        for (String endpoint : new String[] {"https://approved.example.evil.invalid", "http://approved.example",
                "https://SECRET@approved.example", "https://approved.example/?SECRET"}) {
            properties.setProperty("otelEndpoint", endpoint);
            assertThrows(IllegalArgumentException.class,
                    () -> ConnectionErrorDemo.approveDiscovery(properties, "approved.example"));
        }
        assertThrows(IllegalArgumentException.class, () -> ConnectionErrorDemo.approveDiscovery(properties, ""));
        properties.setProperty("otelEndpoint", "http://127.0.0.1");
        properties.setProperty("otelAllowInsecureLocalEndpoint", "true");
        assertThrows(IllegalArgumentException.class,
                () -> ConnectionErrorDemo.approveDiscovery(properties, "127.0.0.1"));
    }

    private static Connection discoveryConnection(AtomicInteger closes) {
        ResultSet result = (ResultSet) Proxy.newProxyInstance(ResultSet.class.getClassLoader(),
                new Class<?>[] {ResultSet.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "next":
                            return true;
                        case "getString":
                            return (Integer) args[0] == 1 ? "https://collector.example/v1/metrics" : null;
                        case "close":
                            closes.incrementAndGet();
                            return null;
                        default:
                            throw new AssertionError("Unexpected discovery result operation");
                    }
                });
        Statement statement = (Statement) Proxy.newProxyInstance(Statement.class.getClassLoader(),
                new Class<?>[] {Statement.class}, (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "setQueryTimeout":
                            assertEquals(5, args[0]);
                            return null;
                        case "executeQuery":
                            assertEquals("SELECT TOP 1 DemoLocalOtelEndpoint, AzureResourceId, "
                                    + "AzureRegion FROM msdb.dbo.SQLServerAzureArcProperties", args[0]);
                            return result;
                        case "close":
                            closes.incrementAndGet();
                            return null;
                        default:
                            throw new AssertionError("Unexpected discovery statement operation");
                    }
                });
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if ("createStatement".equals(method.getName())) {
                        return statement;
                    }
                    if ("close".equals(method.getName())) {
                        closes.incrementAndGet();
                        return null;
                    }
                    throw new AssertionError("Unexpected discovery connection operation");
                });
    }

    private static Connection connection(AtomicInteger closed) {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(), new Class<?>[] {Connection.class},
                (proxy, method, args) -> {
                    if ("close".equals(method.getName())) {
                        closed.incrementAndGet();
                        return null;
                    }
                    throw new AssertionError("Success scenario must only close the connection");
                });
    }

    private static Map<String, String> environment() {
        Map<String, String> env = new HashMap<>();
        env.put("OTEL_EXPORTER_OTLP_ENDPOINT", "https://collector.example");
        env.put("DEMO_PAUSE_SECONDS", "0");
        return env;
    }

    private static int occurrences(String value, String needle) {
        return (value.length() - value.replace(needle, "").length()) / needle.length();
    }

    public static final class TestTokenCallback implements SQLServerAccessTokenCallback {
        static volatile Thread thread;
        static volatile boolean fail;

        @Override
        public SqlAuthenticationToken getAccessToken(String scope, String authority) {
            thread = Thread.currentThread();
            if (fail) {
                throw new IllegalStateException("SECRET provider failure");
            }
            return new SqlAuthenticationToken("TEST-TOKEN", System.currentTimeMillis() + 60_000);
        }
    }

    private static final class Collector implements AutoCloseable {
        final BlockingQueue<String> requests = new LinkedBlockingQueue<>();
        final HttpServer server;

        Collector() throws Exception {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.createContext("/", exchange -> {
                try {
                    ByteArrayOutputStream body = new ByteArrayOutputStream();
                    byte[] buffer = new byte[4096];
                    int read;
                    while ((read = exchange.getRequestBody().read(buffer)) != -1) {
                        body.write(buffer, 0, read);
                    }
                    requests.add(exchange.getRequestURI().getPath() + "\n"
                            + new String(body.toByteArray(), StandardCharsets.UTF_8));
                    exchange.getResponseHeaders().set("Content-Type", "application/x-protobuf");
                    exchange.sendResponseHeaders(200, -1);
                } finally {
                    exchange.close();
                }
            });
            server.start();
        }

        Map<String, String> environment() {
            Map<String, String> env = ConnectionErrorDemoTest.environment();
            env.put("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:" + server.getAddress().getPort());
            env.put("OTEL_ALLOW_INSECURE_LOCAL_ENDPOINT", "true");
            return env;
        }

        String payload() throws Exception {
            String first = requests.poll(10, TimeUnit.SECONDS);
            assertNotNull(first, "Expected positive OTLP network evidence");
            StringBuilder payload = new StringBuilder(first);
            assertTrue(first.startsWith("/v1/traces\n"));
            String next;
            while ((next = requests.poll()) != null) {
                assertTrue(next.startsWith("/v1/traces\n"));
                payload.append(next);
            }
            return payload.toString();
        }

        @Override
        public void close() {
            server.stop(0);
        }
    }
}
