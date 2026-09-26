/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import java.net.URI;
import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.Logger;

import com.microsoft.sqlserver.jdbc.PerformanceActivity;
import com.microsoft.sqlserver.jdbc.PerformanceLogCallback;
import com.microsoft.sqlserver.jdbc.PerformanceLogEvent;
import com.microsoft.sqlserver.jdbc.SQLServerAccessTokenCallback;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;


/**
 * Environment-only, finite failure-only connection demo. No Java agent, statement workload, metric producer or
 * automatic login is installed. Default config/dns failures need neither SQL Server nor Azure. The standalone main
 * owns process logging policy and the single driver callback slot; embedding applications should use the adapter
 * directly. Completion/flush is not delivery proof: inspect the collector's positive trace evidence separately.
 */
public final class ConnectionErrorDemo {
    private static final Duration WAIT = Duration.ofSeconds(5);

    private ConnectionErrorDemo() {}

    /**
     * Runs using process environment only and exits nonzero for invalid settings or unexpected scenario outcomes.
     *
     * @param args
     *        must be empty; credentials and connection strings are never accepted on the command line
     */
    public static void main(String[] args) {
        // This executable's fixed diagnostics must not be undermined by provider/driver/collector exception logs.
        // Do not impose these process-wide settings in run(), tests, or the reusable runtime adapter.
        LogManager.getLogManager().reset();
        Logger.getLogger("").setLevel(Level.OFF);
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "off");
        int exit = 1;
        try {
            if (args.length != 0) {
                throw invalidConfiguration();
            }
            Result result = run(System.getenv(), new SQLServerDriver()::connect);
            System.out.println("demo status=completed failed=" + result.failedConnections + " successful="
                    + result.successfulConnections + " configuration=" + result.configurationFailures + " dns="
                    + result.dnsFailures + " login=" + result.loginFailures + " drained=" + result.drained + " flushed="
                    + result.flushed + " delivery=unverified");
            exit = result.drained && result.flushed ? 0 : 1;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("demo status=interrupted");
        } catch (IllegalArgumentException e) {
            System.err.println("demo status=invalid_configuration");
        } catch (Exception | LinkageError e) {
            System.err.println("demo status=failed");
        }
        if (exit != 0) {
            System.exit(exit);
        }
    }

    @FunctionalInterface
    interface ConnectionProvider {
        Connection open(String url, Properties properties) throws SQLException;
    }

    static Result run(Map<String, String> env, ConnectionProvider connections) throws Exception {
        return run(env, connections, DemoTokenCallback.class);
    }

    static Result run(Map<String, String> environment, ConnectionProvider connections,
            Class<? extends SQLServerAccessTokenCallback> tokenProvider) throws Exception {
        Map<String, String> env = new HashMap<>(environment);
        String[] selection = value(env, "DEMO_SCENARIOS", "config,dns").split(",", -1);
        Set<String> scenarios = new LinkedHashSet<>(Arrays.asList(selection));
        if (scenarios.size() != selection.length
                || !Arrays.asList("config", "dns", "login", "success").containsAll(scenarios)) {
            throw invalidConfiguration();
        }
        int repeats = boundedInteger(env, "DEMO_REPEAT", 1, 1, 1000);
        int pauseSeconds = boundedInteger(env, "DEMO_PAUSE_SECONDS", 1, 0, 60);
        String jdbc = null;
        if (scenarios.contains("login") || scenarios.contains("success")) {
            jdbc = required(env, "JDBC_CONNECTION_STRING");
            validateJdbc(jdbc, scenarios.contains("login"));
        }
        if (scenarios.contains("login")) {
            required(env, "DEMO_LOGIN_USER");
            required(env, "DEMO_LOGIN_PASSWORD");
        }
        Properties settings = telemetryProperties(env);
        if (settings.containsKey("otelAccessTokenCallbackClass")) {
            settings.setProperty("otelAccessTokenCallbackClass", tokenProvider.getName());
        }
        if (!settings.containsKey("otelEndpoint")) {
            String discoveryUrl = required(env, "DEMO_DISCOVERY_CONNECTION_STRING");
            String allowedHost = required(env, "OTEL_DISCOVERY_ALLOWED_HOST");
            validateJdbc(discoveryUrl, false);
            // Explicit preflight, before constructing/registering this demo's telemetry. Operator controls metadata.
            try (Connection connection = connections.open(discoveryUrl, deadlines())) {
                if (connection == null) {
                    throw invalidConfiguration();
                }
                Properties discovered = TelemetryEndpointDiscovery.discover(connection, 5);
                approveDiscovery(discovered, allowedHost);
                settings.setProperty("otelEndpoint", discovered.getProperty("otelEndpoint"));
                if (!settings.containsKey("otelArmResourceId") && discovered.containsKey("otelArmResourceId")) {
                    settings.setProperty("otelArmResourceId", discovered.getProperty("otelArmResourceId"));
                }
            } catch (SQLException e) {
                throw new IllegalStateException("Demo discovery failed");
            }
        }
        Result result = new Result();
        try (OtlpConnectionTelemetry telemetry = OtlpConnectionTelemetry.create(settings)) {
            ObservingCallback callback = new ObservingCallback(telemetry.getCallback());
            boolean registered = false;
            try {
                SQLServerDriver.registerPerformanceLogCallback(callback);
                registered = true;
                for (int repeat = 0; repeat < repeats; repeat++) {
                    for (String scenario : scenarios) {
                        execute(scenario, jdbc, env, connections, callback, result);
                    }
                    if (repeat + 1 < repeats && pauseSeconds > 0) {
                        Thread.sleep(pauseSeconds * 1000L);
                    }
                }
            } finally {
                if (registered) {
                    SQLServerDriver.unregisterPerformanceLogCallback();
                }
                result.drained = telemetry.awaitIdle(WAIT);
                result.flushed = telemetry.forceFlush(WAIT);
            }
        }
        return result;
    }

    private static void execute(String scenario, String jdbc, Map<String, String> env, ConnectionProvider connections,
            ObservingCallback callback, Result result) throws SQLException {
        Properties properties = deadlines();
        String url = jdbc;
        if ("config".equals(scenario) || "dns".equals(scenario)) {
            url = "jdbc:sqlserver://";
            properties.setProperty("authentication", "SqlPassword");
            properties.setProperty("user", "synthetic-demo-user");
            properties.setProperty("password", "synthetic-not-a-secret");
            if ("config".equals(scenario)) {
                properties.setProperty("portNumber", "invalid");
            } else {
                // Malformed IPv6 literal: local name-resolution rejection, without an external DNS lookup.
                properties.setProperty("serverName", "invalid::literal");
            }
        } else if ("login".equals(scenario)) {
            properties.setProperty("authentication", "SqlPassword");
            properties.setProperty("user", required(env, "DEMO_LOGIN_USER"));
            properties.setProperty("password", required(env, "DEMO_LOGIN_PASSWORD"));
        }
        callback.root = null;
        callback.roots = 0;
        boolean failed = false;
        // Deliberately no SQL statements, even for success. A close failure is not a successful negative control.
        try (Connection connection = connections.open(url, properties)) {
            if (connection == null) {
                throw unexpectedOutcome();
            }
        } catch (SQLException e) {
            failed = true;
        }
        if ("success".equals(scenario)) {
            if (failed || (callback.root != null && callback.root.hasException())) {
                throw unexpectedOutcome();
            }
            result.successfulConnections++;
        } else {
            String phase = "config".equals(scenario) ? "configuration" : scenario;
            String category = "config".equals(scenario) ? "configuration"
                                                        : "dns".equals(scenario) ? "name_resolution" : "authentication";
            if (!failed || callback.roots != 1 || callback.root == null || !callback.root.hasException()
                    || !phase.equals(callback.root.getFailurePhase())
                    || !category.equals(callback.root.getAttributes().get("mssql.error.category"))) {
                throw unexpectedOutcome();
            }
            result.failedConnections++;
            if ("config".equals(scenario)) {
                result.configurationFailures++;
            } else if ("dns".equals(scenario)) {
                result.dnsFailures++;
            } else {
                result.loginFailures++;
            }
        }
    }

    static Properties telemetryProperties(Map<String, String> env) {
        try {
            Properties properties = new Properties();
            copy(env, properties, "OTEL_EXPORTER_OTLP_ENDPOINT", "otelEndpoint");
            copy(env, properties, "OTEL_SERVICE_NAME", "otelServiceName");
            copy(env, properties, "OTEL_ARM_RESOURCE_ID", "otelArmResourceId");
            copy(env, properties, "OTEL_ALLOW_INSECURE_LOCAL_ENDPOINT", "otelAllowInsecureLocalEndpoint");
            copy(env, properties, "OTEL_ALLOW_INSECURE_DEVELOPMENT_ENDPOINT", "otelAllowInsecureDevelopmentEndpoint");
            String mode = value(env, "OTEL_AUTH_MODE", "none");
            if (!Arrays.asList("none", "static", "azure_cli", "managed_identity", "default").contains(mode)) {
                throw invalidConfiguration();
            }
            if (!"none".equals(mode)) {
                properties.setProperty("otelTokenScope", required(env, "OTEL_ACCESS_TOKEN_SCOPE"));
                properties.setProperty("otelTokenAuthority", DemoTokenCallback.authority(env));
                if ("static".equals(mode)) {
                    properties.setProperty("otelBearerToken", required(env, "OTEL_BEARER_TOKEN"));
                } else {
                    properties.setProperty("otelAccessTokenCallbackClass", DemoTokenCallback.class.getName());
                }
            }
            boolean discovery = !properties.containsKey("otelEndpoint");
            if (discovery) {
                required(env, "DEMO_DISCOVERY_CONNECTION_STRING");
                required(env, "OTEL_DISCOVERY_ALLOWED_HOST");
                // Validate non-routing configuration before any discovery SQL, without contacting this placeholder.
                properties.setProperty("otelEndpoint", "https://discovery-validation.invalid");
            }
            try (OtlpConfiguration ignored = new OtlpConfiguration(properties)) {
                // Construction validates only; token callbacks are never instantiated here.
            }
            if (discovery) {
                properties.remove("otelEndpoint");
            }
            return properties;
        } catch (IllegalArgumentException e) {
            throw invalidConfiguration();
        }
    }

    static void approveDiscovery(Properties discovered, String allowedHost) {
        try {
            // Ignore insecure flags: discovery is always HTTPS, even when a local transport was opted in.
            Properties routing = new Properties();
            String endpoint = discovered.getProperty("otelEndpoint");
            if (endpoint == null) {
                throw invalidConfiguration();
            }
            routing.setProperty("otelEndpoint", endpoint);
            String normalized = OtlpConfiguration.normalizeEndpoint(routing);
            if (allowedHost == null || allowedHost.isEmpty()
                    || !allowedHost.equalsIgnoreCase(URI.create(normalized).getHost())) {
                throw invalidConfiguration();
            }
        } catch (IllegalArgumentException e) {
            throw invalidConfiguration();
        }
    }

    private static void validateJdbc(String url, boolean sqlPasswordOnly) {
        try {
            for (DriverPropertyInfo property : new SQLServerDriver().getPropertyInfo(url, new Properties())) {
                String name = property.name;
                String value = property.value;
                if (!sqlPasswordOnly || value == null || value.isEmpty()) {
                    continue;
                }
                if (("authentication".equals(name) && !"NotSpecified".equalsIgnoreCase(value)
                        && !"SqlPassword".equalsIgnoreCase(value))
                        || ("integratedSecurity".equals(name) && !"false".equalsIgnoreCase(value))
                        || ("authenticationScheme".equals(name) && !"nativeAuthentication".equalsIgnoreCase(value))
                        || "accessTokenCallbackClass".equals(name)) {
                    throw invalidConfiguration();
                }
            }
        } catch (SQLException e) {
            throw invalidConfiguration();
        }
    }

    private static Properties deadlines() {
        Properties properties = new Properties();
        properties.setProperty("loginTimeout", "5");
        properties.setProperty("socketTimeout", "5000");
        properties.setProperty("queryTimeout", "5");
        properties.setProperty("cancelQueryTimeout", "2");
        properties.setProperty("connectRetryCount", "0");
        return properties;
    }

    private static void copy(Map<String, String> env, Properties properties, String variable, String property) {
        String value = value(env, variable, null);
        if (value != null) {
            properties.setProperty(property, value);
        }
    }

    static String value(Map<String, String> env, String name, String fallback) {
        String value = env.get(name);
        return value == null || value.trim().isEmpty() ? fallback : value;
    }

    static String required(Map<String, String> env, String name) {
        String value = value(env, name, null);
        if (value == null) {
            throw invalidConfiguration();
        }
        return value;
    }

    private static int boundedInteger(Map<String, String> env, String name, int fallback, int minimum, int maximum) {
        try {
            int result = Integer.parseInt(value(env, name, Integer.toString(fallback)));
            if (result < minimum || result > maximum) {
                throw invalidConfiguration();
            }
            return result;
        } catch (NumberFormatException e) {
            throw invalidConfiguration();
        }
    }

    static IllegalArgumentException invalidConfiguration() {
        return new IllegalArgumentException("Invalid demo configuration");
    }

    private static IllegalStateException unexpectedOutcome() {
        return new IllegalStateException("Unexpected demo scenario outcome");
    }

    static final class Result {
        int failedConnections;
        int successfulConnections;
        int configurationFailures;
        int dnsFailures;
        int loginFailures;
        boolean drained;
        boolean flushed;
    }

    /** Observes only sanitized root metadata and delegates to the existing failure-only adapter. */
    private static final class ObservingCallback implements PerformanceLogCallback {
        private final OpenTelemetryConnectionCallback delegate;
        private PerformanceLogEvent root;
        private int roots;

        ObservingCallback(OpenTelemetryConnectionCallback delegate) {
            this.delegate = delegate;
        }

        @Override
        public void publish(PerformanceLogEvent event) {
            if (event.getType() == PerformanceLogEvent.Type.END
                    && event.getActivity() == PerformanceActivity.CONNECTION) {
                root = event.withoutException();
                roots++;
            }
            delegate.publish(event);
        }

        @Override
        public void publish(PerformanceActivity activity, int connectionId, long duration, Exception exception) {}

        @Override
        public void publish(PerformanceActivity activity, int connectionId, int statementId, long duration,
                Exception exception) {}
    }
}
