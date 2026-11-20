/*
 * Microsoft JDBC Driver for SQL Server
 * Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Properties;
import java.util.ServiceLoader;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.UserOtelTelemetryProvider;
import com.microsoft.sqlserver.jdbc.otel.UserEndpointTelemetryProvider;


/** Local-only workload; never reads repository test configuration or cloud credentials. */
public final class UserEndpointDemo {
    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null ? fallback : value;
    }

    private static Properties telemetry() {
        Properties properties = new Properties();
        properties.setProperty("otelEndpoint", "http://127.0.0.1:4318");
        properties.setProperty("otelLogLevel", "2");
        return properties;
    }

    private static void verifyClasspath() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        for (Class<?> type : new Class<?>[] {SQLServerDriver.class, UserEndpointTelemetryProvider.class}) {
            if (!type.getProtectionDomain().getCodeSource().getLocation().getPath().endsWith(".jar")) {
                throw new IllegalStateException("Packaged core and adapter JARs are required");
            }
            System.out.println("Packaged class origin: " + type.getProtectionDomain().getCodeSource().getLocation());
        }
        boolean found = false;
        for (UserOtelTelemetryProvider provider : ServiceLoader.load(UserOtelTelemetryProvider.class)) {
            found |= provider.getClass() == UserEndpointTelemetryProvider.class;
        }
        if (!found) {
            throw new IllegalStateException("Automatic provider missing from runtime SPI");
        }
        System.out.println("Automatic provider SPI present; no manual callback or Java agent");
    }

    private static void expectedConnectionFailure(boolean dns) throws SQLException {
        Properties properties = telemetry();
        properties.setProperty("loginTimeout", "3");
        properties.setProperty("connectRetryCount", "0");
        // These are nonsecret dummy values, never local SQL credentials.
        properties.setProperty("user", "demo-unused");
        properties.setProperty("password", "demo-unused");
        if (!dns) {
            properties.setProperty("portNumber", "invalid");
        }
        String url = dns ? "jdbc:sqlserver://jdbc-user-endpoint-demo.invalid:1433" : "jdbc:sqlserver://";
        try (Connection unexpected = DriverManager.getConnection(url, properties)) {
            throw new IllegalStateException("Expected connection failure did not occur");
        } catch (SQLException expected) {
            System.out.println(
                    dns ? "Expected DNS connection failure observed" : "Expected port configuration failure observed");
        }
    }

    private static void consume(ResultSet results) throws SQLException {
        try (ResultSet rs = results) {
            if (!rs.next() || rs.getInt(1) != 1 || rs.next()) {
                throw new IllegalStateException("Unexpected constant-query result");
            }
        }
    }

    private static void syntaxFailure(Statement statement) throws SQLException {
        boolean failed = false;
        try {
            if (statement instanceof PreparedStatement) {
                ((PreparedStatement) statement).executeQuery();
            } else {
                statement.executeQuery("SELECT 1 + ;");
            }
        } catch (SQLException expected) {
            if (expected.getErrorCode() != 102) {
                throw new IllegalStateException("Expected SQL Server syntax error 102 was not observed");
            }
            failed = true;
            System.out.println("Expected syntax error code: 102");
        }
        if (!failed) {
            throw new IllegalStateException("Expected syntax failure did not occur");
        }
    }

    private static void flush() {
        // This is completion, not a delivery acknowledgement; run.ps1 verifies downstream evidence.
        System.out.println("Flush completed (not delivery ACK): "
                + UserEndpointTelemetryProvider.forceFlush(Duration.ofSeconds(15)));
    }

    private static void workload() throws Exception {
        String password = System.getenv("JDBC_DEMO_PASSWORD");
        if (password == null || password.isEmpty()) {
            throw new IllegalArgumentException("Local container password is required");
        }
        Properties properties = telemetry();
        properties.setProperty("user", "sa");
        properties.setProperty("password", password);
        properties.setProperty("loginTimeout", "10");
        properties.setProperty("connectRetryCount", "0");
        properties.setProperty("responseBuffering", "full");
        int iterations = Integer.parseInt(env("DEMO_ITERATIONS", "5"));
        if (iterations < 1 || iterations > 100) {
            throw new IllegalArgumentException("Demo iterations must be between 1 and 100");
        }
        // Explicit demo-only self-signed certificate trust, exclusively on the local container port.
        String url = "jdbc:sqlserver://127.0.0.1:14333;databaseName=master;encrypt=true;trustServerCertificate=true";
        for (int i = 0; i < iterations; i++) {
            try (Connection connection = DriverManager.getConnection(url, properties);
                    Statement statement = connection.createStatement()) {
                if (i == 0) {
                    flush();
                    // Establish a scrape baseline before statement counters increase.
                    Thread.sleep(6000);
                }
                statement.setQueryTimeout(15);
                consume(statement.executeQuery("SELECT 1 AS demo_value"));
                consume(statement.executeQuery("WAITFOR DELAY '00:00:02'; SELECT 1 AS demo_value"));
                syntaxFailure(statement);
                try (PreparedStatement prepared = connection.prepareStatement("SELECT ? AS demo_value")) {
                    prepared.setQueryTimeout(15);
                    prepared.setInt(1, 1);
                    consume(prepared.executeQuery());
                }
                try (PreparedStatement prepared = connection.prepareStatement("SELECT ? + ;")) {
                    prepared.setQueryTimeout(15);
                    prepared.setInt(1, 1);
                    syntaxFailure(prepared);
                }
            }
            System.out.println(
                    "Completed connection, fast/slow SQL, statement/prepared success/failure iteration " + (i + 1));
            flush();
            // Scrape interval is five seconds. Rates and quantiles remain sparse estimates.
            Thread.sleep(6000);
        }
    }

    public static void main(String[] args) {
        int exit = 0;
        try {
            verifyClasspath();
            switch (env("DEMO_MODE", "workload")) {
                case "workload":
                    expectedConnectionFailure(false);
                    expectedConnectionFailure(true);
                    workload();
                    break;
                case "config":
                    expectedConnectionFailure(false);
                    break;
                case "dns":
                    expectedConnectionFailure(true);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown demo mode");
            }
        } catch (Exception | LinkageError | java.util.ServiceConfigurationError failure) {
            // Never print JDBC messages, SQL text, credentials, URLs or stack traces.
            System.err.println("Demo failed; diagnostic class only: " + failure.getClass().getName());
            exit = 1;
        } finally {
            try {
                flush();
                Thread.sleep(15000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                exit = 1;
            } finally {
                UserEndpointTelemetryProvider.shutdown();
            }
        }
        System.exit(exit);
    }
}
