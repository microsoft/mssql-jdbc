/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.testing.exporter.InMemoryMetricReader;

/**
 * End-to-end POC test for the OpenTelemetry export pipeline added in docs/otelproposal.md (Solution 4).
 * <p>
 * Wires an {@link InMemoryMetricReader} into a {@link SdkMeterProvider}, publishes it as the JVM-wide
 * {@link GlobalOpenTelemetry} (so {@code OtelBootstrap} reuses it instead of building an OTLP exporter),
 * then opens a real JDBC connection with {@code otelEndpoint} set and runs a small workload. After a
 * flush, asserts that the per-event histograms emitted by {@code OpenTelemetryPerformanceCallback}
 * have actually been recorded.
 * <p>
 * Requires the {@code mssql_jdbc_test_connection_properties} env var (same as the rest of the suite).
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class OtelPocSmokeTest extends AbstractTest {

    private static InMemoryMetricReader reader;
    private static SdkMeterProvider meterProvider;

    @BeforeAll
    public static void setupOtelAndConnection() throws Exception {
        // GlobalOpenTelemetry.set(...) must run before any connection opens with otelEndpoint set,
        // otherwise OtelBootstrap will build its own OTLP/HTTP exporter pointed at the (fake) endpoint
        // and we will see no metrics in our reader.
        reader = InMemoryMetricReader.create();
        meterProvider = SdkMeterProvider.builder()
                .setResource(Resource.create(Attributes.empty()))
                .registerMetricReader(reader)
                .build();
        GlobalOpenTelemetry.set(OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build());

        setConnection();
    }

    @AfterAll
    public static void tearDownOtel() {
        if (meterProvider != null) {
            meterProvider.close();
        }
        GlobalOpenTelemetry.resetForTest();
    }

    @Test
    public void endToEndDriverEventsBecomeOtelMetrics() throws SQLException {
        // Append otelEndpoint to flip OtelBootstrap on for this connection. The value is a placeholder;
        // because GlobalOpenTelemetry is already set above, the bootstrap reuses it and never touches
        // this URL.
        String url = connectionString
                + (connectionString.endsWith(";") ? "" : ";")
                + "otelEndpoint=http://unused.invalid/v1/metrics;"
                + "otelServiceName=mssql-jdbc-poc-smoke-test;";

        try (Connection con = DriverManager.getConnection(url);
                Statement stmt = con.createStatement()) {
            for (int i = 0; i < 5; i++) {
                try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
                    while (rs.next()) {
                        rs.getInt(1);
                    }
                }
            }
        }

        // Drive an error so the db.client.errors counter has a non-zero sample.
        try (Connection con = DriverManager.getConnection(url);
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM dbo.__this_table_does_not_exist_otel_poc__")) {
            fail("expected the bad query to throw");
        } catch (SQLException expected) {
            // expected
        }

        // PeriodicMetricReader is not in play; InMemoryMetricReader collects on demand. flush() forces
        // every instrument to publish its current aggregation.
        meterProvider.forceFlush().join(5, java.util.concurrent.TimeUnit.SECONDS);
        Collection<MetricData> metrics = reader.collectAllMetrics();

        Set<String> names = metrics.stream().map(MetricData::getName).collect(Collectors.toSet());

        System.out.println("---- OTel POC metrics captured (" + names.size() + ") ----");
        for (MetricData md : metrics) {
            System.out.println("  " + md.getName() + "  (" + md.getType() + ", unit=" + md.getUnit() + ")");
        }
        System.out.println("---------------------------------------------------------");

        // Connection-lifecycle histograms — at minimum the wrapping CONNECTION metric must show up.
        assertTrue(names.contains("db.client.connection.duration"),
                "expected db.client.connection.duration, got " + names);

        // The statement-level histogram for our executeQuery loop.
        assertTrue(names.contains("db.client.statement.execute.duration"),
                "expected db.client.statement.execute.duration, got " + names);

        // The error counter from the failing query.
        assertTrue(names.contains("db.client.errors"),
                "expected db.client.errors, got " + names);

        // Sanity check: no leaked instruments with empty / unprefixed names.
        assertFalse(names.stream().anyMatch(n -> n == null || n.isEmpty() || !n.startsWith("db.client.")),
                "unexpected metric name(s) in " + names);

        // And the duration histograms must be in ms per the proposal doc.
        for (MetricData md : metrics) {
            if (md.getName().startsWith("db.client.") && md.getName().endsWith(".duration")) {
                assertTrue("ms".equals(md.getUnit()),
                        md.getName() + " expected unit=ms, got " + md.getUnit());
            }
        }
    }

    /**
     * Demonstrates the convenience path: passing properties to DriverManager as a {@link Properties}
     * map (the typical app-server / pooled DataSource shape). Same activation rules apply.
     */
    @Test
    public void worksWhenOtelEndpointIsPassedViaProperties() throws SQLException {
        Properties extra = new Properties();
        extra.putAll(connectionStringToProperties(connectionString));
        extra.setProperty("otelEndpoint", "http://unused.invalid/v1/metrics");

        try (Connection con = DriverManager.getConnection(buildBareUrl(connectionString), extra);
                Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT 42")) {
            while (rs.next()) {
                rs.getInt(1);
            }
        }

        meterProvider.forceFlush().join(5, java.util.concurrent.TimeUnit.SECONDS);
        assertTrue(reader.collectAllMetrics().stream()
                .anyMatch(m -> "db.client.statement.execute.duration".equals(m.getName())),
                "expected at least one db.client.statement.execute.duration sample");
    }

    private static String buildBareUrl(String url) {
        int semi = url.indexOf(';');
        return semi < 0 ? url : url.substring(0, semi);
    }

    private static Properties connectionStringToProperties(String url) {
        Properties p = new Properties();
        int semi = url.indexOf(';');
        if (semi < 0) {
            return p;
        }
        for (String pair : url.substring(semi + 1).split(";")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                p.setProperty(pair.substring(0, eq).trim(), pair.substring(eq + 1).trim());
            }
        }
        return p;
    }
}
