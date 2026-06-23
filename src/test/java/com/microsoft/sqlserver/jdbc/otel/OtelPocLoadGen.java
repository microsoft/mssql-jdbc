/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.otel;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Standalone load generator for the docs/otel-poc Grafana stack. Unlike
 * {@code OtelPocSmokeTest}, this class does NOT pre-register a
 * {@code GlobalOpenTelemetry}, so {@code OtelBootstrap} builds its own
 * OTLP/HTTP exporter that ships metrics to {@code otelEndpoint} for real.
 * <p>
 * Run with:
 * <pre>
 *   mvn -B -Pjre11 -DskipTests test-compile
 *   $env:mssql_jdbc_test_connection_properties = "jdbc:sqlserver://...";
 *   mvn -B -Pjre11 exec:java `
 *     -Dexec.classpathScope=test `
 *     -Dexec.mainClass=com.microsoft.sqlserver.jdbc.otel.OtelPocLoadGen `
 *     -Dexec.args="30"
 * </pre>
 * The single arg is how many iterations to run (default 60). Each iteration:
 * opens a connection, runs 5 SELECT statements, and runs one failing query to
 * bump the {@code db.client.errors} counter. Endpoint defaults to
 * {@code http://localhost:4318/v1/metrics} (the docker-compose collector) and
 * {@code otelExportInterval=5} so you see metrics in Grafana within ~10 s.
 */
public final class OtelPocLoadGen {

    private OtelPocLoadGen() {}

    public static void main(String[] args) throws SQLException, InterruptedException {
        // First arg: positive => iterations, 0 or "forever" => infinite, negative => seconds duration.
        int iterations = 60;
        long durationMs = -1;
        if (args.length > 0) {
            String a = args[0].toLowerCase();
            if (a.equals("forever") || a.equals("inf") || a.equals("-1")) {
                durationMs = Long.MAX_VALUE;
            } else {
                int n = Integer.parseInt(a);
                if (n <= 0) {
                    durationMs = Long.MAX_VALUE;
                } else if (args.length > 1 && args[1].equalsIgnoreCase("s")) {
                    durationMs = n * 1000L;
                } else {
                    iterations = n;
                }
            }
        }

        String baseUrl = System.getenv("mssql_jdbc_test_connection_properties");
        if (baseUrl == null || baseUrl.isEmpty()) {
            System.err.println("Set env var mssql_jdbc_test_connection_properties to a valid JDBC URL.");
            System.exit(2);
        }
        String otelEndpoint = System.getProperty("otelEndpoint", "http://localhost:4318/v1/metrics");
        int intervalSec = Integer.parseInt(System.getProperty("otelExportInterval", "5"));
        int sleepMs = Integer.parseInt(System.getProperty("loadgen.sleepMs", "200"));
        String otelUseSqlAccessToken = System.getProperty("otelUseSqlAccessToken", "");
        String otelAccessTokenCallbackClass = System.getProperty("otelAccessTokenCallbackClass", "");
        String otelHeaders = System.getProperty("otelHeaders", "");

        // Per-connection prefix; prepareMethod is appended below so we can
        // rotate it across iterations and exercise every code path.
        String urlPrefix = baseUrl
                + (baseUrl.endsWith(";") ? "" : ";")
                + "otelEndpoint=" + otelEndpoint + ";"
                + "otelServiceName=mssql-jdbc-poc-loadgen;"
                + "otelExportInterval=" + intervalSec + ";"
                + "trustServerCertificate=true;"
                + "statementPoolingCacheSize=20;"
                + "disableStatementPooling=false;"
                + "enablePrepareOnFirstPreparedStatementCall=true;"
                + (otelUseSqlAccessToken.isEmpty() ? "" : "otelUseSqlAccessToken=" + otelUseSqlAccessToken + ";")
                + (otelAccessTokenCallbackClass.isEmpty() ? ""
                        : "otelAccessTokenCallbackClass=" + otelAccessTokenCallbackClass + ";")
                // Brace-wrap so the comma/equals-laden header list survives connection-string parsing.
                + (otelHeaders.isEmpty() ? "" : "otelHeaders={" + otelHeaders + "};");

        // Rotate the four prepareMethod values so we light up every histogram:
        //   prepare                       -> sp_prepare  (STATEMENT_PREPARE)
        //   prepexec (driver default)     -> sp_prepexec (STATEMENT_PREPEXEC)
        //   none                          -> direct SQL with literal params (no prepare/prepexec, just execute)
        //   scopeTempTablesToConnection   -> same as prepexec for non-temp-table SQL (our case)
        String[] prepareMethods = { "prepare", "prepexec", "none", "scopeTempTablesToConnection" };

        System.out.println("== mssql-jdbc OTel POC load generator ==");
        System.out.println("  mode        = " + (durationMs > 0
                ? (durationMs == Long.MAX_VALUE ? "forever (Ctrl-C to stop)" : (durationMs / 1000) + "s")
                : iterations + " iterations"));
        System.out.println("  otelEndpoint= " + otelEndpoint);
        System.out.println("  interval    = " + intervalSec + "s");
        System.out.println("  sleepMs     = " + sleepMs);
        System.out.println("  Grafana     = http://localhost:3000  (Dashboards -> mssql-jdbc)");
        System.out.println();

        long t0 = System.currentTimeMillis();
        int connects = 0, queries = 0, prepared = 0, errors = 0;
        int i = 0;
        java.util.Random rnd = new java.util.Random();

        // Mix of fast + slow queries so histogram_quantile() actually spreads across buckets.
        // WAITFOR DELAY introduces a deterministic server-side latency in ms.
        String fastQuery = "SELECT TOP 1 name FROM sys.databases";
        String mediumQuery = "WAITFOR DELAY '00:00:00.020'; SELECT TOP 50 name FROM sys.objects";
        String slowQuery = "WAITFOR DELAY '00:00:00.080'; SELECT TOP 200 name FROM sys.all_objects";
        // Parameterised queries -> exercises STATEMENT_PREPARE / STATEMENT_PREPEXEC histograms.
        String prepLookup = "SELECT name, object_id FROM sys.objects WHERE type = ? AND name LIKE ?";
        String prepCount  = "SELECT COUNT(*) FROM sys.all_columns WHERE system_type_id = ?";
        String[] objectTypes = { "U", "V", "P", "FN", "SO" };

        while (true) {
            if (durationMs > 0) {
                if (System.currentTimeMillis() - t0 >= durationMs) break;
            } else if (i >= iterations) {
                break;
            }
            String pm = prepareMethods[i % prepareMethods.length];
            String url = urlPrefix + "prepareMethod=" + pm + ";";
            try (Connection con = DriverManager.getConnection(url);
                    Statement stmt = con.createStatement()) {
                connects++;
                for (int q = 0; q < 3; q++) {
                    try (ResultSet rs = stmt.executeQuery(fastQuery)) {
                        while (rs.next()) { rs.getString(1); queries++; }
                    }
                }
                // ~70% medium, ~25% slow
                int r = rnd.nextInt(100);
                String mixed = r < 70 ? mediumQuery : (r < 95 ? slowQuery : fastQuery);
                try (ResultSet rs = stmt.executeQuery(mixed)) {
                    while (rs.next()) { rs.getString(1); queries++; }
                }

                // PreparedStatement workload: re-use one PreparedStatement across
                // several executions (so the prepared-plan cache kicks in after the
                // driver's prepareThreshold, exercising sp_prepexec then sp_execute),
                // and create a one-shot PreparedStatement for the second query so
                // sp_prepare/sp_unprepare paths get hit too.
                try (PreparedStatement ps = con.prepareStatement(prepLookup)) {
                    for (int k = 0; k < 4; k++) {
                        ps.setString(1, objectTypes[(i + k) % objectTypes.length]);
                        ps.setString(2, "%" + (char) ('a' + ((i + k) % 26)) + "%");
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) { rs.getString(1); }
                        }
                        prepared++;
                    }
                }
                try (PreparedStatement ps = con.prepareStatement(prepCount)) {
                    ps.setInt(1, 56 + (i % 4));
                    try (ResultSet rs = ps.executeQuery()) {
                        if (rs.next()) { rs.getInt(1); }
                    }
                    prepared++;
                }

                if (i % 4 == 0) {
                    try {
                        stmt.executeQuery("SELECT * FROM dbo.__missing_table_otel_poc__").close();
                    } catch (SQLException expected) {
                        errors++;
                    }
                }
            }
            i++;
            if (i % 10 == 0) {
                System.out.printf("  [iter %5d] prepareMethod=%s connects=%d queries=%d prepared=%d errors=%d  (elapsed %ds)%n",
                        i, pm, connects, queries, prepared, errors,
                        (System.currentTimeMillis() - t0) / 1000);
            }
            Thread.sleep(sleepMs);
        }

        System.out.println();
        System.out.println("Waiting one export cycle (" + (intervalSec + 2) + "s) for the final flush...");
        Thread.sleep((intervalSec + 2) * 1000L);
        System.out.println("Done. Open http://localhost:3000 -> Dashboards -> mssql-jdbc.");
    }
}
