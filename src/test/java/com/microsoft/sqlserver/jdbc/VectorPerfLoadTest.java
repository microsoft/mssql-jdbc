/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

/**
 * Standard Application Developer test case for the SQL Server VECTOR data type.
 *
 * Uses ONLY the existing, standard public API:
 * - rs.getObject(col, Vector.class)
 * - vector.getData()
 * - new Vector(dims, type, Object[])
 *
 * Demonstrates the internal performance improvements of bulk vector decoding and
 * off-heap socket buffers without any changes to the public API.
 */
@Tag("VectorPerfLoad")
public class VectorPerfLoadTest {

    private static final int DEFAULT_ROWS = 2_500;
    private static final int DEFAULT_DIMS = 1_536; // OpenAI text-embedding-3-small dimension
    private static final int WARMUP_RUNS = 2;
    private static final int BENCHMARK_RUNS = 5;

    @Test
    @DisplayName("App Developer Test: Read Vector rows using standard public API (Vector.class)")
    public void testStandardVectorTableRead() throws Exception {
        String connStr = getConnectionString();
        assumeTrue(connStr != null && !connStr.trim().isEmpty(),
                "Set -DconnectionString=\"jdbc:sqlserver://...\" to run this test against SQL Server.");

        int rows = Integer.getInteger("mssql.jdbc.perf.rows", DEFAULT_ROWS);
        int dims = Integer.getInteger("mssql.jdbc.perf.dims", DEFAULT_DIMS);
        String bufferMode = System.getProperty("mssql.jdbc.bufferMode", "heap");

        String finalConnStr = connStr;
        if (!finalConnStr.toLowerCase().contains("vectortypesupport")) {
            finalConnStr += ";vectorTypeSupport=v1";
        }
        if (!finalConnStr.toLowerCase().contains("responsebuffering")) {
            finalConnStr += ";responseBuffering=adaptive";
        }

        double totalPayloadMiB = ((long) rows * (8 + dims * 4)) / (1024.0 * 1024.0);

        System.out.println("==================================================================================");
        System.out.println("     SQL SERVER VECTOR BENCHMARK (STANDARD PUBLIC API: Vector.class)              ");
        System.out.println("==================================================================================");
        System.out.printf(" Table: #VectorStandardBenchmark (id INT, v VECTOR(%d))%n", dims);
        System.out.printf(" Workload: %,d vectors x %,d dimensions (%.2f MiB total payload)%n", rows, dims, totalPayloadMiB);
        System.out.printf(" Driver Buffer Mode: %s%n", bufferMode.toUpperCase());
        System.out.printf(" JDK Version: %s (%s)%n", System.getProperty("java.version"), System.getProperty("java.vendor"));
        System.out.println("----------------------------------------------------------------------------------");

        try (Connection con = getConnectionWithRetry(finalConnStr)) {

            // Step 1: Create Table
            System.out.println("[Step 1] Creating temporary table with VECTOR column...");
            try (Statement stmt = con.createStatement()) {
                stmt.execute("IF OBJECT_ID('tempdb..#VectorStandardBenchmark') IS NOT NULL DROP TABLE #VectorStandardBenchmark;");
                stmt.execute("CREATE TABLE #VectorStandardBenchmark (id INT NOT NULL PRIMARY KEY, v VECTOR(" + dims + ") NOT NULL);");
            }

            // Step 2: Insert Vectors using standard public Vector constructor
            System.out.printf("[Step 2] Inserting %,d vector rows...%n", rows);
            Float[] sampleFloats = new Float[dims];
            for (int i = 0; i < dims; i++) {
                sampleFloats[i] = (float) (i * 0.001);
            }
            Vector vectorToInsert = new Vector(dims, VectorDimensionType.FLOAT32, sampleFloats);

            long insertStart = System.currentTimeMillis();
            con.setAutoCommit(false);
            try (PreparedStatement pstmt = con.prepareStatement(
                    "INSERT INTO #VectorStandardBenchmark (id, v) VALUES (?, ?)")) {
                for (int i = 0; i < rows; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setObject(2, vectorToInsert);
                    pstmt.addBatch();
                    if ((i + 1) % 500 == 0) {
                        pstmt.executeBatch();
                    }
                }
                pstmt.executeBatch();
            }
            con.commit();
            con.setAutoCommit(true);
            long insertElapsedMs = System.currentTimeMillis() - insertStart;
            System.out.printf(" -> Insert completed in %d ms (%.2f MiB/s)%n",
                    insertElapsedMs, totalPayloadMiB / (insertElapsedMs / 1000.0));

            // Step 3: Warmup Reads
            System.out.println("[Step 3] Running warmup queries...");
            for (int w = 0; w < WARMUP_RUNS; w++) {
                readVectors(con, rows, dims);
            }

            // Step 4: Timed Reads using standard public rs.getObject(col, Vector.class)
            System.out.printf("[Step 4] Reading %,d vector rows via rs.getObject(col, Vector.class) (%d runs)...%n",
                    rows, BENCHMARK_RUNS);
            long totalReadNanos = 0;
            long minReadNanos = Long.MAX_VALUE;
            for (int r = 0; r < BENCHMARK_RUNS; r++) {
                long start = System.nanoTime();
                int count = readVectors(con, rows, dims);
                long elapsed = System.nanoTime() - start;
                assertEquals(rows, count);
                totalReadNanos += elapsed;
                minReadNanos = Math.min(minReadNanos, elapsed);
                System.out.printf("    Run %d: %.2f ms (%.2f MiB/s)%n",
                        r + 1, elapsed / 1_000_000.0, totalPayloadMiB / (elapsed / 1_000_000_000.0));
            }
            double avgReadMs = (totalReadNanos / (double) BENCHMARK_RUNS) / 1_000_000.0;
            double minReadMs = minReadNanos / 1_000_000.0;
            double avgThroughput = totalPayloadMiB / (avgReadMs / 1000.0);
            double peakThroughput = totalPayloadMiB / (minReadMs / 1000.0);

            // Step 5: Summary Report
            System.out.println("\n==================================================================================");
            System.out.println("                 STANDARD VECTOR QUERY BENCHMARK RESULTS                          ");
            System.out.println("==================================================================================");
            System.out.printf(" Driver Buffer Mode:      %s%n", bufferMode.toUpperCase());
            System.out.printf(" Workload:                %,d vectors x %,d dims (%.2f MiB total)%n", rows, dims, totalPayloadMiB);
            System.out.printf(" Insert Elapsed Time:     %d ms (%.2f MiB/s)%n", insertElapsedMs, totalPayloadMiB / (insertElapsedMs / 1000.0));
            System.out.printf(" Average Read Latency:    %.2f ms%n", avgReadMs);
            System.out.printf(" Fastest Read Latency:    %.2f ms%n", minReadMs);
            System.out.printf(" Average Read Throughput: %.2f MiB/s%n", avgThroughput);
            System.out.printf(" Peak Read Throughput:    %.2f MiB/s%n", peakThroughput);
            System.out.println("==================================================================================");

            // Step 6: Cleanup Table
            try (Statement stmt = con.createStatement()) {
                stmt.execute("DROP TABLE #VectorStandardBenchmark;");
            }
            System.out.println("[Step 6] Test table #VectorStandardBenchmark dropped cleanly.");
        }
    }

    /**
     * Reads vectors from the table using only the existing, standard public API.
     */
    private int readVectors(Connection con, int expectedRows, int expectedDims) throws SQLException {
        int count = 0;
        String sql = "SELECT id, v FROM #VectorStandardBenchmark ORDER BY id";
        try (Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                int id = rs.getInt(1);
                // Standard public API:
                Vector vector = rs.getObject(2, Vector.class);
                assertNotNull(vector, "Vector should not be null");
                Object[] floats = vector.getData();
                assertNotNull(floats, "Vector data array should not be null");
                assertEquals(expectedDims, floats.length, "Dimension count mismatch");
                count++;
            }
        }
        return count;
    }

    private static String getConnectionString() {
        String connStr = System.getProperty("connectionString");
        if (connStr != null && !connStr.trim().isEmpty()) {
            return connStr;
        }
        connStr = System.getProperty("mssql.jdbc.test.connection.properties");
        if (connStr != null && !connStr.trim().isEmpty()) {
            return connStr;
        }
        return System.getenv("MSSQL_JDBC_TEST_CONNECTION_PROPERTIES");
    }

    private static Connection getConnectionWithRetry(String url) throws SQLException {
        SQLException lastException = null;
        for (int retry = 0; retry < 6; retry++) {
            try {
                return DriverManager.getConnection(url);
            } catch (SQLException e) {
                lastException = e;
                System.out.printf("Connection attempt %d failed (%s), retrying in 5s...%n",
                        retry + 1, e.getMessage());
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Connection retry interrupted", ie);
                }
            }
        }
        throw lastException;
    }
}
