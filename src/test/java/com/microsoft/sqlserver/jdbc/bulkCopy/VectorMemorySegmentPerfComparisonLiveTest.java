/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;

import microsoft.sql.Vector;
import microsoft.sql.Vector.VectorDimensionType;

/**
 * Performance comparison between Standard On-Heap ByteBuffer and MemorySegment Off-Heap
 * for Vector (FLOAT32 embeddings) bulk ingestion against live Azure SQL DB.
 */
public class VectorMemorySegmentPerfComparisonLiveTest extends AbstractTest {

    private static final String TABLE_NAME = "vector_perf_comparison";
    private static final int VECTOR_DIMS = 128;
    private static final int TOTAL_ROWS = 20_000;
    private static final int BATCH_SIZE = 5_000;
    private static final int ITERATIONS = 3;

    private static String vectorConnectionString;

    @BeforeAll
    public static void setupTable() throws Exception {
        vectorConnectionString = getConnectionString() + ";vectorTypeSupport=v1;";
        try (Connection con = DriverManager.getConnection(vectorConnectionString);
                Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            try {
                stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT NOT NULL, embedding VECTOR(" + VECTOR_DIMS + ") NOT NULL)");
            } catch (Exception e) {
                Assumptions.assumeTrue(false, "Server does not support VECTOR: " + e.getMessage());
            }
        }
    }

    @AfterAll
    public static void teardownTable() throws Exception {
        try (Connection con = DriverManager.getConnection(vectorConnectionString);
                Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
        }
    }

    private static long totalGcTime() {
        long total = 0;
        for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
            long time = gcBean.getCollectionTime();
            if (time > 0) {
                total += time;
            }
        }
        return total;
    }

    @Test
    @DisplayName("Performance Comparison: Vector(128) Standard On-Heap vs MemorySegment Off-Heap")
    public void testVectorPerfComparison() throws Exception {
        System.out.println("==========================================================================");
        System.out.println("   VECTOR(128) LIVE BENCHMARK: " + TOTAL_ROWS + " rows x " + ITERATIONS + " iterations");
        System.out.println("==========================================================================");

        List<Long> standardTimes = new ArrayList<>();
        List<Long> memSegTimes = new ArrayList<>();
        List<Long> standardGcs = new ArrayList<>();
        List<Long> memSegGcs = new ArrayList<>();

        for (int i = 1; i <= ITERATIONS; i++) {
            System.out.printf("%n>>> Iteration %d of %d...%n", i, ITERATIONS);

            // --- STANDARD ON-HEAP ---
            try (Connection con = DriverManager.getConnection(vectorConnectionString);
                    Statement stmt = con.createStatement()) {
                stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
                System.gc();
                Thread.sleep(150);

                long gcBefore = totalGcTime();
                long start = System.currentTimeMillis();

                runVectorBulkCopy(con, false, TOTAL_ROWS);

                long elapsed = System.currentTimeMillis() - start;
                long gcTime = totalGcTime() - gcBefore;
                standardTimes.add(elapsed);
                standardGcs.add(gcTime);

                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                    rs.next();
                    assertEquals(TOTAL_ROWS, rs.getInt(1));
                }

                System.out.printf("  [Iter %d] Standard On-Heap : %,d ms (GC Time: %d ms) -> %,.0f vectors/sec%n",
                        i, elapsed, gcTime, (TOTAL_ROWS / (elapsed / 1000.0)));
            }

            // --- MEMORYSEGMENT OFF-HEAP ---
            try (Connection con = DriverManager.getConnection(vectorConnectionString);
                    Statement stmt = con.createStatement()) {
                stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
                System.gc();
                Thread.sleep(150);

                long gcBefore = totalGcTime();
                long start = System.currentTimeMillis();

                runVectorBulkCopy(con, true, TOTAL_ROWS);

                long elapsed = System.currentTimeMillis() - start;
                long gcTime = totalGcTime() - gcBefore;
                memSegTimes.add(elapsed);
                memSegGcs.add(gcTime);

                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                    rs.next();
                    assertEquals(TOTAL_ROWS, rs.getInt(1));
                }

                System.out.printf("  [Iter %d] MemorySegment    : %,d ms (GC Time: %d ms) -> %,.0f vectors/sec%n",
                        i, elapsed, gcTime, (TOTAL_ROWS / (elapsed / 1000.0)));
            }
        }

        double avgStandard = standardTimes.stream().mapToLong(Long::longValue).average().orElse(0);
        double avgMemSeg = memSegTimes.stream().mapToLong(Long::longValue).average().orElse(0);
        double avgStandardGc = standardGcs.stream().mapToLong(Long::longValue).average().orElse(0);
        double avgMemSegGc = memSegGcs.stream().mapToLong(Long::longValue).average().orElse(0);

        double standardRate = (TOTAL_ROWS / (avgStandard / 1000.0));
        double memSegRate = (TOTAL_ROWS / (avgMemSeg / 1000.0));
        double speedup = ((avgStandard - avgMemSeg) / avgStandard) * 100.0;
        double throughputGain = ((memSegRate - standardRate) / standardRate) * 100.0;

        System.out.println("%n==========================================================================");
        System.out.println("               VECTOR(128) BENCHMARK FINAL SUMMARY");
        System.out.println("==========================================================================");
        System.out.printf("  Standard On-Heap Avg : %,.0f ms | Rate: %,.0f vectors/sec | GC Time: %,.1f ms%n",
                avgStandard, standardRate, avgStandardGc);
        System.out.printf("  MemorySegment    Avg : %,.0f ms | Rate: %,.0f vectors/sec | GC Time: %,.1f ms%n",
                avgMemSeg, memSegRate, avgMemSegGc);
        System.out.printf("  Speedup Improvement  : %.2f%% faster%n", speedup);
        System.out.printf("  Throughput Gain      : +%.2f%% more vectors/sec%n", throughputGain);
        System.out.println("==========================================================================");

        File reportFile = new File("target/vector-perf-comparison-summary.md");
        reportFile.getParentFile().mkdirs();
        try (PrintWriter pw = new PrintWriter(new FileWriter(reportFile))) {
            pw.println("# Vector(128) BulkCopy Performance: Standard On-Heap vs MemorySegment Off-Heap");
            pw.println();
            pw.println("- **Target Database**: Azure SQL DB (`test-divang-driver`)");
            pw.println("- **Vector Dimensions**: " + VECTOR_DIMS + " (FLOAT32)");
            pw.println("- **Total Rows per Iteration**: " + TOTAL_ROWS);
            pw.println("- **Batch Size**: " + BATCH_SIZE);
            pw.println("- **Iterations**: " + ITERATIONS);
            pw.println();
            pw.println("| Metric | Standard On-Heap ByteBuffer | MemorySegment Off-Heap | Delta |");
            pw.println("|---|---|---|---|");
            pw.printf("| **Average Duration** | %,.0f ms | %,.0f ms | **%.2f%% faster** |%n", avgStandard, avgMemSeg, speedup);
            pw.printf("| **Throughput** | %,.0f vectors/sec | %,.0f vectors/sec | **+%.2f%%** |%n", standardRate, memSegRate, throughputGain);
            pw.printf("| **Average GC Time** | %,.1f ms | %,.1f ms | - |%n", avgStandardGc, avgMemSegGc);
        }
    }

    private static void runVectorBulkCopy(Connection con, boolean useMemorySegment, int rows) throws Exception {
        VectorPerfRowSource source = new VectorPerfRowSource(rows, VECTOR_DIMS);
        try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
            SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
            options.setBatchSize(BATCH_SIZE);
            options.setBulkCopyTimeout(180);
            options.setUseMemorySegment(useMemorySegment);
            bulkCopy.setBulkCopyOptions(options);
            bulkCopy.setDestinationTableName(TABLE_NAME);
            bulkCopy.writeToServer(source);
        }
    }

    private static final class VectorPerfRowSource implements ISQLServerBulkData {
        private static final long serialVersionUID = 1L;
        private final int totalRows;
        private final int dims;
        private int currentRow = -1;

        VectorPerfRowSource(int totalRows, int dims) {
            this.totalRows = totalRows;
            this.dims = dims;
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            return new HashSet<>(Arrays.asList(1, 2));
        }

        @Override
        public String getColumnName(int column) {
            return column == 1 ? "id" : "embedding";
        }

        @Override
        public int getColumnType(int column) {
            return column == 1 ? java.sql.Types.INTEGER : microsoft.sql.Types.VECTOR;
        }

        @Override
        public int getPrecision(int column) {
            return column == 2 ? dims : 0;
        }

        @Override
        public int getScale(int column) {
            return 0;
        }

        @Override
        public Object[] getRowData() {
            Float[] values = new Float[dims];
            for (int d = 0; d < dims; d++) {
                values[d] = (float) (currentRow + d * 0.1f);
            }
            Vector vector = new Vector(dims, VectorDimensionType.FLOAT32, values);
            return new Object[] { currentRow, vector };
        }

        @Override
        public boolean next() {
            if (currentRow + 1 < totalRows) {
                currentRow++;
                return true;
            }
            return false;
        }
    }
}
