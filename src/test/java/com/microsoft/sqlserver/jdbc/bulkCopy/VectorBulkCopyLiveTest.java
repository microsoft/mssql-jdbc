/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
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
 * Validates Vector (FLOAT32 embeddings) data type bulk ingest into actual Azure SQL DB.
 * Measures performance and GC pause times across Standard On-Heap vs MemorySegment Off-Heap.
 */
public class VectorBulkCopyLiveTest extends AbstractTest {

    private static final String TABLE_NAME = "perf_vector_embeddings";
    private static final int VECTOR_DIMS = 128; // 128-dimensional embedding
    private static final int TOTAL_ROWS = 10000;
    private static final int BATCH_SIZE = 5000;

    @BeforeAll
    public static void setupTable() throws Exception {
        try (Connection con = getConnection();
             Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            String createSql = "CREATE TABLE " + TABLE_NAME + " ("
                    + "id INT NOT NULL, "
                    + "embedding VECTOR(" + VECTOR_DIMS + ") NOT NULL"
                    + ")";
            stmt.execute(createSql);
        }
    }

    @AfterAll
    public static void teardownTable() throws Exception {
        try (Connection con = getConnection();
             Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
        }
    }

    private static long getGcTime() {
        long total = 0;
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            total += gc.getCollectionTime();
        }
        return total;
    }

    @Test
    @DisplayName("BulkCopy Vector(128) embeddings into actual Azure SQL DB")
    public void testVectorBulkCopy() throws Exception {
        System.out.println("==========================================================================");
        System.out.println("   VECTOR(128) BULK INGESTION BENCHMARK (" + TOTAL_ROWS + " rows into Azure SQL DB)");
        System.out.println("==========================================================================");

        // Pre-create embedding templates to avoid benchmark GC noise from Random
        Float[][] embeddingPool = new Float[50][VECTOR_DIMS];
        Random rng = new Random(12345);
        for (int i = 0; i < 50; i++) {
            for (int d = 0; d < VECTOR_DIMS; d++) {
                embeddingPool[i][d] = rng.nextFloat();
            }
        }

        long standardElapsed;
        long standardGcTime;
        long memSegElapsed;
        long memSegGcTime;

        // --- RUN 1: Standard On-Heap ByteBuffer ---
        try (Connection con = getConnection();
             Statement stmt = con.createStatement()) {
            stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
            System.gc();
            Thread.sleep(150);

            long gcBefore = getGcTime();
            long start = System.currentTimeMillis();

            VectorBulkDataRecord record = new VectorBulkDataRecord(TOTAL_ROWS, VECTOR_DIMS, embeddingPool);
            try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setBatchSize(BATCH_SIZE);
                options.setBulkCopyTimeout(180);
                options.setUseMemorySegment(false);
                bulkCopy.setBulkCopyOptions(options);
                bulkCopy.setDestinationTableName(TABLE_NAME);
                bulkCopy.writeToServer(record);
            }

            standardElapsed = System.currentTimeMillis() - start;
            standardGcTime = getGcTime() - gcBefore;

            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(TOTAL_ROWS, rs.getInt(1));
            }
        }

        // --- RUN 2: MemorySegment Off-Heap Staging ---
        try (Connection con = getConnection();
             Statement stmt = con.createStatement()) {
            stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
            System.gc();
            Thread.sleep(150);

            long gcBefore = getGcTime();
            long start = System.currentTimeMillis();

            VectorBulkDataRecord record = new VectorBulkDataRecord(TOTAL_ROWS, VECTOR_DIMS, embeddingPool);
            try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
                SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
                options.setBatchSize(BATCH_SIZE);
                options.setBulkCopyTimeout(180);
                options.setUseMemorySegment(true);
                bulkCopy.setBulkCopyOptions(options);
                bulkCopy.setDestinationTableName(TABLE_NAME);
                bulkCopy.writeToServer(record);
            }

            memSegElapsed = System.currentTimeMillis() - start;
            memSegGcTime = getGcTime() - gcBefore;

            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(TOTAL_ROWS, rs.getInt(1));
            }
        }

        // Verify Vector value retrieval from Azure SQL DB
        try (Connection con = getConnection();
             Statement stmt = con.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT TOP 1 id, embedding FROM " + TABLE_NAME + " ORDER BY id ASC")) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
            Object vectorObj = rs.getObject(2);
            assertNotNull(vectorObj);
            System.out.println("  Verified Vector retrieved from DB: " + vectorObj.getClass().getSimpleName() + " -> " + vectorObj);
        }

        double standardThroughput = (TOTAL_ROWS / (standardElapsed / 1000.0));
        double memSegThroughput = (TOTAL_ROWS / (memSegElapsed / 1000.0));
        double speedup = ((double) (standardElapsed - memSegElapsed) / standardElapsed) * 100.0;
        double throughputGain = ((memSegThroughput - standardThroughput) / standardThroughput) * 100.0;

        System.out.println("\n==========================================================================");
        System.out.println("               VECTOR(128) LIVE BENCHMARK SUMMARY");
        System.out.println("==========================================================================");
        System.out.printf("  Standard On-Heap : %,d ms | Throughput: %,.0f vectors/sec | GC Time: %d ms%n",
                standardElapsed, standardThroughput, standardGcTime);
        System.out.printf("  MemorySegment    : %,d ms | Throughput: %,.0f vectors/sec | GC Time: %d ms%n",
                memSegElapsed, memSegThroughput, memSegGcTime);
        System.out.printf("  Speedup          : %.2f%% faster%n", speedup);
        System.out.printf("  Throughput Gain  : +%.2f%% more vectors/sec%n", throughputGain);
        System.out.println("==========================================================================");

        File reportFile = new File("target/vector-benchmark.md");
        reportFile.getParentFile().mkdirs();
        try (PrintWriter pw = new PrintWriter(new FileWriter(reportFile))) {
            pw.println("# Vector Bulk Ingestion Benchmark: Azure SQL DB");
            pw.println();
            pw.println("- **Database**: Azure SQL DB (`test-divang-driver`)");
            pw.println("- **Vector Dimensions**: " + VECTOR_DIMS + " (FLOAT32)");
            pw.println("- **Rows Inserted**: " + TOTAL_ROWS);
            pw.println("- **Batch Size**: " + BATCH_SIZE);
            pw.println();
            pw.println("| Metric | Standard On-Heap ByteBuffer | MemorySegment Off-Heap | Delta |");
            pw.println("|---|---|---|---|");
            pw.printf("| **Total Time** | %,d ms | %,d ms | **%.2f%% faster** |%n", standardElapsed, memSegElapsed, speedup);
            pw.printf("| **Throughput** | %,.0f vectors/sec | %,.0f vectors/sec | **+%.2f%%** |%n", standardThroughput, memSegThroughput, throughputGain);
            pw.printf("| **GC Time** | %d ms | %d ms | - |%n", standardGcTime, memSegGcTime);
        }
    }

    private static class VectorBulkDataRecord implements ISQLServerBulkData {
        private static final long serialVersionUID = 1L;
        private final int totalRows;
        private final int dimensions;
        private final Float[][] pool;
        private int currentRow = 0;

        VectorBulkDataRecord(int totalRows, int dimensions, Float[][] pool) {
            this.totalRows = totalRows;
            this.dimensions = dimensions;
            this.pool = pool;
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            return Set.of(1, 2);
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
            return column == 2 ? dimensions : 0;
        }

        @Override
        public int getScale(int column) {
            return 0; // FLOAT32 scale byte = 0
        }

        @Override
        public Object[] getRowData() {
            Float[] vectorData = pool[currentRow % pool.length];
            Vector vector = new Vector(dimensions, VectorDimensionType.FLOAT32, vectorData);
            return new Object[] {
                currentRow,
                vector
            };
        }

        @Override
        public boolean next() {
            if (currentRow < totalRows) {
                currentRow++;
                return true;
            }
            return false;
        }
    }
}
