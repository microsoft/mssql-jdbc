/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;

/**
 * Manual, isolated-process benchmark comparing standard on-heap {@code ByteBuffer} staging vs.
 * {@code MemorySegment} off-heap staging for {@link SQLServerBulkCopy}.
 *
 * <p>
 * <b>This is NOT a JUnit test.</b> It is a plain {@code main()} entry point, intentionally, so each variant can be
 * run as a completely separate JVM process. Alternating both variants within a single JVM process (e.g. in one
 * JUnit test method) was found to produce misleading results (tens of percent "faster", not reproducible), most
 * likely caused by JIT recompilation/deoptimization interference when a shared code path
 * ({@code TDSWriter.flush()} / {@code enableMemorySegment()}) is exercised with flipping behavior in one process.
 * Running each variant in its own process eliminates that cross-contamination.
 *
 * <h2>How to run</h2>
 * Run this class twice, as two separate Run Configurations (or two separate {@code java} invocations), never
 * together in the same JVM:
 *
 * <pre>
 * Program arguments (variant 1): jdbc:sqlserver://localhost:1433;databaseName=master;encrypt=false false 4
 * Program arguments (variant 2): jdbc:sqlserver://localhost:1433;databaseName=master;encrypt=false true 4
 * </pre>
 *
 * Arguments: {@code <jdbcUrl> <useMemorySegment: true|false> [iterations, default 4]}
 *
 * <p>
 * Set connection credentials via environment variables (never hardcode them):
 * <ul>
 * <li>{@code MSSQL_TEST_USER}</li>
 * <li>{@code MSSQL_TEST_PASS}</li>
 * </ul>
 *
 * <p>
 * Compare the two runs' printed {@code SUMMARY} lines (avg/min/max/avgGC) yourself; this class does not attempt to
 * merge results across processes.
 */
public class MemorySegmentIsolatedBenchmark {

    private static final String TABLE = "memseg_isolated_benchmark";
    private static final int ROW_COUNT = 1_000_000;
    private static final int WARMUP_ROW_COUNT = 100_000;
    private static final int BATCH_SIZE = 50_000;
    private static final int DEFAULT_ITERATIONS = 4;

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: MemorySegmentIsolatedBenchmark <jdbcUrl> <useMemorySegment:true|false> [iterations]");
            System.err.println("Env vars required: MSSQL_TEST_USER, MSSQL_TEST_PASS");
            System.exit(2);
        }

        String url = args[0];
        boolean useMemorySegment = Boolean.parseBoolean(args[1]);
        int iterations = args.length >= 3 ? Integer.parseInt(args[2]) : DEFAULT_ITERATIONS;

        String user = System.getenv("MSSQL_TEST_USER");
        String password = System.getenv("MSSQL_TEST_PASS");
        if (null == user || null == password) {
            System.err.println("Missing MSSQL_TEST_USER / MSSQL_TEST_PASS environment variables.");
            System.exit(2);
        }

        System.out.println("variant=" + (useMemorySegment ? "memorySegment" : "standardHeap")
                + " javaVersion=" + System.getProperty("java.version")
                + " iterations=" + iterations);

        try (Connection con = DriverManager.getConnection(url, user, password)) {
            createTable(con);

            // Warm-up: unmeasured, smaller scale, lets JIT/connection settle before timing starts.
            runBulkCopy(con, useMemorySegment, WARMUP_ROW_COUNT);
            runBulkCopy(con, useMemorySegment, WARMUP_ROW_COUNT);
            System.out.println("[warmup complete]");

            List<Long> elapsedMs = new ArrayList<>();
            List<Long> gcMs = new ArrayList<>();

            for (int i = 1; i <= iterations; i++) {
                System.gc();
                Thread.sleep(200);

                long gcBefore = totalGcTime();
                long start = System.currentTimeMillis();
                runBulkCopy(con, useMemorySegment, ROW_COUNT);
                long elapsed = System.currentTimeMillis() - start;
                long gcDelta = totalGcTime() - gcBefore;

                elapsedMs.add(elapsed);
                gcMs.add(gcDelta);
                System.out.printf("iter %d: %dms (gc=%dms)%n", i, elapsed, gcDelta);
            }

            printSummary(useMemorySegment, elapsedMs, gcMs);
            dropTable(con);
        }
    }

    private static void createTable(Connection con) throws SQLException {
        try (Statement stmt = con.createStatement()) {
            stmt.execute("IF OBJECT_ID('" + TABLE + "','U') IS NOT NULL DROP TABLE " + TABLE);
            stmt.execute("CREATE TABLE " + TABLE + " (id INT NOT NULL, big_val BIGINT NOT NULL, "
                    + "amount DECIMAL(18,4) NOT NULL, small_id SMALLINT NOT NULL)");
        }
    }

    private static void dropTable(Connection con) throws SQLException {
        try (Statement stmt = con.createStatement()) {
            stmt.execute("IF OBJECT_ID('" + TABLE + "','U') IS NOT NULL DROP TABLE " + TABLE);
        }
    }

    private static void runBulkCopy(Connection con, boolean useMemorySegment, int rowCount) throws SQLException {
        try (Statement stmt = con.createStatement()) {
            stmt.execute("TRUNCATE TABLE " + TABLE);
        }

        BulkRowSource source = new BulkRowSource(rowCount);
        try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(con)) {
            SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
            options.setBatchSize(BATCH_SIZE);
            options.setBulkCopyTimeout(300);
            options.setUseMemorySegment(useMemorySegment);
            bulkCopy.setBulkCopyOptions(options);
            bulkCopy.setDestinationTableName(TABLE);
            bulkCopy.writeToServer(source);
        }

        try (Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE)) {
            rs.next();
            int actual = rs.getInt(1);
            if (actual != rowCount) {
                throw new IllegalStateException("Row count mismatch: expected " + rowCount + " but found " + actual);
            }
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

    private static void printSummary(boolean useMemorySegment, List<Long> elapsedMs, List<Long> gcMs) {
        double avgElapsed = elapsedMs.stream().mapToLong(Long::longValue).average().orElse(0);
        double avgGc = gcMs.stream().mapToLong(Long::longValue).average().orElse(0);
        System.out.printf("SUMMARY variant=%s n=%d avg=%.1fms min=%dms max=%dms avgGC=%.1fms%n",
                useMemorySegment ? "memorySegment" : "standardHeap", elapsedMs.size(), avgElapsed,
                Collections.min(elapsedMs), Collections.max(elapsedMs), avgGc);
    }

    /**
     * Fixed 4-column row source (INT, BIGINT, DECIMAL(18,4), SMALLINT) generating {@code rowCount} rows.
     */
    private static final class BulkRowSource implements ISQLServerBulkData {
        private static final long serialVersionUID = 1L;
        private final int totalRows;
        private int currentRow = 0;

        BulkRowSource(int totalRows) {
            this.totalRows = totalRows;
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            return new HashSet<>(Arrays.asList(1, 2, 3, 4));
        }

        @Override
        public String getColumnName(int column) {
            switch (column) {
                case 1:
                    return "id";
                case 2:
                    return "big_val";
                case 3:
                    return "amount";
                case 4:
                    return "small_id";
                default:
                    return "";
            }
        }

        @Override
        public int getColumnType(int column) {
            switch (column) {
                case 1:
                    return java.sql.Types.INTEGER;
                case 2:
                    return java.sql.Types.BIGINT;
                case 3:
                    return java.sql.Types.DECIMAL;
                case 4:
                    return java.sql.Types.SMALLINT;
                default:
                    return java.sql.Types.VARCHAR;
            }
        }

        @Override
        public int getPrecision(int column) {
            return column == 3 ? 18 : 0;
        }

        @Override
        public int getScale(int column) {
            return column == 3 ? 4 : 0;
        }

        @Override
        public Object[] getRowData() {
            return new Object[] { currentRow, 2_000_000_000L + currentRow, new BigDecimal(currentRow + ".7500"),
                    (short) (currentRow % 500) };
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
