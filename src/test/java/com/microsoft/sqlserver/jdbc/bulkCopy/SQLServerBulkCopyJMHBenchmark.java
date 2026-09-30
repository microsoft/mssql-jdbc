/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;

/**
 * Publication-Grade JMH (Java Microbenchmark Harness) Benchmark.
 * Measures:
 * 1. Throughput (operations / second)
 * 2. Heap Allocation Rate via JMH GCProfiler (B/op and MB/sec)
 * 3. Churn differences between Standard On-Heap ByteBuffer and MemorySegment Off-Heap
 */
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 2, time = 3, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 4, time = 3, timeUnit = TimeUnit.SECONDS)
@Fork(value = 1, jvmArgs = {"-Xms2g", "-Xmx2g", "-XX:+UseG1GC"})
public class SQLServerBulkCopyJMHBenchmark {

    private static final String TABLE_NAME = "jmh_bulk_benchmark";
    private static final int ROWS_PER_OPERATION = 50_000;
    private static final int BATCH_SIZE = 25_000;

    @Param({"false", "true"})
    public boolean useMemorySegment;

    private String jdbcUrl;
    private String user;
    private String password;
    private Connection connection;

    @Setup(Level.Trial)
    public void setupTrial() throws Exception {
        jdbcUrl = System.getProperty("jmh.jdbcUrl",
                "jdbc:sqlserver://localhost:1433;databaseName=master;encrypt=false;trustServerCertificate=true;");
        user = System.getProperty("jmh.user",
                System.getenv().getOrDefault("MSSQL_TEST_USER", "sa"));
        password = System.getProperty("jmh.password",
                System.getenv().getOrDefault("MSSQL_TEST_PASS", "Local_356f69c891874a78425f82a2_Pw1!"));

        try (Connection con = DriverManager.getConnection(jdbcUrl, user, password);
             Statement stmt = con.createStatement()) {
            stmt.execute("IF OBJECT_ID('" + TABLE_NAME + "', 'U') IS NOT NULL DROP TABLE " + TABLE_NAME);
            stmt.execute("CREATE TABLE " + TABLE_NAME + " ("
                    + "id INT NOT NULL, "
                    + "big_val BIGINT NOT NULL, "
                    + "amount DECIMAL(18, 4) NOT NULL, "
                    + "small_id SMALLINT NOT NULL"
                    + ")");
        }

        connection = DriverManager.getConnection(jdbcUrl, user, password);
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws Exception {
        if (connection != null && !connection.isClosed()) {
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("IF OBJECT_ID('" + TABLE_NAME + "', 'U') IS NOT NULL DROP TABLE " + TABLE_NAME);
            }
            connection.close();
        }
    }

    @Setup(Level.Invocation)
    public void setupInvocation() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("TRUNCATE TABLE " + TABLE_NAME);
        }
    }

    @Benchmark
    public void benchmarkBulkInsert(Blackhole bh) throws Exception {
        JMHBulkRowSource source = new JMHBulkRowSource(ROWS_PER_OPERATION);
        try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection)) {
            SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
            options.setBatchSize(BATCH_SIZE);
            options.setBulkCopyTimeout(180);
            options.setUseMemorySegment(useMemorySegment);
            bulkCopy.setBulkCopyOptions(options);
            bulkCopy.setDestinationTableName(TABLE_NAME);
            bulkCopy.writeToServer(source);
        }
        bh.consume(source);
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(SQLServerBulkCopyJMHBenchmark.class.getSimpleName())
                .addProfiler(GCProfiler.class) // Third-party OpenJDK GC Profiler
                .build();

        new Runner(opt).run();
    }

    private static final class JMHBulkRowSource implements ISQLServerBulkData {
        private static final long serialVersionUID = 1L;
        private final int totalRows;
        private int currentRow = 0;
        private final BigDecimal sampleDecimal = new BigDecimal("88888.7777");

        JMHBulkRowSource(int totalRows) {
            this.totalRows = totalRows;
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            return new HashSet<>(Arrays.asList(1, 2, 3, 4));
        }

        @Override
        public String getColumnName(int column) {
            switch (column) {
                case 1: return "id";
                case 2: return "big_val";
                case 3: return "amount";
                case 4: return "small_id";
                default: return "";
            }
        }

        @Override
        public int getColumnType(int column) {
            switch (column) {
                case 1: return java.sql.Types.INTEGER;
                case 2: return java.sql.Types.BIGINT;
                case 3: return java.sql.Types.DECIMAL;
                case 4: return java.sql.Types.SMALLINT;
                default: return java.sql.Types.VARCHAR;
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
            return new Object[] {
                currentRow,
                9000000000L + currentRow,
                sampleDecimal,
                (short) (currentRow % 500)
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
