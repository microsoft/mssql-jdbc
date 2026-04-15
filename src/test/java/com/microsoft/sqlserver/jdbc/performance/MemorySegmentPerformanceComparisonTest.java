package com.microsoft.sqlserver.jdbc.performance;

import com.microsoft.sqlserver.testframework.AbstractTest;
import org.junit.jupiter.api.Test;

import java.sql.*;

/**
 * MemorySegment vs ByteBuffer Performance Comparison Test
 * 
 * Compares Traditional ByteBuffer allocation against Java 22+ MemorySegment API
 * for JDBC buffer I/O operations using real database workloads.
 * 
 * CRITICAL JDBC APIs TESTED:
 * =========================================================================
 * 1. Statement.executeQuery()           - Normal SQL execution (1K, 10K, 100K rows)
 * 2. PreparedStatement.executeBatch()   - Batch operations (1K, 10K, 100K rows)
 * 3. ResultSet.next()/getString()       - Result set iteration (1K, 10K, 100K rows)
 * 4. Large ResultSet retrieval          - High-volume data transfer (10K+ rows)
 * 5. Batch INSERT operations            - Bulk insert performance (1K, 10K, 100K rows)
 * 6. BLOB/CLOB streaming                - Large object streaming
 * 
 * WHY THESE APIS:
 * - All involve buffer I/O between heap and native memory (TDS protocol)
 * - Traditional: 2x copy (heap → native → network)
 * - MemorySegment: Direct native memory → network (zero-copy)
 * 
 * RUN:
 * mvn test -Dtest=MemorySegmentPerformanceComparisonTest
 * mvn test -Dtest=MemorySegmentPerformanceComparisonTest -Dmssql.jdbc.useMemorySegment=true
 */
public class MemorySegmentPerformanceComparisonTest extends AbstractTest {
    
    // Test dataset sizes
    private static final int SMALL_DATASET = 1000;      // 1K rows
    private static final int LARGE_DATASET = 10000;     // 10K rows
    private static final int VERY_LARGE_DATASET = 100000;  // 100K rows
    
    // Data sizes for BLOB/CLOB tests
    private static final int CLOB_SIZE = 100000;     // 100KB text
    private static final int BLOB_SIZE = 500000;     // 500KB binary
    
    @Test
    public void testMemorySegmentVsByteBufferPerformance() throws SQLException {
        printTestHeader();
        
        String useMemorySegmentProp = System.getProperty("mssql.jdbc.useMemorySegment", "false");
        String mode = Boolean.parseBoolean(useMemorySegmentProp) ? "MemorySegment" : "Traditional";
        
        System.out.println("[INFO] Running in " + mode + " mode...\n");
        
        PerformanceMetrics metrics = runAllTests(mode + " Mode");
        
        printSingleModeResults(mode, metrics);
    }
    
    private void printTestHeader() {
        System.out.println("\n" + "=".repeat(100));
        System.out.println("MSSQL JDBC DRIVER - COMPREHENSIVE PERFORMANCE TEST");
        System.out.println("Traditional ByteBuffer vs MemorySegment API (JEP 454)");
        System.out.println("100% REAL DATABASE OPERATIONS - NOT SIMULATED OR THEORETICAL");
        System.out.println("=".repeat(100));
        System.out.println("\nCRITICAL JDBC APIs BEING TESTED:");
        System.out.println("   1. Statement.executeQuery()         - Normal SQL execution");
        System.out.println("   2. PreparedStatement.executeBatch() - Batch operations");
        System.out.println("   3. ResultSet iteration              - Result set processing");
        System.out.println("   4. Large ResultSet retrieval        - High-volume data transfer");
        System.out.println("   5. Batch INSERT operations          - Bulk insert performance");
        System.out.println("   6. BLOB/CLOB streaming              - Large object operations");
        System.out.println("\nTest Datasets (REAL SQL INSERT/SELECT operations):");
        System.out.println("   - Small:      " + SMALL_DATASET + " rows (1K)");
        System.out.println("   - Large:      " + LARGE_DATASET + " rows (10K)");
        System.out.println("   - Very Large: " + VERY_LARGE_DATASET + " rows (100K) SCALABILITY TEST");
        System.out.println("   - CLOB: " + CLOB_SIZE + " bytes, BLOB: " + BLOB_SIZE + " bytes");
        System.out.println("\n[INFO] Every millisecond measured = Actual network I/O + TDS protocol + Database round-trip");
        System.out.println("=".repeat(100));
    }
    
    private PerformanceMetrics runAllTests(String modeName) throws SQLException {
        System.out.println("\n" + "=".repeat(100));
        System.out.println("[TEST] " + modeName);
        System.out.println("=".repeat(100));
        
        PerformanceMetrics metrics = new PerformanceMetrics();
        
        try (Connection conn = getConnection()) {
            setupTables(conn);
            
            // Test 1: Normal Statement (1K)
            System.out.println("\n[Test 1] Normal Statement - 1K rows");
            metrics.normalStatement1K = testNormalStatement(conn, SMALL_DATASET);
            
            // Test 2: Normal Statement (10K)
            System.out.println("\n[Test 2] Normal Statement - 10K rows");
            metrics.normalStatement10K = testNormalStatement(conn, LARGE_DATASET);
            
            // Test 3: PreparedStatement Batch (1K)
            System.out.println("\n[Test 3] PreparedStatement.executeBatch()  - 1K rows");
            metrics.batchInsert1K = testBatchInsert(conn, SMALL_DATASET);
            
            // Test 4: PreparedStatement Batch (10K)
            System.out.println("\n[Test 4] PreparedStatement.executeBatch() - 10K rows");
            metrics.batchInsert10K = testBatchInsert(conn, LARGE_DATASET);
            
            // Test 5: ResultSet Iteration (1K)
            System.out.println("\n[Test 5] ResultSet iteration - 1K rows");
            metrics.resultSetIteration1K = testResultSetIteration(conn, SMALL_DATASET);
            
            // Test 6: ResultSet Iteration (10K)
            System.out.println("\n[Test 6] ResultSet iteration - 10K rows");
            metrics.resultSetIteration10K = testResultSetIteration(conn, LARGE_DATASET);
            
            // Test 7: Large ResultSet Retrieval (10K)
            System.out.println("\n[Test 7] Large ResultSet retrieval - 10K rows");
            metrics.largeResultSet10K = testLargeResultSet(conn, LARGE_DATASET);
            
            // Test 8: CLOB Streaming
            System.out.println("\n[Test 8] CLOB streaming operations");
            metrics.clobStreaming = testCLOBStreaming(conn);
            
            // Test 9: BLOB Streaming
            System.out.println("\n[Test 9] BLOB streaming operations");
            metrics.blobStreaming = testBLOBStreaming(conn);
            
            // Test 10: Normal Statement (100K) - Real scalability test
            System.out.println("\n[Test 10] Normal Statement - 100K rows (REAL DATABASE TEST)");
            metrics.normalStatement1M = testNormalStatement(conn, VERY_LARGE_DATASET);
            
            // Test 11: Batch Insert (100K) - Real bulk operation test  
            System.out.println("\n[Test 11] PreparedStatement.executeBatch() - 100K rows (REAL DATABASE TEST)");
            metrics.batchInsert1M = testBatchInsert(conn, VERY_LARGE_DATASET);
            
            // Test 12: ResultSet Iteration (100K) - Real large dataset processing
            System.out.println("\n[Test 12] ResultSet iteration - 100K rows (REAL DATABASE TEST)");
            metrics.resultSetIteration1M = testResultSetIteration(conn, VERY_LARGE_DATASET);
            
            cleanupTables(conn);
            System.out.println("\n[OK] All tests completed for: " + modeName);
        }
        
        return metrics;
    }
    
    private void setupTables(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_normal_stmt");
            stmt.execute("CREATE TABLE test_normal_stmt (id INT PRIMARY KEY, data VARCHAR(500))");
            
            stmt.execute("DROP TABLE IF EXISTS test_batch");
            stmt.execute("CREATE TABLE test_batch (id INT PRIMARY KEY, col1 VARCHAR(100), col2 INT, col3 VARCHAR(100))");
            
            stmt.execute("DROP TABLE IF EXISTS test_resultset");
            stmt.execute("CREATE TABLE test_resultset (id INT PRIMARY KEY, data VARCHAR(500), value INT)");
            
            stmt.execute("DROP TABLE IF EXISTS test_clob");
            stmt.execute("CREATE TABLE test_clob (id INT PRIMARY KEY, text_data NVARCHAR(MAX))");
            
            stmt.execute("DROP TABLE IF EXISTS test_blob");
            stmt.execute("CREATE TABLE test_blob (id INT PRIMARY KEY, binary_data VARBINARY(MAX))");
        }
    }
    
    private void cleanupTables(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS test_normal_stmt");
            stmt.execute("DROP TABLE IF EXISTS test_batch");
            stmt.execute("DROP TABLE IF EXISTS test_resultset");
            stmt.execute("DROP TABLE IF EXISTS test_clob");
            stmt.execute("DROP TABLE IF EXISTS test_blob");
        }
    }
    
    // Test 1 & 2: Normal Statement execution
    private double testNormalStatement(Connection conn, int rows) throws SQLException {
        // First populate data
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM test_normal_stmt");
            for (int i = 0; i < rows; i++) {
                stmt.execute("INSERT INTO test_normal_stmt (id, data) VALUES (" + i + ", 'Data row " + i + "')");
            }
        }
        
        // Measure SELECT with normal Statement
        long start = System.nanoTime();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM test_normal_stmt")) {
            int count = 0;
            while (rs.next()) {
                int id = rs.getInt("id");
                String data = rs.getString("data");
                count++;
            }
        }
        double elapsed = (System.nanoTime() - start) / 1_000_000.0;
        
        System.out.printf("   [OK] Executed and retrieved %d rows in %.2f ms (%.2f rows/sec)\n", 
            rows, elapsed, rows / (elapsed / 1000.0));
        
        return elapsed;
    }
    
    // Test 3 & 4: Batch INSERT operations
    private double testBatchInsert(Connection conn, int rows) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM test_batch");
        }
        
        long start = System.nanoTime();
        try (PreparedStatement pstmt = conn.prepareStatement(
            "INSERT INTO test_batch (id, col1, col2, col3) VALUES (?, ?, ?, ?)")) {
            
            for (int i = 0; i < rows; i++) {
                pstmt.setInt(1, i);
                pstmt.setString(2, "Batch data " + i);
                pstmt.setInt(3, i * 100);
                pstmt.setString(4, "Additional " + i);
                pstmt.addBatch();
                
                // Execute batch every 500 rows
                if ((i + 1) % 500 == 0) {
                    pstmt.executeBatch();
                }
            }
            // Execute remaining
            pstmt.executeBatch();
        }
        double elapsed = (System.nanoTime() - start) / 1_000_000.0;
        
        System.out.printf("   [OK] Batch inserted %d rows in %.2f ms (%.2f rows/sec)\n", 
            rows, elapsed, rows / (elapsed / 1000.0));
        
        return elapsed;
    }
    
    // Test 5 & 6: ResultSet iteration
    private double testResultSetIteration(Connection conn, int rows) throws SQLException {
        // First populate data
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM test_resultset");
        }
        
        try (PreparedStatement pstmt = conn.prepareStatement(
            "INSERT INTO test_resultset (id, data, value) VALUES (?, ?, ?)")) {
            for (int i = 0; i < rows; i++) {
                pstmt.setInt(1, i);
                pstmt.setString(2, "ResultSet row " + i + " with some test data content");
                pstmt.setInt(3, i * 10);
                pstmt.executeUpdate();
            }
        }
        
        // Measure ResultSet iteration
        long start = System.nanoTime();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM test_resultset")) {
            int count = 0;
            while (rs.next()) {
                int id = rs.getInt("id");
                String data = rs.getString("data");
                int value = rs.getInt("value");
                count++;
            }
        }
        double elapsed = (System.nanoTime() - start) / 1_000_000.0;
        
        System.out.printf("   [OK] Iterated through %d rows in %.2f ms (%.2f rows/sec)\n", 
            rows, elapsed, rows / (elapsed / 1000.0));
        
        return elapsed;
    }
    
    // Test 7: Large ResultSet retrieval
    private double testLargeResultSet(Connection conn, int rows) throws SQLException {
        // Populate large dataset
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM test_resultset");
        }
        
        try (PreparedStatement pstmt = conn.prepareStatement(
            "INSERT INTO test_resultset (id, data, value) VALUES (?, ?, ?)")) {
            for (int i = 0; i < rows; i++) {
                pstmt.setInt(1, i);
                pstmt.setString(2, "Large dataset row " + i + " with extended content for realistic testing");
                pstmt.setInt(3, i * 10);
                pstmt.addBatch();
                if ((i + 1) % 500 == 0) {
                    pstmt.executeBatch();
                }
            }
            pstmt.executeBatch();
        }
        
        // Measure large result set retrieval
        long start = System.nanoTime();
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM test_resultset ORDER BY id")) {
            int count = 0;
            long totalBytes = 0;
            while (rs.next()) {
                int id = rs.getInt("id");
                String data = rs.getString("data");
                int value = rs.getInt("value");
                totalBytes += data.length();
                count++;
            }
        }
        double elapsed = (System.nanoTime() - start) / 1_000_000.0;
        
        System.out.printf("   [OK] Retrieved %d rows in %.2f ms (%.2f rows/sec)\n", 
            rows, elapsed, rows / (elapsed / 1000.0));
        
        return elapsed;
    }
    
    // Test 8: CLOB Streaming
    private double testCLOBStreaming(Connection conn) throws SQLException {
        String largeText = generateLargeText(CLOB_SIZE);
        
        // Test CLOB write
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM test_clob");
        }
        
        long start = System.nanoTime();
        try (PreparedStatement pstmt = conn.prepareStatement(
            "INSERT INTO test_clob (id, text_data) VALUES (?, ?)")) {
            for (int i = 0; i < 10; i++) {
                pstmt.setInt(1, i);
                pstmt.setString(2, largeText);
                pstmt.executeUpdate();
            }
        }
        
        // Test CLOB read
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM test_clob")) {
            while (rs.next()) {
                int id = rs.getInt("id");
                String text = rs.getString("text_data");
            }
        }
        double elapsed = (System.nanoTime() - start) / 1_000_000.0;
        
        System.out.printf("   [OK] CLOB operations (10 x %d bytes) completed in %.2f ms\n", 
            CLOB_SIZE, elapsed);
        
        return elapsed;
    }
    
    // Test 9: BLOB Streaming
    private double testBLOBStreaming(Connection conn) throws SQLException {
        byte[] largeBlob = generateLargeBlob(BLOB_SIZE);
        
        // Test BLOB write
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM test_blob");
        }
        
        long start = System.nanoTime();
        try (PreparedStatement pstmt = conn.prepareStatement(
            "INSERT INTO test_blob (id, binary_data) VALUES (?, ?)")) {
            for (int i = 0; i < 10; i++) {
                pstmt.setInt(1, i);
                pstmt.setBytes(2, largeBlob);
                pstmt.executeUpdate();
            }
        }
        
        // Test BLOB read
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM test_blob")) {
            while (rs.next()) {
                int id = rs.getInt("id");
                byte[] blob = rs.getBytes("binary_data");
            }
        }
        double elapsed = (System.nanoTime() - start) / 1_000_000.0;
        
        System.out.printf("   [OK] BLOB operations (10 x %d bytes) completed in %.2f ms\n", 
            BLOB_SIZE, elapsed);
        
        return elapsed;
    }
    
    private void printSingleModeResults(String mode, PerformanceMetrics m) {
        System.out.printf("\nResults for %s Mode:\n", mode);
        System.out.printf("  Normal Statement (1K):      %10.2f ms\n", m.normalStatement1K);
        System.out.printf("  Normal Statement (10K):     %10.2f ms\n", m.normalStatement10K);
        System.out.printf("  Normal Statement (100K):    %10.2f ms\n", m.normalStatement1M);
        System.out.printf("  Batch Insert (1K):          %10.2f ms\n", m.batchInsert1K);
        System.out.printf("  Batch Insert (10K):         %10.2f ms\n", m.batchInsert10K);
        System.out.printf("  Batch Insert (100K):        %10.2f ms\n", m.batchInsert1M);
        System.out.printf("  ResultSet Iteration (1K):   %10.2f ms\n", m.resultSetIteration1K);
        System.out.printf("  ResultSet Iteration (10K):  %10.2f ms\n", m.resultSetIteration10K);
        System.out.printf("  ResultSet Iteration (100K): %10.2f ms\n", m.resultSetIteration1M);
        System.out.printf("  Large ResultSet (10K):      %10.2f ms\n", m.largeResultSet10K);
        System.out.printf("  CLOB Streaming:             %10.2f ms\n", m.clobStreaming);
        System.out.printf("  BLOB Streaming:             %10.2f ms\n", m.blobStreaming);
        System.out.printf("  TOTAL:                      %10.2f ms\n", m.getTotalTime());
    }
    
    // Helper methods
    private String generateLargeText(int size) {
        StringBuilder sb = new StringBuilder(size);
        String pattern = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789 ";
        for (int i = 0; i < size; i++) {
            sb.append(pattern.charAt(i % pattern.length()));
        }
        return sb.toString();
    }
    
    private byte[] generateLargeBlob(int size) {
        byte[] data = new byte[size];
        for (int i = 0; i < size; i++) {
            data[i] = (byte)(i % 256);
        }
        return data;
    }
    
    // Performance metrics holder
    private static class PerformanceMetrics {
        double normalStatement1K;
        double normalStatement10K;
        double normalStatement1M;
        double batchInsert1K;
        double batchInsert10K;
        double batchInsert1M;
        double resultSetIteration1K;
        double resultSetIteration10K;
        double resultSetIteration1M;
        double largeResultSet10K;
        double clobStreaming;
        double blobStreaming;
        
        double getTotalTime() {
            return normalStatement1K + normalStatement10K + normalStatement1M +
                   batchInsert1K + batchInsert10K + batchInsert1M +
                   resultSetIteration1K + resultSetIteration10K + resultSetIteration1M +
                   largeResultSet10K + clobStreaming + blobStreaming;
        }
    }
}
