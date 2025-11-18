package com.example;

import java.sql.*;
import java.math.BigDecimal;
import java.util.Properties;

/**
 * Sybase Connection and PreparedStatement Test Suite
 * Tests DYNAMIC_PREPARE=false behavior (equivalent to SQL Server prepareMethod=exec)
 */
public class SybaseConnection {
    
    // jTDS driver URL format for Sybase - using master database
    private static final String URL = "jdbc:jtds:sybase://localhost:5000/master";
    private static final String USER = "sa";
    private static final String PASSWORD = "myPassword";
    private static final String TABLE_NAME = "#PrepStmtTest"; // Temp table
    
    private static int passedTests = 0;
    private static int failedTests = 0;

    public static void main(String[] args) {
        try {
            System.out.println("=".repeat(80));
            System.out.println("SYBASE BASIC CONNECTION TEST");
            System.out.println("=".repeat(80));
            
            // Load driver - trying jTDS driver
            Class.forName("net.sourceforge.jtds.jdbc.Driver");
            
            // Test basic connection
            testBasicConnection();
            
            // Print summary
            System.out.println("\n" + "=".repeat(80));
            System.out.println("TEST SUMMARY");
            System.out.println("=".repeat(80));
            System.out.println("✅ Passed: " + passedTests);
            System.out.println("❌ Failed: " + failedTests);
            System.out.println("Total:  " + (passedTests + failedTests));
            System.out.println("=".repeat(80));
            
        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void runAllTests() {
        // Connection tests
        testBasicConnection();
        // testConnectionWithDynamicPrepareOff();
        
        // // PreparedStatement tests with various data types
        // testInsertWithVariousDataTypes();
        // testUpdateWithNullValues();
        // testSelectWithEqualsNull();
        // testSelectWithNotEqualsNull();
        // testSelectWithMultipleParameters();
        // testDeleteWithParameters();
        // testUnicodeCharacters();
        // testBatchInsert();
        // testComplexWhereClause();
        // testQuotesInStringParameters();
        // testNumericTypes();
        // testParameterReuse();
        
        // // Temporary table tests
        // testTempTablePersistence();
        // testTempTableMultipleInserts();
        // testTempTableWithNullValues();
        // testTempTableBatchOperations();
    }
    
    private static Connection getConnection() throws SQLException {
        return DriverManager.getConnection(URL, USER, PASSWORD);
    }
    
    private static Connection getConnectionWithDynamicPrepareOff() throws SQLException {
        Properties props = new Properties();
        props.setProperty("user", USER);
        props.setProperty("password", PASSWORD);
        props.setProperty("DYNAMIC_PREPARE", "false");
        return DriverManager.getConnection(URL, props);
    }
    
    private static void setupTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            // Drop temp table if it exists - temp tables auto-drop on connection close
            // No need for explicit drop for temp tables
            
            // Create temporary table with nullable columns
            stmt.execute("CREATE TABLE " + TABLE_NAME + " ("
                    + "id INT PRIMARY KEY, "
                    + "name VARCHAR(100) NULL, "
                    + "age INT NULL, "
                    + "salary DECIMAL(10,2) NULL, "
                    + "is_active TINYINT NULL, "  // BIT doesn't support NULL in Sybase
                    + "hire_date DATE NULL, "
                    + "created_at DATETIME NULL, "
                    + "description VARCHAR(255) NULL, "
                    + "status VARCHAR(50) NULL, "
                    + "manager_id INT NULL"
                    + ")");
        }
    }
    
    private static void cleanupTable(Connection conn) {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE " + TABLE_NAME);
        } catch (SQLException e) {
            // Ignore - temp table may already be dropped
        }
    }
    
    // Test Methods
    
    private static void testBasicConnection() {
        runTest("Basic Connection", () -> {
            try (Connection conn = getConnection();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT @@version")) {
                
                assertTrue(rs.next(), "Should get version");
                String version = rs.getString(1);
                assertNotNull(version, "Version should not be null");
                System.out.println("    Sybase Version: " + version.substring(0, Math.min(50, version.length())));
            }
        });
    }
    
    private static void testConnectionWithDynamicPrepareOff() {
        runTest("Connection with DYNAMIC_PREPARE=false", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff();
                 Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT 'test' as result")) {
                
                assertTrue(rs.next(), "Should get result");
                assertEquals("test", rs.getString("result"), "Should match test string");
            }
        });
    }
    
    private static void testInsertWithVariousDataTypes() {
        runTest("Insert with Various Data Types", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff()) {
                setupTable(conn);
                
                String sql = "INSERT INTO " + TABLE_NAME + " (id, name, age, salary, is_active, hire_date, created_at, description) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
                
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setInt(1, 1);
                    pstmt.setString(2, "O'Reilly");
                    pstmt.setInt(3, 35);
                    pstmt.setBigDecimal(4, new BigDecimal("98765.43"));
                    pstmt.setBoolean(5, true);
                    pstmt.setDate(6, Date.valueOf("2025-11-03"));
                    pstmt.setTimestamp(7, Timestamp.valueOf("2025-11-03 10:45:30"));
                    pstmt.setString(8, "Test with 'quotes'");
                    
                    int rows = pstmt.executeUpdate();
                    assertEquals(1, rows, "Should insert 1 row");
                }
                
                // Verify
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " WHERE id = 1")) {
                    assertTrue(rs.next(), "Row should exist");
                    assertEquals("O'Reilly", rs.getString("name"), "Name should match");
                    assertEquals(35, rs.getInt("age"), "Age should match");
                }
                
                cleanupTable(conn);
            }
        });
    }
    
    private static void testUpdateWithNullValues() {
        runTest("Update with NULL Values", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff()) {
                setupTable(conn);
                
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO " + TABLE_NAME + " (id, name, age, manager_id, status) "
                            + "VALUES (2, 'John Doe', 30, 1, 'ACTIVE')");
                }
                
                String sql = "UPDATE " + TABLE_NAME + " SET manager_id = ?, status = ? WHERE id = ?";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setNull(1, Types.INTEGER);
                    pstmt.setNull(2, Types.VARCHAR);
                    pstmt.setInt(3, 2);
                    
                    int rows = pstmt.executeUpdate();
                    assertEquals(1, rows, "Should update 1 row");
                }
                
                // Verify
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " WHERE id = 2")) {
                    assertTrue(rs.next(), "Row should exist");
                    rs.getInt("manager_id");
                    assertTrue(rs.wasNull(), "manager_id should be NULL");
                }
                
                cleanupTable(conn);
            }
        });
    }
    
    private static void testSelectWithEqualsNull() {
        runTest("SELECT with = NULL", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff()) {
                setupTable(conn);
                
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO " + TABLE_NAME + " (id, name, age, manager_id) VALUES (3, 'Alice', 28, NULL)");
                    stmt.execute("INSERT INTO " + TABLE_NAME + " (id, name, age, manager_id) VALUES (4, 'Bob', 32, 10)");
                }
                
                String sql = "SELECT * FROM " + TABLE_NAME + " WHERE id = 3 AND manager_id IS NULL";
                try (PreparedStatement pstmt = conn.prepareStatement(sql);
                     ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next(), "Should find row with NULL manager_id");
                    assertEquals(3, rs.getInt("id"), "ID should be 3");
                }
                
                cleanupTable(conn);
            }
        });
    }
    
    private static void testSelectWithNotEqualsNull() {
        runTest("SELECT with != NULL", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff()) {
                setupTable(conn);
                
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO " + TABLE_NAME + " (id, name, manager_id) VALUES (3, 'Alice', NULL)");
                    stmt.execute("INSERT INTO " + TABLE_NAME + " (id, name, manager_id) VALUES (4, 'Bob', 10)");
                }
                
                String sql = "SELECT * FROM " + TABLE_NAME + " WHERE manager_id IS NOT NULL";
                try (PreparedStatement pstmt = conn.prepareStatement(sql);
                     ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next(), "Should find row with non-NULL manager_id");
                    assertEquals(4, rs.getInt("id"), "ID should be 4");
                }
                
                cleanupTable(conn);
            }
        });
    }
    
    private static void testSelectWithMultipleParameters() {
        runTest("SELECT with Multiple Parameters", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff()) {
                setupTable(conn);
                
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO " + TABLE_NAME + " (id, name, age, salary, is_active) VALUES (5, 'Charlie', 40, 75000.00, 1)");
                    stmt.execute("INSERT INTO " + TABLE_NAME + " (id, name, age, salary, is_active) VALUES (6, 'Diana', 35, 85000.00, 0)");
                }
                
                String sql = "SELECT * FROM " + TABLE_NAME + " WHERE age > ? AND salary < ? AND is_active = ?";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setInt(1, 30);
                    pstmt.setBigDecimal(2, new BigDecimal("90000"));
                    pstmt.setBoolean(3, true);
                    
                    try (ResultSet rs = pstmt.executeQuery()) {
                        assertTrue(rs.next(), "Should find matching row");
                        assertEquals(5, rs.getInt("id"), "ID should be 5");
                        assertFalse(rs.next(), "Should only find one row");
                    }
                }
                
                cleanupTable(conn);
            }
        });
    }
    
    private static void testDeleteWithParameters() {
        runTest("DELETE with Parameters", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff()) {
                setupTable(conn);
                
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO " + TABLE_NAME + " (id, name, age, status) VALUES (7, 'Test', 25, 'INACTIVE')");
                }
                
                String sql = "DELETE FROM " + TABLE_NAME + " WHERE status = ? AND age < ?";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setString(1, "INACTIVE");
                    pstmt.setInt(2, 30);
                    
                    int rows = pstmt.executeUpdate();
                    assertEquals(1, rows, "Should delete 1 row");
                }
                
                cleanupTable(conn);
            }
        });
    }
    
    private static void testUnicodeCharacters() {
        runTest("Unicode Characters", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff()) {
                setupTable(conn);
                
                String sql = "INSERT INTO " + TABLE_NAME + " (id, name, description) VALUES (?, ?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setInt(1, 8);
                    pstmt.setString(2, "Test User");
                    pstmt.setString(3, "Test Description");
                    
                    int rows = pstmt.executeUpdate();
                    assertEquals(1, rows, "Should insert 1 row");
                }
                
                cleanupTable(conn);
            }
        });
    }
    
    private static void testBatchInsert() {
        runTest("Batch Insert", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff()) {
                setupTable(conn);
                
                String sql = "INSERT INTO " + TABLE_NAME + " (id, name, age) VALUES (?, ?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    for (int i = 10; i < 15; i++) {
                        pstmt.setInt(1, i);
                        pstmt.setString(2, "User" + i);
                        pstmt.setInt(3, 20 + i);
                        pstmt.addBatch();
                    }
                    
                    int[] results = pstmt.executeBatch();
                    assertTrue(results.length == 5, "Should execute 5 batches");
                }
                
                // Verify
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE id >= 10 AND id < 15")) {
                    assertTrue(rs.next(), "Should get count");
                    assertEquals(5, rs.getInt(1), "Should have 5 rows");
                }
                
                cleanupTable(conn);
            }
        });
    }
    
    private static void testComplexWhereClause() {
        runTest("Complex WHERE Clause", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff()) {
                setupTable(conn);
                
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO " + TABLE_NAME + " (id, name, age, salary, status) VALUES (20, 'Emma', 28, 60000, 'ACTIVE')");
                    stmt.execute("INSERT INTO " + TABLE_NAME + " (id, name, age, salary, status) VALUES (21, 'Frank', 35, 75000, 'PENDING')");
                    stmt.execute("INSERT INTO " + TABLE_NAME + " (id, name, age, salary, status) VALUES (22, 'Grace', 42, 90000, 'ACTIVE')");
                }
                
                String sql = "SELECT * FROM " + TABLE_NAME + " WHERE (status = ? OR status = ?) AND age > ? AND salary < ?";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setString(1, "ACTIVE");
                    pstmt.setString(2, "PENDING");
                    pstmt.setInt(3, 25);
                    pstmt.setBigDecimal(4, new BigDecimal("80000"));
                    
                    try (ResultSet rs = pstmt.executeQuery()) {
                        int count = 0;
                        while (rs.next()) {
                            count++;
                        }
                        assertTrue(count >= 1, "Should find at least 1 row");
                    }
                }
                
                cleanupTable(conn);
            }
        });
    }
    
    private static void testQuotesInStringParameters() {
        runTest("Quotes in String Parameters", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff()) {
                setupTable(conn);
                
                String sql = "INSERT INTO " + TABLE_NAME + " (id, name, description) VALUES (?, ?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setInt(1, 30);
                    pstmt.setString(2, "It's a test");
                    pstmt.setString(3, "He said 'Hello'");
                    
                    int rows = pstmt.executeUpdate();
                    assertEquals(1, rows, "Should insert 1 row");
                }
                
                // Verify
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " WHERE id = 30")) {
                    assertTrue(rs.next(), "Row should exist");
                    assertEquals("It's a test", rs.getString("name"), "Name should match");
                }
                
                cleanupTable(conn);
            }
        });
    }
    
    private static void testNumericTypes() {
        runTest("Numeric Types", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff()) {
                setupTable(conn);
                
                String sql = "INSERT INTO " + TABLE_NAME + " (id, age, salary) VALUES (?, ?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setInt(1, 50);
                    pstmt.setInt(2, 999999);
                    pstmt.setBigDecimal(3, new BigDecimal("99999999.99"));
                    
                    int rows = pstmt.executeUpdate();
                    assertEquals(1, rows, "Should insert 1 row");
                }
                
                cleanupTable(conn);
            }
        });
    }
    
    private static void testParameterReuse() {
        runTest("Parameter Reuse", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff()) {
                setupTable(conn);
                
                String sql = "INSERT INTO " + TABLE_NAME + " (id, name, age) VALUES (?, ?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setInt(1, 60);
                    pstmt.setString(2, "User60");
                    pstmt.setInt(3, 60);
                    assertEquals(1, pstmt.executeUpdate(), "First insert should succeed");
                    
                    pstmt.setInt(1, 61);
                    pstmt.setString(2, "User61");
                    pstmt.setInt(3, 61);
                    assertEquals(1, pstmt.executeUpdate(), "Second insert should succeed");
                }
                
                // Verify
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE id IN (60, 61)")) {
                    assertTrue(rs.next(), "Should get count");
                    assertEquals(2, rs.getInt(1), "Should have 2 rows");
                }
                
                cleanupTable(conn);
            }
        });
    }
    
    private static void testTempTablePersistence() {
        runTest("Temp Table Persistence", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff()) {
                // Create temp table
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE #TempTest (id INT, name VARCHAR(100))");
                    stmt.execute("INSERT INTO #TempTest VALUES (1, 'Test')");
                }
                
                // Query in different statement
                try (PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM #TempTest WHERE id = ?")) {
                    pstmt.setInt(1, 1);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        assertTrue(rs.next(), "Temp table should persist");
                        assertEquals("Test", rs.getString("name"), "Name should match");
                    }
                }
                
                // Cleanup
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("DROP TABLE #TempTest");
                }
            }
        });
    }
    
    private static void testTempTableMultipleInserts() {
        runTest("Temp Table Multiple Inserts", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE #MultiInsert (id INT, value VARCHAR(50))");
                }
                
                String sql = "INSERT INTO #MultiInsert VALUES (?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    for (int i = 1; i <= 5; i++) {
                        pstmt.setInt(1, i);
                        pstmt.setString(2, "Value" + i);
                        assertEquals(1, pstmt.executeUpdate(), "Should insert 1 row");
                    }
                }
                
                // Verify
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM #MultiInsert")) {
                    assertTrue(rs.next(), "Should get count");
                    assertEquals(5, rs.getInt(1), "Should have 5 rows");
                }
                
                // Cleanup
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("DROP TABLE #MultiInsert");
                }
            }
        });
    }
    
    private static void testTempTableWithNullValues() {
        runTest("Temp Table with NULL Values", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE #NullTest (id INT NULL, manager_id INT NULL, notes VARCHAR(100) NULL)");
                }
                
                String sql = "INSERT INTO #NullTest VALUES (?, ?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setInt(1, 1);
                    pstmt.setNull(2, Types.INTEGER);
                    pstmt.setString(3, "No manager");
                    pstmt.executeUpdate();
                }
                
                // Query for NULL
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT * FROM #NullTest WHERE manager_id IS NULL")) {
                    assertTrue(rs.next(), "Should find NULL row");
                    assertEquals("No manager", rs.getString("notes"), "Notes should match");
                }
                
                // Cleanup
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("DROP TABLE #NullTest");
                }
            }
        });
    }
    
    private static void testTempTableBatchOperations() {
        runTest("Temp Table Batch Operations", () -> {
            try (Connection conn = getConnectionWithDynamicPrepareOff()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("CREATE TABLE #BatchTest (id INT, data VARCHAR(50))");
                }
                
                String sql = "INSERT INTO #BatchTest VALUES (?, ?)";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    for (int i = 1; i <= 10; i++) {
                        pstmt.setInt(1, i);
                        pstmt.setString(2, "Batch" + i);
                        pstmt.addBatch();
                    }
                    
                    int[] results = pstmt.executeBatch();
                    assertTrue(results.length == 10, "Should execute 10 batches");
                }
                
                // Verify
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM #BatchTest")) {
                    assertTrue(rs.next(), "Should get count");
                    assertEquals(10, rs.getInt(1), "Should have 10 rows");
                }
                
                // Cleanup
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("DROP TABLE #BatchTest");
                }
            }
        });
    }
    
    // Helper methods
    
    private static void runTest(String testName, TestCase test) {
        System.out.println("\n▶ Running: " + testName);
        try {
            test.run();
            passedTests++;
            System.out.println("  ✅ PASSED");
        } catch (Exception e) {
            failedTests++;
            System.out.println("  ❌ FAILED: " + e.getMessage());
            e.printStackTrace();
        }
    }
    
    private static void assertTrue(boolean condition, String message) throws AssertionError {
        if (!condition) {
            throw new AssertionError("Assertion failed: " + message);
        }
    }
    
    private static void assertFalse(boolean condition, String message) throws AssertionError {
        assertTrue(!condition, message);
    }
    
    private static void assertEquals(Object expected, Object actual, String message) throws AssertionError {
        if (expected == null && actual == null) return;
        if (expected == null || !expected.equals(actual)) {
            throw new AssertionError(message + " - Expected: " + expected + ", Got: " + actual);
        }
    }
    
    private static void assertNotNull(Object obj, String message) throws AssertionError {
        if (obj == null) {
            throw new AssertionError(message);
        }
    }
    
    @FunctionalInterface
    interface TestCase {
        void run() throws Exception;
    }
}
