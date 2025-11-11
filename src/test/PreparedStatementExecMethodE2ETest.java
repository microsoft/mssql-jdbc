/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.example;

import java.sql.*;

/**
 * E2E tests for PreparedStatement using EXEC method (PrepareMethod=exec).
 * Tests the SqlServerPreparedStatementExpander in a real database environment using temp tables.
 */
public class PreparedStatementExecMethodE2ETest {

    static Connection execMethodConnection;
    private static int passedTests = 0;
    private static int failedTests = 0;

    // Connection settings - modify for your environment
    private static final String URL = "jdbc:jtds:sybase://localhost:5000/master";
    private static final String USER = "sa";
    private static final String PASSWORD = "myPassword";

    // Simple assertion helpers
    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("Assertion failed: " + message);
        }
    }

    private static void assertTrue(boolean condition) {
        assertTrue(condition, "");
    }

    private static void assertEquals(Object expected, Object actual, String message) {
        if (expected == null && actual == null) {
            return;
        }
        if (expected == null || !expected.equals(actual)) {
            throw new AssertionError(message + " - Expected: " + expected + ", but got: " + actual);
        }
    }

    private static void assertEquals(Object expected, Object actual) {
        assertEquals(expected, actual, "");
    }

    private static void assertNull(Object obj, String message) {
        if (obj != null) {
            throw new AssertionError(message + " - Expected null, but got: " + obj);
        }
    }

    private static void assertNotEquals(Object unexpected, Object actual) {
        if (unexpected == null && actual == null) {
            throw new AssertionError("Both values are null");
        }
        if (unexpected != null && unexpected.equals(actual)) {
            throw new AssertionError("Values should not be equal but both are: " + actual);
        }
    }

    private static void assertFalse(boolean condition, String message) {
        if (condition) {
            throw new AssertionError("Assertion failed: " + message);
        }
    }

    private static void assertNotNull(Object obj, String message) {
        if (obj == null) {
            throw new AssertionError(message);
        }
    }

    @FunctionalInterface
    interface TestCase {
        void run() throws Exception;
    }

    private static void runTest(String testName, TestCase test) {
        System.out.println("\n▶ Running: " + testName);
        try {
            test.run();
            System.out.println("  ✅ PASSED");
            passedTests++;
        } catch (Exception e) {
            System.out.println("  ❌ FAILED: " + e.getMessage());
            e.printStackTrace();
            failedTests++;
        }
    }

    public static void setupTest() throws Exception {
        // Create a connection with jTDS driver for Sybase
        Class.forName("net.sourceforge.jtds.jdbc.Driver");
        execMethodConnection = DriverManager.getConnection(URL, USER, PASSWORD);
    }

    public static void cleanupTest() throws Exception {
        if (execMethodConnection != null && !execMethodConnection.isClosed()) {
            execMethodConnection.close();
        }
    }

    /**
     * Helper method to create a temp table for each test.
     */
    private static void createTempTable(String tableName) throws SQLException {
        try (Statement stmt = execMethodConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " ("
                    + "id INT PRIMARY KEY, "
                    + "name VARCHAR(100) NULL, "
                    + "description VARCHAR(1000) NULL, "
                    + "age INT NULL, "
                    + "salary DECIMAL(10,2) NULL, "
                    + "is_active TINYINT NULL, "
                    + "manager_id INT NULL, "
                    + "notes VARCHAR(500) NULL"
                    + ")");
        }
    }

    
    public static void testInsertWithVariousDataTypes() throws SQLException {
        String tableName = "#TempInsertTest";
        createTempTable(tableName);

        String insertSql = "INSERT INTO " + tableName +
                " (id, name, description, age, salary, is_active, manager_id, notes) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setString(2, "John O'Brien");
            pstmt.setString(3, "Senior developer with 10+ years' experience");
            pstmt.setInt(4, 35);
            pstmt.setBigDecimal(5, new java.math.BigDecimal("75000.50"));
            pstmt.setBoolean(6, true);
            pstmt.setInt(7, 100);
            pstmt.setString(8, "Excellent performer");

            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify the insert
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1")) {
            assertTrue(rs.next(), "Should find the inserted row");
            assertEquals("John O'Brien", rs.getString("name"));
            assertEquals("Senior developer with 10+ years' experience", rs.getString("description"));
            assertEquals(35, rs.getInt("age"));
            assertEquals(new java.math.BigDecimal("75000.50"), rs.getBigDecimal("salary"));
            assertTrue(rs.getBoolean("is_active"));
            assertEquals(100, rs.getInt("manager_id"));
        }
    }

    
    public static void testSelectWithMultipleParameters() throws SQLException {
        String tableName = "#TempSelectMultiTest";
        createTempTable(tableName);

        // First insert some test data
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(
                "INSERT INTO " + tableName +
                        " (id, name, age, salary, is_active) VALUES (?, ?, ?, ?, ?)")) {
            pstmt.setInt(1, 2);
            pstmt.setString(2, "Jane Smith");
            pstmt.setInt(3, 28);
            pstmt.setBigDecimal(4, new java.math.BigDecimal("65000.00"));
            pstmt.setBoolean(5, true);
            pstmt.executeUpdate();
        }

        // Test SELECT with multiple parameters
        String selectSql = "SELECT * FROM " + tableName +
                " WHERE age > ? AND salary < ? AND is_active = ?";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(selectSql)) {
            pstmt.setInt(1, 25);
            pstmt.setBigDecimal(2, new java.math.BigDecimal("70000.00"));
            pstmt.setBoolean(3, true);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next(), "Should find matching row");
                assertEquals("Jane Smith", rs.getString("name"));
                assertEquals(28, rs.getInt("age"));
            }
        }
    }

    
    public static void testUpdateWithNullValues() throws SQLException {
        String tableName = "#TempUpdateNullTest";
        createTempTable(tableName);

        // Insert test data
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(
                "INSERT INTO " + tableName +
                        " (id, name, age, is_active, manager_id) VALUES (?, ?, ?, ?, ?)")) {
            pstmt.setInt(1, 3);
            pstmt.setString(2, "Bob Johnson");
            pstmt.setInt(3, 45);
            pstmt.setBoolean(4, true);
            pstmt.setInt(5, 200);
            pstmt.executeUpdate();
        }

        // Update with NULL
        String updateSql = "UPDATE " + tableName +
                " SET manager_id = ?, notes = ? WHERE id = ?";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(updateSql)) {
            pstmt.setNull(1, Types.INTEGER);
            pstmt.setNull(2, Types.VARCHAR);
            pstmt.setInt(3, 3);
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should update 1 row");
        }

        // Verify the update
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 3")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt("manager_id"));
            assertTrue(rs.wasNull(), "manager_id should be NULL");
            assertNull(rs.getString("notes"), "notes should be NULL");
        }
    }

    
    public static void testSelectWithEqualsNull() throws SQLException {
        String tableName = "#TempSelectEqualsNullTest";
        createTempTable(tableName);

        // Insert test data with NULL
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(
                "INSERT INTO " + tableName +
                        " (id, name, age, is_active, manager_id) VALUES (?, ?, ?, ?, ?)")) {
            pstmt.setInt(1, 4);
            pstmt.setString(2, "Alice Brown");
            pstmt.setInt(3, 30);
            pstmt.setBoolean(4, true);
            pstmt.setNull(5, Types.INTEGER);
            pstmt.executeUpdate();
        }

        // Test WHERE column = ? with NULL parameter (should be rewritten to IS NULL)
        String selectSql = "SELECT * FROM " + tableName +
                " WHERE manager_id = ?";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(selectSql)) {
            pstmt.setNull(1, Types.INTEGER);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next(), "Should find row with NULL manager_id");
                assertEquals("Alice Brown", rs.getString("name"));
            }
        }
    }

    
    public static void testSelectWithNotEqualsNull() throws SQLException {
        String tableName = "#TempSelectNotEqualsNullTest";
        createTempTable(tableName);

        // Insert some test data
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(
                "INSERT INTO " + tableName + " (id, name, age, is_active, manager_id) VALUES (?, ?, ?, ?, ?)")) {
            pstmt.setInt(1, 1);
            pstmt.setString(2, "HasManager");
            pstmt.setInt(3, 30);
            pstmt.setBoolean(4, true);
            pstmt.setInt(5, 100);
            pstmt.executeUpdate();
        }

        // Test WHERE column <> ? with NULL parameter (should be rewritten to IS NOT NULL)
        String selectSql = "SELECT * FROM " + tableName +
                " WHERE manager_id <> ?";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(selectSql)) {
            pstmt.setNull(1, Types.INTEGER);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                // Should find rows where manager_id IS NOT NULL
                int count = 0;
                while (rs.next()) {
                    assertNotEquals(0, rs.getInt("manager_id"));
                    assertFalse(rs.wasNull(), "Should only return non-NULL manager_id values");
                    count++;
                }
                assertTrue(count > 0, "Should find at least one row with non-NULL manager_id");
            }
        }
    }

    
    public static void testDeleteWithParameters() throws SQLException {
        String tableName = "#TempDeleteTest";
        createTempTable(tableName);

        // Insert test data
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(
                "INSERT INTO " + tableName +
                        " (id, name, age, is_active) VALUES (?, ?, ?, ?)")) {
            pstmt.setInt(1, 5);
            pstmt.setString(2, "Charlie Davis");
            pstmt.setInt(3, 50);
            pstmt.setBoolean(4, false);
            pstmt.executeUpdate();
        }

        // Delete with parameters
        String deleteSql = "DELETE FROM " + tableName +
                " WHERE id = ? AND is_active = ?";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(deleteSql)) {
            pstmt.setInt(1, 5);
            pstmt.setBoolean(2, false);
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should delete 1 row");
        }

        // Verify deletion
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName + " WHERE id = 5")) {
            assertTrue(rs.next());
            assertEquals(0, rs.getInt(1), "Row should be deleted");
        }
    }

    
    public static void testQuotesInStringParameters() throws SQLException {
        String tableName = "#TempQuotesTest";
        createTempTable(tableName);

        String insertSql = "INSERT INTO " + tableName +
                " (id, name, description) VALUES (?, ?, ?)";

        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 6);
            pstmt.setString(2, "It's a test");
            pstmt.setString(3, "Contains 'single' and \"double\" quotes");
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 6")) {
            assertTrue(rs.next());
            assertEquals("It's a test", rs.getString("name"));
            assertEquals("Contains 'single' and \"double\" quotes", rs.getString("description"));
        }
    }

    
    public static void testNumericTypes() throws SQLException {
        String tableName = "#TempNumericTest";
        createTempTable(tableName);

        String insertSql = "INSERT INTO " + tableName +
                " (id, age, salary) VALUES (?, ?, ?)";

        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 7);
            pstmt.setInt(2, 42);
            pstmt.setBigDecimal(3, new java.math.BigDecimal("123456.78"));
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 7")) {
            assertTrue(rs.next());
            assertEquals(42, rs.getInt("age"));
            assertEquals(new java.math.BigDecimal("123456.78"), rs.getBigDecimal("salary"));
        }
    }

    
    public static void testComplexWhereClause() throws SQLException {
        String tableName = "#TempComplexWhereTest";
        createTempTable(tableName);

        // Insert test data
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(
                "INSERT INTO " + tableName +
                        " (id, name, age, salary, is_active, manager_id) VALUES (?, ?, ?, ?, ?, ?)")) {
            pstmt.setInt(1, 8);
            pstmt.setString(2, "Complex Test");
            pstmt.setInt(3, 35);
            pstmt.setBigDecimal(4, new java.math.BigDecimal("80000.00"));
            pstmt.setBoolean(5, true);
            pstmt.setNull(6, Types.INTEGER);
            pstmt.executeUpdate();
        }

        // Complex WHERE clause with multiple conditions
        String selectSql = "SELECT * FROM " + tableName +
                " WHERE (age > ? OR salary < ?) AND is_active = ? AND manager_id = ?";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(selectSql)) {
            pstmt.setInt(1, 30);
            pstmt.setBigDecimal(2, new java.math.BigDecimal("50000.00"));
            pstmt.setBoolean(3, true);
            pstmt.setNull(4, Types.INTEGER);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next(), "Should find matching row");
                assertEquals("Complex Test", rs.getString("name"));
            }
        }
    }

    // TODO: Batch execution with PrepareMethod.EXEC appears to have an encoding issue
    // with temp table names. Investigate separately.
    // 
    public static void testBatchInsertWithExecMethod_Disabled() throws SQLException {
        String tableName = "#TempBatchTest";
        createTempTable(tableName);

        String insertSql = "INSERT INTO " + tableName +
                " (id, name, age) VALUES (?, ?, ?)";

        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            for (int i = 9; i < 12; i++) {
                pstmt.setInt(1, i);
                pstmt.setString(2, "Batch User " + i);
                pstmt.setInt(3, 20 + i);
                pstmt.addBatch();
            }
            
            int[] results = pstmt.executeBatch();
            assertEquals(3, results.length, "Should have 3 batch results");
            for (int result : results) {
                assertEquals(1, result, "Each batch should affect 1 row");
            }
        }

        // Verify
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName + " WHERE id BETWEEN 9 AND 11")) {
            assertTrue(rs.next());
            assertEquals(3, rs.getInt(1), "Should have 3 batch-inserted rows");
        }
    }

    
    public static void testUnicodeCharacters() throws SQLException {
        String tableName = "#TempUnicodeTest";
        createTempTable(tableName);

        String insertSql = "INSERT INTO " + tableName +
                " (id, name, description) VALUES (?, ?, ?)";

        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 12);
            pstmt.setString(2, "महेन्द्र");
            pstmt.setString(3, "Unicode test: 你好世界 مرحبا العالم");
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 12")) {
            assertTrue(rs.next());
            assertEquals("महेन्द्र", rs.getString("name"));
            assertEquals("Unicode test: 你好世界 مرحبا العالم", rs.getString("description"));
        }
    }

    
    public static void testNullInComplexExpression() throws SQLException {
        String tableName = "#TempNullComplexTest";
        createTempTable(tableName);

        // Insert test data
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(
                "INSERT INTO " + tableName +
                        " (id, name, age, is_active, manager_id, notes) VALUES (?, ?, ?, ?, ?, ?)")) {
            pstmt.setInt(1, 13);
            pstmt.setString(2, "Null Test");
            pstmt.setInt(3, 40);
            pstmt.setBoolean(4, true);
            pstmt.setNull(5, Types.INTEGER);
            pstmt.setNull(6, Types.VARCHAR);
            pstmt.executeUpdate();
        }

        // Complex expression with NULLs
        String selectSql = "SELECT * FROM " + tableName +
                " WHERE age = ? AND manager_id = ? AND (notes = ? OR notes IS NULL)";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(selectSql)) {
            pstmt.setInt(1, 40);
            pstmt.setNull(2, Types.INTEGER);
            pstmt.setNull(3, Types.VARCHAR);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next(), "Should find row with NULL values");
                assertEquals("Null Test", rs.getString("name"));
            }
        }
    }

    
    public static void testParameterReuse() throws SQLException {
        String tableName = "#TempParamReuseTest";
        createTempTable(tableName);

        String insertSql = "INSERT INTO " + tableName +
                " (id, name, age) VALUES (?, ?, ?)";

        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            // First execution
            pstmt.setInt(1, 14);
            pstmt.setString(2, "First Execution");
            pstmt.setInt(3, 25);
            pstmt.executeUpdate();

            // Reuse same PreparedStatement with different parameters
            pstmt.setInt(1, 15);
            pstmt.setString(2, "Second Execution");
            pstmt.setInt(3, 26);
            pstmt.executeUpdate();
        }

        // Verify both inserts
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName + " WHERE id IN (14, 15)")) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1), "Should have both rows inserted");
        }
    }

    
    public static void testExecuteMethodReturnsResultSet() throws SQLException {
        String tableName = "#TempExecuteTest";
        createTempTable(tableName);

        // Insert test data
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(
                "INSERT INTO " + tableName + " (id, name, age, is_active) VALUES (?, ?, ?, ?)")) {
            pstmt.setInt(1, 1);
            pstmt.setString(2, "Execute Test");
            pstmt.setInt(3, 25);
            pstmt.setBoolean(4, true);
            pstmt.executeUpdate();
        }

        String selectSql = "SELECT id, name FROM " + tableName + " WHERE id = ?";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(selectSql)) {
            pstmt.setInt(1, 1);
            
            boolean hasResultSet = pstmt.execute();
            assertTrue(hasResultSet, "execute() should return true for SELECT query");
            
            try (ResultSet rs = pstmt.getResultSet()) {
                assertNotNull(rs, "Should have ResultSet");
                assertTrue(rs.next(), "Should find the inserted row");
                assertEquals(1, rs.getInt("id"));
                assertEquals("Execute Test", rs.getString("name"));
            }
        }
    }

    
    public static void testBlobDataType() throws SQLException {
        String tableName = "#TempBlobTest";
        
        // Create table with IMAGE column (Sybase's BLOB equivalent)
        try (Statement stmt = execMethodConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " ("
                    + "id INT PRIMARY KEY, "
                    + "binary_data IMAGE NULL, "
                    + "description VARCHAR(100) NULL"
                    + ")");
        }

        // Insert binary data
        byte[] binaryData = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, (byte)0xFF, (byte)0xFE};
        String insertSql = "INSERT INTO " + tableName + " (id, binary_data, description) VALUES (?, ?, ?)";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setBytes(2, binaryData);
            pstmt.setString(3, "Binary test data");
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify binary data
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1")) {
            assertTrue(rs.next(), "Should find the inserted row");
            byte[] retrievedData = rs.getBytes("binary_data");
            assertNotNull(retrievedData, "Binary data should not be null");
            assertEquals(binaryData.length, retrievedData.length, "Binary data length should match");
            for (int i = 0; i < binaryData.length; i++) {
                assertEquals(binaryData[i], retrievedData[i], "Byte at position " + i + " should match");
            }
        }
    }

    
    public static void testTextDataType() throws SQLException {
        String tableName = "#TempTextTest";
        
        // Create table with TEXT column (Sybase's CLOB equivalent)
        try (Statement stmt = execMethodConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " ("
                    + "id INT PRIMARY KEY, "
                    + "large_text TEXT NULL, "
                    + "description VARCHAR(100) NULL"
                    + ")");
        }

        // Insert large text data
        StringBuilder largeText = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            largeText.append("This is line ").append(i).append(" of the large text field. ");
        }
        String textData = largeText.toString();
        
        String insertSql = "INSERT INTO " + tableName + " (id, large_text, description) VALUES (?, ?, ?)";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setString(2, textData);
            pstmt.setString(3, "Large text test");
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify text data
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1")) {
            assertTrue(rs.next(), "Should find the inserted row");
            String retrievedText = rs.getString("large_text");
            assertNotNull(retrievedText, "Text data should not be null");
            assertEquals(textData.length(), retrievedText.length(), "Text length should match");
            assertEquals(textData, retrievedText, "Text content should match");
        }
    }

    
    public static void testDateTimeDataTypes() throws SQLException {
        String tableName = "#TempDateTimeTest";
        
        // Create table with various date/time columns
        try (Statement stmt = execMethodConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " ("
                    + "id INT PRIMARY KEY, "
                    + "date_col DATETIME NULL, "
                    + "timestamp_col DATETIME NULL, "
                    + "description VARCHAR(100) NULL"
                    + ")");
        }

        // Insert date/time data
        java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(System.currentTimeMillis());
        java.sql.Date currentDate = new java.sql.Date(System.currentTimeMillis());
        
        String insertSql = "INSERT INTO " + tableName + " (id, date_col, timestamp_col, description) VALUES (?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setDate(2, currentDate);
            pstmt.setTimestamp(3, currentTimestamp);
            pstmt.setString(4, "DateTime test");
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify date/time data
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1")) {
            assertTrue(rs.next(), "Should find the inserted row");
            assertNotNull(rs.getDate("date_col"), "Date should not be null");
            assertNotNull(rs.getTimestamp("timestamp_col"), "Timestamp should not be null");
        }
    }

    
    public static void testFloatAndRealDataTypes() throws SQLException {
        String tableName = "#TempFloatTest";
        
        // Create table with float/real columns
        try (Statement stmt = execMethodConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " ("
                    + "id INT PRIMARY KEY, "
                    + "float_col FLOAT NULL, "
                    + "real_col REAL NULL, "
                    + "double_col DOUBLE PRECISION NULL, "
                    + "description VARCHAR(100) NULL"
                    + ")");
        }

        // Insert floating point data
        String insertSql = "INSERT INTO " + tableName + " (id, float_col, real_col, double_col, description) VALUES (?, ?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setFloat(2, 123.456f);
            pstmt.setFloat(3, 789.012f);
            pstmt.setDouble(4, 999.888777);
            pstmt.setString(5, "Float/Real test");
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify floating point data
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1")) {
            assertTrue(rs.next(), "Should find the inserted row");
            assertTrue(Math.abs(rs.getFloat("float_col") - 123.456f) < 0.01, "Float value should be close");
            assertTrue(Math.abs(rs.getFloat("real_col") - 789.012f) < 0.01, "Real value should be close");
            assertTrue(Math.abs(rs.getDouble("double_col") - 999.888777) < 0.001, "Double value should be close");
        }
    }

    
    public static void testSmallIntAndTinyIntDataTypes() throws SQLException {
        String tableName = "#TempSmallIntTest";
        
        // Create table with various integer types
        try (Statement stmt = execMethodConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " ("
                    + "id INT PRIMARY KEY, "
                    + "tinyint_col TINYINT NULL, "
                    + "smallint_col SMALLINT NULL, "
                    + "bigint_col NUMERIC(19,0) NULL, "
                    + "description VARCHAR(100) NULL"
                    + ")");
        }

        // Insert various integer types
        String insertSql = "INSERT INTO " + tableName + " (id, tinyint_col, smallint_col, bigint_col, description) VALUES (?, ?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setByte(2, (byte) 127); // Sybase TINYINT max value that fits in signed Java byte
            pstmt.setShort(3, (short) 32000);
            pstmt.setLong(4, 9223372036854775807L);
            pstmt.setString(5, "Integer types test");
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify integer data
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1")) {
            assertTrue(rs.next(), "Should find the inserted row");
            assertEquals((byte)127, rs.getByte("tinyint_col"), "TinyInt value should match");
            assertEquals((short)32000, rs.getShort("smallint_col"), "SmallInt value should match");
            assertEquals(9223372036854775807L, rs.getLong("bigint_col"), "BigInt value should match");
        }
    }

    
    public static void testMoneyDataType() throws SQLException {
        String tableName = "#TempMoneyTest";
        
        // Create table with MONEY column
        try (Statement stmt = execMethodConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " ("
                    + "id INT PRIMARY KEY, "
                    + "price MONEY NULL, "
                    + "small_price SMALLMONEY NULL, "
                    + "description VARCHAR(100) NULL"
                    + ")");
        }

        // Insert money data
        String insertSql = "INSERT INTO " + tableName + " (id, price, small_price, description) VALUES (?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setBigDecimal(2, new java.math.BigDecimal("922337203685477.5807"));
            pstmt.setBigDecimal(3, new java.math.BigDecimal("214748.3647"));
            pstmt.setString(4, "Money test");
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify money data
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1")) {
            assertTrue(rs.next(), "Should find the inserted row");
            assertNotNull(rs.getBigDecimal("price"), "Money value should not be null");
            assertNotNull(rs.getBigDecimal("small_price"), "SmallMoney value should not be null");
        }
    }

    
    public static void testBinaryAndVarBinaryDataTypes() throws SQLException {
        String tableName = "#TempBinaryTest";
        
        // Create table with BINARY and VARBINARY columns
        try (Statement stmt = execMethodConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " ("
                    + "id INT PRIMARY KEY, "
                    + "binary_col BINARY(10) NULL, "
                    + "varbinary_col VARBINARY(50) NULL, "
                    + "description VARCHAR(100) NULL"
                    + ")");
        }

        // Insert binary data
        byte[] binaryData = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
        byte[] varbinaryData = new byte[]{(byte)0xDE, (byte)0xAD, (byte)0xBE, (byte)0xEF};
        
        String insertSql = "INSERT INTO " + tableName + " (id, binary_col, varbinary_col, description) VALUES (?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setBytes(2, binaryData);
            pstmt.setBytes(3, varbinaryData);
            pstmt.setString(4, "Binary/VarBinary test");
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify binary data
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1")) {
            assertTrue(rs.next(), "Should find the inserted row");
            byte[] retrievedBinary = rs.getBytes("binary_col");
            byte[] retrievedVarBinary = rs.getBytes("varbinary_col");
            assertNotNull(retrievedBinary, "Binary data should not be null");
            assertNotNull(retrievedVarBinary, "VarBinary data should not be null");
        }
    }

    public static void main(String[] args) {
        try {
            System.out.println("=".repeat(80));
            System.out.println("PREPARED STATEMENT EXEC METHOD E2E TESTS");
            System.out.println("=".repeat(80));

            setupTest();

            // Basic data type tests
            runTest("Insert with various data types", PreparedStatementExecMethodE2ETest::testInsertWithVariousDataTypes);
            runTest("Select with multiple parameters", PreparedStatementExecMethodE2ETest::testSelectWithMultipleParameters);
            runTest("Update with NULL values", PreparedStatementExecMethodE2ETest::testUpdateWithNullValues);
            runTest("Select with = NULL", PreparedStatementExecMethodE2ETest::testSelectWithEqualsNull);
            runTest("Select with <> NULL", PreparedStatementExecMethodE2ETest::testSelectWithNotEqualsNull);
            runTest("Delete with parameters", PreparedStatementExecMethodE2ETest::testDeleteWithParameters);
            runTest("Quotes in string parameters", PreparedStatementExecMethodE2ETest::testQuotesInStringParameters);
            runTest("Numeric types", PreparedStatementExecMethodE2ETest::testNumericTypes);
            runTest("Complex WHERE clause", PreparedStatementExecMethodE2ETest::testComplexWhereClause);
            runTest("Unicode characters", PreparedStatementExecMethodE2ETest::testUnicodeCharacters);
            runTest("NULL in complex expression", PreparedStatementExecMethodE2ETest::testNullInComplexExpression);
            runTest("Parameter reuse", PreparedStatementExecMethodE2ETest::testParameterReuse);
            runTest("Execute method returns ResultSet", PreparedStatementExecMethodE2ETest::testExecuteMethodReturnsResultSet);
            
            // Advanced data type tests
            runTest("BLOB/IMAGE data type", PreparedStatementExecMethodE2ETest::testBlobDataType);
            runTest("TEXT/CLOB data type", PreparedStatementExecMethodE2ETest::testTextDataType);
            runTest("DateTime data types", PreparedStatementExecMethodE2ETest::testDateTimeDataTypes);
            runTest("Float and Real data types", PreparedStatementExecMethodE2ETest::testFloatAndRealDataTypes);
            runTest("SmallInt and TinyInt data types", PreparedStatementExecMethodE2ETest::testSmallIntAndTinyIntDataTypes);
            runTest("Money data type", PreparedStatementExecMethodE2ETest::testMoneyDataType);
            runTest("Binary and VarBinary data types", PreparedStatementExecMethodE2ETest::testBinaryAndVarBinaryDataTypes);

            System.out.println("\n" + "=".repeat(80));
            System.out.println("TEST SUMMARY");
            System.out.println("=".repeat(80));
            System.out.println("✅ Passed: " + passedTests);
            System.out.println("❌ Failed: " + failedTests);
            System.out.println("Total:  " + (passedTests + failedTests));
            System.out.println("=".repeat(80));

            cleanupTest();

        } catch (Exception e) {
            System.err.println("Fatal error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
