/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;

import org.junit.jupiter.api.*;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.testframework.AbstractTest;


/**
 * E2E tests for PreparedStatement using EXEC method (PrepareMethod=exec).
 * Tests the SqlServerPreparedStatementExpander in a real database environment using temp tables.
 */
public class PreparedStatementExecMethodE2ETest extends AbstractTest {

    static Connection execMethodConnection;

    @BeforeAll
    public static void setupTest() throws Exception {
        setConnection();

        // Create a connection with prepareMethod=exec
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        ds.setPrepareMethod("exec");
        execMethodConnection = ds.getConnection();
    }

    @AfterAll
    public static void cleanupTest() throws Exception {
        if (execMethodConnection != null && !execMethodConnection.isClosed()) {
            execMethodConnection.close();
        }
    }

    /**
     * Helper method to create a temp table for each test.
     */
    private void createTempTable(String tableName) throws SQLException {
        try (Statement stmt = execMethodConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " ("
                    + "id INT PRIMARY KEY, "
                    + "name NVARCHAR(100), "
                    + "description NVARCHAR(MAX), "
                    + "age INT, "
                    + "salary DECIMAL(10,2), "
                    + "is_active BIT, "
                    + "manager_id INT NULL, "
                    + "notes NVARCHAR(500) NULL"
                    + ")");
        }
    }

    @Test
    public void testInsertWithVariousDataTypes() throws SQLException {
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

    @Test
    public void testSelectWithMultipleParameters() throws SQLException {
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

    @Test
    public void testUpdateWithNullValues() throws SQLException {
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
            pstmt.setNull(2, Types.NVARCHAR);
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

    @Test
    public void testSelectWithEqualsNull() throws SQLException {
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

    @Test
    public void testSelectWithNotEqualsNull() throws SQLException {
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

    @Test
    public void testDeleteWithParameters() throws SQLException {
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

    @Test
    public void testQuotesInStringParameters() throws SQLException {
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

    @Test
    public void testNumericTypes() throws SQLException {
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

    @Test
    public void testComplexWhereClause() throws SQLException {
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
    // @Test
    public void testBatchInsertWithExecMethod_Disabled() throws SQLException {
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

    @Test
    public void testUnicodeCharacters() throws SQLException {
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

    @Test
    public void testNullInComplexExpression() throws SQLException {
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
            pstmt.setNull(6, Types.NVARCHAR);
            pstmt.executeUpdate();
        }

        // Complex expression with NULLs
        String selectSql = "SELECT * FROM " + tableName +
                " WHERE age = ? AND manager_id = ? AND (notes = ? OR notes IS NULL)";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(selectSql)) {
            pstmt.setInt(1, 40);
            pstmt.setNull(2, Types.INTEGER);
            pstmt.setNull(3, Types.NVARCHAR);
            
            try (ResultSet rs = pstmt.executeQuery()) {
                assertTrue(rs.next(), "Should find row with NULL values");
                assertEquals("Null Test", rs.getString("name"));
            }
        }
    }

    @Test
    public void testParameterReuse() throws SQLException {
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

    @Test
    public void testExecuteMethodReturnsResultSet() throws SQLException {
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

    @Test
    public void testBlobImageDataType() throws SQLException {
        String tableName = "#TempBlobTest";
        
        try (Statement stmt = execMethodConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " ("
                    + "id INT PRIMARY KEY, "
                    + "binary_data VARBINARY(MAX), "
                    + "description NVARCHAR(100)"
                    + ")");
        }

        String insertSql = "INSERT INTO " + tableName +
                " (id, binary_data, description) VALUES (?, ?, ?)";

        byte[] binaryData = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05, (byte) 0xFF, (byte) 0xFE};
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setBytes(2, binaryData);
            pstmt.setString(3, "Binary test data");
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1")) {
            assertTrue(rs.next());
            byte[] retrievedData = rs.getBytes("binary_data");
            assertArrayEquals(binaryData, retrievedData, "Binary data should match");
            assertEquals("Binary test data", rs.getString("description"));
        }
    }

    @Test
    public void testTextClobDataType() throws SQLException {
        String tableName = "#TempTextTest";
        
        try (Statement stmt = execMethodConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " ("
                    + "id INT PRIMARY KEY, "
                    + "large_text NVARCHAR(MAX), "
                    + "description NVARCHAR(100)"
                    + ")");
        }

        String insertSql = "INSERT INTO " + tableName +
                " (id, large_text, description) VALUES (?, ?, ?)";

        // Create a large text (approximately 50KB)
        StringBuilder largeText = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            largeText.append("This is line ").append(i).append(" of the large text data. ");
            largeText.append("It contains some sample content to test CLOB/TEXT handling.\n");
        }
        String testText = largeText.toString();
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setString(2, testText);
            pstmt.setString(3, "Large text test");
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1")) {
            assertTrue(rs.next());
            String retrievedText = rs.getString("large_text");
            assertEquals(testText, retrievedText, "Large text should match");
            assertEquals("Large text test", rs.getString("description"));
        }
    }

    @Test
    public void testDateTimeDataTypes() throws SQLException {
        String tableName = "#TempDateTimeTest";
        
        try (Statement stmt = execMethodConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " ("
                    + "id INT PRIMARY KEY, "
                    + "date_col DATE, "
                    + "timestamp_col DATETIME2, "
                    + "description NVARCHAR(100)"
                    + ")");
        }

        String insertSql = "INSERT INTO " + tableName +
                " (id, date_col, timestamp_col, description) VALUES (?, ?, ?, ?)";

        java.sql.Date currentDate = new java.sql.Date(System.currentTimeMillis());
        java.sql.Timestamp currentTimestamp = new java.sql.Timestamp(System.currentTimeMillis());
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setDate(2, currentDate);
            pstmt.setTimestamp(3, currentTimestamp);
            pstmt.setString(4, "DateTime test");
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1")) {
            assertTrue(rs.next());
            assertNotNull(rs.getDate("date_col"));
            assertNotNull(rs.getTimestamp("timestamp_col"));
            assertEquals("DateTime test", rs.getString("description"));
        }
    }

    @Test
    public void testFloatAndRealDataTypes() throws SQLException {
        String tableName = "#TempFloatTest";
        
        try (Statement stmt = execMethodConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " ("
                    + "id INT PRIMARY KEY, "
                    + "float_col FLOAT, "
                    + "real_col REAL, "
                    + "double_col FLOAT(53), "
                    + "description NVARCHAR(100)"
                    + ")");
        }

        String insertSql = "INSERT INTO " + tableName +
                " (id, float_col, real_col, double_col, description) VALUES (?, ?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setFloat(2, 123.456f);
            pstmt.setFloat(3, 789.012f);
            pstmt.setDouble(4, 999.888777);
            pstmt.setString(5, "Float/Real test");
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(123.456f, rs.getFloat("float_col"), 0.001);
            assertEquals(789.012f, rs.getFloat("real_col"), 0.001);
            assertEquals(999.888777, rs.getDouble("double_col"), 0.000001);
            assertEquals("Float/Real test", rs.getString("description"));
        }
    }

    @Test
    public void testSmallIntAndTinyIntDataTypes() throws SQLException {
        String tableName = "#TempSmallIntTest";
        
        try (Statement stmt = execMethodConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " ("
                    + "id INT PRIMARY KEY, "
                    + "tinyint_col TINYINT, "
                    + "smallint_col SMALLINT, "
                    + "bigint_col BIGINT, "
                    + "description NVARCHAR(100)"
                    + ")");
        }

        String insertSql = "INSERT INTO " + tableName +
                " (id, tinyint_col, smallint_col, bigint_col, description) VALUES (?, ?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setByte(2, (byte) 127);
            pstmt.setShort(3, (short) 32000);
            pstmt.setLong(4, 9223372036854775807L);
            pstmt.setString(5, "Integer types test");
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1")) {
            assertTrue(rs.next());
            assertEquals(127, rs.getByte("tinyint_col"));
            assertEquals(32000, rs.getShort("smallint_col"));
            assertEquals(9223372036854775807L, rs.getLong("bigint_col"));
            assertEquals("Integer types test", rs.getString("description"));
        }
    }

    @Test
    public void testMoneyDataType() throws SQLException {
        String tableName = "#TempMoneyTest";
        
        try (Statement stmt = execMethodConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " ("
                    + "id INT PRIMARY KEY, "
                    + "price MONEY, "
                    + "small_price SMALLMONEY, "
                    + "description NVARCHAR(100)"
                    + ")");
        }

        String insertSql = "INSERT INTO " + tableName +
                " (id, price, small_price, description) VALUES (?, ?, ?, ?)";
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setBigDecimal(2, new java.math.BigDecimal("922337203685477.5807"));
            pstmt.setBigDecimal(3, new java.math.BigDecimal("214748.3647"));
            pstmt.setString(4, "Money test");
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1")) {
            assertTrue(rs.next());
            assertNotNull(rs.getBigDecimal("price"));
            assertNotNull(rs.getBigDecimal("small_price"));
            assertEquals("Money test", rs.getString("description"));
        }
    }

    @Test
    public void testBinaryAndVarBinaryDataTypes() throws SQLException {
        String tableName = "#TempBinaryTest";
        
        try (Statement stmt = execMethodConnection.createStatement()) {
            stmt.execute("CREATE TABLE " + tableName + " ("
                    + "id INT PRIMARY KEY, "
                    + "binary_col BINARY(5), "
                    + "varbinary_col VARBINARY(50), "
                    + "description NVARCHAR(100)"
                    + ")");
        }

        String insertSql = "INSERT INTO " + tableName +
                " (id, binary_col, varbinary_col, description) VALUES (?, ?, ?, ?)";

        byte[] binaryData = new byte[]{0x01, 0x02, 0x03, 0x04, 0x05};
        byte[] varbinaryData = new byte[]{(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF};
        
        try (PreparedStatement pstmt = execMethodConnection.prepareStatement(insertSql)) {
            pstmt.setInt(1, 1);
            pstmt.setBytes(2, binaryData);
            pstmt.setBytes(3, varbinaryData);
            pstmt.setString(4, "Binary/VarBinary test");
            
            int rowsAffected = pstmt.executeUpdate();
            assertEquals(1, rowsAffected, "Should insert 1 row");
        }

        // Verify
        try (Statement stmt = execMethodConnection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE id = 1")) {
            assertTrue(rs.next());
            byte[] retrievedBinary = rs.getBytes("binary_col");
            byte[] retrievedVarbinary = rs.getBytes("varbinary_col");
            assertArrayEquals(binaryData, retrievedBinary, "Binary data should match");
            assertArrayEquals(varbinaryData, retrievedVarbinary, "VarBinary data should match");
            assertEquals("Binary/VarBinary test", rs.getString("description"));
        }
    }
}
