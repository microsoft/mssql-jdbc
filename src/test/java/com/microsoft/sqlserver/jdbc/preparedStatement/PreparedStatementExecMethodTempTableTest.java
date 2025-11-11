/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.*;

import org.junit.jupiter.api.*;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.testframework.AbstractTest;


/**
 * E2E tests for PreparedStatement using EXEC method with temp tables.
 * This tests the SqlServerPreparedStatementExpander without requiring persistent tables.
 */
public class PreparedStatementExecMethodTempTableTest extends AbstractTest {

    @BeforeAll
    public static void setupTest() throws Exception {
        setConnection();
    }

    @Test
    public void testInsertWithExecMethodAndTempTable() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        ds.setPrepareMethod("exec");
        
        try (SQLServerConnection execConn = (SQLServerConnection) ds.getConnection()) {
            // Verify the prepare method is set
            assertEquals("exec", execConn.getPrepareMethod(), "PrepareMethod should be 'exec'");
            
            // Create a temp table (automatically dropped when connection closes)
            try (Statement stmt = execConn.createStatement()) {
                stmt.execute("CREATE TABLE #TempTest (id INT, name NVARCHAR(50), age INT)");
            }
            
            // Insert using prepared statement with exec method
            String insertSql = "INSERT INTO #TempTest (id, name, age) VALUES (?, ?, ?)";
            try (PreparedStatement pstmt = execConn.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setString(2, "Test User");
                pstmt.setInt(3, 30);
                
                int rowsAffected = pstmt.executeUpdate();
                assertEquals(1, rowsAffected, "Should insert 1 row");
            }
            
            // Verify the insert
            try (Statement stmt = execConn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM #TempTest WHERE id = 1")) {
                assertTrue(rs.next(), "Should find the inserted row");
                assertEquals("Test User", rs.getString("name"));
                assertEquals(30, rs.getInt("age"));
            }
        }
    }

    @Test
    public void testSelectWithNullComparison() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        ds.setPrepareMethod("exec");
        
        try (SQLServerConnection execConn = (SQLServerConnection) ds.getConnection()) {
            // Create temp table and insert test data
            try (Statement stmt = execConn.createStatement()) {
                stmt.execute("CREATE TABLE #NullTest (id INT, manager_id INT NULL)");
                stmt.execute("INSERT INTO #NullTest (id, manager_id) VALUES (1, NULL)");
                stmt.execute("INSERT INTO #NullTest (id, manager_id) VALUES (2, 100)");
            }
            
            // Test SELECT with = NULL (should be rewritten to IS NULL)
            String selectSql = "SELECT * FROM #NullTest WHERE manager_id = ?";
            try (PreparedStatement pstmt = execConn.prepareStatement(selectSql)) {
                pstmt.setNull(1, Types.INTEGER);
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next(), "Should find row where manager_id IS NULL");
                    assertEquals(1, rs.getInt("id"));
                    assertFalse(rs.next(), "Should only find one NULL row");
                }
            }
            
            // Test SELECT with != NULL (should be rewritten to IS NOT NULL)
            String selectSql2 = "SELECT * FROM #NullTest WHERE manager_id != ?";
            try (PreparedStatement pstmt = execConn.prepareStatement(selectSql2)) {
                pstmt.setNull(1, Types.INTEGER);
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next(), "Should find row where manager_id IS NOT NULL");
                    assertEquals(2, rs.getInt("id"));
                    assertEquals(100, rs.getInt("manager_id"));
                    assertFalse(rs.next(), "Should only find one NOT NULL row");
                }
            }
        }
    }

    @Test
    public void testStringWithQuotesInExecMethod() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        ds.setPrepareMethod("exec");
        
        try (SQLServerConnection execConn = (SQLServerConnection) ds.getConnection()) {
            // Create temp table
            try (Statement stmt = execConn.createStatement()) {
                stmt.execute("CREATE TABLE #QuoteTest (id INT, description NVARCHAR(100))");
            }
            
            // Insert string with quotes
            String insertSql = "INSERT INTO #QuoteTest (id, description) VALUES (?, ?)";
            try (PreparedStatement pstmt = execConn.prepareStatement(insertSql)) {
                pstmt.setInt(1, 1);
                pstmt.setString(2, "O'Brien's description");
                
                int rowsAffected = pstmt.executeUpdate();
                assertEquals(1, rowsAffected, "Should insert 1 row");
            }
            
            // Verify the insert
            try (Statement stmt = execConn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM #QuoteTest WHERE id = 1")) {
                assertTrue(rs.next(), "Should find the inserted row");
                assertEquals("O'Brien's description", rs.getString("description"));
            }
        }
    }

    @Test
    public void testMultipleParametersWithMixedTypes() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        ds.setPrepareMethod("exec");
        
        try (SQLServerConnection execConn = (SQLServerConnection) ds.getConnection()) {
            // Create temp table
            try (Statement stmt = execConn.createStatement()) {
                stmt.execute("CREATE TABLE #MixedTest (id INT, name NVARCHAR(50), salary DECIMAL(10,2), is_active BIT, dept_id INT NULL)");
                stmt.execute("INSERT INTO #MixedTest VALUES (1, 'Alice', 50000.00, 1, NULL)");
                stmt.execute("INSERT INTO #MixedTest VALUES (2, 'Bob', 60000.00, 1, 10)");
                stmt.execute("INSERT INTO #MixedTest VALUES (3, 'Charlie', 55000.00, 0, 20)");
            }
            
            // Test complex WHERE with multiple conditions including NULL
            String selectSql = "SELECT * FROM #MixedTest WHERE is_active = ? AND salary > ? AND dept_id = ?";
            try (PreparedStatement pstmt = execConn.prepareStatement(selectSql)) {
                pstmt.setBoolean(1, true);
                pstmt.setBigDecimal(2, new java.math.BigDecimal("45000.00"));
                pstmt.setNull(3, Types.INTEGER);
                
                try (ResultSet rs = pstmt.executeQuery()) {
                    assertTrue(rs.next(), "Should find matching row");
                    assertEquals("Alice", rs.getString("name"));
                    assertFalse(rs.next(), "Should only find one matching row");
                }
            }
        }
    }

    @Test
    public void testUpdateWithNullValue() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        ds.setPrepareMethod("exec");
        
        try (SQLServerConnection execConn = (SQLServerConnection) ds.getConnection()) {
            // Create temp table and insert test data
            try (Statement stmt = execConn.createStatement()) {
                stmt.execute("CREATE TABLE #UpdateTest (id INT, notes NVARCHAR(100))");
                stmt.execute("INSERT INTO #UpdateTest VALUES (1, 'Original notes')");
            }
            
            // Update to NULL
            String updateSql = "UPDATE #UpdateTest SET notes = ? WHERE id = ?";
            try (PreparedStatement pstmt = execConn.prepareStatement(updateSql)) {
                pstmt.setNull(1, Types.NVARCHAR);
                pstmt.setInt(2, 1);
                
                int rowsAffected = pstmt.executeUpdate();
                assertEquals(1, rowsAffected, "Should update 1 row");
            }
            
            // Verify the update
            try (Statement stmt = execConn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM #UpdateTest WHERE id = 1")) {
                assertTrue(rs.next(), "Should find the updated row");
                assertNull(rs.getString("notes"), "Notes should be NULL");
            }
        }
    }

    @Test
    public void testDeleteWithNullCondition() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        ds.setPrepareMethod("exec");
        
        try (SQLServerConnection execConn = (SQLServerConnection) ds.getConnection()) {
            // Create temp table and insert test data
            try (Statement stmt = execConn.createStatement()) {
                stmt.execute("CREATE TABLE #DeleteTest (id INT, category NVARCHAR(50) NULL)");
                stmt.execute("INSERT INTO #DeleteTest VALUES (1, NULL)");
                stmt.execute("INSERT INTO #DeleteTest VALUES (2, 'Active')");
            }
            
            // Delete rows where category = NULL (should use IS NULL)
            String deleteSql = "DELETE FROM #DeleteTest WHERE category = ?";
            try (PreparedStatement pstmt = execConn.prepareStatement(deleteSql)) {
                pstmt.setNull(1, Types.NVARCHAR);
                
                int rowsAffected = pstmt.executeUpdate();
                assertEquals(1, rowsAffected, "Should delete 1 row with NULL category");
            }
            
            // Verify only non-NULL row remains
            try (Statement stmt = execConn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM #DeleteTest")) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "Should have 1 row remaining");
            }
        }
    }
}
