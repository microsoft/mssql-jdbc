/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;

/**
 * Test class for combined batch execution with complex SQL scenarios.
 * Tests the doExecuteExecMethodBatchCombined method which handles:
 * - Multiple parameterized statements
 * - Non-parameterized statements before, after, or between parameterized statements
 * - Complex interleaving of parameterized and non-parameterized statements
 */
@RunWith(JUnitPlatform.class)
public class BatchCombinedExecutionTest extends AbstractTest {

    private static final String TEST_TABLE_1 = "BatchCombinedTest_Table1";
    private static final String TEST_TABLE_2 = "BatchCombinedTest_Table2";
    private static final String TEMP_TABLE = "#BatchCombinedTest_Temp";

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            // Drop tables if they exist
            try {
                stmt.execute("DROP TABLE IF EXISTS " + TEST_TABLE_1);
                stmt.execute("DROP TABLE IF EXISTS " + TEST_TABLE_2);
                stmt.execute("DROP TABLE IF EXISTS " + TEMP_TABLE);
            } catch (SQLException e) {
                // Ignore if tables don't exist
            }

            // Create test tables
            stmt.execute("CREATE TABLE " + TEST_TABLE_1 + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)");
            stmt.execute("CREATE TABLE " + TEST_TABLE_2 + " (id INT PRIMARY KEY, description NVARCHAR(100))");
        }
    }

    @AfterAll
    public static void cleanupTests() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS " + TEST_TABLE_1);
            stmt.execute("DROP TABLE IF EXISTS " + TEST_TABLE_2);
            stmt.execute("DROP TABLE IF EXISTS " + TEMP_TABLE);
        }
    }

    /**
     * Test Case 1: Non-parameterized SQL after parameterized SQL
     * SQL: "INSERT INTO table1 VALUES (?); SET NOCOUNT OFF"
     */
    @Test
    public void testNonParameterizedAfterParameterized() throws Exception {
        try (Connection conn = getConnection()) {
            // Enable scopeTempTablesToConnection to trigger the combined execution path
            conn.unwrap(SQLServerConnection.class).setDisableStatementPooling(false);
            
            // Create temp table first
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + TEMP_TABLE + " (id INT)");
            }
            
            String sql = "INSERT INTO " + TEMP_TABLE + " VALUES (?); " +
                         "SET NOCOUNT OFF";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                // Add multiple batches
                pstmt.setInt(1, 100);
                pstmt.addBatch();
                
                pstmt.setInt(1, 200);
                pstmt.addBatch();
                
                pstmt.setInt(1, 300);
                pstmt.addBatch();

                int[] updateCounts = pstmt.executeBatch();
                
                // Verify batch execution succeeded
                assertEquals(3, updateCounts.length);
                for (int count : updateCounts) {
                    assertTrue(count >= 0, "Update count should be non-negative");
                }
            }

            // Verify data was inserted correctly
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TEMP_TABLE)) {
                rs.next();
                assertEquals(3, rs.getInt(1), "Should have inserted 3 rows");
            }
        }
    }

    /**
     * Test Case 2: Multiple parameterized statements
     * SQL: "INSERT INTO table1 VALUES (?); INSERT INTO table2 VALUES (?)"
     */
    @Test
    public void testMultipleParameterizedStatements() throws Exception {
        try (Connection conn = getConnection()) {
            conn.unwrap(SQLServerConnection.class).setDisableStatementPooling(false);
            
            // Create temp table first
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + TEMP_TABLE + " (id INT)");
            }
            
            String sql = "INSERT INTO " + TEMP_TABLE + " VALUES (?); " +
                         "INSERT INTO " + TEST_TABLE_1 + " VALUES (?, ?, ?)";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                // Batch 1
                pstmt.setInt(1, 1);  // First INSERT param
                pstmt.setInt(2, 101); // Second INSERT - id
                pstmt.setString(3, "Test1"); // Second INSERT - name
                pstmt.setInt(4, 10); // Second INSERT - value
                pstmt.addBatch();
                
                // Batch 2
                pstmt.setInt(1, 2);
                pstmt.setInt(2, 102);
                pstmt.setString(3, "Test2");
                pstmt.setInt(4, 20);
                pstmt.addBatch();

                int[] updateCounts = pstmt.executeBatch();
                assertEquals(2, updateCounts.length);
            }

            // Verify both tables have data
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TEMP_TABLE);
                rs.next();
                assertEquals(2, rs.getInt(1));
                
                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TEST_TABLE_1);
                rs.next();
                assertEquals(2, rs.getInt(1));
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DELETE FROM " + TEST_TABLE_1);
            }
        }
    }

    /**
     * Test Case 3: Complex interleaving - non-parameterized before, between, and after
     * SQL: "SET NOCOUNT ON; INSERT INTO table1 VALUES (?); INSERT INTO table2 VALUES (?); SELECT @@ROWCOUNT"
     */
    @Test
    public void testComplexInterleaving() throws Exception {
        try (Connection conn = getConnection()) {
            conn.unwrap(SQLServerConnection.class).setDisableStatementPooling(false);
            
            String sql = "SET NOCOUNT ON; " +
                         "CREATE TABLE " + TEMP_TABLE + " (id INT); " +
                         "INSERT INTO " + TEMP_TABLE + " VALUES (?); " +
                         "INSERT INTO " + TEST_TABLE_1 + " VALUES (?, ?, ?); " +
                         "SET NOCOUNT OFF";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                // Batch 1
                pstmt.setInt(1, 10);
                pstmt.setInt(2, 201);
                pstmt.setString(3, "Interleaved1");
                pstmt.setInt(4, 100);
                pstmt.addBatch();
                
                // Batch 2
                pstmt.setInt(1, 20);
                pstmt.setInt(2, 202);
                pstmt.setString(3, "Interleaved2");
                pstmt.setInt(4, 200);
                pstmt.addBatch();

                int[] updateCounts = pstmt.executeBatch();
                assertEquals(2, updateCounts.length);
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DELETE FROM " + TEST_TABLE_1);
            }
        }
    }

    /**
     * Test Case 4: Non-parameterized statement between parameterized statements
     * SQL: "INSERT INTO table1 VALUES (?); DELETE FROM temp; INSERT INTO table2 VALUES (?)"
     */
    @Test
    public void testNonParameterizedBetweenParameterized() throws Exception {
        try (Connection conn = getConnection()) {
            conn.unwrap(SQLServerConnection.class).setDisableStatementPooling(false);
            
            // Create and populate a temp table first
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("CREATE TABLE " + TEMP_TABLE + " (id INT)");
                stmt.execute("INSERT INTO " + TEMP_TABLE + " VALUES (999)");
            }

            String sql = "INSERT INTO " + TEST_TABLE_1 + " VALUES (?, ?, ?); " +
                         "DELETE FROM " + TEMP_TABLE + "; " +
                         "INSERT INTO " + TEST_TABLE_2 + " VALUES (?, ?)";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                // Batch 1
                pstmt.setInt(1, 301);
                pstmt.setString(2, "Before");
                pstmt.setInt(3, 1);
                pstmt.setInt(4, 401);
                pstmt.setString(5, "After");
                pstmt.addBatch();
                
                // Batch 2
                pstmt.setInt(1, 302);
                pstmt.setString(2, "Before2");
                pstmt.setInt(3, 2);
                pstmt.setInt(4, 402);
                pstmt.setString(5, "After2");
                pstmt.addBatch();

                int[] updateCounts = pstmt.executeBatch();
                assertEquals(2, updateCounts.length);
            }

            // Verify the DELETE was executed (temp table should be empty)
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TEMP_TABLE)) {
                rs.next();
                assertEquals(0, rs.getInt(1), "Temp table should be empty after DELETE");
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DELETE FROM " + TEST_TABLE_1);
                stmt.execute("DELETE FROM " + TEST_TABLE_2);
            }
        }
    }

    /**
     * Test Case 5: Multiple parameters per statement
     * SQL: "INSERT INTO table1 VALUES (?, ?); INSERT INTO table2 VALUES (?, ?, ?)"
     */
    @Test
    public void testMultipleParametersPerStatement() throws Exception {
        try (Connection conn = getConnection()) {
            conn.unwrap(SQLServerConnection.class).setDisableStatementPooling(false);
            
            String sql = "CREATE TABLE " + TEMP_TABLE + " (id INT); " +
                         "INSERT INTO " + TEST_TABLE_1 + " VALUES (?, ?, ?); " +
                         "INSERT INTO " + TEST_TABLE_2 + " VALUES (?, ?)";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                // Batch 1
                pstmt.setInt(1, 501);
                pstmt.setString(2, "Multi1");
                pstmt.setInt(3, 111);
                pstmt.setInt(4, 601);
                pstmt.setString(5, "Description1");
                pstmt.addBatch();
                
                // Batch 2
                pstmt.setInt(1, 502);
                pstmt.setString(2, "Multi2");
                pstmt.setInt(3, 222);
                pstmt.setInt(4, 602);
                pstmt.setString(5, "Description2");
                pstmt.addBatch();
                
                // Batch 3
                pstmt.setInt(1, 503);
                pstmt.setString(2, "Multi3");
                pstmt.setInt(3, 333);
                pstmt.setInt(4, 603);
                pstmt.setString(5, "Description3");
                pstmt.addBatch();

                int[] updateCounts = pstmt.executeBatch();
                assertEquals(3, updateCounts.length);
            }

            // Verify correct number of rows inserted in both tables
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TEST_TABLE_1);
                rs.next();
                assertEquals(3, rs.getInt(1), "Table1 should have 3 rows");
                
                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TEST_TABLE_2);
                rs.next();
                assertEquals(3, rs.getInt(1), "Table2 should have 3 rows");
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DELETE FROM " + TEST_TABLE_1);
                stmt.execute("DELETE FROM " + TEST_TABLE_2);
            }
        }
    }

    /**
     * Test Case 6: Only non-parameterized statements (edge case)
     */
    @Test
    public void testOnlyNonParameterizedStatements() throws Exception {
        try (Connection conn = getConnection()) {
            conn.unwrap(SQLServerConnection.class).setDisableStatementPooling(false);
            
            // This SQL has no parameters, but we still call addBatch
            String sql = "CREATE TABLE " + TEMP_TABLE + " (id INT); " +
                         "INSERT INTO " + TEMP_TABLE + " VALUES (1); " +
                         "SET NOCOUNT OFF";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.addBatch();
                pstmt.addBatch(); // Adding same batch twice
                pstmt.addBatch();

                int[] updateCounts = pstmt.executeBatch();
                assertEquals(3, updateCounts.length);
            }
        }
    }

    /**
     * Test Case 7: String literals and comments in SQL (ensure proper parsing)
     */
    @Test
    public void testWithStringLiteralsAndComments() throws Exception {
        try (Connection conn = getConnection()) {
            conn.unwrap(SQLServerConnection.class).setDisableStatementPooling(false);
            
            String sql = "/* Comment with ; semicolon */ " +
                         "CREATE TABLE " + TEMP_TABLE + " (id INT); " +
                         "INSERT INTO " + TEST_TABLE_1 + " VALUES (?, 'Name;with;semicolons', ?); " +
                         "-- Another comment\n" +
                         "INSERT INTO " + TEST_TABLE_2 + " VALUES (?, 'Desc;test')";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, 701);
                pstmt.setInt(2, 77);
                pstmt.setInt(3, 801);
                pstmt.addBatch();

                int[] updateCounts = pstmt.executeBatch();
                assertEquals(1, updateCounts.length);
            }

            // Verify data
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery(
                     "SELECT name, description FROM " + TEST_TABLE_1 + " t1 " +
                     "JOIN " + TEST_TABLE_2 + " t2 ON t1.id - 700 = t2.id - 800")) {
                assertTrue(rs.next());
                assertEquals("Name;with;semicolons", rs.getString(1));
                assertEquals("Desc;test", rs.getString(2));
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DELETE FROM " + TEST_TABLE_1);
                stmt.execute("DELETE FROM " + TEST_TABLE_2);
            }
        }
    }

    /**
     * Test Case 8: Verify correct parameter mapping for multiple parameterized statements
     */
    @Test
    public void testParameterMappingCorrectness() throws Exception {
        try (Connection conn = getConnection()) {
            conn.unwrap(SQLServerConnection.class).setDisableStatementPooling(false);
            
            String sql = "CREATE TABLE " + TEMP_TABLE + " (id INT); " +
                         "INSERT INTO " + TEST_TABLE_1 + " VALUES (?, ?, ?); " +
                         "INSERT INTO " + TEST_TABLE_2 + " VALUES (?, ?)";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                // Batch with specific values to verify correct mapping
                pstmt.setInt(1, 1001);
                pstmt.setString(2, "Param2");
                pstmt.setInt(3, 3);
                pstmt.setInt(4, 2001);
                pstmt.setString(5, "Param5");
                pstmt.addBatch();

                pstmt.executeBatch();
            }

            // Verify the exact values were inserted in correct tables
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(
                    "SELECT id, name, value FROM " + TEST_TABLE_1 + " WHERE id = 1001");
                assertTrue(rs.next());
                assertEquals(1001, rs.getInt("id"));
                assertEquals("Param2", rs.getString("name"));
                assertEquals(3, rs.getInt("value"));
                
                rs = stmt.executeQuery(
                    "SELECT id, description FROM " + TEST_TABLE_2 + " WHERE id = 2001");
                assertTrue(rs.next());
                assertEquals(2001, rs.getInt("id"));
                assertEquals("Param5", rs.getString("description"));
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DELETE FROM " + TEST_TABLE_1);
                stmt.execute("DELETE FROM " + TEST_TABLE_2);
            }
        }
    }

    /**
     * Test Case 9: Batch with errors - verify update counts reflect failures
     */
    @Test
    public void testBatchWithErrors() throws Exception {
        try (Connection conn = getConnection()) {
            conn.unwrap(SQLServerConnection.class).setDisableStatementPooling(false);
            
            String sql = "CREATE TABLE " + TEMP_TABLE + " (id INT); " +
                         "INSERT INTO " + TEST_TABLE_1 + " VALUES (?, ?, ?)";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                // Batch 1 - success
                pstmt.setInt(1, 9001);
                pstmt.setString(2, "Success");
                pstmt.setInt(3, 1);
                pstmt.addBatch();
                
                // Batch 2 - duplicate key error
                pstmt.setInt(1, 9001); // Same ID - will fail
                pstmt.setString(2, "Duplicate");
                pstmt.setInt(3, 2);
                pstmt.addBatch();
                
                // Batch 3 - would succeed but may not execute due to previous error
                pstmt.setInt(1, 9002);
                pstmt.setString(2, "After Error");
                pstmt.setInt(3, 3);
                pstmt.addBatch();

                try {
                    pstmt.executeBatch();
                    fail("Should have thrown BatchUpdateException");
                } catch (java.sql.BatchUpdateException e) {
                    int[] updateCounts = e.getUpdateCounts();
                    assertEquals(3, updateCounts.length);
                    // First batch should succeed
                    assertTrue(updateCounts[0] >= 0 || updateCounts[0] == Statement.SUCCESS_NO_INFO);
                    // Second batch should fail
                    assertEquals(Statement.EXECUTE_FAILED, updateCounts[1]);
                }
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DELETE FROM " + TEST_TABLE_1);
            }
        }
    }
}
