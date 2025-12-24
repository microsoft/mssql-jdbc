/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;

/**
 * Test class for SQLServerPreparedStatement temporary table operations.
 * This class is in the same package as SQLServerPreparedStatement to access package-level methods.
 */
@RunWith(JUnitPlatform.class)
public class SQLServerPreparedStatementTempTableTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /**
     * Test the containsTemporaryTableOperations method for detecting temp table operations in SQL.
     */
    @Test
    public void testContainsTemporaryTableOperations() throws Exception {
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            String sql = "SELECT 1"; // Simple SQL to create a statement
            
            try (SQLServerPreparedStatement stmt = (SQLServerPreparedStatement) con.prepareStatement(sql)) {
                // Test case: SQL with temporary table creation should return true
                String tempTableSql = "CREATE TABLE #tempTable (id INT, name NVARCHAR(50))";
                boolean result = stmt.containsTemporaryTableOperations(tempTableSql);
                assertTrue(result, "Should detect temporary table operations in CREATE TABLE statement");
                
                // Test case: Regular SQL without temp tables should return false
                String regularSql = "SELECT * FROM users WHERE id = 1";
                result = stmt.containsTemporaryTableOperations(regularSql);
                assertTrue(!result, "Should not detect temporary table operations in regular SELECT statement");
                
                // Test case: INSERT into temp table
                String insertTempSql = "INSERT INTO #tempTable VALUES (1, 'test')";
                result = stmt.containsTemporaryTableOperations(insertTempSql);
                assertTrue(result, "Should detect temporary table operations in INSERT statement");
                
                // Test case: UPDATE temp table
                String updateTempSql = "UPDATE #tempTable SET name = 'updated' WHERE id = 1";
                result = stmt.containsTemporaryTableOperations(updateTempSql);
                assertTrue(result, "Should detect temporary table operations in UPDATE statement");
                
                // Test case: DELETE from temp table
                String deleteTempSql = "DELETE FROM #tempTable WHERE id = 1";
                result = stmt.containsTemporaryTableOperations(deleteTempSql);
                assertTrue(result, "Should detect temporary table operations in DELETE statement");
                
                // Test case: Bracketed temp table identifier with special characters (single quote, hyphens, GUID)
                String bracketedTempTableSql = "CREATE TABLE [#tempConstraintTable_jdbc_'2242d9a9-74f3-4643-9f9e-f91015343592] (C1 int check (C1 > 0));INSERT INTO [tempConstraintTable_jdbc_'2242d9a9-74f3-4643-9f9e-f91015343592] VALUES (?)";
                result = stmt.containsTemporaryTableOperations(bracketedTempTableSql);
                assertTrue(result, "Should detect temporary table operations in bracketed temp table identifier with special characters");
            }
        }
    }

    /**
     * Test complex scenarios with prepareMethod=scopeTempTablesToConnection including
     * multiple SQL statements with and without temporary tables.
     */
    @Test
    public void testScopeTempTablesToConnectionComplexScenarios() throws Exception {
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            // Set the prepare method to scopeTempTablesToConnection
            con.setPrepareMethod("scopeTempTablesToConnection");
            
            // Use Case 1: Multiple SQL statements WITH temporary tables
            String createTempTableSql = "CREATE TABLE #testTemp (id INT, value NVARCHAR(50))";
            String insertTempTableSql = "INSERT INTO #testTemp (id, value) VALUES (?, ?)";
            String selectTempTableSql = "SELECT id, value FROM #testTemp WHERE id = ?";
            String updateTempTableSql = "UPDATE #testTemp SET value = ? WHERE id = ?";
            String deleteTempTableSql = "DELETE FROM #testTemp WHERE id = ?";
            
            // Test CREATE TABLE with temp table
            try (SQLServerPreparedStatement createStmt = (SQLServerPreparedStatement) con.prepareStatement(createTempTableSql)) {
                boolean hasTemporaryTables = createStmt.containsTemporaryTableOperations(createTempTableSql);
                assertTrue(hasTemporaryTables, "Should detect temporary table operations in CREATE TABLE statement");
                createStmt.execute();
            }
            
            // Test INSERT into temp table
            try (SQLServerPreparedStatement insertStmt = (SQLServerPreparedStatement) con.prepareStatement(insertTempTableSql)) {
                boolean hasTemporaryTables = insertStmt.containsTemporaryTableOperations(insertTempTableSql);
                assertTrue(hasTemporaryTables, "Should detect temporary table operations in INSERT statement");
                insertStmt.setInt(1, 1);
                insertStmt.setString(2, "test value");
                insertStmt.executeUpdate();
            }
            
            // Test SELECT from temp table
            try (SQLServerPreparedStatement selectStmt = (SQLServerPreparedStatement) con.prepareStatement(selectTempTableSql)) {
                boolean hasTemporaryTables = selectStmt.containsTemporaryTableOperations(selectTempTableSql);
                assertTrue(hasTemporaryTables, "Should detect temporary table operations in SELECT statement");
                selectStmt.setInt(1, 1);
                boolean hasResultSet = selectStmt.execute();
                assertTrue(hasResultSet, "Should return true indicating a ResultSet is available");
            }
            
            // Test UPDATE temp table
            try (SQLServerPreparedStatement updateStmt = (SQLServerPreparedStatement) con.prepareStatement(updateTempTableSql)) {
                boolean hasTemporaryTables = updateStmt.containsTemporaryTableOperations(updateTempTableSql);
                assertTrue(hasTemporaryTables, "Should detect temporary table operations in UPDATE statement");
                updateStmt.setString(1, "updated value");
                updateStmt.setInt(2, 1);
                updateStmt.executeUpdate();
            }
            
            // Test DELETE from temp table
            try (SQLServerPreparedStatement deleteStmt = (SQLServerPreparedStatement) con.prepareStatement(deleteTempTableSql)) {
                boolean hasTemporaryTables = deleteStmt.containsTemporaryTableOperations(deleteTempTableSql);
                assertTrue(hasTemporaryTables, "Should detect temporary table operations in DELETE statement");
                deleteStmt.setInt(1, 1);
                deleteStmt.executeUpdate();
            }
            
            // Use Case 2: Multiple SQL statements WITHOUT temporary tables (fallback behavior)
            String regularSelectSql = "SELECT 1 AS result";
            String regularInsertSql = "SELECT ? AS param_value";
            String regularUpdateSql = "SELECT GETDATE() AS current_datetime";
            
            // Test regular SELECT
            try (SQLServerPreparedStatement regularSelectStmt = (SQLServerPreparedStatement) con.prepareStatement(regularSelectSql)) {
                boolean hasTemporaryTables = regularSelectStmt.containsTemporaryTableOperations(regularSelectSql);
                assertTrue(!hasTemporaryTables, "Should not detect temporary table operations in regular SELECT statement");
                boolean hasResultSet = regularSelectStmt.execute();
                assertTrue(hasResultSet, "Should return true indicating a ResultSet is available");
            }
            
            // Test regular parameterized query
            try (SQLServerPreparedStatement regularParamStmt = (SQLServerPreparedStatement) con.prepareStatement(regularInsertSql)) {
                boolean hasTemporaryTables = regularParamStmt.containsTemporaryTableOperations(regularInsertSql);
                assertTrue(!hasTemporaryTables, "Should not detect temporary table operations in parameterized SELECT statement");
                regularParamStmt.setString(1, "test parameter");
                boolean hasResultSet = regularParamStmt.execute();
                assertTrue(hasResultSet, "Should return true indicating a ResultSet is available");
            }
            
            // Test regular function call
            try (SQLServerPreparedStatement regularFuncStmt = (SQLServerPreparedStatement) con.prepareStatement(regularUpdateSql)) {
                boolean hasTemporaryTables = regularFuncStmt.containsTemporaryTableOperations(regularUpdateSql);
                assertTrue(!hasTemporaryTables, "Should not detect temporary table operations in function call statement");
                boolean hasResultSet = regularFuncStmt.execute();
                assertTrue(hasResultSet, "Should return true indicating a ResultSet is available");
            }
        }
    }

    /**
     * Test batch operations with prepareMethod=scopeTempTablesToConnection for both
     * temporary table and non-temporary table scenarios.
     */
    @Test
    public void testScopeTempTablesToConnectionBatchOperations() throws Exception {
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            // Set the prepare method to scopeTempTablesToConnection
            con.setPrepareMethod("scopeTempTablesToConnection");
            
            // Batch Scenario 1: WITH temporary tables
            String createTempSql = "CREATE TABLE #batchTemp (id INT, value NVARCHAR(50))";
            String insertTempSql = "INSERT INTO #batchTemp (id, value) VALUES (?, ?)";
            
            // Create temp table first
            try (SQLServerPreparedStatement createStmt = (SQLServerPreparedStatement) con.prepareStatement(createTempSql)) {
                boolean hasTemporaryTables = createStmt.containsTemporaryTableOperations(createTempSql);
                assertTrue(hasTemporaryTables, "Should detect temporary table operations in CREATE statement");
                createStmt.execute();
            }
            
            // Test batch insert into temp table
            try (SQLServerPreparedStatement batchTempStmt = (SQLServerPreparedStatement) con.prepareStatement(insertTempSql)) {
                boolean hasTemporaryTables = batchTempStmt.containsTemporaryTableOperations(insertTempSql);
                assertTrue(hasTemporaryTables, "Should detect temporary table operations in batch INSERT statement");
                
                // Add multiple entries to batch
                batchTempStmt.setInt(1, 1);
                batchTempStmt.setString(2, "batch value 1");
                batchTempStmt.addBatch();
                
                batchTempStmt.setInt(1, 2);
                batchTempStmt.setString(2, "batch value 2");
                batchTempStmt.addBatch();
                
                batchTempStmt.setInt(1, 3);
                batchTempStmt.setString(2, "batch value 3");
                batchTempStmt.addBatch();
                
                // Execute batch
                int[] updateCounts = batchTempStmt.executeBatch();
                assertTrue(updateCounts.length == 3, "Should have 3 batch results");
                for (int count : updateCounts) {
                    assertTrue(count == 1, "Each batch operation should affect 1 row");
                }
            }
            
            // Batch Scenario 2: WITHOUT temporary tables (fallback behavior)
            String createRegularSql = "CREATE TABLE regularBatchTest (id INT, value NVARCHAR(50))";
            String insertRegularSql = "INSERT INTO regularBatchTest (id, value) VALUES (?, ?)";
            String dropRegularSql = "DROP TABLE regularBatchTest";
            
            // Create regular table first
            try (SQLServerPreparedStatement createStmt = (SQLServerPreparedStatement) con.prepareStatement(createRegularSql)) {
                boolean hasTemporaryTables = createStmt.containsTemporaryTableOperations(createRegularSql);
                assertTrue(!hasTemporaryTables, "Should not detect temporary table operations in CREATE regular table statement");
                createStmt.execute();
            }
            
            // Test batch insert into regular table
            try (SQLServerPreparedStatement batchRegularStmt = (SQLServerPreparedStatement) con.prepareStatement(insertRegularSql)) {
                boolean hasTemporaryTables = batchRegularStmt.containsTemporaryTableOperations(insertRegularSql);
                assertTrue(!hasTemporaryTables, "Should not detect temporary table operations in batch INSERT to regular table");
                
                // Add multiple entries to batch
                batchRegularStmt.setInt(1, 10);
                batchRegularStmt.setString(2, "regular batch value 1");
                batchRegularStmt.addBatch();
                
                batchRegularStmt.setInt(1, 20);
                batchRegularStmt.setString(2, "regular batch value 2");
                batchRegularStmt.addBatch();
                
                batchRegularStmt.setInt(1, 30);
                batchRegularStmt.setString(2, "regular batch value 3");
                batchRegularStmt.addBatch();
                
                // Execute batch
                int[] updateCounts = batchRegularStmt.executeBatch();
                assertTrue(updateCounts.length == 3, "Should have 3 batch results");
                for (int count : updateCounts) {
                    assertTrue(count == 1, "Each batch operation should affect 1 row");
                }
            }
            
            // Clean up - drop regular table
            try (SQLServerPreparedStatement dropStmt = (SQLServerPreparedStatement) con.prepareStatement(dropRegularSql)) {
                boolean hasTemporaryTables = dropStmt.containsTemporaryTableOperations(dropRegularSql);
                assertTrue(!hasTemporaryTables, "Should not detect temporary table operations in DROP regular table statement");
                dropStmt.execute();
            }
        }
    }

    /**
     * Test batch execution with SQL that contains both non-parameterized prefix (CREATE TABLE)
     * and parameterized part (INSERT with ?). This tests the fix for ensuring CREATE TABLE
     * only executes once, not repeated for each batch item.
     */
    @Test
    public void testBatchWithCreateTablePrefixAndParameterizedInsert() throws Exception {
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            // Set the prepare method to scopeTempTablesToConnection to trigger combined batch execution
            con.setPrepareMethod("scopeTempTablesToConnection");
            
            // Test Scenario 1: Simple temp table name with CREATE TABLE + INSERT
            String simpleTempTableSql = "CREATE TABLE #batchCreateTest (id INT); INSERT INTO #batchCreateTest (id) VALUES (?)";
            
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(simpleTempTableSql)) {
                // Verify temp table detection works
                boolean hasTemporaryTables = pstmt.containsTemporaryTableOperations(simpleTempTableSql);
                assertTrue(hasTemporaryTables, "Should detect temporary table operations in combined CREATE+INSERT statement");
                
                // Add multiple batch entries
                pstmt.setInt(1, 100);
                pstmt.addBatch();
                
                pstmt.setInt(1, 200);
                pstmt.addBatch();
                
                pstmt.setInt(1, 300);
                pstmt.addBatch();
                
                // Execute batch - CREATE TABLE should only execute once
                int[] updateCounts = pstmt.executeBatch();
                assertTrue(updateCounts.length == 3, "Should have 3 batch results for INSERT operations");
                for (int count : updateCounts) {
                    assertTrue(count == 1, "Each INSERT operation should affect 1 row");
                }
            }
            
            // Test Scenario 2: Bracketed temp table name with special characters
            String bracketedTempTableSql = "CREATE TABLE [#batchTest_'special-chars] (C1 INT CHECK (C1 > 0)); INSERT INTO [#batchTest_'special-chars] (C1) VALUES (?)";
            
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(bracketedTempTableSql)) {
                // Verify temp table detection works with bracketed identifiers
                boolean hasTemporaryTables = pstmt.containsTemporaryTableOperations(bracketedTempTableSql);
                assertTrue(hasTemporaryTables, "Should detect temporary table operations with bracketed identifier");
                
                // Add multiple batch entries
                pstmt.setInt(1, 10);
                pstmt.addBatch();
                
                pstmt.setInt(1, 20);
                pstmt.addBatch();
                
                pstmt.setInt(1, 30);
                pstmt.addBatch();
                
                // Execute batch
                int[] updateCounts = pstmt.executeBatch();
                assertTrue(updateCounts.length == 3, "Should have 3 batch results");
                for (int count : updateCounts) {
                    assertTrue(count == 1, "Each batch operation should affect 1 row");
                }
            }
        }
    }

    /**
     * Test batch execution with constraint violations when SQL contains CREATE TABLE prefix.
     * This ensures that constraint violations are properly handled and update counts reflect
     * which batch items succeeded or failed.
     */
    @Test
    public void testBatchWithCreateTablePrefixAndConstraintViolation() throws Exception {
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            // Set the prepare method to scopeTempTablesToConnection
            con.setPrepareMethod("scopeTempTablesToConnection");
            
            // Create SQL with CHECK constraint: C1 > 0
            String constraintSql = "CREATE TABLE #constraintBatchTest (C1 INT CHECK (C1 > 0)); INSERT INTO #constraintBatchTest (C1) VALUES (?)";
            
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(constraintSql)) {
                // Verify temp table detection
                boolean hasTemporaryTables = pstmt.containsTemporaryTableOperations(constraintSql);
                assertTrue(hasTemporaryTables, "Should detect temporary table operations");
                
                // Add batch items: 1st valid, 2nd violates constraint, 3rd valid, 4th valid
                pstmt.setInt(1, 1);  // Valid: 1 > 0
                pstmt.addBatch();
                
                pstmt.setInt(1, -1); // Violates constraint: -1 is not > 0
                pstmt.addBatch();
                
                pstmt.setInt(1, 2);  // Valid: 2 > 0
                pstmt.addBatch();
                
                pstmt.setInt(1, 3);  // Valid: 3 > 0
                pstmt.addBatch();
                
                try {
                    pstmt.executeBatch();
                    assertTrue(false, "Should have thrown exception due to constraint violation");
                } catch (SQLServerException sse) {
                    // Expected exception - the BatchUpdateException is wrapped in SQLServerException
                    // The SQLServerException's cause might be another SQLServerException or BatchUpdateException
                    Throwable cause = sse.getCause();
                    
                    // Drill down to find BatchUpdateException
                    while (cause != null && !(cause instanceof java.sql.BatchUpdateException)) {
                        cause = cause.getCause();
                    }
                    
                    assertTrue(cause instanceof java.sql.BatchUpdateException,
                            "Should contain BatchUpdateException in cause chain");
                    
                    java.sql.BatchUpdateException bue = (java.sql.BatchUpdateException) cause;
                    int[] updateCounts = bue.getUpdateCounts();
                    assertTrue(updateCounts.length == 4, "Should have 4 update counts");
                    
                    // First insert should succeed
                    assertTrue(updateCounts[0] == 1, "First batch item should succeed (count=1)");
                    
                    // Second insert should fail (constraint violation)
                    assertTrue(updateCounts[1] == java.sql.Statement.EXECUTE_FAILED, 
                            "Second batch item should fail (EXECUTE_FAILED)");
                    
                    // Third and fourth inserts depend on SQL Server behavior
                    // They might fail if SQL Server aborts the batch, or succeed if it continues
                    // Just verify they have valid status codes
                    assertTrue(updateCounts[2] >= java.sql.Statement.EXECUTE_FAILED, 
                            "Third batch item should have valid status");
                    assertTrue(updateCounts[3] >= java.sql.Statement.EXECUTE_FAILED, 
                            "Fourth batch item should have valid status");
                    
                    // Verify exception message mentions constraint
                    assertTrue(bue.getMessage().contains("CHECK") || bue.getMessage().contains("constraint") 
                            || bue.getMessage().contains("violated"),
                            "Exception message should mention CHECK constraint violation");
                }
            }
        }
    }
}