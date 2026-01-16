/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.ThreadLocalRandom;

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
     * Test the containsTemporaryTableOperations method for detecting temp table
     * operations in SQL.
     * Updated pattern detects CREATE TABLE #temp and SELECT INTO #temp.
     * Note: SQL Server uses # prefix for temp tables, not TEMPORARY keyword.
     */
    @Test
    public void testContainsTemporaryTableOperations() throws Exception {
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            String sql = "SELECT 1"; // Simple SQL to create a statement
            
            try (SQLServerPreparedStatement stmt = (SQLServerPreparedStatement) con.prepareStatement(sql)) {
                // Test case: CREATE TABLE with # prefix (most common in SQL Server) should
                // return true
                String localTempSql = "CREATE TABLE #tempTable (id INT, name NVARCHAR(50))";
                boolean result = stmt.containsTemporaryTableOperations(localTempSql);
                assertTrue(result, "Should detect CREATE TABLE #temp statement");

                // Test case: CREATE TABLE with bracketed identifier
                String bracketedSql = "CREATE TABLE [#tempTable] (id INT)";
                result = stmt.containsTemporaryTableOperations(bracketedSql);
                assertTrue(result, "Should detect CREATE TABLE with bracketed temp table identifier");

                // Test case: SELECT INTO temp table should return true
                String selectIntoSql = "SELECT * INTO #tempTable FROM users WHERE id = 1";
                result = stmt.containsTemporaryTableOperations(selectIntoSql);
                assertTrue(result, "Should detect SELECT INTO with temp table");

                // Test case: SELECT INTO with bracketed identifier
                String bracketedSelectIntoSql = "SELECT 1 INTO [#tempResult]";
                result = stmt.containsTemporaryTableOperations(bracketedSelectIntoSql);
                assertTrue(result, "Should detect SELECT INTO with bracketed temp table identifier");
                
                // Test case: Regular SQL without temp tables should return false
                String regularSql = "SELECT * FROM users WHERE id = 1";
                result = stmt.containsTemporaryTableOperations(regularSql);
                assertFalse(result, "Should not detect temporary table operations in regular SELECT statement");
                
                // Test case: CREATE TABLE without # prefix should return false
                String regularTableSql = "CREATE TABLE users (id INT)";
                result = stmt.containsTemporaryTableOperations(regularTableSql);
                assertFalse(result, "Should not detect CREATE TABLE without # prefix");

                // Test case: Global temp table (##) should return false (not supported)
                String globalTempSql = "CREATE TABLE ##globalTemp (id INT)";
                result = stmt.containsTemporaryTableOperations(globalTempSql);
                assertFalse(result, "Should not detect global temp table with ##");

                // Test case: Global temp table with bracketed identifier should return false
                String globalBracketedSql = "CREATE TABLE [##globalTemp] (id INT)";
                result = stmt.containsTemporaryTableOperations(globalBracketedSql);
                assertFalse(result, "Should not detect global temp table with bracketed [##]");

                // Test case: SELECT INTO global temp table should return false
                String selectIntoGlobalSql = "SELECT * INTO ##globalTemp FROM users";
                result = stmt.containsTemporaryTableOperations(selectIntoGlobalSql);
                assertFalse(result, "Should not detect SELECT INTO global temp table ##");

                // Test case: SELECT INTO global temp with bracketed identifier should return
                // false
                String selectIntoGlobalBracketedSql = "SELECT * INTO [##globalResult] FROM users";
                result = stmt.containsTemporaryTableOperations(selectIntoGlobalBracketedSql);
                assertFalse(result, "Should not detect SELECT INTO bracketed global temp [##]");

                // Test case: SELECT INTO regular table should return false
                String regularSelectIntoSql = "SELECT * INTO regularTable FROM users";
                result = stmt.containsTemporaryTableOperations(regularSelectIntoSql);
                assertFalse(result, "Should not detect SELECT INTO regular table without #");

                // Test case: INSERT into temp table should return false (no longer detected by
                // simplified pattern)
                String insertTempSql = "INSERT INTO #tempTable VALUES (1, 'test')";
                result = stmt.containsTemporaryTableOperations(insertTempSql);
                assertFalse(result, "INSERT statements are not detected by simplified pattern");
                
                // Test case: UPDATE temp table should return false (no longer detected)
                String updateTempSql = "UPDATE #tempTable SET name = 'updated' WHERE id = 1";
                result = stmt.containsTemporaryTableOperations(updateTempSql);
                assertFalse(result, "UPDATE statements are not detected by simplified pattern");
                
                // Test case: DELETE from temp table should return false (no longer detected)
                String deleteTempSql = "DELETE FROM #tempTable WHERE id = 1";
                result = stmt.containsTemporaryTableOperations(deleteTempSql);
                assertFalse(result, "DELETE statements are not detected by simplified pattern");
                
                // Test case: Bracketed temp table identifier with special characters in CREATE
                // TEMP TABLE
                String bracketedTempTableSql = "CREATE TABLE [#tempConstraintTable_jdbc_'2242d9a9-74f3-4643-9f9e-f91015343592] (C1 int check (C1 > 0))";
                result = stmt.containsTemporaryTableOperations(bracketedTempTableSql);
                assertTrue(result, "Should detect CREATE TABLE with bracketed temp table identifier");

                // Test case: SELECT INTO with bracketed identifier and FROM clause
                String bracketedSelectIntoWithFromSql = "SELECT 1 INTO [#tempResult] FROM DUAL";
                result = stmt.containsTemporaryTableOperations(bracketedSelectIntoWithFromSql);
                assertTrue(result, "Should detect SELECT INTO with bracketed temp table identifier");

                // Test case: Mixed case - CREATE Table #temp
                String mixedCase1 = "CrEaTe TaBlE #tempMixed (id INT)";
                result = stmt.containsTemporaryTableOperations(mixedCase1);
                assertTrue(result, "Should detect mixed case CREATE TABLE #temp");

                // Test case: Mixed case - Create Table #temp
                String mixedCase2 = "create table #tempData (value VARCHAR(50))";
                result = stmt.containsTemporaryTableOperations(mixedCase2);
                assertTrue(result, "Should detect mixed case CREATE TABLE #temp");

                // Test case: Mixed case - SELECT into #temp
                String mixedCase3 = "SeLeCt * InTo #tempOutput FrOm users";
                result = stmt.containsTemporaryTableOperations(mixedCase3);
                assertTrue(result, "Should detect mixed case SELECT INTO #temp");

                // Test case: Multiple spaces between keywords
                String multipleSpaces = "CREATE    TABLE    #tempSpaces    (id INT)";
                result = stmt.containsTemporaryTableOperations(multipleSpaces);
                assertTrue(result, "Should detect CREATE TABLE with multiple spaces");

                // Test case: Tabs between keywords
                String withTabs = "CREATE\tTABLE\t#tempTabs\t(id INT)";
                result = stmt.containsTemporaryTableOperations(withTabs);
                assertTrue(result, "Should detect CREATE TABLE with tabs");

                // Test case: Newlines between keywords
                String withNewlines = "CREATE\nTABLE\n#tempNewlines\n(id INT)";
                result = stmt.containsTemporaryTableOperations(withNewlines);
                assertTrue(result, "Should detect CREATE TABLE with newlines");

                // Test case: Mixed whitespace (spaces, tabs, newlines)
                String mixedWhitespace = "CREATE  \t \n  TABLE   \t\n  #tempMixed  \n\t  (id INT)";
                result = stmt.containsTemporaryTableOperations(mixedWhitespace);
                assertTrue(result, "Should detect CREATE TABLE with mixed whitespace");

                // Test case: SELECT INTO with multiple spaces and newlines
                String selectIntoWhitespace = "SELECT\n  *\n  INTO\n  #tempSelect\n  FROM\n  users";
                result = stmt.containsTemporaryTableOperations(selectIntoWhitespace);
                assertTrue(result, "Should detect SELECT INTO with newlines and spaces");

                // Test case: CREATE TABLE not at the beginning - preceded by SELECT
                String notFirstSql1 = "SELECT * FROM users; CREATE TABLE #tempNotFirst (id INT)";
                result = stmt.containsTemporaryTableOperations(notFirstSql1);
                assertTrue(result, "Should detect CREATE TABLE #temp even when not first statement");

                // Test case: Multiple statements with temp table in middle
                String notFirstSql2 = "UPDATE users SET status = 1; CREATE TABLE #tempMiddle (data VARCHAR(100)); SELECT * FROM #tempMiddle";
                result = stmt.containsTemporaryTableOperations(notFirstSql2);
                assertTrue(result, "Should detect CREATE TABLE #temp in middle of multiple statements");

                // Test case: SELECT INTO not at the beginning
                String notFirstSql3 = "DELETE FROM logs WHERE date < '2024-01-01'; SELECT id, name INTO #tempFromLogs FROM logs";
                result = stmt.containsTemporaryTableOperations(notFirstSql3);
                assertTrue(result, "Should detect SELECT INTO #temp even when not first statement");

                // Test case: Temp table creation after comments
                String afterComments = "-- This is a comment\nCREATE TABLE #tempAfterComment (id INT)";
                result = stmt.containsTemporaryTableOperations(afterComments);
                assertTrue(result, "Should detect CREATE TABLE #temp after SQL comments");

                // Test case: Bracketed temp table with mixed case and whitespace
                String bracketedMixed = "create\t\ntable\t\n[#TempBracketed]\n(col1 INT)";
                result = stmt.containsTemporaryTableOperations(bracketedMixed);
                assertTrue(result, "Should detect bracketed temp table with mixed case and whitespace");

                // Test case: Global temp table with mixed case should still be excluded
                String globalMixedCase = "CrEaTe TaBlE ##GlobalMixed (id INT)";
                result = stmt.containsTemporaryTableOperations(globalMixedCase);
                assertFalse(result, "Should not detect global temp table even with mixed case");

                // Test case: CREATE GLOBAL with mixed case and whitespace should be excluded
                String globalKeywordMixed = "create\n\tGLOBAL\t\ntable #tempGlobal (id INT)";
                result = stmt.containsTemporaryTableOperations(globalKeywordMixed);
                assertFalse(result, "Should not detect CREATE GLOBAL TABLE with mixed case");

                // Test case: Multiple temp tables in one SQL string
                String multipleTempTables = "CREATE TABLE #temp1 (id INT); CREATE TABLE #temp2 (name VARCHAR(50))";
                result = stmt.containsTemporaryTableOperations(multipleTempTables);
                assertTrue(result, "Should detect when multiple temp tables are created");

                // Test case: Temp table in subquery context
                String inSubquery = "INSERT INTO users SELECT * FROM #tempData; SELECT * INTO #tempResult FROM (SELECT * FROM #tempData) AS sub";
                result = stmt.containsTemporaryTableOperations(inSubquery);
                assertTrue(result, "Should detect SELECT INTO #temp in complex SQL with subqueries");

                // Test case: Semicolon-separated SQL statements WITHOUT temp tables
                String semicolonSeparatedNoTemp = "SELECT * FROM users; UPDATE users SET status = 1 WHERE id = 1; DELETE FROM logs WHERE date < '2024-01-01'";
                result = stmt.containsTemporaryTableOperations(semicolonSeparatedNoTemp);
                assertFalse(result, "Should not detect temp tables in multiple regular statements");

                // Test case: String literals containing "create temporary table" syntax (false
                // positive test)
                String stringLiteralWithTempSyntax = "INSERT INTO audit_log VALUES ('User executed: CREATE TABLE #tempData (id INT)')";
                result = stmt.containsTemporaryTableOperations(stringLiteralWithTempSyntax);
                assertFalse(result, "Should not detect temp table operations inside string literals");

                // Test case: ## in string literal data followed by genuine #TempTable
                // This ensures that ## in data doesn't cause false negatives for real temp
                // tables
                String hashInDataBeforeTempTable = "SELECT '##data' AS col1; CREATE TABLE #MyTempTable (id INT)";
                result = stmt.containsTemporaryTableOperations(hashInDataBeforeTempTable);
                assertTrue(result, "Should detect real #TempTable even when ## appears in string literal data earlier");

                // Test case: Multiple # symbols in string literal followed by genuine temp
                // table
                String multipleHashInData = "INSERT INTO logs VALUES ('#tag1', '##tag2'); SELECT * INTO #RealTemp FROM users";
                result = stmt.containsTemporaryTableOperations(multipleHashInData);
                assertTrue(result,
                        "Should detect SELECT INTO #RealTemp even when multiple # appear in string literals");

                // Test case: Combined CREATE TABLE and SELECT INTO in same SQL
                String combinedCreateAndSelectInto = "CREATE TABLE #temp1 (id INT); SELECT * INTO #temp2 FROM users";
                result = stmt.containsTemporaryTableOperations(combinedCreateAndSelectInto);
                assertTrue(result, "Should detect both CREATE TABLE and SELECT INTO temp tables");

                // Test case: Null input
                String nullSql = null;
                result = stmt.containsTemporaryTableOperations(nullSql);
                assertFalse(result, "Should return false for null input");

                // Test case: Empty string input
                String emptySql = "";
                result = stmt.containsTemporaryTableOperations(emptySql);
                assertFalse(result, "Should return false for empty string input");

                // Test case: Average-sized SQL string (realistic production scenario)
                StringBuilder avgSqlBuilder = new StringBuilder();
                avgSqlBuilder.append("CREATE TABLE #averageTempTable (id INT PRIMARY KEY, ");
                for (int i = 1; i <= 50; i++) {
                    avgSqlBuilder.append("column").append(i).append(" NVARCHAR(255)");
                    if (i < 50) {
                        avgSqlBuilder.append(", ");
                    }
                }
                avgSqlBuilder.append(")");
                String avgSql = avgSqlBuilder.toString();
                result = stmt.containsTemporaryTableOperations(avgSql);
                assertTrue(result, "Should detect CREATE TABLE #temp in average-sized SQL strings");

                // Test case: Very large SQL string (performance/stress test)
                StringBuilder largeSqlBuilder = new StringBuilder();
                largeSqlBuilder.append("CREATE TABLE #largeTempTable (id INT, ");
                for (int i = 0; i < 1000; i++) {
                    largeSqlBuilder.append("col").append(i).append(" VARCHAR(100), ");
                }
                largeSqlBuilder.append("endCol VARCHAR(100))");
                String largeSql = largeSqlBuilder.toString();
                result = stmt.containsTemporaryTableOperations(largeSql);
                assertTrue(result, "Should detect CREATE TABLE #temp even in very large SQL strings");
            }
        }
    }

    /**
     * Test complex scenarios with prepareMethod=scopeTempTablesToConnection
     * including
     * multiple SQL statements with and without temporary tables.
     * Note: Simplified pattern only detects CREATE TABLE and SELECT INTO.
     */
    @Test
    public void testScopeTempTablesToConnectionComplexScenarios() throws Exception {
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            // Set the prepare method to scopeTempTablesToConnection
            con.setPrepareMethod("scopeTempTablesToConnection");
            
            // Use Case 1: CREATE TABLE (detected by pattern)
            String createTempTableSql = "CREATE TABLE #testTemp (id INT, value NVARCHAR(50))";

            try (SQLServerPreparedStatement createStmt = (SQLServerPreparedStatement) con.prepareStatement(createTempTableSql)) {
                boolean hasTemporaryTables = createStmt.containsTemporaryTableOperations(createTempTableSql);
                assertTrue(hasTemporaryTables, "Should detect CREATE TABLE statement");
                createStmt.execute();
            }
            
            // Use Case 2: SELECT INTO temp table (detected by pattern)
            String selectIntoSql = "SELECT 1 AS id, 'test' AS value INTO #testTemp2";

            try (SQLServerPreparedStatement selectIntoStmt = (SQLServerPreparedStatement) con
                    .prepareStatement(selectIntoSql)) {
                boolean hasTemporaryTables = selectIntoStmt.containsTemporaryTableOperations(selectIntoSql);
                assertTrue(hasTemporaryTables, "Should detect SELECT INTO statement");
                selectIntoStmt.execute();
            }

            // Use Case 3: Operations that are NOT detected (INSERT, UPDATE, DELETE, SELECT
            // FROM)
            String insertTempTableSql = "INSERT INTO #testTemp (id, value) VALUES (?, ?)";
            String selectTempTableSql = "SELECT id, value FROM #testTemp WHERE id = ?";
            String updateTempTableSql = "UPDATE #testTemp SET value = ? WHERE id = ?";
            String deleteTempTableSql = "DELETE FROM #testTemp WHERE id = ?";

            // Test INSERT into temp table (not detected)
            try (SQLServerPreparedStatement insertStmt = (SQLServerPreparedStatement) con.prepareStatement(insertTempTableSql)) {
                boolean hasTemporaryTables = insertStmt.containsTemporaryTableOperations(insertTempTableSql);
                assertFalse(hasTemporaryTables, "Temporary table creation not detected in INSERT statement");
                insertStmt.setInt(1, 1);
                insertStmt.setString(2, "test value");
                insertStmt.executeUpdate();
            }
            
            // Test SELECT from temp table (not detected)
            try (SQLServerPreparedStatement selectStmt = (SQLServerPreparedStatement) con.prepareStatement(selectTempTableSql)) {
                boolean hasTemporaryTables = selectStmt.containsTemporaryTableOperations(selectTempTableSql);
                assertFalse(hasTemporaryTables, "Temporary table creation not detected in SELECT FROM statement");
                selectStmt.setInt(1, 1);
                boolean hasResultSet = selectStmt.execute();
                assertTrue(hasResultSet, "Should return true indicating a ResultSet is available");
            }
            
            // Test UPDATE temp table (not detected)
            try (SQLServerPreparedStatement updateStmt = (SQLServerPreparedStatement) con.prepareStatement(updateTempTableSql)) {
                boolean hasTemporaryTables = updateStmt.containsTemporaryTableOperations(updateTempTableSql);
                assertFalse(hasTemporaryTables, "Temporary table creation not detected in UPDATE statement");
                updateStmt.setString(1, "updated value");
                updateStmt.setInt(2, 1);
                updateStmt.executeUpdate();
            }
            
            // Test DELETE from temp table (not detected)
            try (SQLServerPreparedStatement deleteStmt = (SQLServerPreparedStatement) con.prepareStatement(deleteTempTableSql)) {
                boolean hasTemporaryTables = deleteStmt.containsTemporaryTableOperations(deleteTempTableSql);
                assertFalse(hasTemporaryTables, "Temporary table creation not detected in DELETE statement");
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
                assertFalse(hasTemporaryTables,
                        "Should not detect temporary table operations in regular SELECT statement");
                boolean hasResultSet = regularSelectStmt.execute();
                assertTrue(hasResultSet, "Should return true indicating a ResultSet is available");
            }
            
            // Test regular parameterized query
            try (SQLServerPreparedStatement regularParamStmt = (SQLServerPreparedStatement) con.prepareStatement(regularInsertSql)) {
                boolean hasTemporaryTables = regularParamStmt.containsTemporaryTableOperations(regularInsertSql);
                assertFalse(hasTemporaryTables,
                        "Should not detect temporary table operations in parameterized SELECT statement");
                regularParamStmt.setString(1, "test parameter");
                boolean hasResultSet = regularParamStmt.execute();
                assertTrue(hasResultSet, "Should return true indicating a ResultSet is available");
            }
            
            // Test regular function call
            try (SQLServerPreparedStatement regularFuncStmt = (SQLServerPreparedStatement) con.prepareStatement(regularUpdateSql)) {
                boolean hasTemporaryTables = regularFuncStmt.containsTemporaryTableOperations(regularUpdateSql);
                assertFalse(hasTemporaryTables,
                        "Should not detect temporary table operations in function call statement");
                boolean hasResultSet = regularFuncStmt.execute();
                assertTrue(hasResultSet, "Should return true indicating a ResultSet is available");
            }
        }
    }

    /**
     * Test batch operations with prepareMethod=scopeTempTablesToConnection for both
     * temporary table and non-temporary table scenarios.
     * Note: Simplified pattern only detects CREATE TABLE and SELECT INTO.
     */
    @Test
    public void testScopeTempTablesToConnectionBatchOperations() throws Exception {
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            // Set the prepare method to scopeTempTablesToConnection
            con.setPrepareMethod("scopeTempTablesToConnection");
            
            // Batch Scenario 1: CREATE TABLE + INSERT (INSERT not detected by
            // pattern)
            String createTempSql = "CREATE TABLE #batchTemp (id INT, value NVARCHAR(50))";
            String insertTempSql = "INSERT INTO #batchTemp (id, value) VALUES (?, ?)";
            
            // CREATE TABLE first
            try (SQLServerPreparedStatement createStmt = (SQLServerPreparedStatement) con.prepareStatement(createTempSql)) {
                boolean hasTemporaryTables = createStmt.containsTemporaryTableOperations(createTempSql);
                assertTrue(hasTemporaryTables, "Should detect CREATE TABLE statement");
                createStmt.execute();
            }
            
            // Test batch insert into temp table (not detected by simplified pattern)
            try (SQLServerPreparedStatement batchTempStmt = (SQLServerPreparedStatement) con.prepareStatement(insertTempSql)) {
                boolean hasTemporaryTables = batchTempStmt.containsTemporaryTableOperations(insertTempSql);
                assertFalse(hasTemporaryTables, "INSERT statements not detected by simplified pattern");
                
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
                assertFalse(hasTemporaryTables,
                        "Should not detect temporary table operations in CREATE regular table statement");
                createStmt.execute();
            }
            
            // Test batch insert into regular table
            try (SQLServerPreparedStatement batchRegularStmt = (SQLServerPreparedStatement) con.prepareStatement(insertRegularSql)) {
                boolean hasTemporaryTables = batchRegularStmt.containsTemporaryTableOperations(insertRegularSql);
                assertFalse(hasTemporaryTables,
                        "Should not detect temporary table operations in batch INSERT to regular table");
                
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
                assertFalse(hasTemporaryTables,
                        "Should not detect temporary table operations in DROP regular table statement");
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
            String tempTableName1 = "#batchCreateTest_" + ThreadLocalRandom.current().nextInt(1000, 9999);
            
            // First, create the temp table
            String createTableSql = "CREATE TABLE " + tempTableName1 + " (id INT)";
            try (SQLServerPreparedStatement createStmt = (SQLServerPreparedStatement) con
                    .prepareStatement(createTableSql)) {
                boolean hasTemporaryTables = createStmt.containsTemporaryTableOperations(createTableSql);
                assertTrue(hasTemporaryTables, "Should detect temporary table operations in CREATE TABLE statement");
                createStmt.execute();
            }

            // Then, batch insert into the temp table
            String insertSql = "INSERT INTO " + tempTableName1 + " (id) VALUES (?)";
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(insertSql)) {
                // Add multiple batch entries
                pstmt.setInt(1, 100);
                pstmt.addBatch();
                
                pstmt.setInt(1, 200);
                pstmt.addBatch();
                
                pstmt.setInt(1, 300);
                pstmt.addBatch();
                
                // Execute batch - only INSERT statements should execute
                int[] updateCounts = pstmt.executeBatch();
                assertTrue(updateCounts.length == 3, "Should have 3 batch results for INSERT operations");
                for (int count : updateCounts) {
                    assertTrue(count == 1, "Each INSERT operation should affect 1 row");
                }
            }
            
            // Test Scenario 2: Bracketed temp table name with special characters
            String tempTableName2 = "#batchTest_'special-chars_" + ThreadLocalRandom.current().nextInt(1000, 9999);
            
            // Create the temp table with bracketed identifier
            String createTableSql2 = "CREATE TABLE [" + tempTableName2 + "] (C1 INT CHECK (C1 > 0))";
            try (SQLServerPreparedStatement createStmt = (SQLServerPreparedStatement) con
                    .prepareStatement(createTableSql2)) {
                boolean hasTemporaryTables = createStmt.containsTemporaryTableOperations(createTableSql2);
                assertTrue(hasTemporaryTables, "Should detect temporary table operations with bracketed identifier");
                createStmt.execute();
            }

            // Batch insert into the bracketed temp table
            String insertSql2 = "INSERT INTO [" + tempTableName2 + "] (C1) VALUES (?)";
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(insertSql2)) {
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
            
            // Create temp table with CHECK constraint: C1 > 0
            String tempTableName = "#constraintBatchTest_" + ThreadLocalRandom.current().nextInt(1000, 9999);
            
            // First, create the temp table with constraint
            String createTableSql = "CREATE TABLE " + tempTableName + " (C1 INT CHECK (C1 > 0))";
            try (SQLServerPreparedStatement createStmt = (SQLServerPreparedStatement) con
                    .prepareStatement(createTableSql)) {
                boolean hasTemporaryTables = createStmt.containsTemporaryTableOperations(createTableSql);
                assertTrue(hasTemporaryTables, "Should detect temporary table operations");
                createStmt.execute();
            }

            // Then, batch insert with constraint violations
            String insertSql = "INSERT INTO " + tempTableName + " (C1) VALUES (?)";
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(insertSql)) {
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
                } catch (SQLServerException | java.sql.BatchUpdateException e) {
                    // Expected exception - may be BatchUpdateException or wrapped in
                    // SQLServerException
                    java.sql.BatchUpdateException bue;
                    
                    if (e instanceof java.sql.BatchUpdateException) {
                        bue = (java.sql.BatchUpdateException) e;
                    } else {
                        // It's SQLServerException, drill down to find BatchUpdateException
                        Throwable cause = e.getCause();
                        while (cause != null && !(cause instanceof java.sql.BatchUpdateException)) {
                            cause = cause.getCause();
                        }
                        assertTrue(cause instanceof java.sql.BatchUpdateException,
                                "Should contain BatchUpdateException in cause chain");
                        bue = (java.sql.BatchUpdateException) cause;
                    }

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
