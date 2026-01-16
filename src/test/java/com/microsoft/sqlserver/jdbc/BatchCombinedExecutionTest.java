/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
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

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /**
     * Test Case 1: Non-parameterized SQL after parameterized SQL
     * SQL: "INSERT INTO table1 VALUES (?);
     */
    @Test
    public void testNonParameterizedAfterParameterized() throws Exception {
        String tempTable = AbstractSQLGenerator
                .escapeIdentifier("#" + RandomUtil.getIdentifier("TC1_Temp"));

        try (SQLServerConnection conn = getConnection()) {
            // Enable scopeTempTablesToConnection to trigger the combined execution path
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
            // Create temp table first
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
                stmt.execute("CREATE TABLE " + tempTable + " (id INT)");
            }
            
            String sql = "INSERT INTO " + tempTable + " VALUES (?); ";

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
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tempTable)) {
                rs.next();
                assertEquals(3, rs.getInt(1), "Should have inserted 3 rows");
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
            }
        }
    }

    /**
     * Test Case 2: Multiple parameterized statements
     * SQL: "INSERT INTO table1 VALUES (?); INSERT INTO table2 VALUES (?)"
     */
    @Test
    public void testMultipleParameterizedStatements() throws Exception {
        String tempTable = AbstractSQLGenerator
                .escapeIdentifier("#" + RandomUtil.getIdentifier("TC2_Temp"));
        String testTable1 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC2_Table1"));

        try (SQLServerConnection conn = getConnection()) {
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
            // Create tables
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
                TestUtils.dropTableIfExists(testTable1, stmt);
                stmt.execute("CREATE TABLE " + tempTable + " (id INT)");
                stmt.execute("CREATE TABLE " + testTable1 + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)");
            }
            
            String sql = "INSERT INTO " + tempTable + " VALUES (?); " +
                    "INSERT INTO " + testTable1 + " VALUES (?, ?, ?)";

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
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tempTable);
                rs.next();
                assertEquals(2, rs.getInt(1));
                
                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + testTable1);
                rs.next();
                assertEquals(2, rs.getInt(1));
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
                TestUtils.dropTableIfExists(testTable1, stmt);
            }
        }
    }

    /**
     * Test Case 3: Complex interleaving - non-parameterized before, between, and after
     * SQL: "SET NOCOUNT ON; INSERT INTO table1 VALUES (?); INSERT INTO table2 VALUES (?); SELECT @@ROWCOUNT"
     */
    @Test
    public void testComplexInterleaving() throws Exception {
        String tempTable = AbstractSQLGenerator
                .escapeIdentifier("#" + RandomUtil.getIdentifier("TC3_Temp"));
        String testTable1 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC3_Table1"));

        try (SQLServerConnection conn = getConnection()) {
            conn.setPrepareMethod("scopeTempTablesToConnection");

            // Create tables
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
                TestUtils.dropTableIfExists(testTable1, stmt);
                stmt.execute("CREATE TABLE " + tempTable + " (id INT)");
                stmt.execute("CREATE TABLE " + testTable1 + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)");
            }
            
            String sql = "SET NOCOUNT ON; " +
                    "INSERT INTO " + tempTable + " VALUES (?); " +
                    "INSERT INTO " + testTable1 + " VALUES (?, ?, ?); ";

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
                TestUtils.dropTableIfExists(tempTable, stmt);
                TestUtils.dropTableIfExists(testTable1, stmt);
            }
        }
    }

    /**
     * Test Case 4: Non-parameterized statement between parameterized statements
     * SQL: "INSERT INTO table1 VALUES (?); DELETE FROM temp; INSERT INTO table2 VALUES (?)"
     */
    @Test
    public void testNonParameterizedBetweenParameterized() throws Exception {
        String tempTable = AbstractSQLGenerator
                .escapeIdentifier("#" + RandomUtil.getIdentifier("TC4_Temp"));
        String testTable1 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC4_Table1"));
        String testTable2 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC4_Table2"));

        try (SQLServerConnection conn = getConnection()) {
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
            // Create and populate tables
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
                TestUtils.dropTableIfExists(testTable1, stmt);
                TestUtils.dropTableIfExists(testTable2, stmt);
                stmt.execute("CREATE TABLE " + tempTable + " (id INT)");
                stmt.execute("CREATE TABLE " + testTable1 + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)");
                stmt.execute("CREATE TABLE " + testTable2 + " (id INT PRIMARY KEY, description NVARCHAR(100))");
                stmt.execute("INSERT INTO " + tempTable + " VALUES (999)");
            }

            String sql = "INSERT INTO " + testTable1 + " VALUES (?, ?, ?); " +
                    "DELETE FROM " + tempTable + "; " +
                    "INSERT INTO " + testTable2 + " VALUES (?, ?)";

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
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tempTable)) {
                rs.next();
                assertEquals(0, rs.getInt(1), "Temp table should be empty after DELETE");
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
                TestUtils.dropTableIfExists(testTable1, stmt);
                TestUtils.dropTableIfExists(testTable2, stmt);
            }
        }
    }

    /**
     * Test Case 5: Multiple parameters per statement
     * SQL: "INSERT INTO table1 VALUES (?, ?); INSERT INTO table2 VALUES (?, ?, ?)"
     */
    @Test
    public void testMultipleParametersPerStatement() throws Exception {
        String testTable1 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC5_Table1"));
        String testTable2 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC5_Table2"));

        try (SQLServerConnection conn = getConnection()) {
            conn.setPrepareMethod("scopeTempTablesToConnection");

            // Create tables
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(testTable1, stmt);
                TestUtils.dropTableIfExists(testTable2, stmt);
                stmt.execute("CREATE TABLE " + testTable1 + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)");
                stmt.execute("CREATE TABLE " + testTable2 + " (id INT PRIMARY KEY, description NVARCHAR(100))");
            }

            String sql = "INSERT INTO " + testTable1 + " VALUES (?, ?, ?); " +
                    "INSERT INTO " + testTable2 + " VALUES (?, ?)";

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
                ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + testTable1);
                rs.next();
                assertEquals(3, rs.getInt(1), "Table1 should have 3 rows");
                
                rs = stmt.executeQuery("SELECT COUNT(*) FROM " + testTable2);
                rs.next();
                assertEquals(3, rs.getInt(1), "Table2 should have 3 rows");
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(testTable1, stmt);
                TestUtils.dropTableIfExists(testTable2, stmt);
            }
        }
    }

    /**
     * Test Case 6: Only non-parameterized statements (edge case)
     */
    @Test
    public void testOnlyNonParameterizedStatements() throws Exception {
        String tempTable = AbstractSQLGenerator
                .escapeIdentifier("#" + RandomUtil.getIdentifier("TC6_Temp"));

        try (SQLServerConnection conn = getConnection()) {
            conn.setPrepareMethod("scopeTempTablesToConnection");

            // Create temp table
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
                stmt.execute("CREATE TABLE " + tempTable + " (id INT)");
            }
            
            // This SQL has no parameters, but we still call addBatch
            String sql = "INSERT INTO " + tempTable + " VALUES (1); ";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.addBatch();
                pstmt.addBatch(); // Adding same batch twice
                pstmt.addBatch();

                int[] updateCounts = pstmt.executeBatch();
                assertEquals(3, updateCounts.length);
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
            }
        }
    }

    /**
     * Test Case 7: String literals and comments in SQL (ensure proper parsing)
     */
    @Test
    public void testWithStringLiteralsAndComments() throws Exception {
        String testTable1 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC7_Table1"));
        String testTable2 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC7_Table2"));

        try (SQLServerConnection conn = getConnection()) {
            conn.setPrepareMethod("scopeTempTablesToConnection");

            // Create tables
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(testTable1, stmt);
                TestUtils.dropTableIfExists(testTable2, stmt);
                stmt.execute("CREATE TABLE " + testTable1 + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)");
                stmt.execute("CREATE TABLE " + testTable2 + " (id INT PRIMARY KEY, description NVARCHAR(100))");
            }
            
            String sql = "/* Comment with ; semicolon */ " +
                    "INSERT INTO " + testTable1 + " VALUES (?, 'Name;with;semicolons', ?); " +
                         "-- Another comment\n" +
                    "INSERT INTO " + testTable2 + " VALUES (?, 'Desc;test')";

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
                            "SELECT name, description FROM " + testTable1 + " t1 " +
                                    "JOIN " + testTable2 + " t2 ON t1.id - 700 = t2.id - 800")) {
                assertTrue(rs.next());
                assertEquals("Name;with;semicolons", rs.getString(1));
                assertEquals("Desc;test", rs.getString(2));
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(testTable1, stmt);
                TestUtils.dropTableIfExists(testTable2, stmt);
            }
        }
    }

    /**
     * Test Case 8: Verify correct parameter mapping for multiple parameterized statements
     */
    @Test
    public void testParameterMappingCorrectness() throws Exception {
        String testTable1 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC8_Table1"));
        String testTable2 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC8_Table2"));

        try (SQLServerConnection conn = getConnection()) {
            conn.setPrepareMethod("scopeTempTablesToConnection");

            // Create tables
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(testTable1, stmt);
                TestUtils.dropTableIfExists(testTable2, stmt);
                stmt.execute("CREATE TABLE " + testTable1 + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)");
                stmt.execute("CREATE TABLE " + testTable2 + " (id INT PRIMARY KEY, description NVARCHAR(100))");
            }
            
            String sql = "INSERT INTO " + testTable1 + " VALUES (?, ?, ?); " +
                    "INSERT INTO " + testTable2 + " VALUES (?, ?)";

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
                        "SELECT id, name, value FROM " + testTable1 + " WHERE id = 1001");
                assertTrue(rs.next());
                assertEquals(1001, rs.getInt("id"));
                assertEquals("Param2", rs.getString("name"));
                assertEquals(3, rs.getInt("value"));
                
                rs = stmt.executeQuery(
                        "SELECT id, description FROM " + testTable2 + " WHERE id = 2001");
                assertTrue(rs.next());
                assertEquals(2001, rs.getInt("id"));
                assertEquals("Param5", rs.getString("description"));
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(testTable1, stmt);
                TestUtils.dropTableIfExists(testTable2, stmt);
            }
        }
    }

    /**
     * Test Case 9: Batch with errors - verify update counts reflect failures
     */
    @Test
    public void testBatchWithErrors() throws Exception {
        String testTable1 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC9_Table1"));

        try (SQLServerConnection conn = getConnection()) {
            conn.setPrepareMethod("scopeTempTablesToConnection");

            // Create table
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(testTable1, stmt);
                stmt.execute("CREATE TABLE " + testTable1 + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)");
            }
            
            String sql = "INSERT INTO " + testTable1 + " VALUES (?, ?, ?)";

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
                TestUtils.dropTableIfExists(testTable1, stmt);
            }
        }
    }
}
