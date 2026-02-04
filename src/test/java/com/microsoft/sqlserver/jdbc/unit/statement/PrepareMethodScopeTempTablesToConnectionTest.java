/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;
import org.junit.jupiter.api.Tag;

import sun.misc.Unsafe;

/**
 * Comprehensive test suite for scopeTempTablesToConnection prepare method
 * implementation
 * Focus: Temp table persistence, parameter
 * substitution, security, edge cases, and performance
 */
@RunWith(JUnitPlatform.class)
public class PrepareMethodScopeTempTablesToConnectionTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /**
     * NEGATIVE TEST: Temp table should FAIL with "prepare" method
     * Demonstrates that temp tables are scoped to the prepared statement handle,
     * not the connection, when using traditional prepare method.
     */
    @Test
    public void testTempTableFailureWithPrepareMethod() throws SQLException {
        String tableName = "#temp_prepare_fail_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            // Use traditional "prepare" method
            conn.setPrepareMethod("prepare");
            
            // Step 1: Create temp table using prepare method
            String createTempSql = "CREATE TABLE " + tableName + " (id INT, name VARCHAR(50))";
            try (PreparedStatement ps1 = conn.prepareStatement(createTempSql)) {
                ps1.execute();
                // Temp table is scoped to this prepared statement handle
            }
            
            // Step 2: Try to insert into temp table using different PreparedStatement
            String insertSql = "INSERT INTO " + tableName + " (id, name) VALUES (?, ?)";
            try (PreparedStatement ps2 = conn.prepareStatement(insertSql)) {
                ps2.setInt(1, 123);
                ps2.setString(2, "Test Data");
                
                // This SHOULD FAIL because temp table was scoped to ps1's handle
                assertThrows(SQLException.class, () -> ps2.executeUpdate(),
                    "Should fail with 'Invalid object name' when using prepare method");
            } catch (SQLException e) {
                // Expected failure: temp table doesn't exist in this prepared statement's scope
                assertTrue(e.getMessage().contains("Invalid object name") || 
                          e.getMessage().contains(tableName),
                    "Should fail with invalid object error for: " + tableName);
            }
        }
        // Clean up
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }

    /**
     * NEGATIVE TEST: Temp table should FAIL with "prepexec" method
     * Demonstrates that temp tables are scoped to the prepared statement handle,
     * not the connection, when using sp_prepexec method.
     */
    @Test
    public void testTempTableFailureWithPrepexecMethod() throws SQLException {
        String tableName = "#temp_prepexec_fail_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            // Use "prepexec" method
            conn.setPrepareMethod("prepexec");
            
            // Step 1: Create temp table using prepexec method
            String createTempSql = "CREATE TABLE " + tableName + " (id INT, value DECIMAL(10,2))";
            try (PreparedStatement ps1 = conn.prepareStatement(createTempSql)) {
                ps1.execute();
                // Temp table is scoped to this sp_prepexec handle
            }
            
            // Step 2: Try to query temp table using different PreparedStatement
            String selectSql = "SELECT id, value FROM " + tableName + " WHERE id = ?";
            try (PreparedStatement ps2 = conn.prepareStatement(selectSql)) {
                ps2.setInt(1, 123);
                
                // This SHOULD FAIL because temp table was scoped to ps1's handle
                assertThrows(SQLException.class, () -> ps2.executeQuery(),
                    "Should fail with 'Invalid object name' when using prepexec method");
            } catch (SQLException e) {
                // Expected failure: temp table doesn't exist in this prepared statement's scope
                assertTrue(e.getMessage().contains("Invalid object name") || 
                          e.getMessage().contains(tableName),
                    "Should fail with invalid object error for: " + tableName);
            }
        }
        // Clean up
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }

    /**
     * CORE TEST: Temp table persistence across PreparedStatement boundaries
     */
    @Test
    public void testTempTablePersistenceWithScopeTempTablesToConnection() throws SQLException {
        String tableName = "#temp_exec_test_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            // Set prepare method to scopeTempTablesToConnection
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
            // Step 1: Create temp table using EXEC method
            String createTempSql = "CREATE TABLE " + tableName + " (id INT, name VARCHAR(50), value DECIMAL(10,2))";
            try (PreparedStatement ps1 = conn.prepareStatement(createTempSql)) {
                ps1.execute();
                // Temp table should now exist in session scope
            }
            
            // Step 2: Insert data into temp table using different PreparedStatement
            String insertSql = "INSERT INTO " + tableName + " (id, name, value) VALUES (?, ?, ?)";
            try (PreparedStatement ps2 = conn.prepareStatement(insertSql)) {
                ps2.setInt(1, 123);
                ps2.setString(2, "Test Data");
                ps2.setBigDecimal(3, new BigDecimal("45.67"));
                
                // This should NOT fail with scopeTempTablesToConnection method (would fail with
                // prepexec/prepare)
                int rowsInserted = ps2.executeUpdate();
                assertEquals(1, rowsInserted, "Should insert 1 row into temp table");
            }
            
            // Step 3: Query temp table using third PreparedStatement
            String selectSql = "SELECT id, name, value FROM " + tableName + " WHERE id = ?";
            try (PreparedStatement ps3 = conn.prepareStatement(selectSql)) {
                ps3.setInt(1, 123);
                
                ResultSet rs = ps3.executeQuery();
                assertTrue(rs.next(), "Should find inserted data in temp table");
                assertEquals(123, rs.getInt("id"));
                assertEquals("Test Data", rs.getString("name"));
                assertEquals(new BigDecimal("45.67"), rs.getBigDecimal("value"));
                assertFalse(rs.next(), "Should have exactly one row");
            }
            
            // Step 4: Update temp table data
            String updateSql = "UPDATE " + tableName + " SET value = ? WHERE id = ?";
            try (PreparedStatement ps4 = conn.prepareStatement(updateSql)) {
                ps4.setBigDecimal(1, new BigDecimal("99.99"));
                ps4.setInt(2, 123);
                
                int rowsUpdated = ps4.executeUpdate();
                assertEquals(1, rowsUpdated, "Should update 1 row in temp table");
            }
            
            // Step 5: Verify update
            try (PreparedStatement ps5 = conn.prepareStatement("SELECT value FROM " + tableName + " WHERE id = 123")) {
                ResultSet rs = ps5.executeQuery();
                assertTrue(rs.next(), "Should find updated data");
                assertEquals(new BigDecimal("99.99"), rs.getBigDecimal("value"));
            }
            
            // Clean up
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * Test multiple temp table scenario
     */
    @Test
    public void testMultipleTempTablesScenario() throws SQLException {
        String tempTable1 = "#orders_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        String tempTable2 = "#customers_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
            // Create first temp table
            try (PreparedStatement ps1 = conn.prepareStatement(
                "CREATE TABLE " + tempTable1 + " (order_id INT, customer_id INT, amount DECIMAL(10,2))")) {
                ps1.execute();
            }
            
            // Create second temp table
            try (PreparedStatement ps2 = conn.prepareStatement(
                "CREATE TABLE " + tempTable2 + " (customer_id INT, name VARCHAR(50))")) {
                ps2.execute();
            }
            
            // Insert data into both tables
            try (PreparedStatement ps3 = conn.prepareStatement(
                "INSERT INTO " + tempTable1 + " VALUES (?, ?, ?)")) {
                ps3.setInt(1, 1001);
                ps3.setInt(2, 501);
                ps3.setBigDecimal(3, new BigDecimal("199.99"));
                ps3.execute();
            }
            
            try (PreparedStatement ps4 = conn.prepareStatement(
                "INSERT INTO " + tempTable2 + " VALUES (?, ?)")) {
                ps4.setInt(1, 501);
                ps4.setString(2, "John Doe");
                ps4.execute();
            }
            
            // Join both temp tables
            try (PreparedStatement ps5 = conn.prepareStatement(
                "SELECT o.order_id, o.amount, c.name FROM " + tempTable1 + " o " +
                "INNER JOIN " + tempTable2 + " c ON o.customer_id = c.customer_id " +
                "WHERE o.amount > ?")) {
                ps5.setBigDecimal(1, new BigDecimal("100.00"));
                
                ResultSet rs = ps5.executeQuery();
                assertTrue(rs.next(), "Should find joined data from both temp tables");
                assertEquals(1001, rs.getInt("order_id"));
                assertEquals(new BigDecimal("199.99"), rs.getBigDecimal("amount"));
                assertEquals("John Doe", rs.getString("name"));
            }
            
            // Clean up
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(tempTable1, stmt);
                TestUtils.dropTableIfExists(tempTable2, stmt);
            }
        }
    }

    /**
     * Test temp table lifecycle with ETL/data processing pattern:
     * Create Temp1 → Insert Temp1 → Create Temp2 → Drop Temp1 → Insert Temp2
     * 
     * Real-world use case: Multi-stage data transformation pipeline where intermediate
     * staging tables are dropped early to free tempdb resources before populating
     * final result tables. Common in ETL workflows where:
     * - Temp1 holds large raw/staging data (e.g., 1M rows)
     * - Data is processed/aggregated
     * - Temp2 holds final results (e.g., 10K summary rows)
     * - Drop Temp1 early to free valuable tempdb space
     * - Continue with only Temp2
     * 
     * This validates:
     * 1. Resource management - dropping intermediate tables mid-workflow
     * 2. Independence - dropping Temp1 doesn't affect Temp2
     * 3. Session integrity - connection maintains proper state throughout
     */
    @Test
    public void testTempTableLifecycleWithResourceManagement() throws SQLException {
        String tempTable1 = "#staging_data_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        String tempTable2 = "#final_results_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
            // Step 1: Create Temp1 (staging table for raw data)
            try (PreparedStatement ps1 = conn.prepareStatement(
                "CREATE TABLE " + tempTable1 + " (id INT, raw_data VARCHAR(100), category VARCHAR(50))")) {
                ps1.execute();
            }
            
            // Step 2: Insert into Temp1 (load staging data)
            try (PreparedStatement ps2 = conn.prepareStatement(
                "INSERT INTO " + tempTable1 + " (id, raw_data, category) VALUES (?, ?, ?)")) {
                ps2.setInt(1, 1);
                ps2.setString(2, "Raw staging data for processing");
                ps2.setString(3, "CategoryA");
                int rowsInserted = ps2.executeUpdate();
                assertEquals(1, rowsInserted, "Should insert 1 row into staging table");
            }
            
            // Verify Temp1 has data before proceeding
            try (PreparedStatement psVerify1 = conn.prepareStatement(
                "SELECT COUNT(*) as cnt FROM " + tempTable1)) {
                ResultSet rs = psVerify1.executeQuery();
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("cnt"), "Staging table should have 1 row");
            }
            
            // Step 3: Create Temp2 (final results table)
            try (PreparedStatement ps3 = conn.prepareStatement(
                "CREATE TABLE " + tempTable2 + " (result_id INT, summary VARCHAR(200), amount DECIMAL(10,2))")) {
                ps3.execute();
            }
            
            // Step 4: Drop Temp1 (free tempdb resources - critical for large datasets)
            // In real ETL: Temp1 might have millions of rows, Temp2 only thousands
            try (PreparedStatement ps4 = conn.prepareStatement("DROP TABLE " + tempTable1)) {
                ps4.execute();
            }
            
            // Verify Temp1 is dropped
            try (PreparedStatement psVerifyDropped = conn.prepareStatement(
                "SELECT COUNT(*) FROM " + tempTable1)) {
                assertThrows(SQLException.class, () -> psVerifyDropped.executeQuery(),
                    "Accessing dropped staging table should fail");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("Invalid object name") || 
                          e.getMessage().contains(tempTable1),
                    "Should get 'Invalid object name' error for dropped table");
            }
            
            // Step 5: Insert into Temp2 (populate final results)
            // This should work even though Temp1 was dropped - demonstrates independence
            try (PreparedStatement ps5 = conn.prepareStatement(
                "INSERT INTO " + tempTable2 + " (result_id, summary, amount) VALUES (?, ?, ?)")) {
                ps5.setInt(1, 100);
                ps5.setString(2, "Processed final results after staging cleanup");
                ps5.setBigDecimal(3, new BigDecimal("1234.56"));
                int rowsInserted = ps5.executeUpdate();
                assertEquals(1, rowsInserted, "Should insert into final table after dropping staging table");
            }
            
            // Verify Temp2 has correct data and is fully functional
            try (PreparedStatement psVerify2 = conn.prepareStatement(
                "SELECT result_id, summary, amount FROM " + tempTable2 + " WHERE result_id = ?")) {
                psVerify2.setInt(1, 100);
                ResultSet rs = psVerify2.executeQuery();
                assertTrue(rs.next(), "Should find data in final results table");
                assertEquals(100, rs.getInt("result_id"));
                assertEquals("Processed final results after staging cleanup", rs.getString("summary"));
                assertEquals(new BigDecimal("1234.56"), rs.getBigDecimal("amount"));
                assertFalse(rs.next(), "Should have exactly one row");
            }
            
            // Additional validation: Temp2 remains fully operational
            try (PreparedStatement ps6 = conn.prepareStatement(
                "INSERT INTO " + tempTable2 + " (result_id, summary, amount) VALUES (?, ?, ?)")) {
                ps6.setInt(1, 101);
                ps6.setString(2, "Additional final data");
                ps6.setBigDecimal(3, new BigDecimal("5678.90"));
                ps6.executeUpdate();
            }
            
            // Verify Temp2 now has 2 rows
            try (PreparedStatement psCount = conn.prepareStatement(
                "SELECT COUNT(*) as total FROM " + tempTable2)) {
                ResultSet rs = psCount.executeQuery();
                assertTrue(rs.next());
                assertEquals(2, rs.getInt("total"), "Final table should have 2 rows");
            }
            
            // Clean up
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(tempTable1, stmt);
                TestUtils.dropTableIfExists(tempTable2, stmt);
            }
        }
    }

    /**
     * Test batch execution with scopeTempTablesToConnection method
     */
    @Test
    public void testBatchExecution() throws SQLException {
        String tempTable = "#batch_test_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
            // Create temp table
            try (PreparedStatement createPs = conn.prepareStatement(
                "CREATE TABLE " + tempTable + " (id INT, name VARCHAR(50), value DECIMAL(8,2))")) {
                createPs.execute();
            }
            
            // Test batch insert
            String insertSql = "INSERT INTO " + tempTable + " (id, name, value) VALUES (?, ?, ?)";
            try (PreparedStatement ps = conn.prepareStatement(insertSql)) {
                // Add multiple batches
                for (int i = 1; i <= 5; i++) {
                    ps.setInt(1, i);
                    ps.setString(2, "Name" + i);
                    ps.setBigDecimal(3, new BigDecimal(i * 10.5));
                    ps.addBatch();
                }
                
                int[] batchResults = ps.executeBatch();
                assertEquals(5, batchResults.length, "Should execute 5 batch statements");
                for (int result : batchResults) {
                    // scopeTempTablesToConnection method may return SUCCESS_NO_INFO (-2) or
                    // EXECUTE_FAILED (-3) for batch
                    // execution
                    // which is valid per JDBC spec, or exact row count (1)
                    assertTrue(result >= -2, "Each batch should succeed (may return SUCCESS_NO_INFO)");
                }
            }
            
            // Verify batch results
            try (PreparedStatement selectPs = conn.prepareStatement(
                "SELECT COUNT(*) as row_count FROM " + tempTable)) {
                ResultSet rs = selectPs.executeQuery();
                assertTrue(rs.next());
                assertEquals(5, rs.getInt("row_count"), "Should have 5 rows inserted via batch");
            }
            // Clean up
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
            }
        }
    }

    // ==================== FormatLiteralValue Tests ====================
    // Tests for SQLServerPreparedStatement.formatLiteralValue method
    // These tests use reflection to access the package-private method

    /**
     * Helper method to create a PreparedStatement for formatLiteralValue testing
     */
    private SQLServerPreparedStatement createPreparedStatementForFormatTest() throws Exception {
        SQLServerConnection conn = PrepUtil.getConnection(connectionString);

        Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Unsafe unsafe = (Unsafe) unsafeField.get(null);
        SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) unsafe
                .allocateInstance(SQLServerPreparedStatement.class);

        Field connField = SQLServerStatement.class.getDeclaredField("connection");
        connField.setAccessible(true);
        connField.set(pstmt, conn);

        return pstmt;
    }

    /**
     * Helper method to call formatLiteralValue using reflection
     */
    private String callFormatLiteralValue(SQLServerPreparedStatement pstmt, Object value) throws Exception {
        Method formatMethod = SQLServerPreparedStatement.class.getDeclaredMethod("formatLiteralValue", Object.class);
        formatMethod.setAccessible(true);
        return (String) formatMethod.invoke(pstmt, value);
    }

    // ==================== DateTime Formatting Tests ====================

    @Test
    public void testFormatLiteralValueDateTimeOptimization() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Test all date/time types with optimized formatting
        Date testDate = Date.valueOf("2023-12-15");
        Time testTime = Time.valueOf("14:30:45");
        Timestamp testTimestamp = Timestamp.valueOf("2023-12-15 14:30:45.123456789");

        // Verify optimized formatting produces correct SQL CAST syntax
        assertEquals("CAST('2023-12-15' AS DATE)", callFormatLiteralValue(pstmt, testDate));
        assertEquals("CAST('14:30:45' AS TIME)", callFormatLiteralValue(pstmt, testTime));
        assertEquals("CAST('2023-12-15 14:30:45.123456789' AS DATETIME2)",
                callFormatLiteralValue(pstmt, testTimestamp));

        // Test edge cases - java.sql.Date.toString() always produces ISO format
        // (yyyy-MM-dd)
        Date minDate = Date.valueOf("1900-01-01");
        Date maxDate = Date.valueOf("9999-12-31");
        Date leapYear = Date.valueOf("2024-02-29");
        assertEquals("CAST('1900-01-01' AS DATE)", callFormatLiteralValue(pstmt, minDate));
        assertEquals("CAST('9999-12-31' AS DATE)", callFormatLiteralValue(pstmt, maxDate));
        assertEquals("CAST('2024-02-29' AS DATE)", callFormatLiteralValue(pstmt, leapYear));

        // Test time edge cases
        Time midnight = Time.valueOf("00:00:00");
        Time endOfDay = Time.valueOf("23:59:59");
        assertEquals("CAST('00:00:00' AS TIME)", callFormatLiteralValue(pstmt, midnight));
        assertEquals("CAST('23:59:59' AS TIME)", callFormatLiteralValue(pstmt, endOfDay));
    }

    @Test
    public void testFormatLiteralValuePrecisionImprovement() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Demonstrate that timestamp optimization preserves DATETIME2 precision (7
        // digits) vs old TS_FMT
        Timestamp nanoTimestamp = Timestamp.valueOf("2023-12-15 14:30:45.0");
        nanoTimestamp.setNanos(123456700); // DATETIME2 has precision = 7

        String optimizedResult = callFormatLiteralValue(pstmt, nanoTimestamp);
        assertEquals("CAST('2023-12-15 14:30:45.1234567' AS DATETIME2)", optimizedResult);

        // Compare with what TS_FMT would produce (lost precision)
        java.text.SimpleDateFormat TS_FMT = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String oldResult = "CAST('" + TS_FMT.format(nanoTimestamp) + "' AS DATETIME2)";
        assertEquals("CAST('2023-12-15 14:30:45.123' AS DATETIME2)", oldResult);

        assertNotEquals(oldResult, optimizedResult, "Optimized version preserves DATETIME2 precision (7 digits)");
    }

    // ==================== Numeric Formatting Tests ====================

    @Test
    public void testFormatLiteralValueByteMinMax() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Test Byte.MIN_VALUE
        assertEquals("-128", callFormatLiteralValue(pstmt, Byte.MIN_VALUE));

        // Test Byte.MAX_VALUE
        assertEquals("127", callFormatLiteralValue(pstmt, Byte.MAX_VALUE));

        // Test zero
        assertEquals("0", callFormatLiteralValue(pstmt, (byte) 0));
    }

    @Test
    public void testFormatLiteralValueShortMinMax() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Test Short.MIN_VALUE
        assertEquals("-32768", callFormatLiteralValue(pstmt, Short.MIN_VALUE));

        // Test Short.MAX_VALUE
        assertEquals("32767", callFormatLiteralValue(pstmt, Short.MAX_VALUE));
    }

    @Test
    public void testFormatLiteralValueIntegerMinMax() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Test Integer.MIN_VALUE
        assertEquals("-2147483648", callFormatLiteralValue(pstmt, Integer.MIN_VALUE));

        // Test Integer.MAX_VALUE
        assertEquals("2147483647", callFormatLiteralValue(pstmt, Integer.MAX_VALUE));
    }

    @Test
    public void testFormatLiteralValueLongMinMax() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Test Long.MIN_VALUE
        assertEquals("-9223372036854775808", callFormatLiteralValue(pstmt, Long.MIN_VALUE));

        // Test Long.MAX_VALUE
        assertEquals("9223372036854775807", callFormatLiteralValue(pstmt, Long.MAX_VALUE));
    }

    @Test
    public void testFormatLiteralValueFloatMinMax() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Test Float.MIN_VALUE (smallest positive value)
        String result = callFormatLiteralValue(pstmt, Float.MIN_VALUE);
        assertEquals(new BigDecimal(Float.toString(Float.MIN_VALUE)).toPlainString(), result);

        // Test Float.MAX_VALUE
        result = callFormatLiteralValue(pstmt, Float.MAX_VALUE);
        assertEquals(new BigDecimal(Float.toString(Float.MAX_VALUE)).toPlainString(), result);

        // Test negative Float.MAX_VALUE
        result = callFormatLiteralValue(pstmt, -Float.MAX_VALUE);
        assertEquals(new BigDecimal(Float.toString(-Float.MAX_VALUE)).toPlainString(), result);

        // Test Float.MIN_NORMAL
        result = callFormatLiteralValue(pstmt, Float.MIN_NORMAL);
        assertEquals(new BigDecimal(Float.toString(Float.MIN_NORMAL)).toPlainString(), result);
    }

    @Test
    public void testFormatLiteralValueDoubleMinMax() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Test Double.MIN_VALUE (smallest positive value)
        String result = callFormatLiteralValue(pstmt, Double.MIN_VALUE);
        assertEquals(new BigDecimal(Double.toString(Double.MIN_VALUE)).toPlainString(), result);

        // Test Double.MAX_VALUE
        result = callFormatLiteralValue(pstmt, Double.MAX_VALUE);
        assertEquals(new BigDecimal(Double.toString(Double.MAX_VALUE)).toPlainString(), result);

        // Test negative Double.MAX_VALUE
        result = callFormatLiteralValue(pstmt, -Double.MAX_VALUE);
        assertEquals(new BigDecimal(Double.toString(-Double.MAX_VALUE)).toPlainString(), result);

        // Test Double.MIN_NORMAL
        result = callFormatLiteralValue(pstmt, Double.MIN_NORMAL);
        assertEquals(new BigDecimal(Double.toString(Double.MIN_NORMAL)).toPlainString(), result);
    }

    @Test
    public void testFormatLiteralValueBigDecimalPrecision() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Test BigDecimal with precision <= 18 and scale <= 6 (no CAST needed)
        BigDecimal smallDecimal = new BigDecimal("123456789.123456");
        String result = callFormatLiteralValue(pstmt, smallDecimal);
        assertEquals("123456789.123456", result);

        // Test BigDecimal with high precision (needs CAST)
        BigDecimal highPrecisionDecimal = new BigDecimal("1234567890123456789012345678901234567890.123456789");
        result = callFormatLiteralValue(pstmt, highPrecisionDecimal);
        assertTrue(result.startsWith("CAST("));
        assertTrue(result.contains("AS DECIMAL(38,"));

        // Test BigDecimal with high scale (needs CAST)
        BigDecimal highScaleDecimal = new BigDecimal("123.1234567890123456789");
        result = callFormatLiteralValue(pstmt, highScaleDecimal);
        assertTrue(result.startsWith("CAST("));
        assertTrue(result.contains("AS DECIMAL("));
    }

    @Test
    public void testFormatLiteralValueBigDecimalMinMax() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Test very large BigDecimal
        BigDecimal largeDecimal = new BigDecimal("99999999999999999999999999999999999999");
        String result = callFormatLiteralValue(pstmt, largeDecimal);
        assertTrue(result.startsWith("CAST("));
        assertTrue(result.contains("AS DECIMAL(38,"));

        // Test very small BigDecimal
        BigDecimal smallDecimal = new BigDecimal("-99999999999999999999999999999999999999");
        result = callFormatLiteralValue(pstmt, smallDecimal);
        assertTrue(result.startsWith("CAST("));
        assertTrue(result.contains("AS DECIMAL(38,"));

        // Test BigDecimal with maximum SQL Server precision (38)
        StringBuilder sb = new StringBuilder("1");
        for (int i = 0; i < 37; i++) {
            sb.append("0");
        }
        String maxPrecisionStr = sb.toString(); // 38 digits
        BigDecimal maxPrecisionDecimal = new BigDecimal(maxPrecisionStr);
        result = callFormatLiteralValue(pstmt, maxPrecisionDecimal);
        assertTrue(result.startsWith("CAST("));
        assertTrue(result.contains("AS DECIMAL(38,0)"));
    }

    @Test
    public void testFormatLiteralValueBigInteger() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // BigInteger should be handled as fallback (toString with quotes)
        BigInteger bigInt = new BigInteger("12345678901234567890123456789012345678901234567890");
        String result = callFormatLiteralValue(pstmt, bigInt);
        // Should fall through to the fallback case and be treated as string
        assertTrue(result.startsWith("'") || result.startsWith("N'"));
        assertTrue(result.endsWith("'"));
    }

    @Test
    public void testFormatLiteralValueScientificNotationHandling() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Test very small double that might use scientific notation
        double scientificDouble = 1.23e-100;
        String result = callFormatLiteralValue(pstmt, scientificDouble);
        // Should not contain 'E' or 'e' (scientific notation)
        assertTrue(!result.contains("E") && !result.contains("e"));

        // Test very large double that might use scientific notation
        double largeDouble = 1.23e100;
        result = callFormatLiteralValue(pstmt, largeDouble);
        // Should not contain 'E' or 'e' (scientific notation)
        assertTrue(!result.contains("E") && !result.contains("e"));
    }

    @Test
    public void testFormatLiteralValueFloatSpecialValues() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Test Float.POSITIVE_INFINITY - should be formatted as string
        String result = callFormatLiteralValue(pstmt, Float.POSITIVE_INFINITY);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("Infinity"));

        // Test Float.NEGATIVE_INFINITY - should be formatted as string
        result = callFormatLiteralValue(pstmt, Float.NEGATIVE_INFINITY);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("-Infinity"));

        // Test Float.NaN - should be formatted as string
        result = callFormatLiteralValue(pstmt, Float.NaN);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("NaN"));
    }

    @Test
    public void testFormatLiteralValueDoubleSpecialValues() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Test Double.POSITIVE_INFINITY - should be formatted as string
        String result = callFormatLiteralValue(pstmt, Double.POSITIVE_INFINITY);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("Infinity"));

        // Test Double.NEGATIVE_INFINITY - should be formatted as string
        result = callFormatLiteralValue(pstmt, Double.NEGATIVE_INFINITY);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("-Infinity"));

        // Test Double.NaN - should be formatted as string
        result = callFormatLiteralValue(pstmt, Double.NaN);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("NaN"));
    }

    @Test
    public void testFormatLiteralValueBigDecimalSpecialCases() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Test BigDecimal.ZERO
        assertEquals("0", callFormatLiteralValue(pstmt, BigDecimal.ZERO));

        // Test BigDecimal.ONE
        assertEquals("1", callFormatLiteralValue(pstmt, BigDecimal.ONE));

        // Test BigDecimal.TEN
        assertEquals("10", callFormatLiteralValue(pstmt, BigDecimal.TEN));

        // Test BigDecimal with trailing zeros
        BigDecimal trailingZeros = new BigDecimal("123.4500");
        assertEquals("123.4500", callFormatLiteralValue(pstmt, trailingZeros));

        // Test BigDecimal with leading zeros
        BigDecimal leadingZeros = new BigDecimal("000123.45");
        assertEquals("123.45", callFormatLiteralValue(pstmt, leadingZeros));
    }

    @Test
    public void testFormatLiteralValuePrecisionAndScaleBoundaries() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Test boundary case: precision = 18, scale = 6 (should not need CAST)
        BigDecimal boundaryDecimal = new BigDecimal("123456789012.123456"); // 18 total digits
        String result = callFormatLiteralValue(pstmt, boundaryDecimal);
        assertEquals("123456789012.123456", result);

        // Test boundary case: precision = 19, scale = 6 (should need CAST)
        BigDecimal overBoundaryDecimal = new BigDecimal("1234567890123456789.123456");
        result = callFormatLiteralValue(pstmt, overBoundaryDecimal);
        assertTrue(result.startsWith("CAST("));

        // Test boundary case: precision = 18, scale = 7 (should need CAST)
        BigDecimal overScaleBoundaryDecimal = new BigDecimal("12345678901234567.1234567");
        result = callFormatLiteralValue(pstmt, overScaleBoundaryDecimal);
        assertTrue(result.startsWith("CAST("));
    }

    @Test
    public void testFormatLiteralValueNullValue() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        String result = callFormatLiteralValue(pstmt, null);
        assertEquals("NULL", result);
    }

    @Test
    public void testFormatLiteralValueBooleanValues() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Test true
        assertEquals("1", callFormatLiteralValue(pstmt, Boolean.TRUE));

        // Test false
        assertEquals("0", callFormatLiteralValue(pstmt, Boolean.FALSE));

        // Test boolean primitive true
        assertEquals("1", callFormatLiteralValue(pstmt, true));

        // Test boolean primitive false
        assertEquals("0", callFormatLiteralValue(pstmt, false));
    }

    @Test
    public void testFormatLiteralValueEdgeCasesForSQLServerLimits() throws Exception {
        SQLServerPreparedStatement pstmt = createPreparedStatementForFormatTest();

        // Test decimal with scale > 38 (should be clamped to 38)
        BigDecimal highScaleDecimal = new BigDecimal("1.123456789012345678901234567890123456789012345678901234567890");
        String result = callFormatLiteralValue(pstmt, highScaleDecimal);
        assertTrue(result.startsWith("CAST("));
        // Scale should be clamped to not exceed precision and SQL Server limits
        assertTrue(result.contains("AS DECIMAL("));

        // Test decimal where scale would exceed precision after clamping
        BigDecimal problematicDecimal = new BigDecimal("1.123456789012345678901234567890123456789");
        result = callFormatLiteralValue(pstmt, problematicDecimal);
        assertTrue(result.startsWith("CAST("));
    }

    // ==================== Batch Combined Execution Tests ====================
    // Tests for doExecuteExecMethodBatchCombined method which handles:
    // - Multiple parameterized statements
    // - Non-parameterized statements before, after, or between parameterized
    // statements
    // - Complex interleaving of parameterized and non-parameterized statements

    /**
     * Test Case: Non-parameterized SQL after parameterized SQL
     * SQL: "INSERT INTO table1 VALUES (?);
     */
    @Test
    public void testBatchNonParameterizedAfterParameterized() throws Exception {
        String tempTable = AbstractSQLGenerator
                .escapeIdentifier("#" + RandomUtil.getIdentifier("TC1_Temp"));

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            // Enable scopeTempTablesToConnection to trigger the combined execution path
            conn.setPrepareMethod("scopeTempTablesToConnection");

            // Create temp table first using PreparedStatement for
            // scopeTempTablesToConnection
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
            }
            try (PreparedStatement createPs = conn.prepareStatement("CREATE TABLE " + tempTable + " (id INT)")) {
                createPs.execute();
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
     * Test Case: Multiple parameterized statements
     * SQL: "INSERT INTO table1 VALUES (?); INSERT INTO table2 VALUES (?)"
     */
    @Test
    public void testBatchMultipleParameterizedStatements() throws Exception {
        String tempTable = AbstractSQLGenerator
                .escapeIdentifier("#" + RandomUtil.getIdentifier("TC2_Temp"));
        String testTable1 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC2_Table1"));

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("scopeTempTablesToConnection");

            // Create tables - temp table via PreparedStatement, regular table via Statement
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
                TestUtils.dropTableIfExists(testTable1, stmt);
                stmt.execute("CREATE TABLE " + testTable1 + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)");
            }
            try (PreparedStatement createPs = conn.prepareStatement("CREATE TABLE " + tempTable + " (id INT)")) {
                createPs.execute();
            }

            String sql = "INSERT INTO " + tempTable + " VALUES (?); " +
                    "INSERT INTO " + testTable1 + " VALUES (?, ?, ?)";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                // Batch 1
                pstmt.setInt(1, 1); // First INSERT param
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
     * Test Case: Complex interleaving - non-parameterized before, between, and
     * after
     * SQL: "SET NOCOUNT ON; INSERT INTO table1 VALUES (?); INSERT INTO table2
     * VALUES (?); SELECT @@ROWCOUNT"
     */
    @Test
    public void testBatchComplexInterleaving() throws Exception {
        String tempTable = AbstractSQLGenerator
                .escapeIdentifier("#" + RandomUtil.getIdentifier("TC3_Temp"));
        String testTable1 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC3_Table1"));

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("scopeTempTablesToConnection");

            // Create tables - temp table via PreparedStatement, regular table via Statement
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
                TestUtils.dropTableIfExists(testTable1, stmt);
                stmt.execute("CREATE TABLE " + testTable1 + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)");
            }
            try (PreparedStatement createPs = conn.prepareStatement("CREATE TABLE " + tempTable + " (id INT)")) {
                createPs.execute();
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
     * Test Case: Non-parameterized statement between parameterized statements
     * SQL: "INSERT INTO table1 VALUES (?); DELETE FROM temp; INSERT INTO table2
     * VALUES (?)"
     */
    @Test
    public void testBatchNonParameterizedBetweenParameterized() throws Exception {
        String tempTable = AbstractSQLGenerator
                .escapeIdentifier("#" + RandomUtil.getIdentifier("TC4_Temp"));
        String testTable1 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC4_Table1"));
        String testTable2 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC4_Table2"));

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("scopeTempTablesToConnection");

            // Create and populate tables - temp table via PreparedStatement, regular tables
            // via Statement
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
                TestUtils.dropTableIfExists(testTable1, stmt);
                TestUtils.dropTableIfExists(testTable2, stmt);
                stmt.execute("CREATE TABLE " + testTable1 + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)");
                stmt.execute("CREATE TABLE " + testTable2 + " (id INT PRIMARY KEY, description NVARCHAR(100))");
            }
            try (PreparedStatement createPs = conn.prepareStatement("CREATE TABLE " + tempTable + " (id INT)")) {
                createPs.execute();
            }
            try (PreparedStatement insertPs = conn.prepareStatement("INSERT INTO " + tempTable + " VALUES (999)")) {
                insertPs.execute();
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
     * Test Case: Multiple parameters per statement
     * SQL: "INSERT INTO table1 VALUES (?, ?); INSERT INTO table2 VALUES (?, ?, ?)"
     */
    @Test
    public void testBatchMultipleParametersPerStatement() throws Exception {
        String testTable1 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC5_Table1"));
        String testTable2 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC5_Table2"));

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
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
     * Test Case: Only non-parameterized statements (edge case)
     */
    @Test
    public void testBatchOnlyNonParameterizedStatements() throws Exception {
        String tempTable = AbstractSQLGenerator
                .escapeIdentifier("#" + RandomUtil.getIdentifier("TC6_Temp"));

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("scopeTempTablesToConnection");

            // Create temp table using PreparedStatement
            try (PreparedStatement pstmt = conn.prepareStatement("CREATE TABLE " + tempTable + " (id INT)")) {
                pstmt.execute();
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
     * Test Case: String literals and comments in SQL (ensure proper parsing)
     */
    @Test
    public void testBatchWithStringLiteralsAndComments() throws Exception {
        String testTable1 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC7_Table1"));
        String testTable2 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC7_Table2"));

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
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
     * Test Case: Verify correct parameter mapping for multiple parameterized
     * statements
     */
    @Test
    public void testBatchParameterMappingCorrectness() throws Exception {
        String testTable1 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC8_Table1"));
        String testTable2 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC8_Table2"));

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
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
     * Test Case: Batch with errors - verify update counts reflect failures
     */
    @Test
    public void testBatchCombinedWithErrors() throws Exception {
        String testTable1 = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("TC9_Table1"));

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
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

    // ==================== prepareMethod=none Tests ====================
    // Tests for prepareMethod=none which always uses direct SQL batch execution
    // (no temp table detection required)

    /**
     * Test prepareMethod=none with temp table operations
     * Unlike scopeTempTablesToConnection, none always uses direct SQL regardless of
     * temp tables
     */
    @Test
    public void testTempTablePersistenceWithPrepareMethodNone() throws SQLException {
        String tableName = "#temp_none_test_" + ThreadLocalRandom.current().nextInt(1000, 9999);

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("none");

            // Create temp table
            try (PreparedStatement ps1 = conn.prepareStatement(
                    "CREATE TABLE " + tableName + " (id INT, name VARCHAR(50), value DECIMAL(10,2))")) {
                ps1.execute();
            }

            // Insert data using different PreparedStatement - should work with none
            try (PreparedStatement ps2 = conn.prepareStatement(
                    "INSERT INTO " + tableName + " (id, name, value) VALUES (?, ?, ?)")) {
                ps2.setInt(1, 123);
                ps2.setString(2, "Test Data");
                ps2.setBigDecimal(3, new BigDecimal("45.67"));
                int rowsInserted = ps2.executeUpdate();
                assertEquals(1, rowsInserted, "Should insert 1 row into temp table");
            }

            // Query temp table using third PreparedStatement
            try (PreparedStatement ps3 = conn.prepareStatement(
                    "SELECT id, name, value FROM " + tableName + " WHERE id = ?")) {
                ps3.setInt(1, 123);
                try (ResultSet rs = ps3.executeQuery()) {
                    assertTrue(rs.next(), "Should find inserted data in temp table");
                    assertEquals(123, rs.getInt("id"));
                    assertEquals("Test Data", rs.getString("name"));
                    assertEquals(new BigDecimal("45.67"), rs.getBigDecimal("value"));
                }
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * Test prepareMethod=none with temp tables (non-CREATE scenario)
     * This is the key use case: always use direct SQL for server-side visibility
     */
    @Test
    public void testTempTableWithPrepareMethodNone() throws SQLException {
        String tableName = "#temp_none_regular_" + ThreadLocalRandom.current().nextInt(1000, 9999);

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("none");

            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tableName + " (id INT, name NVARCHAR(100), amount DECIMAL(10,2))")) {
                ps.execute();
            }

            // Insert using PreparedStatement with none - should use direct SQL
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + tableName + " (id, name, amount) VALUES (?, ?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "First");
                ps.setBigDecimal(3, new BigDecimal("100.50"));
                ps.executeUpdate();

                ps.setInt(1, 2);
                ps.setString(2, "Second");
                ps.setBigDecimal(3, new BigDecimal("200.75"));
                ps.executeUpdate();

                ps.setInt(1, 3);
                ps.setString(2, "Third");
                ps.setBigDecimal(3, new BigDecimal("300.25"));
                ps.executeUpdate();
            }

            // Verify data
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + tableName);
                    ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(3, rs.getInt(1), "Should have 3 rows inserted");
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * Test batch execution with prepareMethod=none using temp table
     */
    @Test
    public void testBatchExecutionWithPrepareMethodNone() throws SQLException {
        String tableName = "#temp_none_batch_" + ThreadLocalRandom.current().nextInt(1000, 9999);

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("none");

            // Create temp table using PreparedStatement with none
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tableName + " (id INT, name VARCHAR(50), value DECIMAL(8,2))")) {
                ps.execute();
            }

            // Batch insert with none - temp table should persist across PreparedStatements
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + tableName + " (id, name, value) VALUES (?, ?, ?)")) {
                for (int i = 1; i <= 5; i++) {
                    ps.setInt(1, i);
                    ps.setString(2, "Name" + i);
                    ps.setBigDecimal(3, new BigDecimal(i * 10.5));
                    ps.addBatch();
                }

                int[] batchResults = ps.executeBatch();
                assertEquals(5, batchResults.length, "Should execute 5 batch statements");
            }

            // Verify batch results - temp table should still be accessible
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + tableName);
                    ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(5, rs.getInt(1), "Should have 5 rows inserted via batch");
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * Test prepareMethod=none with multiple data types using temp table
     */
    @Test
    public void testMultipleDataTypesWithPrepareMethodNone() throws SQLException {
        String tableName = "#temp_none_datatypes_" + ThreadLocalRandom.current().nextInt(1000, 9999);

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("none");

            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tableName + " (" +
                            "col_int INT, " +
                            "col_bigint BIGINT, " +
                            "col_decimal DECIMAL(18,6), " +
                            "col_float FLOAT, " +
                            "col_varchar VARCHAR(100), " +
                            "col_nvarchar NVARCHAR(100), " +
                            "col_bit BIT, " +
                            "col_date DATE, " +
                            "col_datetime2 DATETIME2)")) {
                ps.execute();
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + tableName + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")) {
                ps.setInt(1, Integer.MAX_VALUE);
                ps.setLong(2, Long.MAX_VALUE);
                ps.setBigDecimal(3, new BigDecimal("123456789.123456"));
                ps.setDouble(4, 3.14159265359);
                ps.setString(5, "ASCII string");
                ps.setNString(6, "Unicode: café 日本語");
                ps.setBoolean(7, true);
                ps.setDate(8, Date.valueOf("2023-12-15"));
                ps.setTimestamp(9, Timestamp.valueOf("2023-12-15 14:30:45.123456"));
                ps.executeUpdate();
            }

            // Verify data types preserved
            try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + tableName);
                    ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(Integer.MAX_VALUE, rs.getInt("col_int"));
                assertEquals(Long.MAX_VALUE, rs.getLong("col_bigint"));
                assertEquals(new BigDecimal("123456789.123456"), rs.getBigDecimal("col_decimal"));
                assertEquals("ASCII string", rs.getString("col_varchar"));
                assertEquals("Unicode: café 日本語", rs.getNString("col_nvarchar"));
                assertTrue(rs.getBoolean("col_bit"));
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * Test prepareMethod=none with NULL values using temp table
     */
    @Test
    public void testNullValuesWithPrepareMethodNone() throws SQLException {
        String tableName = "#temp_none_nulls_" + ThreadLocalRandom.current().nextInt(1000, 9999);

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("none");

            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tableName + " (id INT, name VARCHAR(50), value DECIMAL(10,2))")) {
                ps.execute();
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + tableName + " (id, name, value) VALUES (?, ?, ?)")) {
                ps.setInt(1, 1);
                ps.setNull(2, Types.VARCHAR);
                ps.setNull(3, Types.DECIMAL);
                ps.executeUpdate();
            }

            // Verify NULL values
            try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + tableName + " WHERE id = ?")) {
                ps.setInt(1, 1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt("id"));
                    assertNull(rs.getString("name"));
                    assertTrue(rs.wasNull());
                    assertNull(rs.getBigDecimal("value"));
                    assertTrue(rs.wasNull());
                }
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * Test SELECT queries with prepareMethod=none using temp table
     */
    @Test
    public void testSelectQueryWithPrepareMethodNone() throws SQLException {
        String tableName = "#temp_none_select_" + ThreadLocalRandom.current().nextInt(1000, 9999);

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("none");

            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tableName + " (id INT, name VARCHAR(50))")) {
                ps.execute();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + tableName + " VALUES (1, 'One'), (2, 'Two'), (3, 'Three')")) {
                ps.execute();
            }

            // Select with parameters using none
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id, name FROM " + tableName + " WHERE id > ? AND name LIKE ?")) {
                ps.setInt(1, 1);
                ps.setString(2, "T%");
                try (ResultSet rs = ps.executeQuery()) {
                    int count = 0;
                    while (rs.next()) {
                        count++;
                        assertTrue(rs.getInt("id") > 1);
                        assertTrue(rs.getString("name").startsWith("T"));
                    }
                    assertEquals(2, count, "Should find 2 rows matching criteria");
                }
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    // ==================== prepareMethod=none SQL Batch Tests ====================
    // Tests for prepareMethod=none with SQL Batch execution patterns
    // Verifies literal parameter substitution works correctly in batch scenarios

    /**
     * Test Case: Multiple parameterized statements with prepareMethod=none
     * SQL: "INSERT INTO table1 VALUES (?); INSERT INTO table2 VALUES (?)"
     * Verifies literal parameter substitution for multiple parameterized statements
     * in batch
     */
    @Test
    public void testBatchMultipleParameterizedStatementsWithNone() throws Exception {
        String tempTable1 = "#temp_none_batch1_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        String tempTable2 = "#temp_none_batch2_" + ThreadLocalRandom.current().nextInt(1000, 9999);

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("none");

            // Create temp tables using PreparedStatement with none
            try (PreparedStatement ps = conn.prepareStatement("CREATE TABLE " + tempTable1 + " (id INT)")) {
                ps.execute();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tempTable2 + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)")) {
                ps.execute();
            }

            String sql = "INSERT INTO " + tempTable1 + " VALUES (?); " +
                    "INSERT INTO " + tempTable2 + " VALUES (?, ?, ?)";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                // Batch 1
                pstmt.setInt(1, 1); // First INSERT param
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

            // Verify both temp tables have data
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + tempTable1);
                    ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(2, rs.getInt(1));
            }

            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + tempTable2);
                    ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(2, rs.getInt(1));
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable1, stmt);
                TestUtils.dropTableIfExists(tempTable2, stmt);
            }
        }
    }

    /**
     * Test Case: Non-parameterized statement between parameterized statements with
     * none
     * SQL: "INSERT INTO table1 VALUES (?); DELETE FROM temp; INSERT INTO table2
     * VALUES (?)"
     */
    @Test
    public void testBatchNonParameterizedBetweenParameterizedWithNone() throws Exception {
        String tempTable = "#temp_none_between_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        String tempTable1 = "#temp_none_between1_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        String tempTable2 = "#temp_none_between2_" + ThreadLocalRandom.current().nextInt(1000, 9999);

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("none");

            // Create and populate temp tables
            try (PreparedStatement ps = conn.prepareStatement("CREATE TABLE " + tempTable + " (id INT)")) {
                ps.execute();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tempTable1 + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)")) {
                ps.execute();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tempTable2 + " (id INT PRIMARY KEY, description NVARCHAR(100))")) {
                ps.execute();
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + tempTable + " VALUES (999)")) {
                ps.execute();
            }

            String sql = "INSERT INTO " + tempTable1 + " VALUES (?, ?, ?); " +
                    "DELETE FROM " + tempTable + "; " +
                    "INSERT INTO " + tempTable2 + " VALUES (?, ?)";

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
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + tempTable);
                    ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(0, rs.getInt(1), "Temp table should be empty after DELETE");
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
                TestUtils.dropTableIfExists(tempTable1, stmt);
                TestUtils.dropTableIfExists(tempTable2, stmt);
            }
        }
    }

    /**
     * Test Case: Multiple parameters per statement with prepareMethod=none
     * SQL: "INSERT INTO table1 VALUES (?, ?, ?); INSERT INTO table2 VALUES (?, ?)"
     * Verifies literal parameter substitution handles multiple parameters correctly
     */
    @Test
    public void testBatchMultipleParametersPerStatementWithNone() throws Exception {
        String tempTable1 = "#temp_none_multi1_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        String tempTable2 = "#temp_none_multi2_" + ThreadLocalRandom.current().nextInt(1000, 9999);

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("none");

            // Create temp tables
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tempTable1 + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)")) {
                ps.execute();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tempTable2 + " (id INT PRIMARY KEY, description NVARCHAR(100))")) {
                ps.execute();
            }

            String sql = "INSERT INTO " + tempTable1 + " VALUES (?, ?, ?); " +
                    "INSERT INTO " + tempTable2 + " VALUES (?, ?)";

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
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + tempTable1);
                    ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(3, rs.getInt(1), "Table1 should have 3 rows");
            }

            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + tempTable2);
                    ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(3, rs.getInt(1), "Table2 should have 3 rows");
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable1, stmt);
                TestUtils.dropTableIfExists(tempTable2, stmt);
            }
        }
    }

    /**
     * Test Case: Only non-parameterized statements with prepareMethod=none
     * Verifies batch execution works correctly even without parameters
     */
    @Test
    public void testBatchOnlyNonParameterizedStatementsWithNone() throws Exception {
        String tempTable = "#temp_none_nonparam_" + ThreadLocalRandom.current().nextInt(1000, 9999);

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("none");

            // Create temp table using PreparedStatement
            try (PreparedStatement pstmt = conn.prepareStatement("CREATE TABLE " + tempTable + " (id INT)")) {
                pstmt.execute();
            }

            // SQL has no parameters, but we still call addBatch
            String sql = "INSERT INTO " + tempTable + " VALUES (1); ";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.addBatch();
                pstmt.addBatch(); // Adding same batch twice
                pstmt.addBatch();

                int[] updateCounts = pstmt.executeBatch();
                assertEquals(3, updateCounts.length);
            }

            // Verify 3 rows inserted
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + tempTable);
                    ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(3, rs.getInt(1), "Should have 3 rows inserted");
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
            }
        }
    }

    /**
     * Test Case: String literals and comments in SQL with prepareMethod=none
     * Verifies literal parameter substitution handles SQL parsing edge cases
     */
    @Test
    public void testBatchWithStringLiteralsAndCommentsWithNone() throws Exception {
        String tempTable1 = "#temp_none_literal1_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        String tempTable2 = "#temp_none_literal2_" + ThreadLocalRandom.current().nextInt(1000, 9999);

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("none");

            // Create temp tables
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tempTable1 + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)")) {
                ps.execute();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tempTable2 + " (id INT PRIMARY KEY, description NVARCHAR(100))")) {
                ps.execute();
            }

            String sql = "/* Comment with ; semicolon */ " +
                    "INSERT INTO " + tempTable1 + " VALUES (?, 'Name;with;semicolons', ?); " +
                    "-- Another comment\n" +
                    "INSERT INTO " + tempTable2 + " VALUES (?, 'Desc;test')";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setInt(1, 701);
                pstmt.setInt(2, 77);
                pstmt.setInt(3, 801);
                pstmt.addBatch();

                int[] updateCounts = pstmt.executeBatch();
                assertEquals(1, updateCounts.length);
            }

            // Verify data
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT t1.name, t2.description FROM " + tempTable1 + " t1 " +
                            "JOIN " + tempTable2 + " t2 ON t1.id - 700 = t2.id - 800");
                    ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("Name;with;semicolons", rs.getString(1));
                assertEquals("Desc;test", rs.getString(2));
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable1, stmt);
                TestUtils.dropTableIfExists(tempTable2, stmt);
            }
        }
    }

    /**
     * Test Case: Verify correct parameter mapping with prepareMethod=none
     * Ensures literal parameter substitution maps parameters to correct positions
     */
    @Test
    public void testBatchParameterMappingCorrectnessWithNone() throws Exception {
        String tempTable1 = "#temp_none_mapping1_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        String tempTable2 = "#temp_none_mapping2_" + ThreadLocalRandom.current().nextInt(1000, 9999);

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("none");

            // Create temp tables
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tempTable1 + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)")) {
                ps.execute();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tempTable2 + " (id INT PRIMARY KEY, description NVARCHAR(100))")) {
                ps.execute();
            }

            String sql = "INSERT INTO " + tempTable1 + " VALUES (?, ?, ?); " +
                    "INSERT INTO " + tempTable2 + " VALUES (?, ?)";

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

            // Verify the exact values were inserted in correct temp tables
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id, name, value FROM " + tempTable1 + " WHERE id = ?")) {
                ps.setInt(1, 1001);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1001, rs.getInt("id"));
                    assertEquals("Param2", rs.getString("name"));
                    assertEquals(3, rs.getInt("value"));
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id, description FROM " + tempTable2 + " WHERE id = ?")) {
                ps.setInt(1, 2001);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(2001, rs.getInt("id"));
                    assertEquals("Param5", rs.getString("description"));
                }
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable1, stmt);
                TestUtils.dropTableIfExists(tempTable2, stmt);
            }
        }
    }

    /**
     * Test Case: Batch with errors using prepareMethod=none
     * Verifies update counts reflect failures correctly with literal parameter
     * substitution
     */
    @Test
    public void testBatchCombinedWithErrorsWithNone() throws Exception {
        String tempTable = "#temp_none_errors_" + ThreadLocalRandom.current().nextInt(1000, 9999);

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("none");

            // Create temp table
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tempTable + " (id INT PRIMARY KEY, name NVARCHAR(50), value INT)")) {
                ps.execute();
            }

            String sql = "INSERT INTO " + tempTable + " VALUES (?, ?, ?)";

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

                // prepareMethod=none uses literal substitution and direct SQL execution
                // Expect SQLException (or subclass) for duplicate key violation
                // SQL Server error codes: 2627 (PK violation), 2601 (unique index violation)
                assertThrows(SQLException.class, () -> pstmt.executeBatch(),
                        "Should throw SQLException for duplicate key violation");
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
            }
        }
    }

    /**
     * Test Case: Large batch with prepareMethod=none
     * Verifies literal parameter substitution handles large batches correctly
     */
    @Test
    public void testLargeBatchWithNone() throws Exception {
        String tempTable = "#temp_none_large_" + ThreadLocalRandom.current().nextInt(1000, 9999);

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("none");

            // Create temp table
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tempTable + " (id INT, name VARCHAR(50), value DECIMAL(10,2))")) {
                ps.execute();
            }

            String sql = "INSERT INTO " + tempTable + " VALUES (?, ?, ?)";

            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                // Add 100 batches
                for (int i = 1; i <= 100; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setString(2, "Name" + i);
                    pstmt.setBigDecimal(3, new BigDecimal(i * 1.5));
                    pstmt.addBatch();
                }

                int[] updateCounts = pstmt.executeBatch();
                assertEquals(100, updateCounts.length, "Should execute 100 batch statements");

                // Verify all succeeded
                for (int count : updateCounts) {
                    assertTrue(count >= 0 || count == Statement.SUCCESS_NO_INFO,
                            "Each batch should succeed");
                }
            }

            // Verify all rows inserted
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + tempTable);
                    ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(100, rs.getInt(1), "Should have 100 rows inserted");
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tempTable, stmt);
            }
        }
    }

    // ==================== prepareMethod=none Connection String Tests
    // ====================
    // Tests that use prepareMethod=none directly in the connection string
    // instead of calling conn.setPrepareMethod("none")

    /**
     * Test prepareMethod=none set via connection string property
     * Verifies the connection property works correctly when specified in URL
     */
    @Test
    public void testPrepareMethodNoneViaConnectionString() throws SQLException {
        String tableName = "#temp_none_connstr_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        String connStrWithNone = connectionString + ";prepareMethod=none";

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrWithNone)) {
            // Verify the prepareMethod is set correctly
            assertEquals("none", conn.getPrepareMethod(), "prepareMethod should be 'none' from connection string");

            // Create temp table
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tableName + " (id INT, name VARCHAR(50))")) {
                ps.execute();
            }

            // Insert data - should use direct SQL execution
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + tableName + " (id, name) VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, "Test1");
                ps.executeUpdate();

                ps.setInt(1, 2);
                ps.setString(2, "Test2");
                ps.executeUpdate();
            }

            // Verify data persists across PreparedStatements (proves direct SQL execution)
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + tableName);
                    ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(2, rs.getInt(1), "Should have 2 rows inserted");
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * Test batch execution with prepareMethod=none set via connection string
     */
    @Test
    public void testBatchExecutionWithPrepareMethodNoneViaConnectionString() throws SQLException {
        String tableName = "#temp_none_batch_connstr_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        String connStrWithNone = connectionString + ";prepareMethod=none";

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrWithNone)) {
            // Create temp table
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tableName + " (id INT, name VARCHAR(50), value DECIMAL(10,2))")) {
                ps.execute();
            }

            // Batch insert using connection string prepareMethod=none
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + tableName + " VALUES (?, ?, ?)")) {
                for (int i = 1; i <= 10; i++) {
                    ps.setInt(1, i);
                    ps.setString(2, "BatchItem" + i);
                    ps.setBigDecimal(3, new BigDecimal(i * 5.5));
                    ps.addBatch();
                }

                int[] results = ps.executeBatch();
                assertEquals(10, results.length, "Should execute 10 batch statements");
            }

            // Verify all rows inserted
            try (PreparedStatement ps = conn.prepareStatement("SELECT COUNT(*) FROM " + tableName);
                    ResultSet rs = ps.executeQuery()) {
                rs.next();
                assertEquals(10, rs.getInt(1), "Should have 10 rows from batch insert");
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * Test multiple data types with prepareMethod=none set via connection string
     */
    @Test
    public void testMultipleDataTypesWithPrepareMethodNoneViaConnectionString() throws SQLException {
        String tableName = "#temp_none_types_connstr_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        String connStrWithNone = connectionString + ";prepareMethod=none";

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrWithNone)) {
            // Create table with various data types
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tableName + " (" +
                            "col_int INT, col_bigint BIGINT, col_decimal DECIMAL(18,6), " +
                            "col_varchar VARCHAR(100), col_nvarchar NVARCHAR(100), col_bit BIT)")) {
                ps.execute();
            }

            // Insert with various data types
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + tableName + " VALUES (?, ?, ?, ?, ?, ?)")) {
                ps.setInt(1, Integer.MAX_VALUE);
                ps.setLong(2, Long.MAX_VALUE);
                ps.setBigDecimal(3, new BigDecimal("999999.123456"));
                ps.setString(4, "ASCII text");
                ps.setNString(5, "Unicode: 日本語");
                ps.setBoolean(6, true);
                ps.executeUpdate();
            }

            // Verify data
            try (PreparedStatement ps = conn.prepareStatement("SELECT * FROM " + tableName);
                    ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(Integer.MAX_VALUE, rs.getInt("col_int"));
                assertEquals(Long.MAX_VALUE, rs.getLong("col_bigint"));
                assertEquals(new BigDecimal("999999.123456"), rs.getBigDecimal("col_decimal"));
                assertEquals("ASCII text", rs.getString("col_varchar"));
                assertEquals("Unicode: 日本語", rs.getNString("col_nvarchar"));
                assertTrue(rs.getBoolean("col_bit"));
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * Test SELECT query with parameters using prepareMethod=none via connection
     * string
     */
    @Test
    public void testSelectQueryWithPrepareMethodNoneViaConnectionString() throws SQLException {
        String tableName = "#temp_none_select_connstr_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        String connStrWithNone = connectionString + ";prepareMethod=none";

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrWithNone)) {
            // Create and populate table
            try (PreparedStatement ps = conn.prepareStatement(
                    "CREATE TABLE " + tableName + " (id INT, category VARCHAR(20), amount DECIMAL(10,2))")) {
                ps.execute();
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "INSERT INTO " + tableName + " VALUES (1, 'A', 100.00), (2, 'B', 200.00), " +
                            "(3, 'A', 150.00), (4, 'C', 300.00), (5, 'A', 250.00)")) {
                ps.execute();
            }

            // Parameterized SELECT query
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id, amount FROM " + tableName + " WHERE category = ? AND amount > ?")) {
                ps.setString(1, "A");
                ps.setBigDecimal(2, new BigDecimal("120.00"));

                try (ResultSet rs = ps.executeQuery()) {
                    int count = 0;
                    while (rs.next()) {
                        count++;
                        assertTrue(rs.getBigDecimal("amount").compareTo(new BigDecimal("120.00")) > 0);
                    }
                    assertEquals(2, count, "Should find 2 rows with category='A' and amount > 120");
                }
            }

            // Cleanup
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            }
        }
    }
}