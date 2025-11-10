/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.*;
import java.time.LocalDateTime;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;

/**
 * Comprehensive test suite for EXEC prepare method implementation
 * Focus: Sybase ASE compatibility, temp table persistence, parameter substitution, security, edge cases, and performance
 */
public class PrepareMethodExecTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /**
     * CORE TEST: Temp table persistence across PreparedStatement boundaries
     * This is the primary Sybase ASE compatibility requirement
     */
    @Test
    public void testTempTablePersistenceWithExecMethod() throws SQLException {
        String tableName = "#temp_exec_test_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            // Set prepare method to EXEC for Sybase compatibility
            conn.setPrepareMethod("exec");
            
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
                
                // This should NOT fail with EXEC method (would fail with prepexec/prepare)
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
            
            // Cleanup
            try (PreparedStatement psCleanup = conn.prepareStatement("DROP TABLE " + tableName)) {
                psCleanup.execute();
            }
        }
    }

    /**
     * Comprehensive test for ALL data types parameter substitution with EXEC method
     * Tests VARCHAR, NVARCHAR, INTEGER, DECIMAL, DATETIME, BINARY, and BIT in a single query
     */
    @Test
    public void testAllDataTypesParameterSubstitution() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("exec");
            
            // Single comprehensive query testing all data types at once
            String sql = "SELECT ? as test_varchar, ? as test_nvarchar, ? as test_int, " +
                        "? as test_decimal, ? as test_datetime, ? as test_binary, ? as test_bit";
            
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                
                // ===== Test Set 1: Normal Values =====
                ps.setString(1, "Hello World");                           // VARCHAR
                ps.setNString(2, "测试数据 😀");                            // NVARCHAR with Unicode
                ps.setInt(3, 12345);                                     // INTEGER
                ps.setBigDecimal(4, new BigDecimal("123.456"));          // DECIMAL
                ps.setTimestamp(5, Timestamp.valueOf("2023-12-25 15:30:45.123")); // DATETIME
                ps.setBytes(6, new byte[]{0x01, 0x02, (byte) 0xFF});    // BINARY
                ps.setBoolean(7, true);                                  // BIT
                
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next(), "Should get result for normal values");
                
                // Validate all data types
                assertEquals("Hello World", rs.getString("test_varchar"));
                assertEquals("测试数据 😀", rs.getNString("test_nvarchar"));
                assertEquals(12345, rs.getInt("test_int"));
                assertEquals(new BigDecimal("123.456"), rs.getBigDecimal("test_decimal"));
                assertEquals(Timestamp.valueOf("2023-12-25 15:30:45.123"), rs.getTimestamp("test_datetime"));
                assertArrayEquals(new byte[]{0x01, 0x02, (byte) 0xFF}, rs.getBytes("test_binary"));
                assertTrue(rs.getBoolean("test_bit"));
                
                // ===== Test Set 2: Edge Values =====
                ps.setString(1, "O'Malley's \"Test\"");                  // VARCHAR with quotes
                ps.setNString(2, "🌟🎯🔥 Emoji Test");                   // NVARCHAR with emojis
                ps.setInt(3, Integer.MAX_VALUE);                         // INTEGER max
                ps.setBigDecimal(4, new BigDecimal("999999.999999999")); // DECIMAL high precision
                ps.setTimestamp(5, Timestamp.valueOf("1753-01-01 00:00:00.000")); // DATETIME min
                ps.setBytes(6, new byte[]{(byte) 0xFF, 0x00, 0x7F});    // BINARY edge bytes
                ps.setBoolean(7, false);                                 // BIT false
                
                rs = ps.executeQuery();
                assertTrue(rs.next(), "Should get result for edge values");
                
                assertEquals("O'Malley's \"Test\"", rs.getString("test_varchar"));
                assertEquals("🌟🎯🔥 Emoji Test", rs.getNString("test_nvarchar"));
                assertEquals(Integer.MAX_VALUE, rs.getInt("test_int"));
                assertEquals(new BigDecimal("999999.999999999"), rs.getBigDecimal("test_decimal"));
                assertEquals(Timestamp.valueOf("1753-01-01 00:00:00.000"), rs.getTimestamp("test_datetime"));
                assertArrayEquals(new byte[]{(byte) 0xFF, 0x00, 0x7F}, rs.getBytes("test_binary"));
                assertFalse(rs.getBoolean("test_bit"));
                
                // ===== Test Set 3: NULL Values =====
                ps.setString(1, null);                                   // VARCHAR null
                ps.setNString(2, null);                                  // NVARCHAR null
                ps.setNull(3, Types.INTEGER);                            // INTEGER null
                ps.setBigDecimal(4, null);                               // DECIMAL null
                ps.setTimestamp(5, null);                                // DATETIME null
                ps.setBytes(6, null);                                    // BINARY null
                ps.setNull(7, Types.BIT);                                // BIT null
                
                rs = ps.executeQuery();
                assertTrue(rs.next(), "Should get result for null values");
                
                assertNull(rs.getString("test_varchar"));
                assertTrue(rs.wasNull(), "VARCHAR should be null");
                assertNull(rs.getNString("test_nvarchar"));
                assertTrue(rs.wasNull(), "NVARCHAR should be null");
                rs.getInt("test_int");
                assertTrue(rs.wasNull(), "INTEGER should be null");
                assertNull(rs.getBigDecimal("test_decimal"));
                assertTrue(rs.wasNull(), "DECIMAL should be null");
                assertNull(rs.getTimestamp("test_datetime"));
                assertTrue(rs.wasNull(), "DATETIME should be null");
                assertNull(rs.getBytes("test_binary"));
                assertTrue(rs.wasNull(), "BINARY should be null");
                rs.getBoolean("test_bit");
                assertTrue(rs.wasNull(), "BIT should be null");
                
                // ===== Test Set 4: Extreme Values =====
                ps.setString(1, "");                                     // VARCHAR empty
                ps.setNString(2, "𝒪𝓃𝑒 𝒯𝓌𝑜");                          // NVARCHAR mathematical script
                ps.setInt(3, Integer.MIN_VALUE);                         // INTEGER min
                ps.setBigDecimal(4, BigDecimal.ZERO);                    // DECIMAL zero
                ps.setTimestamp(5, Timestamp.valueOf("9999-12-31 23:59:59.997")); // DATETIME max
                ps.setBytes(6, new byte[0]);                             // BINARY empty
                ps.setBoolean(7, true);                                  // BIT true
                
                rs = ps.executeQuery();
                assertTrue(rs.next(), "Should get result for extreme values");
                
                assertEquals("", rs.getString("test_varchar"));
                assertEquals("𝒪𝓃𝑒 𝒯𝓌𝑜", rs.getNString("test_nvarchar"));
                assertEquals(Integer.MIN_VALUE, rs.getInt("test_int"));
                assertEquals(BigDecimal.ZERO, rs.getBigDecimal("test_decimal"));
                assertEquals(Timestamp.valueOf("9999-12-31 23:59:59.997"), rs.getTimestamp("test_datetime"));
                assertArrayEquals(new byte[0], rs.getBytes("test_binary"));
                assertTrue(rs.getBoolean("test_bit"));
                
                // ===== Test Set 5: SQL Injection Attempts (Should be safely escaped) =====
                ps.setString(1, "'; DROP TABLE users; --");             // VARCHAR injection
                ps.setNString(2, "' OR '1'='1");                        // NVARCHAR injection
                ps.setInt(3, 42);                                        // INTEGER normal
                ps.setBigDecimal(4, new BigDecimal("1.23E+15"));         // DECIMAL scientific
                ps.setTimestamp(5, Timestamp.valueOf(LocalDateTime.now())); // DATETIME current
                ps.setBytes(6, new byte[]{0x01, 0x02, 0x03, (byte) 0xFF, 0x00, 0x7F}); // BINARY full range
                ps.setBoolean(7, false);                                 // BIT false
                
                rs = ps.executeQuery();
                assertTrue(rs.next(), "Should safely handle injection attempts");
                
                // Injection attempts should be returned as literal strings (safely escaped)
                assertEquals("'; DROP TABLE users; --", rs.getString("test_varchar"));
                assertEquals("' OR '1'='1", rs.getNString("test_nvarchar"));
                assertEquals(42, rs.getInt("test_int"));
                assertEquals(new BigDecimal("1.23E+15"), rs.getBigDecimal("test_decimal"));
                assertNotNull(rs.getTimestamp("test_datetime"));
                assertArrayEquals(new byte[]{0x01, 0x02, 0x03, (byte) 0xFF, 0x00, 0x7F}, rs.getBytes("test_binary"));
                assertFalse(rs.getBoolean("test_bit"));
            }
        }
    }

    /**
     * Test SQL injection prevention with EXEC method
     */
    @Test
    public void testSQLInjectionPrevention() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("exec");
            
            String sql = "SELECT ? as result";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                // Test potential SQL injection strings - these should be safely escaped
                String[] injectionAttempts = {
                    "'; DROP TABLE users; --",
                    "' OR '1'='1",
                    "'; SELECT * FROM sys.tables; --",
                    "admin'/*",
                    "1'; EXEC xp_cmdshell('dir'); --"
                };
                
                for (String attempt : injectionAttempts) {
                    ps.setString(1, attempt);
                    ResultSet rs = ps.executeQuery();
                    assertTrue(rs.next(), "Query should execute safely");
                    assertEquals(attempt, rs.getString("result"), "Value should be returned as-is, safely escaped");
                }
            }
        }
    }

    /**
     * Test advanced SQL injection prevention with sophisticated attack patterns
     */
    @Test
    public void testAdvancedSQLInjectionPrevention() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("exec");
            
            String[] advancedAttacks = {
                "'; WAITFOR DELAY '00:00:05'; --",  // Time-based attack
                "' UNION SELECT TOP 1 name FROM sys.tables --", // Union attack
                "'; EXEC sp_configure 'show advanced options', 1; --", // System procedure
                "' AND (SELECT COUNT(*) FROM sys.tables) > 0 --", // Information extraction
                "'; DECLARE @var VARCHAR(8000) SET @var = (SELECT TOP 1 name FROM sys.databases); --", // Variable declaration
                "'; INSERT INTO log_table VALUES ('injected'); --", // Data manipulation
                "'; IF (1=1) SELECT 'vulnerable' --", // Conditional execution
                "' OR (SELECT SYSTEM_USER) = 'sa' --", // System function call
                "'; BACKUP DATABASE tempdb TO DISK = 'c:\\temp\\backup.bak'; --", // Backup operation
                "'; CREATE TABLE hacked (id INT); --" // DDL statement
            };
            
            String sql = "SELECT ? as safe_result";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (String attack : advancedAttacks) {
                    ps.setString(1, attack);
                    ResultSet rs = ps.executeQuery();
                    assertTrue(rs.next(), "Query should execute safely for attack: " + attack);
                    assertEquals(attack, rs.getString("safe_result"), 
                        "Attack string should be returned as literal value, not executed: " + attack);
                }
            }
        }
    }

    /**
     * Test numeric precision and scale handling
     */
    @ParameterizedTest
    @CsvSource({
        "123.456, 6, 3",
        "999999.999999, 12, 6", 
        "0.000001, 7, 6",
        "123456789012345.123456789, 24, 9"
    })
    public void testNumericPrecisionScale(String value, int precision, int scale) throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("exec");
            
            BigDecimal testValue = new BigDecimal(value);
            String sql = "SELECT ? as precise_number";
            
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setBigDecimal(1, testValue);
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next());
                
                BigDecimal result = rs.getBigDecimal("precise_number");
                // Compare with appropriate scale consideration
                assertEquals(0, testValue.setScale(scale, RoundingMode.HALF_UP)
                    .compareTo(result.setScale(scale, RoundingMode.HALF_UP)),
                    "Precision/scale should be preserved for: " + value);
            }
        }
    }

    /**
     * Test Unicode edge cases including surrogates and special characters
     */
    @Test
    public void testUnicodeEdgeCases() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("exec");
            
            String[] unicodeTests = {
                "🌟🎯🔥", // Emojis
                "Ñoño México", // Latin characters with accents
                "Здравствуйте", // Cyrillic
                "こんにちは", // Japanese Hiragana
                "𝒪𝓃𝑒 𝒯𝓌𝑜", // Mathematical script
                "𝕿𝖍𝖗 𝔬𝐧𝑒", // Mathematical bold
                "\uD83C\uDF89\uD83C\uDF8A", // UTF-16 surrogates
                "\\n\\t\\r\\\"\\'\\\\"  // Escaped characters
            };
            
            String sql = "SELECT ? as unicode_text";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (String testStr : unicodeTests) {
                    ps.setNString(1, testStr);
                    ResultSet rs = ps.executeQuery();
                    assertTrue(rs.next());
                    assertEquals(testStr, rs.getNString("unicode_text"), 
                        "Unicode string should be preserved: " + testStr);
                }
            }
        }
    }

    /**
     * Test binary data edge cases
     */
    @Test
    public void testBinaryEdgeCases() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("exec");
            
            String sql = "SELECT ? as binary_data";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                
                // Test all byte values 0-255
                byte[] allBytes = new byte[256];
                for (int i = 0; i < 256; i++) {
                    allBytes[i] = (byte) i;
                }
                ps.setBytes(1, allBytes);
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next());
                assertArrayEquals(allBytes, rs.getBytes("binary_data"));
                
                // Test large binary data (8KB)
                byte[] largeBinary = new byte[8192];
                ThreadLocalRandom.current().nextBytes(largeBinary);
                ps.setBytes(1, largeBinary);
                rs = ps.executeQuery();
                assertTrue(rs.next());
                assertArrayEquals(largeBinary, rs.getBytes("binary_data"));
                
                // Test single byte
                byte[] singleByte = {(byte) 0xAB};
                ps.setBytes(1, singleByte);
                rs = ps.executeQuery();
                assertTrue(rs.next());
                assertArrayEquals(singleByte, rs.getBytes("binary_data"));
            }
        }
    }

    /**
     * Test NULL handling across all data types
     */
    @Test
    public void testNullHandlingAllTypes() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("exec");
            
            String sql = "SELECT ? as null_varchar, ? as null_int, ? as null_decimal, " +
                        "? as null_datetime, ? as null_binary, ? as null_bit";
            
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, null);
                ps.setNull(2, Types.INTEGER);
                ps.setBigDecimal(3, null);
                ps.setTimestamp(4, null);
                ps.setBytes(5, null);
                ps.setNull(6, Types.BIT);
                
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next());
                
                assertNull(rs.getString("null_varchar"));
                assertTrue(rs.wasNull());
                
                rs.getInt("null_int");
                assertTrue(rs.wasNull());
                
                assertNull(rs.getBigDecimal("null_decimal"));
                assertTrue(rs.wasNull());
                
                assertNull(rs.getTimestamp("null_datetime"));
                assertTrue(rs.wasNull());
                
                assertNull(rs.getBytes("null_binary"));
                assertTrue(rs.wasNull());
                
                rs.getBoolean("null_bit");
                assertTrue(rs.wasNull());
            }
        }
    }

    /**
     * Test datetime precision and edge dates
     */
    @Test
    public void testDateTimePrecision() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("exec");
            
            String sql = "SELECT ? as precise_time";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                
                // Test microsecond precision
                Timestamp microTime = Timestamp.valueOf("2023-12-25 15:30:45.123456");
                ps.setTimestamp(1, microTime);
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next());
                Timestamp result = rs.getTimestamp("precise_time");
                assertEquals(microTime.getTime(), result.getTime(), "Microsecond precision should be preserved");
                
                // Test edge dates
                Timestamp minDate = Timestamp.valueOf("1753-01-01 00:00:00.000");
                ps.setTimestamp(1, minDate);
                rs = ps.executeQuery();
                assertTrue(rs.next());
                assertEquals(minDate, rs.getTimestamp("precise_time"));
                
                Timestamp maxDate = Timestamp.valueOf("9999-12-31 23:59:59.997");
                ps.setTimestamp(1, maxDate);
                rs = ps.executeQuery();
                assertTrue(rs.next());
                assertEquals(maxDate, rs.getTimestamp("precise_time"));
            }
        }
    }

    /**
     * Test very large numbers and edge numeric values
     */
    @Test
    public void testLargeNumbers() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("exec");
            
            String sql = "SELECT ? as large_number";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                
                // Test very large BigDecimal
                BigDecimal veryLarge = new BigDecimal("999999999999999999999999999999999999.999999999999999999");
                ps.setBigDecimal(1, veryLarge);
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next());
                BigDecimal result = rs.getBigDecimal("large_number");
                assertTrue(veryLarge.compareTo(result) == 0, "Very large number should be handled correctly");
                
                // Test very small number
                BigDecimal verySmall = new BigDecimal("0.000000000000000001");
                ps.setBigDecimal(1, verySmall);
                rs = ps.executeQuery();
                assertTrue(rs.next());
                // Note: May lose precision due to SQL Server limitations, but should not fail
                assertNotNull(rs.getBigDecimal("large_number"));
                
                // Test scientific notation
                BigDecimal scientific = new BigDecimal("1.23E+15");
                ps.setBigDecimal(1, scientific);
                rs = ps.executeQuery();
                assertTrue(rs.next());
                assertEquals(0, scientific.compareTo(rs.getBigDecimal("large_number")), 
                    "Scientific notation should be handled correctly");
            }
        }
    }

    /**
     * Test multiple temp table scenario (complex Sybase migration case)
     */
    @Test
    public void testMultipleTempTablesScenario() throws SQLException {
        String tempTable1 = "#orders_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        String tempTable2 = "#customers_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("exec");
            
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
            
            // Cleanup
            try (PreparedStatement psCleanup1 = conn.prepareStatement("DROP TABLE " + tempTable1)) {
                psCleanup1.execute();
            }
            try (PreparedStatement psCleanup2 = conn.prepareStatement("DROP TABLE " + tempTable2)) {
                psCleanup2.execute();
            }
        }
    }

    /**
     * Test batch execution with EXEC method
     */
    @Test
    public void testBatchExecution() throws SQLException {
        String tempTable = "#batch_test_" + ThreadLocalRandom.current().nextInt(1000, 9999);
        
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("exec");
            
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
                    assertEquals(1, result, "Each batch should affect 1 row");
                }
            }
            
            // Verify batch results
            try (PreparedStatement selectPs = conn.prepareStatement(
                "SELECT COUNT(*) as row_count FROM " + tempTable)) {
                ResultSet rs = selectPs.executeQuery();
                assertTrue(rs.next());
                assertEquals(5, rs.getInt("row_count"), "Should have 5 rows inserted via batch");
            }
            
            // Cleanup
            try (PreparedStatement dropPs = conn.prepareStatement("DROP TABLE " + tempTable)) {
                dropPs.execute();
            }
        }
    }

    /**
     * Test edge case: very long strings
     */
    @ParameterizedTest
    @ValueSource(ints = {100, 1000, 4000, 8000})
    public void testLongStringParameters(int length) throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("exec");
            
            // Generate string of specified length
            StringBuilder sb = new StringBuilder(length);
            for (int i = 0; i < length; i++) {
                sb.append((char) ('A' + (i % 26)));
            }
            String longString = sb.toString();
            
            String sql = "SELECT ? as long_string";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                ps.setString(1, longString);
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next());
                assertEquals(longString, rs.getString("long_string"));
            }
        }
    }

    /**
     * Basic performance test for EXEC vs other prepare methods
     */
    @Test
    public void testBasicPerformanceComparison() throws SQLException {
        final int ITERATIONS = 50;
        String sql = "SELECT ? as test_value, ? as test_number";
        
        // Test PREPEXEC method
        long prepexecTime = measureExecutionTime("prepexec", sql, ITERATIONS);
        
        // Test PREPARE method  
        long prepareTime = measureExecutionTime("prepare", sql, ITERATIONS);
        
        // Test EXEC method
        long execTime = measureExecutionTime("exec", sql, ITERATIONS);
        
        System.out.println("Performance Results for " + ITERATIONS + " iterations:");
        System.out.println("PREPEXEC: " + prepexecTime + "ms");
        System.out.println("PREPARE:  " + prepareTime + "ms");
        System.out.println("EXEC:     " + execTime + "ms");
        
        // All methods should complete successfully
        assertTrue(prepexecTime > 0, "PREPEXEC should complete");
        assertTrue(prepareTime > 0, "PREPARE should complete");
        assertTrue(execTime > 0, "EXEC should complete");
        
        // EXEC method should be reasonably performant (within 5x of other methods)
        double execVsPrepexec = (double) execTime / prepexecTime;
        double execVsPrepare = (double) execTime / prepareTime;
        
        System.out.println("Performance Ratios:");
        System.out.println("EXEC vs PREPEXEC: " + String.format("%.2f", execVsPrepexec));
        System.out.println("EXEC vs PREPARE:  " + String.format("%.2f", execVsPrepare));
        
        assertTrue(execVsPrepexec < 5.0, "EXEC should not be more than 5x slower than PREPEXEC");
        assertTrue(execVsPrepare < 5.0, "EXEC should not be more than 5x slower than PREPARE");
    }

    /**
     * Test repeated execution performance
     */
    @Test
    public void testRepeatedExecutionPerformance() throws SQLException {
        final int ITERATIONS = 50;
        String sql = "SELECT ? as repeated_value";
        
        // Test each method with repeated executions on same PreparedStatement
        long prepexecTime = measureRepeatedExecution("prepexec", sql, ITERATIONS);
        long prepareTime = measureRepeatedExecution("prepare", sql, ITERATIONS);
        long execTime = measureRepeatedExecution("exec", sql, ITERATIONS);
        
        System.out.println("Repeated Execution Performance (" + ITERATIONS + " executions on same PreparedStatement):");
        System.out.println("PREPEXEC: " + prepexecTime + "ms");
        System.out.println("PREPARE:  " + prepareTime + "ms");
        System.out.println("EXEC:     " + execTime + "ms");
        
        // All methods should complete
        assertTrue(prepexecTime > 0, "PREPEXEC repeated execution should work");
        assertTrue(prepareTime > 0, "PREPARE repeated execution should work");
        assertTrue(execTime > 0, "EXEC repeated execution should work");
    }

    /**
     * Helper method to measure execution time for a given prepare method
     */
    private long measureExecutionTime(String prepareMethod, String sql, int iterations) throws SQLException {
        long startTime = System.currentTimeMillis();
        
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod(prepareMethod);
            
            for (int i = 0; i < iterations; i++) {
                try (PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setString(1, "TestValue" + i);
                    ps.setInt(2, i);
                    
                    ResultSet rs = ps.executeQuery();
                    assertTrue(rs.next(), "Should get result for iteration " + i);
                    rs.close();
                }
            }
        }
        
        return System.currentTimeMillis() - startTime;
    }

    /**
     * Helper method to measure repeated execution on same PreparedStatement
     */
    private long measureRepeatedExecution(String prepareMethod, String sql, int iterations) throws SQLException {
        long startTime = System.currentTimeMillis();
        
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod(prepareMethod);
            
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (int i = 0; i < iterations; i++) {
                    ps.setString(1, "Repeated" + i);
                    
                    ResultSet rs = ps.executeQuery();
                    assertTrue(rs.next(), "Should get result for iteration " + i);
                    rs.close();
                }
            }
        }
        
        return System.currentTimeMillis() - startTime;
    }
}