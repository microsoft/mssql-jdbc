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
     * Comprehensive test for ALL data types parameter substitution with
     * scopeTempTablesToConnection method
     * Tests VARCHAR, NVARCHAR, INTEGER, DECIMAL, DATETIME, BINARY, and BIT in a
     * single query
     */
    @Test
    public void testAllDataTypesParameterSubstitution() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
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
                // For scopeTempTablesToConnection method, BigDecimal values may be formatted
                // differently but are
                // mathematically equal
                assertEquals(0, new BigDecimal("1.23E+15").compareTo(rs.getBigDecimal("test_decimal")),
                        "BigDecimal values should be mathematically equal");
                assertNotNull(rs.getTimestamp("test_datetime"));
                assertArrayEquals(new byte[]{0x01, 0x02, 0x03, (byte) 0xFF, 0x00, 0x7F}, rs.getBytes("test_binary"));
                assertFalse(rs.getBoolean("test_bit"));
            }
        }
    }

    /**
     * Test SQL injection prevention with scopeTempTablesToConnection method
     */
    @Test
    public void testSQLInjectionPrevention() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
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
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
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
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
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
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
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
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
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
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
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
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
            String sql = "SELECT ? as precise_time";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                
                // Test milliseconds precision
                Timestamp microTime = Timestamp.valueOf("2023-12-25 15:30:45.123456");
                ps.setTimestamp(1, microTime);
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next());
                Timestamp result = rs.getTimestamp("precise_time");
                assertEquals(microTime.getTime(), result.getTime(), "Milliseconds precision should be preserved");
                
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
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
            String sql = "SELECT ? as large_number";
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                
                // Test very large BigDecimal
                BigDecimal veryLarge = new BigDecimal("999999999999999999999999999999999999.999999999999999999");
                ps.setBigDecimal(1, veryLarge);
                ResultSet rs = ps.executeQuery();
                assertTrue(rs.next());
                BigDecimal result = rs.getBigDecimal("large_number");
                // SQL Server decimal has max precision of 38, so very large numbers may be
                // truncated
                // Just verify we got a non-null result
                assertNotNull(result, "Very large number should return a result");
                
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

    /**
     * Test edge case: very long strings
     */
    @ParameterizedTest
    @ValueSource(ints = {100, 1000, 4000, 8000})
    public void testLongStringParameters(int length) throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString)) {
            conn.setPrepareMethod("scopeTempTablesToConnection");
            
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
     * Basic performance test for scopeTempTablesToConnection vs other prepare
     * methods
     */
    @Test
    @Tag(Constants.PrepareMethodUseTempTableScopeTest)
    public void testBasicPerformanceComparison() throws SQLException {
        final int ITERATIONS = 50;
        String sql = "SELECT ? as test_value, ? as test_number";
        
        // Test PREPEXEC method
        long prepexecTime = measureExecutionTime("prepexec", sql, ITERATIONS);
        
        // Test PREPARE method  
        long prepareTime = measureExecutionTime("prepare", sql, ITERATIONS);
        
        // Test scopeTempTablesToConnection method
        long execTime = measureExecutionTime("scopeTempTablesToConnection", sql, ITERATIONS);
        
        System.out.println("Performance Results for " + ITERATIONS + " iterations:");
        System.out.println("PREPEXEC: " + prepexecTime + "ms");
        System.out.println("PREPARE:  " + prepareTime + "ms");
        System.out.println("scopeTempTablesToConnection:     " + execTime + "ms");
        
        // All methods should complete successfully
        assertTrue(prepexecTime > 0, "PREPEXEC should complete");
        assertTrue(prepareTime > 0, "PREPARE should complete");
        assertTrue(execTime > 0, "scopeTempTablesToConnection should complete");
        
        // scopeTempTablesToConnection method should be reasonably performant (within 5x
        // of other methods)
        double execVsPrepexec = (double) execTime / prepexecTime;
        double execVsPrepare = (double) execTime / prepareTime;
        
        System.out.println("Performance Ratios:");
        System.out.println("scopeTempTablesToConnection vs PREPEXEC: " + String.format("%.2f", execVsPrepexec));
        System.out.println("scopeTempTablesToConnection vs PREPARE:  " + String.format("%.2f", execVsPrepare));
        
        assertTrue(execVsPrepexec < 5.0, "scopeTempTablesToConnection should not be more than 5x slower than PREPEXEC");
        assertTrue(execVsPrepare < 5.0, "scopeTempTablesToConnection should not be more than 5x slower than PREPARE");
    }

    /**
     * Test repeated execution performance
     */
    @Test
    @Tag(Constants.PrepareMethodUseTempTableScopeTest)
    public void testRepeatedExecutionPerformance() throws SQLException {
        final int ITERATIONS = 50;
        String sql = "SELECT ? as repeated_value";
        
        // Test each method with repeated executions on same PreparedStatement
        long prepexecTime = measureRepeatedExecution("prepexec", sql, ITERATIONS);
        long prepareTime = measureRepeatedExecution("prepare", sql, ITERATIONS);
        long execTime = measureRepeatedExecution("scopeTempTablesToConnection", sql, ITERATIONS);
        
        System.out.println("Repeated Execution Performance (" + ITERATIONS + " executions on same PreparedStatement):");
        System.out.println("PREPEXEC: " + prepexecTime + "ms");
        System.out.println("PREPARE:  " + prepareTime + "ms");
        System.out.println("scopeTempTablesToConnection:     " + execTime + "ms");
        
        // All methods should complete
        assertTrue(prepexecTime > 0, "PREPEXEC repeated execution should work");
        assertTrue(prepareTime > 0, "PREPARE repeated execution should work");
        assertTrue(execTime > 0, "scopeTempTablesToConnection repeated execution should work");
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
}