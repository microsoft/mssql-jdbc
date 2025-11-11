/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.sql.rowset.serial.SerialClob;

import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.SqlServerPreparedStatementExpander.InputStreamWrapper;


/**
 * Test class for SqlServerPreparedStatementExpander.
 * Tests various scenarios including:
 * - Simple value substitution
 * - Unicode strings
 * - Booleans
 * - Dates, times, timestamps
 * - Decimals and numerics
 * - Binary data (byte arrays)
 * - CLOBs
 * - NULLs with operator rewriting (= ? -> IS NULL, <> ? -> IS NOT NULL, etc.)
 * - Placeholders inside strings and comments (should be ignored)
 * - Complex SQL with IN clauses, mixed operators
 */
public class SqlServerPreparedStatementExpanderTest {

    /**
     * Test basic INSERT with all common data types.
     */
    @Test
    public void testBasicInsertWithAllTypes() throws Exception {
        List<Object> params = new ArrayList<>();

        // 1: simple varchar containing single quote
        params.add("O'Reilly");

        // 2: nvarchar (unicode)
        params.add("महेन्द्र चव्हाण");

        // 3: boolean
        params.add(true);

        // 4: date
        params.add(Date.valueOf("2025-11-03"));

        // 5: time
        params.add(Time.valueOf("10:45:30"));

        // 6: timestamp
        params.add(Timestamp.valueOf("2025-11-03 10:45:30.123"));

        // 7: decimal
        params.add(new BigDecimal("98765.43"));

        // 8: binary blob
        params.add(new byte[] {(byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF});

        // 9: clob
        Clob clob = new SerialClob("Seasoned engineer with 17 years' experience.".toCharArray());
        params.add(clob);

        // 10: int
        params.add(101);

        // 11: NULL for terminated flag (should rewrite "= ?" -> "IS NULL")
        params.add(null);

        // 12: NULL for termination_reason
        params.add(null);

        String insertSql = "INSERT INTO employees (name, nickname, is_active, hire_date, login_time, last_updated, salary, photo, bio, manager_id, terminated, termination_reason) "
                + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        String result = SqlServerPreparedStatementExpander.expand(insertSql, params);

        // Verify the result contains expected values
        assertTrue(result.contains("N'O''Reilly'"), "Should escape single quote");
        assertTrue(result.contains("N'महेन्द्र चव्हाण'"), "Should handle unicode");
        assertTrue(result.contains("1"), "Boolean true should be 1");
        assertTrue(result.contains("CAST('2025-11-03' AS DATE)"), "Should format date");
        assertTrue(result.contains("CAST('10:45:30' AS TIME)"), "Should format time");
        assertTrue(result.contains("CAST('2025-11-03 10:45:30.123' AS DATETIME2)"), "Should format timestamp");
        assertTrue(result.contains("98765.43"), "Should format decimal");
        assertTrue(result.contains("0xDEADBEEF"), "Should format binary as hex");
        assertTrue(result.contains("N'Seasoned engineer with 17 years'' experience.'"),
                "Should handle CLOB with escaped quote");
        assertTrue(result.contains("101"), "Should format int");
        assertTrue(result.contains("NULL"), "Should handle NULL values");

        System.out.println("=== INSERT expanded ===");
        System.out.println(result);
    }

    /**
     * Test WHERE clause with NULL handling for = and <> operators.
     */
    @Test
    public void testWhereWithNullAndOperators() {
        List<Object> params = Arrays.asList(101, null, null);
        String whereSql = "SELECT * FROM employees WHERE manager_id = ? AND terminated = ? AND reason <> ?";

        String result = SqlServerPreparedStatementExpander.expand(whereSql, params);

        assertTrue(result.contains("manager_id = 101"), "Should substitute non-null value");
        assertTrue(result.contains("terminated IS NULL"), "= ? with NULL should become IS NULL");
        assertTrue(result.contains("reason IS NOT NULL"), "<> ? with NULL should become IS NOT NULL");

        System.out.println("\n=== WHERE with = and <> and nulls ===");
        System.out.println(result);
    }

    /**
     * Test IS and IS NOT forms with NULL.
     */
    @Test
    public void testIsAndIsNotForms() {
        List<Object> params = Arrays.asList(null, null);
        String whereSql = "SELECT * FROM employees WHERE terminated IS ? AND reason IS NOT ?";

        String result = SqlServerPreparedStatementExpander.expand(whereSql, params);

        assertTrue(result.contains("terminated IS NULL"), "IS ? with NULL should become IS NULL");
        assertTrue(result.contains("reason IS NOT NULL"), "IS NOT ? with NULL should become IS NOT NULL");

        System.out.println("\n=== IS / IS NOT forms ===");
        System.out.println(result);
    }

    /**
     * Test IN clause with NULL and mixed conditions.
     */
    @Test
    public void testInClauseWithMixedConditions() {
        String inSql = "SELECT * FROM t WHERE id IN (? ) AND name = ? AND meta <> ? ";
        List<Object> params = Arrays.asList(null, "Joe", null);

        String result = SqlServerPreparedStatementExpander.expand(inSql, params);

        assertTrue(result.contains("id IN (NULL )"), "IN with NULL should substitute NULL");
        assertTrue(result.contains("name = N'Joe'"), "Should substitute string value");
        assertTrue(result.contains("meta IS NOT NULL"), "<> ? with NULL should become IS NOT NULL");

        System.out.println("\n=== IN and mixed ===");
        System.out.println(result);
    }

    /**
     * Test that placeholders inside strings and comments are ignored.
     */
    @Test
    public void testPlaceholdersInStringsAndComments() {
        String tricky = "SELECT '?', col FROM t WHERE note = ? -- ? in comment\n AND description = '?' /* ? in block */ AND code = ?";
        List<Object> params = Arrays.asList("alpha", "beta");

        String result = SqlServerPreparedStatementExpander.expand(tricky, params);

        // The '?' inside strings and comments should remain as '?'
        assertTrue(result.contains("SELECT '?'"), "? in string literal should be preserved");
        assertTrue(result.contains("description = '?'"), "? in string literal should be preserved");
        assertTrue(result.contains("-- ? in comment"), "? in line comment should be preserved");
        assertTrue(result.contains("/* ? in block */"), "? in block comment should be preserved");

        // Only the actual placeholders should be replaced
        assertTrue(result.contains("note = N'alpha'"), "First placeholder should be replaced");
        assertTrue(result.contains("code = N'beta'"), "Second placeholder should be replaced");

        System.out.println("\n=== ignoring ? inside quotes/comments ===");
        System.out.println(result);
    }

    /**
     * Test != operator handling with NULL.
     */
    @Test
    public void testNotEqualsOperator() {
        String neq = "SELECT * FROM t WHERE status != ?";
        String result = SqlServerPreparedStatementExpander.expand(neq, Collections.singletonList(null));

        assertTrue(result.contains("status IS NOT NULL"), "!= ? with NULL should become IS NOT NULL");

        System.out.println("\n=== != handling ===");
        System.out.println(result);
    }

    /**
     * Test boolean values are converted to 1/0.
     */
    @Test
    public void testBooleanConversion() {
        String boolSql = "UPDATE t SET enabled = ? WHERE id = ?";
        String result = SqlServerPreparedStatementExpander.expand(boolSql, Arrays.asList(false, 55));

        assertTrue(result.contains("enabled = 0"), "Boolean false should be 0");
        assertTrue(result.contains("id = 55"), "Integer should be preserved");

        System.out.println("\n=== boolean 1/0 ===");
        System.out.println(result);
    }

    /**
     * Test binary data via InputStreamWrapper.
     */
    @Test
    public void testBinaryInsertViaInputStreamWrapper() {
        String binSql = "INSERT INTO files (data) VALUES (?)";
        String result = SqlServerPreparedStatementExpander.expand(binSql,
                Collections.singletonList(new InputStreamWrapper(new byte[] {0x01, 0x02, 0x03})));

        assertTrue(result.contains("0x010203"), "Binary data should be formatted as hex");

        System.out.println("\n=== binary insert ===");
        System.out.println(result);
    }

    /**
     * Test empty parameter list.
     */
    @Test
    public void testEmptyParameterList() {
        String sql = "SELECT * FROM t WHERE id = 1";
        String result = SqlServerPreparedStatementExpander.expand(sql, Collections.emptyList());

        assertEquals(sql, result, "SQL without parameters should remain unchanged");
    }

    /**
     * Test null SQL input.
     */
    @Test
    public void testNullSqlInput() {
        String result = SqlServerPreparedStatementExpander.expand(null, Arrays.asList(1, 2, 3));
        assertNull(result, "Null SQL should return null");
    }

    /**
     * Test null parameters list.
     */
    @Test
    public void testNullParametersList() {
        String sql = "SELECT * FROM t WHERE id = ?";
        String result = SqlServerPreparedStatementExpander.expand(sql, null);

        assertEquals(sql, result, "SQL with null parameters should remain unchanged");
    }

    /**
     * Test single quote escaping in strings.
     */
    @Test
    public void testSingleQuoteEscaping() {
        String sql = "INSERT INTO t (name) VALUES (?)";
        String result = SqlServerPreparedStatementExpander.expand(sql,
                Collections.singletonList("It's a test with 'quotes'"));

        assertTrue(result.contains("N'It''s a test with ''quotes'''"),
                "Single quotes should be doubled for escaping");
    }

    /**
     * Test numeric types.
     */
    @Test
    public void testNumericTypes() {
        String sql = "INSERT INTO t (a, b, c, d, e, f) VALUES (?, ?, ?, ?, ?, ?)";
        List<Object> params = Arrays.asList((byte) 127, (short) 32000, 2147483647, 9223372036854775807L, 3.14f,
                2.71828);

        String result = SqlServerPreparedStatementExpander.expand(sql, params);

        assertTrue(result.contains("127"), "Byte should be formatted");
        assertTrue(result.contains("32000"), "Short should be formatted");
        assertTrue(result.contains("2147483647"), "Int should be formatted");
        assertTrue(result.contains("9223372036854775807"), "Long should be formatted");
        assertTrue(result.contains("3.14"), "Float should be formatted");
        assertTrue(result.contains("2.71828"), "Double should be formatted");
    }

    /**
     * Test BigDecimal formatting.
     */
    @Test
    public void testBigDecimalFormatting() {
        String sql = "INSERT INTO t (price) VALUES (?)";
        String result = SqlServerPreparedStatementExpander.expand(sql,
                Collections.singletonList(new BigDecimal("123456789.987654321")));

        assertTrue(result.contains("123456789.987654321"), "BigDecimal should preserve precision");
    }

    /**
     * Test byte array formatting.
     */
    @Test
    public void testByteArrayFormatting() {
        String sql = "INSERT INTO t (data) VALUES (?)";
        byte[] data = new byte[] {(byte) 0xFF, (byte) 0xAB, (byte) 0xCD, (byte) 0xEF};
        String result = SqlServerPreparedStatementExpander.expand(sql, Collections.singletonList(data));

        assertTrue(result.contains("0xFFABCDEF"), "Byte array should be formatted as uppercase hex");
    }

    /**
     * Test empty byte array.
     */
    @Test
    public void testEmptyByteArray() {
        String sql = "INSERT INTO t (data) VALUES (?)";
        String result = SqlServerPreparedStatementExpander.expand(sql, Collections.singletonList(new byte[0]));

        assertTrue(result.contains("0x"), "Empty byte array should be 0x");
    }

    /**
     * Test Date formatting.
     */
    @Test
    public void testDateFormatting() {
        String sql = "INSERT INTO t (hire_date) VALUES (?)";
        String result = SqlServerPreparedStatementExpander.expand(sql, Collections.singletonList(Date.valueOf("2025-01-15")));

        assertTrue(result.contains("CAST('2025-01-15' AS DATE)"), "Date should be properly formatted");
    }

    /**
     * Test Time formatting.
     */
    @Test
    public void testTimeFormatting() {
        String sql = "INSERT INTO t (login_time) VALUES (?)";
        String result = SqlServerPreparedStatementExpander.expand(sql, Collections.singletonList(Time.valueOf("14:30:45")));

        assertTrue(result.contains("CAST('14:30:45' AS TIME)"), "Time should be properly formatted");
    }

    /**
     * Test Timestamp formatting.
     */
    @Test
    public void testTimestampFormatting() {
        String sql = "INSERT INTO t (updated_at) VALUES (?)";
        String result = SqlServerPreparedStatementExpander.expand(sql,
                Collections.singletonList(Timestamp.valueOf("2025-11-03 15:45:30.456")));

        assertTrue(result.contains("CAST('2025-11-03 15:45:30.456' AS DATETIME2)"),
                "Timestamp should be properly formatted");
    }

    /**
     * Test complex WHERE clause with multiple NULL rewrites.
     */
    @Test
    public void testComplexWhereWithMultipleNullRewrites() {
        String sql = "SELECT * FROM emp WHERE active = ? OR deleted <> ? AND manager_id != ? OR notes IS ?";
        List<Object> params = Arrays.asList(null, null, null, null);

        String result = SqlServerPreparedStatementExpander.expand(sql, params);

        assertTrue(result.contains("active IS NULL"), "= ? should become IS NULL");
        assertTrue(result.contains("deleted IS NOT NULL"), "<> ? should become IS NOT NULL");
        assertTrue(result.contains("manager_id IS NOT NULL"), "!= ? should become IS NOT NULL");
        assertTrue(result.contains("notes IS NULL"), "IS ? should become IS NULL");
    }

    /**
     * Test Character type.
     */
    @Test
    public void testCharacterType() {
        String sql = "INSERT INTO t (initial) VALUES (?)";
        String result = SqlServerPreparedStatementExpander.expand(sql, Collections.singletonList('X'));

        assertTrue(result.contains("N'X'"), "Character should be formatted as string");
    }

    /**
     * Test doubled quotes in string literals are preserved.
     */
    @Test
    public void testDoubledQuotesInStrings() {
        String sql = "SELECT * FROM t WHERE description = 'It''s quoted' AND note = ?";
        String result = SqlServerPreparedStatementExpander.expand(sql, Collections.singletonList("test"));

        assertTrue(result.contains("'It''s quoted'"), "Doubled quotes in original SQL should be preserved");
        assertTrue(result.contains("N'test'"), "Parameter should be replaced");
    }

    /**
     * Test multiple placeholders in complex nested SQL.
     */
    @Test
    public void testMultiplePlaceholdersComplexSQL() {
        String sql = "UPDATE t SET col1 = ?, col2 = ? WHERE id = ? AND (status = ? OR flag <> ?)";
        List<Object> params = Arrays.asList("value1", 42, 100, null, null);

        String result = SqlServerPreparedStatementExpander.expand(sql, params);

        assertTrue(result.contains("col1 = N'value1'"));
        assertTrue(result.contains("col2 = 42"));
        assertTrue(result.contains("id = 100"));
        assertTrue(result.contains("status IS NULL"));
        assertTrue(result.contains("flag IS NOT NULL"));
    }

    /**
     * Test UPDATE SET clause with NULL value (should NOT be rewritten to IS NULL).
     */
    @Test
    public void testUpdateSetWithNull() {
        String sql = "UPDATE employees SET manager_id = ?, notes = ? WHERE id = ?";
        List<Object> params = Arrays.asList(null, null, 123);

        String result = SqlServerPreparedStatementExpander.expand(sql, params);

        // In SET clause, "= NULL" should remain "= NULL", not be rewritten to "IS NULL"
        assertTrue(result.contains("manager_id = NULL"), "SET clause should use '= NULL'");
        assertTrue(result.contains("notes = NULL"), "SET clause should use '= NULL'");
        assertTrue(result.contains("id = 123"), "WHERE clause parameter should be replaced");
        assertFalse(result.contains("IS NULL"), "SET clause should not use 'IS NULL'");
    }

    /**
     * Test UPDATE with NULL in both SET and WHERE clauses.
     */
    @Test
    public void testUpdateSetAndWhereWithNull() {
        String sql = "UPDATE employees SET status = ? WHERE manager_id = ?";
        List<Object> params = Arrays.asList(null, null);

        String result = SqlServerPreparedStatementExpander.expand(sql, params);

        // First NULL is in SET clause (keep = NULL)
        assertTrue(result.contains("SET status = NULL"), "SET clause should use '= NULL'");
        // Second NULL is in WHERE clause (rewrite to IS NULL)
        assertTrue(result.contains("WHERE manager_id IS NULL"), "WHERE clause should use 'IS NULL'");
    }
}
