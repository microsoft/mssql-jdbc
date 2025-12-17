/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test cases for formatLiteralValue method with numeric types including MIN and MAX values
 * This test is in the same package so it can access the package-private formatLiteralValue method directly
 */
public class FormatLiteralValueNumericTest {

    private SQLServerConnection connection;

    @BeforeEach
    public void setUp() throws Exception {
        // Create a minimal SQLServerConnection for testing
        // We don't need a real database connection for testing formatLiteralValue
        connection = new SQLServerConnection("");
    }

    @Test
    public void testByteMinMax() throws SQLException {
        
        // Test Byte.MIN_VALUE
        String result = connection.formatLiteralValue(Byte.MIN_VALUE);
        assertEquals("-128", result);
        
        // Test Byte.MAX_VALUE  
        result = connection.formatLiteralValue(Byte.MAX_VALUE);
        assertEquals("127", result);
        
        // Test zero
        result = connection.formatLiteralValue((byte) 0);
        assertEquals("0", result);
    }

    @Test
    public void testShortMinMax() throws SQLException {
        
        // Test Short.MIN_VALUE
        String result = connection.formatLiteralValue(Short.MIN_VALUE);
        assertEquals("-32768", result);
        
        // Test Short.MAX_VALUE
        result = connection.formatLiteralValue(Short.MAX_VALUE);
        assertEquals("32767", result);
    }

    @Test
    public void testIntegerMinMax() throws SQLException {
        
        // Test Integer.MIN_VALUE
        String result = connection.formatLiteralValue(Integer.MIN_VALUE);
        assertEquals("-2147483648", result);
        
        // Test Integer.MAX_VALUE
        result = connection.formatLiteralValue(Integer.MAX_VALUE);
        assertEquals("2147483647", result);
    }

    @Test
    public void testLongMinMax() throws SQLException {
        
        // Test Long.MIN_VALUE
        String result = connection.formatLiteralValue(Long.MIN_VALUE);
        assertEquals("-9223372036854775808", result);
        
        // Test Long.MAX_VALUE
        result = connection.formatLiteralValue(Long.MAX_VALUE);
        assertEquals("9223372036854775807", result);
    }

    @Test
    public void testFloatMinMax() throws SQLException {
        
        
        // Test Float.MIN_VALUE (smallest positive value)
        String result = connection.formatLiteralValue(Float.MIN_VALUE);
        assertEquals(new BigDecimal(Float.toString(Float.MIN_VALUE)).toPlainString(), result);
        
        // Test Float.MAX_VALUE
        result = connection.formatLiteralValue(Float.MAX_VALUE);
        assertEquals(new BigDecimal(Float.toString(Float.MAX_VALUE)).toPlainString(), result);
        
        // Test negative Float.MAX_VALUE
        result = connection.formatLiteralValue(-Float.MAX_VALUE);
        assertEquals(new BigDecimal(Float.toString(-Float.MAX_VALUE)).toPlainString(), result);
        
        // Test Float.MIN_NORMAL
        result = connection.formatLiteralValue(Float.MIN_NORMAL);
        assertEquals(new BigDecimal(Float.toString(Float.MIN_NORMAL)).toPlainString(), result);
    }

    @Test
    public void testDoubleMinMax() throws SQLException {
        
        
        // Test Double.MIN_VALUE (smallest positive value)
        String result = connection.formatLiteralValue(Double.MIN_VALUE);
        assertEquals(new BigDecimal(Double.toString(Double.MIN_VALUE)).toPlainString(), result);
        
        // Test Double.MAX_VALUE
        result = connection.formatLiteralValue(Double.MAX_VALUE);
        assertEquals(new BigDecimal(Double.toString(Double.MAX_VALUE)).toPlainString(), result);
        
        // Test negative Double.MAX_VALUE
        result = connection.formatLiteralValue(-Double.MAX_VALUE);
        assertEquals(new BigDecimal(Double.toString(-Double.MAX_VALUE)).toPlainString(), result);
        
        // Test Double.MIN_NORMAL
        result = connection.formatLiteralValue(Double.MIN_NORMAL);
        assertEquals(new BigDecimal(Double.toString(Double.MIN_NORMAL)).toPlainString(), result);
    }

    @Test
    public void testBigDecimalPrecision() throws SQLException {
        
        
        // Test BigDecimal with precision <= 18 and scale <= 6 (no CAST needed)
        BigDecimal smallDecimal = new BigDecimal("123456789.123456");
        String result = connection.formatLiteralValue(smallDecimal);
        assertEquals("123456789.123456", result);
        
        // Test BigDecimal with high precision (needs CAST)
        BigDecimal highPrecisionDecimal = new BigDecimal("1234567890123456789012345678901234567890.123456789");
        result = connection.formatLiteralValue(highPrecisionDecimal);
        assertTrue(result.startsWith("CAST("));
        assertTrue(result.contains("AS DECIMAL(38,"));
        
        // Test BigDecimal with high scale (needs CAST)
        BigDecimal highScaleDecimal = new BigDecimal("123.1234567890123456789");
        result = connection.formatLiteralValue(highScaleDecimal);
        assertTrue(result.startsWith("CAST("));
        assertTrue(result.contains("AS DECIMAL("));
    }

    @Test
    public void testBigDecimalMinMax() throws SQLException {
        
        
        // Test very large BigDecimal
        BigDecimal largeDecimal = new BigDecimal("99999999999999999999999999999999999999");
        String result = connection.formatLiteralValue(largeDecimal);
        assertTrue(result.startsWith("CAST("));
        assertTrue(result.contains("AS DECIMAL(38,"));
        
        // Test very small BigDecimal
        BigDecimal smallDecimal = new BigDecimal("-99999999999999999999999999999999999999");
        result = connection.formatLiteralValue(smallDecimal);
        assertTrue(result.startsWith("CAST("));
        assertTrue(result.contains("AS DECIMAL(38,"));
        
        // Test BigDecimal with maximum SQL Server precision (38)
        StringBuilder sb = new StringBuilder("1");
        for (int i = 0; i < 37; i++) {
            sb.append("0");
        }
        String maxPrecisionStr = sb.toString(); // 38 digits
        BigDecimal maxPrecisionDecimal = new BigDecimal(maxPrecisionStr);
        result = connection.formatLiteralValue(maxPrecisionDecimal);
        assertTrue(result.startsWith("CAST("));
        assertTrue(result.contains("AS DECIMAL(38,0)"));
    }

    @Test
    public void testBigInteger() throws SQLException {
        
        
        // BigInteger should be handled as fallback (toString with quotes)
        BigInteger bigInt = new BigInteger("12345678901234567890123456789012345678901234567890");
        String result = connection.formatLiteralValue(bigInt);
        // Should fall through to the fallback case and be treated as string
        assertTrue(result.startsWith("'") || result.startsWith("N'"));
        assertTrue(result.endsWith("'"));
    }

    @Test
    public void testScientificNotationHandling() throws SQLException {
        
        
        // Test very small double that might use scientific notation
        double scientificDouble = 1.23e-100;
        String result = connection.formatLiteralValue(scientificDouble);
        // Should not contain 'E' or 'e' (scientific notation)
        assertTrue(!result.contains("E") && !result.contains("e"));
        
        // Test very large double that might use scientific notation
        double largeDouble = 1.23e100;
        result = connection.formatLiteralValue(largeDouble);
        // Should not contain 'E' or 'e' (scientific notation)
        assertTrue(!result.contains("E") && !result.contains("e"));
    }

    @Test
    public void testFloatSpecialValues() throws SQLException {
        
        
        // Test Float.POSITIVE_INFINITY - should be formatted as string
        String result = connection.formatLiteralValue(Float.POSITIVE_INFINITY);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("Infinity"));
        
        // Test Float.NEGATIVE_INFINITY - should be formatted as string
        result = connection.formatLiteralValue(Float.NEGATIVE_INFINITY);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("-Infinity"));
        
        // Test Float.NaN - should be formatted as string
        result = connection.formatLiteralValue(Float.NaN);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("NaN"));
    }

    @Test
    public void testDoubleSpecialValues() throws SQLException {
        
        
        // Test Double.POSITIVE_INFINITY - should be formatted as string
        String result = connection.formatLiteralValue(Double.POSITIVE_INFINITY);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("Infinity"));
        
        // Test Double.NEGATIVE_INFINITY - should be formatted as string
        result = connection.formatLiteralValue(Double.NEGATIVE_INFINITY);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("-Infinity"));
        
        // Test Double.NaN - should be formatted as string
        result = connection.formatLiteralValue(Double.NaN);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("NaN"));
    }

    @Test
    public void testBigDecimalSpecialCases() throws SQLException {
        
        
        // Test BigDecimal.ZERO
        String result = connection.formatLiteralValue(BigDecimal.ZERO);
        assertEquals("0", result);
        
        // Test BigDecimal.ONE
        result = connection.formatLiteralValue(BigDecimal.ONE);
        assertEquals("1", result);
        
        // Test BigDecimal.TEN
        result = connection.formatLiteralValue(BigDecimal.TEN);
        assertEquals("10", result);
        
        // Test BigDecimal with trailing zeros
        BigDecimal trailingZeros = new BigDecimal("123.4500");
        result = connection.formatLiteralValue(trailingZeros);
        assertEquals("123.4500", result);
        
        // Test BigDecimal with leading zeros
        BigDecimal leadingZeros = new BigDecimal("000123.45");
        result = connection.formatLiteralValue(leadingZeros);
        assertEquals("123.45", result);
    }

    @Test
    public void testPrecisionAndScaleBoundaries() throws SQLException {
        
        
        // Test boundary case: precision = 18, scale = 6 (should not need CAST)
        BigDecimal boundaryDecimal = new BigDecimal("123456789012.123456"); // 18 total digits
        String result = connection.formatLiteralValue(boundaryDecimal);
        assertEquals("123456789012.123456", result);
        
        // Test boundary case: precision = 19, scale = 6 (should need CAST)
        BigDecimal overBoundaryDecimal = new BigDecimal("1234567890123456789.123456");
        result = connection.formatLiteralValue(overBoundaryDecimal);
        assertTrue(result.startsWith("CAST("));
        
        // Test boundary case: precision = 18, scale = 7 (should need CAST)
        BigDecimal overScaleBoundaryDecimal = new BigDecimal("12345678901234567.1234567");
        result = connection.formatLiteralValue(overScaleBoundaryDecimal);
        assertTrue(result.startsWith("CAST("));
    }

    @Test
    public void testNullValue() throws SQLException {
        
        
        String result = connection.formatLiteralValue(null);
        assertEquals("NULL", result);
    }

    @Test
    public void testBooleanValues() throws SQLException {
        
        
        // Test true
        String result = connection.formatLiteralValue(Boolean.TRUE);
        assertEquals("1", result);
        
        // Test false
        result = connection.formatLiteralValue(Boolean.FALSE);
        assertEquals("0", result);
        
        // Test boolean primitive true
        result = connection.formatLiteralValue(true);
        assertEquals("1", result);
        
        // Test boolean primitive false
        result = connection.formatLiteralValue(false);
        assertEquals("0", result);
    }

    @Test
    public void testEdgeCasesForSQLServerLimits() throws SQLException {
        
        
        // Test decimal with scale > 38 (should be clamped to 38)
        BigDecimal highScaleDecimal = new BigDecimal("1.123456789012345678901234567890123456789012345678901234567890");
        String result = connection.formatLiteralValue(highScaleDecimal);
        assertTrue(result.startsWith("CAST("));
        // Scale should be clamped to not exceed precision and SQL Server limits
        assertTrue(result.contains("AS DECIMAL("));
        
        // Test decimal where scale would exceed precision after clamping
        BigDecimal problematicDecimal = new BigDecimal("1.123456789012345678901234567890123456789");
        result = connection.formatLiteralValue(problematicDecimal);
        assertTrue(result.startsWith("CAST("));
    }
}
