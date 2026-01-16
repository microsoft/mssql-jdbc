/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.Constants;

import sun.misc.Unsafe;

/**
 * Test cases for formatLiteralValue method with numeric types including MIN and MAX values
 * This test uses reflection to access the package-private formatLiteralValue method
 */
@RunWith(JUnitPlatform.class)
public class FormatLiteralValueNumericTest {

    private SQLServerConnection connection;
    private SQLServerPreparedStatement preparedStatement;
    private Method formatLiteralValueMethod;

    @BeforeEach
    public void setUp() throws Exception {
        // Create a minimal SQLServerConnection for testing
        connection = new SQLServerConnection("");
        
        // Use Unsafe to create an instance without calling constructor
        Field unsafeField = Unsafe.class.getDeclaredField("theUnsafe");
        unsafeField.setAccessible(true);
        Unsafe unsafe = (Unsafe) unsafeField.get(null);
        preparedStatement = (SQLServerPreparedStatement) unsafe.allocateInstance(SQLServerPreparedStatement.class);
        
        // Set the connection field via reflection
        Field connField = SQLServerStatement.class.getDeclaredField("connection");
        connField.setAccessible(true);
        connField.set(preparedStatement, connection);
        
        // Get the formatLiteralValue method via reflection
        formatLiteralValueMethod = SQLServerPreparedStatement.class.getDeclaredMethod("formatLiteralValue", Object.class);
        formatLiteralValueMethod.setAccessible(true);
    }

    // Helper method to call formatLiteralValue using reflection
    private String formatLiteralValue(Object value) throws Exception {
        return (String) formatLiteralValueMethod.invoke(preparedStatement, value);
    }

    @Test
    public void testByteMinMax() throws Exception {
        
        // Test Byte.MIN_VALUE
        String result = formatLiteralValue(Byte.MIN_VALUE);
        assertEquals("-128", result);
        
        // Test Byte.MAX_VALUE  
        result = formatLiteralValue(Byte.MAX_VALUE);
        assertEquals("127", result);
        
        // Test zero
        result = formatLiteralValue((byte) 0);
        assertEquals("0", result);
    }

    @Test
    public void testShortMinMax() throws Exception {
        
        // Test Short.MIN_VALUE
        String result = formatLiteralValue(Short.MIN_VALUE);
        assertEquals("-32768", result);
        
        // Test Short.MAX_VALUE
        result = formatLiteralValue(Short.MAX_VALUE);
        assertEquals("32767", result);
    }

    @Test
    public void testIntegerMinMax() throws Exception {
        
        // Test Integer.MIN_VALUE
        String result = formatLiteralValue(Integer.MIN_VALUE);
        assertEquals("-2147483648", result);
        
        // Test Integer.MAX_VALUE
        result = formatLiteralValue(Integer.MAX_VALUE);
        assertEquals("2147483647", result);
    }

    @Test
    public void testLongMinMax() throws Exception {
        
        // Test Long.MIN_VALUE
        String result = formatLiteralValue(Long.MIN_VALUE);
        assertEquals("-9223372036854775808", result);
        
        // Test Long.MAX_VALUE
        result = formatLiteralValue(Long.MAX_VALUE);
        assertEquals("9223372036854775807", result);
    }

    @Test
    public void testFloatMinMax() throws Exception {
        
        
        // Test Float.MIN_VALUE (smallest positive value)
        String result = formatLiteralValue(Float.MIN_VALUE);
        assertEquals(new BigDecimal(Float.toString(Float.MIN_VALUE)).toPlainString(), result);
        
        // Test Float.MAX_VALUE
        result = formatLiteralValue(Float.MAX_VALUE);
        assertEquals(new BigDecimal(Float.toString(Float.MAX_VALUE)).toPlainString(), result);
        
        // Test negative Float.MAX_VALUE
        result = formatLiteralValue(-Float.MAX_VALUE);
        assertEquals(new BigDecimal(Float.toString(-Float.MAX_VALUE)).toPlainString(), result);
        
        // Test Float.MIN_NORMAL
        result = formatLiteralValue(Float.MIN_NORMAL);
        assertEquals(new BigDecimal(Float.toString(Float.MIN_NORMAL)).toPlainString(), result);
    }

    @Test
    public void testDoubleMinMax() throws Exception {
        
        
        // Test Double.MIN_VALUE (smallest positive value)
        String result = formatLiteralValue(Double.MIN_VALUE);
        assertEquals(new BigDecimal(Double.toString(Double.MIN_VALUE)).toPlainString(), result);
        
        // Test Double.MAX_VALUE
        result = formatLiteralValue(Double.MAX_VALUE);
        assertEquals(new BigDecimal(Double.toString(Double.MAX_VALUE)).toPlainString(), result);
        
        // Test negative Double.MAX_VALUE
        result = formatLiteralValue(-Double.MAX_VALUE);
        assertEquals(new BigDecimal(Double.toString(-Double.MAX_VALUE)).toPlainString(), result);
        
        // Test Double.MIN_NORMAL
        result = formatLiteralValue(Double.MIN_NORMAL);
        assertEquals(new BigDecimal(Double.toString(Double.MIN_NORMAL)).toPlainString(), result);
    }

    @Test
    public void testBigDecimalPrecision() throws Exception {
        
        
        // Test BigDecimal with precision <= 18 and scale <= 6 (no CAST needed)
        BigDecimal smallDecimal = new BigDecimal("123456789.123456");
        String result = formatLiteralValue(smallDecimal);
        assertEquals("123456789.123456", result);
        
        // Test BigDecimal with high precision (needs CAST)
        BigDecimal highPrecisionDecimal = new BigDecimal("1234567890123456789012345678901234567890.123456789");
        result = formatLiteralValue(highPrecisionDecimal);
        assertTrue(result.startsWith("CAST("));
        assertTrue(result.contains("AS DECIMAL(38,"));
        
        // Test BigDecimal with high scale (needs CAST)
        BigDecimal highScaleDecimal = new BigDecimal("123.1234567890123456789");
        result = formatLiteralValue(highScaleDecimal);
        assertTrue(result.startsWith("CAST("));
        assertTrue(result.contains("AS DECIMAL("));
    }

    @Test
    public void testBigDecimalMinMax() throws Exception {
        
        
        // Test very large BigDecimal
        BigDecimal largeDecimal = new BigDecimal("99999999999999999999999999999999999999");
        String result = formatLiteralValue(largeDecimal);
        assertTrue(result.startsWith("CAST("));
        assertTrue(result.contains("AS DECIMAL(38,"));
        
        // Test very small BigDecimal
        BigDecimal smallDecimal = new BigDecimal("-99999999999999999999999999999999999999");
        result = formatLiteralValue(smallDecimal);
        assertTrue(result.startsWith("CAST("));
        assertTrue(result.contains("AS DECIMAL(38,"));
        
        // Test BigDecimal with maximum SQL Server precision (38)
        StringBuilder sb = new StringBuilder("1");
        for (int i = 0; i < 37; i++) {
            sb.append("0");
        }
        String maxPrecisionStr = sb.toString(); // 38 digits
        BigDecimal maxPrecisionDecimal = new BigDecimal(maxPrecisionStr);
        result = formatLiteralValue(maxPrecisionDecimal);
        assertTrue(result.startsWith("CAST("));
        assertTrue(result.contains("AS DECIMAL(38,0)"));
    }

    @Test
    public void testBigInteger() throws Exception {
        
        
        // BigInteger should be handled as fallback (toString with quotes)
        BigInteger bigInt = new BigInteger("12345678901234567890123456789012345678901234567890");
        String result = formatLiteralValue(bigInt);
        // Should fall through to the fallback case and be treated as string
        assertTrue(result.startsWith("'") || result.startsWith("N'"));
        assertTrue(result.endsWith("'"));
    }

    @Test
    public void testScientificNotationHandling() throws Exception {
        
        
        // Test very small double that might use scientific notation
        double scientificDouble = 1.23e-100;
        String result = formatLiteralValue(scientificDouble);
        // Should not contain 'E' or 'e' (scientific notation)
        assertTrue(!result.contains("E") && !result.contains("e"));
        
        // Test very large double that might use scientific notation
        double largeDouble = 1.23e100;
        result = formatLiteralValue(largeDouble);
        // Should not contain 'E' or 'e' (scientific notation)
        assertTrue(!result.contains("E") && !result.contains("e"));
    }

    @Test
    public void testFloatSpecialValues() throws Exception {
        
        
        // Test Float.POSITIVE_INFINITY - should be formatted as string
        String result = formatLiteralValue(Float.POSITIVE_INFINITY);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("Infinity"));
        
        // Test Float.NEGATIVE_INFINITY - should be formatted as string
        result = formatLiteralValue(Float.NEGATIVE_INFINITY);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("-Infinity"));
        
        // Test Float.NaN - should be formatted as string
        result = formatLiteralValue(Float.NaN);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("NaN"));
    }

    @Test
    public void testDoubleSpecialValues() throws Exception {
        
        
        // Test Double.POSITIVE_INFINITY - should be formatted as string
        String result = formatLiteralValue(Double.POSITIVE_INFINITY);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("Infinity"));
        
        // Test Double.NEGATIVE_INFINITY - should be formatted as string
        result = formatLiteralValue(Double.NEGATIVE_INFINITY);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("-Infinity"));
        
        // Test Double.NaN - should be formatted as string
        result = formatLiteralValue(Double.NaN);
        assertTrue(result.startsWith("N'") || result.startsWith("'"));
        assertTrue(result.contains("NaN"));
    }

    @Test
    public void testBigDecimalSpecialCases() throws Exception {
        
        
        // Test BigDecimal.ZERO
        String result = formatLiteralValue(BigDecimal.ZERO);
        assertEquals("0", result);
        
        // Test BigDecimal.ONE
        result = formatLiteralValue(BigDecimal.ONE);
        assertEquals("1", result);
        
        // Test BigDecimal.TEN
        result = formatLiteralValue(BigDecimal.TEN);
        assertEquals("10", result);
        
        // Test BigDecimal with trailing zeros
        BigDecimal trailingZeros = new BigDecimal("123.4500");
        result = formatLiteralValue(trailingZeros);
        assertEquals("123.4500", result);
        
        // Test BigDecimal with leading zeros
        BigDecimal leadingZeros = new BigDecimal("000123.45");
        result = formatLiteralValue(leadingZeros);
        assertEquals("123.45", result);
    }

    @Test
    public void testPrecisionAndScaleBoundaries() throws Exception {
        
        
        // Test boundary case: precision = 18, scale = 6 (should not need CAST)
        BigDecimal boundaryDecimal = new BigDecimal("123456789012.123456"); // 18 total digits
        String result = formatLiteralValue(boundaryDecimal);
        assertEquals("123456789012.123456", result);
        
        // Test boundary case: precision = 19, scale = 6 (should need CAST)
        BigDecimal overBoundaryDecimal = new BigDecimal("1234567890123456789.123456");
        result = formatLiteralValue(overBoundaryDecimal);
        assertTrue(result.startsWith("CAST("));
        
        // Test boundary case: precision = 18, scale = 7 (should need CAST)
        BigDecimal overScaleBoundaryDecimal = new BigDecimal("12345678901234567.1234567");
        result = formatLiteralValue(overScaleBoundaryDecimal);
        assertTrue(result.startsWith("CAST("));
    }

    @Test
    public void testNullValue() throws Exception {
        
        
        String result = formatLiteralValue(null);
        assertEquals("NULL", result);
    }

    @Test
    public void testBooleanValues() throws Exception {
        
        
        // Test true
        String result = formatLiteralValue(Boolean.TRUE);
        assertEquals("1", result);
        
        // Test false
        result = formatLiteralValue(Boolean.FALSE);
        assertEquals("0", result);
        
        // Test boolean primitive true
        result = formatLiteralValue(true);
        assertEquals("1", result);
        
        // Test boolean primitive false
        result = formatLiteralValue(false);
        assertEquals("0", result);
    }

    @Test
    public void testEdgeCasesForSQLServerLimits() throws Exception {
        
        
        // Test decimal with scale > 38 (should be clamped to 38)
        BigDecimal highScaleDecimal = new BigDecimal("1.123456789012345678901234567890123456789012345678901234567890");
        String result = formatLiteralValue(highScaleDecimal);
        assertTrue(result.startsWith("CAST("));
        // Scale should be clamped to not exceed precision and SQL Server limits
        assertTrue(result.contains("AS DECIMAL("));
        
        // Test decimal where scale would exceed precision after clamping
        BigDecimal problematicDecimal = new BigDecimal("1.123456789012345678901234567890123456789");
        result = formatLiteralValue(problematicDecimal);
        assertTrue(result.startsWith("CAST("));
    }
}
