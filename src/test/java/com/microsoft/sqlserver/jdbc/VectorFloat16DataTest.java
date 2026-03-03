/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;

@DisplayName("Test Vector Float16 Data Type")
@Tag(Constants.vectorTest)
public class VectorFloat16DataTest extends AbstractTest {

    @BeforeAll
    private static void setupTest() throws Exception {
        setConnection();
    }

    @Test
    @DisplayName("Test serializeFloat16Array: Float[] → Short[]")
    public void testSerializeFloat16Array() {

        Float[] input = new Float[] {
                1.0f, // 0x3C00
                -2.0f, // 0xC000
                0.5f, // 0x3800
                0.0f, // 0x0000
                -0.0f, // 0x8000
                Float.POSITIVE_INFINITY, // 0x7C00
                Float.NEGATIVE_INFINITY, // 0xFC00
                Float.NaN // 0x7E00
        };

        Short[] result = VectorUtils.serializeFloat16Array(input);

        Short[] expected = new Short[] {
                (short) 0x3C00,
                (short) 0xC000,
                (short) 0x3800,
                (short) 0x0000,
                (short) 0x8000,
                (short) 0x7C00,
                (short) 0xFC00,
                (short) 0x7E00
        };

        assertNotNull(result);
        assertEquals(expected.length, result.length);

        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], result[i], "Mismatch at index " + i);
        }
    }

    @Test
    @DisplayName("Test deserializeFloat16Array: Short[] → Float[]")
    public void testDeserializeFloat16Array() {

        Short[] input = new Short[] {
                (short) 0x3C00, // 1.0
                (short) 0xC000, // -2.0
                (short) 0x3800, // 0.5
                (short) 0x0000, // +0
                (short) 0x8000, // -0
                (short) 0x7C00, // +Inf
                (short) 0xFC00, // -Inf
                (short) 0x7E00 // NaN
        };

        Float[] result = VectorUtils.deserializeFloat16Array(input);

        assertNotNull(result);
        assertEquals(input.length, result.length);

        assertEquals(1.0f, result[0]);
        assertEquals(-2.0f, result[1]);
        assertEquals(0.5f, result[2]);
        assertEquals(0.0f, result[3]);
        assertEquals(-0.0f, result[4]);
        assertEquals(Float.POSITIVE_INFINITY, result[5]);
        assertEquals(Float.NEGATIVE_INFINITY, result[6]);
        assertTrue(Float.isNaN(result[7]));
    }

    @Test
    @DisplayName("Float -> Float16 Serialization: All Scenarios")
    void testFloatToFloat16Serialization() {

        Float[] input = new Float[] {

                // Normal number: well within float16 representable range
                // Should convert to a normal float16 value
                1.5f,

                // Very small number: representable as subnormal in float16
                // Should convert to subnormal, not zero
                5.96e-8f,

                // Extremely small number: below float16 subnormal range
                // Should underflow to signed zero
                1.0e-10f,

                // Large number beyond float16 max (65504)
                // Should overflow to +Infinity
                70000.0f,

                // Negative overflow
                // Should overflow to -Infinity
                -100000.0f,

                // Exactly representable boundary value
                // Should serialize without rounding error
                0.5f,

                // Value needing rounding (tie-to-even scenario)
                // Should round correctly using nearest-even rule
                1.0009766f,

                // Special value: +Infinity
                // Should map to float16 Infinity
                Float.POSITIVE_INFINITY,

                // Special value: -Infinity
                // Should map to float16 -Infinity
                Float.NEGATIVE_INFINITY,

                // Special value: NaN
                // Should convert to canonical float16 NaN (0x7E00)
                Float.NaN,

                // Positive zero
                // Should preserve sign bit
                +0.0f,

                // Negative zero
                // Must preserve negative zero sign
                -0.0f
        };

        Short[] result = VectorUtils.serializeFloat16Array(input);

        // Assertions
        assertEquals((short) 0x3E00, result[0]); // 1.5
        assertNotEquals((short) 0x0000, result[1]); // Subnormal not zero
        assertEquals((short) 0x0000, result[2]); // Underflow to zero
        assertEquals((short) 0x7C00, result[3]); // +Infinity
        assertEquals((short) 0xFC00, result[4]); // -Infinity
        assertEquals((short) 0x3800, result[5]); // 0.5
        assertEquals((short) 0x3C01, result[6]); // rounded value
        assertEquals((short) 0x7C00, result[7]); // +Infinity
        assertEquals((short) 0xFC00, result[8]); // -Infinity
        assertEquals((short) 0x7E00, result[9]); // NaN
        assertEquals((short) 0x0000, result[10]); // +0
        assertEquals((short) 0x8000, result[11]); // -0 preserved
    }

    @Test
    @DisplayName("Float16 -> Float Deserialization: All Scenarios")
    void testFloat16ToFloatDeserialization() {

        Short[] input = new Short[] {

                // Normal float16 number → normal float
                // 1.5 in float16 representation
                (short) 0x3E00,

                // Smallest positive subnormal float16
                // Should convert to tiny non-zero float
                (short) 0x0001,

                // Zero
                // Must become +0.0
                (short) 0x0000,

                // Negative zero
                // Must preserve -0.0 sign
                (short) 0x8000,

                // Largest normal float16 value (65504)
                // Should deserialize to approx 65504f
                (short) 0x7BFF,

                // Positive Infinity
                // Must deserialize to Float.POSITIVE_INFINITY
                (short) 0x7C00,

                // Negative Infinity
                // Must deserialize to Float.NEGATIVE_INFINITY
                (short) 0xFC00,

                // Canonical NaN
                // Must deserialize to Float.NaN
                (short) 0x7E00,

                // A random normal float16
                // Validates general path
                (short) 0x3555
        };

        Float[] result = VectorUtils.deserializeFloat16Array(input);

        // Assertions
        assertEquals(1.5f, result[0]);
        assertTrue(result[1] > 0 && result[1] < 1e-6); // subnormal tiny positive
        assertEquals(0.0f, result[2]);
        assertEquals(Float.floatToRawIntBits(-0.0f), Float.floatToRawIntBits(result[3])); // sign preserved
        assertEquals(65504.0f, result[4]);
        assertEquals(Float.POSITIVE_INFINITY, result[5]);
        assertEquals(Float.NEGATIVE_INFINITY, result[6]);
        assertTrue(Float.isNaN(result[7]));
        assertNotNull(result[8]); // general valid float
    }

}
