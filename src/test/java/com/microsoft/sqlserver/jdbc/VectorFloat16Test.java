/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;

@DisplayName("Test Vector Float16 Data Type")
@Tag(Constants.vectorTest)
public class VectorFloat16Test extends AbstractTest {

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

}