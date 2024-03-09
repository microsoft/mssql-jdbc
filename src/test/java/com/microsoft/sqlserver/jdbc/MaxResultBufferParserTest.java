/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.management.ManagementFactory;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;


/**
 * A class for testing MaxResultBufferParser functionality.
 */
class MaxResultBufferParserTest {

    /**
     * Method with input data for testValidateMaxResultBuffer Tests
     */
    public static Iterable<Object[]> data() {
        return Arrays
                .asList(new Object[][] {{"10p", (long) (0.1 * getMaxMemory())}, {"010p", (long) (0.1 * getMaxMemory())},
                        {"10pct", (long) (0.1 * getMaxMemory())}, {"10percent", (long) (0.1 * getMaxMemory())},
                        {"100", 100}, {"0100", 100}, {"100k", 100 * 1000}, {"0100k", 100 * 1000}, {"100K", 100 * 1000},
                        {"0100K", 100 * 1000}, {"100m", 100 * 1000 * 1000}, {"100M", 100 * 1000 * 1000},
                        // these values are too big (assuming heap size is 4GB)
                        {"200p", (long) (0.9 * getMaxMemory())}, {"0200p", (long) (0.9 * getMaxMemory())},
                        {"200pct", (long) (0.9 * getMaxMemory())}, {"200percent", (long) (0.9 * getMaxMemory())},
                        {"100g", (long) (0.9 * getMaxMemory())}, {"100G", (long) (0.9 * getMaxMemory())},
                        {"100t", (long) (0.9 * getMaxMemory())}, {"100T", (long) (0.9 * getMaxMemory())},
                        // when maxResultBuffer property is not supplied, assume -1
                        {"", -1}, {null, -1}, {"-1", -1},});
    }

    /**
     * Method with input data for testValidateMaxResultBufferException Tests
     */
    public static Iterable<Object[]> exceptionData() {
        return Arrays.asList(new Object[][] {{"-123p"}, {"-423pct"}, {"-100m"}, {"-500K"}, {"-123"}, // values are correctly formatted, but they're negative
                {"123precd"}, {"456pc"}, // percent phrases are misspelled
                {"32P"}, {"-456PCT"}, {"150PERCENT"}, // percent phrases are correct, but they're in upper Case also middle one is negative
                {"0101D"}, {"100l"}, {"-100L"}, // incorrect prefixes, last value is also negative
                {"1@D"}, // incorrect prefix and malformed value as well
                {"0"}, {"0t"}, {"0T"}, {"0p"}, {"0pct"}, {"0percent"}, // 0 is not positive, maxResultBuffer must have positive value
                {"0.5"}, {"0.5g"}, {"0.5G"}, {"0.5p"}, {"0.5pct"}, {"0.5percent"}, // maxResultBuffer must be whole number
                {" "}, {"ASD"}, {"@!|?:'{}"}, {"a5D"} // malformed values
        });
    }

    /**
     * 
     * Tests for correctly formatted input String
     * 
     * @param input
     *        MaxResultBuffer property
     * @param expected
     *        Expected number of bytes
     */
    @ParameterizedTest
    @MethodSource("data")
    void testValidateMaxResultBuffer(String input, long expected) {
        try {
            assertEquals(expected, MaxResultBufferParser.validateMaxResultBuffer(input));
        } catch (SQLServerException throwables) {
            fail();
        }
    }

    /**
     * 
     * Tests for badly formatted maxResultProperty
     * 
     * @param input
     *        Badly formatted MaxResultBuffer property
     */
    @ParameterizedTest
    @MethodSource("exceptionData")
    void testValidateMaxResultBufferException(String input) {
        Assertions.assertThrows(SQLServerException.class, () -> MaxResultBufferParser.validateMaxResultBuffer(input));
    }

    private static long getMaxMemory() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    }
}
