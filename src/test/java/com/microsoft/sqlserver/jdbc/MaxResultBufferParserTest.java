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
        return Arrays.asList(new Object[][] {{"10p", (long) (0.1 * getMaxMemory())},
                {"10pct", (long) (0.1 * getMaxMemory())}, {"10percent", (long) (0.1 * getMaxMemory())}, {"100", 100},
                {"100k", 100 * 1000}, {"100m", 100 * 1000 * 1000},
                // these values are too big
                {"100G", (long) (0.9 * getMaxMemory())}, {"100T", (long) (0.9 * getMaxMemory())},});
    }

    /**
     * Method with input data for testValidateMaxResultBufferException Tests
     */
    public static Iterable<Object[]> exceptionData() {
        return Arrays.asList(new Object[][] {{"ASD"}, {"123PRECD"}, {"0101D"}, {"1@D"}, {"-1"}});
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
