package com.microsoft.sqlserver.jdbc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.lang.management.ManagementFactory;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MaxResultBufferParserTest {

    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {"10p", (long) (0.1 * getMaxMemory())},
                {"10pct", (long) (0.1 * getMaxMemory())},
                {"10percent", (long) (0.1 * getMaxMemory())},
                {"100", 100},
                {"100k", 100 * 1000},
                {"100m", 100 * 1000 * 1000},
                //these values are too big
                {"100G", (long) (0.9 * getMaxMemory())},
                {"100T", (long) (0.9 * getMaxMemory())},
        });
    }

    public static Iterable<Object[]> exceptionData() {
        return Arrays.asList(new Object[][]{
                {"ASD"},
                {"123PRECD"},
                {"0101D"},
                {"1@D"},
        });
    }

    @ParameterizedTest
    @MethodSource("data")
    void testValidateMaxResultBuffer(String input, long expected) throws SQLServerException {
        assertEquals(expected, MaxResultBufferParser.validateMaxResultBuffer(input));
    }

    @ParameterizedTest
    @MethodSource("exceptionData")
    void testValidateMaxResultBufferException(String input) {
        Assertions.assertThrows(SQLServerException.class, () -> MaxResultBufferParser.validateMaxResultBuffer(input));
    }

    private static long getMaxMemory() {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
    }
}