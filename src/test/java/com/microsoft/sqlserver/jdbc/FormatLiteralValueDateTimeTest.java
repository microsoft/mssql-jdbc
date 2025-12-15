/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Test cases for SQLServerConnection.formatLiteralValue method focusing on Date and Time types
 * This test is in the same package so it can access the package-private formatLiteralValue method directly
 */
public class FormatLiteralValueDateTimeTest {

    private SQLServerConnection connection;

    @BeforeEach
    public void setUp() throws Exception {
        // Create a minimal SQLServerConnection for testing
        // We don't need a real database connection for testing formatLiteralValue
        connection = new SQLServerConnection("");
    }

    @Test
    public void testFormatLiteralValueDateTimeOptimization() throws Exception {
        // Test all date/time types with optimized formatting
        Date testDate = Date.valueOf("2023-12-15");
        Time testTime = Time.valueOf("14:30:45");
        Timestamp testTimestamp = Timestamp.valueOf("2023-12-15 14:30:45.123456789");
        
        // Verify optimized formatting produces correct SQL CAST syntax  
        assertEquals("CAST('2023-12-15' AS DATE)", connection.formatLiteralValue(testDate));
        assertEquals("CAST('14:30:45' AS TIME)", connection.formatLiteralValue(testTime));
        assertEquals("CAST('2023-12-15 14:30:45.123456789' AS DATETIME2)", connection.formatLiteralValue(testTimestamp));
        
        // Test edge cases - java.sql.Date.toString() always produces ISO format (yyyy-MM-dd)
        Date minDate = Date.valueOf("1900-01-01");
        Date maxDate = Date.valueOf("9999-12-31"); 
        Date leapYear = Date.valueOf("2024-02-29");
        assertEquals("CAST('1900-01-01' AS DATE)", connection.formatLiteralValue(minDate));
        assertEquals("CAST('9999-12-31' AS DATE)", connection.formatLiteralValue(maxDate));
        assertEquals("CAST('2024-02-29' AS DATE)", connection.formatLiteralValue(leapYear));
        
        // Test time edge cases
        Time midnight = Time.valueOf("00:00:00");
        Time endOfDay = Time.valueOf("23:59:59");
        assertEquals("CAST('00:00:00' AS TIME)", connection.formatLiteralValue(midnight));
        assertEquals("CAST('23:59:59' AS TIME)", connection.formatLiteralValue(endOfDay));
    }

    @Test  
    public void testFormatLiteralValuePrecisionImprovement() throws Exception {
        // Demonstrate that timestamp optimization preserves nanosecond precision vs old TS_FMT
        Timestamp nanoTimestamp = Timestamp.valueOf("2023-12-15 14:30:45.0");
        nanoTimestamp.setNanos(123456789);
        
        String optimizedResult = connection.formatLiteralValue(nanoTimestamp);
        assertEquals("CAST('2023-12-15 14:30:45.123456789' AS DATETIME2)", optimizedResult);
        
        // Compare with what TS_FMT would produce (lost precision)
        java.text.SimpleDateFormat TS_FMT = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        String oldResult = "CAST('" + TS_FMT.format(nanoTimestamp) + "' AS DATETIME2)";
        assertEquals("CAST('2023-12-15 14:30:45.123' AS DATETIME2)", oldResult);
        
        assertNotEquals(oldResult, optimizedResult, "Optimized version preserves full nanosecond precision");
    }
}