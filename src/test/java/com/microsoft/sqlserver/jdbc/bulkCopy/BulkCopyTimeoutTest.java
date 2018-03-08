/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.SQLException;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;

/**
 * Test the timeout in SQLServerBulkCopyOptions. Source table is created with large row count so skip data validation.
 */
@RunWith(JUnitPlatform.class)
@DisplayName("BulkCopy Timeout Test")
public class BulkCopyTimeoutTest extends BulkCopyTestSetUp {

    /**
     * TODO: add support for small timeout value once test framework has support to add more than 10K rows, to check for Timeout Exception
     */

    /**
     * BulkCopy:test zero timeout
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("BulkCopy:test zero timeout")
    void testZeroTimeOut() throws SQLException {
        testBulkCopyWithTimeout(0);
    }

    /**
     * To verify SQLException: The timeout argument cannot be negative.
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("BulkCopy:test negative timeout")
    void testNegativeTimeOut() throws SQLException {
        assertThrows(SQLException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() throws SQLException {
                testBulkCopyWithTimeout(-1);
            }
        });
    }
    
    private void testBulkCopyWithTimeout(int timeout) throws SQLException {
    	BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        SQLServerBulkCopyOptions option = new SQLServerBulkCopyOptions();
        option.setBulkCopyTimeout(timeout);
        bulkWrapper.useBulkCopyOptions(true);
        bulkWrapper.setBulkOptions(option);
        BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, false);
    }
}
