/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerException;

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
     * @throws SQLServerException
     */
    @Test
    @DisplayName("BulkCopy:test zero timeout")
    void testZeroTimeOut() throws SQLServerException {
        BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
        bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
        SQLServerBulkCopyOptions option = new SQLServerBulkCopyOptions();
        option.setBulkCopyTimeout(0);
        bulkWrapper.useBulkCopyOptions(true);
        bulkWrapper.setBulkOptions(option);
        BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, false);
    }

    /**
     * To verify SQLServerException: The timeout argument cannot be negative.
     * 
     * @throws SQLServerException
     */
    @Test
    @DisplayName("BulkCopy:test negative timeout")
    void testNegativeTimeOut() throws SQLServerException {
        assertThrows(SQLServerException.class, new org.junit.jupiter.api.function.Executable() {
            @Override
            public void execute() throws SQLServerException {
                BulkCopyTestWrapper bulkWrapper = new BulkCopyTestWrapper(connectionString);
                bulkWrapper.setUsingConnection((0 == ThreadLocalRandom.current().nextInt(2)) ? true : false);
                SQLServerBulkCopyOptions option = new SQLServerBulkCopyOptions();
                option.setBulkCopyTimeout(-1);
                bulkWrapper.useBulkCopyOptions(true);
                bulkWrapper.setBulkOptions(option);
                BulkCopyTestUtil.performBulkCopy(bulkWrapper, sourceTable, false);
            }
        });
    }
}
