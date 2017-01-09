// ---------------------------------------------------------------------------------------------------------------------------------
// File: BulkCopyTimeoutTest.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"),
// to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense,
// and / or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.
// ---------------------------------------------------------------------------------------------------------------------------------
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCopyOptions;
import com.microsoft.sqlserver.jdbc.SQLServerException;

/**
 * Test the timeout in SQLServerBulkCopyOptions. Source table is created with large row count so skip data validation.
 */
@RunWith(JUnitPlatform.class)
public class BulkCopyTimeoutTest extends BulkCopyTestSetUp {

    /**
     * TODO: add support for small timeout value once test framework has support to add more than 10K rows, to check for Timeout Exception
     */


    /**
     * 
     * @throws SQLServerException
     */
    @Test
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
     * @throws SQLServerException
     */
    @Test
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
