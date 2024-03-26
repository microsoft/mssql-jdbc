/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.exception;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
public class ChainedExceptionTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testTwoExceptions() throws Exception {
        try (Connection conn = getConnection()) {

            // The below should yield the following Server Messages:
            // 1 : Msg 5074, Level 16, State 1: The object 'DF__#chained_exc__c1__AB25243A' is dependent on column 'c1'.
            // 1 : Msg 4922, Level 16, State 9: ALTER TABLE ALTER COLUMN c1 failed because one or more objects access this column.
            try (Statement stmnt = conn.createStatement()) {
                stmnt.executeUpdate("CREATE TABLE #chained_exception_test_x1(c1 INT DEFAULT(0))");
                stmnt.executeUpdate("ALTER  TABLE #chained_exception_test_x1 ALTER COLUMN c1 VARCHAR(10)");
                stmnt.executeUpdate("DROP   TABLE IF EXISTS #chained_exception_test_x1");
            }

            fail(TestResource.getResource("R_expectedFailPassed"));

        } catch (SQLException ex) {

            // Check the SQLException and the chain
            int exCount = 0;
            int firstMsgNum = ex.getErrorCode();
            int lastMsgNum = -1;

            while (ex != null) {
                exCount++;

                lastMsgNum = ex.getErrorCode();

                ex = ex.getNextException();
            }

            // Exception Count should be: 2
            assertEquals(2, exCount, "Number of SQLExceptions in the SQLException chain");

            // Check first Msg: 5074
            assertEquals(5074, firstMsgNum, "First SQL Server Message");

            // Check last Msg: 4922
            assertEquals(4922, lastMsgNum, "Last SQL Server Message");
        }
    }
}
