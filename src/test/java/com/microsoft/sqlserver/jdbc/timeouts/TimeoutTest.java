/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.timeouts;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class TimeoutTest extends AbstractTest {
    @Test
    public void testBasicQueryTimeout() {
        boolean exceptionThrown = false;
        try {
            // wait 1 minute and timeout after 10 seconds
            Assert.assertTrue("Select succeeded", runQuery("WAITFOR DELAY '00:01'", 10));
        } catch (SQLException e) {
            exceptionThrown = true;
            Assert.assertTrue("Timeout exception not thrown", e.getClass().equals(SQLTimeoutException.class));
        }
        Assert.assertTrue("A SQLTimeoutException was expected", exceptionThrown);
    }

    @Test
    public void testQueryTimeoutValid() {
        boolean exceptionThrown = false;
        int timeoutInSeconds = 10;
        long start = System.currentTimeMillis();
        try {
            // wait 1 minute and timeout after 10 seconds
            Assert.assertTrue("Select succeeded", runQuery("WAITFOR DELAY '00:01'", timeoutInSeconds));
        } catch (SQLException e) {
            int secondsElapsed = (int) ((System.currentTimeMillis() - start) / 1000);
            Assert.assertTrue("Query did not timeout expected, elapsedTime=" + secondsElapsed,
                    secondsElapsed >= timeoutInSeconds);
            exceptionThrown = true;
            Assert.assertTrue("Timeout exception not thrown", e.getClass().equals(SQLTimeoutException.class));
        }
        Assert.assertTrue("A SQLTimeoutException was expected", exceptionThrown);
    }

    private boolean runQuery(String query, int timeout) throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString)) {
            // wait 1 minute
            PreparedStatement preparedStatement = con.prepareStatement(query);
            // timeout after X seconds
            preparedStatement.setQueryTimeout(timeout);
            return preparedStatement.execute();
        }
    }
}
