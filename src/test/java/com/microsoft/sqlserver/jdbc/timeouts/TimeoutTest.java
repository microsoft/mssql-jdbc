package com.microsoft.sqlserver.jdbc.timeouts;

import com.microsoft.sqlserver.testframework.AbstractTest;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.sql.*;

@RunWith(JUnitPlatform.class)
public class TimeoutTest extends AbstractTest {

    @Test
    public void testBasicQueryTimeout() {
        boolean exceptionThrown = false;
        try {
            //wait 1 minute and timeout after 10 seconds
            Assert.assertTrue("Select succeeded", runQuery("WAITFOR DELAY '00:01'", 10));
        } catch (SQLException e) {
            exceptionThrown = true;
            Assert.assertTrue("Timeout exception not thrown", e.getClass().equals(SQLTimeoutException.class));
        }
        Assert.assertTrue("No sql exception was thrown, a sql timeout exception should of been thrown", exceptionThrown);
    }

    @Test
    public void testQueryTimeoutValid() {
        boolean exceptionThrown = false;
        int timeoutInSeconds = 10;
        long start = System.currentTimeMillis();
        try {
            //wait 1 minute and timeout after 10 seconds
            Assert.assertTrue("Select succeeded", runQuery("WAITFOR DELAY '00:01'", timeoutInSeconds));
        } catch (SQLException e) {
            int secondsElapsed = (int) ((System.currentTimeMillis() - start) / 1000);
            Assert.assertTrue("Query did not timeout expected, elapsedTime=" + secondsElapsed, secondsElapsed >= timeoutInSeconds);

            exceptionThrown = true;
            Assert.assertTrue("Timeout exception not thrown", e.getClass().equals(SQLTimeoutException.class));
        }
        Assert.assertTrue("No sql exception was thrown, a sql timeout exception should of been thrown", exceptionThrown);
    }

    private boolean runQuery(String query, int timeout) throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString)) {
            //wait 1 minute
            PreparedStatement preparedStatement = con.prepareStatement(query);
            //timeout after X seconds
            preparedStatement.setQueryTimeout(timeout);
            return preparedStatement.execute();
        }
    }
}
