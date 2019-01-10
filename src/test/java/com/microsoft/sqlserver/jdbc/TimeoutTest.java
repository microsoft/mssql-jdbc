/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class TimeoutTest extends AbstractTest {
    private static final int TIMEOUT_SECONDS = 2;
    private static final String WAIT_FOR_ONE_MINUTE_SQL = "WAITFOR DELAY '00:01:00'";

    @BeforeAll
    public static void beforeAll() throws SQLException, InterruptedException {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        waitForSharedTimerThreadToStop();
    }

    @Before
    public void before() throws InterruptedException {
        waitForSharedTimerThreadToStop();
    }

    @After
    public void after() throws InterruptedException {
        waitForSharedTimerThreadToStop();
    }

    @Test
    public void testBasicQueryTimeout() {
        boolean exceptionThrown = false;
        try {
            Assert.assertTrue("Select succeeded", runQuery(WAIT_FOR_ONE_MINUTE_SQL, TIMEOUT_SECONDS));
        } catch (SQLException e) {
            exceptionThrown = true;
            Assert.assertTrue("Timeout exception not thrown", e.getClass().equals(SQLTimeoutException.class));
        }
        Assert.assertTrue("A SQLTimeoutException was expected", exceptionThrown);
    }

    @Test
    public void testQueryTimeoutValid() {
        boolean exceptionThrown = false;
        long start = System.currentTimeMillis();
        try {
            // wait 1 minute but timeout well before that
            Assert.assertTrue("Select succeeded", runQuery(WAIT_FOR_ONE_MINUTE_SQL, TIMEOUT_SECONDS));
        } catch (SQLException e) {
            int secondsElapsed = (int) ((System.currentTimeMillis() - start) / 1000);
            Assert.assertTrue("Query did not timeout expected, elapsedTime=" + secondsElapsed,
                    secondsElapsed >= TIMEOUT_SECONDS);
            exceptionThrown = true;
            Assert.assertTrue("Timeout exception not thrown", e.getClass().equals(SQLTimeoutException.class));
        }
        Assert.assertTrue("A SQLTimeoutException was expected", exceptionThrown);
    }

    private boolean runQuery(String query, int timeout) throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString);
                PreparedStatement preparedStatement = con.prepareStatement(query)) {
            // set provided timeout
            preparedStatement.setQueryTimeout(timeout);
            return preparedStatement.execute();
        }
    }

    @Test
    public void testSameSharedTimerRetrieved() {
        SharedTimer timer = SharedTimer.getTimer();
        try {
            SharedTimer otherTimer = SharedTimer.getTimer();
            try {
                assertEquals("The same SharedTimer should be returned", timer.getId(), otherTimer.getId());
            } finally {
                otherTimer.removeRef();
            }
        } finally {
            timer.removeRef();
        }
    }

    private static boolean isSharedTimerThreadRunning() {
        Set<Thread> threadSet = Thread.getAllStackTraces().keySet();
        for (Thread thread : threadSet) {
            if (thread.getName().startsWith(SharedTimer.CORE_THREAD_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    private static void waitForSharedTimerThreadToStop() throws InterruptedException {
        if (isSharedTimerThreadRunning()) {
            // Timer thread is still running so wait a bit for it to stop
            Thread.sleep(500);
        }
        assertFalse("SharedTimer thread should not be running", isSharedTimerThreadRunning());
    }
}
