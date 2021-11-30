/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
public class TimeoutTest extends AbstractTest {
    private static final int TIMEOUT_SECONDS = 2;
    private static final String WAIT_FOR_ONE_MINUTE_SQL = "WAITFOR DELAY '00:01:00'";

    @BeforeAll
    public static void beforeAll() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();

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
    @Tag(Constants.xAzureSQLDW)
    public void testBasicQueryTimeout() {
        assertThrows(SQLTimeoutException.class, () -> {
            runQuery(WAIT_FOR_ONE_MINUTE_SQL, TIMEOUT_SECONDS);
        });
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testQueryTimeoutValid() {
        long start = System.currentTimeMillis();
        assertThrows(SQLTimeoutException.class, () -> {
            runQuery(WAIT_FOR_ONE_MINUTE_SQL, TIMEOUT_SECONDS);
        });
        long elapsedSeconds = (System.currentTimeMillis() - start) / 1000;
        Assert.assertTrue("Query duration must be at least timeout amount, elapsed=" + elapsedSeconds,
                elapsedSeconds >= TIMEOUT_SECONDS);
    }

    @Test
    public void testZeroTimeoutShouldNotStartTimerThread() throws SQLException {
        try (Connection conn = getConnection()) {
            // Connection is open but we have not used a timeout so it should be running
            assertSharedTimerNotRunning();
            runQuery(conn, "SELECT 1", 0);
            // Our statement does not have a timeout so the timer should not be started yet
            assertSharedTimerNotRunning();
        }
    }

    @Test
    public void testNoTimeoutShouldNotStartTimerThread() throws SQLException {
        try (Connection conn = getConnection()) {
            // Connection is open but we have not used a timeout so it should not be running
            assertSharedTimerNotRunning();
            runQuery(conn, "SELECT 1", 0);
            // Ran a query but our statement does not have a timeout so the timer should not be running
            assertSharedTimerNotRunning();
        }
    }

    @Test
    public void testPositiveTimeoutShouldStartTimerThread() throws SQLException {
        try (Connection conn = getConnection()) {
            // Connection is open but we have not used a timeout so it should not be running
            assertSharedTimerNotRunning();
            runQuery(conn, "SELECT 1", TIMEOUT_SECONDS);
            // Ran a query with a timeout so the thread should continue running
            assertSharedTimerIsRunning();
        }
    }

    @Test
    public void testNestedTimeoutShouldKeepTimerThreadRunning() throws SQLException {
        try (Connection conn = getConnection()) {
            // Connection is open but we have not used a timeout so it should not be running
            assertSharedTimerNotRunning();
            runQuery(conn, "SELECT 1", TIMEOUT_SECONDS);
            // Ran a query with a timeout so the thread should continue running
            assertSharedTimerIsRunning();

            // Open a new connection
            try (Connection otherConn = getConnection()) {
                assertSharedTimerIsRunning();
                runQuery(otherConn, "SELECT 1", TIMEOUT_SECONDS);
                assertSharedTimerIsRunning();
            }

            // Timer should still be running because our original connection is still open
            assertSharedTimerIsRunning();
        }
    }

    @Test
    public void testGetClosedTimer() throws SQLServerException, SQLException {
        try (SQLServerConnection conn = getConnection()) {
            conn.close();
            @SuppressWarnings("unused")
            SharedTimer timer = conn.getSharedTimer();
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_connectionIsClosed")));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
        }
    }

    private static void runQuery(String query, int timeout) throws SQLException {
        try (Connection conn = getConnection()) {
            runQuery(conn, query, timeout);
        }
    }

    private static void runQuery(Connection conn, String query, int timeout) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(query)) {
            if (timeout > 0) {
                stmt.setQueryTimeout(timeout);
            }
            try (ResultSet rs = stmt.executeQuery()) {}
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
        long started = System.currentTimeMillis();
        long MAX_WAIT_FOR_STOP_SECONDS = 10;
        while (isSharedTimerThreadRunning()) {
            long elapsed = System.currentTimeMillis() - started;
            if (elapsed > MAX_WAIT_FOR_STOP_SECONDS * 1000) {
                fail("SharedTimer thread did not stop within " + MAX_WAIT_FOR_STOP_SECONDS + " seconds");
            }
            // Sleep a bit and try again
            Thread.sleep(1000);
        }
        assertSharedTimerNotRunning();
    }

    private static void assertSharedTimerNotRunning() {
        assertFalse("SharedTimer should not be running", isSharedTimerThreadRunning());
    }

    private static void assertSharedTimerIsRunning() {
        assertTrue("SharedTimer should be running", isSharedTimerThreadRunning());
    }
}
