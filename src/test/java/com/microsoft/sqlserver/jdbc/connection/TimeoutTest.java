/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


@RunWith(JUnitPlatform.class)
@Tag("slow")
public class TimeoutTest extends AbstractTest {
    static String randomServer = RandomUtil.getIdentifier("Server");
    static String waitForDelaySPName = RandomUtil.getIdentifier("waitForDelaySP");
    static final int waitForDelaySeconds = 10;
    static final int defaultTimeout = 60; // loginTimeout default value

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    public void testDefaultLoginTimeout() {
        long timerEnd = 0;

        long timerStart = System.currentTimeMillis();
        // Try a non existing server and see if the default timeout is 15 seconds
        try (Connection con = PrepUtil.getConnection("jdbc:sqlserver://" + randomServer + "connectRetryCount=0")) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            timerEnd = System.currentTimeMillis();
            assertTrue((e.getMessage().contains(TestResource.getResource("R_tcpipConnectionToHost")))
                    || ((isSqlAzure() || isSqlAzureDW())
                                                         ? e.getMessage().contains(
                                                                 TestResource.getResource("R_connectTimedOut"))
                                                         : false),
                    e.getMessage());
        }

        verifyTimeout(timerEnd - timerStart, defaultTimeout);
    }

    @Test
    public void testURLLoginTimeout() {
        long timerEnd = 0;
        int timeout = 10;

        long timerStart = System.currentTimeMillis();

        try (Connection con = PrepUtil.getConnection("jdbc:sqlserver://" + randomServer + ";logintimeout=" + timeout)) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            timerEnd = System.currentTimeMillis();
            assertTrue((e.getMessage().contains(TestResource.getResource("R_tcpipConnectionToHost")))
                    || ((isSqlAzure() || isSqlAzureDW())
                                                         ? e.getMessage().contains(
                                                                 TestResource.getResource("R_connectTimedOut"))
                                                         : false),
                    e.getMessage());
        }

        verifyTimeout(timerEnd - timerStart, timeout);
    }

    @Test
    public void testDMLoginTimeoutApplied() {
        long timerEnd = 0;
        int timeout = 10;

        DriverManager.setLoginTimeout(timeout);
        long timerStart = System.currentTimeMillis();

        try (Connection con = PrepUtil.getConnection("jdbc:sqlserver://" + randomServer)) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            timerEnd = System.currentTimeMillis();
            assertTrue((e.getMessage().contains(TestResource.getResource("R_tcpipConnectionToHost")))
                    || ((isSqlAzure() || isSqlAzureDW())
                                                         ? e.getMessage().contains(
                                                                 TestResource.getResource("R_connectTimedOut"))
                                                         : false),
                    e.getMessage());
        }

        verifyTimeout(timerEnd - timerStart, timeout);
    }

    @Test
    public void testDMLoginTimeoutNotApplied() {
        long timerEnd = 0;
        int timeout = 10;
        try {
            DriverManager.setLoginTimeout(timeout * 3); // 30 seconds
            long timerStart = System.currentTimeMillis();

            try (Connection con = PrepUtil
                    .getConnection("jdbc:sqlserver://" + randomServer + ";loginTimeout=" + timeout)) {
                fail(TestResource.getResource("R_shouldNotConnect"));
            } catch (Exception e) {
                timerEnd = System.currentTimeMillis();
                assertTrue(
                        (e.getMessage().contains(TestResource.getResource("R_tcpipConnectionToHost")))
                                || ((isSqlAzure() || isSqlAzureDW())
                                                                     ? e.getMessage()
                                                                             .contains(TestResource
                                                                                     .getResource("R_connectTimedOut"))
                                                                     : false),
                        e.getMessage());
            }
            verifyTimeout(timerEnd - timerStart, timeout);
        } finally {
            DriverManager.setLoginTimeout(0); // Default to 0 again
        }
    }

    // Test connect retry for non-existent server with loginTimeout
    @Test
    public void testConnectRetryBadServer() {
        long timerEnd = 0;
        long timerStart = System.currentTimeMillis();
        int loginTimeout = 15;

        // non existent server with very short loginTimeout, no retry will happen as not a transient error
        try (Connection con = PrepUtil
                .getConnection("jdbc:sqlserver://" + randomServer + ";loginTimeout=" + loginTimeout)) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            timerEnd = System.currentTimeMillis();
            assertTrue((e.getMessage().contains(TestResource.getResource("R_tcpipConnectionToHost")))
                    || ((isSqlAzure() || isSqlAzureDW())
                                                         ? e.getMessage().contains(
                                                                 TestResource.getResource("R_connectTimedOut"))
                                                         : false),
                    e.getMessage());
        }

        verifyTimeout(timerEnd - timerStart, loginTimeout);
    }

    // Test connect retry for database error
    @Test
    public void testConnectRetryServerError() {
        long timerEnd = 0;
        long timerStart = System.currentTimeMillis();

        // non existent database with interval < loginTimeout this will generate a 4060 transient error and retry
        int connectRetryCount = new Random().nextInt(256);
        int connectRetryInterval = new Random().nextInt(defaultTimeout) + 1;
        try (Connection con = PrepUtil.getConnection(
                TestUtils.addOrOverrideProperty(connectionString, "database", RandomUtil.getIdentifier("database"))
                        + ";logintimeout=" + defaultTimeout + ";connectRetryCount=" + connectRetryCount
                        + ";connectRetryInterval=" + connectRetryInterval)) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            timerEnd = System.currentTimeMillis();
            assertTrue((e.getMessage().contains(TestResource.getResource("R_cannotOpenDatabase")))
                    || ((isSqlAzure() || isSqlAzureDW())
                                                         ? e.getMessage().contains(
                                                                 TestResource.getResource("R_connectTimedOut"))
                                                         : false),
                    e.getMessage());
        }

        // connect + all retries should always be <= loginTimeout
        verifyTimeout(timerEnd - timerStart, defaultTimeout);
    }

    // Test connect retry for database error using Datasource
    @Test
    public void testConnectRetryServerErrorDS() {
        long timerEnd = 0;
        long timerStart = System.currentTimeMillis();

        // non existent database with interval < loginTimeout this will generate a 4060 transient error and retry
        int connectRetryCount = new Random().nextInt(256);
        int connectRetryInterval = new Random().nextInt(defaultTimeout) + 1;

        SQLServerDataSource ds = new SQLServerDataSource();
        String connectStr = TestUtils.addOrOverrideProperty(connectionString, "database",
                RandomUtil.getIdentifier("database")) + ";logintimeout=" + defaultTimeout + ";connectRetryCount="
                + connectRetryCount + ";connectRetryInterval=" + connectRetryInterval;
        updateDataSource(connectStr, ds);

        try (Connection con = PrepUtil.getConnection(connectStr)) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            timerEnd = System.currentTimeMillis();
            assertTrue((e.getMessage().contains(TestResource.getResource("R_cannotOpenDatabase")))
                    || ((isSqlAzure() || isSqlAzureDW())
                                                         ? e.getMessage().contains(
                                                                 TestResource.getResource("R_connectTimedOut"))
                                                         : false),
                    e.getMessage());
        }

        // connect + all retries should always be <= loginTimeout
        verifyTimeout(timerEnd - timerStart, defaultTimeout);
    }

    // Test connect retry for database error with loginTimeout
    @Test
    public void testConnectRetryTimeout() {
        long timerEnd = 0;
        int loginTimeout = 2;
        long timerStart = System.currentTimeMillis();

        // non existent server with very short loginTimeout so there is no time to do all retries
        try (Connection con = PrepUtil.getConnection(
                TestUtils.addOrOverrideProperty(connectionString, "database", RandomUtil.getIdentifier("database"))
                        + "connectRetryCount=" + (new Random().nextInt(256)) + ";connectRetryInterval="
                        + (new Random().nextInt(defaultTimeout - 1) + 1) + ";loginTimeout=" + loginTimeout)) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            timerEnd = System.currentTimeMillis();
            assertTrue((e.getMessage().contains(TestResource.getResource("R_cannotOpenDatabase")))
                    || ((isSqlAzure() || isSqlAzureDW())
                                                         ? e.getMessage().contains(
                                                                 TestResource.getResource("R_connectTimedOut"))
                                                         : false),
                    e.getMessage());
        }

        verifyTimeout(timerEnd - timerStart, loginTimeout);
    }

    @Test
    public void testFailoverInstanceResolution() throws SQLException {
        long timerEnd = 0;
        long timerStart = System.currentTimeMillis();

        // Try a non existing server and see if the default timeout is 15 seconds
        try (Connection con = PrepUtil.getConnection("jdbc:sqlserver://" + randomServer
                + ";databaseName=FailoverDB_abc;failoverPartner=" + randomServer + "\\foo;user=sa;password=pwd;")) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            timerEnd = System.currentTimeMillis();
            assertTrue((e.getMessage().contains(TestResource.getResource("R_tcpipConnectionToHost")))
                    || ((isSqlAzure() || isSqlAzureDW())
                                                         ? e.getMessage().contains(
                                                                 TestResource.getResource("R_connectTimedOut"))
                                                         : false),
                    e.getMessage());
        }

        verifyTimeout(timerEnd - timerStart, defaultTimeout * 2);
    }

    @Test
    public void testFOInstanceResolution2() throws SQLException {
        long timerEnd = 0;

        long timerStart = System.currentTimeMillis();
        try (Connection con = PrepUtil.getConnection("jdbc:sqlserver://" + randomServer
                + "\\fooggg;databaseName=FailoverDB;failoverPartner=" + randomServer + "\\foo;user=sa;password=pwd;")) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            timerEnd = System.currentTimeMillis();
        }

        verifyTimeout(timerEnd - timerStart, defaultTimeout);
    }

    /**
     * Tests that failover is correctly used after a socket timeout, by confirming total time includes socketTimeout
     * for both primary and failover server.
     */
    @Test
    public void testFailoverInstanceResolutionWithSocketTimeout() {
        long timerEnd;
        long timerStart = System.currentTimeMillis();

        try (Connection con = PrepUtil
                .getConnection("jdbc:sqlserver://" + randomServer + ";databaseName=FailoverDB;failoverPartner="
                        + randomServer + "\\foo;user=sa;password=pwd;" + ";socketTimeout=" + waitForDelaySeconds)) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            timerEnd = System.currentTimeMillis();
            if (!(e instanceof SQLException)) {
                fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
            }

            // Driver should correctly attempt to connect to db, experience a socketTimeout, attempt to connect to
            // failover, and then have another socketTimeout. So, expected total time is 2 x socketTimeout.
            long totalTime = timerEnd - timerStart;
            long totalExpectedTime = waitForDelaySeconds * 1000L * 2; // We expect 2 * socketTimeout
            assertTrue(totalTime >= totalExpectedTime, TestResource.getResource("R_executionNotLong") + "totalTime: "
                    + totalTime + " expectedTime: " + totalExpectedTime);
        }
    }

    private void verifyTimeout(long timeDiff, int timeout) {
        // Verify that login timeout does not take longer than <timeout * 2> seconds.
        assertTrue(timeDiff < TimeUnit.SECONDS.toMillis(timeout * 2),
                "timeout: " + TimeUnit.SECONDS.toMillis(timeout) + " timediff: " + timeDiff);
    }

    /**
     * When query timeout occurs, the connection is still usable.
     * 
     * @throws Exception
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testQueryTimeout() throws Exception {
        try (Connection conn = getConnection()) {
            dropWaitForDelayProcedure(conn);
            createWaitForDelayPreocedure(conn);
        }

        try (Connection conn = PrepUtil.getConnection(
                connectionString + ";queryTimeout=" + (waitForDelaySeconds / 2) + Constants.SEMI_COLON)) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("exec " + AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName));
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception e) {
                if (!(e instanceof java.sql.SQLTimeoutException)) {
                    throw e;
                }
                assertEquals(e.getMessage(), TestResource.getResource("R_queryTimedOut"),
                        TestResource.getResource("R_invalidExceptionMessage"));
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT @@version");
            } catch (Exception e) {
                fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    /**
     * Tests sanity of connection property.
     * 
     * @throws Exception
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testCancelQueryTimeout() throws Exception {
        try (Connection conn = getConnection()) {
            dropWaitForDelayProcedure(conn);
            createWaitForDelayPreocedure(conn);
        }

        try (Connection conn = PrepUtil.getConnection(connectionString + ";queryTimeout=" + (waitForDelaySeconds / 2)
                + ";cancelQueryTimeout=" + waitForDelaySeconds + Constants.SEMI_COLON)) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("exec " + AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName));
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception e) {
                if (!(e instanceof java.sql.SQLTimeoutException)) {
                    fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                }
                assertEquals(e.getMessage(), TestResource.getResource("R_queryTimedOut"),
                        TestResource.getResource("R_invalidExceptionMessage"));
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT @@version");
            } catch (Exception e) {
                fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    /**
     * Tests sanity of connection property.
     * 
     * @throws Exception
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testCancelQueryTimeoutOnStatement() throws Exception {
        try (Connection conn = getConnection()) {
            dropWaitForDelayProcedure(conn);
            createWaitForDelayPreocedure(conn);
        }

        try (Connection conn = PrepUtil.getConnection(connectionString + Constants.SEMI_COLON)) {

            try (SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {
                stmt.setQueryTimeout(waitForDelaySeconds / 2);
                stmt.setCancelQueryTimeout(waitForDelaySeconds);
                stmt.execute("exec " + AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName));
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception e) {
                if (!(e instanceof java.sql.SQLTimeoutException)) {
                    fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                }
                assertEquals(e.getMessage(), TestResource.getResource("R_queryTimedOut"),
                        TestResource.getResource("R_invalidExceptionMessage"));
            }

            try (SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {
                stmt.execute("SELECT @@version");
            } catch (Exception e) {
                fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    /**
     * When socketTimeout occurs, the connection will be marked as closed.
     * 
     * @throws Exception
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testSocketTimeout() throws Exception {
        try (Connection conn = getConnection()) {
            dropWaitForDelayProcedure(conn);
            createWaitForDelayPreocedure(conn);
        }

        try (Connection conn = PrepUtil.getConnection(
                connectionString + ";socketTimeout=" + (waitForDelaySeconds * 1000 / 2) + Constants.SEMI_COLON)) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("exec " + AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName));
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception e) {
                if (!(e instanceof SQLException)) {
                    throw e;
                }
                assertEquals(e.getMessage(), TestResource.getResource("R_readTimedOut"),
                        TestResource.getResource("R_invalidExceptionMessage"));
            }

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT @@version");
            } catch (SQLException e) {
                assertEquals(e.getMessage(), TestResource.getResource("R_connectionIsClosed"),
                        TestResource.getResource("R_invalidExceptionMessage"));
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    private static void dropWaitForDelayProcedure(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName), stmt);
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    private void createWaitForDelayPreocedure(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName) + " AS"
                    + " BEGIN" + " WAITFOR DELAY '00:00:" + waitForDelaySeconds + "';" + " END";
            stmt.execute(sql);
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
    }

    @AfterAll
    public static void cleanup() throws SQLException {
        try (Connection conn = getConnection()) {
            dropWaitForDelayProcedure(conn);
        }
    }
}
