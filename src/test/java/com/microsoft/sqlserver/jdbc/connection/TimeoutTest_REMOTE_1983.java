/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
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
public class TimeoutTest extends AbstractTest {
    static String randomServer = RandomUtil.getIdentifier("Server");
    static String waitForDelaySPName = RandomUtil.getIdentifier("waitForDelaySP");
    static final int waitForDelaySeconds = 10;
    static final int defaultTimeout = 30; // loginTimeout default value

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /*
     * TODO:
     * The tests below uses a simple interval counting logic to determine whether there was at least 1 retry.
     * Given the interval is long enough, then 1 retry should take at least 1 interval long, so if it took < 1 interval, then it assumes there were no retry. However, this only works if TNIR or failover is not enabled since those cases should retry but no wait interval in between. So this interval counting can not detect these cases.
     * Note a better and more reliable way would be to check attemptNumber using reflection to determine the number of retries.
     */

    // test default loginTimeout used if not specified in connection string
    @Test
    public void testDefaultLoginTimeout() {
        long totalTime = 0;
        long timerStart = System.currentTimeMillis();

        // non existing server and default values to see if took default timeout
        try (Connection con = PrepUtil.getConnection("jdbc:sqlserver://" + randomServer)) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            totalTime = System.currentTimeMillis() - timerStart;

            assertTrue(
                    (e.getMessage().toLowerCase()
                            .contains(TestResource.getResource("R_tcpipConnectionToHost").toLowerCase()))
                            || (TestUtils.getProperty(connectionString, "msiClientId") != null && e.getMessage()
                                    .toLowerCase().contains(TestResource.getResource("R_loginFailedMI").toLowerCase()))
                            || ((isSqlAzure() || isSqlAzureDW()) ? e.getMessage().toLowerCase()
                                    .contains(TestResource.getResource("R_connectTimedOut").toLowerCase()) : false),
                    e.getMessage());
        }

        // time should be < default loginTimeout
        assertTrue(totalTime < TimeUnit.SECONDS.toMillis(defaultTimeout),
                "total time: " + totalTime + " default loginTimout: " + TimeUnit.SECONDS.toMillis(defaultTimeout));
    }

    // test setting loginTimeout value
    @Test
    public void testURLLoginTimeout() {
        long totalTime = 0;
        int timeout = 15;

        long timerStart = System.currentTimeMillis();

        // non existing server and set loginTimeout
        try (Connection con = PrepUtil.getConnection("jdbc:sqlserver://" + randomServer + ";logintimeout=" + timeout)) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            totalTime = System.currentTimeMillis() - timerStart;

            assertTrue(
                    (e.getMessage().toLowerCase()
                            .contains(TestResource.getResource("R_tcpipConnectionToHost").toLowerCase()))
                            || (TestUtils.getProperty(connectionString, "msiClientId") != null && e.getMessage()
                                    .toLowerCase().contains(TestResource.getResource("R_loginFailedMI").toLowerCase()))
                            || ((isSqlAzure() || isSqlAzureDW()) ? e.getMessage().toLowerCase()
                                    .contains(TestResource.getResource("R_connectTimedOut").toLowerCase()) : false),
                    e.getMessage());
        }

        // time should be < set loginTimeout
        assertTrue(totalTime < TimeUnit.SECONDS.toMillis(timeout),
                "total time: " + totalTime + " loginTimeout: " + TimeUnit.SECONDS.toMillis(timeout));
    }

    // test setting timeout in DM
    @Test
    public void testDMLoginTimeoutApplied() {
        long totalTime = 0;
        int timeout = 15;

        DriverManager.setLoginTimeout(timeout);
        long timerStart = System.currentTimeMillis();

        try (Connection con = PrepUtil.getConnection("jdbc:sqlserver://" + randomServer)) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            totalTime = System.currentTimeMillis() - timerStart;

            assertTrue(
                    (e.getMessage().toLowerCase()
                            .contains(TestResource.getResource("R_tcpipConnectionToHost").toLowerCase()))
                            || (TestUtils.getProperty(connectionString, "msiClientId") != null && e.getMessage()
                                    .toLowerCase().contains(TestResource.getResource("R_loginFailedMI").toLowerCase()))
                            || ((isSqlAzure() || isSqlAzureDW()) ? e.getMessage().toLowerCase()
                                    .contains(TestResource.getResource("R_connectTimedOut").toLowerCase()) : false),
                    e.getMessage());
        }

        // time should be < DM timeout
        assertTrue(totalTime < TimeUnit.SECONDS.toMillis(timeout),
                "total time: " + totalTime + " DM loginTimeout: " + TimeUnit.SECONDS.toMillis(timeout));
    }

    // test that setting in connection string overrides value set in DM
    @Test
    public void testDMLoginTimeoutNotApplied() {
        long totalTime = 0;
        int timeout = 15;
        try {
            DriverManager.setLoginTimeout(timeout * 3); // 30 seconds
            long timerStart = System.currentTimeMillis();

            try (Connection con = PrepUtil
                    .getConnection("jdbc:sqlserver://" + randomServer + ";loginTimeout=" + timeout)) {
                fail(TestResource.getResource("R_shouldNotConnect"));
            } catch (Exception e) {
                totalTime = System.currentTimeMillis() - timerStart;

                assertTrue(
                        (e.getMessage().toLowerCase()
                                .contains(TestResource.getResource("R_tcpipConnectionToHost").toLowerCase()))
                                || (TestUtils.getProperty(connectionString, "msiClientId") != null
                                        && e.getMessage().toLowerCase()
                                                .contains(TestResource.getResource("R_loginFailedMI").toLowerCase()))
                                || ((isSqlAzure() || isSqlAzureDW()) ? e.getMessage().toLowerCase()
                                        .contains(TestResource.getResource("R_connectTimedOut").toLowerCase()) : false),
                        e.getMessage());
            }

            // time should be < connection string loginTimeout
            assertTrue(totalTime < TimeUnit.SECONDS.toMillis(timeout),
                    "total time: " + totalTime + " loginTimeout: " + TimeUnit.SECONDS.toMillis(timeout));
        } finally {
            DriverManager.setLoginTimeout(0); // Default to 0 again
        }
    }

    // Test connect retry set to 0 (disabled)
    @Test
    public void testConnectRetryDisable() {
        long totalTime = 0;
        long timerStart = System.currentTimeMillis();
        int interval = defaultTimeout; // long interval so we can tell if there was a retry
        long timeout = defaultTimeout * 2; // long loginTimeout to accommodate the long interval

        // non existent server with long loginTimeout, should return fast if no retries at all
        try (Connection con = PrepUtil.getConnection(
                "jdbc:sqlserver://" + randomServer + ";transparentNetworkIPResolution=false;loginTimeout=" + timeout
                        + ";connectRetryCount=0;connectInterval=" + interval)) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            totalTime = System.currentTimeMillis() - timerStart;

            assertTrue(
                    e.getMessage().matches(TestUtils.formatErrorMsg("R_tcpipConnectionFailed"))
                            || (TestUtils.getProperty(connectionString, "msiClientId") != null && e.getMessage()
                                    .toLowerCase().contains(TestResource.getResource("R_loginFailedMI").toLowerCase()))
                            || ((isSqlAzure() || isSqlAzureDW()) ? e.getMessage().toLowerCase()
                                    .contains(TestResource.getResource("R_connectTimedOut").toLowerCase()) : false),
                    e.getMessage());
        }

        // if there was a retry then it would take at least 1 interval long, so if < interval means there were no retries
        assertTrue(totalTime < TimeUnit.SECONDS.toMillis(interval),
                "total time: " + totalTime + " interval: " + TimeUnit.SECONDS.toMillis(interval));
    }

    // Test connect retry for non-existent server with loginTimeout
    @Test
    public void testConnectRetryBadServer() {
        long totalTime = 0;
        long timerStart = System.currentTimeMillis();
        int timeout = 15;

        // non existent server with very short loginTimeout, no retry will happen as not a transient error
        try (Connection con = PrepUtil.getConnection("jdbc:sqlserver://" + randomServer + ";loginTimeout=" + timeout)) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            totalTime = System.currentTimeMillis() - timerStart;

            assertTrue(
                    (e.getMessage().toLowerCase()
                            .contains(TestResource.getResource("R_tcpipConnectionToHost").toLowerCase()))
                            || (TestUtils.getProperty(connectionString, "msiClientId") != null && e.getMessage()
                                    .toLowerCase().contains(TestResource.getResource("R_loginFailedMI").toLowerCase()))
                            || ((isSqlAzure() || isSqlAzureDW()) ? e.getMessage().toLowerCase()
                                    .contains(TestResource.getResource("R_connectTimedOut").toLowerCase()) : false),
                    e.getMessage());
        }

        // time should be < loginTimeout set
        assertTrue(totalTime < TimeUnit.SECONDS.toMillis(timeout),
                "total time: " + totalTime + " loginTimeout: " + TimeUnit.SECONDS.toMillis(timeout));
    }

    // Test connect retry for database error
    @Test
    public void testConnectRetryServerError() {
        String auth = TestUtils.getProperty(connectionString, "authentication");
        org.junit.Assume.assumeTrue(auth != null
                && (auth.equalsIgnoreCase("SqlPassword") || auth.equalsIgnoreCase("ActiveDirectoryPassword")));

        long totalTime = 0;
        long timerStart = System.currentTimeMillis();
        int interval = defaultTimeout; // long interval so we can tell if there was a retry
        long timeout = defaultTimeout * 2; // long loginTimeout to accommodate the long interval

        // non existent database with interval < loginTimeout this will generate a 4060 transient error and retry 1 time
        try (Connection con = PrepUtil.getConnection(
                TestUtils.addOrOverrideProperty(connectionString, "database", RandomUtil.getIdentifier("database"))
                        + ";loginTimeout=" + timeout + ";connectRetryCount=" + 1 + ";connectRetryInterval=" + interval
                        + ";transparentNetworkIPResolution=false")) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            totalTime = System.currentTimeMillis() - timerStart;

            assertTrue(
                    (e.getMessage().toLowerCase()
                            .contains(TestResource.getResource("R_cannotOpenDatabase").toLowerCase()))
                            || (TestUtils.getProperty(connectionString, "msiClientId") != null && e.getMessage()
                                    .toLowerCase().contains(TestResource.getResource("R_loginFailedMI").toLowerCase()))
                            || ((isSqlAzure() || isSqlAzureDW()) ? e.getMessage().toLowerCase()
                                    .contains(TestResource.getResource("R_connectTimedOut").toLowerCase()) : false),
                    e.getMessage());
        }

        // 1 retry should be at least 1 interval long but < 2 intervals
        assertTrue(TimeUnit.SECONDS.toMillis(interval) < totalTime,
                "interval: " + TimeUnit.SECONDS.toMillis(interval) + " total time: " + totalTime);
        assertTrue(totalTime < TimeUnit.SECONDS.toMillis(2 * interval),
                "total time: " + totalTime + " 2 * interval: " + TimeUnit.SECONDS.toMillis(interval));
    }

    // Test connect retry for database error using Datasource
    @Test
    public void testConnectRetryServerErrorDS() {
        String auth = TestUtils.getProperty(connectionString, "authentication");
        org.junit.Assume.assumeTrue(auth != null
                && (auth.equalsIgnoreCase("SqlPassword") || auth.equalsIgnoreCase("ActiveDirectoryPassword")));

        long totalTime = 0;
        long timerStart = System.currentTimeMillis();
        int interval = defaultTimeout; // long interval so we can tell if there was a retry
        long loginTimeout = defaultTimeout * 2; // long loginTimeout to accommodate the long interval

        // non existent database with interval < loginTimeout this will generate a 4060 transient error and retry 1 time
        SQLServerDataSource ds = new SQLServerDataSource();
        String connectStr = TestUtils.addOrOverrideProperty(connectionString, "database",
                RandomUtil.getIdentifier("database")) + ";logintimeout=" + loginTimeout + ";connectRetryCount=1"
                + ";connectRetryInterval=" + interval;
        updateDataSource(connectStr, ds);

        try (Connection con = PrepUtil.getConnection(connectStr)) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            assertTrue(
                    (e.getMessage().toLowerCase()
                            .contains(TestResource.getResource("R_cannotOpenDatabase").toLowerCase()))
                            || (TestUtils.getProperty(connectionString, "msiClientId") != null && e.getMessage()
                                    .toLowerCase().contains(TestResource.getResource("R_loginFailedMI").toLowerCase()))
                            || ((isSqlAzure() || isSqlAzureDW()) ? e.getMessage().toLowerCase()
                                    .contains(TestResource.getResource("R_connectTimedOut").toLowerCase()) : false),
                    e.getMessage());
            totalTime = System.currentTimeMillis() - timerStart;
        }

        // 1 retry should be at least 1 interval long but < 2 intervals
        assertTrue(TimeUnit.SECONDS.toMillis(interval) < totalTime,
                "interval: " + TimeUnit.SECONDS.toMillis(interval) + " total time: " + totalTime);
        assertTrue(totalTime < TimeUnit.SECONDS.toMillis(2 * interval),
                "total time: " + totalTime + " 2 * interval: " + TimeUnit.SECONDS.toMillis(2 * interval));
    }

    // Test connect retry for database error with loginTimeout
    @Test
    public void testConnectRetryTimeout() {
        long totalTime = 0;
        long timerStart = System.currentTimeMillis();
        int interval = defaultTimeout; // long interval so we can tell if there was a retry
        int loginTimeout = 2;

        // non existent database with very short loginTimeout so there is no time to do any retry
        try (Connection con = PrepUtil.getConnection(
                TestUtils.addOrOverrideProperty(connectionString, "database", RandomUtil.getIdentifier("database"))
                        + "connectRetryCount=" + (new Random().nextInt(256)) + ";connectRetryInterval=" + interval
                        + ";loginTimeout=" + loginTimeout)) {
            fail(TestResource.getResource("R_shouldNotConnect"));
        } catch (Exception e) {
            totalTime = System.currentTimeMillis() - timerStart;

            assertTrue(
                    (e.getMessage().toLowerCase()
                            .contains(TestResource.getResource("R_cannotOpenDatabase").toLowerCase()))
                            || (TestUtils.getProperty(connectionString, "msiClientId") != null && e.getMessage()
                                    .toLowerCase().contains(TestResource.getResource("R_loginFailedMI").toLowerCase()))
                            || ((isSqlAzure() || isSqlAzureDW()) ? e.getMessage().toLowerCase()
                                    .contains(TestResource.getResource("R_connectTimedOut").toLowerCase()) : false),
                    e.getMessage());
        }

        // if there was a retry then it would take at least 1 interval long, so if < interval means there were no retries
        assertTrue(totalTime < TimeUnit.SECONDS.toMillis(interval),
                "total time: " + totalTime + " interval: " + TimeUnit.SECONDS.toMillis(interval));
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testSocketTimeoutBoundedByLoginTimeoutReset() throws Exception {
        try (Connection con = PrepUtil.getConnection(connectionString + ";socketTimeout=90000;loginTimeout=10;");
                Statement stmt = con.createStatement()) {
            // Login timeout (10s) is less than the 15s sec WAITFOR DELAY. Upon a login attempt, socketTimeout should be bounded
            // by loginTimeout. After a successful login, when executing a query, socketTimeout should be reset to the
            // original 90000ms timeout. The statement below should successfully execute as socketTimeout should not be bounded
            // by loginTimeout, otherwise the test fails with a socket read timeout error.
            stmt.execute("WAITFOR DELAY '00:00:15';");
        }
    }

    // Test for detecting Azure server for connection retries
    @Test
    public void testAzureEndpointRetry() {

        try (Connection con = PrepUtil.getConnection(connectionString)) {
            Field fields[] = con.getClass().getSuperclass().getDeclaredFields();
            for (Field f : fields) {
                if (f.getName().equals("connectRetryCount")) {
                    f.setAccessible(true);
                    int retryCount = f.getInt(con);

                    if (TestUtils.isAzureSynapseOnDemand(con)) {
                        assertTrue(retryCount == 5); // AZURE_SYNAPSE_ONDEMAND_ENDPOINT_RETRY_COUNT_DEFAULT
                    } else if (TestUtils.isAzure(con)) {
                        assertTrue(retryCount == 2); // AZURE_SERVER_ENDPOINT_RETRY_COUNT_DEFAFULT
                    } else {
                        // default retryCount is 1 if not set in connection string
                        String retryCountFromConnStr = TestUtils.getProperty(connectionString, "connectRetryCount");
                        int expectedRetryCount = (retryCountFromConnStr != null) ? Integer
                                .parseInt(retryCountFromConnStr) : 1;

                        assertTrue(retryCount == expectedRetryCount); // default connectRetryCount
                    }
                }
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
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

    static Field[] getConnectionFields(Connection c) {
        Class<? extends Connection> cls = c.getClass();
        // SQLServerConnection43 is returned for Java >=9 so need to get super class
        if (cls.getName() == "com.microsoft.sqlserver.jdbc.SQLServerConnection43") {
            return cls.getSuperclass().getDeclaredFields();
        }

        return cls.getDeclaredFields();
    }

    @AfterAll
    public static void cleanup() throws SQLException {
        try (Connection conn = getConnection()) {
            dropWaitForDelayProcedure(conn);
        }
    }
}
