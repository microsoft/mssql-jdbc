/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.configurableretry;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.File;
import java.io.FileWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


/**
 * Test connection and statement retry for configurable retry logic.
 */
public class ConfigurableRetryLogicTest extends AbstractTest {
    /**
     * The table used throughout the tests.
     */
    private static final String CRLTestTable = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("crlTestTable"));

    /**
     * Sets up tests.
     *
     * @throws Exception
     *         if an exception occurs
     */
    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /**
     * Test that the SQLServerConnection methods getRetryExec and setRetryExec correctly get the existing retryExec, and
     * set the retryExec connection parameter respectively.
     * 
     * @throws Exception
     *         if an exception occurs
     */
    @Test
    public void testRetryExecConnectionStringOption() throws Exception {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
                Statement s = conn.createStatement()) {
            String test = conn.getRetryExec();
            assertTrue(test.isEmpty());
            conn.setRetryExec("{2714:3,2*2:CREATE;2715:1,3}");
            test = conn.getRetryExec();
            assertFalse(test.isEmpty());
            try {
                PreparedStatement ps = conn.prepareStatement("create table " + CRLTestTable + " (c1 int null);");
                createTable(s);
                ps.execute();
                Assertions.fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (SQLServerException e) {
                assertTrue(e.getMessage().startsWith("There is already an object"),
                        TestResource.getResource("R_unexpectedExceptionContent") + ": " + e.getMessage());
            } finally {
                dropTable(s);
            }
        }
    }

    @Test
    public void testRetryConnConnectionStringOption() throws Exception {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
                Statement s = conn.createStatement()) {
            String test = conn.getRetryConn();
            assertTrue(test.isEmpty());
            conn.setRetryConn("{4060}");
            test = conn.getRetryConn();
            assertFalse(test.isEmpty());
            try {
                PreparedStatement ps = conn.prepareStatement("create table " + CRLTestTable + " (c1 int null);");
                createTable(s);
                ps.execute();
                Assertions.fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (SQLServerException e) {
                assertTrue(e.getMessage().startsWith("There is already an object"),
                        TestResource.getResource("R_unexpectedExceptionContent") + ": " + e.getMessage());
            } finally {
                dropTable(s);
            }
        }
    }

    /**
     * Tests that statement retry with prepared statements correctly retries given the provided retryExec rule.
     * 
     * @throws Exception
     *         if unable to connect or execute against db
     */
    @Test
    public void testStatementRetryPreparedStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(
                TestUtils.addOrOverrideProperty(connectionString, "retryExec", "{2714:3,2*2:CREATE;2715:1,3}"));
                Statement s = conn.createStatement();
                PreparedStatement ps = conn.prepareStatement("create table " + CRLTestTable + " (c1 int null);")) {
            try {
                createTable(s);
                ps.execute();
                Assertions.fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (SQLServerException e) {
                assertTrue(e.getMessage().startsWith("There is already an object"),
                        TestResource.getResource("R_unexpectedExceptionContent") + ": " + e.getMessage());
            } finally {
                dropTable(s);
            }
        }
    }

    /**
     * Tests that statement retry with callable statements correctly retries given the provided retryExec rule.
     * 
     * @throws Exception
     *         if unable to connect or execute against db
     */
    @Test
    public void testStatementRetryCallableStatement() throws Exception {
        try (Connection conn = DriverManager.getConnection(
                TestUtils.addOrOverrideProperty(connectionString, "retryExec", "{2714:3,2*2:CREATE;2715:1,3}"));
                Statement s = conn.createStatement();
                CallableStatement cs = conn.prepareCall("create table " + CRLTestTable + " (c1 int null);")) {
            try {
                createTable(s);
                cs.execute();
                fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (SQLServerException e) {
                assertTrue(e.getMessage().startsWith("There is already an object"),
                        TestResource.getResource("R_unexpectedExceptionContent") + ": " + e.getMessage());
            } finally {
                dropTable(s);
            }
        }
    }

    /**
     * Tests that statement retry with SQL server statements correctly retries given the provided retryExec rule.
     * 
     * @throws Exception
     *         if unable to connect or execute against db
     */
    public void testStatementRetry(String addedRetryParams) throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString + addedRetryParams);
                Statement s = conn.createStatement()) {
            try {
                createTable(s);
                s.execute("create table " + CRLTestTable + " (c1 int null);");
                fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (SQLServerException e) {
                assertTrue(e.getMessage().startsWith("There is already an object"),
                        TestResource.getResource("R_unexpectedExceptionContent") + ": " + e.getMessage());
            } finally {
                dropTable(s);
            }
        }
    }

    /**
     * Tests that statement retry with SQL server statements correctly attempts to retry, but eventually cancels due
     * to the retry wait interval being longer than queryTimeout.
     *
     * @throws Exception
     *         if unable to connect or execute against db
     */
    public void testStatementRetryWithShortQueryTimeout(String addedRetryParams) throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString + addedRetryParams);
                Statement s = conn.createStatement()) {
            try {
                createTable(s);
                s.execute("create table " + CRLTestTable + " (c1 int null);");
                fail(TestResource.getResource("R_expectedFailPassed"));
            } finally {
                dropTable(s);
            }
        }
    }

    /**
     * Tests connection retry. Used in other tests.
     *
     * @throws Exception
     *         if unable to connect or execute against db
     */
    public void testConnectionRetry(String replacedDbName, String addedRetryParams) throws Exception {
        String cxnString = connectionString + addedRetryParams;
        cxnString = TestUtils.addOrOverrideProperty(cxnString, "database", replacedDbName);

        try (Connection conn = DriverManager.getConnection(cxnString); Statement s = conn.createStatement()) {
            try {
                fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (Exception e) {
                System.out.println("blah");
                assertTrue(e.getMessage().startsWith("There is already an object"),
                        TestResource.getResource("R_unexpectedExceptionContent") + ": " + e.getMessage());
            } finally {
                dropTable(s);
            }
        }
    }

    /**
     * Tests that the correct number of retries are happening for all statement scenarios. Tests are expected to take
     * a minimum of the sum of whatever has been defined for the waiting intervals. Maximum is not tested due to the
     * unpredictable factor of slowness that can be applied to these tests.
     */
    @Test
    public void statementTimingTests() {
        long totalTime;
        long timerStart = System.currentTimeMillis();

        // A single retry immediately
        try {
            testStatementRetry("retryExec={2714:1;};");
        } catch (Exception e) {
            Assertions.fail(TestResource.getResource("R_unexpectedException"));
        } finally {
            totalTime = System.currentTimeMillis() - timerStart;
            assertTrue(totalTime < TimeUnit.SECONDS.toMillis(10),
                    "total time: " + totalTime + ", expected time: " + TimeUnit.SECONDS.toMillis(10));
        }

        timerStart = System.currentTimeMillis();

        // A single retry waiting 5 seconds
        try {
            testStatementRetry("retryExec={2714:1,5;};");
        } catch (Exception e) {
            Assertions.fail(TestResource.getResource("R_unexpectedException"));
        } finally {
            totalTime = System.currentTimeMillis() - timerStart;
            assertTrue(totalTime > TimeUnit.SECONDS.toMillis(5),
                    "total time: " + totalTime + ", expected minimum time: " + TimeUnit.SECONDS.toMillis(5));
        }

        timerStart = System.currentTimeMillis();

        // Two retries. The first after 2 seconds, the next after 6
        try {
            testStatementRetry("retryExec={2714,2716:2,2*3:CREATE};");
        } catch (Exception e) {
            Assertions.fail(TestResource.getResource("R_unexpectedException"));
        } finally {
            totalTime = System.currentTimeMillis() - timerStart;
            assertTrue(totalTime > TimeUnit.SECONDS.toMillis(2),
                    "total time: " + totalTime + ", expected minimum time: " + TimeUnit.SECONDS.toMillis(8));
        }

        timerStart = System.currentTimeMillis();

        // Two retries. The first after 3 seconds, the next after 7
        try {
            testStatementRetry("retryExec={2714,2716:2,3+4:CREATE};");
        } catch (Exception e) {
            Assertions.fail(TestResource.getResource("R_unexpectedException"));
        } finally {
            totalTime = System.currentTimeMillis() - timerStart;
            assertTrue(totalTime > TimeUnit.SECONDS.toMillis(3),
                    "total time: " + totalTime + ", expected minimum time: " + TimeUnit.SECONDS.toMillis(10));
        }
    }

    /**
     * Tests that configurable retry logic correctly parses, and retries using, multiple rules provided at once.
     */
    @Test
    public void multipleRules() {
        try {
            testStatementRetry("retryExec={2716:1,2*2:CREATE;2714:1,2*2:CREATE};");
        } catch (Exception e) {
            Assertions.fail(TestResource.getResource("R_unexpectedException"));
        }
    }

    /**
     * Tests that CRL is able to read from a properties file, in the event the connection property is not used.
     */
    @Test
    public void readFromFile() {
        File propsFile = null;
        try {
            propsFile = new File("mssql-jdbc.properties");
            FileWriter propFileWriter = new FileWriter(propsFile);
            propFileWriter.write("retryExec={2716:1,2*2:CREATE;2714:1,2*2:CREATE};");
            propFileWriter.close();
            testStatementRetry("");

        } catch (Exception e) {
            Assertions.fail(TestResource.getResource("R_unexpectedException"));
        } finally {
            if (propsFile != null && !propsFile.delete()) { // If unable to delete, fail test
                Assertions.fail(TestResource.getResource("R_unexpectedException"));
            }
        }
    }

    /**
     * Ensure that CRL properly re-reads rules after INTERVAL_BETWEEN_READS_IN_MS (30 secs).
     */
    @Test
    public void rereadAfterInterval() {
        try {
            testStatementRetry("retryExec={2716:1,2*2:CREATE;};");
            Thread.sleep(30000); // Sleep to ensure it has been INTERVAL_BETWEEN_READS_IN_MS between reads
            testStatementRetry("retryExec={2714:1,2*2:CREATE;};");
        } catch (Exception e) {
            Assertions.fail(TestResource.getResource("R_unexpectedException"));
        }
    }

    /**
     * Tests that rules of the correct length, and containing valid values, pass.
     */
    @Test
    public void testCorrectlyFormattedRules() {
        // Correctly formatted rules
        try {
            // Empty rule set
            testStatementRetry("retryExec={};");
            testStatementRetry("retryExec={;};");

            // Test length 1
            testStatementRetry("retryExec={2714:1;};");

            // Test length 2
            testStatementRetry("retryExec={2714:1,3;};");

            // Test length 2, with operand, but no initial-retry-time
            testStatementRetry("retryExec={2714:1,3+;};");
            testStatementRetry("retryExec={2714:1,3*;};");

            // Test length 3, but query is empty
            testStatementRetry("retryExec={2714:1,3:;};");

            // Test length 3, also multiple statement errors
            testStatementRetry("retryExec={2714,2716:1,2*2:CREATE};");

            // Same as above but using + operator
            testStatementRetry("retryExec={2714,2716:1,2+2:CREATE};");
            testStatementRetry("retryExec={2714,2716:1,2+2};");

        } catch (Exception e) {
            Assertions.fail(TestResource.getResource("R_unexpectedException"));
        }

        // Test length >3
        try {
            testStatementRetry("retryExec={2714,2716:1,2*2:CREATE:4};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidRuleFormat")));
        } catch (Exception e) {
            Assertions.fail(TestResource.getResource("R_unexpectedException"));
        }
    }

    /**
     * Tests that too many timing parameters (>2) causes InvalidParameterFormat Exception.
     */
    @Test
    public void testTooManyTimings() {
        try {
            testStatementRetry("retryExec={2714,2716:1,2*2,1:CREATE};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_invalidParameterNumber")));
        } catch (Exception e) {
            Assertions.fail(TestResource.getResource("R_unexpectedException"));
        }
    }

    /**
     * Tests that rules with an invalid retry error correctly fail.
     * 
     * @throws Exception
     *         for the invalid parameter
     */
    @Test
    public void testRetryError() throws Exception {
        // Test incorrect format (NaN)
        try {
            testStatementRetry("retryExec={TEST:TEST};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_invalidParameterNumber")));
        }

        // Test empty error
        try {
            testStatementRetry("retryExec={:1,2*2:CREATE};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_invalidParameterNumber")));
        }
    }

    /**
     * Tests that rules with an invalid retry count correctly fail.
     * 
     * @throws Exception
     *         for the invalid parameter
     */
    @Test
    public void testRetryCount() throws Exception {
        // Test min
        try {
            testStatementRetry("retryExec={2714,2716:-1,2+2:CREATE};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_invalidParameterNumber")));
        }

        // Test max (query timeout)
        try {
            testStatementRetryWithShortQueryTimeout("queryTimeout=3;retryExec={2714,2716:11,2+2:CREATE};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidRetryInterval")));
        }
    }

    /**
     * Tests that rules with an invalid initial retry time correctly fail.
     * 
     * @throws Exception
     *         for the invalid parameter
     */
    @Test
    public void testInitialRetryTime() throws Exception {
        // Test min
        try {
            testStatementRetry("retryExec={2714,2716:4,-1+1:CREATE};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_invalidParameterNumber")));
        }

        // Test max
        try {
            testStatementRetryWithShortQueryTimeout("queryTimeout=3;retryExec={2714,2716:4,100+1:CREATE};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidRetryInterval")));
        }
    }

    /**
     * Tests that rules with an invalid operand correctly fail.
     * 
     * @throws Exception
     *         for the invalid parameter
     */
    @Test
    public void testOperand() throws Exception {
        try {
            testStatementRetry("retryExec={2714,2716:1,2AND2:CREATE};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_invalidParameterNumber")));
        }
    }

    /**
     * Tests that rules with an invalid retry change correctly fail.
     * 
     * @throws Exception
     *         for the invalid parameter
     */
    @Test
    public void testRetryChange() throws Exception {
        try {
            testStatementRetry("retryExec={2714,2716:1,2+2:CREATE};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_invalidParameterNumber")));
        }
    }

    /**
     * Tests that the correct number of retries are happening for all connection scenarios. Tests are expected to take
     * a minimum of the sum of whatever has been defined for the waiting intervals. Maximum is not tested due to the
     * unpredictable factor of slowness that can be applied to these tests.
     */
    @Test
    public void connectionTimingTest() {
        long totalTime;
        long timerStart = System.currentTimeMillis();

        // No retries since CRL rules override, expected time ~1 second
        try {
            testConnectionRetry("blah", "retryConn={9999};");
        } catch (Exception e) {
            assertTrue(
                    (e.getMessage().toLowerCase()
                            .contains(TestResource.getResource("R_cannotOpenDatabase").toLowerCase()))
                            || (TestUtils.getProperty(connectionString, "msiClientId") != null && e.getMessage()
                                    .toLowerCase().contains(TestResource.getResource("R_loginFailedMI").toLowerCase()))
                            || ((isSqlAzure() || isSqlAzureDW()) && e.getMessage().toLowerCase()
                                    .contains(TestResource.getResource("R_connectTimedOut").toLowerCase())),
                    e.getMessage());
        }

        timerStart = System.currentTimeMillis();
        long expectedMinTime = 10;

        // (0s attempt + 0s attempt + 10s wait + 0s attempt) = expected 10s execution time
        try {
            testConnectionRetry("blah", "retryConn={4060,4070};connectRetryCount=2;connectRetryInterval=10");
        } catch (Exception e) {
            assertTrue(
                    (e.getMessage().toLowerCase()
                            .contains(TestResource.getResource("R_cannotOpenDatabase").toLowerCase()))
                            || (TestUtils.getProperty(connectionString, "msiClientId") != null && e.getMessage()
                                    .toLowerCase().contains(TestResource.getResource("R_loginFailedMI").toLowerCase()))
                            || ((isSqlAzure() || isSqlAzureDW()) && e.getMessage().toLowerCase()
                                    .contains(TestResource.getResource("R_connectTimedOut").toLowerCase())),
                    e.getMessage());

            if (e.getMessage().toLowerCase().contains(TestResource.getResource("R_cannotOpenDatabase").toLowerCase())) {
                // Only check the timing if the correct error, "cannot open database", is returned.
                totalTime = System.currentTimeMillis() - timerStart;
                assertTrue(totalTime > TimeUnit.SECONDS.toMillis(expectedMinTime), "total time: " + totalTime
                        + ", expected min time: " + TimeUnit.SECONDS.toMillis(expectedMinTime));
            }
        }

        timerStart = System.currentTimeMillis();

        // Append should work the same way
        try {
            testConnectionRetry("blah", "retryConn={+4060,4070};connectRetryCount=2;connectRetryInterval=10");
        } catch (Exception e) {
            assertTrue(
                    (e.getMessage().toLowerCase()
                            .contains(TestResource.getResource("R_cannotOpenDatabase").toLowerCase()))
                            || (TestUtils.getProperty(connectionString, "msiClientId") != null && e.getMessage()
                                    .toLowerCase().contains(TestResource.getResource("R_loginFailedMI").toLowerCase()))
                            || ((isSqlAzure() || isSqlAzureDW()) && e.getMessage().toLowerCase()
                                    .contains(TestResource.getResource("R_connectTimedOut").toLowerCase())),
                    e.getMessage());

            if (e.getMessage().toLowerCase().contains(TestResource.getResource("R_cannotOpenDatabase").toLowerCase())) {
                // Only check the timing if the correct error, "cannot open database", is returned.
                totalTime = System.currentTimeMillis() - timerStart;
                assertTrue(totalTime > TimeUnit.SECONDS.toMillis(expectedMinTime), "total time: " + totalTime
                        + ", expected min time: " + TimeUnit.SECONDS.toMillis(expectedMinTime));
            }
        }
    }

    /**
     * Creates table for use in ConfigurableRetryLogic tests.
     *
     * @param stmt
     *        the SQL statement to use to create the table
     * @throws SQLException
     *         if unable to execute statement
     */
    private static void createTable(Statement stmt) throws SQLException {
        String sql = "create table " + CRLTestTable + " (c1 int null);";
        stmt.execute(sql);
    }

    /**
     * Drops the table used in ConfigurableRetryLogic tests.
     *
     * @param stmt
     *        the SQL statement to use to drop the table
     * @throws SQLException
     *         if unable to execute statement
     */
    private static void dropTable(Statement stmt) throws SQLException {
        TestUtils.dropTableIfExists(CRLTestTable, stmt);
    }
}
