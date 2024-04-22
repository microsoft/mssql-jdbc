package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


/**
 * Test connection and statement retry for configurable retry logic
 */
public class ConfigurableRetryLogicTest extends AbstractTest {
    private static String connectionStringCRL = null;
    private static final String tableName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("crlTestTable"));

    @Test
    public void testStatementRetryPreparedStatement() throws Exception {
        connectionStringCRL = TestUtils.addOrOverrideProperty(connectionString, "retryExec",
                "{2714:3,2*2:CREATE;2715:1,3;+4060,4070}");

        try (Connection conn = DriverManager.getConnection(connectionStringCRL); Statement s = conn.createStatement()) {
            PreparedStatement ps = conn.prepareStatement("create table " + tableName + " (c1 int null);");
            try {
                createTable(s);
                ps.execute();
                fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (SQLServerException e) {
                assertTrue(e.getMessage().startsWith("There is already an object"),
                        TestResource.getResource("R_unexpectedExceptionContent") + ": " + e.getMessage());
            } finally {
                dropTable(s);
            }
        }
    }

    @Test
    public void testStatementRetryCallableStatement() throws Exception {
        connectionStringCRL = TestUtils.addOrOverrideProperty(connectionString, "retryExec",
                "{2714:3,2*2:CREATE;2715:1,3;+4060,4070}");
        String call = "create table " + tableName + " (c1 int null);";

        try (Connection conn = DriverManager.getConnection(connectionStringCRL); Statement s = conn.createStatement();
                CallableStatement cs = conn.prepareCall(call)) {
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

    public void testStatementRetry(String addedRetryParams) throws Exception {
        String cxnString = connectionString + addedRetryParams; // 2714 There is already an object named x

        try (Connection conn = DriverManager.getConnection(cxnString); Statement s = conn.createStatement()) {
            try {
                createTable(s);
                s.execute("create table " + tableName + " (c1 int null);");
                fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (SQLServerException e) {
                assertTrue(e.getMessage().startsWith("There is already an object"),
                        TestResource.getResource("R_unexpectedExceptionContent") + ": " + e.getMessage());
            } finally {
                dropTable(s);
            }
        }
    }

    public void testStatementRetryWithShortQueryTimeout(String addedRetryParams) throws Exception {
        String cxnString = connectionString + addedRetryParams; // 2714 There is already an object named x

        try (Connection conn = DriverManager.getConnection(cxnString); Statement s = conn.createStatement()) {
            try {
                createTable(s);
                s.execute("create table " + tableName + " (c1 int null);");
                fail(TestResource.getResource("R_expectedFailPassed"));
            } finally {
                dropTable(s);
            }
        }
    }

    @Test
    public void timingTests() {

    }

    @Test
    public void readFromFile() {
        try {
            testStatementRetry("");
        } catch (Exception e) {
            Assertions.fail(TestResource.getResource("R_unexpectedException"));
        }
    }

    @Test
    public void testCorrectlyFormattedRulesPass() {
        // Correctly formatted rules
        try {
            // Empty rule set
            testStatementRetry("retryExec={};");
            testStatementRetry("retryExec={;};");

            // Test length 1
            testStatementRetry("retryExec={4060};");
            testStatementRetry("retryExec={+4060,4070};");

            testStatementRetry("retryExec={2714:1;};");

            // Test length 2
            testStatementRetry("retryExec={2714:1,3;};");

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

    @Test
    public void testCorrectRetryError() throws Exception {
        // Test incorrect format (NaN)
        try {
            testStatementRetry("retryExec={TEST};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidParameterFormat")));
        }

        // Test empty error
        try {
            testStatementRetry("retryExec={:1,2*2:CREATE};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidParameterFormat")));
        }
    }

    @Test
    public void testProperRetryCount() throws Exception {
        // Test min
        try {
            testStatementRetry("retryExec={2714,2716:-1,2+2:CREATE};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidParameterFormat")));
        }

        // Test max (query timeout)
        try {
            testStatementRetryWithShortQueryTimeout("queryTimeout=10;retryExec={2714,2716:11,2+2:CREATE};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidRetryInterval")));
        }
    }

    @Test
    public void testProperInitialRetryTime() throws Exception {
        // Test min
        try {
            testStatementRetry("retryExec={2714,2716:4,-1+1:CREATE};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidParameterFormat")));
        }

        // Test max
        try {
            testStatementRetryWithShortQueryTimeout("queryTimeout=3;retryExec={2714,2716:4,100+1:CREATE};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidRetryInterval")));
        }
    }

    @Test
    public void testProperOperand() throws Exception {
        // Test incorrect
        try {
            testStatementRetry("retryExec={2714,2716:1,2AND2:CREATE};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidParameterFormat")));
        }
    }

    @Test
    public void testProperRetryChange() throws Exception {
        // Test incorrect
        try {
            testStatementRetry("retryExec={2714,2716:1,2+2:CREATE};");
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidParameterFormat")));
        }
    }

    private static void createTable(Statement stmt) throws SQLException {
        String sql = "create table " + tableName + " (c1 int null);";
        stmt.execute(sql);
    }

    private static void dropTable(Statement stmt) throws SQLException {
        TestUtils.dropTableIfExists(tableName, stmt);
    }

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }
}
