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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class TimeoutTest extends AbstractTest {
    String randomServer = RandomUtil.getIdentifier("Server");
    static String waitForDelaySPName = RandomUtil.getIdentifier("waitForDelaySP");
    final int waitForDelaySeconds = 10;

    @Test
    @Tag("slow")
    public void testDefaultLoginTimeout() {
        long timerStart = 0;
        long timerEnd = 0;

        timerStart = System.currentTimeMillis();
        // Try a non existing server and see if the default timeout is 15 seconds
        try (Connection con = DriverManager
                .getConnection("jdbc:sqlserver://" + randomServer + ";user=sa;password=pwd;")) {

        } catch (Exception e) {
            assertTrue(e.getMessage().contains(TestResource.getResource("R_tcpipConnectionToHost")));
            timerEnd = System.currentTimeMillis();
        }

        assertTrue(0 != timerEnd, TestResource.getResource("R_shouldNotConnect"));

        long timeDiff = timerEnd - timerStart;
        assertTrue(timeDiff > 14000);
    }

    @Test
    public void testFailoverInstanceResolution() throws SQLException {
        long timerStart = 0;
        long timerEnd = 0;

        timerStart = System.currentTimeMillis();
        // Try a non existing server and see if the default timeout is 15 seconds
        try (Connection con = DriverManager
                .getConnection("jdbc:sqlserver://" + randomServer + ";databaseName=FailoverDB_abc;failoverPartner="
                        + randomServer + "\\foo;user=sa;password=pwd;")) {} catch (Exception e) {
            assertTrue(e.getMessage().contains(TestResource.getResource("R_tcpipConnectionToHost")));
            timerEnd = System.currentTimeMillis();
        }
        assertTrue(0 != timerEnd, TestResource.getResource("R_shouldNotConnect"));

        long timeDiff = timerEnd - timerStart;
        assertTrue(timeDiff > 14000);
    }

    @Test
    public void testFOInstanceResolution2() throws SQLException {
        long timerStart = 0;
        long timerEnd = 0;

        timerStart = System.currentTimeMillis();
        try (Connection con = DriverManager
                .getConnection("jdbc:sqlserver://" + randomServer + "\\fooggg;databaseName=FailoverDB;failoverPartner="
                        + randomServer + "\\foo;user=sa;password=pwd;")) {} catch (Exception e) {
            timerEnd = System.currentTimeMillis();
        }
        assertTrue(0 != timerEnd, TestResource.getResource("R_shouldNotConnect"));

        long timeDiff = timerEnd - timerStart;
        assertTrue(timeDiff > 14000);
    }

    /**
     * When query timeout occurs, the connection is still usable.
     * 
     * @throws Exception
     */
    @Test
    public void testQueryTimeout() throws Exception {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString)) {
            dropWaitForDelayProcedure(conn);
            createWaitForDelayPreocedure(conn);
        }

        try (SQLServerConnection conn = (SQLServerConnection) DriverManager
                .getConnection(connectionString + ";queryTimeout=" + (waitForDelaySeconds / 2) + ";")) {

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("exec " + AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName));
                throw new Exception(TestResource.getResource("R_expectedExceptionNotThrown"));
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
                fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        }
    }

    /**
     * Tests sanity of connection property.
     * 
     * @throws Exception
     */
    @Test
    public void testCancelQueryTimeout() throws Exception {

        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString)) {
            dropWaitForDelayProcedure(conn);
            createWaitForDelayPreocedure(conn);
        }

        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString
                + ";queryTimeout=" + (waitForDelaySeconds / 2) + ";cancelQueryTimeout=" + waitForDelaySeconds + ";")) {

            try (SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {
                stmt.execute("exec " + AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName));
                throw new Exception(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception e) {
                if (!(e instanceof java.sql.SQLTimeoutException)) {
                    throw e;
                }
                assertEquals(e.getMessage(), TestResource.getResource("R_queryTimedOut"),
                        TestResource.getResource("R_invalidExceptionMessage"));
            }

            try (SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {
                stmt.execute("SELECT @@version");
            } catch (Exception e) {
                fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        }
    }

    /**
     * Tests sanity of connection property.
     * 
     * @throws Exception
     */
    @Test
    public void testCancelQueryTimeoutOnStatement() throws Exception {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString)) {
            dropWaitForDelayProcedure(conn);
            createWaitForDelayPreocedure(conn);
        }

        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString + ";")) {

            try (SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {
                stmt.setQueryTimeout(waitForDelaySeconds / 2);
                stmt.setCancelQueryTimeout(waitForDelaySeconds);
                stmt.execute("exec " + AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName));
                throw new Exception(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception e) {
                if (!(e instanceof java.sql.SQLTimeoutException)) {
                    throw e;
                }
                assertEquals(e.getMessage(), TestResource.getResource("R_queryTimedOut"),
                        TestResource.getResource("R_invalidExceptionMessage"));
            }

            try (SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {
                stmt.execute("SELECT @@version");
            } catch (Exception e) {
                fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        }
    }

    /**
     * When socketTimeout occurs, the connection will be marked as closed.
     * 
     * @throws Exception
     */
    @Test
    public void testSocketTimeout() throws Exception {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString)) {
            dropWaitForDelayProcedure(conn);
            createWaitForDelayPreocedure(conn);
        }

        try (SQLServerConnection conn = (SQLServerConnection) DriverManager
                .getConnection(connectionString + ";socketTimeout=" + (waitForDelaySeconds * 1000 / 2) + ";")) {

            try (SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {
                stmt.execute("exec " + AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName));
                throw new Exception(TestResource.getResource("R_expectedExceptionNotThrown"));
            } catch (Exception e) {
                if (!(e instanceof SQLException)) {
                    throw e;
                }
                assertEquals(e.getMessage(), TestResource.getResource("R_readTimedOut"),
                        TestResource.getResource("R_invalidExceptionMessage"));
            }

            try (SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {
                stmt.execute("SELECT @@version");
            } catch (SQLException e) {
                assertEquals(e.getMessage(), TestResource.getResource("R_connectionIsClosed"),
                        TestResource.getResource("R_invalidExceptionMessage"));
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        }
    }

    private static void dropWaitForDelayProcedure(SQLServerConnection conn) throws SQLException {
        try (SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName), stmt);
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        }
    }

    private void createWaitForDelayPreocedure(SQLServerConnection conn) throws SQLException {
        try (SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {
            String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName) + " AS"
                    + " BEGIN" + " WAITFOR DELAY '00:00:" + waitForDelaySeconds + "';" + " END";
            stmt.execute(sql);
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        }
    }

    @AfterAll
    public static void cleanup() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString)) {
            dropWaitForDelayProcedure(conn);
        }
    }
}
