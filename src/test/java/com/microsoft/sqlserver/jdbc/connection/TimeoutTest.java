/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;
import com.microsoft.sqlserver.testframework.util.RandomUtil;

@RunWith(JUnitPlatform.class)
public class TimeoutTest extends AbstractTest {
    String randomServer = RandomUtil.getIdentifier("Server");
    String waitForDelaySPName = "waitForDelaySP";
    final int waitForDelaySeconds = 10;

    @Test
    @Tag("slow")
    public void testDefaultLoginTimeout() {
        long timerStart = 0;
        long timerEnd = 0;
        try {
            timerStart = System.currentTimeMillis();
            // Try a non existing server and see if the default timeout is 15 seconds
            DriverManager.getConnection("jdbc:sqlserver://" + randomServer + ";user=sa;password=pwd;");
        }
        catch (Exception e) {
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
        try {
            timerStart = System.currentTimeMillis();
            // Try a non existing server and see if the default timeout is 15 seconds
            DriverManager.getConnection("jdbc:sqlserver://" + randomServer + ";databaseName=FailoverDB_abc;failoverPartner=" + randomServer
                    + "\\foo;user=sa;password=pwd;");
        }
        catch (Exception e) {
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
        try {
            timerStart = System.currentTimeMillis();
            // Try a non existing server and see if the default timeout is 15 secs at least
            DriverManager.getConnection("jdbc:sqlserver://" + randomServer + "\\fooggg;databaseName=FailoverDB;failoverPartner=" + randomServer
                    + "\\foo;user=sa;password=pwd;");
        }
        catch (Exception e) {
            timerEnd = System.currentTimeMillis();
        }
        assertTrue(0 != timerEnd, TestResource.getResource("R_shouldNotConnect"));

        long timeDiff = timerEnd - timerStart;
        assertTrue(timeDiff > 14000);
    }

    /**
     * When query timeout occurs, the connection is still usable. 
     * @throws Exception
     */
    @Test
    public void testQueryTimeout() throws Exception {
        SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);

        dropWaitForDelayProcedure(conn);
        createWaitForDelayPreocedure(conn);

        conn = (SQLServerConnection) DriverManager.getConnection(connectionString + ";queryTimeout=" + (waitForDelaySeconds / 2) + ";");
        
        try {
            conn.createStatement().execute("exec " + waitForDelaySPName);
            throw new Exception(TestResource.getResource("R_expectedExceptionNotThrown"));
        }
        catch (Exception e) {
            if (!(e instanceof java.sql.SQLTimeoutException)) {
                throw e;
            }
            assertEquals(e.getMessage(), TestResource.getResource("R_queryTimedOut"), TestResource.getResource("R_invalidExceptionMessage"));
        }
        try{
            conn.createStatement().execute("SELECT @@version");
        }catch (Exception e) {
           fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString() );
        }
    }
    
    /**
     * Tests sanity of connection property.
     * @throws Exception
     */
    @Test
    public void testCancelQueryTimeout() throws Exception {
        SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);

        dropWaitForDelayProcedure(conn);
        createWaitForDelayPreocedure(conn);

        conn = (SQLServerConnection) DriverManager.getConnection(connectionString + ";queryTimeout=" + (waitForDelaySeconds / 2) + ";cancelQueryTimeout=" + waitForDelaySeconds + ";");
        
        try {
            SQLServerStatement statement = (SQLServerStatement) conn.createStatement();
            statement.execute("exec " + waitForDelaySPName);
            throw new Exception(TestResource.getResource("R_expectedExceptionNotThrown"));
        }
        catch (Exception e) {
            if (!(e instanceof java.sql.SQLTimeoutException)) {
                throw e;
            }
            assertEquals(e.getMessage(), TestResource.getResource("R_queryTimedOut"), TestResource.getResource("R_invalidExceptionMessage"));
        }
        try{
            conn.createStatement().execute("SELECT @@version");
        }catch (Exception e) {
           fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString() );
        }
    }

    /**
     * Tests sanity of connection property.
     * @throws Exception
     */
    @Test
    public void testCancelQueryTimeoutOnStatement() throws Exception {
        SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);

        dropWaitForDelayProcedure(conn);
        createWaitForDelayPreocedure(conn);

        conn = (SQLServerConnection) DriverManager.getConnection(connectionString + ";");
        
        try {
            SQLServerStatement statement = (SQLServerStatement) conn.createStatement();
            statement.setQueryTimeout(waitForDelaySeconds/2);
            statement.setCancelQueryTimeout(waitForDelaySeconds);
            statement.execute("exec " + waitForDelaySPName);
            throw new Exception(TestResource.getResource("R_expectedExceptionNotThrown"));
        }
        catch (Exception e) {
            if (!(e instanceof java.sql.SQLTimeoutException)) {
                throw e;
            }
            assertEquals(e.getMessage(), TestResource.getResource("R_queryTimedOut"), TestResource.getResource("R_invalidExceptionMessage"));
        }
        try{
            conn.createStatement().execute("SELECT @@version");
        }catch (Exception e) {
           fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString() );
        }
    }
    
    /**
     * When socketTimeout occurs, the connection will be marked as closed.
     * @throws Exception
     */
    @Test
    public void testSocketTimeout() throws Exception {
        SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);

        dropWaitForDelayProcedure(conn);
        createWaitForDelayPreocedure(conn);

        conn = (SQLServerConnection) DriverManager.getConnection(connectionString + ";socketTimeout=" + (waitForDelaySeconds * 1000 / 2) + ";");

        try {
            conn.createStatement().execute("exec " + waitForDelaySPName);
            throw new Exception(TestResource.getResource("R_expectedExceptionNotThrown"));
        }
        catch (Exception e) {
            if (!(e instanceof SQLException)) {
                throw e;
            }
            assertEquals(e.getMessage(), TestResource.getResource("R_readTimedOut"), TestResource.getResource("R_invalidExceptionMessage"));
        }
        try{
            conn.createStatement().execute("SELECT @@version");
        }catch (SQLException e) {
            assertEquals(e.getMessage(), TestResource.getResource("R_connectionIsClosed"), TestResource.getResource("R_invalidExceptionMessage"));
        }
    }

    private void dropWaitForDelayProcedure(SQLServerConnection conn) throws SQLException {
        Utils.dropProcedureIfExists(waitForDelaySPName, conn.createStatement());
    }

    private void createWaitForDelayPreocedure(SQLServerConnection conn) throws SQLException {
        String sql = "CREATE PROCEDURE " + waitForDelaySPName + " AS" + " BEGIN" + " WAITFOR DELAY '00:00:" + waitForDelaySeconds + "';" + " END";
        conn.createStatement().execute(sql);
    }
}
