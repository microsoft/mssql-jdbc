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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;

import javax.sql.ConnectionEvent;
import javax.sql.PooledConnection;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBTable;
import com.microsoft.sqlserver.testframework.util.RandomUtil;

@RunWith(JUnitPlatform.class)
public class ConnectionDriverTest extends AbstractTest {
    // If no retry is done, the function should atleast exit in 5 seconds
    static int threshHoldForNoRetryInMilliseconds = 5000;
    static int loginTimeOutInSeconds = 10;

    String randomServer = RandomUtil.getIdentifier("Server");

    /**
     * test SSL properties
     * 
     * @throws SQLException
     */
    @Test
    public void testConnectionDriver() throws SQLException {
        SQLServerDriver d = new SQLServerDriver();
        Properties info = new Properties();
        StringBuffer url = new StringBuffer();
        url.append("jdbc:sqlserver://" + randomServer + ";packetSize=512;");
        // test defaults
        DriverPropertyInfo[] infoArray = d.getPropertyInfo(url.toString(), info);
        for (DriverPropertyInfo anInfoArray1 : infoArray) {
            logger.fine(anInfoArray1.name);
            logger.fine(anInfoArray1.description);
            logger.fine(new Boolean(anInfoArray1.required).toString());
            logger.fine(anInfoArray1.value);
        }

        url.append("encrypt=true; trustStore=someStore; trustStorePassword=somepassword;");
        url.append("hostNameInCertificate=someHost; trustServerCertificate=true");
        infoArray = d.getPropertyInfo(url.toString(), info);
        for (DriverPropertyInfo anInfoArray : infoArray) {
            if (anInfoArray.name.equals("encrypt")) {
                assertTrue(anInfoArray.value.equals("true"), "Values are different");
            }
            if (anInfoArray.name.equals("trustStore")) {
                assertTrue(anInfoArray.value.equals("someStore"), "Values are different");
            }
            if (anInfoArray.name.equals("trustStorePassword")) {
                assertTrue(anInfoArray.value.equals("somepassword"), "Values are different");
            }
            if (anInfoArray.name.equals("hostNameInCertificate")) {
                assertTrue(anInfoArray.value.equals("someHost"), "Values are different");
            }
        }
    }

    /**
     * test SSL properties with SQLServerDataSource
     */
    @Test
    public void testDataSource() {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setUser("User");
        ds.setPassword("sUser");
        ds.setApplicationName("User");
        ds.setURL("jdbc:sqlserver://" + randomServer + ";packetSize=512");

        String trustStore = "Store";
        String trustStorePassword = "pwd";

        ds.setTrustStore(trustStore);
        ds.setEncrypt(true);
        ds.setTrustStorePassword(trustStorePassword);
        ds.setTrustServerCertificate(true);
        assertEquals(trustStore, ds.getTrustStore(), "Values are different");
        assertEquals(true, ds.getEncrypt(), "Values are different");
        assertEquals(true, ds.getTrustServerCertificate(), "Values are different");
    }

    @Test
    public void testEncryptedConnection() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setApplicationName("User");
        ds.setURL(connectionString);
        ds.setEncrypt(true);
        ds.setTrustServerCertificate(true);
        ds.setPacketSize(8192);
        try(Connection con = ds.getConnection()) {}
    }

    @Test
    public void testJdbcDriverMethod() throws SQLFeatureNotSupportedException {
        SQLServerDriver serverDriver = new SQLServerDriver();
        Logger logger = serverDriver.getParentLogger();
        assertEquals(logger.getName(), "com.microsoft.sqlserver.jdbc", "Parent Logger name is wrong");
    }

    @Test
    public void testJdbcDataSourceMethod() throws SQLFeatureNotSupportedException {
        SQLServerDataSource fxds = new SQLServerDataSource();
        Logger logger = fxds.getParentLogger();
        assertEquals(logger.getName(), "com.microsoft.sqlserver.jdbc", "Parent Logger name is wrong");
    }

    class MyEventListener implements javax.sql.ConnectionEventListener {
        boolean connClosed = false;
        boolean errorOccurred = false;

        public MyEventListener() {
        }

        public void connectionClosed(ConnectionEvent event) {
            connClosed = true;
        }

        public void connectionErrorOccurred(ConnectionEvent event) {
            errorOccurred = true;
        }
    }

    /**
     * Attach the Event listener and listen for connection events, fatal errors should not close the pooled connection objects
     * 
     * @throws SQLException
     */
    @Test
    public void testConnectionEvents() throws SQLException {
        assumeTrue(!DBConnection.isSqlAzure(DriverManager.getConnection(connectionString)), "Skipping test case on Azure SQL.");

        SQLServerConnectionPoolDataSource mds = new SQLServerConnectionPoolDataSource();
        mds.setURL(connectionString);
        PooledConnection pooledConnection = mds.getPooledConnection();

        // Attach the Event listener and listen for connection events.
        MyEventListener myE = new MyEventListener();
        pooledConnection.addConnectionEventListener(myE);	// ConnectionListener implements ConnectionEventListener
        
        try(Connection con = pooledConnection.getConnection();
        	Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE)) {

	        boolean exceptionThrown = false;
	        try {
	            // raise a severe exception and make sure that the connection is not
	            // closed.
	            stmt.executeUpdate("RAISERROR ('foo', 20,1) WITH LOG");
	        }
	        catch (Exception e) {
	            exceptionThrown = true;
	        }
	        assertTrue(exceptionThrown, "Expected exception is not thrown.");
	
	        // Check to see if error occurred.
	        assertTrue(myE.errorOccurred, "Error occurred is not called.");
        }
        // make sure that connection is closed.
    }

    @Test
    public void testConnectionPoolGetTwice() throws SQLException {
        assumeTrue(!DBConnection.isSqlAzure(DriverManager.getConnection(connectionString)), "Skipping test case on Azure SQL.");

        SQLServerConnectionPoolDataSource mds = new SQLServerConnectionPoolDataSource();
        mds.setURL(connectionString);
        PooledConnection pooledConnection = mds.getPooledConnection();

        // Attach the Event listener and listen for connection events.
        MyEventListener myE = new MyEventListener();
        pooledConnection.addConnectionEventListener(myE);	// ConnectionListener implements ConnectionEventListener

    	Connection con = pooledConnection.getConnection();
    	Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
        // raise a non severe exception and make sure that the connection is not closed.
        stmt.executeUpdate("RAISERROR ('foo', 3,1) WITH LOG");
        // not a serious error there should not be any errors.
        assertTrue(!myE.errorOccurred, "Error occurred is called.");
        // check to make sure that connection is not closed.
	    assertTrue(!con.isClosed(), "Connection is closed.");
	    stmt.close();
	    con.close();
        // check to make sure that connection is closed.
        assertTrue(con.isClosed(), "Connection is not closed.");
    }

    @Test
    public void testConnectionClosed() throws SQLException {
        assumeTrue(!DBConnection.isSqlAzure(DriverManager.getConnection(connectionString)), "Skipping test case on Azure SQL.");

        SQLServerDataSource mds = new SQLServerDataSource();
        mds.setURL(connectionString);
        Connection con = mds.getConnection();
        Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

        boolean exceptionThrown = false;
        try {
            stmt.executeUpdate("RAISERROR ('foo', 20,1) WITH LOG");
        }
        catch (Exception e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown, "Expected exception is not thrown.");
        
        // check to make sure that connection is closed.
        assertTrue(con.isClosed(), "Connection is not closed.");
    }

    @Test
    public void testIsWrapperFor() throws SQLException, ClassNotFoundException {
        try(Connection conn = DriverManager.getConnection(connectionString);
        		SQLServerConnection ssconn = (SQLServerConnection) conn) {
	        boolean isWrapper;
	        isWrapper = ssconn.isWrapperFor(ssconn.getClass());
	        assertTrue(isWrapper, "SQLServerConnection supports unwrapping");
	        assertEquals(ssconn.TRANSACTION_SNAPSHOT, ssconn.TRANSACTION_SNAPSHOT, "Cant access the TRANSACTION_SNAPSHOT ");
	
	        isWrapper = ssconn.isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerConnection"));
	        assertTrue(isWrapper, "ISQLServerConnection supports unwrapping");
	        ISQLServerConnection iSql = (ISQLServerConnection) ssconn.unwrap(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerConnection"));
	        assertEquals(iSql.TRANSACTION_SNAPSHOT, iSql.TRANSACTION_SNAPSHOT, "Cant access the TRANSACTION_SNAPSHOT ");
	
	        ssconn.unwrap(Class.forName("java.sql.Connection"));
        }
    }

    @Test
    public void testNewConnection() throws SQLException {
        try(SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString)) {
        	assertTrue(conn.isValid(0), "Newly created connection should be valid");
        }
    }

    @Test
    public void testClosedConnection() throws SQLException {
        SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
        conn.close();
        assertTrue(!conn.isValid(0), "Closed connection should be invalid");
    }

    @Test
    public void testNegativeTimeout() throws Exception {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString)) {
	        try {
	            conn.isValid(-42);
	            throw new Exception("No exception thrown with negative timeout");
	        }
	        catch (SQLException e) {
	            assertEquals(e.getMessage(), "The query timeout value -42 is not valid.", "Wrong exception message");
	        }
        }
    }

    @Test
    public void testDeadConnection() throws SQLException {
        assumeTrue(!DBConnection.isSqlAzure(DriverManager.getConnection(connectionString)), "Skipping test case on Azure SQL.");

        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString + ";responseBuffering=adaptive")) {
        	
        	Statement stmt = null;
	        String tableName = RandomUtil.getIdentifier("Table");
	        tableName = DBTable.escapeIdentifier(tableName);
	
	        conn.setAutoCommit(false);
	        stmt = conn.createStatement();
	        stmt.executeUpdate("CREATE TABLE " + tableName + " (col1 int primary key)");
	        for (int i = 0; i < 80; i++) {
	            stmt.executeUpdate("INSERT INTO " + tableName + "(col1) values (" + i + ")");
	        }
	        conn.commit();
	        try {
	            stmt.execute("SELECT x1.col1 as foo, x2.col1 as bar, x1.col1 as eeep FROM " + tableName + " as x1, " + tableName
	                    + " as x2; RAISERROR ('Oops', 21, 42) WITH LOG");
	        }
	        catch (SQLException e) {
	            assertEquals(e.getMessage(), "Connection reset", "Unknown Exception");
	        }
	        finally {
	            DriverManager.getConnection(connectionString).createStatement().execute("drop table " + tableName);
	        }
	        assertEquals(conn.isValid(5), false, "Dead connection should be invalid");
        }
    }

    @Test
    public void testClientConnectionId() throws Exception {
        SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
        assertTrue(conn.getClientConnectionId() != null, "ClientConnectionId is null");
        conn.close();
        try {
            // Call getClientConnectionId on a closed connection, should raise exception
            conn.getClientConnectionId();
            throw new Exception("No exception thrown calling getClientConnectionId on a closed connection");
        }
        catch (SQLException e) {
            assertEquals(e.getMessage(), "The connection is closed.", "Wrong exception message");
        }

        conn = null;
        try {
            // Wrong database, ClientConnectionId should be available in error message
            conn = (SQLServerConnection) DriverManager
                    .getConnection(connectionString + ";databaseName=" + RandomUtil.getIdentifierForDB("DataBase") + ";");
            conn.close();

        }
        catch (SQLException e) {
            assertTrue(e.getMessage().indexOf("ClientConnectionId") != -1,
                    "Unexpected: ClientConnectionId is not in exception message due to wrong DB");
        }

        try {
            // Nonexist host, ClientConnectionId should not be available in error message
            conn = (SQLServerConnection) DriverManager
                    .getConnection(connectionString + ";instanceName=" + RandomUtil.getIdentifier("Instance") + ";logintimeout=5;");
            conn.close();

        }
        catch (SQLException e) {
            assertEquals(false, e.getMessage().indexOf("ClientConnectionId") != -1,
                    "Unexpected: ClientConnectionId is in exception message due to wrong host");
        }
    }

    @Test
    public void testIncorrectDatabase() throws SQLException {
        long timerStart = 0;
        long timerEnd = 0;
        Connection con = null;
        final long milsecs = threshHoldForNoRetryInMilliseconds;
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setLoginTimeout(loginTimeOutInSeconds);
            ds.setDatabaseName(RandomUtil.getIdentifier("DataBase"));
            timerStart = System.currentTimeMillis();
            con = ds.getConnection();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Cannot open database"));
            timerEnd = System.currentTimeMillis();
        }

        long timeDiff = timerEnd - timerStart;
        assertTrue(con == null, "Should not have connected.");
        assertTrue(timeDiff <= milsecs, "Exited in more than " + (milsecs / 1000) + " seconds.");
    }

    @Test
    public void testIncorrectUserName() throws SQLException {
        long timerStart = 0;
        long timerEnd = 0;
        Connection con = null;
        final long milsecs = threshHoldForNoRetryInMilliseconds;
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setLoginTimeout(loginTimeOutInSeconds);
            ds.setUser(RandomUtil.getIdentifier("User"));
            timerStart = System.currentTimeMillis();
            con = ds.getConnection();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Login failed"));
            timerEnd = System.currentTimeMillis();
        }

        long timeDiff = timerEnd - timerStart;
        assertTrue(con == null, "Should not have connected.");
        assertTrue(timeDiff <= milsecs, "Exited in more than " + (milsecs / 1000) + " seconds.");
    }

    @Test
    public void testIncorrectPassword() throws SQLException {
        long timerStart = 0;
        long timerEnd = 0;
        Connection con = null;
        final long milsecs = threshHoldForNoRetryInMilliseconds;
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setLoginTimeout(loginTimeOutInSeconds);
            ds.setPassword(RandomUtil.getIdentifier("Password"));
            timerStart = System.currentTimeMillis();
            con = ds.getConnection();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Login failed"));
            timerEnd = System.currentTimeMillis();
        }

        long timeDiff = timerEnd - timerStart;
        assertTrue(con == null, "Should not have connected.");
        assertTrue(timeDiff <= milsecs, "Exited in more than " + (milsecs / 1000) + " seconds.");
    }

    @Test
    public void testInvalidCombination() throws SQLException {
        long timerStart = 0;
        long timerEnd = 0;
        Connection con = null;
        final long milsecs = threshHoldForNoRetryInMilliseconds;
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setLoginTimeout(loginTimeOutInSeconds);
            ds.setMultiSubnetFailover(true);
            ds.setFailoverPartner(RandomUtil.getIdentifier("FailoverPartner"));
            timerStart = System.currentTimeMillis();
            con = ds.getConnection();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Connecting to a mirrored"));
            timerEnd = System.currentTimeMillis();
        }

        long timeDiff = timerEnd - timerStart;
        assertTrue(con == null, "Should not have connected.");
        assertTrue(timeDiff <= milsecs, "Exited in more than " + (milsecs / 1000) + " seconds.");
    }

    @Test
    @Tag("slow")
    public void testIncorrectDatabaseWithFailoverPartner() throws SQLException {
        long timerStart = 0;
        long timerEnd = 0;
        Connection con = null;
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setLoginTimeout(loginTimeOutInSeconds);
            ds.setDatabaseName(RandomUtil.getIdentifierForDB("DB"));
            ds.setFailoverPartner(RandomUtil.getIdentifier("FailoverPartner"));
            timerStart = System.currentTimeMillis();
            con = ds.getConnection();
        }
        catch (Exception e) {
            timerEnd = System.currentTimeMillis();
        }

        long timeDiff = timerEnd - timerStart;
        assertTrue(con == null, "Should not have connected.");
        assertTrue(timeDiff >= ((loginTimeOutInSeconds - 1) * 1000), "Exited in less than " + (loginTimeOutInSeconds - 1) + " seconds.");
    }

    @Test
    public void testAbortBadParam() throws SQLException {
        SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
        try {
            conn.abort(null);
        }
        catch (SQLException e) {
            assertTrue(e.getMessage().contains("The argument executor is not valid"));
        }
    }

    @Test
    public void testAbort() throws SQLException {
        SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
        Executor executor = Executors.newFixedThreadPool(2);
        conn.abort(executor);
    }

    @Test
    public void testSetSchema() throws SQLException {
        SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
        conn.setSchema(RandomUtil.getIdentifier("schema"));
    }

    @Test
    public void testGetSchema() throws SQLException {
        SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
        conn.getSchema();
    }
    
    static Boolean isInterrupted = false;

    /**
     * Test thread's interrupt status is not cleared.
     * 
     * @throws InterruptedException
     */
    @Test
    @Tag("slow")
    public void testThreadInterruptedStatus() throws InterruptedException {
        Runnable runnable = new Runnable() {
            public void run() {
                SQLServerDataSource ds = new SQLServerDataSource();

                ds.setURL(connectionString);
                ds.setServerName("invalidServerName" + UUID.randomUUID());
                ds.setLoginTimeout(5);

                try {
                    ds.getConnection();
                }
                catch (SQLException e) {
                    isInterrupted = Thread.currentThread().isInterrupted();
                }
            }
        };

        ExecutorService executor = Executors.newFixedThreadPool(1);
        Future<?> future = executor.submit(runnable);

        Thread.sleep(1000);

        // interrupt the thread in the Runnable
        future.cancel(true);

        Thread.sleep(8000);

        executor.shutdownNow();

        assertTrue(isInterrupted, "Thread's interrupt status is not set.");
    }
}
