package com.microsoft.sqlserver.jdbc.requestboundary;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;
import com.microsoft.sqlserver.testframework.Utils;
import com.microsoft.sqlserver.testframework.util.RandomUtil;

/**
 * A class for testing Request Boundary Methods.
 */
@RunWith(JUnitPlatform.class)
public class RequestBoundaryMethodsTest extends AbstractTest {

    /**
     * Tests Request Boundary methods with SQLServerConnection properties that are modifiable through public APIs.
     * 
     * @throws SQLException
     */

    @Test
    public void testModifiableConnectionProperties() throws SQLException {
        // List of SQLServerConnection fields that can be modified through public APIs.
        boolean autoCommitMode1 = true;
        int transactionIsolationLevel1 = SQLServerConnection.TRANSACTION_READ_COMMITTED;
        int networkTimeout1 = 5000;
        int holdability1 = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        boolean sendTimeAsDatetime1 = true;
        int statementPoolingCacheSize1 = 0;
        boolean disableStatementPooling1 = true;
        int serverPreparedStatementDiscardThreshold1 = 10;
        boolean enablePrepareOnFirstPreparedStatementCall1 = false;
        String sCatalog1 = "master";

        boolean autoCommitMode2 = false;
        int transactionIsolationLevel2 = SQLServerConnection.TRANSACTION_SERIALIZABLE;
        int networkTimeout2 = 10000;
        int holdability2 = ResultSet.CLOSE_CURSORS_AT_COMMIT;
        boolean sendTimeAsDatetime2 = false;
        int statementPoolingCacheSize2 = 10;
        boolean disableStatementPooling2 = false;
        int serverPreparedStatementDiscardThreshold2 = 100;
        boolean enablePrepareOnFirstPreparedStatementCall2 = true;
        String sCatalog2 = RandomUtil.getIdentifier("RequestBoundaryDatabase");

        try (SQLServerConnection con = connect()) {
            if (Utils.isJDBC43OrGreater(con)) {
                // Second database
                con.createStatement().executeUpdate("CREATE DATABASE [" + sCatalog2 + "]");

                // First set of values.
                setConnectionFields(con, autoCommitMode1, transactionIsolationLevel1, networkTimeout1, holdability1, sendTimeAsDatetime1,
                        statementPoolingCacheSize1, disableStatementPooling1, serverPreparedStatementDiscardThreshold1,
                        enablePrepareOnFirstPreparedStatementCall1, sCatalog1);
                con.beginRequest();
                // Call setters with the second set of values inside beginRequest()/endRequest() block.
                setConnectionFields(con, autoCommitMode2, transactionIsolationLevel2, networkTimeout2, holdability2, sendTimeAsDatetime2,
                        statementPoolingCacheSize2, disableStatementPooling2, serverPreparedStatementDiscardThreshold2,
                        enablePrepareOnFirstPreparedStatementCall2, sCatalog2);
                con.endRequest();
                // Test if endRequest() resets the SQLServerConnection properties back to the first set of values.
                compareValuesAgainstConnection(con, autoCommitMode1, transactionIsolationLevel1, networkTimeout1, holdability1, sendTimeAsDatetime1,
                        statementPoolingCacheSize1, disableStatementPooling1, serverPreparedStatementDiscardThreshold1,
                        enablePrepareOnFirstPreparedStatementCall1, sCatalog1);

                // Multiple calls to beginRequest() without an intervening call to endRequest() are no-op.
                setConnectionFields(con, autoCommitMode2, transactionIsolationLevel2, networkTimeout2, holdability2, sendTimeAsDatetime2,
                        statementPoolingCacheSize2, disableStatementPooling2, serverPreparedStatementDiscardThreshold2,
                        enablePrepareOnFirstPreparedStatementCall2, sCatalog2);
                con.beginRequest();
                setConnectionFields(con, autoCommitMode1, transactionIsolationLevel1, networkTimeout1, holdability1, sendTimeAsDatetime1,
                        statementPoolingCacheSize1, disableStatementPooling1, serverPreparedStatementDiscardThreshold1,
                        enablePrepareOnFirstPreparedStatementCall1, sCatalog1);
                con.beginRequest();
                con.endRequest();
                // Same values as before the first beginRequest()
                compareValuesAgainstConnection(con, autoCommitMode2, transactionIsolationLevel2, networkTimeout2, holdability2, sendTimeAsDatetime2,
                        statementPoolingCacheSize2, disableStatementPooling2, serverPreparedStatementDiscardThreshold2,
                        enablePrepareOnFirstPreparedStatementCall2, sCatalog2);

                // A call to endRequest() without an intervening call to beginRequest() is no-op.
                setConnectionFields(con, autoCommitMode1, transactionIsolationLevel1, networkTimeout1, holdability1, sendTimeAsDatetime1,
                        statementPoolingCacheSize1, disableStatementPooling1, serverPreparedStatementDiscardThreshold1,
                        enablePrepareOnFirstPreparedStatementCall1, sCatalog1);
                setConnectionFields(con, autoCommitMode2, transactionIsolationLevel2, networkTimeout2, holdability2, sendTimeAsDatetime2,
                        statementPoolingCacheSize2, disableStatementPooling2, serverPreparedStatementDiscardThreshold2,
                        enablePrepareOnFirstPreparedStatementCall2, sCatalog2);
                con.endRequest();
                // No change.
                compareValuesAgainstConnection(con, autoCommitMode2, transactionIsolationLevel2, networkTimeout2, holdability2, sendTimeAsDatetime2,
                        statementPoolingCacheSize2, disableStatementPooling2, serverPreparedStatementDiscardThreshold2,
                        enablePrepareOnFirstPreparedStatementCall2, sCatalog2);
            }
        }
        finally {
            SQLServerConnection conDrop = connect();
            Utils.dropDatabaseIfExists(sCatalog2, conDrop.createStatement());
            conDrop.close();
        }
    }

    /**
     * Tests Request Boundary methods with warnings.
     * 
     * @throws SQLException
     */
    @Test
    public void testWarnings() throws SQLException {
        try (SQLServerConnection con = connect()) {
            if (Utils.isJDBC43OrGreater(con)) {
                con.beginRequest();
                generateWarning(con);
                assertNotNull(con.getWarnings());
                con.endRequest();
                assertNull(con.getWarnings());

                generateWarning(con);
                con.endRequest();
                assertNotNull(con.getWarnings());

                con.clearWarnings();
                con.beginRequest();
                generateWarning(con);
                con.beginRequest();
                con.endRequest();
                assertNull(con.getWarnings());
            }
        }
    }

    /**
     * Tests Request Boundary methods when there are open transactions.
     * 
     * @throws SQLException
     */
    @Test
    public void testOpenTransactions() throws SQLException {
        ResultSet rs = null;
        String tableName = null;

        try (SQLServerConnection con = connect(); Statement stmt = con.createStatement();) {
            if (Utils.isJDBC43OrGreater(con)) {
                tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("RequestBoundaryTable"));
                Utils.dropTableIfExists(tableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + tableName + " (col int)");
                con.beginRequest();
                con.setAutoCommit(false);
                stmt.executeUpdate("INSERT INTO " + tableName + " values(5)");
                // endRequest() does a rollback here, the value does not get inserted into the table.
                con.endRequest();
                con.commit();

                rs = con.createStatement().executeQuery("SELECT * from " + tableName);
                assertTrue(!rs.isBeforeFirst(), "Should not have returned a result set.");
            }
        }
        finally {
            SQLServerConnection conDrop = connect();
            Utils.dropTableIfExists(tableName, conDrop.createStatement());
            conDrop.close();
        }
    }

    /**
     * Tests Request Boundary methods with statements.
     * 
     * @throws SQLException
     */
    @SuppressWarnings("resource")
    @Test
    public void testStatements() throws SQLException {
        Statement stmt = null;
        ResultSet rs = null;
        Statement stmt1 = null;
        PreparedStatement ps = null;
        CallableStatement cs = null;
        ResultSet rs1 = null;
        String tableName = null;

        try (SQLServerConnection con = connect();) {
            if (Utils.isJDBC43OrGreater(con)) {
                stmt1 = con.createStatement();
                con.beginRequest();
                stmt = con.createStatement();
                rs = stmt.executeQuery("SELECT 1");
                rs.next();
                assertEquals(1, rs.getInt(1));
                con.endRequest();

                assertTrue(!stmt1.isClosed(), "Statement created outside of beginRequest()/endRequest() block should not be closed.");
                assertTrue(stmt.isClosed(), "Statment created inside beginRequest()/endRequest() block should be closed after endRequest().");
                assertTrue(rs.isClosed(), "ResultSet should be closed after endRequest().");
                stmt1.close();

                // Multiple statements inside beginRequest()/endRequest() block
                con.beginRequest();
                stmt = con.createStatement();
                tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("RequestBoundary"));
                Utils.dropTableIfExists(tableName, stmt);
                stmt.executeUpdate("CREATE TABLE " + tableName + " (col int)");
                ps = con.prepareStatement("INSERT INTO " + tableName + " values (?)");
                ps.setInt(1, 2);
                ps.executeUpdate();

                stmt1 = con.createStatement();
                rs1 = stmt1.executeQuery("SELECT * FROM " + tableName);
                rs1.next();
                assertEquals(2, rs1.getInt(1));
                Utils.dropTableIfExists(tableName, stmt);

                cs = con.prepareCall("{call sp_server_info}");
                cs.execute();
                con.endRequest();

                assertTrue(stmt.isClosed());
                assertTrue(ps.isClosed());
                assertTrue(stmt1.isClosed());
                assertTrue(cs.isClosed());
                assertTrue(rs1.isClosed());
            }
        }
        finally {
            if (null != stmt) {
                stmt.close();
            }
            if (null != stmt1) {
                stmt1.close();
            }
            if (null != ps) {
                ps.close();
            }
            if (null != cs) {
                cs.close();
            }
        }
    }

    /**
     * Tests Request Boundary methods in a multi-threaded environment.
     * 
     * @throws SQLException
     */
    @Test
    public void testThreads() throws SQLException {
        class Variables {
            volatile SQLServerConnection con = null;
            volatile Statement stmt = null;
            volatile PreparedStatement pstmt = null;
        }

        final Variables sharedVariables = new Variables();
        final CountDownLatch latch = new CountDownLatch(3);
        try {
            sharedVariables.con = connect();
            if (Utils.isJDBC43OrGreater(sharedVariables.con)) {
                Thread thread1 = new Thread() {
                    public void run() {
                        try {
                            sharedVariables.con.setNetworkTimeout(null, 100);
                            sharedVariables.con.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
                            latch.countDown();
                        }
                        catch (SQLException e) {
                            e.printStackTrace();
                            Thread.currentThread().interrupt();
                        }
                    }
                };

                Thread thread2 = new Thread() {
                    public void run() {
                        try {
                            sharedVariables.stmt = sharedVariables.con.createStatement();
                            ResultSet rs = sharedVariables.stmt.executeQuery("SELECT 1");
                            rs.next();
                            assertEquals(1, rs.getInt(1));
                            latch.countDown();
                        }
                        catch (SQLException e) {
                            e.printStackTrace();
                            Thread.currentThread().interrupt();
                        }
                    }
                };

                Thread thread3 = new Thread() {
                    public void run() {
                        try {
                            sharedVariables.pstmt = sharedVariables.con.prepareStatement("SELECT 1");
                            ResultSet rs = sharedVariables.pstmt.executeQuery();
                            rs.next();
                            assertEquals(1, rs.getInt(1));
                            latch.countDown();
                        }
                        catch (SQLException e) {
                            e.printStackTrace();
                            Thread.currentThread().interrupt();
                        }

                    }
                };

                int originalNetworkTimeout = sharedVariables.con.getNetworkTimeout();
                int originalHoldability = sharedVariables.con.getHoldability();
                sharedVariables.con.beginRequest();
                thread1.start();
                thread2.start();
                thread3.start();
                latch.await();
                sharedVariables.con.endRequest();

                assertEquals(originalNetworkTimeout, sharedVariables.con.getNetworkTimeout());
                assertEquals(originalHoldability, sharedVariables.con.getHoldability());
                assertTrue(sharedVariables.stmt.isClosed());
                assertTrue(sharedVariables.pstmt.isClosed());
            }
        }
        catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        }
        finally {
            if (null != sharedVariables.stmt) {
                sharedVariables.stmt.close();
            }
            if (null != sharedVariables.pstmt) {
                sharedVariables.pstmt.close();
            }
            if (null != sharedVariables.con) {
                sharedVariables.con.close();
            }
        }
    }

    private SQLServerConnection connect() throws SQLException {
        SQLServerConnection connection = null;
        try {
            connection = PrepUtil.getConnection(getConfiguredProperty("mssql_jdbc_test_connection_properties"));
        }
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return connection;
    }

    private void setConnectionFields(SQLServerConnection con,
            boolean autoCommitMode,
            int transactionIsolationLevel,
            int networkTimeout,
            int holdability,
            boolean sendTimeAsDatetime,
            int statementPoolingCacheSize,
            boolean disableStatementPooling,
            int serverPreparedStatementDiscardThreshold,
            boolean enablePrepareOnFirstPreparedStatementCall,
            String sCatalog) throws SQLException {
        con.setAutoCommit(autoCommitMode);
        con.setTransactionIsolation(transactionIsolationLevel);
        con.setNetworkTimeout(null, networkTimeout);
        con.setHoldability(holdability);
        con.setSendTimeAsDatetime(sendTimeAsDatetime);
        con.setStatementPoolingCacheSize(statementPoolingCacheSize);
        con.setDisableStatementPooling(disableStatementPooling);
        con.setServerPreparedStatementDiscardThreshold(serverPreparedStatementDiscardThreshold);
        con.setEnablePrepareOnFirstPreparedStatementCall(enablePrepareOnFirstPreparedStatementCall);
        con.setCatalog(sCatalog);
    }

    private void compareValuesAgainstConnection(SQLServerConnection con,
            boolean autoCommitMode,
            int transactionIsolationLevel,
            int networkTimeout,
            int holdability,
            boolean sendTimeAsDatetime,
            int statementPoolingCacheSize,
            boolean disableStatementPooling,
            int serverPreparedStatementDiscardThreshold,
            boolean enablePrepareOnFirstPreparedStatementCall,
            String sCatalog) throws SQLException {
        final String description = " values do not match.";
        assertEquals(autoCommitMode, con.getAutoCommit(), "autoCommitmode" + description);
        assertEquals(transactionIsolationLevel, con.getTransactionIsolation(), "transactionIsolationLevel" + description);
        assertEquals(networkTimeout, con.getNetworkTimeout(), "networkTimeout" + description);
        assertEquals(holdability, con.getHoldability(), "holdability" + description);
        assertEquals(sendTimeAsDatetime, con.getSendTimeAsDatetime(), "sendTimeAsDatetime" + description);
        assertEquals(statementPoolingCacheSize, con.getStatementPoolingCacheSize(), "statementPoolingCacheSize" + description);
        assertEquals(disableStatementPooling, con.getDisableStatementPooling(), "disableStatementPooling" + description);
        assertEquals(serverPreparedStatementDiscardThreshold, con.getServerPreparedStatementDiscardThreshold(),
                "serverPreparedStatementDiscardThreshold" + description);
        assertEquals(enablePrepareOnFirstPreparedStatementCall, con.getEnablePrepareOnFirstPreparedStatementCall(),
                "enablePrepareOnFirstPreparedStatementCall" + description);
        assertEquals(sCatalog, con.getCatalog(), "sCatalog" + description);
    }

    private void generateWarning(SQLServerConnection con) throws SQLException {
        con.setClientInfo("name", "value");
    }
}
