/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * A class for testing Request Boundary Methods.
 */
@RunWith(JUnitPlatform.class)
public class RequestBoundaryMethodsTest extends AbstractTest {

    static String tableName = RandomUtil.getIdentifier("RequestBoundaryTable");

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
        boolean useBulkCopyForBatchInsert1 = true;

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
        boolean useBulkCopyForBatchInsert2 = false;

        try (SQLServerConnection con = connect(); Statement stmt = con.createStatement()) {
            if (TestUtils.isJDBC43OrGreater(con)) {
                // Second database
                stmt.executeUpdate("CREATE DATABASE [" + sCatalog2 + "]");

                // First set of values.
                setConnectionFields(con, autoCommitMode1, transactionIsolationLevel1, networkTimeout1, holdability1,
                        sendTimeAsDatetime1, statementPoolingCacheSize1, disableStatementPooling1,
                        serverPreparedStatementDiscardThreshold1, enablePrepareOnFirstPreparedStatementCall1, sCatalog1,
                        useBulkCopyForBatchInsert1);
                con.beginRequest();
                // Call setters with the second set of values inside beginRequest()/endRequest() block.
                setConnectionFields(con, autoCommitMode2, transactionIsolationLevel2, networkTimeout2, holdability2,
                        sendTimeAsDatetime2, statementPoolingCacheSize2, disableStatementPooling2,
                        serverPreparedStatementDiscardThreshold2, enablePrepareOnFirstPreparedStatementCall2, sCatalog2,
                        useBulkCopyForBatchInsert2);
                con.endRequest();
                // Test if endRequest() resets the SQLServerConnection properties back to the first set of values.
                compareValuesAgainstConnection(con, autoCommitMode1, transactionIsolationLevel1, networkTimeout1,
                        holdability1, sendTimeAsDatetime1, statementPoolingCacheSize1, disableStatementPooling1,
                        serverPreparedStatementDiscardThreshold1, enablePrepareOnFirstPreparedStatementCall1, sCatalog1,
                        useBulkCopyForBatchInsert1);

                // Multiple calls to beginRequest() without an intervening call to endRequest() are no-op.
                setConnectionFields(con, autoCommitMode2, transactionIsolationLevel2, networkTimeout2, holdability2,
                        sendTimeAsDatetime2, statementPoolingCacheSize2, disableStatementPooling2,
                        serverPreparedStatementDiscardThreshold2, enablePrepareOnFirstPreparedStatementCall2, sCatalog2,
                        useBulkCopyForBatchInsert2);
                con.beginRequest();
                setConnectionFields(con, autoCommitMode1, transactionIsolationLevel1, networkTimeout1, holdability1,
                        sendTimeAsDatetime1, statementPoolingCacheSize1, disableStatementPooling1,
                        serverPreparedStatementDiscardThreshold1, enablePrepareOnFirstPreparedStatementCall1, sCatalog1,
                        useBulkCopyForBatchInsert1);
                con.beginRequest();
                con.endRequest();
                // Same values as before the first beginRequest()
                compareValuesAgainstConnection(con, autoCommitMode2, transactionIsolationLevel2, networkTimeout2,
                        holdability2, sendTimeAsDatetime2, statementPoolingCacheSize2, disableStatementPooling2,
                        serverPreparedStatementDiscardThreshold2, enablePrepareOnFirstPreparedStatementCall2, sCatalog2,
                        useBulkCopyForBatchInsert2);

                // A call to endRequest() without an intervening call to beginRequest() is no-op.
                setConnectionFields(con, autoCommitMode1, transactionIsolationLevel1, networkTimeout1, holdability1,
                        sendTimeAsDatetime1, statementPoolingCacheSize1, disableStatementPooling1,
                        serverPreparedStatementDiscardThreshold1, enablePrepareOnFirstPreparedStatementCall1, sCatalog1,
                        useBulkCopyForBatchInsert1);
                setConnectionFields(con, autoCommitMode2, transactionIsolationLevel2, networkTimeout2, holdability2,
                        sendTimeAsDatetime2, statementPoolingCacheSize2, disableStatementPooling2,
                        serverPreparedStatementDiscardThreshold2, enablePrepareOnFirstPreparedStatementCall2, sCatalog2,
                        useBulkCopyForBatchInsert2);
                con.endRequest();
                // No change.
                compareValuesAgainstConnection(con, autoCommitMode2, transactionIsolationLevel2, networkTimeout2,
                        holdability2, sendTimeAsDatetime2, statementPoolingCacheSize2, disableStatementPooling2,
                        serverPreparedStatementDiscardThreshold2, enablePrepareOnFirstPreparedStatementCall2, sCatalog2,
                        useBulkCopyForBatchInsert2);
                // drop the database
                con.setCatalog("master");
            }
        } finally {
            try (SQLServerConnection con = connect(); Statement stmt = con.createStatement()) {
                TestUtils.dropDatabaseIfExists(sCatalog2, stmt);
            }
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
            if (TestUtils.isJDBC43OrGreater(con)) {
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
        try (SQLServerConnection con = connect(); Statement stmt = con.createStatement()) {
            if (TestUtils.isJDBC43OrGreater(con)) {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col int)");
                con.beginRequest();
                con.setAutoCommit(false);
                stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(5)");
                // endRequest() does a rollback here, the value does not get inserted into the table.
                con.endRequest();
                con.commit();

                try (ResultSet rs = con.createStatement()
                        .executeQuery("SELECT * from " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                    assertTrue(!rs.isBeforeFirst(), "Should not have returned a result set.");
                } finally {
                    if (null != tableName) {
                        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
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
        try (SQLServerConnection con = connect();) {
            if (TestUtils.isJDBC43OrGreater(con)) {
                try (Statement stmt1 = con.createStatement()) {
                    con.beginRequest();
                    try (Statement stmt = con.createStatement()) {
                        try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
                            rs.next();
                            assertEquals(1, rs.getInt(1));
                            con.endRequest();

                            assertTrue(!stmt1.isClosed(),
                                    "Statement created outside of beginRequest()/endRequest() block should not be closed.");
                            assertTrue(stmt.isClosed(),
                                    "Statement created inside beginRequest()/endRequest() block should be closed after endRequest().");
                            assertTrue(rs.isClosed(), "ResultSet should be closed after endRequest().");
                        }
                    }
                }

                // Multiple statements inside beginRequest()/endRequest() block
                con.beginRequest();
                try (Statement stmt = con.createStatement()) {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                    stmt.executeUpdate(
                            "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col int)");
                    try (PreparedStatement ps = con.prepareStatement(
                            "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values (?)")) {
                        ps.setInt(1, 2);
                        ps.executeUpdate();

                        try (Statement stmt1 = con.createStatement(); ResultSet rs = stmt1
                                .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                            rs.next();
                            assertEquals(2, rs.getInt(1));
                            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);

                            try (CallableStatement cs = con.prepareCall("{call sp_server_info}")) {
                                cs.execute();
                                con.endRequest();

                                assertTrue(stmt.isClosed());
                                assertTrue(ps.isClosed());
                                assertTrue(stmt1.isClosed());
                                assertTrue(cs.isClosed());
                                assertTrue(rs.isClosed());
                            }
                        }
                    }
                } finally {
                    if (null != tableName) {
                        try (Statement stmt = con.createStatement()) {
                            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                        }
                    }
                }

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
            if (TestUtils.isJDBC43OrGreater(sharedVariables.con)) {
                Thread thread1 = new Thread() {
                    public void run() {
                        try {
                            sharedVariables.con.setNetworkTimeout(null, 100);
                            sharedVariables.con.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
                            latch.countDown();
                        } catch (SQLException e) {
                            e.printStackTrace();
                            Thread.currentThread().interrupt();
                        }
                    }
                };

                Thread thread2 = new Thread() {
                    public void run() {
                        try {
                            sharedVariables.stmt = sharedVariables.con.createStatement();
                            try (ResultSet rs = sharedVariables.stmt.executeQuery("SELECT 1")) {
                                rs.next();
                                assertEquals(1, rs.getInt(1));
                                latch.countDown();
                            }
                        } catch (SQLException e) {
                            e.printStackTrace();
                            Thread.currentThread().interrupt();
                        }
                    }
                };

                Thread thread3 = new Thread() {
                    public void run() {
                        try {
                            sharedVariables.pstmt = sharedVariables.con.prepareStatement("SELECT 1");
                            try (ResultSet rs = sharedVariables.pstmt.executeQuery()) {
                                rs.next();
                                assertEquals(1, rs.getInt(1));
                                latch.countDown();
                            }
                        } catch (SQLException e) {
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
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        } finally {
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

    /**
     * This is not really a test. The goal is to make the build fail if there are new public non-static methods in
     * SQLServerConnection and notify the developer to decide whether it needs to be handled by
     * beginRequest()/endRequest().
     *
     * To fix the failure, you first need to check if the new method can modify connection local state after connection
     * has been created. (See beginRequestInternal()/endRequestInternal() in SQLServerConnection). If yes, make sure it
     * is handled by beginRequest()/endRequest() and then add it to <code>verifiedMethodNames</code>. If not, just
     * adding the new method's name to the same list of verified methods is enough.
     */
    @Test
    public void testNewMethods() {
        Method[] methods = SQLServerConnection.class.getDeclaredMethods();
        for (Method method : methods) {
            assertTrue(isVerified(method),
                    "A failure is expected if you are adding a new public non-static method to SQLServerConnection."
                            + " See the test for instructions on how to fix the failure. ");
        }
    }

    private SQLServerConnection connect() throws SQLException {
        SQLServerConnection connection = null;
        try {
            connection = PrepUtil.getConnection(connectionString);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
        return connection;
    }

    private void setConnectionFields(SQLServerConnection con, boolean autoCommitMode, int transactionIsolationLevel,
            int networkTimeout, int holdability, boolean sendTimeAsDatetime, int statementPoolingCacheSize,
            boolean disableStatementPooling, int serverPreparedStatementDiscardThreshold,
            boolean enablePrepareOnFirstPreparedStatementCall, String sCatalog,
            boolean useBulkCopyForBatchInsert) throws SQLException {
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
        con.setUseBulkCopyForBatchInsert(useBulkCopyForBatchInsert);
    }

    private void compareValuesAgainstConnection(SQLServerConnection con, boolean autoCommitMode,
            int transactionIsolationLevel, int networkTimeout, int holdability, boolean sendTimeAsDatetime,
            int statementPoolingCacheSize, boolean disableStatementPooling, int serverPreparedStatementDiscardThreshold,
            boolean enablePrepareOnFirstPreparedStatementCall, String sCatalog,
            boolean useBulkCopyForBatchInsert) throws SQLException {
        final String description = " values do not match.";
        assertEquals(autoCommitMode, con.getAutoCommit(), "autoCommitmode" + description);
        assertEquals(transactionIsolationLevel, con.getTransactionIsolation(),
                "transactionIsolationLevel" + description);
        assertEquals(networkTimeout, con.getNetworkTimeout(), "networkTimeout" + description);
        assertEquals(holdability, con.getHoldability(), "holdability" + description);
        assertEquals(sendTimeAsDatetime, con.getSendTimeAsDatetime(), "sendTimeAsDatetime" + description);
        assertEquals(statementPoolingCacheSize, con.getStatementPoolingCacheSize(),
                "statementPoolingCacheSize" + description);
        assertEquals(disableStatementPooling, con.getDisableStatementPooling(),
                "disableStatementPooling" + description);
        assertEquals(serverPreparedStatementDiscardThreshold, con.getServerPreparedStatementDiscardThreshold(),
                "serverPreparedStatementDiscardThreshold" + description);
        assertEquals(enablePrepareOnFirstPreparedStatementCall, con.getEnablePrepareOnFirstPreparedStatementCall(),
                "enablePrepareOnFirstPreparedStatementCall" + description);
        assertEquals(sCatalog, con.getCatalog(), "sCatalog" + description);
        assertEquals(useBulkCopyForBatchInsert, con.getUseBulkCopyForBatchInsert(),
                "useBulkCopyForBatchInsert" + description);
    }

    private void generateWarning(SQLServerConnection con) throws SQLException {
        con.setClientInfo("name", "value");
    }

    private boolean isVerified(Method method) {
        return (!Modifier.isPublic(method.getModifiers()) || Modifier.isStatic(method.getModifiers())
                || method.getName().startsWith("get") || getVerifiedMethodNames().contains(method.getName()));
    }

    private List<String> getVerifiedMethodNames() {
        List<String> verifiedMethodNames = new ArrayList<String>();

        verifiedMethodNames.add("toString");
        verifiedMethodNames.add("setReadOnly");
        verifiedMethodNames.add("close");
        verifiedMethodNames.add("unwrap");
        verifiedMethodNames.add("isReadOnly");
        verifiedMethodNames.add("abort");
        verifiedMethodNames.add("isValid");
        verifiedMethodNames.add("setServerPreparedStatementDiscardThreshold");
        verifiedMethodNames.add("setEnablePrepareOnFirstPreparedStatementCall");
        verifiedMethodNames.add("isClosed");
        verifiedMethodNames.add("setSendTimeAsDatetime");
        verifiedMethodNames.add("setStatementPoolingCacheSize");
        verifiedMethodNames.add("setDisableStatementPooling");
        verifiedMethodNames.add("setTransactionIsolation");
        verifiedMethodNames.add("setUseBulkCopyForBatchInsert");
        verifiedMethodNames.add("commit");
        verifiedMethodNames.add("clearWarnings");
        verifiedMethodNames.add("prepareStatement");
        verifiedMethodNames.add("prepareCall");
        verifiedMethodNames.add("setCatalog");
        verifiedMethodNames.add("setAutoCommit");
        verifiedMethodNames.add("createStatement");
        verifiedMethodNames.add("setClientInfo");
        verifiedMethodNames.add("setNetworkTimeout");
        verifiedMethodNames.add("setHoldability");
        verifiedMethodNames.add("closeUnreferencedPreparedStatementHandles");
        verifiedMethodNames.add("isStatementPoolingEnabled");
        verifiedMethodNames.add("rollback");
        verifiedMethodNames.add("releaseSavepoint");
        verifiedMethodNames.add("createStruct");
        verifiedMethodNames.add("createSQLXML");
        verifiedMethodNames.add("setSchema");
        verifiedMethodNames.add("createNClob");
        verifiedMethodNames.add("nativeSQL");
        verifiedMethodNames.add("setSavepoint");
        verifiedMethodNames.add("createClob");
        verifiedMethodNames.add("createBlob");
        verifiedMethodNames.add("isWrapperFor");
        verifiedMethodNames.add("setTypeMap");
        verifiedMethodNames.add("createArrayOf");

        return verifiedMethodNames;
    }
}
