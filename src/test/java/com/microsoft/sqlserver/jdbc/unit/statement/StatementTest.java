/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.StringReader;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerResultSetMetaData;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;


/**
 * 
 * Statement class for testing statement APIs and triggers
 *
 */
@RunWith(JUnitPlatform.class)
public class StatementTest extends AbstractTest {
    public static final Logger log = Logger.getLogger("StatementTest");

    @Nested
    public class TCAttentionHandling {
        private static final int NUM_TABLE_ROWS = 1000;
        private static final int MIN_TABLE_ROWS = 100;
        private static final String TEST_STRING = "Hello." + "  This is a test string."
                + "  It is particularly long so that we will get a multipacket TDS response back from the server."
                + "  This is a test string." + "  This is a test string." + "  This is a test string."
                + "  This is a test string." + "  This is a test string." + "  This is a test string.";
        String tableName = RandomUtil.getIdentifier("TCAttentionHandling");

        @BeforeEach
        public void init() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString)) {
                con.setAutoCommit(false);
                try (Statement stmt = con.createStatement()) {
                    try {
                        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                    } catch (SQLException e) {}
                    stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                            + " (col1 INT, col2 VARCHAR(" + TEST_STRING.length() + "))");
                    for (int i = 0; i < NUM_TABLE_ROWS; i++)
                        stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                + " (col1, col2) VALUES (" + i + ", '" + TEST_STRING + "')");
                }
                con.commit();
            }
        }

        @AfterEach
        public void terminate() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement()) {
                try {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                } catch (SQLException e) {}
            }
        }

        /**
         * Test canceling a Statement before executing it.
         * 
         * Expected: no attention is sent to the server; statement executes normally
         */
        @Test
        public void testCancelBeforeExecute() throws Exception {

            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement()) {
                stmt.cancel();
                try (ResultSet rs = stmt
                        .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                    int numSelectedRows = 0;
                    while (rs.next())
                        ++numSelectedRows;

                    // Wrong number of rows returned
                    assertEquals(NUM_TABLE_ROWS, numSelectedRows, TestResource.getResource("R_valueNotMatch"));
                }
            }
        }

        /**
         * Test attention sent as a result of an unrecoverable error in the request.
         *
         * Expected: Attention sent and handled gracefully. Subsequent use of connection succeeds.
         *
         * Details: Do a batch update that is longer than one TDS packet (so that we are guaranteed to send at least one
         * packet to the server) where the last item in the batch contains an unrecoverable error (misstating the length
         * of a stream value).
         */
        @Test
        public void testErrorInRequest() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    PreparedStatement ps = con.prepareStatement("UPDATE "
                            + AbstractSQLGenerator.escapeIdentifier(tableName) + " SET col2 = ? WHERE col1 = ?")) {
                ps.setString(1, TEST_STRING);
                for (int i = 0; i < MIN_TABLE_ROWS; i++) {
                    ps.setInt(2, i);
                    ps.addBatch();
                }
                ps.setCharacterStream(1, new StringReader(TEST_STRING), TEST_STRING.length() - 1);
                ps.addBatch();

                try {
                    ps.executeBatch();
                } catch (SQLException e) {
                    assertEquals("The stream value is not the specified length. The specified length was "
                            + (TEST_STRING.length() - 1) + ", the actual length is " + TEST_STRING.length() + ".",
                            e.getMessage(), TestResource.getResource("R_unexpectedException"));
                }

                // Successfully closing the PreparedStatement is verification enough that the connection is
                // still usable and that there isn't a left over attention ack on the wire.
            }
        }

        /**
         * Test attention sent to interrupt query once it has started execution.
         *
         * Expected: Timeout executing request. Verify timeout. Verify subsequent use of connection succeeds.
         *
         * Details: The methodology here is similar to our WAITFOR query timeout tests and Defect 58095.
         */
        @Test
        public void testQueryTimeout() throws Exception {
            long elapsedMillis;

            try (Connection con = DriverManager.getConnection(connectionString);
                    PreparedStatement ps = con.prepareStatement("WAITFOR DELAY '00:00:07'")) {

                // First execution:
                // Verify timeout actually cancels statement execution.
                elapsedMillis = -System.currentTimeMillis();
                ps.setQueryTimeout(2);
                try {
                    ps.execute();

                    assertEquals(false, true, TestResource.getResource("R_executionNotTimeout"));
                } catch (SQLException e) {
                    assertTrue(TestResource.getResource("R_queryTimedOut").equalsIgnoreCase(e.getMessage()),
                            TestResource.getResource("R_unexpectedException"));
                    assertTrue("The query has timed out.".equalsIgnoreCase(e.getMessage()),
                            TestResource.getResource("R_unexpectedException"));
                }
                elapsedMillis += System.currentTimeMillis();
                if (elapsedMillis >= 3000) {
                    assertEquals(2000, (int) elapsedMillis, TestResource.getResource("R_executionTooLong"));
                }

                // Second execution:
                // Verify connection is still usable.
                // Verify execution with no timeout doesn't return too soon.
                ps.setQueryTimeout(0);
                elapsedMillis = -System.currentTimeMillis();
                ps.execute();
                elapsedMillis += System.currentTimeMillis();

                // Oddly enough, the server's idea of 7 seconds is actually slightly less than
                // 7000 milliseconds by our clock (!) so we have to allow some slack here.
                if (elapsedMillis < 6500) {
                    assertEquals(6500, (int) elapsedMillis, TestResource.getResource("R_executionNotLong"));
                }
            }
        }

        /**
         * Test that cancelling a Statement while consuming a large response ends the response.
         *
         * Expected: Response terminates before complete. Verify subsequent use of connection succeeds.
         *
         * Details: Test does a large SELECT and expects to get back fewer rows than it asked for after cancelling the
         * ResultSet's associated Statement.
         */
        @Test
        public void testCancelLongResponse() throws Exception {
            assumeTrue("JDBC42".equals(TestUtils.getConfiguredProperty("JDBC_Version")),
                    TestResource.getResource("R_incompatJDBC"));

            try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con
                    .createStatement(SQLServerResultSet.TYPE_SS_DIRECT_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                ((SQLServerStatement) stmt).setResponseBuffering("adaptive");

                // enable isCloseOnCompletion
                try {
                    stmt.closeOnCompletion();
                } catch (Exception e) {

                    throw new SQLException(TestResource.getResource("R_unexpectedException") + ": ", e);

                }

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT " + "a.col1, a.col2 FROM " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                + " a CROSS JOIN " + AbstractSQLGenerator.escapeIdentifier(tableName) + " b")) {

                    // Scan the first MIN_TABLE_ROWS rows
                    int numSelectedRows = 0;
                    while (rs.next() && ++numSelectedRows < MIN_TABLE_ROWS);

                    // Verify that MIN_TABLE_ROWS rows were returned
                    // Wrong number of rows returned in first scan
                    assertEquals(MIN_TABLE_ROWS, numSelectedRows, TestResource.getResource("R_valueNotMatch"));

                    // Cancel the statement and verify that the ResultSet
                    // does NOT return all the remaining rows.
                    stmt.cancel();

                    try {
                        while (rs.next())
                            ++numSelectedRows;

                        assertEquals(false, true, TestResource.getResource("R_expectedExceptionNotThrown"));
                    } catch (SQLException e) {
                        assertEquals(TestResource.getResource("R_queryCancelled"),
                                TestResource.getResource("R_unexpectedException"));
                    }

                    assertEquals(false, NUM_TABLE_ROWS * NUM_TABLE_ROWS == numSelectedRows,
                            "All rows returned after cancel");

                    rs.close();
                    assertEquals(stmt.isClosed(), true, TestResource.getResource("R_statementShouldBeClosed"));
                }
            }
        }

        /**
         * Test cancelling a response that is blocked reading from the server.
         *
         * Expected: Response can be cancelled. Connection is still usable.
         *
         * Details: One connection locks part of the table while another connection tries to SELECT everything. The
         * SELECT connection blocks while reading the rows from the ResultSet. Cancelling the blocking statement (from
         * another thread) should allow it to finish execution normally, up to the row where it was canceled. No
         * cancellation exception is thrown.
         */
        class OneShotCancel implements Runnable {
            private Statement stmt;
            int timeout;

            OneShotCancel(Statement stmt, int timeout) {
                this.stmt = stmt;
                this.timeout = timeout;
            }

            public void run() {
                try {
                    Thread.sleep(1000 * timeout);
                } catch (InterruptedException e) {
                    log.fine("OneShotCancel sleep interrupted: " + e.getMessage());
                    return;
                }
                try {
                    stmt.cancel();
                } catch (SQLException e) {
                    log.fine("Statement.cancel threw exception: " + e.getMessage());
                }
                return;
            }
        }

        @Test
        public void testCancelBlockedResponse() throws Exception {

            Thread oneShotCancel = null;

            try (Connection conLock = DriverManager.getConnection(connectionString)) {
                // Start a transaction on a second connection that locks the last part of the table
                // and leave it non-responsive for now...
                conLock.setAutoCommit(false);
                try (Statement stmtLock = conLock.createStatement()) {
                    stmtLock.executeUpdate("UPDATE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                            + " SET col2 = 'New Value!' WHERE col1 = " + (NUM_TABLE_ROWS - MIN_TABLE_ROWS));

                    try (Connection con = DriverManager.getConnection(connectionString)) {
                        // In SQL Azure, both ALLOW_SNAPSHOT_ISOLATION and READ_COMMITTED_SNAPSHOT options
                        // are always ON and can NOT be turned OFF. Thus the default transaction isolation level
                        // READ_COMMITTED
                        // always uses snapshot row-versioning in SQL Azure, and the reader transaction will not be
                        // blocked
                        // if
                        // it's executing at the default isolation level.
                        // To allow the blocking behavior for the reader transaction (as required by the test logic),
                        // we have to set its isolation level to REPEATABLE_READ (or SERIALIZABLE) in SQL Azure.
                        //
                        // Reference: http://msdn.microsoft.com/en-us/library/ee336245.aspx#isolevels
                        if (isSqlAzure()) {
                            con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                        }

                        // Try to SELECT the entire table. This should return some rows and then block
                        // on the locked part of the table until the one shot cancel thread cancels
                        // statement execution.
                        //
                        // Need to use adaptive response buffering when executing the statement.
                        // Otherwise, we would block in executeQuery()...
                        try (Statement stmt = con.createStatement(SQLServerResultSet.TYPE_SS_DIRECT_FORWARD_ONLY,
                                ResultSet.CONCUR_READ_ONLY)) {
                            ((SQLServerStatement) stmt).setResponseBuffering("adaptive");
                            try (ResultSet rs = stmt.executeQuery(
                                    "SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {

                                // Time how long it takes for execution to be cancelled...
                                long elapsedMillis = -System.currentTimeMillis();

                                // Start up a thread to cancel the SELECT after 3 seconds.
                                oneShotCancel = new Thread(new OneShotCancel(stmt, 3));
                                oneShotCancel.start();

                                // Start retrieving rows
                                int numSelectedRows = 0;

                                try {
                                    while (rs.next())
                                        ++numSelectedRows;
                                    log.fine("numSelectedRows: " + numSelectedRows);

                                    assertEquals(false, true, TestResource.getResource("R_expectedExceptionNotThrown"));
                                } catch (SQLException e) {
                                    assertTrue(TestResource.getResource("R_queryCancelled").equalsIgnoreCase(
                                            e.getMessage()), TestResource.getResource("R_unexpectedException"));
                                }

                                elapsedMillis += System.currentTimeMillis();

                                // We should be able to retrieve no more than the number of rows before the blocked row.
                                // Note that we may actually get fewer rows than the number of rows before the blocked
                                // row
                                // if SQL Server is a little slow in returning rows to us.
                                if (numSelectedRows >= NUM_TABLE_ROWS - MIN_TABLE_ROWS) {
                                    assertEquals(NUM_TABLE_ROWS - MIN_TABLE_ROWS, numSelectedRows,
                                            TestResource.getResource("R_valueNotMatch"));
                                }

                                // If we were able to iterate through all of the expected
                                // rows without blocking, then something went wrong with our
                                // efforts to block execution.
                                if (elapsedMillis < 2500) {
                                    assertEquals(2500, (int) elapsedMillis,
                                            TestResource.getResource("R_executionNotLong"));
                                }
                            }

                            // Verify the statement & connection are still usable after cancelling
                            try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
                                while (rs.next());
                            }
                        }
                    }
                }
            }
        }

        @Test
        public void testCancelBlockedResponsePS() throws Exception {

            Thread oneShotCancel = null;

            try (Connection conLock = DriverManager.getConnection(connectionString)) {
                // Start a transaction on a second connection that locks the last part of the table
                // and leave it non-responsive for now...
                conLock.setAutoCommit(false);
                try (Statement stmtLock = conLock.createStatement()) {
                    stmtLock.executeUpdate("UPDATE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                            + " SET col2 = 'New Value!' WHERE col1 = " + (NUM_TABLE_ROWS - MIN_TABLE_ROWS));

                    try (Connection con = DriverManager.getConnection(connectionString)) {
                        // In SQL Azure, both ALLOW_SNAPSHOT_ISOLATION and READ_COMMITTED_SNAPSHOT options
                        // are always ON and can NOT be turned OFF. Thus the default transaction isolation level
                        // READ_COMMITTED
                        // always uses snapshot row-versioning in SQL Azure, and the reader transaction will not be
                        // blocked if
                        // it's executing at the default isolation level.
                        // To allow the blocking behavior for the reader transaction (as required by the test logic),
                        // we have to set its isolation level to REPEATABLE_READ (or SERIALIZABLE) in SQL Azure.
                        //
                        // Reference: http://msdn.microsoft.com/en-us/library/ee336245.aspx#isolevels
                        if (isSqlAzure()) {
                            con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                        }

                        // Try to SELECT the entire table. This should return some rows and then block
                        // on the locked part of the table until the one shot cancel thread cancels
                        // statement execution.
                        //
                        // Need to use adaptive response buffering when executing the statement.
                        // Otherwise, we would block in executeQuery()...
                        try (PreparedStatement stmt = con.prepareStatement(
                                "SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName),
                                SQLServerResultSet.TYPE_SS_DIRECT_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {
                            ((SQLServerStatement) stmt).setResponseBuffering("adaptive");
                            try (ResultSet rs = stmt.executeQuery()) {

                                // Time how long it takes for execution to be cancelled...
                                long elapsedMillis = -System.currentTimeMillis();

                                // Start up a thread to cancel the SELECT after 3 seconds.
                                oneShotCancel = new Thread(new OneShotCancel(stmt, 3));
                                oneShotCancel.start();

                                // Start retrieving rows and see how far we get...
                                int numSelectedRows = 0;
                                try {
                                    while (rs.next())
                                        ++numSelectedRows;

                                    assertEquals(false, true, TestResource.getResource("R_expectedExceptionNotThrown"));
                                } catch (SQLException e) {
                                    assertTrue(TestResource.getResource("R_queryCancelled").contains(e.getMessage()),
                                            TestResource.getResource("R_unexpectedException"));
                                }

                                elapsedMillis += System.currentTimeMillis();

                                // We should be able to retrieve no more than the number of rows before the blocked row.
                                // Note that we may actually get fewer rows than the number of rows before the blocked
                                // row
                                // if SQL Server is a little slow in returning rows to us.
                                if (numSelectedRows >= NUM_TABLE_ROWS - MIN_TABLE_ROWS) {
                                    assertEquals(NUM_TABLE_ROWS - MIN_TABLE_ROWS, numSelectedRows,
                                            TestResource.getResource("R_valueNotMatch"));
                                }

                                // If we were able to iterate through all of the expected
                                // rows without blocking, then something went wrong with our
                                // efforts to block execution.
                                if (elapsedMillis < 2500) {
                                    assertEquals(2500, (int) elapsedMillis,
                                            TestResource.getResource("R_executionNotLong"));
                                }
                            }

                            // Verify the statement & connection are still usable after cancelling
                            try (ResultSet rs = stmt.executeQuery()) {
                                rs.next();
                                stmt.cancel();
                            }
                        }
                    }
                }
            }
        }

        /**
         * Same as testCancelBlockedResponse, but with a server cursor.
         *
         * Expected: Statement cancel should cancel blocked server fetch in rs.next() call and connection should remain
         * usable.
         */
        @Test
        public void testCancelBlockedCursoredResponse() throws Exception {
            Thread oneShotCancel = null;

            try (Connection conLock = DriverManager.getConnection(connectionString)) {
                // Start a transaction on a second connection that locks the last part of the table
                // and leave it non-responsive for now...
                conLock.setAutoCommit(false);
                try (Statement stmtLock = conLock.createStatement()) {
                    stmtLock.executeUpdate("UPDATE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                            + " SET col2 = 'New Value!' WHERE col1 = " + (NUM_TABLE_ROWS - MIN_TABLE_ROWS));

                    try (Connection con = DriverManager.getConnection(connectionString)) {
                        // In SQL Azure, both ALLOW_SNAPSHOT_ISOLATION and READ_COMMITTED_SNAPSHOT options
                        // are always ON and can NOT be turned OFF. Thus the default transaction isolation level
                        // READ_COMMITTED
                        // always uses snapshot row-versioning in SQL Azure, and the reader transaction will not be
                        // blocked
                        // if
                        // it's executing at the default isolation level.
                        // To allow the blocking behavior for the reader transaction (as required by the test logic),
                        // we have to set its isolation level to REPEATABLE_READ (or SERIALIZABLE) in SQL Azure.
                        //
                        // Reference: http://msdn.microsoft.com/en-us/library/ee336245.aspx#isolevels
                        if (isSqlAzure()) {
                            con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                        }

                        try (PreparedStatement stmt = con.prepareStatement(
                                "SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName),
                                SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)) {

                            // Start up a thread to cancel the following SELECT after 3 seconds of blocking.
                            oneShotCancel = new Thread(new OneShotCancel(stmt, 3));
                            oneShotCancel.start();
                            long elapsedMillis = -System.currentTimeMillis();

                            // Try to SELECT the entire table.
                            try (ResultSet rs = stmt.executeQuery()) {
                                int numSelectedRows = 0;

                                // Verify that we can get the first block of rows. A DYNAMIC cursor won't block
                                // on the selection until it encounters the table page with the blocked row.
                                while (numSelectedRows < MIN_TABLE_ROWS && rs.next())
                                    ++numSelectedRows;

                                assertEquals(MIN_TABLE_ROWS, numSelectedRows,
                                        TestResource.getResource("R_valueNotMatch"));

                                // Now, try to grab the remaining rows from the result set. At some point the call
                                // to ResultSet.next() should block until the statement is cancelled from the other
                                // thread.
                                try {
                                    while (rs.next())
                                        ++numSelectedRows;

                                    assertEquals(false, true, TestResource.getResource("R_expectedExceptionNotThrown"));
                                } catch (SQLException e) {
                                    assertTrue(TestResource.getResource("R_queryCancelled").contains(e.getMessage()),
                                            TestResource.getResource("R_unexpectedException"));
                                }
                                elapsedMillis += System.currentTimeMillis();

                                // If we get here to early, then we were able to scan through the rows too fast.
                                // There's some slop in the elapsed time due to imprecise timer resolution.
                                if (elapsedMillis < 2500) {
                                    assertEquals(2500, (int) elapsedMillis,
                                            TestResource.getResource("R_executionNotLong"));
                                }

                                // Looks like we were canceled. Exception message matched. Time took as long
                                // as expected. One last check: Make sure we actually get back fewer rows than
                                // we initially asked for. If any rows beyond the locked row were returned
                                // then something went wrong.
                                assertEquals(true, (numSelectedRows <= NUM_TABLE_ROWS - MIN_TABLE_ROWS),
                                        TestResource.getResource("R_valueNotMatch"));
                            }
                        }
                    }
                }
            }
        }

        /**
         * Test that cancellation after processing the response does not impact subsequent reexecution.
         */
        @Test
        public void testCancelAfterResponse() throws Exception {
            int numSelectedRows;
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement()) {
                try (ResultSet rs = stmt
                        .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                    numSelectedRows = 0;
                    while (rs.next())
                        ++numSelectedRows;
                }

                assertEquals(NUM_TABLE_ROWS, numSelectedRows, TestResource.getResource("R_valueNotMatch"));

                // "Cancel" the executed query
                stmt.cancel();

                // Verify that the query can be re-executed without error
                try (ResultSet rs = stmt
                        .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                    numSelectedRows = 0;
                    while (rs.next())
                        ++numSelectedRows;
                }
                assertEquals(NUM_TABLE_ROWS, numSelectedRows, TestResource.getResource("R_valueNotMatch"));
            }
        }

        /**
         * Test various scenarios for cancelling CallableStatement execution between first availability of the results
         * and handling of the last OUT parameter
         */
        @Test
        public void testCancelGetOutParams() throws Exception {
            // Use small packet size to force OUT params to span multiple packets
            // so that cancelling execution from the same thread will work.
            final String procName = RandomUtil.getIdentifier("p1");
            try (Connection con = DriverManager.getConnection(connectionString + ";packetSize=512");
                    Statement stmt = con.createStatement()) {

                try {
                    TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(procName), stmt);
                } catch (Exception ex) {} ;
                stmt.executeUpdate("CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procName)
                        + "    @arg1 CHAR(512) OUTPUT, " + "    @arg2 CHAR(512) OUTPUT, "
                        + "    @arg3 CHAR(512) OUTPUT " + "AS " + "BEGIN " + "   SET @arg1='hi' "
                        + "   SET @arg2='there' " + "   SET @arg3='!' " + "END");
                try (CallableStatement cstmt = con
                        .prepareCall("{call " + AbstractSQLGenerator.escapeIdentifier(procName) + "(?, ?, ?)}")) {
                    ((SQLServerStatement) cstmt).setResponseBuffering("adaptive");
                    cstmt.registerOutParameter(1, Types.CHAR);
                    cstmt.registerOutParameter(2, Types.CHAR);
                    cstmt.registerOutParameter(3, Types.CHAR);

                    // Cancel before getting any OUT params
                    cstmt.execute();
                    cstmt.cancel();

                    // Cancel after getting first OUT param
                    cstmt.execute();
                    cstmt.getString(1);
                    cstmt.cancel();

                    // Cancel after getting last OUT param
                    cstmt.execute();
                    cstmt.getString(3);
                    cstmt.cancel();

                    // Cancel after getting OUT params out of order
                    cstmt.execute();
                    cstmt.getString(2);
                    cstmt.getString(1);
                    cstmt.cancel();

                    // Reexecute to prove CS is still good after last cancel
                    cstmt.execute();
                }
                TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(procName), stmt);
            }
        }

        static final int RUN_TIME_MILLIS = 10000;

        /**
         * Test that tries to flush out cancellation synchronization issues by repeatedly executing and cancelling
         * statements on multiple threads.
         *
         * Typical expected failures would be liveness issues (which would manifest as a test being non-responsive),
         * incorrect results, or TDS corruption problems.
         *
         * A set of thread pairs runs for 10 seconds. Each pair has one thread repeatedly executing a SELECT statement
         * and one thread repeatedly cancelling execution of that statement. Nothing is done to validate whether any
         * particular call to cancel had any affect on the statement. Liveness issues typically would manifest as a no
         * response in this test.
         *
         * In order to maximize the likelihood of this test finding bugs, it should run on a multi-proc machine with the
         * -server flag specified to the JVM. Also, the debugging println statements are commented out deliberately to
         * minimize the impact to the test from the diagnostics, which may artificially synchronize execution.
         */
        @Test
        public void testHammerCancel() throws Exception {

            class Hammer {
                final int id;
                int numCancelTries = 0;
                int numCancelSuccesses = 0;
                int numCancelExceptions = 0;
                int numCancellations = 0;
                int numExecuteSuccesses = 0;
                int numExecuteExceptions = 0;
                int numCloseExceptions = 0;

                private final int startDelay;
                private final int cancelInterval;
                private Statement newStmt = null;

                final ScheduledExecutorService executionScheduler = Executors.newSingleThreadScheduledExecutor();

                final ScheduledExecutorService cancelScheduler = Executors.newSingleThreadScheduledExecutor();

                Hammer(int id, int startDelay, int maxCancels) {
                    this.id = id;
                    this.startDelay = startDelay;
                    this.cancelInterval = RUN_TIME_MILLIS / maxCancels;
                }

                void start(final Connection con) {

                    try {
                        newStmt = con.createStatement();
                    } catch (SQLException e) {
                        fail(id + " " + e.getMessage());
                    }

                    final Statement stmt = newStmt;

                    final Runnable runner = new Runnable() {
                        public void run() {
                            try (ResultSet rs = stmt.executeQuery(
                                    "SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {

                                while (rs.next())
                                    ++numExecuteSuccesses;
                            } catch (SQLException e) {
                                // "Statement cancelled" (SQLState "HY008") exceptions
                                // are to be expected, of course...
                                if (e.getSQLState().equals("HY008")) {
                                    ++numCancellations;
                                } else {
                                    log.fine(id + ": execute/next threw: " + e.getSQLState() + " " + e.getMessage());
                                    ++numExecuteExceptions;
                                }
                            }
                        }
                    };

                    final Runnable canceller = new Runnable() {
                        public void run() {
                            try {
                                ++numCancelTries;
                                log.fine(id + " cancelling " + numCancelTries);
                                stmt.cancel();
                                ++numCancelSuccesses;
                            } catch (SQLException e) {
                                ++numCancelExceptions;
                                log.fine(id + ": cancel threw: " + e.getMessage());
                            }
                        }
                    };

                    executionScheduler.scheduleAtFixedRate(runner, startDelay, 1, TimeUnit.MILLISECONDS);

                    cancelScheduler.scheduleAtFixedRate(canceller, cancelInterval, cancelInterval,
                            TimeUnit.MILLISECONDS);
                }

                void stop() {

                    cancelScheduler.shutdown();
                    executionScheduler.shutdown();

                    while (!cancelScheduler.isTerminated()) {
                        try {
                            cancelScheduler.awaitTermination(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            log.fine(id + " ignoring interrupted exception while waiting for shutdown: "
                                    + e.getMessage());
                        }
                    }

                    while (!executionScheduler.isTerminated()) {
                        try {
                            executionScheduler.awaitTermination(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            log.fine(id + " ignoring interrupted exception while waiting for shutdown: "
                                    + e.getMessage());
                        }
                    }

                    try {
                        if (null != newStmt) {
                            newStmt.close();
                        }
                    } catch (SQLException e) {
                        log.fine(id + ": close threw: " + e.getMessage());
                        ++numCloseExceptions;
                    }
                }
            }

            final Hammer[] hammers = new Hammer[] {
                    // Execution and cancel intervals in milliseconds
                    //
                    // Aguments are:
                    // (hammer ID, execute interval, cancel interval)
                    new Hammer(4, 120, 180), new Hammer(3, 60, 184), new Hammer(2, 30, 150), new Hammer(1, 10, 50)};

            final Connection dbCon = DriverManager.getConnection(connectionString);

            for (Hammer hammer : hammers)
                hammer.start(dbCon);

            Thread.sleep(RUN_TIME_MILLIS); // Wait for everything to run a while

            for (Hammer hammer : hammers)
                hammer.stop();

            dbCon.close();

            // Gather and validate statistics
            int numExecuteSuccesses = 0;
            int numCancelSuccesses = 0;
            int numCancellations = 0;
            int numExecuteExceptions = 0;
            int numCancelExceptions = 0;
            int numCloseExceptions = 0;

            for (Hammer hammer : hammers) {
                log.fine("Hammer: " + hammer.id);
                log.fine("Execute successes: " + hammer.numExecuteSuccesses);
                log.fine("Cancel successes: " + hammer.numCancelSuccesses);
                log.fine("Cancellations: " + hammer.numCancellations);
                log.fine("Execute exceptions: " + hammer.numExecuteExceptions);
                log.fine("Cancel exceptions: " + hammer.numCancelExceptions);
                log.fine("Close exceptions: " + hammer.numCloseExceptions);
                log.fine("");

                numExecuteSuccesses += hammer.numExecuteSuccesses;
                numCancelSuccesses += hammer.numExecuteSuccesses;
                numCancellations += hammer.numCancellations;
                numExecuteExceptions += hammer.numExecuteExceptions;
                numCancelExceptions += hammer.numCancelExceptions;
                numCloseExceptions += hammer.numCloseExceptions;
            }

            assertEquals(true, 0 != numExecuteSuccesses, "No execution successes");
            assertEquals(true, 0 != numCancelSuccesses, "No cancels succeeded");
            assertEquals(true, 0 != numCancellations, "No executions cancelled");
            assertEquals(numExecuteExceptions, 0, "Test had execution exceptions");
            assertEquals(numCancelExceptions, 0, "Test had cancel exceptions");
            assertEquals(numCloseExceptions, 0, "Test had close exceptions");
        }

        @Test
        public void testIsCloseOnCompletion() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    PreparedStatement ps = con.prepareStatement("")) {

                boolean result = false;
                try {
                    result = ps.isCloseOnCompletion();
                } catch (Exception e) {

                    throw new SQLException(TestResource.getResource("R_unexpectedException") + ": ", e);

                }

                assertEquals(false, result, "isCloseOnCompletion: " + TestResource.getResource("R_incorrectDefault"));
            }
        }

        @Test
        public void testCloseOnCompletion() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    PreparedStatement ps = con.prepareStatement("select ?")) {
                ps.setInt(1, 1);

                // enable isCloseOnCompletion
                try {
                    ps.closeOnCompletion();
                } catch (Exception e) {
                    throw new SQLException(TestResource.getResource("R_unexpectedException") + ": ", e);

                }

                try (ResultSet rs = ps.executeQuery()) {
                    rs.close();
                } catch (SQLException e) {
                    log.fine("testIsCloseOnCompletion threw: " + e.getMessage());
                }

                assertEquals(ps.isClosed(), true, TestResource.getResource("R_statementShouldBeClosed"));
            }
        }
    }

    @Nested
    @Tag("AzureDWTest")
    public class TCStatement {
        private final String table1Name = RandomUtil.getIdentifier("TCStatement1");
        private final String table2Name = RandomUtil.getIdentifier("TCStatement2");

        /**
         * test statement.closeOnCompltetion method
         * 
         * @throws Exception
         */
        @Test
        public void testIsCloseOnCompletion() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement()) {

                assertEquals(false, stmt.isCloseOnCompletion(), "isCloseOnCompletion default should be false.");

                // enable isCloseOnCompletion
                try {
                    stmt.closeOnCompletion();
                } catch (Exception e) {
                    throw new SQLException(TestResource.getResource("R_unexpectedException") + ": ", e);
                }

                assertEquals(true, stmt.isCloseOnCompletion(),
                        "isCloseOnCompletion " + TestResource.getResource("R_shouldBeEnabled"));
            }
        }

        /**
         * Tests updateCount method after error in trigger with having connection property lastUpdateCount = false
         */
        @Test
        public void testCloseOnCompletion() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement()) {
                // enable isCloseOnCompletion
                try {
                    stmt.closeOnCompletion();
                } catch (Exception e) {

                    throw new SQLException(TestResource.getResource("R_unexpectedException") + ": ", e);

                }

                try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
                    assertEquals(stmt.isClosed(), false, TestResource.getResource("R_statementShouldBeOpened"));

                    // now statement should be closed
                    rs.close();

                    assertEquals(stmt.isClosed(), true, TestResource.getResource("R_statementShouldBeClosed"));
                }
            }
        }

        /**
         * Tests several queries
         * 
         * @throws Exception
         */
        @Test
        public void testConsecutiveQueries() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement()) {
                // enable isCloseOnCompletion
                try {
                    stmt.closeOnCompletion();
                } catch (Exception e) {
                    throw new SQLException(TestResource.getResource("R_unexpectedException") + ": ", e);
                }

                try {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table1Name), stmt);
                } catch (SQLException e) {}
                try {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table2Name), stmt);
                } catch (SQLException e) {}

                stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(table1Name) + " (col1 INT)");
                stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(table2Name) + " (col1 INT)");

                try (ResultSet rs = stmt.executeQuery(
                        "SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(table2Name))) {} catch (Exception e) {
                    assertEquals(stmt.isClosed(), true, TestResource.getResource("R_statementShouldBeClosed"));
                }
            }
        }

        /**
         * TestJDBCVersion.value < 42 getLargeMaxRows / setLargeMaxRows should throw exception for version before
         * sqljdbc42
         * 
         * @throws Exception
         */
        @Test
        public void testLargeMaxRowsJDBC41() throws Exception {
            assumeTrue("JDBC41".equals(TestUtils.getConfiguredProperty("JDBC_Version")),
                    TestResource.getResource("R_incompatJDBC"));

            try (Connection con = DriverManager.getConnection(connectionString);
                    SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {

                // testing exception for getLargeMaxRows method
                try {
                    stmt.getLargeMaxRows();
                    throw new SQLException(TestResource.getResource("R_unexpectedException"));
                } catch (Exception e) {
                    fail(e.getMessage());
                }

                // testing exception for setLargeMaxRows method
                try {
                    stmt.setLargeMaxRows(2015);
                    throw new SQLException(TestResource.getResource("R_unexpectedException"));
                } catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        }

        /**
         * testLargeMaxRows on JDBCVersion = 42 or later
         * 
         * @throws Exception
         */
        @Test
        public void testLargeMaxRowsJDBC42() throws Exception {
            assumeTrue("JDBC42".equals(TestUtils.getConfiguredProperty("JDBC_Version")),
                    TestResource.getResource("R_incompatJDBC"));

            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement()) {

                // Default value should return zero
                long actual = stmt.getLargeMaxRows();
                assertEquals(actual, (long) 0, "getLargeMaxRows():" + TestResource.getResource("R_incorrectDefault"));

                // Set a new value less than MAX_VALUE, and then get the modified value
                long newValue = 2012L;
                stmt.setLargeMaxRows(newValue);
                actual = stmt.getLargeMaxRows();
                assertEquals(actual, newValue, "LargeMaxRows() : set/get problem");

                // Set a new value grater than MAX_VALUE, and then get the modified value
                // SQL Server only supports integer limits for setting max rows
                // If the value MAX_VALUE + 1 is accepted, throw exception
                try {
                    newValue = (long) Integer.MAX_VALUE + 1;
                    stmt.setLargeMaxRows(newValue);
                    throw new SQLException("setLargeMaxRows(): Long values should not be set");
                } catch (Exception e) {
                    assertEquals(
                            ("calling setLargeMaxRows failed : java.lang.UnsupportedOperationException: "
                                    + "The supported maximum row count for a result set is Integer.MAX_VALUE or less."),
                            (e.getMessage()), TestResource.getResource("R_unexpectedException"));
                }

                // Set a negative value. If negative is accepted, throw exception
                try {
                    stmt.setLargeMaxRows(-2012L);
                    throw new SQLException("setLargeMaxRows():  Negative value not allowed");
                } catch (Exception e) {
                    assertEquals(
                            "calling setLargeMaxRows failed : com.microsoft.sqlserver.jdbc.SQLServerException: "
                                    + "The maximum row count -2,012 for a result set must be non-negative.",
                            e.getMessage(), TestResource.getResource("R_unexpectedException"));
                }
            }
        }

        @AfterEach
        public void terminate() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement();) {
                try {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table1Name), stmt);
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table2Name), stmt);
                } catch (SQLException e) {}
            }
        }
    }

    @Nested
    public class TCStatementCallable {
        String procName = RandomUtil.getIdentifier("p1");

        /**
         * Tests CallableStatementMethods on jdbc41
         * 
         * @throws Exception
         */
        @Test
        public void testJdbc41CallableStatementMethods() throws Exception {
            // Prepare database setup

            try (Connection conn = DriverManager.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
                String query = "create procedure " + AbstractSQLGenerator.escapeIdentifier(procName)
                        + " @col1Value varchar(512) OUTPUT," + " @col2Value int OUTPUT," + " @col3Value float OUTPUT,"
                        + " @col4Value decimal(10,5) OUTPUT," + " @col5Value uniqueidentifier OUTPUT,"
                        + " @col6Value xml OUTPUT," + " @col7Value varbinary(max) OUTPUT," + " @col8Value text OUTPUT,"
                        + " @col9Value ntext OUTPUT," + " @col10Value varbinary(max) OUTPUT,"
                        + " @col11Value date OUTPUT," + " @col12Value time OUTPUT," + " @col13Value datetime2 OUTPUT,"
                        + " @col14Value datetimeoffset OUTPUT," + " @col15Value decimal(10,10) OUTPUT,"
                        + " @col16Value decimal(38,38) OUTPUT" + " AS BEGIN " + " SET @col1Value = 'hello'"
                        + " SET @col2Value = 1" + " SET @col3Value = 2.0" + " SET @col4Value = 123.45"
                        + " SET @col5Value = '6F9619FF-8B86-D011-B42D-00C04FC964FF'" + " SET @col6Value = '<test/>'"
                        + " SET @col7Value = 0x63C34D6BCAD555EB64BF7E848D02C376" + " SET @col8Value = 'text'"
                        + " SET @col9Value = 'ntext'" + " SET @col10Value = 0x63C34D6BCAD555EB64BF7E848D02C376"
                        + " SET @col11Value = '2017-05-19'" + " SET @col12Value = '10:47:15.1234567'"
                        + " SET @col13Value = '2017-05-19T10:47:15.1234567'"
                        + " SET @col14Value = '2017-05-19T10:47:15.1234567+02:00'" + " SET @col15Value = 0.123456789"
                        + " SET @col16Value = 0.1234567890123456789012345678901234567" + " END";
                stmt.execute(query);

                // Test JDBC 4.1 methods for CallableStatement
                try (CallableStatement cstmt = conn
                        .prepareCall("{call " + AbstractSQLGenerator.escapeIdentifier(procName)
                                + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}")) {
                    cstmt.registerOutParameter(1, java.sql.Types.VARCHAR);
                    cstmt.registerOutParameter(2, java.sql.Types.INTEGER);
                    cstmt.registerOutParameter("col3Value", java.sql.Types.FLOAT);
                    cstmt.registerOutParameter(4, java.sql.Types.DECIMAL, 2);
                    cstmt.registerOutParameter(5, microsoft.sql.Types.GUID);
                    cstmt.registerOutParameter(6, java.sql.Types.SQLXML);
                    cstmt.registerOutParameter("col7Value", java.sql.Types.VARBINARY, 10);
                    cstmt.registerOutParameter(8, java.sql.Types.CLOB);
                    cstmt.registerOutParameter(9, java.sql.Types.NCLOB);
                    cstmt.registerOutParameter(10, java.sql.Types.VARBINARY);
                    cstmt.registerOutParameter(11, java.sql.Types.DATE);
                    cstmt.registerOutParameter(12, java.sql.Types.TIME);
                    cstmt.registerOutParameter(13, java.sql.Types.TIMESTAMP);
                    cstmt.registerOutParameter(14, java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
                    cstmt.registerOutParameter(15, java.sql.Types.DECIMAL, "DECIMAL");
                    cstmt.registerOutParameter("col16Value", java.sql.Types.DECIMAL, "DECIMAL");
                    cstmt.execute();

                    assertEquals("hello", cstmt.getObject(1, String.class));
                    assertEquals("hello", cstmt.getObject("col1Value", String.class));

                    assertEquals(Integer.valueOf(1), cstmt.getObject(2, Integer.class));
                    assertEquals(Integer.valueOf(1), cstmt.getObject("col2Value", Integer.class));

                    assertEquals(2.0f, cstmt.getObject(3, Float.class), 0.0001f);
                    assertEquals(2.0f, cstmt.getObject("col3Value", Float.class), 0.0001f);
                    assertEquals(2.0d, cstmt.getObject(3, Double.class), 0.0001d);
                    assertEquals(2.0d, cstmt.getObject("col3Value", Double.class), 0.0001d);

                    // BigDecimal#equals considers the number of decimal places
                    assertEquals(0, cstmt.getObject(4, BigDecimal.class).compareTo(new BigDecimal("123.45")));
                    assertEquals(0, cstmt.getObject("col4Value", BigDecimal.class).compareTo(new BigDecimal("123.45")));

                    assertEquals(UUID.fromString("6F9619FF-8B86-D011-B42D-00C04FC964FF"),
                            cstmt.getObject(5, UUID.class));
                    assertEquals(UUID.fromString("6F9619FF-8B86-D011-B42D-00C04FC964FF"),
                            cstmt.getObject("col5Value", UUID.class));

                    SQLXML sqlXml;
                    sqlXml = cstmt.getObject(6, SQLXML.class);
                    try {
                        assertEquals("<test/>", sqlXml.getString());
                    } finally {
                        sqlXml.free();
                    }

                    Blob blob;
                    blob = cstmt.getObject(7, Blob.class);
                    try {
                        assertArrayEquals(
                                new byte[] {0x63, (byte) 0xC3, 0x4D, 0x6B, (byte) 0xCA, (byte) 0xD5, 0x55, (byte) 0xEB,
                                        0x64, (byte) 0xBF, 0x7E, (byte) 0x84, (byte) 0x8D, 0x02, (byte) 0xC3, 0x76},
                                blob.getBytes(1, 16));
                    } finally {
                        blob.free();
                    }

                    Clob clob;
                    clob = cstmt.getObject(8, Clob.class);
                    try {
                        assertEquals("text", clob.getSubString(1, 4));
                    } finally {
                        clob.free();
                    }

                    NClob nclob;
                    nclob = cstmt.getObject(9, NClob.class);
                    try {
                        assertEquals("ntext", nclob.getSubString(1, 5));
                    } finally {
                        nclob.free();
                    }

                    assertArrayEquals(
                            new byte[] {0x63, (byte) 0xC3, 0x4D, 0x6B, (byte) 0xCA, (byte) 0xD5, 0x55, (byte) 0xEB,
                                    0x64, (byte) 0xBF, 0x7E, (byte) 0x84, (byte) 0x8D, 0x02, (byte) 0xC3, 0x76},
                            cstmt.getObject(10, byte[].class));
                    assertEquals(java.sql.Date.valueOf("2017-05-19"), cstmt.getObject(11, java.sql.Date.class));
                    assertEquals(java.sql.Date.valueOf("2017-05-19"),
                            cstmt.getObject("col11Value", java.sql.Date.class));

                    java.sql.Time expectedTime = new java.sql.Time(java.sql.Time.valueOf("10:47:15").getTime() + 123L);
                    assertEquals(expectedTime, cstmt.getObject(12, java.sql.Time.class));
                    assertEquals(expectedTime, cstmt.getObject("col12Value", java.sql.Time.class));

                    assertEquals(java.sql.Timestamp.valueOf("2017-05-19 10:47:15.1234567"),
                            cstmt.getObject(13, java.sql.Timestamp.class));
                    assertEquals(java.sql.Timestamp.valueOf("2017-05-19 10:47:15.1234567"),
                            cstmt.getObject("col13Value", java.sql.Timestamp.class));

                    assertEquals("2017-05-19 10:47:15.1234567 +02:00",
                            cstmt.getObject(14, microsoft.sql.DateTimeOffset.class).toString());
                    assertEquals("2017-05-19 10:47:15.1234567 +02:00",
                            cstmt.getObject("col14Value", microsoft.sql.DateTimeOffset.class).toString());

                    // BigDecimal#equals considers the number of decimal places (OutParams always return 4 decimal
                    // digits rounded up)
                    assertEquals(0, cstmt.getObject(15, BigDecimal.class).compareTo(new BigDecimal("0.1235")));
                    assertEquals(0,
                            cstmt.getObject("col15Value", BigDecimal.class).compareTo(new BigDecimal("0.1235")));

                    assertEquals(0, cstmt.getObject(16, BigDecimal.class).compareTo(new BigDecimal("0.1235")));
                    assertEquals(0,
                            cstmt.getObject("col16Value", BigDecimal.class).compareTo(new BigDecimal("0.1235")));
                }
            }
        }

        @AfterEach
        public void terminate() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement()) {
                try {
                    TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(procName), stmt);
                } catch (SQLException e) {
                    fail(e.toString());
                }
            }
        }

    }

    @Nested
    public class TCStatementParam {
        private final String tableName = RandomUtil.getIdentifier("TCStatementParam");
        private final String procName = RandomUtil.getIdentifier("TCStatementParam");

        /**
         * 
         * @throws Exception
         */
        @Test
        public void testStatementOutParamGetsTwice() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement()) {

                // enable isCloseOnCompletion
                try {
                    stmt.closeOnCompletion();
                } catch (Exception e) {
                    log.fine("testStatementOutParamGetsTwice threw: " + e.getMessage());
                }

                stmt.executeUpdate("CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procName)
                        + " ( @p2_smallint smallint,  @p3_smallint_out smallint OUTPUT) AS SELECT @p3_smallint_out=@p2_smallint RETURN @p2_smallint + 1");

                try (ResultSet rs = stmt.getResultSet()) {
                    if (rs != null) {
                        rs.close();
                        assertEquals(stmt.isClosed(), true, TestResource.getResource("R_statementShouldBeClosed"));
                    } else {
                        assertEquals(stmt.isClosed(), false, TestResource.getResource("R_statementShouldBeOpened"));
                    }
                }

                try (CallableStatement cstmt = con
                        .prepareCall("{  ? = CALL " + AbstractSQLGenerator.escapeIdentifier(procName) + " (?,?)}")) {
                    cstmt.registerOutParameter(1, Types.INTEGER);
                    cstmt.setObject(2, Short.valueOf("32"), Types.SMALLINT);
                    cstmt.registerOutParameter(3, Types.SMALLINT);
                    cstmt.execute();
                    assertEquals(cstmt.getInt(1), 33, TestResource.getResource("R_valueNotMatch"));
                    assertEquals(cstmt.getInt(3), 32, TestResource.getResource("R_valueNotMatch"));

                    cstmt.setObject(2, Short.valueOf("34"), Types.SMALLINT);
                    cstmt.execute();
                    assertEquals(cstmt.getInt(1), 35, TestResource.getResource("R_valueNotMatch"));
                    assertEquals(cstmt.getInt(3), 34, TestResource.getResource("R_valueNotMatch"));
                    try (ResultSet rs = cstmt.getResultSet()) {
                        if (rs != null) {
                            rs.close();
                            assertEquals(stmt.isClosed(), true, TestResource.getResource("R_statementShouldBeClosed"));
                        } else {
                            assertEquals(stmt.isClosed(), false, TestResource.getResource("R_statementShouldBeOpened"));
                        }
                    }
                }
            }
        }

        @Test
        public void testStatementOutManyParamGetsTwiceRandomOrder() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement()) {
                stmt.executeUpdate("CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procName)
                        + " ( @p2_smallint smallint,  @p3_smallint_out smallint OUTPUT,  @p4_smallint smallint OUTPUT, @p5_smallint_out smallint OUTPUT) AS SELECT @p3_smallint_out=@p2_smallint, @p5_smallint_out=@p4_smallint RETURN @p2_smallint + 1");

                try (CallableStatement cstmt = con.prepareCall(
                        "{  ? = CALL " + AbstractSQLGenerator.escapeIdentifier(procName) + " (?,?, ?, ?)}")) {
                    cstmt.registerOutParameter(1, Types.INTEGER);
                    cstmt.setObject(2, Short.valueOf("32"), Types.SMALLINT);
                    cstmt.registerOutParameter(3, Types.SMALLINT);
                    cstmt.setObject(4, Short.valueOf("23"), Types.SMALLINT);
                    cstmt.registerOutParameter(5, Types.INTEGER);
                    cstmt.execute();
                    assertEquals(cstmt.getInt(1), 33, TestResource.getResource("R_valueNotMatch"));
                    assertEquals(cstmt.getInt(5), 23, TestResource.getResource("R_valueNotMatch"));
                    assertEquals(cstmt.getInt(3), 32, TestResource.getResource("R_valueNotMatch"));

                    cstmt.setObject(2, Short.valueOf("34"), Types.SMALLINT);
                    cstmt.setObject(4, Short.valueOf("24"), Types.SMALLINT);
                    cstmt.execute();
                    assertEquals(cstmt.getInt(3), 34, TestResource.getResource("R_valueNotMatch"));
                    assertEquals(cstmt.getInt(5), 24, TestResource.getResource("R_valueNotMatch"));
                    assertEquals(cstmt.getInt(1), 35, TestResource.getResource("R_valueNotMatch"));
                }
            }
        }

        /**
         * Tests callablestatement output params input and output
         * 
         * @throws Exception
         */
        @Test
        public void testStatementOutParamGetsTwiceInOut() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement()) {
                stmt.executeUpdate("CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procName)
                        + " ( @p2_smallint smallint,  @p3_smallint_out smallint OUTPUT) AS SELECT @p3_smallint_out=@p3_smallint_out +1 RETURN @p2_smallint + 1");

                try (CallableStatement cstmt = con
                        .prepareCall("{  ? = CALL " + AbstractSQLGenerator.escapeIdentifier(procName) + " (?,?)}")) {
                    cstmt.registerOutParameter(1, Types.INTEGER);
                    cstmt.setObject(2, Short.valueOf("1"), Types.SMALLINT);
                    cstmt.setObject(3, Short.valueOf("100"), Types.SMALLINT);
                    cstmt.registerOutParameter(3, Types.SMALLINT);
                    cstmt.execute();
                    assertEquals(cstmt.getInt(1), 2, TestResource.getResource("R_valueNotMatch"));
                    assertEquals(cstmt.getInt(1), 2, TestResource.getResource("R_valueNotMatch"));

                    cstmt.setObject(2, Short.valueOf("10"), Types.SMALLINT);
                    cstmt.execute();
                    assertEquals(cstmt.getInt(1), 11, TestResource.getResource("R_valueNotMatch"));
                    assertEquals(cstmt.getInt(3), 101, TestResource.getResource("R_valueNotMatch"));
                }
            }
        }

        /**
         * Tests resultset parameters
         * 
         * @throws Exception
         */
        @Test
        public void testResultSetParams() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {

                stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " (col1 int, col2 text, col3 int identity(1,1))");
                stmt.executeUpdate(
                        "Insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(0, 'hello')");
                stmt.executeUpdate(
                        "Insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(0, 'hi')");
                String query = "create procedure " + AbstractSQLGenerator.escapeIdentifier(procName)
                        + " @col1Value int, @col2Value varchar(512) OUTPUT AS BEGIN SELECT * from "
                        + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " where col1=@col1Value SET @col2Value='hi' END";
                stmt.execute(query);

                try (CallableStatement cstmt = con
                        .prepareCall("{call " + AbstractSQLGenerator.escapeIdentifier(procName) + "(?, ?)}")) {
                    cstmt.setInt(1, 0);
                    cstmt.registerOutParameter(2, java.sql.Types.VARCHAR);
                    try (ResultSet rs = cstmt.executeQuery()) {
                        rs.next();
                        assertEquals(rs.getString(2), "hello", TestResource.getResource("R_valueNotMatch"));
                        assertEquals(cstmt.getString(2), "hi", TestResource.getResource("R_valueNotMatch"));
                    }
                }
            }
        }

        /**
         * Tests resultset params with null parameters
         * 
         * @throws Exception
         */
        @Test
        public void testResultSetNullParams() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {

                stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " (col1 int, col2 text, col3 int identity(1,1))");
                stmt.executeUpdate(
                        "Insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(0, 'hello')");
                stmt.executeUpdate(
                        "Insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(0, 'hi')");
                String query = "create procedure " + AbstractSQLGenerator.escapeIdentifier(procName)
                        + " @col1Value int, @col2Value varchar(512) OUTPUT AS BEGIN SELECT * from "
                        + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " where col1=@col1Value SET @col2Value='hi' END";
                stmt.execute(query);

                try (CallableStatement cstmt = con
                        .prepareCall("{call " + AbstractSQLGenerator.escapeIdentifier(procName) + "(?, ?)}")) {
                    cstmt.setInt(1, 0);
                    try {
                        cstmt.getInt(2);
                    } catch (Exception ex) {
                        if (!ex.getMessage().equalsIgnoreCase("The output parameter 2 was not registered for output."))
                            throw ex;
                    }
                }
            }
        }

        /**
         * 
         * @throws Exception
         */
        @Test
        public void testFailedToResumeTransaction() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement()) {

                stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 int)");
                stmt.executeUpdate("Insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(0)");
                stmt.executeUpdate("Insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(1)");
                stmt.executeUpdate("Insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(2)");
                stmt.executeUpdate("Insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(3)");
                try (PreparedStatement ps = con.prepareStatement("BEGIN TRAN " + "Insert into "
                        + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(4) " + "ROLLBACK")) {}
                con.setAutoCommit(false);

                try (PreparedStatement ps2 = con.prepareStatement(
                        "Insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values('a')")) {
                    try {
                        ps2.execute();
                    } catch (SQLException e) {}
                    try {
                        stmt.executeUpdate(
                                "Insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(4)");
                    } catch (SQLException ex) {}
                }
            }
        }

        /**
         * 
         * @throws Exception
         */
        @Test
        public void testResultSetErrors() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {

                stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " (col1 int, col2 text, col3 int identity(1,1))");
                stmt.executeUpdate(
                        "Insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(0, 'hello')");
                stmt.executeUpdate(
                        "Insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(0, 'hi')");
                String query = "create procedure " + AbstractSQLGenerator.escapeIdentifier(procName)
                        + " @col1Value int, @col2Value varchar(512) OUTPUT AS BEGIN SELECT * from somenonexistenttable where col1=@col1Value SET @col2Value='hi' END";
                stmt.execute(query);

                try (CallableStatement cstmt = con
                        .prepareCall("{call " + AbstractSQLGenerator.escapeIdentifier(procName) + "(?, ?)}")) {
                    cstmt.setInt(1, 0);
                    cstmt.registerOutParameter(2, Types.VARCHAR);

                    try (ResultSet rs = cstmt.executeQuery()) {} catch (Exception ex) {} ;

                    assertEquals(null, cstmt.getString(2), TestResource.getResource("R_valueNotMatch"));
                }
            }
        }

        /**
         * Verify proper handling of row errors in ResultSets.
         */
        @Test
        @Disabled
        // TODO: We are commenting this out due to random AppVeyor failures. We will investigate later.
        public void testRowError() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement()) {

                stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 int)");
                stmt.executeUpdate("insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(0)");
                stmt.executeUpdate("insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(1)");
                stmt.executeUpdate("insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(2)");
                stmt.execute(
                        "create procedure " + AbstractSQLGenerator.escapeIdentifier(procName) + " @col1Value int AS "
                                + " BEGIN " + "    SELECT col1 FROM " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                + "       WITH (UPDLOCK) WHERE (col1 = @col1Value) " + " END");

                // For the test, lock each row in the table, one by one, for update
                // on one connection and, on another connection, verify that the
                // row is locked by looking for an expected row error for that row.
                // The expectation is that ResultSet.next() will throw an lock timeout
                // exception when the locked row is encountered, no matter whether the
                // locked row is the first one in the table or not. Also, the connection
                // must be usable after properly handling and dispatching the row locked
                // error.

                for (int row = 0; row <= 2; row++) {
                    // On the first connection, retrieve the indicated row,
                    // locking it for update.
                    try (Connection testConn1 = DriverManager.getConnection(connectionString)) {
                        testConn1.setAutoCommit(false);
                        try (CallableStatement cstmt = testConn1
                                .prepareCall("{call " + AbstractSQLGenerator.escapeIdentifier(procName) + "(?)}")) {
                            cstmt.setInt(1, row);

                            // enable isCloseOnCompletion
                            try {
                                cstmt.closeOnCompletion();
                            } catch (Exception e) {
                                throw new SQLException(TestResource.getResource("R_unexpectedException"));
                            }

                            try (ResultSet rs = cstmt.executeQuery()) {
                                assertEquals(true, rs.next(), "Query returned no rows");
                            }
                            assertEquals(cstmt.isClosed(), true, TestResource.getResource("R_statementShouldBeClosed"));

                            // On a second connection, repeat the query, with an immediate
                            // lock timeout to induce an error.
                            try (Connection testConn2 = DriverManager.getConnection(connectionString)) {
                                testConn2.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                                testConn2.setAutoCommit(false);
                                try (Statement stmt2 = testConn2.createStatement()) {
                                    stmt2.executeUpdate("SET LOCK_TIMEOUT 0");

                                    try (CallableStatement cstmt2 = testConn2.prepareCall("SELECT col1 FROM "
                                            + AbstractSQLGenerator.escapeIdentifier(tableName) + " WITH (UPDLOCK)")) {

                                        // Verify that the result set can be closed after
                                        // the lock timeout error
                                        try (ResultSet rs = cstmt2.executeQuery()) {
                                            rs.close();
                                        }
                                        // Verify that the ResultSet hits a lock timeout exception on the
                                        // indicated row and continues to report that exception on subsequent
                                        // accesses to that row.
                                        try (ResultSet rs = cstmt2.executeQuery()) {
                                            for (int i = 0; i < row; i++)
                                                assertEquals(true, rs.next(), "Query returned wrong number of rows.");

                                            for (int i = 0; i < 2; i++) {
                                                try {
                                                    rs.next();
                                                    assertEquals(false, true, "lock timeout"
                                                            + TestResource.getResource("R_expectedExceptionNotThrown"));
                                                } catch (SQLException e) {
                                                    assertEquals(1222, // lock timeout
                                                            e.getErrorCode(),
                                                            TestResource.getResource("R_unexpectedException")
                                                                    + e.getMessage());
                                                }
                                            }
                                        }
                                    }

                                    // Verify testConn2 is still usable for queries
                                    stmt2.executeQuery("SELECT 1").close();
                                }
                            }
                        }
                    }
                }
            }
        }

        @AfterEach
        public void terminate() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement()) {
                try {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                    TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(procName), stmt);
                } catch (SQLException e) {
                    fail(e.toString());
                }
            }
        }
    }

    @Nested
    public class TCSparseColumnSetAndNBCROW {
        private final String tableName = RandomUtil.getIdentifier("TCStatementSparseColumnSetAndNBCROW");

        private Connection createConnectionAndPopulateData() throws Exception {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setSelectMethod("direct");
            Connection con = null;

            con = ds.getConnection();

            Statement stmt = con.createStatement();

            stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + "(col1_int int IDENTITY(1,1), col2_varchar varchar(200), col3_varchar varchar(20) SPARSE NULL, col4_smallint smallint SPARSE NULL, col5_xml XML COLUMN_SET FOR ALL_SPARSE_COLUMNS, col6_nvarcharMax NVARCHAR(MAX), col7_varcharMax VARCHAR(MAX))");
            stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " DEFAULT VALUES");

            assertTrue(con != null, "connection is null");
            return con;
        }

        @AfterEach
        public void terminate() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement()) {
                try {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                } catch (SQLException e) {
                    fail(e.toString());
                }
            }
        }

        /**
         * tests that varchar(max) and nvarchar(max) columns return null correctly
         * 
         * @throws Exception
         */
        @Test
        public void testNBCROWNullsForLOBs() throws Exception {
            try (DBConnection dbconn = new DBConnection(connectionString)) {
                if (dbconn.getServerVersion() <= 9.0) {
                    log.fine("testNBCROWNullsForLOBs skipped for Yukon");
                }
            }

            try (Connection con = createConnectionAndPopulateData()) {
                Statement stmt = con.createStatement();
                String selectQuery = "SELECT col1_int, col2_varchar, col3_varchar, col4_smallint, col5_xml, col6_nvarcharMax, col7_varcharMax FROM "
                        + AbstractSQLGenerator.escapeIdentifier(tableName);
                try (ResultSet rs = stmt.executeQuery(selectQuery)) {
                    rs.next();

                    for (int i = 1; i <= 7; i++) {
                        String value = rs.getString(i);
                        if (i != 1) {
                            assertEquals(value, null, "expected null:" + value);
                        }
                    }
                }
            } finally {
                terminate();
            }
        }

        /**
         * Tests the following a) isSparseColumnSet returns true for column set b) isSparseColumnSet returns false for
         * non column set column
         * 
         * @throws Exception
         */
        @Test
        public void testSparseColumnSetValues() throws Exception {
            try (DBConnection dbconn = new DBConnection(connectionString)) {
                if (dbconn.getServerVersion() <= 9.0) {
                    log.fine("testSparseColumnSetValues skipped for Yukon");
                }
            }

            try (Connection con = createConnectionAndPopulateData(); Statement stmt = con.createStatement()) {
                String selectQuery = "SELECT col1_int, col2_varchar, col3_varchar, col4_smallint, col5_xml, col6_nvarcharMax, col7_varcharMax FROM "
                        + AbstractSQLGenerator.escapeIdentifier(tableName);
                try (ResultSet rs = stmt.executeQuery(selectQuery)) {
                    rs.next();

                    SQLServerResultSetMetaData rsmd = (SQLServerResultSetMetaData) rs.getMetaData();

                    // Test that isSparseColumnSet returns correct value for various columns
                    boolean isSparseColumnSet = false;
                    for (int i = 1; i <= 7; i++) {

                        isSparseColumnSet = rsmd.isSparseColumnSet(i);
                        if (i == 5) {
                            // this is the only sparse column set
                            assertEquals(isSparseColumnSet, true, "Incorrect value " + isSparseColumnSet);
                        } else {
                            assertEquals(isSparseColumnSet, false, "Incorrect value " + isSparseColumnSet);
                        }
                    }
                }
            } finally {
                terminate();
            }
        }

        /**
         * tests that isSparseColumnSet throws an exception for out of range index values
         * 
         * @throws Exception
         */
        @Test
        public void testSparseColumnSetIndex() throws Exception {
            try (DBConnection dbconn = new DBConnection(connectionString)) {
                if (dbconn.getServerVersion() <= 9.0) {
                    log.fine("testSparseColumnSetIndex skipped for Yukon");
                }
            }

            try (Connection con = createConnectionAndPopulateData(); Statement stmt = con.createStatement()) {
                String selectQuery = "SELECT col1_int, col2_varchar, col3_varchar, col4_smallint, col5_xml, col6_nvarcharMax, col7_varcharMax FROM "
                        + AbstractSQLGenerator.escapeIdentifier(tableName);
                try (ResultSet rs = stmt.executeQuery(selectQuery)) {
                    rs.next();

                    SQLServerResultSetMetaData rsmd = (SQLServerResultSetMetaData) rs.getMetaData();
                    try {
                        // test that an exception is thrown when invalid index(lower limit) is used
                        rsmd.isSparseColumnSet(0);
                        assertEquals(true, false, "Using index as 0 should have thrown an exception");
                    } catch (ArrayIndexOutOfBoundsException e) {}

                    try {
                        // test that an exception is thrown when invalid index(upper limit) is used
                        rsmd.isSparseColumnSet(8);
                        assertEquals(true, false, "Using index as 8 should have thrown an exception");
                    } catch (ArrayIndexOutOfBoundsException e) {}
                }
            } finally {
                terminate();
            }
        }

        /**
         * Tests the following for isSparseColumnSet api a) Metadata is available when result set is closed b) Metadata
         * is available when statement is closed c) Metadata is available when connection is closed
         * 
         * @throws Exception
         */
        @Test
        public void testSparseColumnSetForException() throws Exception {
            try (DBConnection dbconn = new DBConnection(connectionString)) {
                if (dbconn.getServerVersion() <= 9.0) {
                    log.fine("testSparseColumnSetForException skipped for Yukon");
                }
            }

            SQLServerResultSetMetaData rsmd;
            try (Connection con = createConnectionAndPopulateData(); Statement stmt = con.createStatement()) {

                String selectQuery = "SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName);
                try (ResultSet rs = stmt.executeQuery(selectQuery)) {
                    rs.next();
                    rsmd = (SQLServerResultSetMetaData) rs.getMetaData();
                    rs.close();
                    rsmd.isSparseColumnSet(1);
                }
                try (ResultSet rs = stmt.executeQuery(selectQuery)) {
                    rsmd = (SQLServerResultSetMetaData) rs.getMetaData();
                    stmt.close();
                    rsmd.isSparseColumnSet(1);
                }
                try (ResultSet rs = con.createStatement()
                        .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                    rsmd = (SQLServerResultSetMetaData) rs.getMetaData();
                    con.close();
                    rsmd.isSparseColumnSet(1);
                }
            }
        }

        /**
         * Tests that null values are returned correctly for a row containing all nulls(except the primary key column)
         * 
         * @throws Exception
         */
        @Test
        public void testNBCRowForAllNulls() throws Exception {
            try (DBConnection dbconn = new DBConnection(connectionString)) {
                if (dbconn.getServerVersion() <= 9.0) {
                    log.fine("testNBCRowForAllNulls skipped for Yukon");
                }
            }

            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setSelectMethod("direct");

            try (Connection con = ds.getConnection(); Statement stmt = con.createStatement()) {
                try {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                } catch (SQLException e) {}

                String createTableQuery = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + "(col1 int IDENTITY(1,1)";

                int noOfColumns = 128;
                for (int i = 2; i <= noOfColumns; i++) {
                    createTableQuery = createTableQuery + ", col" + i + " int";
                }
                createTableQuery += ")";
                stmt.executeUpdate(createTableQuery);
                stmt.executeUpdate(
                        "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " DEFAULT VALUES");
                try (ResultSet rs = stmt
                        .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                    rs.next();

                    // test that all columns except the first one are null
                    for (int i = 2; i <= noOfColumns; i++) {
                        String value = rs.getString(i);
                        assertEquals(value, null, "expected null:" + value);
                    }
                }
            } finally {
                terminate();
            }
        }

        /**
         * Tests that the null values are returned correctly when rows and columns are accessed in a random manner
         * 
         * @throws Exception
         */
        @Test
        public void testNBCROWWithRandomAccess() throws Exception {
            try (DBConnection dbconn = new DBConnection(connectionString)) {
                if (dbconn.getServerVersion() <= 9.0) {
                    log.fine("testNBCROWWithRandomAccess skipped for Yukon");
                }
            }

            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setSelectMethod("direct");

            try (Connection con = ds.getConnection()) {
                Random r = new Random();
                int noOfColumns = 100;
                int noOfRows = 10;
                ArrayList<Integer> nonNullColumns = null;

                try (Statement stmt = con.createStatement()) {
                    try {
                        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                    } catch (SQLException e) {}

                    // construct a query to create a table with 100 columns
                    String createTableQuery = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                            + "(col1 int IDENTITY(1,1)";

                    for (int i = 2; i <= noOfColumns; i++) {
                        createTableQuery = createTableQuery + ", col" + i + " int";
                    }
                    createTableQuery += ")";
                    stmt.executeUpdate(createTableQuery);

                    stmt.executeUpdate("TRUNCATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName));

                    // randomly generate columns whose values would be set to a non null value
                    nonNullColumns = new ArrayList<>();
                    nonNullColumns.add(1);// this is always non-null

                    // Add approximately 10 non-null columns. The number should be low
                    // so that we get NBCROW token
                    for (int i = 0; i < 10; i++) {
                        int nonNullColumn = (int) (r.nextDouble() * noOfColumns) + 1;
                        if (!nonNullColumns.contains(nonNullColumn)) {
                            nonNullColumns.add(nonNullColumn);
                        }
                    }

                    // construct the insert query
                    String insertQuery = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + "(";
                    String values = " VALUES(";
                    for (int i = 1; i < nonNullColumns.size(); i++) {
                        insertQuery = insertQuery + "col" + nonNullColumns.get(i);
                        values += "1";
                        if (i == nonNullColumns.size() - 1) {
                            insertQuery += ")";
                            values += ")";
                        } else {
                            insertQuery += ",";
                            values += ",";
                        }
                    }
                    insertQuery += values;

                    // if there are no non-null columns
                    if (nonNullColumns.size() == 1)
                        insertQuery = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                + " DEFAULT VALUES";

                    log.fine("INSEER Query:" + insertQuery);
                    // populate the table by executing the insert query
                    for (int i = 0; i < noOfRows; i++) {
                        stmt.executeUpdate(insertQuery);
                    }
                }

                try (Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                        ResultSet rs = stmt
                                .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {

                    // Try accessing rows and columns randomly for 10 times
                    for (int j = 0; j < 10; j++) {
                        int rowNo = (int) (r.nextDouble() * noOfRows) + 1;
                        log.fine("Moving to row no:" + rowNo);
                        rs.absolute(rowNo);// move to a row
                        // With in a row try accessing columns randomly 10 times
                        for (int k = 1; k < 10; k++) {
                            int columnNo = (int) (r.nextDouble() * noOfColumns) + 1;
                            log.fine("Moving to column no:" + columnNo);
                            String value = rs.getString(columnNo);// get a particular column value
                            if (nonNullColumns.contains(columnNo)) {
                                assertTrue(value != null, "value should not be null");
                            } else {
                                assertTrue(value == null, "value should be null:" + value);
                            }
                        }
                    }
                }
            } finally {
                terminate();
            }

        }
    }

    @Nested
    @Tag("AzureDWTest")
    public class TCStatementIsClosed {
        @Test
        public void testActiveStatement() throws Exception {
            try (Connection conn = DriverManager.getConnection(connectionString);
                    SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {

                // enable isCloseOnCompletion
                try {
                    stmt.closeOnCompletion();
                } catch (Exception e) {
                    log.fine("testCloseOnCompletion threw: " + e.getMessage());
                }

                try {
                    assertEquals(stmt.isClosed(), false, "Wrong return value from Statement.isClosed");
                } catch (UnsupportedOperationException e) {
                    assertEquals(e.getMessage(), TestResource.getResource("R_unexpectedException"), e.getMessage());
                }

                assertEquals(stmt.isClosed(), false, TestResource.getResource("R_statementShouldBeOpened"));
            }
        }

        /**
         * Tests closed statement throw proper exception
         * 
         * @throws Exception
         */
        @Test
        public void testClosedStatement() throws Exception {
            try (Connection conn = DriverManager.getConnection(connectionString);
                    SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {
                stmt.close();

                try {
                    assertEquals(stmt.isClosed(), true, "Wrong return value from Statement.isClosed");
                } catch (UnsupportedOperationException e) {
                    assertEquals(e.getMessage(), TestResource.getResource("R_unexpectedException"), e.getMessage());
                }
            }
        }

        /**
         * Tests closed connection throws proper exception
         * 
         * @throws Exception
         */
        @Test
        public void testClosedConnection() throws Exception {
            try (Connection conn = DriverManager.getConnection(connectionString);
                    SQLServerStatement stmt = (SQLServerStatement) conn.createStatement()) {
                conn.close();

                try {
                    assertEquals(stmt.isClosed(), true, "Wrong return value from Statement.isClosed");
                } catch (UnsupportedOperationException e) {
                    assertEquals(e.getMessage(), TestResource.getResource("R_unexpectedException"), e.getMessage());
                }
            }
        }
    }

    @Nested
    @Tag("AzureDWTest")
    public class TCResultSetIsClosed {

        /**
         * 
         * @throws Exception
         */
        @Test
        public void testActiveResultSet() throws Exception {
            try (Connection conn = DriverManager.getConnection(connectionString);
                    Statement stmt = conn.createStatement()) {

                // enable isCloseOnCompletion
                try {
                    stmt.closeOnCompletion();
                } catch (Exception e) {
                    throw new SQLException(TestResource.getResource("R_unexpectedException"));
                }

                try (SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT 1")) {

                    try {
                        assertEquals(rs.isClosed(), false, "Wrong return value from ResultSet.isClosed");
                    } catch (UnsupportedOperationException e) {
                        assertEquals(e.getMessage(), TestResource.getResource("R_unexpectedException"), e.getMessage());
                    }

                    rs.close();
                }
                assertEquals(stmt.isClosed(), true, TestResource.getResource("R_statementShouldBeClosed"));
            }
        }

        /**
         * Tests closing resultset
         * 
         * @throws Exception
         */
        @Test
        public void testClosedResultSet() throws Exception {
            try (Connection conn = DriverManager.getConnection(connectionString);
                    Statement stmt = conn.createStatement()) {

                // enable isCloseOnCompletion
                try {
                    stmt.closeOnCompletion();
                } catch (Exception e) {
                    throw new SQLException(TestResource.getResource("R_unexpectedException"));
                }

                try (SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT 1")) {
                    rs.close();

                    try {
                        assertEquals(rs.isClosed(), true, "Wrong return value from ResultSet.isClosed");
                    } catch (UnsupportedOperationException e) {
                        assertEquals(e.getMessage(), TestResource.getResource("R_unexpectedException"), e.getMessage());
                    }
                }
                assertEquals(stmt.isClosed(), true, TestResource.getResource("R_statementShouldBeClosed"));
            }
        }

        /**
         * Tests closing statement will close resultset
         * 
         * @throws Exception
         */
        @Test
        public void testClosedStatement() throws Exception {
            try (Connection conn = DriverManager.getConnection(connectionString);
                    Statement stmt = conn.createStatement()) {

                try (SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT 1")) {
                    stmt.close();

                    try {
                        assertEquals(rs.isClosed(), true, "Wrong return value from ResultSet.isClosed");
                    } catch (UnsupportedOperationException e) {
                        assertEquals(e.getMessage(), TestResource.getResource("R_unexpectedException"), e.getMessage());
                    }
                }
            }
        }

        /**
         * Tests closing connection will close resultSet
         * 
         * @throws Exception
         */
        @Test
        public void testClosedConnection() throws Exception {
            try (Connection conn = DriverManager.getConnection(connectionString);
                    Statement stmt = conn.createStatement()) {

                try (SQLServerResultSet rs = (SQLServerResultSet) stmt.executeQuery("SELECT 1")) {
                    conn.close();

                    try {
                        assertEquals(rs.isClosed(), true, "Wrong return value from ResultSet.isClosed");
                    } catch (UnsupportedOperationException e) {
                        assertEquals(e.getMessage(), TestResource.getResource("R_unexpectedException"), e.getMessage());
                    }
                }
            }
        }
    }

    @Nested
    public class TCUpdateCountWithTriggers {
        private static final int NUM_ROWS = 3;

        private final String tableName = RandomUtil.getIdentifier("TCUpdateCountWithTriggersTable1");
        private final String table2Name = RandomUtil.getIdentifier("TCUpdateCountWithTriggersTable2");
        private final String sprocName = RandomUtil.getIdentifier("TCUpdateCountWithTriggersProc");
        private final String triggerName = RandomUtil.getIdentifier("TCUpdateCountWithTriggersTrigger");

        @BeforeEach
        public void setup() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString)) {
                con.setAutoCommit(false);
                try (Statement stmt = con.createStatement()) {

                    try {
                        stmt.executeUpdate("if EXISTS (SELECT * FROM sys.triggers where name = '"
                                + TestUtils.escapeSingleQuotes((triggerName)) + "') drop trigger "
                                + AbstractSQLGenerator.escapeIdentifier(triggerName));
                    } catch (SQLException e) {
                        throw new SQLException(e);
                    }
                    stmt.executeUpdate(
                            "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 INT )");
                    for (int i = 0; i < NUM_ROWS; i++)
                        stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                + " (col1) VALUES (" + i + ")");

                    stmt.executeUpdate("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(table2Name)
                            + " (NAME VARCHAR(100), col2 int identity(1,1) )");
                    stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(table2Name)
                            + " (NAME) VALUES ('BLAH')");
                    stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(table2Name)
                            + " (NAME) VALUES ('FNORD')");
                    stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(table2Name)
                            + " (NAME) VALUES ('EEEP')");

                    stmt.executeUpdate("Create Procedure " + AbstractSQLGenerator.escapeIdentifier(sprocName) + " AS "
                            + "Begin " + "   Update " + AbstractSQLGenerator.escapeIdentifier(table2Name) + " SET "
                            + " NAME = 'Update' Where NAME = 'TEST' " + "Return 0 " + "End");

                    stmt.executeUpdate("CREATE Trigger " + AbstractSQLGenerator.escapeIdentifier(triggerName) + " ON "
                            + AbstractSQLGenerator.escapeIdentifier(tableName) + " FOR DELETE AS " + "Begin "
                            + "Declare @l_retstat Integer " + "Execute @l_retstat = "
                            + AbstractSQLGenerator.escapeIdentifier(sprocName) + " " + "If (@l_retstat <> 0) "
                            + "Begin " + "  Rollback Transaction " + "End " + "End");

                }
                con.commit();
            }
        }

        /**
         * Tests statement with having connection property as lastUpdateCount=true
         * 
         * @throws Exception
         */
        @Test
        public void testLastUpdateCountTrue() throws Exception {

            try (Connection con = DriverManager.getConnection(connectionString + ";lastUpdateCount=true");
                    PreparedStatement ps = con.prepareStatement(
                            "DELETE FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE col1 = ?")) {
                ps.setInt(1, 1);
                int updateCount = ps.executeUpdate();

                // updateCount should be from the DELETE,
                // which should be 1, since there is onw row with the specified column value (1).
                assertEquals(updateCount, 1, "Wrong update count");
            }
        }

        /**
         * Tests statement with having connection property as lastUpdateCount=false
         * 
         * @throws Exception
         */
        @Test
        public void testLastUpdateCountFalse() throws Exception {

            try (Connection con = DriverManager.getConnection(connectionString + ";lastUpdateCount=false");
                    PreparedStatement ps = con.prepareStatement(
                            "DELETE FROM " + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE col1 = ?")) {
                ps.setInt(1, 1);
                int updateCount = ps.executeUpdate();

                // updateCount should be from the UDPATE in the trigger procedure,
                // which should have affected 0 rows since no row satisfies the WHERE clause.
                assertEquals(updateCount, 0, "Wrong update count");
            }
        }

        /**
         * Tests insert, exec and insert in one preparedstatement command
         * 
         * @throws Exception
         */
        @Test
        public void testPreparedStatementInsertExecInsert() throws Exception {

            try (Connection con = DriverManager.getConnection(connectionString + ";lastUpdateCount=true");
                    PreparedStatement ps = con.prepareStatement("INSERT INTO "
                            + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1) VALUES (" + (NUM_ROWS + 1)
                            + "); " + "EXEC " + AbstractSQLGenerator.escapeIdentifier(sprocName) + "; " + "UPDATE "
                            + AbstractSQLGenerator.escapeIdentifier(table2Name) + " SET NAME = 'FISH'")) {

                int updateCount = ps.executeUpdate();

                // updateCount should be from the UPDATE,
                // which should have affected all 3 rows in table2Name.
                assertEquals(updateCount, 3, "Wrong update count");
            }
        }

        /**
         * Tests insert, exec and insert in one statement command
         * 
         * @throws Exception
         */
        @Test
        public void testStatementInsertExecInsert() throws Exception {

            try (Connection con = DriverManager.getConnection(connectionString + ";lastUpdateCount=true");
                    Statement stmt = con.createStatement()) {
                int updateCount = stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " (col1) VALUES (" + (NUM_ROWS + 1) + "); " + "EXEC "
                        + AbstractSQLGenerator.escapeIdentifier(sprocName) + "; " + "UPDATE "
                        + AbstractSQLGenerator.escapeIdentifier(table2Name) + " SET NAME = 'FISH'");

                // updateCount should be from the INSERT,
                // which should have affected 1 (new) row in AbstractSQLGenerator.escapeIdentifier(tableName).
                assertEquals(updateCount, 1, "Wrong update count");
            }
        }

        @AfterEach
        public void terminate() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement();) {
                try {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table2Name), stmt);
                    TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(sprocName), stmt);
                } catch (SQLException e) {
                    fail(e.toString());
                }
            }
        }
    }

    @Nested
    public class TCUpdateCountAfterRaiseError {
        private final String tableName = RandomUtil.getIdentifier("TCUpdateCountAfterRaiseError");
        private final String triggerName = "TCUpdateCountAfterRaiseErrorTrigger";
        private final int NUM_ROWS = 3;
        private final String errorMessage50001InSqlAzure = "Error 50001, severity 17, state 1 was raised, but no message with that error number was found in sys.messages. If error is larger than 50000, make sure the user-defined message is added using sp_addmessage.";

        @BeforeEach
        public void setup() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString)) {
                con.setAutoCommit(false);
                try (Statement stmt = con.createStatement()) {

                    try {
                        stmt.executeUpdate("if EXISTS (SELECT * FROM sys.triggers where name = '"
                                + AbstractSQLGenerator.escapeIdentifier(triggerName) + "') drop trigger "
                                + AbstractSQLGenerator.escapeIdentifier(triggerName));
                    } catch (SQLException e) {
                        System.out.println(e.toString());
                    }
                    stmt.executeUpdate(
                            "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 INT )");
                    for (int i = 0; i < NUM_ROWS; i++)
                        stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                + " (col1) VALUES (" + i + ")");

                    // Skip adding message for 50001 if the target server is SQL Azure, because SQL Azure does not
                    // support
                    // sp_addmessage.
                    try (Connection dbConn = DriverManager.getConnection(connectionString)) {
                        if (isSqlAzure()) {
                            log.fine(
                                    "Because SQL Azure does not support sp_addmessage, 'EXEC sp_addmessage ...' is skipped.");
                        } else {
                            try {
                                stmt.executeUpdate(
                                        "EXEC sp_addmessage @msgnum=50001, @severity=11, @msgtext='MyError'");
                            } catch (SQLException e) {}
                        }
                    }

                    stmt.executeUpdate("CREATE TRIGGER " + AbstractSQLGenerator.escapeIdentifier(triggerName) + " ON "
                            + AbstractSQLGenerator.escapeIdentifier(tableName) + " FOR INSERT AS BEGIN DELETE FROM "
                            + AbstractSQLGenerator.escapeIdentifier(tableName)
                            + " WHERE col1 = 1 RAISERROR(50001, 17, 1) END");
                }
                con.commit();
            }
        }

        /**
         * Tests updateCount method after raising error
         * 
         * @throws Exception
         */
        @Test
        public void testUpdateCountAfterRaiseError() throws Exception {

            try (Connection con = DriverManager.getConnection(connectionString);
                    PreparedStatement pstmt = con
                            .prepareStatement("UPDATE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                    + " SET col1 = 5 WHERE col1 = 2 RAISERROR(50001, 17, 1) SELECT * FROM "
                                    + AbstractSQLGenerator.escapeIdentifier(tableName))) {

                // enable isCloseOnCompletion
                try {
                    pstmt.closeOnCompletion();
                } catch (Exception e) {
                    throw new SQLException(TestResource.getResource("R_unexpectedException"));
                }

                boolean result = pstmt.execute();

                assertEquals(result, false, "First result: should have been an update count");
                assertEquals(pstmt.getUpdateCount(), 1, "First result: Unexpected number of rows affected by UPDATE");

                try {
                    result = pstmt.getMoreResults();
                    assertEquals(true, false, TestResource.getResource("R_expectedExceptionNotThrown"));
                } catch (SQLException e) {
                    String expectedMessage;
                    // SQL Azure does not support sp_addmessage, so the user-defined message cannot be added.
                    if (isSqlAzure()) // SQL Azure
                    {
                        expectedMessage = errorMessage50001InSqlAzure;
                    } else // SQL Server
                    {
                        expectedMessage = "MyError";
                    }
                    assertEquals(e.getMessage(), expectedMessage,
                            "Second result: Unexpected error message from RAISERROR");
                }

                result = pstmt.getMoreResults();
                assertEquals(result, true, "Third result: wrong result type; ResultSet expected");
                assertEquals(pstmt.getUpdateCount(), -1, "Third result: wrong update count");
                try (ResultSet rs = pstmt.getResultSet()) {
                    int rowCount = 0;
                    while (rs.next())
                        ++rowCount;
                    assertEquals(rowCount, NUM_ROWS, "Third result: wrong number of rows returned");
                }
                assertEquals(pstmt.isClosed(), true, TestResource.getResource("R_statementShouldBeClosed"));
            }
        }

        /**
         * Tests updateCount method after error in trigger with having connection property lastUpdateCount = false
         * 
         * @throws Exception
         */
        @Test
        public void testUpdateCountAfterErrorInTriggerLastUpdateCountFalse() throws Exception {

            try (Connection con = DriverManager.getConnection(connectionString + ";lastUpdateCount = false");
                    PreparedStatement pstmt = con.prepareStatement(
                            "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " VALUES (5)")) {

                int updateCount = pstmt.executeUpdate();
                assertEquals(updateCount, 1, "First result: should have been 1 row deleted");
                assertEquals(pstmt.getUpdateCount(), 1, "First result: Wrong return from getUpdateCount");

                boolean result;

                try {
                    result = pstmt.getMoreResults();
                    assertEquals(true, false, TestResource.getResource("R_expectedExceptionNotThrown"));
                } catch (SQLException e) {
                    String expectedMessage;
                    // SQL Azure does not support sp_addmessage, so the user-defined message cannot be added.
                    if (isSqlAzure()) // SQL Azure
                    {
                        expectedMessage = errorMessage50001InSqlAzure;
                    } else // SQL Server
                    {
                        expectedMessage = "MyError";
                    }
                    assertEquals(e.getMessage(), expectedMessage,
                            "Second result: Unexpected error message from RAISERROR");
                }

                result = pstmt.getMoreResults();
                assertEquals(result, false, "Third result: wrong result type; update count expected");
                assertEquals(pstmt.getUpdateCount(), 1, "Third result: wrong number of rows inserted");
                try (ResultSet rs = con.createStatement()
                        .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                    int rowCount = 0;
                    while (rs.next())
                        ++rowCount;
                    assertEquals(rowCount, NUM_ROWS, "Wrong number of rows in table");
                    assertEquals(pstmt.isClosed(), false, TestResource.getResource("R_statementShouldBeOpened"));
                }
            }
        }

        /**
         * Tests updateCount method after error in trigger with having connection property lastUpdateCount = true
         * 
         * @throws Exception
         */
        @Test
        public void testUpdateCountAfterErrorInTriggerLastUpdateCountTrue() throws Exception {

            try (Connection con = DriverManager.getConnection(connectionString + ";lastUpdateCount = true");
                    PreparedStatement pstmt = con.prepareStatement(
                            "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " VALUES (5)")) {

                try {
                    pstmt.executeUpdate();
                    assertEquals(true, false, TestResource.getResource("R_expectedExceptionNotThrown"));
                } catch (SQLException e) {
                    String expectedMessage;
                    // SQL Azure does not support sp_addmessage, so the user-defined message cannot be added.
                    if (isSqlAzure()) // SQL Azure
                    {
                        expectedMessage = errorMessage50001InSqlAzure;
                    } else // SQL Server
                    {
                        expectedMessage = "MyError";
                    }
                    assertEquals(e.getMessage(), expectedMessage,
                            "Second result: Unexpected error message from RAISERROR");
                }

                try (ResultSet rs = pstmt.getResultSet()) {
                    assertEquals(rs, null, "First result: Unexpected update count");

                    assertEquals(pstmt.getUpdateCount(), -1, "First result: Unexpected update count");

                    boolean result = pstmt.getMoreResults();
                    assertEquals(result, false, "Second result: wrong result type; update count expected");
                    assertEquals(pstmt.getUpdateCount(), 1, "Second result: wrong number of rows inserted");
                }

                try (ResultSet rs = con.createStatement()
                        .executeQuery("SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                    int rowCount = 0;
                    while (rs.next())
                        ++rowCount;
                    assertEquals(rowCount, NUM_ROWS, "Wrong number of rows in table");
                }
            }
        }

        @AfterEach
        public void terminate() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement();) {
                try {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                } catch (SQLException e) {
                    fail(e.toString());
                }
            }
        }
    }

    @Nested
    public class TCNocount {
        private final String tableName = RandomUtil.getIdentifier("TCNoCount");

        private static final int NUM_ROWS = 3;

        @BeforeEach
        public void setup() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString)) {
                con.setAutoCommit(false);
                try (Statement stmt = con.createStatement()) {

                    // enable isCloseOnCompletion
                    try {
                        stmt.closeOnCompletion();
                    } catch (Exception e) {
                        throw new SQLException(TestResource.getResource("R_unexpectedException"), e);
                    }
                    stmt.executeUpdate(
                            "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 INT )");
                    for (int i = 0; i < NUM_ROWS; i++)
                        stmt.executeUpdate("INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                + " (col1) VALUES (" + i + ")");

                    assertEquals(stmt.isClosed(), false, TestResource.getResource("R_statementShouldBeOpened"));
                }
                con.commit();
            }
        }

        /**
         * tests no count in execute command
         * 
         * @throws Exception
         */
        @Test
        public void testNoCountWithExecute() throws Exception {
            // Ensure lastUpdateCount=true...
            try (Connection con = DriverManager.getConnection(connectionString + ";lastUpdateCount = true");
                    Statement stmt = con.createStatement();) {

                boolean isResultSet = stmt
                        .execute("set nocount on\n" + "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                                + "(col1) values(" + (NUM_ROWS + 1) + ")\n" + "select 1");

                assertEquals(true, isResultSet, "execute() said first result was an update count");

                try (ResultSet rs = stmt.getResultSet()) {
                    while (rs.next());
                }

                boolean moreResults = stmt.getMoreResults();
                assertEquals(false, moreResults, "next result is a ResultSet?");

                int updateCount = stmt.getUpdateCount();
                assertEquals(-1, updateCount, "only one result was expected...");
            }
        }

        @AfterEach
        public void terminate() throws Exception {
            try (Connection con = DriverManager.getConnection(connectionString);
                    Statement stmt = con.createStatement()) {
                try {
                    TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
                } catch (SQLException e) {
                    fail(e.toString());
                }
            }
        }
    }
}
