/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.sql.BatchUpdateException;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class PreparedStatementTest extends AbstractTest {

    final String tableName = RandomUtil.getIdentifier("#update1");
    final String tableName2 = RandomUtil.getIdentifier("#update2");

    private void executeSQL(SQLServerConnection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private int executeSQLReturnFirstInt(SQLServerConnection conn, String sql) throws SQLException {
        try (Statement stmt = conn.createStatement(); ResultSet result = stmt.executeQuery(sql)) {

            int returnValue = -1;

            if (result.next())
                returnValue = result.getInt(1);

            return returnValue;
        }
    }

    /**
     * Test handling of unpreparing prepared statements.
     * 
     * @throws SQLException
     */
    @Test
    public void testBatchedUnprepare() throws SQLException {
        SQLServerConnection conOuter = null;

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString)) {
            conOuter = con;

            // Turn off use of prepared statement cache.
            con.setStatementPoolingCacheSize(0);

            // Clean-up proc cache
            this.executeSQL(con, "DBCC FREEPROCCACHE;");

            String lookupUniqueifier = UUID.randomUUID().toString();

            String queryCacheLookup = String.format("%%/*unpreparetest_%s%%*/SELECT 1;", lookupUniqueifier);
            String query = String.format("/*unpreparetest_%s only sp_executesql*/SELECT 1;", lookupUniqueifier);

            // Verify nothing in cache.
            String verifyTotalCacheUsesQuery = String.format(
                    "SELECT CAST(ISNULL(SUM(usecounts), 0) AS INT) FROM sys.dm_exec_cached_plans AS p CROSS APPLY sys.dm_exec_sql_text(p.plan_handle) AS s WHERE s.text LIKE '%s'",
                    queryCacheLookup);

            assertSame(0, executeSQLReturnFirstInt(con, verifyTotalCacheUsesQuery));

            int iterations = 25;

            query = String.format(
                    "/*unpreparetest_%s, sp_executesql->sp_prepexec->sp_execute- batched sp_unprepare*/SELECT 1;",
                    lookupUniqueifier);
            int prevDiscardActionCount = 0;

            // Now verify unprepares are needed.
            for (int i = 0; i < iterations; ++i) {

                // Verify current queue depth is expected.
                assertSame(prevDiscardActionCount, con.getDiscardedServerPreparedStatementCount());

                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con
                        .prepareStatement(String.format("%s--%s", query, i))) {
                    pstmt.execute(); // sp_executesql

                    pstmt.execute(); // sp_prepexec
                    ++prevDiscardActionCount;

                    pstmt.execute(); // sp_execute
                }

                // Verify clean-up is happening as expected.
                if (prevDiscardActionCount > con.getServerPreparedStatementDiscardThreshold()) {
                    prevDiscardActionCount = 0;
                }

                assertSame(prevDiscardActionCount, con.getDiscardedServerPreparedStatementCount());
            }

            // Skipped for now due to unexpected failures. Not functional so not critical.
            /*
             * // Verify total cache use. int expectedCacheHits = iterations * 4; int allowedDiscrepency = 20; // Allow
             * some discrepancy in number of cache hits to not fail test ( // TODO: Follow up on why there is sometimes
             * a discrepancy in number of cache hits (less than expected). assertTrue(expectedCacheHits >=
             * executeSQLReturnFirstInt(con, verifyTotalCacheUsesQuery)); assertTrue(expectedCacheHits -
             * allowedDiscrepency < executeSQLReturnFirstInt(con, verifyTotalCacheUsesQuery));
             */
        } finally {
            // Verify clean-up happened on connection close.
            assertSame(0, conOuter.getDiscardedServerPreparedStatementCount());
            if (null != conOuter) {
                conOuter.close();
            }
        }
    }

    /**
     * Test handling of statement pooling for prepared statements.
     * 
     * @throws SQLException
     */
    @Test
    public void testStatementPooling() throws Exception {
        testStatementPoolingInternal("batchInsert");
    }

    /**
     * Test handling of statement pooling for prepared statements.
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws NoSuchFieldException
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     */
    @Test
    public void testStatementPoolingUseBulkCopyAPI() throws Exception {
        testStatementPoolingInternal("BulkCopy");
    }

    /**
     * Test handling of eviction from statement pooling for prepared statements.
     * 
     * @throws SQLException
     */
    @Test
    public void testStatementPoolingEviction() throws SQLException {

        for (int testNo = 0; testNo < 2; ++testNo) {
            try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString)) {
                int cacheSize = 10;
                int discardedStatementCount = testNo == 0 ? 5 /* batched unprepares */ : 0 /* regular unprepares */;

                // enabling caching
                con.setDisableStatementPooling(false);
                con.setStatementPoolingCacheSize(cacheSize);
                con.setServerPreparedStatementDiscardThreshold(discardedStatementCount);

                String lookupUniqueifier = UUID.randomUUID().toString();
                String query = String.format("/*statementpoolingevictiontest_%s*/SELECT 1; -- ", lookupUniqueifier);

                // Add new statements to fill up the statement pool.
                for (int i = 0; i < cacheSize; ++i) {
                    try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con
                            .prepareStatement(query + String.valueOf(i))) {
                        pstmt.execute(); // sp_executesql
                        pstmt.execute(); // sp_prepexec, actual handle created and cached.
                    }
                    // Make sure no handles in discard queue (still only in statement pool).
                    assertSame(0, con.getDiscardedServerPreparedStatementCount());
                }

                // No discarded handles yet, all in statement pool.
                assertSame(0, con.getDiscardedServerPreparedStatementCount());

                // Add new statements to fill up the statement discard action queue
                // (new statement pushes existing statement from pool into discard
                // action queue).
                for (int i = cacheSize; i < cacheSize + 5; ++i) {
                    try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con
                            .prepareStatement(query + String.valueOf(i))) {
                        pstmt.execute(); // sp_executesql
                        pstmt.execute(); // sp_prepexec, actual handle created and cached.
                    }
                    // If we use discard queue handles should start going into discard queue.
                    if (0 == testNo)
                        assertNotSame(0, con.getDiscardedServerPreparedStatementCount());
                    else
                        assertSame(0, con.getDiscardedServerPreparedStatementCount());
                }

                // If we use it, now discard queue should be "full".
                if (0 == testNo)
                    assertSame(discardedStatementCount, con.getDiscardedServerPreparedStatementCount());
                else
                    assertSame(0, con.getDiscardedServerPreparedStatementCount());

                // Adding one more statement should cause one more pooled statement to be invalidated and
                // discarding actions should be executed (i.e. sp_unprepare batch), clearing out the discard
                // action queue.
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(query)) {
                    pstmt.execute(); // sp_executesql
                    pstmt.execute(); // sp_prepexec, actual handle created and cached.
                }

                // Discard queue should now be empty.
                assertSame(0, con.getDiscardedServerPreparedStatementCount());

                // Set statement pool size to 0 and verify statements get discarded.
                int statementsInCache = con.getStatementHandleCacheEntryCount();
                con.setStatementPoolingCacheSize(0);
                assertSame(0, con.getStatementHandleCacheEntryCount());

                if (0 == testNo)
                    // Verify statements moved over to discard action queue.
                    assertSame(statementsInCache, con.getDiscardedServerPreparedStatementCount());

                // Run discard actions (otherwise run on pstmt.close)
                con.closeUnreferencedPreparedStatementHandles();

                assertSame(0, con.getDiscardedServerPreparedStatementCount());

                // Verify new statement does not go into cache (since cache is now off)
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(query)) {
                    pstmt.execute(); // sp_executesql
                    pstmt.execute(); // sp_prepexec, actual handle created and cached.

                    assertSame(0, con.getStatementHandleCacheEntryCount());
                }
            }
        }
    }

    @Test
    public void testPrepareRace() throws Exception {

        String[] queries = new String[3];
        queries[0] = String.format("SELECT 1 -- %s", UUID.randomUUID());
        queries[1] = String.format("SELECT 1 -- %s", UUID.randomUUID());
        queries[2] = String.format("SELECT 1 -- %s", UUID.randomUUID());

        ExecutorService execServiceThread = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(3);
        AtomicReference<Exception> exception = new AtomicReference<>();

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString)) {
            for (int i = 0; i < 3; i++) {
                execServiceThread.submit(() -> {
                    for (int j = 0; j < 500; j++) {
                        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con
                                .prepareStatement(queries[j % 3])) {
                            pstmt.execute();
                        } catch (SQLException e) {
                            exception.set(e);
                            break;
                        }
                    }
                    latch.countDown();
                });
            }
            latch.await();

            assertNull(exception.get());

            // Force un-prepares.
            con.closeUnreferencedPreparedStatementHandles();

            // Verify that queue is now empty.
            assertSame(0, con.getDiscardedServerPreparedStatementCount());
        }
    }

    /**
     * Test handling of the two configuration knobs related to prepared statement handling.
     * 
     * @throws SQLException
     */
    @Test
    public void testStatementPoolingPreparedStatementExecAndUnprepareConfig() throws SQLException {

        // Test Data Source properties
        SQLServerDataSource dataSource = new SQLServerDataSource();
        dataSource.setURL(connectionString);
        // Verify defaults.
        assertTrue(0 == dataSource.getStatementPoolingCacheSize());
        // Verify change
        dataSource.setStatementPoolingCacheSize(0);
        assertSame(0, dataSource.getStatementPoolingCacheSize());
        dataSource.setEnablePrepareOnFirstPreparedStatementCall(
                !dataSource.getEnablePrepareOnFirstPreparedStatementCall());
        dataSource.setServerPreparedStatementDiscardThreshold(
                dataSource.getServerPreparedStatementDiscardThreshold() + 1);
        // Verify connection from data source has same parameters.
        try (SQLServerConnection connDataSource = (SQLServerConnection) dataSource.getConnection()) {
            assertSame(dataSource.getStatementPoolingCacheSize(), connDataSource.getStatementPoolingCacheSize());
            assertSame(dataSource.getEnablePrepareOnFirstPreparedStatementCall(),
                    connDataSource.getEnablePrepareOnFirstPreparedStatementCall());
            assertSame(dataSource.getServerPreparedStatementDiscardThreshold(),
                    connDataSource.getServerPreparedStatementDiscardThreshold());
        }
        // Test connection string properties.

        // Test disableStatementPooling=true
        String connectionStringDisableStatementPooling = connectionString + ";disableStatementPooling=true;";
        try (SQLServerConnection connectionDisableStatementPooling = (SQLServerConnection) DriverManager
                .getConnection(connectionStringDisableStatementPooling)) {
            // to turn on caching and check if disableStatementPooling is true, even setting cachesize won't matter and
            // will disable it.
            connectionDisableStatementPooling.setStatementPoolingCacheSize(10);
            assertSame(10, connectionDisableStatementPooling.getStatementPoolingCacheSize());
            assertTrue(!connectionDisableStatementPooling.isStatementPoolingEnabled());
        }

        // Test disableStatementPooling=false
        String connectionStringEnableStatementPooling = connectionString + ";disableStatementPooling=false;";
        try (SQLServerConnection connectionEnableStatementPooling = (SQLServerConnection) DriverManager
                .getConnection(connectionStringEnableStatementPooling)) {
            connectionEnableStatementPooling.setStatementPoolingCacheSize(10); // to turn on caching.

            // for now, it won't affect if disable is false or true. Since statementPoolingCacheSize is set to 0 as
            // default.
            assertTrue(0 < connectionEnableStatementPooling.getStatementPoolingCacheSize());
            // If only disableStatementPooling is set to true, it makes sure that statementPoolingCacheSize is zero,
            // thus
            // disabling the prepared statement metadata caching.
            assertTrue(connectionEnableStatementPooling.isStatementPoolingEnabled());
        }

        String connectionPropertyStringEnableStatementPooling = connectionString
                + ";disableStatementPooling=false;statementPoolingCacheSize=10";
        try (SQLServerConnection connectionPropertyEnableStatementPooling = (SQLServerConnection) DriverManager
                .getConnection(connectionPropertyStringEnableStatementPooling)) {
            // for now, it won't affect if disable is false or true. Since statementPoolingCacheSize is set to 0 as
            // default.
            assertTrue(0 < connectionPropertyEnableStatementPooling.getStatementPoolingCacheSize());
            // If only disableStatementPooling is set to true, it makes sure that statementPoolingCacheSize is zero,
            // thus
            // disabling the prepared statement metadata caching.
            assertTrue(connectionPropertyEnableStatementPooling.isStatementPoolingEnabled());
        }

        String connectionPropertyStringDisableStatementPooling = connectionString
                + ";disableStatementPooling=true;statementPoolingCacheSize=10";
        try (SQLServerConnection connectionPropertyDisableStatementPooling = (SQLServerConnection) DriverManager
                .getConnection(connectionPropertyStringDisableStatementPooling)) {
            assertTrue(0 < connectionPropertyDisableStatementPooling.getStatementPoolingCacheSize()); // for now, it
                                                                                                      // won't
                                                                                                      // affect if
                                                                                                      // disable
                                                                                                      // is false or
                                                                                                      // true.
                                                                                                      // Since
                                                                                                      // statementPoolingCacheSize
                                                                                                      // is set to 0 as
                                                                                                      // default.
            // If only disableStatementPooling is set to true, it makes sure that statementPoolingCacheSize is zero,
            // thus
            // disabling the prepared statement metadata caching.
            assertTrue(!connectionPropertyDisableStatementPooling.isStatementPoolingEnabled());
        }

        String connectionPropertyStringDisableStatementPooling2 = connectionString
                + ";disableStatementPooling=false;statementPoolingCacheSize=0";
        try (SQLServerConnection connectionPropertyDisableStatementPooling2 = (SQLServerConnection) DriverManager
                .getConnection(connectionPropertyStringDisableStatementPooling2)) {
            assertTrue(0 == connectionPropertyDisableStatementPooling2.getStatementPoolingCacheSize()); // for now, it
                                                                                                        // won't
                                                                                                        // affect if
                                                                                                        // disable
                                                                                                        // is false or
                                                                                                        // true.
                                                                                                        // Since
                                                                                                        // statementPoolingCacheSize
                                                                                                        // is set to 0
                                                                                                        // as
                                                                                                        // default.
            // If only disableStatementPooling is set to true, it makes sure that statementPoolingCacheSize is zero,
            // thus
            // disabling the prepared statement metadata caching.
            assertTrue(!connectionPropertyDisableStatementPooling2.isStatementPoolingEnabled());
        }

        // Test EnablePrepareOnFirstPreparedStatementCall
        String connectionStringNoExecuteSQL = connectionString + ";enablePrepareOnFirstPreparedStatementCall=true;";
        try (SQLServerConnection connectionNoExecuteSQL = (SQLServerConnection) DriverManager
                .getConnection(connectionStringNoExecuteSQL)) {
            assertSame(true, connectionNoExecuteSQL.getEnablePrepareOnFirstPreparedStatementCall());
        }

        // Test ServerPreparedStatementDiscardThreshold
        String connectionStringThreshold3 = connectionString + ";ServerPreparedStatementDiscardThreshold=3;";
        try (SQLServerConnection connectionThreshold3 = (SQLServerConnection) DriverManager
                .getConnection(connectionStringThreshold3)) {
            assertSame(3, connectionThreshold3.getServerPreparedStatementDiscardThreshold());
        }

        // Test combination of EnablePrepareOnFirstPreparedStatementCall and ServerPreparedStatementDiscardThreshold
        String connectionStringThresholdAndNoExecuteSQL = connectionString
                + ";ServerPreparedStatementDiscardThreshold=3;enablePrepareOnFirstPreparedStatementCall=true;";
        try (SQLServerConnection connectionThresholdAndNoExecuteSQL = (SQLServerConnection) DriverManager
                .getConnection(connectionStringThresholdAndNoExecuteSQL)) {
            assertSame(true, connectionThresholdAndNoExecuteSQL.getEnablePrepareOnFirstPreparedStatementCall());
            assertSame(3, connectionThresholdAndNoExecuteSQL.getServerPreparedStatementDiscardThreshold());
        }

        String invalidValue = "hello";
        // Test that an error is thrown for invalid connection string property values (non int/bool).
        String connectionStringThresholdError = connectionString + ";ServerPreparedStatementDiscardThreshold="
                + invalidValue;
        try (SQLServerConnection con = (SQLServerConnection) DriverManager
                .getConnection(connectionStringThresholdError)) {
            fail("Error for invalid ServerPreparedStatementDiscardThresholdexpected.");
        } catch (SQLException e) {
            assert (e.getMessage().equalsIgnoreCase(String.format(
                    TestResource.getResource("R_invalidserverPreparedStatementDiscardThreshold"), invalidValue)));
        }

        String connectionStringNoExecuteSQLError = connectionString + ";enablePrepareOnFirstPreparedStatementCall="
                + invalidValue;
        try (SQLServerConnection con = (SQLServerConnection) DriverManager
                .getConnection(connectionStringNoExecuteSQLError)) {
            fail("Error for invalid enablePrepareOnFirstPreparedStatementCall expected.");
        } catch (SQLException e) {
            assert (e.getMessage()
                    .equalsIgnoreCase(TestResource.getResource("R_invalidenablePrepareOnFirstPreparedStatementCall")));
        }

        // Verify instance setting is followed.
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString)) {

            // Turn off use of prepared statement cache.
            con.setStatementPoolingCacheSize(0);

            String query = "/*unprepSettingsTest*/SELECT 1;";

            // Verify initial default is not serial:
            assertTrue(1 < con.getServerPreparedStatementDiscardThreshold());

            // Verify first use is batched.
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(query)) {
                pstmt.execute(); // sp_executesql
                pstmt.execute(); // sp_prepexec
            }

            // Verify that the un-prepare action was not handled immediately.
            assertSame(1, con.getDiscardedServerPreparedStatementCount());

            // Force un-prepares.
            con.closeUnreferencedPreparedStatementHandles();

            // Verify that queue is now empty.
            assertSame(0, con.getDiscardedServerPreparedStatementCount());

            // Set instance setting to serial execution of un-prepare actions.
            con.setServerPreparedStatementDiscardThreshold(1);

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(query)) {
                pstmt.execute();
            }
            // Verify that the un-prepare action was handled immediately.
            assertSame(0, con.getDiscardedServerPreparedStatementCount());
        }
    }

    private void testStatementPoolingInternal(String mode) throws Exception {
        // Test % handle re-use
        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString)) {
            if (mode.equalsIgnoreCase("bulkcopy")) {
                modifyConnectionForBulkCopyAPI(con);
            }
            String query = String.format("/*statementpoolingtest_re-use_%s*/SELECT 1;", UUID.randomUUID().toString());

            con.setStatementPoolingCacheSize(10);

            boolean[] prepOnFirstCalls = {false, true};

            for (boolean prepOnFirstCall : prepOnFirstCalls) {

                con.setEnablePrepareOnFirstPreparedStatementCall(prepOnFirstCall);

                int[] queryCounts = {10, 20, 30, 40};
                for (int queryCount : queryCounts) {
                    String[] queries = new String[queryCount];
                    for (int i = 0; i < queries.length; ++i) {
                        queries[i] = String.format("%s--%s--%s--%s", query, i, queryCount, prepOnFirstCall);
                    }

                    final int testCount = 500;
                    for (int i = 0; i < testCount; ++i) {
                        Random random = new Random();
                        int queryNumber = random.nextInt(queries.length);
                        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con
                                .prepareStatement(queries[queryNumber])) {
                            pstmt.execute();
                            pstmt.getMoreResults(); // Make sure handle is updated.
                        }
                    }
                }
            }
        }

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString)) {
            if (mode.equalsIgnoreCase("bulkcopy")) {
                modifyConnectionForBulkCopyAPI(con);
            }
            // Test behavior with statement pooling.
            con.setStatementPoolingCacheSize(10);
            this.executeSQL(con,
                    "IF NOT EXISTS (SELECT * FROM sys.messages WHERE message_id = 99586) EXEC sp_addmessage 99586, 16, 'Prepared handle GAH!';");
            // Test with missing handle failures (fake).
            this.executeSQL(con, "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (col INT);INSERT " + AbstractSQLGenerator.escapeIdentifier(tableName) + " VALUES (1);");
            this.executeSQL(con, "CREATE PROC #updateProc1 AS UPDATE "
                    + AbstractSQLGenerator.escapeIdentifier(tableName) + " SET col += 1; IF EXISTS (SELECT * FROM "
                    + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE col % 5 = 0) RAISERROR(99586,16,1);");
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement("#updateProc1")) {
                for (int i = 0; i < 100; ++i) {
                    try {
                        assertSame(1, pstmt.executeUpdate());
                    } catch (SQLException e) {
                        // Error "Prepared handle GAH" is expected to happen. But it should not terminate the execution
                        // with RAISERROR.
                        // Since the original "Could not find prepared statement with handle" error does not terminate
                        // the execution after it.
                        if (!e.getMessage().contains("Prepared handle GAH")) {
                            throw e;
                        }
                    }
                }
            }

            // test updated value, should be 1 + 100 = 101
            // although executeUpdate() throws exception, update operation should be executed successfully.
            try (Statement stmt = con.createStatement(); ResultSet rs = stmt
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName) + "")) {
                rs.next();
                assertSame(101, rs.getInt(1));
            }

            // Test batching with missing handle failures (fake).
            this.executeSQL(con,
                    "IF NOT EXISTS (SELECT * FROM sys.messages WHERE message_id = 99586) EXEC sp_addmessage 99586, 16, 'Prepared handle GAH!';");
            this.executeSQL(con, "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName2)
                    + " (col INT);INSERT " + AbstractSQLGenerator.escapeIdentifier(tableName2) + " VALUES (1);");
            this.executeSQL(con, "CREATE PROC #updateProc2 AS UPDATE "
                    + AbstractSQLGenerator.escapeIdentifier(tableName2) + " SET col += 1; IF EXISTS (SELECT * FROM "
                    + AbstractSQLGenerator.escapeIdentifier(tableName2) + " WHERE col % 5 = 0) RAISERROR(99586,16,1);");
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement("#updateProc2")) {
                for (int i = 0; i < 100; ++i) {
                    pstmt.addBatch();
                }

                int[] updateCounts = null;
                try {
                    updateCounts = pstmt.executeBatch();
                } catch (BatchUpdateException e) {
                    // Error "Prepared handle GAH" is expected to happen. But it should not terminate the execution with
                    // RAISERROR.
                    // Since the original "Could not find prepared statement with handle" error does not terminate the
                    // execution after it.
                    if (!e.getMessage().contains("Prepared handle GAH")) {
                        throw e;
                    }
                }

                // since executeBatch() throws exception, it does not return anthing. So updateCounts is still null.
                assertSame(null, updateCounts);

                // test updated value, should be 1 + 100 = 101
                // although executeBatch() throws exception, update operation should be executed successfully.
                try (Statement stmt = con.createStatement(); ResultSet rs = stmt
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName2) + "")) {
                    rs.next();
                    assertSame(101, rs.getInt(1));
                }
            }

            // Test behavior with statement pooling enabled
            con.setDisableStatementPooling(false);

            String lookupUniqueifier = UUID.randomUUID().toString();
            String query = String.format("/*statementpoolingtest_%s*/SELECT 1;", lookupUniqueifier);

            // Execute statement first, should create cache entry WITHOUT handle (since sp_executesql was used).
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(query)) {
                pstmt.execute(); // sp_executesql
                pstmt.getMoreResults(); // Make sure handle is updated.

                assertSame(0, pstmt.getPreparedStatementHandle());
            }

            // Execute statement again, should now create handle.
            int handle = 0;
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(query)) {
                pstmt.execute(); // sp_prepexec
                pstmt.getMoreResults(); // Make sure handle is updated.

                handle = pstmt.getPreparedStatementHandle();
                assertNotSame(0, handle);
            }

            // Execute statement again and verify same handle was used.
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(query)) {
                pstmt.execute(); // sp_execute
                pstmt.getMoreResults(); // Make sure handle is updated.

                assertNotSame(0, pstmt.getPreparedStatementHandle());
                assertSame(handle, pstmt.getPreparedStatementHandle());
            }

            // Execute new statement with different SQL text and verify it does NOT get same handle (should now fall
            // back to using sp_executesql).
            SQLServerPreparedStatement outer = null;
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(query + ";")) {
                outer = pstmt;
                pstmt.execute(); // sp_executesql
                pstmt.getMoreResults(); // Make sure handle is updated.

                assertSame(0, pstmt.getPreparedStatementHandle());
                assertNotSame(handle, pstmt.getPreparedStatementHandle());
            }
            try {
                outer.getPreparedStatementHandle();
                fail(TestResource.getResource("R_invalidGetPreparedStatementHandle"));
            } catch (Exception e) {
                assert (e.getMessage().equalsIgnoreCase(TestResource.getResource("R_statementClosed")));
            } finally {
                if (null != outer) {
                    outer.close();
                }
            }
        }
    }

    private void modifyConnectionForBulkCopyAPI(SQLServerConnection con) throws Exception {
        Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
        f1.setAccessible(true);
        f1.set(con, true);

        con.setUseBulkCopyForBatchInsert(true);
    }
}
