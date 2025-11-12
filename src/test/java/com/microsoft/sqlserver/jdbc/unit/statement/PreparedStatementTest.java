/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.JDBCType;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.rowset.serial.SerialClob;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


@RunWith(JUnitPlatform.class)
public class PreparedStatementTest extends AbstractTest {

    final static String tableName = RandomUtil.getIdentifier("tableTestStatementPoolingInternal1");
    final static String tableName2 = RandomUtil.getIdentifier("tableTestStatementPoolingInternal2");
    final static String tableName3 = RandomUtil.getIdentifier("tableTestPreparedStatementWithSpPrepare");
    final static String tableName4 = RandomUtil.getIdentifier("tableTestPreparedStatementWithMultipleParams");
    final static String tableName5 = RandomUtil.getIdentifier("tableTestPreparedStatementWithTimestamp");

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @BeforeEach
    public void testInit() throws Exception {
        dropTables();
    }

    @AfterEach
    public void terminateVariation() throws Exception {
        dropTables();
    }

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

    @Test
    public void testSpPrepareConfigurationConnectionPropValues() throws SQLException {
        String connectionStringPrepare = connectionString + ";prepareMethod=prepare;";
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionStringPrepare)) {
            assertEquals("prepare", conn.getPrepareMethod());
        }

        String connectionStringExec = connectionString + ";prepareMethod=exec;";
        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionStringExec)) {
            assertEquals("exec", conn.getPrepareMethod());
        }

        try (SQLServerConnection conn = (SQLServerConnection) getConnection()) {
            assertEquals("prepexec", conn.getPrepareMethod()); // default is prepexec
        }
    }

    @Test
    public void testPreparedStatementWithSpPrepare() throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName3)
                + " (c1_nchar, c2_int) values (?, ?)";

        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            con.setPrepareMethod("prepare"); // Use sp_prepare rather than sp_prepexec

            executeSQL(con, "create table " + AbstractSQLGenerator.escapeIdentifier(tableName3)
                    + " (c1_nchar nchar(512), c2_int integer)");

            try (SQLServerPreparedStatement ps = (SQLServerPreparedStatement) con.prepareStatement(sql)) {
                ps.setString(1, "test");
                ps.setInt(2, 0);
                ps.executeUpdate();
                ps.executeUpdate(); // Takes sp_prepare path
                ps.executeUpdate();
            }
        }
    }

    @Test
    public void testPreparedStatementWithExec() throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName4)
                + " (c1_nchar, c2_int) values (?, ?)";

        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            con.setPrepareMethod("exec"); // Use exec method

            executeSQL(con, "create table " + AbstractSQLGenerator.escapeIdentifier(tableName4)
                    + " (c1_nchar nchar(512), c2_int integer)");

            try (SQLServerPreparedStatement ps = (SQLServerPreparedStatement) con.prepareStatement(sql)) {
                ps.setString(1, "test");
                ps.setInt(2, 0);
                ps.executeUpdate();
                ps.executeUpdate();
                ps.executeUpdate();
            }
        }
    }
    
    @ParameterizedTest
    @ValueSource(strings = {"prepexec", "prepare", "exec"})
    public void testPreparedStatementWithDifferentPrepareMethods(String prepareMethod) throws SQLException {
        String tableName = RandomUtil.getIdentifier("testTablePrepareMethod");
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                + " (c1_int, c2_nvarchar, c3_datetime, c4_bit) values (?, ?, ?, ?)";

        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            con.setPrepareMethod(prepareMethod);

            executeSQL(con, "create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (c1_int int, c2_nvarchar nvarchar(100), c3_datetime datetime2, c4_bit bit)");

            try (SQLServerPreparedStatement ps = (SQLServerPreparedStatement) con.prepareStatement(sql)) {
                // Test with different data types
                ps.setInt(1, 123);
                ps.setString(2, "test string");
                ps.setTimestamp(3, java.sql.Timestamp.valueOf("2023-01-01 12:30:45.123"));
                ps.setBoolean(4, true);
                ps.executeUpdate();
                
                ps.setInt(1, 456);
                ps.setString(2, "second test");
                ps.setTimestamp(3, java.sql.Timestamp.valueOf("2023-01-02 13:31:46.456"));
                ps.setBoolean(4, false);
                ps.executeUpdate(); // Second execution should follow the prepare method path
                
                ps.setInt(1, 789);
                ps.setString(2, "third test");
                ps.setTimestamp(3, java.sql.Timestamp.valueOf("2023-01-03 14:32:47.789"));
                ps.setBoolean(4, true);
                ps.executeUpdate(); // Third execution should also follow the prepare method path
            }

            // Verify data was inserted correctly
            try (Statement stmt = con.createStatement(); 
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + AbstractSQLGenerator.escapeIdentifier(tableName))) {
                rs.next();
                assertEquals(3, rs.getInt(1), "Should have 3 rows inserted");
            }

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), con.createStatement());
        }
    }
    
    @Test
    void testDatabaseQueryMetaData() throws SQLException {
        try (Connection connection = getConnection()) {
            try (SQLServerPreparedStatement stmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "select 1 as \"any questions ???\"")) {
                ResultSetMetaData metaData = stmt.getMetaData();
                String actualLabel = metaData.getColumnLabel(1);
                String actualName = metaData.getColumnName(1);

                String expected = "any questions ???";
                assertEquals(expected, actualLabel, "Column label should match the expected value");
                assertEquals(expected, actualName, "Column name should match the expected value");
            }
        } catch (SQLException e) {
            e.printStackTrace();
            fail("SQLException occurred during test: " + e.getMessage());
        }
    }

    @Test
    public void testPreparedStatementParamNameSpacingWithMultipleParams() throws SQLException {
        int paramNameCount = 105;

        StringBuilder insertSql = new StringBuilder(
                "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName4) + "values(");

        for (int i = 1; i <= paramNameCount; i++) {
            insertSql.append(i % 10);

            if (i != paramNameCount) {
                insertSql.append(",");
            } else {
                insertSql.append(")");
            }
        }

        StringBuilder createTableSql = new StringBuilder(
                "create table " + AbstractSQLGenerator.escapeIdentifier(tableName4) + "(");

        for (int i = 1; i <= paramNameCount; i++) {
            createTableSql.append("c" + i + " char(1)");

            if (i != paramNameCount) {
                createTableSql.append(",");
            } else {
                createTableSql.append(")");
            }
        }

        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            executeSQL(con, createTableSql.toString());
            executeSQL(con, insertSql.toString());

            // There are no typos in the queries eg. The 'c1=?and' is not a typo. We are testing the spacing, or lack of spacing.
            // The driver should automatically space out the params eg. 'c1=?and' becomes 'c1= ? and' in the final string we send to the server.

            // Testing with less than 10 params
            String sql1 = "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName4) + " where c1=?and c2=?";

            // Testing with number of params between 10 and 100
            String sql2 = "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName4)
                    + " where c1=?and c2=? and c3=?and c4=? and c5=? and c6=? and c7=? and c8=? and c9=? and c10=? and c11=? and c12=?";

            // Testing with more than 100 params
            StringBuilder sql3 = new StringBuilder(
                    "select * from " + AbstractSQLGenerator.escapeIdentifier(tableName4) + " where c1=?and ");

            for (int i = 2; i <= paramNameCount; i++) {
                sql3.append("c" + i + "=?");

                if (i != paramNameCount) {
                    sql3.append(" and ");
                }
            }

            try (SQLServerPreparedStatement ps = (SQLServerPreparedStatement) con.prepareStatement(sql1)) {
                ps.setString(1, "1");
                ps.setString(2, "2");
                ps.executeQuery();
            }

            try (SQLServerPreparedStatement ps = (SQLServerPreparedStatement) con.prepareStatement(sql2)) {
                ps.setString(1, "1");
                ps.setString(2, "2");
                ps.setString(3, "3");
                ps.setString(4, "4");
                ps.setString(5, "5");
                ps.setString(6, "6");
                ps.setString(7, "7");
                ps.setString(8, "8");
                ps.setString(9, "9");
                ps.setString(10, "0");
                ps.setString(11, "1");
                ps.setString(12, "2");
                ps.executeQuery();
            }

            try (SQLServerPreparedStatement ps = (SQLServerPreparedStatement) con.prepareStatement(sql3.toString())) {
                ps.setString(1, "1");
                for (int i = 2; i <= paramNameCount; i++) {
                    ps.setString(i, Integer.toString(i % 10));
                }

                ps.executeQuery();
            }
        }
    }

    @Test
    public void testPreparedStatementPoolEvictionWithSpPrepare() throws SQLException {
        int cacheSize = 3;
        int discardStatementCount = 3;

        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {

            con.setPrepareMethod("prepare"); // Use sp_prepare rather than sp_prepexec
            con.setDisableStatementPooling(false);
            con.setStatementPoolingCacheSize(cacheSize);
            con.setServerPreparedStatementDiscardThreshold(discardStatementCount);

            String query = "select 1 --";

            for (int i = 0; i < cacheSize; i++) {
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(query + i)) {
                    pstmt.execute(); // sp_executesql
                    pstmt.execute(); // sp_prepare and sp_execute, handle created and cached
                }
                // No handles in discard queue
                assertSame(0, con.getDiscardedServerPreparedStatementCount());
            }

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con
                    .prepareStatement(query + cacheSize)) {
                pstmt.execute(); // sp_executesql
                pstmt.execute(); // sp_prepare and sp_execute, handle created and cached
            }
            // Handle should be discarded
            assertSame(1, con.getDiscardedServerPreparedStatementCount());
        }
    }

    /**
     * Test handling of unpreparing prepared statements.
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    public void testBatchedUnprepare() throws SQLException {
        SQLServerConnection conOuter = null;

        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            conOuter = con;

            // Turn off use of prepared statement cache.
            con.setStatementPoolingCacheSize(0);

            // Clean-up proc cache
            try (Statement stmt = con.createStatement()) {
                TestUtils.freeProcCache(stmt);
            }

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
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
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
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
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
            try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
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

        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
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

    @Test
    public void testTimestampStringTimeZoneFormat() throws SQLException {
        String SELECT_SQL = "SELECT id, created_date, deleted_date FROM "
                + AbstractSQLGenerator.escapeIdentifier(tableName5) + " WHERE id = ?";
        String INSERT_SQL = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName5)
                + " (id, created_date, deleted_date) VALUES (?, ?, ?)";
        String DATE_FORMAT_WITH_Z = "yyyy-MM-dd'T'HH:mm:ss'Z'";
        SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_WITH_Z);

        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            executeSQL(con, "create table " + AbstractSQLGenerator.escapeIdentifier(tableName5)
                    + "(id int, created_date datetime2, deleted_date datetime2)");
        }

        try (PreparedStatement selectStatement = connection.prepareCall(SELECT_SQL);
                PreparedStatement insertStatement = connection.prepareCall(INSERT_SQL);) {
            Date createdDate = Date.from(Instant.parse("2024-01-16T05:12:00Z"));
            Date deletedDate = Date.from(Instant.parse("2024-01-16T06:34:00Z"));
            int id = 1;

            insertStatement.setInt(1, id);
            insertStatement.setObject(2, sdf.format(createdDate.getTime()), Types.TIMESTAMP);
            insertStatement.setObject(3, sdf.format(deletedDate.getTime()), Types.TIMESTAMP);

            insertStatement.executeUpdate();

            selectStatement.setInt(1, id);

            try (ResultSet result = selectStatement.executeQuery()) {
                result.next();
                Assertions.assertEquals(id, result.getInt("id"));
                Assertions.assertEquals(createdDate, new Date(result.getTimestamp("created_date").getTime()));
                Assertions.assertEquals(deletedDate, new Date(result.getTimestamp("deleted_date").getTime()));
            }
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
        try (SQLServerConnection connectionDisableStatementPooling = (SQLServerConnection) PrepUtil
                .getConnection(connectionStringDisableStatementPooling)) {
            // to turn on caching and check if disableStatementPooling is true, even setting cachesize won't matter and
            // will disable it.
            connectionDisableStatementPooling.setStatementPoolingCacheSize(10);
            assertSame(10, connectionDisableStatementPooling.getStatementPoolingCacheSize());
            assertTrue(!connectionDisableStatementPooling.isStatementPoolingEnabled());
        }

        // Test disableStatementPooling=false
        String connectionStringEnableStatementPooling = connectionString + ";disableStatementPooling=false;";
        try (SQLServerConnection connectionEnableStatementPooling = (SQLServerConnection) PrepUtil
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
        try (SQLServerConnection connectionPropertyEnableStatementPooling = (SQLServerConnection) PrepUtil
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
        try (SQLServerConnection connectionPropertyDisableStatementPooling = (SQLServerConnection) PrepUtil
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
        try (SQLServerConnection connectionPropertyDisableStatementPooling2 = (SQLServerConnection) PrepUtil
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
        try (SQLServerConnection connectionNoExecuteSQL = (SQLServerConnection) PrepUtil
                .getConnection(connectionStringNoExecuteSQL)) {
            assertSame(true, connectionNoExecuteSQL.getEnablePrepareOnFirstPreparedStatementCall());
        }

        // Test ServerPreparedStatementDiscardThreshold
        String connectionStringThreshold3 = connectionString + ";ServerPreparedStatementDiscardThreshold=3;";
        try (SQLServerConnection connectionThreshold3 = (SQLServerConnection) PrepUtil
                .getConnection(connectionStringThreshold3)) {
            assertSame(3, connectionThreshold3.getServerPreparedStatementDiscardThreshold());
        }

        // Test combination of EnablePrepareOnFirstPreparedStatementCall and ServerPreparedStatementDiscardThreshold
        String connectionStringThresholdAndNoExecuteSQL = connectionString
                + ";ServerPreparedStatementDiscardThreshold=3;enablePrepareOnFirstPreparedStatementCall=true;";
        try (SQLServerConnection connectionThresholdAndNoExecuteSQL = (SQLServerConnection) PrepUtil
                .getConnection(connectionStringThresholdAndNoExecuteSQL)) {
            assertSame(true, connectionThresholdAndNoExecuteSQL.getEnablePrepareOnFirstPreparedStatementCall());
            assertSame(3, connectionThresholdAndNoExecuteSQL.getServerPreparedStatementDiscardThreshold());
        }

        String invalidValue = "hello";
        // Test that an error is thrown for invalid connection string property values (non int/bool).
        String connectionStringThresholdError = connectionString + ";ServerPreparedStatementDiscardThreshold="
                + invalidValue;
        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(connectionStringThresholdError)) {
            fail("Error for invalid ServerPreparedStatementDiscardThresholdexpected.");
        } catch (SQLException e) {
            assert (e.getMessage().equalsIgnoreCase(String.format(
                    TestResource.getResource("R_invalidserverPreparedStatementDiscardThreshold"), invalidValue)));
        }

        String connectionStringNoExecuteSQLError = connectionString + ";enablePrepareOnFirstPreparedStatementCall="
                + invalidValue;
        try (SQLServerConnection con = (SQLServerConnection) PrepUtil
                .getConnection(connectionStringNoExecuteSQLError)) {
            fail("Error for invalid enablePrepareOnFirstPreparedStatementCall expected.");
        } catch (SQLException e) {
            assert (e.getMessage()
                    .equalsIgnoreCase(TestResource.getResource("R_invalidenablePrepareOnFirstPreparedStatementCall")));
        }

        // Verify instance setting is followed.
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {

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

    /**
     * Test various setter methods of PreparedStatement.
     */
    @Test
    @Tag(Constants.CodeCov)
    @SuppressWarnings("deprecation")
    public void testPreparedStatementSetterMethods()
            throws SQLException, UnsupportedEncodingException, MalformedURLException {
        final String testTableName = RandomUtil.getIdentifier("testSetterMethods");

        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            String createTableSql = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(testTableName) + " ("
                    + "id INT, ascii_stream_col1 VARCHAR(MAX), ascii_stream_col2 VARCHAR(MAX), ascii_stream_col3 VARCHAR(MAX), "
                    + "binary_stream_col1 VARBINARY(MAX), binary_stream_col2 VARBINARY(MAX), character_stream_col1 NVARCHAR(MAX), "
                    + "character_stream_col2 NVARCHAR(MAX), character_stream_col3 NVARCHAR(MAX), ncharacter_stream_col1 NVARCHAR(MAX), "
                    + "ncharacter_stream_col2 NVARCHAR(MAX), decimal_col DECIMAL(18,2), money_col MONEY, smallmoney_col SMALLMONEY, "
                    + "boolean_col BIT, byte_col TINYINT, bytes_col VARBINARY(50), guid_col UNIQUEIDENTIFIER, "
                    + "double_col FLOAT, float_col REAL, int_col INT, long_col BIGINT, short_col SMALLINT, "
                    + "string_col NVARCHAR(100), nstring_col NVARCHAR(100), time_col TIME(3), timestamp_col DATETIME2(3), "
                    + "datetimeoffset_col DATETIMEOFFSET(3), datetime_col DATETIME, smalldatetime_col SMALLDATETIME, "
                    + "date_col DATE, time_cal_col TIME, timestamp_cal_col DATETIME2, date_cal_col DATE, "
                    + "date_cal_force_col DATE, timestamp_cal_force_col DATETIME2, blob_col VARBINARY(MAX), "
                    + "blob_stream_col VARBINARY(MAX), clob_col NVARCHAR(MAX), clob_reader_col NVARCHAR(MAX), "
                    + "clob_reader_length_col NVARCHAR(MAX), nclob_col NVARCHAR(MAX), nclob_reader_col NVARCHAR(MAX), "
                    + "nclob_reader_length_col NVARCHAR(MAX), xml_col XML" + ")";

            executeSQL(con, createTableSql);

            String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(testTableName)
                    + " (id, ascii_stream_col1, ascii_stream_col2, ascii_stream_col3, binary_stream_col1, binary_stream_col2, "
                    + "character_stream_col1, character_stream_col2, character_stream_col3, "
                    + "ncharacter_stream_col1, ncharacter_stream_col2, "
                    + "decimal_col, money_col, smallmoney_col, "
                    + "boolean_col, byte_col, bytes_col, guid_col, double_col, float_col, int_col, long_col, "
                    + "short_col, string_col, nstring_col, time_col, timestamp_col, datetimeoffset_col, "
                    + "datetime_col, smalldatetime_col, date_col, time_cal_col, timestamp_cal_col, date_cal_col, "
                    + "date_cal_force_col, timestamp_cal_force_col, "
                    + "blob_col, blob_stream_col, clob_col, clob_reader_col, clob_reader_length_col, "
                    + "nclob_col, nclob_reader_col, nclob_reader_length_col, xml_col) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(insertSql)) {

                pstmt.setInt(1, 1);

                String asciiData1 = "Test ASCII Stream Data 1";
                ByteArrayInputStream asciiStream1 = new ByteArrayInputStream(asciiData1.getBytes("ASCII"));
                pstmt.setAsciiStream(2, asciiStream1);

                String asciiData2 = "Test ASCII Stream Data 2";
                ByteArrayInputStream asciiStream2 = new ByteArrayInputStream(asciiData2.getBytes("ASCII"));
                pstmt.setAsciiStream(3, asciiStream2, asciiData2.length());

                String asciiData3 = "Test ASCII Stream Data 3";
                ByteArrayInputStream asciiStream3 = new ByteArrayInputStream(asciiData3.getBytes("ASCII"));
                pstmt.setAsciiStream(4, asciiStream3, (long) asciiData3.length());

                byte[] binaryData1 = { 1, 2, 3, 4, 5 };
                ByteArrayInputStream binaryStream1 = new ByteArrayInputStream(binaryData1);
                pstmt.setBinaryStream(5, binaryStream1);

                byte[] binaryData2 = { 6, 7, 8, 9, 10 };
                ByteArrayInputStream binaryStream2 = new ByteArrayInputStream(binaryData2);
                pstmt.setBinaryStream(6, binaryStream2, (long) binaryData2.length);

                String charData1 = "Test Character Stream 1";
                java.io.StringReader charReader1 = new java.io.StringReader(charData1);
                pstmt.setCharacterStream(7, charReader1);

                String charData2 = "Test Character Stream 2";
                java.io.StringReader charReader2 = new java.io.StringReader(charData2);
                pstmt.setCharacterStream(8, charReader2, charData2.length());

                String charData3 = "Test Character Stream 3";
                java.io.StringReader charReader3 = new java.io.StringReader(charData3);
                pstmt.setCharacterStream(9, charReader3, (long) charData3.length());

                String ncharData1 = "Test NCharacter Stream 1";
                java.io.StringReader ncharReader1 = new java.io.StringReader(ncharData1);
                pstmt.setNCharacterStream(10, ncharReader1);

                String ncharData2 = "Test NCharacter Stream 2";
                java.io.StringReader ncharReader2 = new java.io.StringReader(ncharData2);
                pstmt.setNCharacterStream(11, ncharReader2, (long) ncharData2.length());

                BigDecimal decimalValue = new BigDecimal("123.45");
                pstmt.setBigDecimal(12, decimalValue, 18, 2, false);

                BigDecimal moneyValue = new BigDecimal("999.99");
                pstmt.setMoney(13, moneyValue, false);

                BigDecimal smallMoneyValue = new BigDecimal("99.99");
                pstmt.setSmallMoney(14, smallMoneyValue, false);

                pstmt.setBoolean(15, true, false);

                pstmt.setByte(16, (byte) 100, false);

                byte[] bytesValue = { 10, 20, 30 };
                pstmt.setBytes(17, bytesValue, false);

                String guidValue = "12345678-1234-1234-1234-123456789ABC";
                pstmt.setUniqueIdentifier(18, guidValue, false);

                pstmt.setDouble(19, 123.456, false);

                pstmt.setFloat(20, 78.9f, false);

                pstmt.setInt(21, 42, false);

                pstmt.setLong(22, 9876543210L, false);

                pstmt.setShort(23, (short) 123, false);

                pstmt.setString(24, "Test String", false);

                pstmt.setNString(25, "Test NString", false);

                java.sql.Time timeValue = java.sql.Time.valueOf("12:30:45");
                pstmt.setTime(26, timeValue, 3, false);

                java.sql.Timestamp timestampValue = java.sql.Timestamp.valueOf("2024-01-15 12:30:45.123");
                pstmt.setTimestamp(27, timestampValue, 3, false);

                microsoft.sql.DateTimeOffset dateTimeOffsetValue = microsoft.sql.DateTimeOffset.valueOf(timestampValue,
                        0);
                pstmt.setDateTimeOffset(28, dateTimeOffsetValue, 3, false);

                pstmt.setDateTime(29, timestampValue, false);

                pstmt.setSmallDateTime(30, timestampValue, false);

                java.sql.Date dateValue = java.sql.Date.valueOf("2024-01-15");
                pstmt.setDate(31, dateValue);

                java.util.Calendar cal = java.util.Calendar.getInstance();
                pstmt.setTime(32, timeValue, cal);

                pstmt.setTimestamp(33, timestampValue, cal);

                pstmt.setDate(34, dateValue, cal);

                pstmt.setDate(35, dateValue, cal, false);

                pstmt.setTimestamp(36, timestampValue, cal, false);

                byte[] blobData = "Test Blob Data".getBytes();

                ByteArrayInputStream blobStream = new ByteArrayInputStream(blobData);
                pstmt.setBlob(37, blobStream);
                pstmt.setBlob(38, blobStream, (long) blobData.length);

                String clobData = "Test Clob Data";
                java.sql.Clob clobValue = new javax.sql.rowset.serial.SerialClob(clobData.toCharArray());
                pstmt.setClob(39, clobValue);

                SerialClob clobReader1 = new javax.sql.rowset.serial.SerialClob("ascii-stream".toCharArray());
                pstmt.setClob(40, clobReader1);

                java.io.StringReader clobReader2 = new java.io.StringReader(clobData);
                pstmt.setClob(41, clobReader2, (long) clobData.length());

                String nclobData = "Test NClob Data";
                java.sql.NClob nclobValue = con.createNClob();
                nclobValue.setString(1, nclobData);
                pstmt.setNClob(42, nclobValue);

                java.io.StringReader nclobReader1 = new java.io.StringReader(nclobData);
                pstmt.setNClob(43, nclobReader1);

                java.io.StringReader nclobReader2 = new java.io.StringReader(nclobData);
                pstmt.setNClob(44, nclobReader2, (long) nclobData.length());

                String xmlData = "<root><test>XML Data</test></root>";
                java.sql.SQLXML sqlXmlValue = con.createSQLXML();
                sqlXmlValue.setString(xmlData);
                pstmt.setSQLXML(45, sqlXmlValue);

                try {
                    pstmt.setRef(1, null);
                    fail("setRef should throw UnsupportedOperationException");
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage().contains("This operation is not supported."));
                }

                try {
                    pstmt.setArray(1, null);
                    fail("setArray should throw UnsupportedOperationException");
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage().contains("This operation is not supported."));
                }

                try {
                    pstmt.setRowId(1, null);
                    fail("setRowId should throw UnsupportedOperationException");
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage().contains("This operation is not supported."));
                }

                try {
                    pstmt.setUnicodeStream(1, null, 0);
                    fail("setUnicodeStream should throw UnsupportedOperationException");
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage().contains("This operation is not supported."));
                }

                try {
                    java.net.URL testUrl = new java.net.URL("http://example.com");
                    pstmt.setURL(1, testUrl);
                    fail("setURL should throw UnsupportedOperationException");
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage().contains("This operation is not supported."));
                }

                byte[] blobStreamData = "Test Blob Stream Data".getBytes();
                ByteArrayInputStream blobInputStream = new ByteArrayInputStream(blobStreamData);
                pstmt.setBlob(37, blobInputStream);

                String clobReaderData = "Test Clob Reader Data";
                java.io.StringReader clobStringReader = new java.io.StringReader(clobReaderData);
                pstmt.setClob(39, clobStringReader);

                Time time = Time.valueOf("15:45:30");
                pstmt.setTime(26, time, Calendar.getInstance(), false);

                pstmt.setNull(21, Types.INTEGER, "Test Null Column");

                int rowsAffected = pstmt.executeUpdate();
                assertEquals(1, rowsAffected, "Should insert exactly one row");
            }

            String selectSql = "SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(testTableName)
                    + " WHERE id = ?";
            try (SQLServerPreparedStatement selectStmt = (SQLServerPreparedStatement) con.prepareStatement(selectSql)) {
                selectStmt.setInt(1, 1);

                try (ResultSet rs = selectStmt.executeQuery()) {
                    assertTrue(rs.next(), "Should have at least one result");

                    assertEquals(1, rs.getInt("id"));
                    assertEquals("Test ASCII Stream Data 1", rs.getString("ascii_stream_col1"));
                    assertEquals("Test ASCII Stream Data 2", rs.getString("ascii_stream_col2"));
                    assertEquals("Test ASCII Stream Data 3", rs.getString("ascii_stream_col3"));
                    assertEquals("Test Character Stream 1", rs.getString("character_stream_col1"));
                    assertEquals("Test Character Stream 2", rs.getString("character_stream_col2"));
                    assertEquals("Test Character Stream 3", rs.getString("character_stream_col3"));
                    assertEquals("Test NCharacter Stream 1", rs.getString("ncharacter_stream_col1"));
                    assertEquals("Test NCharacter Stream 2", rs.getString("ncharacter_stream_col2"));
                    assertEquals(new BigDecimal("123.45"), rs.getBigDecimal("decimal_col"));
                    assertEquals(new BigDecimal("999.9900"), rs.getBigDecimal("money_col"));
                    assertEquals(new BigDecimal("99.9900"), rs.getBigDecimal("smallmoney_col"));
                    assertTrue(rs.getBoolean("boolean_col"));
                    assertEquals(100, rs.getByte("byte_col"));
                    assertEquals("12345678-1234-1234-1234-123456789ABC", rs.getString("guid_col").toUpperCase());
                    assertEquals(123.456, rs.getDouble("double_col"), 0.001);
                    assertEquals(78.9f, rs.getFloat("float_col"), 0.1f);
                    assertEquals(0, rs.getInt("int_col"));
                    assertEquals(9876543210L, rs.getLong("long_col"));
                    assertEquals(123, rs.getShort("short_col"));
                    assertEquals("Test String", rs.getString("string_col"));
                    assertEquals("Test NString", rs.getString("nstring_col"));
                    assertEquals("15:45:30", rs.getTime("time_col").toString());
                    assertEquals("2024-01-15", rs.getDate("date_col").toString());
                    assertEquals("2024-01-15", rs.getDate("date_cal_col").toString());
                    assertEquals("2024-01-15", rs.getDate("date_cal_force_col").toString());
                    assertEquals("Test Clob Reader Data", rs.getString("clob_col"));
                    assertEquals("ascii-stream", rs.getString("clob_reader_col"));
                    assertEquals("Test Clob Data", rs.getString("clob_reader_length_col"));
                    assertEquals("Test NClob Data", rs.getString("nclob_col"));
                    assertEquals("Test NClob Data", rs.getString("nclob_reader_col"));
                    assertEquals("Test NClob Data", rs.getString("nclob_reader_length_col"));

                    String xmlResult = rs.getString("xml_col");
                    assertTrue(xmlResult.contains("XML Data"), "XML data should contain expected content");
                }
            }
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(testTableName),
                    con.createStatement());
        }
    }

    /**
     * Test executeLargeUpdate() method for prepared statements.
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testExecuteLargeUpdate() throws SQLException {
        final String testTableName = RandomUtil.getIdentifier("testExecuteLargeUpdate");

        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            String createTableSql = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(testTableName) + " ("
                    + "id INT IDENTITY(1,1) PRIMARY KEY, "
                    + "name NVARCHAR(50), value INT)";

            executeSQL(con, createTableSql);

            String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(testTableName)
                    + " (name, value) VALUES (?, ?)";
            try (SQLServerPreparedStatement insertStmt = (SQLServerPreparedStatement) con.prepareStatement(insertSql)) {
                insertStmt.setString(1, "Test Name");
                insertStmt.setInt(2, 100);

                long insertCount = insertStmt.executeLargeUpdate();
                assertEquals(1L, insertCount, "Should insert exactly one row");
            }

            String updateSql = "UPDATE " + AbstractSQLGenerator.escapeIdentifier(testTableName)
                    + " SET value = ? WHERE name = ?";
            try (SQLServerPreparedStatement updateStmt = (SQLServerPreparedStatement) con.prepareStatement(updateSql)) {
                updateStmt.setInt(1, 200);
                updateStmt.setString(2, "Test Name");

                long updateCount = updateStmt.executeLargeUpdate();
                assertEquals(1L, updateCount, "Should update exactly one row");
            }

            String deleteSql = "DELETE FROM " + AbstractSQLGenerator.escapeIdentifier(testTableName)
                    + " WHERE name = ?";
            try (SQLServerPreparedStatement deleteStmt = (SQLServerPreparedStatement) con.prepareStatement(deleteSql)) {
                deleteStmt.setString(1, "Test Name");
                long deleteCount = deleteStmt.executeLargeUpdate();
                assertEquals(1L, deleteCount, "Should delete exactly one row");
            }
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(testTableName), con.createStatement());
        }
    }

    @Test
    @Tag(Constants.CodeCov)
    public void testExecuteUpdateCountOutOfRange() throws Exception {
        final String testTableName = RandomUtil.getIdentifier("testUpdateCount");

        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            String createTableSql = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(testTableName) + " ("
                    + "id INT, value INT)";
            executeSQL(con, createTableSql);
            executeSQL(con, "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(testTableName) + " VALUES (1, 100)");
            String sql = "UPDATE " + AbstractSQLGenerator.escapeIdentifier(testTableName) + " SET value = value + 1";

            try (SQLServerPreparedStatement stmt = (SQLServerPreparedStatement) con.prepareStatement(sql)) {
                int normalUpdateCount = stmt.executeUpdate();
                assertEquals(1, normalUpdateCount, "Should update 1 row");

                Field updateCountField = SQLServerStatement.class.getDeclaredField("updateCount");
                updateCountField.setAccessible(true);
                updateCountField.setLong(stmt, (long) Integer.MAX_VALUE + 1);

                try {
                    stmt.getUpdateCount();
                    fail("Should have thrown SQLServerException for updateCount out of range");
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage().contains("The update count value is out of range"),
                            "Exception should indicate updateCount out of range: " + e.getMessage());
                }
            }
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(testTableName), con.createStatement());
        }
    }

    /**
     * Test that executeUpdate, execute, executeQuery and addBatch methods that accept a String argument throw SQLServerException.
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testPreparedStatementStringMethodsThrowException() throws Exception {
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            String sql = "SELECT 1";

            try (SQLServerPreparedStatement stmt = (SQLServerPreparedStatement) con.prepareStatement(sql)) {

                try {
                    stmt.executeUpdate("UPDATE table SET col = 1");
                    fail("executeUpdate(String) should throw SQLServerException");
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage().contains("The method executeUpdate() cannot take arguments "
                            + "on a PreparedStatement or CallableStatement."),
                            "Exception should mention executeUpdate(): " + e.getMessage());
                }

                try {
                    stmt.execute("SELECT 2");
                    fail("execute(String) should throw SQLServerException");
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage().contains("The method execute() cannot take arguments "
                            + "on a PreparedStatement or CallableStatement."),
                            "Exception should mention execute(): " + e.getMessage());
                }

                try {
                    stmt.executeQuery("SELECT 3");
                    fail("executeQuery(String) should throw SQLServerException");
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage().contains("The method executeQuery() cannot take arguments "
                            + "on a PreparedStatement or CallableStatement."),
                            "Exception should mention executeQuery(): " + e.getMessage());
                }

                try {
                    stmt.addBatch("INSERT INTO table VALUES (1)");
                    fail("addBatch(String) should throw SQLServerException");
                } catch (SQLServerException e) {
                    assertTrue(e.getMessage().contains("The method addBatch() cannot take arguments "
                            + "on a PreparedStatement or CallableStatement."),
                            "Exception should mention addBatch(): " + e.getMessage());
                }
            }
        }
    }

    /**
     * Test setNull with structured type (Table-Valued Parameter).
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testSetNullWithStructuredType() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            String typeName = RandomUtil.getIdentifier("TestTVPType");
            String procName = RandomUtil.getIdentifier("TestTVPProc");
            String createTypeSql = "CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(typeName)
                    + " AS TABLE (id INT, name NVARCHAR(50))";
            String createProcSql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procName)
                        + " @tvp " + AbstractSQLGenerator.escapeIdentifier(typeName)
                        + " READONLY AS BEGIN SELECT 1 END";

            try {
                executeSQL(con, createTypeSql);
                executeSQL(con, createProcSql);

                String callSql = "{call " + AbstractSQLGenerator.escapeIdentifier(procName) + "(?)}";

                try (SQLServerPreparedStatement stmt = (SQLServerPreparedStatement) con.prepareStatement(callSql)) {
                    stmt.setNull(1, microsoft.sql.Types.STRUCTURED, typeName);
                    stmt.execute();
                }
            } finally {
                executeSQL(con, "DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procName));
                executeSQL(con, "DROP TYPE " + AbstractSQLGenerator.escapeIdentifier(typeName));
            }
        }
    }

    @Test
    @Tag(Constants.CodeCov)
    public void testSetObjectWithSQLType() throws SQLException {
        final String testTableName = RandomUtil.getIdentifier("testSetObjectSQLType");

        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            // Create test table with various data types
            String createTableSql = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(testTableName) + " ("
                    + "id INT IDENTITY(1,1) PRIMARY KEY, decimal_col DECIMAL(10,2), "
                    + "numeric_col NUMERIC(15,3), varchar_col VARCHAR(100), "
                    + "int_col INT, bigint_col BIGINT)";

            executeSQL(con, createTableSql);

            String insertSql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(testTableName)
                    + " (decimal_col, numeric_col, varchar_col, int_col, bigint_col) VALUES (?, ?, ?, ?, ?)";

            try (SQLServerPreparedStatement stmt = (SQLServerPreparedStatement) con.prepareStatement(insertSql)) {

                BigDecimal decimalValue = new BigDecimal("123.45");
                stmt.setObject(1, decimalValue, JDBCType.DECIMAL, 2);

                BigDecimal numericValue = new BigDecimal("987.654");
                stmt.setObject(2, numericValue, JDBCType.NUMERIC, 15, 3);

                stmt.setObject(3, "Test String", JDBCType.VARCHAR, 100, 0, false);
                stmt.setObject(4, 42, Types.INTEGER, 10, 0, false);
                stmt.setObject(5, 9876543210L, Types.BIGINT, 19, 0);

                int rowsAffected = stmt.executeUpdate();
                assertEquals(1, rowsAffected, "Should insert exactly one row");
            }

            String selectSql = "SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(testTableName)
                    + " WHERE id = 1";
            try (SQLServerPreparedStatement selectStmt = (SQLServerPreparedStatement) con.prepareStatement(selectSql)) {
                try (ResultSet rs = selectStmt.executeQuery()) {
                    assertTrue(rs.next(), "Should have at least one result");

                    assertEquals(new BigDecimal("123.45"), rs.getBigDecimal("decimal_col"));
                    assertEquals(new BigDecimal("987.654"), rs.getBigDecimal("numeric_col"));
                    assertEquals("Test String", rs.getString("varchar_col"));
                    assertEquals(42, rs.getInt("int_col"));
                    assertEquals(9876543210L, rs.getLong("bigint_col"));
                }
            }

            try (SQLServerPreparedStatement stmt = (SQLServerPreparedStatement) con.prepareStatement(insertSql)) {
                stmt.setObject(1, new BigDecimal("111.11"), JDBCType.DECIMAL, 2);
                stmt.setObject(2, new BigDecimal("222.222"), JDBCType.NUMERIC, 15, 3);

                String testData = "Stream data test";
                ByteArrayInputStream inputStream = new ByteArrayInputStream(testData.getBytes());
                stmt.setObject(3, inputStream, Types.VARCHAR, 100, testData.length(), false);

                stmt.setObject(4, 100, Types.INTEGER, 10, 0, false);
                stmt.setObject(5, 5555555555L, Types.BIGINT, 19, 0);

                int rowsAffected = stmt.executeUpdate();
                assertEquals(1, rowsAffected, "Should insert exactly one row with InputStream");
            }

            try (SQLServerPreparedStatement stmt = (SQLServerPreparedStatement) con.prepareStatement(insertSql)) {
                stmt.setObject(1, new BigDecimal("333.33"), JDBCType.DECIMAL, 2);
                stmt.setObject(2, new BigDecimal("444.444"), JDBCType.NUMERIC, 15, 3);

                String readerData = "Reader data test";
                StringReader reader = new StringReader(readerData);
                stmt.setObject(3, reader, Types.VARCHAR, 100, readerData.length(), false);

                stmt.setObject(4, 200, Types.INTEGER, 10, 0, false);
                stmt.setObject(5, 7777777777L, Types.BIGINT, 19, 0);

                int rowsAffected = stmt.executeUpdate();
                assertEquals(1, rowsAffected, "Should insert exactly one row with Reader");
            }
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(testTableName), con.createStatement());
        }
    }

    /**
     * Test clearBatch method functionality.
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testClearBatch() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            String sql = "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1) VALUES (?)";
            executeSQL(con, "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 INT)");

            try (SQLServerPreparedStatement stmt = (SQLServerPreparedStatement) con.prepareStatement(sql)) {
                stmt.setInt(1, 1);
                stmt.addBatch();
                stmt.setInt(1, 2);
                stmt.addBatch();

                stmt.clearBatch();

                int[] results = stmt.executeBatch();
                assertEquals(0, results.length, "Batch should be empty after clearBatch()");
            }
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), con.createStatement());
        }
    }

    /**
     * Test batch execution with null batchParamValues.
     * Executes an empty batch and verifies it returns an empty array.
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testExecuteBatchWithNullBatchValues() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            String sql = "SELECT 1";

            try (SQLServerPreparedStatement stmt = (SQLServerPreparedStatement) con.prepareStatement(sql)) {
                int[] results = stmt.executeBatch();
                assertEquals(0, results.length, "Should return empty array for null batchParamValues");
            }
        }
    }

    /**
     * Test batch execution with OUT parameters. It will trigger BatchUpdateException.
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testBatchExecutionWithOutputParameters() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            String procName = RandomUtil.getIdentifier("testOutputProc");
            String createProc = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procName)
                    + " @input INT, @output INT OUTPUT AS BEGIN SET @output = @input * 2 END";

            executeSQL(con, createProc);
            String callSql = "{call " + AbstractSQLGenerator.escapeIdentifier(procName) + "(?, ?)}";

            try (SQLServerCallableStatement stmt = (SQLServerCallableStatement) con.prepareCall(callSql)) {
                stmt.setInt(1, 5);
                stmt.registerOutParameter(2, Types.INTEGER);
                stmt.addBatch();

                try {
                    stmt.executeBatch();
                    fail("Should throw BatchUpdateException for OUT parameters in batch");
                } catch (BatchUpdateException e) {
                    assertTrue(e.getMessage().contains("The OUT and INOUT parameters are not permitted in a batch."),
                            "Exception should indicate OUT parameters not permitted in batch");
                }
            } finally {
                executeSQL(con, "DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procName));
            }
        }
    }

    /**
     * Test SQL parsing methods for comments and spaces.
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testSQLParsingWithCommentsAndSpaces() throws Exception {
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            modifyConnectionForBulkCopyAPI(con);

            String tableName = RandomUtil.getIdentifier("testSQLParsing");
            executeSQL(con, "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (col1 INT)");

            String sqlWithComments = "/* comment */ INSERT /* another comment */ INTO " +
                    AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " /* more comments */ VALUES /* final comment */ (?)";

            try (SQLServerPreparedStatement stmt = (SQLServerPreparedStatement) con.prepareStatement(sqlWithComments)) {
                stmt.setInt(1, 123);
                stmt.addBatch();

                int[] results = stmt.executeBatch();
                assertEquals(1, results.length);
                assertEquals(1, results[0]);
            }

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), con.createStatement());
        }
    }

    /**
     * Test SELECT statement in batch execution which is not allowed.
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testSelectStatementInBatch() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            String selectSql = "SELECT ?";

            try (SQLServerPreparedStatement stmt = (SQLServerPreparedStatement) con.prepareStatement(selectSql)) {
                stmt.setInt(1, 1);
                stmt.addBatch();

                try {
                    stmt.executeBatch();
                    fail("Should throw exception for SELECT statement in batch");
                } catch (SQLException e) {
                    assertTrue(e.getMessage().contains("The SELECT statement is not permitted in a batch."),
                            "Exception should indicate SELECT not permitted in batch");
                }
            }
        }
    }

    /**
     * This test when a ResultSet is generated during a batch update operation, which is not allowed.
     */
    @Test
    @Tag(Constants.CodeCov)
    public void testResultSetInBatchUpdate() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            // Create a procedure that returns a ResultSet instead of update count
            String procName = RandomUtil.getIdentifier("testResultSetProc");
            String createProc = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procName)
                    + " AS BEGIN SELECT 'This should not be returned in batch' AS message END";

            executeSQL(con, createProc);
            String callSql = "{call " + AbstractSQLGenerator.escapeIdentifier(procName) + "}";
            try (SQLServerPreparedStatement stmt = (SQLServerPreparedStatement) con.prepareStatement(callSql)) {
                stmt.addBatch();
                try {
                    stmt.executeBatch();
                } catch (BatchUpdateException e) {
                    assertTrue(e.getMessage().contains("A result set was generated for update."),
                            "Exception should indicate ResultSet generated for update operation");
                } catch (SQLException e) {
                    assertTrue(e.getMessage().contains("ResultSet") || e.getMessage().contains("batch"),
                            "Exception should be related to ResultSet or batch operations");
                }
            } finally {
                executeSQL(con, "DROP PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procName));
            }
        }
    }

    private void testStatementPoolingInternal(String mode) throws Exception {
        // Test % handle re-use
        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
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

        try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
            if (mode.equalsIgnoreCase("bulkcopy")) {
                modifyConnectionForBulkCopyAPI(con);
            }

            int msgId = Constants.RANDOM.nextInt(50000, 99999);

            // Test behavior with statement pooling.
            con.setStatementPoolingCacheSize(10);

            this.executeSQL(con, "IF EXISTS (SELECT * FROM sys.messages WHERE message_id = " + msgId
                    + ") EXEC sp_dropmessage @msgnum = " + msgId + ", @lang = 'all';");
            this.executeSQL(con, "EXEC sp_addmessage " + msgId + ", 16, 'Prepared handle GAH!';");
            // Test with missing handle failures (fake).
            this.executeSQL(con, "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (col INT);INSERT " + AbstractSQLGenerator.escapeIdentifier(tableName) + " VALUES (1);");
            this.executeSQL(con,
                    "CREATE PROC #updateProc1 AS UPDATE " + AbstractSQLGenerator.escapeIdentifier(tableName)
                            + " SET col += 1; IF EXISTS (SELECT * FROM "
                            + AbstractSQLGenerator.escapeIdentifier(tableName) + " WHERE col % 5 = 0) RAISERROR("
                            + msgId + ",16,1);");
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
            } finally {
                this.executeSQL(con, "IF EXISTS (SELECT * FROM sys.messages WHERE message_id = " + msgId
                        + ") EXEC sp_dropmessage @msgnum = " + msgId + ", @lang = 'all';");
            }

            // test updated value, should be 1 + 100 = 101
            // although executeUpdate() throws exception, update operation should be executed successfully.
            try (Statement stmt = con.createStatement(); ResultSet rs = stmt
                    .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(tableName) + "")) {
                rs.next();
                assertSame(101, rs.getInt(1));
            }

            // Test batching with missing handle failures (fake).

            this.executeSQL(con, "IF EXISTS (SELECT * FROM sys.messages WHERE message_id = " + msgId
                    + ") EXEC sp_dropmessage @msgnum = " + msgId + ", @lang = 'all';");
            this.executeSQL(con, "EXEC sp_addmessage " + msgId + ", 16, 'Prepared handle GAH!';");
            this.executeSQL(con, "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName2)
                    + " (col INT);INSERT " + AbstractSQLGenerator.escapeIdentifier(tableName2) + " VALUES (1);");
            this.executeSQL(con,
                    "CREATE PROC #updateProc2 AS UPDATE " + AbstractSQLGenerator.escapeIdentifier(tableName2)
                            + " SET col += 1; IF EXISTS (SELECT * FROM "
                            + AbstractSQLGenerator.escapeIdentifier(tableName2) + " WHERE col % 5 = 0) RAISERROR("
                            + msgId + ",16,1);");
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
            } finally {
                this.executeSQL(con, "IF EXISTS (SELECT * FROM sys.messages WHERE message_id = " + msgId
                        + ") EXEC sp_dropmessage @msgnum = " + msgId + ", @lang = 'all';");
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

            // AE has 1 more prepared statement handle as it calls sp_describe_parameter_encryption
            if (ds.getColumnEncryptionSetting().equalsIgnoreCase(Constants.ENABLED)) {
                handle++;
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
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con
                    .prepareStatement(query + Constants.SEMI_COLON)) {
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

    private static void dropTables() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName2), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName3), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName4), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName5), stmt);
        }
    }

}
