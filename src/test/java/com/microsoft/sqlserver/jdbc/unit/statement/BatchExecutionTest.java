/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assume.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.TimeZone;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Tests batch execution with AE On connection
 *
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xSQLv11)
@Tag(Constants.xSQLv12)
@Tag(Constants.xAzureSQLDW)
public class BatchExecutionTest extends AbstractTest {

    private static String ctstable1;
    private static String ctstable2;
    private static String ctstable3;
    private static String ctstable4;
    private static String ctstable3Procedure1;
    private static String timestampTable1 = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("timestamptable1"));
    private static String timestampTable2 = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("timestamptable2"));
    private static String caseSensitiveDatabase = "BD_Collation_SQL_Latin1_General_CP1_CS_AS";
    private static String caseInsensitiveDatabase = "BD_Collation_SQL_Latin1_General_CP1_CI_AS";

    /**
     * This tests the updateCount when the error query does cause a SQL state HY008.
     *
     * @throws Exception
     */
    @Test
    public void testBatchUpdateCountFalseOnFirstPstmtPrepexec() throws Exception {
        long[] expectedUpdateCount = {1, 1, 1, 1, -3, -3, -3, -3, -3, -3};
        testBatchUpdateCountWith(10, 6, false, "prepexec", expectedUpdateCount);
    }

    /**
     * This tests the updateCount when the error query does cause a SQL state HY008.
     *
     * @throws Exception
     */
    @Test
    public void testBatchUpdateCountTrueOnFirstPstmtPrepexec() throws Exception {
        long[] expectedUpdateCount = {1, 1, -3, -3, -3};
        testBatchUpdateCountWith(5, 4, true, "prepexec", expectedUpdateCount);
    }

    /**
     * This tests the updateCount when the error query does cause a SQL state HY008.
     *
     * @throws Exception
     */
    @Test
    public void testBatchUpdateCountFalseOnFirstPstmtSpPrepare() throws Exception {
        long[] expectedUpdateCount = {1, 1, 1, 1, -3, -3, -3, -3, -3, -3};
        testBatchUpdateCountWith(10, 6, false, "prepare", expectedUpdateCount);
    }

    /**
     * This tests the updateCount when the error query does cause a SQL state HY008.
     *
     * @throws Exception
     */
    @Test
    public void testBatchUpdateCountTrueOnFirstPstmtSpPrepare() throws Exception {
        long[] expectedUpdateCount = {1, 1, -3, -3, -3};
        testBatchUpdateCountWith(5, 4, true, "prepare", expectedUpdateCount);
    }

    /**
     * This tests the updateCount when the error query does cause a SQL state HY008.
     *
     * @throws Exception
     */
    @Test
    public void testBatchUpdateCountFalseOnFirstPstmtExec() throws Exception {
        long[] expectedUpdateCount = {1, 1, 1, 1, -3, -3, -3, -3, -3, -3};
        testBatchUpdateCountWith(10, 6, false, "exec", expectedUpdateCount);
    }

    /**
     * This tests the updateCount when the error query does cause a SQL state HY008.
     *
     * @throws Exception
     */
    @Test
    public void testBatchUpdateCountTrueOnFirstPstmtExec() throws Exception {
        long[] expectedUpdateCount = {1, 1, -3, -3, -3};
        testBatchUpdateCountWith(5, 4, true, "exec", expectedUpdateCount);
    }

    @Test
    public void testSqlServerBulkCopyCachingPstmtLevel() throws Exception {
        Calendar gmtCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        long ms = 1578743412000L;

        try (Connection con = DriverManager.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;sendTemporalDataTypesAsStringForBulkCopy=false;");
                Statement stmt = con.createStatement();
                PreparedStatement pstmt = con.prepareStatement("INSERT INTO " + timestampTable1 + " VALUES(?)")) {

            TestUtils.dropTableIfExists(timestampTable1, stmt);
            String createSql = "CREATE TABLE " + timestampTable1 + " (c1 DATETIME2(3))";
            stmt.execute(createSql);

            Field cachedBulkCopyOperationField = pstmt.getClass().getDeclaredField("bcOperation");
            cachedBulkCopyOperationField.setAccessible(true);
            Object cachedBulkCopyOperation = cachedBulkCopyOperationField.get(pstmt);
            assertEquals(null, cachedBulkCopyOperation, "SqlServerBulkCopy object should not be cached yet.");

            Timestamp timestamp = new Timestamp(ms);

            pstmt.setTimestamp(1, timestamp, gmtCal);
            pstmt.addBatch();
            pstmt.executeBatch();

            cachedBulkCopyOperation = cachedBulkCopyOperationField.get(pstmt);
            assertNotNull("SqlServerBulkCopy object should be cached.", cachedBulkCopyOperation);
        }
    }

    @Test
    public void testSqlServerBulkCopyCachingConnectionLevel() throws Exception {
        Calendar gmtCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        long ms = 1578743412000L;

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;cacheBulkCopyMetadata=true;sendTemporalDataTypesAsStringForBulkCopy=false;");
                Statement stmt = con.createStatement()) {

            // Needs to be on a JDK version greater than 8
            assumeTrue(TestUtils.getJVMVersion() > 8);

            TestUtils.dropTableIfExists(timestampTable1, stmt);
            TestUtils.dropTableIfExists(timestampTable2, stmt);
            String createSqlTable1 = "CREATE TABLE " + timestampTable1 + " (c1 DATETIME2(3))";
            String createSqlTable2 = "CREATE TABLE " + timestampTable2 + " (c1 DATETIME2(3))";
            stmt.execute(createSqlTable1);
            stmt.execute(createSqlTable2);

            Field bulkcopyMetadataCacheField;

            if (con.getClass().getName().equals("com.microsoft.sqlserver.jdbc.SQLServerConnection43")) {
                bulkcopyMetadataCacheField = con.getClass().getSuperclass()
                        .getDeclaredField("bulkCopyOperationCache");
            } else {
                bulkcopyMetadataCacheField = con.getClass().getDeclaredField("bulkCopyOperationCache");
            }

            bulkcopyMetadataCacheField.setAccessible(true);
            Object bulkcopyCache = bulkcopyMetadataCacheField.get(con);

            assertTrue(((HashMap) bulkcopyCache).isEmpty(), "Cache should be empty");

            for (int i = 0; i < 5; i++) {
                PreparedStatement pstmt = con.prepareStatement("INSERT INTO " + timestampTable1 + " VALUES(?)");
                Timestamp timestamp = new Timestamp(ms);
                pstmt.setTimestamp(1, timestamp, gmtCal);
                pstmt.addBatch();
                pstmt.executeBatch();

                bulkcopyCache = bulkcopyMetadataCacheField.get(con);
                assertTrue(!((HashMap) bulkcopyCache).isEmpty(), "Cache should not be empty");
            }

            // Cache should have 1 metadata item cached
            assertEquals(1, ((HashMap) bulkcopyCache).size());

            // Execute a different batch call on a different table
            for (int i = 0; i < 5; i++) {
                PreparedStatement pstmt = con.prepareStatement("INSERT INTO " + timestampTable2 + " VALUES(?)");
                Timestamp timestamp = new Timestamp(ms);
                pstmt.setTimestamp(1, timestamp, gmtCal);
                pstmt.addBatch();
                pstmt.executeBatch();

                bulkcopyCache = bulkcopyMetadataCacheField.get(con);
                assertTrue(!((HashMap) bulkcopyCache).isEmpty(), "Cache should not be empty");
            }

            // Cache should now have 2 metadata items cached
            assertEquals(2, ((HashMap) bulkcopyCache).size());
        }
    }

    @Test
    public void testSqlServerBulkCopyCachingConnectionLevelMultiThreaded() throws Exception {
        // Needs to be on a JDK version greater than 8
        assumeTrue(TestUtils.getJVMVersion() > 8);

        Calendar gmtCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        long ms = 1578743412000L;
        long timeOut = 30000;
        int NUMBER_SIMULTANEOUS_INSERTS = 5;

        try (SQLServerConnection con = (SQLServerConnection) DriverManager.getConnection(connectionString
                + ";useBulkCopyForBatchInsert=true;cacheBulkCopyMetadata=true;sendTemporalDataTypesAsStringForBulkCopy=false;");
                Statement stmt = con.createStatement()) {

            TestUtils.dropTableIfExists(timestampTable1, stmt);
            String createSqlTable1 = "CREATE TABLE " + timestampTable1 + " (c1 DATETIME2(3))";
            stmt.execute(createSqlTable1);

            Field bulkcopyMetadataCacheField;

            if (con.getClass().getName().equals("com.microsoft.sqlserver.jdbc.SQLServerConnection43")) {
                bulkcopyMetadataCacheField = con.getClass().getSuperclass()
                        .getDeclaredField("bulkCopyOperationCache");
            } else {
                bulkcopyMetadataCacheField = con.getClass().getDeclaredField("bulkCopyOperationCache");
            }

            bulkcopyMetadataCacheField.setAccessible(true);
            Object bulkcopyCache = bulkcopyMetadataCacheField.get(con);

            ((HashMap<?, ?>) bulkcopyCache).clear();

            TimerTask task = new TimerTask() {
                public void run() {
                    ((HashMap<?, ?>) bulkcopyCache).clear();
                }
            };
            Timer timer = new Timer("Timer");
            timer.schedule(task, timeOut); // Run a timer to help us exit if we get deadlocked

            final CountDownLatch countDownLatch = new CountDownLatch(NUMBER_SIMULTANEOUS_INSERTS);
            Runnable runnable = () -> {
                try {
                    for (int i = 0; i < 5; ++i) {
                        PreparedStatement preparedStatement = con
                                .prepareStatement("INSERT INTO " + timestampTable1 + " VALUES(?)");
                        Timestamp timestamp = new Timestamp(ms);
                        preparedStatement.setTimestamp(1, timestamp, gmtCal);
                        preparedStatement.addBatch();
                        preparedStatement.executeBatch();
                    }
                    countDownLatch.countDown();
                    countDownLatch.await();
                } catch (Exception e) {
                    fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
                } finally {
                    ((HashMap<?, ?>) bulkcopyCache).clear();
                }
            };

            ExecutorService executor = Executors.newFixedThreadPool(NUMBER_SIMULTANEOUS_INSERTS);

            try {
                for (int i = 0; i < NUMBER_SIMULTANEOUS_INSERTS; i++) {
                    executor.submit(runnable);
                }
                executor.shutdown();
            } catch (Exception e) {
                fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
            } finally {
                ((HashMap<?, ?>) bulkcopyCache).clear();
            }
        }
    }

    @Test
    public void testValidTimezoneForTimestampBatchInsertWithBulkCopy() throws Exception {
        Calendar gmtCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        long ms = 1578743412000L;

        // Insert Timestamp using batch insert
        try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement();
                PreparedStatement pstmt = con.prepareStatement("INSERT INTO " + timestampTable1 + " VALUES(?)")) {

            TestUtils.dropTableIfExists(timestampTable1, stmt);
            String createSql = "CREATE TABLE " + timestampTable1 + " (c1 DATETIME2(3))";
            stmt.execute(createSql);

            Timestamp timestamp = new Timestamp(ms);

            pstmt.setTimestamp(1, timestamp, gmtCal);
            pstmt.addBatch();
            pstmt.executeBatch();
        }

        // Insert Timestamp using bulkcopy for batch insert
        try (Connection con = DriverManager.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;sendTemporalDataTypesAsStringForBulkCopy=false;");
                PreparedStatement pstmt = con.prepareStatement("INSERT INTO " + timestampTable1 + " VALUES(?)")) {

            Timestamp timestamp = new Timestamp(ms);

            pstmt.setTimestamp(1, timestamp, gmtCal);
            pstmt.addBatch();
            pstmt.executeBatch();
        }

        // Compare Timestamp values inserted, should be the same
        try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + timestampTable1)) {
            Timestamp ts0;
            Timestamp ts1;
            Time t0;
            Time t1;
            Date d0;
            Date d1;

            rs.next();
            ts0 = rs.getTimestamp(1);
            t0 = rs.getTime(1);
            d0 = rs.getDate(1);
            rs.next();
            ts1 = rs.getTimestamp(1);
            t1 = rs.getTime(1);
            d1 = rs.getDate(1);

            assertEquals(ts0, ts1);
            assertEquals(t0, t1);
            assertEquals(d0, d1);
        }
    }

    @Test
    public void testValidTimezonesDstTimestampBatchInsertWithBulkCopy() throws Exception {
        Calendar gmtCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));

        for (String tzId : TimeZone.getAvailableIDs()) {
            TimeZone.setDefault(TimeZone.getTimeZone(tzId));

            long ms = 1696127400000L; // DST

            // Insert Timestamp using batch insert
            try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement();
                    PreparedStatement pstmt = con.prepareStatement("INSERT INTO " + timestampTable1 + " VALUES(?)")) {

                TestUtils.dropTableIfExists(timestampTable1, stmt);
                String createSql = "CREATE TABLE " + timestampTable1 + " (c1 DATETIME2(3))";
                stmt.execute(createSql);

                Timestamp timestamp = new Timestamp(ms);

                pstmt.setTimestamp(1, timestamp, gmtCal);
                pstmt.addBatch();
                pstmt.executeBatch();
            } catch (Exception e) {
                fail(e.getMessage());
            }

            // Insert Timestamp using bulkcopy for batch insert
            try (Connection con = DriverManager.getConnection(
                    connectionString + ";useBulkCopyForBatchInsert=true;sendTemporalDataTypesAsStringForBulkCopy=false;");
                    PreparedStatement pstmt = con.prepareStatement("INSERT INTO " + timestampTable1 + " VALUES(?)")) {

                Timestamp timestamp = new Timestamp(ms);

                pstmt.setTimestamp(1, timestamp, gmtCal);
                pstmt.addBatch();
                pstmt.executeBatch();
            } catch (Exception e) {
                fail(e.getMessage());
            }

            // Compare Timestamp values inserted, should be the same
            try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + timestampTable1)) {
                Timestamp ts0;
                Timestamp ts1;
                Time t0;
                Time t1;
                Date d0;
                Date d1;

                rs.next();
                ts0 = rs.getTimestamp(1);
                t0 = rs.getTime(1);
                d0 = rs.getDate(1);
                rs.next();
                ts1 = rs.getTimestamp(1);
                t1 = rs.getTime(1);
                d1 = rs.getDate(1);

                String failureMsg = "Failed for time zone: " + tzId;
                assertEquals(ts0, ts1, failureMsg);
                assertEquals(t0, t1, failureMsg);
                assertEquals(d0, d1, failureMsg);
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void testBatchInsertTimestampNoTimezoneDoubleConversion() throws Exception {
        Calendar gmtCal = Calendar.getInstance(TimeZone.getTimeZone("GMT"));
        long ms = 1578743412000L;

        // Insert Timestamp using prepared statement when useBulkCopyForBatchInsert=true
        try (Connection con = DriverManager.getConnection(connectionString
                + ";useBulkCopyForBatchInsert=true;sendTemporalDataTypesAsStringForBulkCopy=false;"); Statement stmt = con.createStatement();
                PreparedStatement pstmt = con.prepareStatement("INSERT INTO " + timestampTable2 + " VALUES(?)")) {

            TestUtils.dropTableIfExists(timestampTable2, stmt);
            String createSql = "CREATE TABLE" + timestampTable2 + " (c1 DATETIME2(3))";
            stmt.execute(createSql);

            Timestamp timestamp = new Timestamp(ms);

            pstmt.setTimestamp(1, timestamp, gmtCal);
            pstmt.execute();
        }

        // Insert Timestamp using bulkcopy for batch insert
        try (Connection con = DriverManager.getConnection(
                connectionString + ";useBulkCopyForBatchInsert=true;sendTemporalDataTypesAsStringForBulkCopy=false;");
                PreparedStatement pstmt = con.prepareStatement("INSERT INTO " + timestampTable2 + " VALUES(?)")) {

            Timestamp timestamp = new Timestamp(ms);

            pstmt.setTimestamp(1, timestamp, gmtCal);
            pstmt.addBatch();
            pstmt.executeBatch();
        }

        // Compare Timestamp values inserted, should be the same
        try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + timestampTable2)) {
            Timestamp ts0;
            Timestamp ts1;
            Time t0;
            Time t1;
            Date d0;
            Date d1;

            rs.next();
            ts0 = rs.getTimestamp(1);
            t0 = rs.getTime(1);
            d0 = rs.getDate(1);
            rs.next();
            ts1 = rs.getTimestamp(1);
            t1 = rs.getTime(1);
            d1 = rs.getDate(1);

            assertEquals(ts0, ts1);
            assertEquals(t0, t1);
            assertEquals(d0, d1);
        }
    }

    /**
     * This tests the updateCount when the error query does not cause a SQL state HY008.
     *
     * @throws Exception
     */
    @Test
    public void testBatchUpdateCount() throws Exception {
        long[] expectedUpdateCount = {1, 1, 1, 1, -3, 1, 1, 1, 1, 1};

        try (SQLServerConnection connection = PrepUtil.getConnection(connectionString)) {
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection.prepareStatement(
                    "insert into " + AbstractSQLGenerator.escapeIdentifier(ctstable4) + " values(?)")) {
                for (int i = 1; i <= 10; i++) {
                    if (i == 5) {
                        pstmt.setInt(1, -1);
                    } else {
                        pstmt.setInt(1, i);
                    }
                    pstmt.addBatch();
                }

                try {
                    pstmt.executeBatch();
                } catch (BatchUpdateException e) {
                    assertArrayEquals(expectedUpdateCount, e.getLargeUpdateCounts(),
                            "Actual: " + Arrays.toString(e.getLargeUpdateCounts()));
                }
            }
        }
    }

    /**
     * testAddBatch1 and testExecutionBatch one looks similar except for the parameters being passed for select query.
     * TODO: we should look and simply the test later by parameterized values
     * 
     * @throws Exception
     */
    @Test
    public void testBatchExceptionAEOn() throws Exception {
        testAddBatch1();
        testExecuteBatch1();
        testAddBatch1UseBulkCopyAPI();
        testExecuteBatch1UseBulkCopyAPI();
    }

    @Test
    public void testBatchSpPrepare() throws Exception {
        connectionString += ";prepareMethod=prepare;";
        testAddBatch1();
        testExecuteBatch1();
        testAddBatch1UseBulkCopyAPI();
        testExecuteBatch1UseBulkCopyAPI();
    }

    @Test
    public void testBatchExec() throws Exception {
        connectionString += ";prepareMethod=exec;";
        testAddBatch1();
        testExecuteBatch1();
        testAddBatch1UseBulkCopyAPI();
        testExecuteBatch1UseBulkCopyAPI();
    }

    @Test
    public void testBatchStatementCancellation() throws Exception {
        String testTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("test_table"));
        try (Connection connection = PrepUtil.getConnection(connectionString)) {
            connection.setAutoCommit(false);

            try (Statement statement = connection.createStatement()) {
                TestUtils.dropTableIfExists(testTable, statement);
            }
            connection.commit();

            try (Statement statement = connection.createStatement()) {
                statement.execute("create table " + testTable + " (column_name bit)");
            }
            connection.commit();

            try {
                for (long delayInMilliseconds : new long[] {1, 2, 4, 8, 16, 32, 64, 128}) {
                    for (int numberOfCommands : new int[] {1, 2, 4, 8, 16, 32, 64}) {
                        int parameterCount = 512;

                        try (PreparedStatement statement = connection.prepareStatement(
                                "insert into " + testTable + " values (?)" + repeat(",(?)", parameterCount - 1))) {

                            for (int i = 0; i < numberOfCommands; i++) {
                                for (int j = 0; j < parameterCount; j++) {
                                    statement.setBoolean(j + 1, true);
                                }
                                statement.addBatch();
                            }

                            Thread cancelThread = cancelAsync(statement, delayInMilliseconds);
                            try {
                                statement.executeBatch();
                            } catch (SQLException e) {
                                assertEquals(TestResource.getResource("R_queryCanceled"), e.getMessage());
                            }
                            cancelThread.join();
                        }
                        connection.commit();
                    }
                }
            } finally {
                try (Statement statement = connection.createStatement()) {
                    TestUtils.dropTableIfExists(testTable, statement);
                    connection.commit();
                }
            }
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDB)
    public void testExecuteBatchColumnCaseMismatch_CI() throws Exception {
        String connectionStringCollationCaseInsensitive = TestUtils.addOrOverrideProperty(connectionString, "databaseName", caseInsensitiveDatabase);
        String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("caseInsensitiveTable"));

        try (Connection conTemp = DriverManager.getConnection(connectionString); Statement stmt = conTemp.createStatement()) {
            TestUtils.dropDatabaseIfExists(caseInsensitiveDatabase, connectionString);
            stmt.executeUpdate("CREATE DATABASE " + caseInsensitiveDatabase);
            stmt.executeUpdate("ALTER DATABASE " + caseInsensitiveDatabase + " COLLATE SQL_Latin1_General_CP1_CI_AS;");

            // Insert Timestamp using prepared statement when useBulkCopyForBatchInsert=true
            try (Connection con = DriverManager.getConnection(connectionStringCollationCaseInsensitive
                    + ";useBulkCopyForBatchInsert=true;sendTemporalDataTypesAsStringForBulkCopy=false;")) {
                try (Statement statement = con.createStatement()) {
                    TestUtils.dropTableIfExists(tableName, statement);
                    String createSql = "CREATE TABLE" + tableName + " (c1 varchar(10))";
                    statement.execute(createSql);
                }
                // upper case C1
                try (PreparedStatement preparedStatement = con.prepareStatement("INSERT INTO " + tableName + "(C1) VALUES(?)")) {
                    preparedStatement.setObject(1, "value1");
                    preparedStatement.addBatch();
                    preparedStatement.setObject(1, "value2");
                    preparedStatement.addBatch();
                    preparedStatement.executeBatch();
                }
            }
        } finally {
            TestUtils.dropDatabaseIfExists(caseInsensitiveDatabase, connectionString);
        }
    }

    // adapter Azure pipeline CI, need to add a new environment variable `mssql_jdbc_test_connection_properties_collation_cs`
    @Test
    @Tag(Constants.xAzureSQLDB)
    public void testExecuteBatchColumnCaseMismatch_CS_throwException() throws Exception {
        String connectionStringCollationCaseSensitive = TestUtils.addOrOverrideProperty(connectionString, "databaseName", caseSensitiveDatabase);
        String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("caseSensitiveTable"));

        try (Connection conTemp = DriverManager.getConnection(connectionString); Statement stmt = conTemp.createStatement()) {
            TestUtils.dropDatabaseIfExists(caseSensitiveDatabase, connectionString);
            stmt.executeUpdate("CREATE DATABASE " + caseSensitiveDatabase);
            stmt.executeUpdate("ALTER DATABASE " + caseSensitiveDatabase + " COLLATE SQL_Latin1_General_CP1_CS_AS;");

            try (Connection con = DriverManager.getConnection(connectionStringCollationCaseSensitive
                    + ";useBulkCopyForBatchInsert=true;sendTemporalDataTypesAsStringForBulkCopy=false;")) {
                try (Statement statement = con.createStatement()) {
                    TestUtils.dropTableIfExists(tableName, statement);
                    String createSql = "CREATE TABLE" + tableName + " (c1 varchar(10))";
                    statement.execute(createSql);
                }
                // upper case C1
                try (PreparedStatement preparedStatement = con.prepareStatement("INSERT INTO " + tableName + "(C1) VALUES(?)")) {
                    preparedStatement.setObject(1, "value1");
                    preparedStatement.addBatch();
                    preparedStatement.setObject(1, "value2");
                    preparedStatement.addBatch();
                    try {
                        preparedStatement.executeBatch();
                        fail("Should have failed");
                    } catch (Exception ex) {
                        assertInstanceOf(java.sql.BatchUpdateException.class, ex);
                        assertEquals("Unable to retrieve column metadata.", ex.getMessage());
                    }
                }
            }
        } finally {
            TestUtils.dropDatabaseIfExists(caseSensitiveDatabase, connectionString);
        }
    }

    // adapter Azure pipeline CI, need to add a new environment variable `mssql_jdbc_test_connection_properties_collation_cs`
    @Test
    @Tag(Constants.xAzureSQLDB)
    public void testExecuteBatchColumnCaseMismatch_CS() throws Exception {
        String connectionStringCollationCaseSensitive = TestUtils.addOrOverrideProperty(connectionString, "databaseName", caseSensitiveDatabase);
        String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("caseSensitiveTable"));

        try (Connection conTemp = DriverManager.getConnection(connectionString); Statement stmt = conTemp.createStatement()) {
            TestUtils.dropDatabaseIfExists(caseSensitiveDatabase, connectionString);
            stmt.executeUpdate("CREATE DATABASE " + caseSensitiveDatabase);
            stmt.executeUpdate("ALTER DATABASE " + caseSensitiveDatabase + " COLLATE SQL_Latin1_General_CP1_CS_AS;");

            // Insert Timestamp using prepared statement when useBulkCopyForBatchInsert=true
            try (Connection con = DriverManager.getConnection(connectionStringCollationCaseSensitive
                    + ";useBulkCopyForBatchInsert=true;sendTemporalDataTypesAsStringForBulkCopy=false;")) {
                try (Statement statement = con.createStatement()) {
                    TestUtils.dropTableIfExists(tableName, statement);
                    String createSql = "CREATE TABLE" + tableName + " (c1 varchar(10), C1 varchar(10))";
                    statement.execute(createSql);
                }
                // upper case C1
                try (PreparedStatement preparedStatement = con.prepareStatement("INSERT INTO " + tableName + "(c1, C1) VALUES(?,?)")) {
                    preparedStatement.setObject(1, "value1-1");
                    preparedStatement.setObject(2, "value1-2");
                    preparedStatement.addBatch();
                    preparedStatement.setObject(1, "value2-1");
                    preparedStatement.setObject(2, "value2-2");
                    preparedStatement.addBatch();
                    preparedStatement.executeBatch();
                }
            }
        } finally {
            TestUtils.dropDatabaseIfExists(caseSensitiveDatabase, connectionString);
        }
    }

    /**
     * Get a PreparedStatement object and call the addBatch() method with 3 SQL statements and call the executeBatch()
     * method and it should return array of Integer values of length 3
     */
    public void testAddBatch1() {
        testAddBatch1Internal("BatchInsert");
    }

    public void testAddBatch1UseBulkCopyAPI() {
        testAddBatch1Internal("BulkCopy");
    }

    /**
     * Get a PreparedStatement object and call the addBatch() method with a 3 valid SQL statements and call the
     * executeBatch() method It should return an array of Integer values of length 3.
     */
    public void testExecuteBatch1() {
        testExecuteBatch1Internal("BatchInsert");
    }

    public void testExecuteBatch1UseBulkCopyAPI() {
        testExecuteBatch1Internal("BulkCopy");
    }

    private void testBatchUpdateCountWith(int numOfInserts, int errorQueryIndex,
            boolean prepareOnFirstPreparedStatement, String prepareMethod,
            long[] expectedUpdateCount) throws Exception {
        try (SQLServerConnection connection = PrepUtil.getConnection(connectionString)) {
            connection.setEnablePrepareOnFirstPreparedStatementCall(prepareOnFirstPreparedStatement);
            connection.setPrepareMethod(prepareMethod);
            try (CallableStatement cstmt = connection.prepareCall(
                    AbstractSQLGenerator.escapeIdentifier(ctstable3Procedure1) + " @duration=?, @value=?")) {
                cstmt.setQueryTimeout(7);
                for (int i = 1; i <= numOfInserts; i++) {
                    if (i == errorQueryIndex) {
                        cstmt.setString(1, "00:00:14");
                    } else {
                        cstmt.setString(1, "00:00:00");
                    }
                    cstmt.setInt(2, i);
                    cstmt.addBatch();
                }

                try {
                    cstmt.executeBatch();
                } catch (BatchUpdateException e) {
                    assertArrayEquals(expectedUpdateCount, e.getLargeUpdateCounts(),
                            "Actual: " + Arrays.toString(e.getLargeUpdateCounts()));
                }
            }
        }
    }

    private void testExecuteBatch1Internal(String mode) {
        int i = 0;
        int retValue[] = {0, 0, 0};
        int updateCountlen = 0;
        try (Connection connection = PrepUtil.getConnection(connectionString + ";columnEncryptionSetting=Enabled;");) {
            String sPrepStmt = "update " + AbstractSQLGenerator.escapeIdentifier(ctstable2)
                    + " set PRICE=PRICE*20 where TYPE_ID=?";

            if (mode.equalsIgnoreCase("bulkcopy")) {
                modifyConnectionForBulkCopyAPI((SQLServerConnection) connection);
            }

            try (PreparedStatement pstmt = connection.prepareStatement(sPrepStmt)) {
                pstmt.setInt(1, 1);
                pstmt.addBatch();

                pstmt.setInt(1, 2);
                pstmt.addBatch();

                pstmt.setInt(1, 3);
                pstmt.addBatch();

                int[] updateCount = pstmt.executeBatch();

                updateCountlen = updateCount.length;

                assertTrue(updateCountlen == 3, TestResource.getResource("R_executeBatchFailed") + ": "
                        + TestResource.getResource("R_incorrectUpdateCount"));

                String sPrepStmt1 = "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(ctstable2)
                        + " where TYPE_ID=?";

                try (PreparedStatement pstmt1 = connection.prepareStatement(sPrepStmt1)) {
                    for (int n = 1; n <= 3; n++) {
                        pstmt1.setInt(1, n);
                        try (ResultSet rs = pstmt1.executeQuery()) {
                            rs.next();
                            retValue[i++] = rs.getInt(1);
                        }
                    }
                }

                for (int j = 0; j < updateCount.length; j++) {
                    if (updateCount[j] != retValue[j] && updateCount[j] != Statement.SUCCESS_NO_INFO) {
                        fail(TestResource.getResource("R_executeBatchFailed") + ": "
                                + TestResource.getResource("R_incorrectUpdateCount"));
                    }
                }
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_executeBatchFailed") + ": " + e.getMessage());
        }
    }

    private static void createProcedure() throws SQLException {
        String sql1 = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(ctstable3Procedure1) + "\n"
                + "@value int,\n" + "@duration varchar(8)\n" + "AS\n" + "BEGIN\n" + "WAITFOR DELAY @duration;\n"
                + "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(ctstable3) + " VALUES (@value);\n" + "END";

        try (Connection connection = PrepUtil.getConnection(connectionString);
                Statement stmt = (SQLServerStatement) connection.createStatement()) {
            stmt.execute(sql1);
        } ;
    }

    private static void createTable() throws SQLException {
        try (Connection connection = PrepUtil.getConnection(connectionString + ";columnEncryptionSetting=Enabled;");
                Statement stmt = (SQLServerStatement) connection.createStatement()) {
            String sql1 = "create table " + AbstractSQLGenerator.escapeIdentifier(ctstable1)
                    + " (TYPE_ID int, TYPE_DESC varchar(32), primary key(TYPE_ID)) ";
            String sql2 = "create table " + AbstractSQLGenerator.escapeIdentifier(ctstable2)
                    + " (KEY_ID int,  COF_NAME varchar(32),  PRICE float, TYPE_ID int, primary key(KEY_ID), foreign key(TYPE_ID) references "
                    + AbstractSQLGenerator.escapeIdentifier(ctstable1) + ")";
            String sql3 = "create table " + AbstractSQLGenerator.escapeIdentifier(ctstable3) + "(C1 int)";
            String sql4 = "create table " + AbstractSQLGenerator.escapeIdentifier(ctstable4)
                    + "(C1 int check (C1 > 0))";
            stmt.execute(sql1);
            stmt.execute(sql2);
            stmt.execute(sql3);
            stmt.execute(sql4);

            String sqlin2 = "insert into " + AbstractSQLGenerator.escapeIdentifier(ctstable1)
                    + " values (1,'COFFEE-Desc')";
            stmt.execute(sqlin2);
            sqlin2 = "insert into " + AbstractSQLGenerator.escapeIdentifier(ctstable1) + " values (2,'COFFEE-Desc2')";
            stmt.execute(sqlin2);
            sqlin2 = "insert into " + AbstractSQLGenerator.escapeIdentifier(ctstable1) + " values (3,'COFFEE-Desc3')";
            stmt.execute(sqlin2);

            String sqlin1 = "insert into " + AbstractSQLGenerator.escapeIdentifier(ctstable2)
                    + " values (9,'COFFEE-9',9.0, 1)";
            stmt.execute(sqlin1);
            sqlin1 = "insert into " + AbstractSQLGenerator.escapeIdentifier(ctstable2)
                    + " values (10,'COFFEE-10',10.0, 2)";
            stmt.execute(sqlin1);
            sqlin1 = "insert into " + AbstractSQLGenerator.escapeIdentifier(ctstable2)
                    + " values (11,'COFFEE-11',11.0, 3)";
            stmt.execute(sqlin1);
        }
    }

    private void testAddBatch1Internal(String mode) {
        int i = 0;
        int retValue[] = {0, 0, 0};
        try (Connection connection = PrepUtil.getConnection(connectionString + ";columnEncryptionSetting=Enabled;");) {
            String sPrepStmt = "update " + AbstractSQLGenerator.escapeIdentifier(ctstable2)
                    + " set PRICE=PRICE*20 where TYPE_ID=?";

            if (mode.equalsIgnoreCase("bulkcopy")) {
                modifyConnectionForBulkCopyAPI((SQLServerConnection) connection);
            }

            try (PreparedStatement pstmt = connection.prepareStatement(sPrepStmt)) {
                pstmt.setInt(1, 2);
                pstmt.addBatch();

                pstmt.setInt(1, 3);
                pstmt.addBatch();

                pstmt.setInt(1, 4);
                pstmt.addBatch();

                int[] updateCount = pstmt.executeBatch();
                int updateCountlen = updateCount.length;

                assertTrue(updateCountlen == 3, TestResource.getResource("R_addBatchFailed") + ": "
                        + TestResource.getResource("R_incorrectUpdateCount"));

                String sPrepStmt1 = "select count(*) from " + AbstractSQLGenerator.escapeIdentifier(ctstable2)
                        + " where TYPE_ID=?";

                try (PreparedStatement pstmt1 = connection.prepareStatement(sPrepStmt1)) {

                    // 2 is the number that is set First for Type Id in Prepared Statement
                    for (int n = 2; n <= 4; n++) {
                        pstmt1.setInt(1, n);
                        try (ResultSet rs = pstmt1.executeQuery()) {
                            rs.next();
                            retValue[i++] = rs.getInt(1);
                        }
                    }
                }

                for (int j = 0; j < updateCount.length; j++) {

                    if (updateCount[j] != retValue[j] && updateCount[j] != Statement.SUCCESS_NO_INFO) {
                        fail(TestResource.getResource("R_incorrectUpdateCount"));
                    }
                }
            }
        } catch (Exception e) {
            fail(TestResource.getResource("R_addBatchFailed") + ": " + e.getMessage());
        }
    }

    private void modifyConnectionForBulkCopyAPI(SQLServerConnection con) throws Exception {
        Field f1 = SQLServerConnection.class.getDeclaredField("isAzureDW");
        f1.setAccessible(true);
        f1.set(con, true);

        con.setUseBulkCopyForBatchInsert(true);
    }

    private static String repeat(String string, int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            sb.append(string);
        }
        return sb.toString();
    }

    private static Thread cancelAsync(Statement statement, long delayInMilliseconds) {
        Thread thread = new Thread(() -> {
            try {
                Thread.sleep(delayInMilliseconds);
                statement.cancel();
            } catch (SQLException | InterruptedException e) {
                // does not/must not happen
                e.printStackTrace();
                throw new IllegalStateException(e);
            }
        });
        thread.start();
        return thread;
    }

    @BeforeAll
    public static void testSetup() throws TestAbortedException, Exception {
        setConnection();

        ctstable1 = RandomUtil.getIdentifier("ctstable1");
        ctstable2 = RandomUtil.getIdentifier("ctstable2");
        ctstable3 = RandomUtil.getIdentifier("ctstable3");
        ctstable4 = RandomUtil.getIdentifier("ctstable4");
        ctstable3Procedure1 = RandomUtil.getIdentifier("ctstable3Procedure1");

        dropTable();
        createTable();

        dropProcedure();
        createProcedure();
    }

    private static void dropProcedure() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(ctstable3Procedure1), stmt);
        }
    }

    private static void dropTable() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(ctstable2), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(ctstable1), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(ctstable3), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(ctstable4), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(ctstable4), stmt);
            TestUtils.dropTableIfExists(timestampTable1, stmt);
            TestUtils.dropTableIfExists(timestampTable2, stmt);
        }
    }

    @AfterAll
    public static void terminateVariation() throws Exception {
        dropProcedure();
        dropTable();
    }
}
