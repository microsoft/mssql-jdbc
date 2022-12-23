/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Field;
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
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
    private static String ctstable3Procedure;

    @Test
    public void testBatchUpdateCountFalseOnFirstPstmtPrepexec() throws Exception {
        long[] expectedUpdateCount = {1, 1, 1, 1, -3, -3, -3, -3, -3, -3};
        testBatchUpdateCountWith(10, 6, false, "prepexec", expectedUpdateCount);
    }

    @Test
    public void testBatchUpdateCountTrueOnFirstPstmtPrepexec() throws Exception {
        long[] expectedUpdateCount = {1, 1, -3, -3, -3};
        testBatchUpdateCountWith(5, 4, true, "prepexec", expectedUpdateCount);
    }

    @Test
    public void testBatchUpdateCountFalseOnFirstPstmtSpPrepare() throws Exception {
        long[] expectedUpdateCount = {1, 1, 1, 1, -3, -3, -3, -3, -3, -3};
        testBatchUpdateCountWith(10, 6, false, "prepare", expectedUpdateCount);
    }

    @Test
    public void testBatchUpdateCountTrueOnFirstPstmtSpPrepare() throws Exception {
        long[] expectedUpdateCount = {1, 1, -3, -3, -3};
        testBatchUpdateCountWith(5, 4, true, "prepare", expectedUpdateCount);
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
    public void testBatchStatementCancellation() throws Exception {
        try (Connection connection = PrepUtil.getConnection(connectionString)) {
            connection.setAutoCommit(false);

            try (PreparedStatement statement = connection.prepareStatement(
                    "if object_id('test_table') is not null drop table test_table")) {
                statement.execute();
            }
            connection.commit();

            try (PreparedStatement statement = connection.prepareStatement(
                    "create table test_table (column_name bit)")) {
                statement.execute();
            }
            connection.commit();

            for (long delayInMilliseconds : new long[] { 1, 2, 4, 8, 16, 32, 64, 128 }) {
                for (int numberOfCommands : new int[] { 1, 2, 4, 8, 16, 32, 64 }) {
                    int parameterCount = 512;

                    try (PreparedStatement statement = connection.prepareStatement(
                            "insert into test_table values (?)" + repeat(",(?)", parameterCount - 1))) {

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
                                          boolean prepareOnFirstPreparedStatement, String prepareMethod, long[] expectedUpdateCount) throws Exception {
        try (SQLServerConnection connection = PrepUtil.getConnection(connectionString)) {
            connection.setEnablePrepareOnFirstPreparedStatementCall(prepareOnFirstPreparedStatement);
            connection.setPrepareMethod(prepareMethod);
            try (CallableStatement cstmt = connection.prepareCall(AbstractSQLGenerator.escapeIdentifier(ctstable3Procedure) + " @duration=?, @value=?")) {
                cstmt.setQueryTimeout(2);
                for (int i = 1; i <= numOfInserts; i++) {
                    if (i == errorQueryIndex) {
                        cstmt.setString(1, "00:00:05");
                    } else {
                        cstmt.setString(1, "00:00:00");
                    }
                    cstmt.setInt(2, i);
                    cstmt.addBatch();
                }

                try {
                    cstmt.executeBatch();
                } catch (BatchUpdateException e) {
                    assertArrayEquals(expectedUpdateCount, e.getLargeUpdateCounts(), "Actual: "
                            + Arrays.toString(e.getLargeUpdateCounts()));
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
        String sql1 = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(ctstable3Procedure)+ "\n" +
                            "@value int,\n" +
                            "@duration varchar(8)\n" +
                            "AS\n" +
                            "BEGIN\n" +
                            "WAITFOR DELAY @duration;\n" +
                            "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(ctstable3) + " VALUES (@value);\n" +
                            "END";

        try (Connection connection = PrepUtil.getConnection(connectionString);
             Statement stmt = (SQLServerStatement) connection.createStatement()) {
            stmt.execute(sql1);
        };
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
            stmt.execute(sql1);
            stmt.execute(sql2);
            stmt.execute(sql3);

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
        ctstable3Procedure = RandomUtil.getIdentifier("ctstable3Procedure");

        dropTable();
        createTable();

        dropProcedure();
        createProcedure();
    }

    private static void dropProcedure() throws SQLException {
         try (Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(ctstable3Procedure), stmt);
        }
    }

    private static void dropTable() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(ctstable2), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(ctstable1), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(ctstable3), stmt);
        }
    }

    @AfterAll
    public static void terminateVariation() throws Exception {
        dropTable();
    }
}
