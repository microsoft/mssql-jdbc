/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
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
import com.microsoft.sqlserver.testframework.DBConnection;


/**
 * Tests batch execution with AE On connection
 *
 */
@RunWith(JUnitPlatform.class)
public class BatchExecutionTest extends AbstractTest {

    static String ctstable1;
    static String ctstable2;

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

    private void testExecuteBatch1Internal(String mode) {
        int i = 0;
        int retValue[] = {0, 0, 0};
        int updateCountlen = 0;
        try (Connection connection = DriverManager
                .getConnection(connectionString + ";columnEncryptionSetting=Enabled;");) {
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

    private static void createTable() throws SQLException {
        try (Connection connection = DriverManager
                .getConnection(connectionString + ";columnEncryptionSetting=Enabled;");
                Statement stmt = (SQLServerStatement) connection.createStatement()) {
            String sql1 = "create table " + AbstractSQLGenerator.escapeIdentifier(ctstable1)
                    + " (TYPE_ID int, TYPE_DESC varchar(32), primary key(TYPE_ID)) ";
            String sql2 = "create table " + AbstractSQLGenerator.escapeIdentifier(ctstable2)
                    + " (KEY_ID int,  COF_NAME varchar(32),  PRICE float, TYPE_ID int, primary key(KEY_ID), foreign key(TYPE_ID) references "
                    + AbstractSQLGenerator.escapeIdentifier(ctstable1) + ")";
            stmt.execute(sql1);
            stmt.execute(sql2);

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
        try (Connection connection = DriverManager
                .getConnection(connectionString + ";columnEncryptionSetting=Enabled;");) {
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

    @BeforeAll
    public static void testSetup() throws TestAbortedException, Exception {
        ctstable1 = RandomUtil.getIdentifier("ctstable1");
        ctstable2 = RandomUtil.getIdentifier("ctstable2");

        try (DBConnection con = new DBConnection(connectionString)) {
            assumeTrue(13 <= con.getServerVersion(), TestResource.getResource("R_Incompat_SQLServerVersion"));
        }

        dropTable();
        createTable();
    }

    private static void dropTable() throws SQLException {
        try (Connection connection = DriverManager
                .getConnection(connectionString + ";columnEncryptionSetting=Enabled;");
                Statement stmt = (SQLServerStatement) connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(ctstable2), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(ctstable1), stmt);
        }
    }

    @AfterAll
    public static void terminateVariation() throws Exception {
        try (DBConnection con = new DBConnection(connectionString)) {
            assumeTrue(13 <= con.getServerVersion(), TestResource.getResource("R_Incompat_SQLServerVersion"));
        }

        dropTable();
    }
}
