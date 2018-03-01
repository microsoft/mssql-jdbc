/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.BatchUpdateException;
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

import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.Utils;

/**
 * Tests batch execution with AE On connection
 *
 */
@RunWith(JUnitPlatform.class)
public class BatchExecutionTest extends AbstractTest {

    static Statement stmt = null;
    static Connection connection = null;
    static PreparedStatement pstmt = null;
    static PreparedStatement pstmt1 = null;
    static ResultSet rs = null;

    /**
     * testAddBatch1 and testExecutionBatch one looks similar except for the parameters being passed for select query. 
     * TODO: we should look and simply the test later by parameterized values
     * @throws Exception
     */
    @Test
    public void testBatchExceptionAEOn() throws Exception {
        testAddBatch1();
        testExecuteBatch1();
    }

    /**
     * Get a PreparedStatement object and call the addBatch() method with 3 SQL statements and call the executeBatch() method and it should return
     * array of Integer values of length 3
     */
    public void testAddBatch1() {
        int i = 0;
        int retValue[] = {0, 0, 0};
        try {
            String sPrepStmt = "update ctstable2 set PRICE=PRICE*20 where TYPE_ID=?";
            pstmt = connection.prepareStatement(sPrepStmt);
            pstmt.setInt(1, 2);
            pstmt.addBatch();

            pstmt.setInt(1, 3);
            pstmt.addBatch();

            pstmt.setInt(1, 4);
            pstmt.addBatch();

            int[] updateCount = pstmt.executeBatch();
            int updateCountlen = updateCount.length;

            assertTrue(updateCountlen == 3, "addBatch does not add the SQL Statements to Batch ,call to addBatch failed");

            String sPrepStmt1 = "select count(*) from ctstable2 where TYPE_ID=?";

            pstmt1 = connection.prepareStatement(sPrepStmt1);

            // 2 is the number that is set First for Type Id in Prepared Statement
            for (int n = 2; n <= 4; n++) {
                pstmt1.setInt(1, n);
                rs = pstmt1.executeQuery();
                rs.next();
                retValue[i++] = rs.getInt(1);
            }

            pstmt1.close();

            for (int j = 0; j < updateCount.length; j++) {

                if (updateCount[j] != retValue[j] && updateCount[j] != Statement.SUCCESS_NO_INFO) {
                    fail("affected row count does not match with the updateCount value, Call to addBatch is Failed!");
                }
            }
        }
        catch (BatchUpdateException b) {
            fail("BatchUpdateException :  Call to addBatch is Failed!");
        }
        catch (SQLException sqle) {
            fail("Call to addBatch is Failed!");
        }
        catch (Exception e) {
            fail("Call to addBatch is Failed!");
        }
    }

    /**
     * Get a PreparedStatement object and call the addBatch() method with a 3 valid SQL statements and call the executeBatch() method It should return
     * an array of Integer values of length 3.
     */
    public void testExecuteBatch1() {
        int i = 0;
        int retValue[] = {0, 0, 0};
        int updCountLength = 0;
        try {
            String sPrepStmt = "update ctstable2 set PRICE=PRICE*20 where TYPE_ID=?";

            pstmt = connection.prepareStatement(sPrepStmt);
            pstmt.setInt(1, 1);
            pstmt.addBatch();

            pstmt.setInt(1, 2);
            pstmt.addBatch();

            pstmt.setInt(1, 3);
            pstmt.addBatch();

            int[] updateCount = pstmt.executeBatch();
            updCountLength = updateCount.length;

            assertTrue(updCountLength == 3, "executeBatch does not execute the Batch of SQL statements, Call to executeBatch is Failed!");

            String sPrepStmt1 = "select count(*) from ctstable2 where TYPE_ID=?";

            pstmt1 = connection.prepareStatement(sPrepStmt1);

            for (int n = 1; n <= 3; n++) {
                pstmt1.setInt(1, n);
                rs = pstmt1.executeQuery();
                rs.next();
                retValue[i++] = rs.getInt(1);
            }

            pstmt1.close();

            for (int j = 0; j < updateCount.length; j++) {
                if (updateCount[j] != retValue[j] && updateCount[j] != Statement.SUCCESS_NO_INFO) {
                    fail("executeBatch does not execute the Batch of SQL statements, Call to executeBatch is Failed!");
                }
            }
        }
        catch (BatchUpdateException b) {
            fail("BatchUpdateException :  Call to executeBatch is Failed!");
        }
        catch (SQLException sqle) {
            fail("Call to executeBatch is Failed!");
        }
        catch (Exception e) {
            fail("Call to executeBatch is Failed!");
        }
    }

    private static void createTable() throws SQLException {
        String sql1 = "create table ctstable1 (TYPE_ID int, TYPE_DESC varchar(32), primary key(TYPE_ID)) ";
        String sql2 = "create table ctstable2 (KEY_ID int,  COF_NAME varchar(32),  PRICE float, TYPE_ID int, primary key(KEY_ID), foreign key(TYPE_ID) references ctstable1) ";
        stmt.execute(sql1);
        stmt.execute(sql2);

        String sqlin2 = "insert into ctstable1 values (1,'COFFEE-Desc')";
        stmt.execute(sqlin2);
        sqlin2 = "insert into ctstable1 values (2,'COFFEE-Desc2')";
        stmt.execute(sqlin2);
        sqlin2 = "insert into ctstable1 values (3,'COFFEE-Desc3')";
        stmt.execute(sqlin2);

        String sqlin1 = "insert into ctstable2 values (9,'COFFEE-9',9.0, 1)";
        stmt.execute(sqlin1);
        sqlin1 = "insert into ctstable2 values (10,'COFFEE-10',10.0, 2)";
        stmt.execute(sqlin1);
        sqlin1 = "insert into ctstable2 values (11,'COFFEE-11',11.0, 3)";
        stmt.execute(sqlin1);

    }

    @BeforeAll
    public static void testSetup() throws TestAbortedException, Exception {
        assumeTrue(13 <= new DBConnection(connectionString).getServerVersion(),
                "Aborting test case as SQL Server version is not compatible with Always encrypted ");
        connection = DriverManager.getConnection(connectionString + ";columnEncryptionSetting=Enabled;");
        stmt = (SQLServerStatement) connection.createStatement();
        dropTable();
        createTable();
    }

    private static void dropTable() throws SQLException {
        Utils.dropTableIfExists("ctstable2", stmt);
        Utils.dropTableIfExists("ctstable1", stmt);
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {

        dropTable();

        if (null != connection) {
            connection.close();
        }
        if (null != pstmt) {
            pstmt.close();
        }
        if (null != pstmt1) {
            pstmt1.close();
        }
        if (null != stmt) {
            stmt.close();
        }
        if (null != rs) {
            rs.close();
        }
    }
}