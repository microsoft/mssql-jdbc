/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;


/**
 * Tests batch execution with trigger exception
 *
 */
@RunWith(JUnitPlatform.class)
public class BatchTriggerTest extends AbstractTest {

    static String tableName = "triggerTable";
    static String triggerName = "triggerTest";
    static String insertQuery = "insert into " + tableName
            + " (col1, col2, col3, col4) values (1, '22-08-2017 17:30:00.000', 'R4760', 31)";

    /**
     * Tests that the proper trigger exception is thrown using statement
     * 
     * @throws SQLException
     */
    @Test
    public void statementTest() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString);
                Statement stmt = connection.createStatement()) {
            stmt.addBatch(insertQuery);
            stmt.executeBatch();
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            assertTrue(e.getMessage().equalsIgnoreCase(TestResource.getResource("R_customErrorMessage")));
        }
    }

    /**
     * Tests that the proper trigger exception is thrown using preparedSatement
     * 
     * @throws SQLException
     */
    @Test
    public void preparedStatementTest() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString);
                PreparedStatement pstmt = connection.prepareStatement(insertQuery)) {

            pstmt.addBatch();
            pstmt.executeBatch();
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {

            assertTrue(e.getMessage().equalsIgnoreCase(TestResource.getResource("R_customErrorMessage")));
        }
    }

    /**
     * Create the trigger
     * 
     * @param triggerName
     * @throws SQLException
     */
    private static void createTrigger(String triggerName) throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString);
                Statement stmt = connection.createStatement()) {
            String sql = "create trigger " + triggerName + " on " + tableName + " for insert " + "as " + "begin "
                    + "if (select col1 from " + tableName + ") > 10 " + "begin " + "return " + "end " + "RAISERROR ('"
                    + TestResource.getResource("R_customErrorMessage") + "', 16, 0) " + "rollback transaction " + "end";
            stmt.execute(sql);
        }
    }

    /**
     * Creating tables
     * 
     * @throws SQLException
     */
    private static void createTable() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString);
                Statement stmt = connection.createStatement()) {
            String sql = "create table " + tableName + " ( col1 int, col2 varchar(50), col3 varchar(10), col4 int)";
            stmt.execute(sql);
        }
    }

    /**
     * Setup test
     * 
     * @throws TestAbortedException
     * @throws Exception
     */
    @BeforeAll
    public static void testSetup() throws TestAbortedException, Exception {
        try (Connection connection = DriverManager.getConnection(connectionString);
                SQLServerStatement stmt = (SQLServerStatement) connection.createStatement()) {
            stmt.execute("IF EXISTS (\r\n" + "    SELECT *\r\n" + "    FROM sys.objects\r\n"
                    + "    WHERE [type] = 'TR' AND [name] = '" + triggerName + "'\r\n" + "    )\r\n"
                    + "    DROP TRIGGER " + triggerName + ";");
            dropTable();
            createTable();
            createTrigger(triggerName);
        }
    }

    /**
     * Drop the table
     * 
     * @throws SQLException
     */
    private static void dropTable() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString);
                Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }

    /**
     * Cleaning up
     * 
     * @throws SQLException
     */
    @AfterAll
    public static void terminateVariation() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString);
                SQLServerStatement stmt = (SQLServerStatement) connection.createStatement()) {

            dropTable();
            stmt.execute("IF EXISTS (\r\n" + "    SELECT *\r\n" + "    FROM sys.objects\r\n"
                    + "    WHERE [type] = 'TR' AND [name] = '" + triggerName + "'\r\n" + "    )\r\n"
                    + "    DROP TRIGGER " + triggerName + ";");
        }
    }
}
