/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.Utils;
import com.microsoft.sqlserver.testframework.util.RandomUtil;

/**
 * Tests batch execution with errors
 *
 */
@RunWith(JUnitPlatform.class)
public class BatchExecuteWithErrorsTest extends AbstractTest {

    public static final Logger log = Logger.getLogger("BatchExecuteWithErrors");
    Connection con = null;
    String tableN = RandomUtil.getIdentifier("t_Repro47239");
    final String tableName = AbstractSQLGenerator.escapeIdentifier(tableN);
    final String insertStmt = "INSERT INTO " + tableName + " VALUES (999, 'HELLO', '4/12/1994')";
    final String error16 = "RAISERROR ('raiserror level 16',16,42)";
    final String select = "SELECT 1";
    final String dateConversionError = "insert into " + tableName + " values (999999, 'Hello again', 'asdfasdf')";

    /**
     * Batch test
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("Batch Test")
    public void Repro47239() throws SQLException {
        String tableN = RandomUtil.getIdentifier("t_Repro47239");
        final String tableName = AbstractSQLGenerator.escapeIdentifier(tableN);
        final String insertStmt = "INSERT INTO " + tableName + " VALUES (999, 'HELLO', '4/12/1994')";
        final String error16 = "RAISERROR ('raiserror level 16',16,42)";
        final String select = "SELECT 1";
        final String dateConversionError = "insert into " + tableName + " values (999999, 'Hello again', 'asdfasdf')";

        String warning;
        String error;
        String severe;
        con = DriverManager.getConnection(connectionString);
        if (DBConnection.isSqlAzure(con)) {
            // SQL Azure will throw exception for "raiserror WITH LOG", so the following RAISERROR statements have not "with log" option
            warning = "RAISERROR ('raiserror level 4',4,1)";
            error = "RAISERROR ('raiserror level 11',11,1)";
            // On SQL Azure, raising FATAL error by RAISERROR() is not supported and there is no way to
            // cut the current connection by a statement inside a SQL batch.
            // Details: Although one can simulate a fatal error (that cuts the connections) by dropping the database,
            // this simulation cannot be written entirely in TSQL (because it needs a new connection),
            // and thus it cannot be put into a TSQL batch and it is useless here.
            // So we have to skip the last scenario of this test case, i.e. "Test Severe (connection-closing) errors"
            // It is worthwhile to still execute the first 5 test scenarios of this test case, in order to have best test coverage.
            severe = "--Not executed when testing against SQL Azure";  // this is a dummy statement that never being executed on SQL Azure
        }
        else {
            warning = "RAISERROR ('raiserror level 4',4,1) WITH LOG";
            error = "RAISERROR ('raiserror level 11',11,1) WITH LOG";
            severe = "RAISERROR ('raiserror level 20',20,1) WITH LOG";
        }
        con.close();

        int[] actualUpdateCounts;
        int[] expectedUpdateCounts;
        String actualExceptionText;

        // SQL Server 2005 driver
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        }
        catch (ClassNotFoundException e1) {
            fail(e1.toString());
        }
        Connection conn = DriverManager.getConnection(connectionString);
        Statement stmt = conn.createStatement();
        try {
            stmt.executeUpdate("drop table " + tableName);
        }
        catch (Exception ignored) {
        }
        stmt.executeUpdate(
                "create table " + tableName + " (c1_int int, c2_varchar varchar(20), c3_date datetime, c4_int int identity(1,1) primary key)");

        // Regular Statement batch update
        expectedUpdateCounts = new int[] {1, -2, 1, -2, 1, -2};
        Statement batchStmt = conn.createStatement();
        batchStmt.addBatch(insertStmt);
        batchStmt.addBatch(warning);
        batchStmt.addBatch(insertStmt);
        batchStmt.addBatch(warning);
        batchStmt.addBatch(insertStmt);
        batchStmt.addBatch(warning);
        try {
            actualUpdateCounts = batchStmt.executeBatch();
            actualExceptionText = "";
        }
        catch (BatchUpdateException bue) {
            actualUpdateCounts = bue.getUpdateCounts();
            actualExceptionText = bue.getMessage();
            if (log.isLoggable(Level.FINE)) {
                log.fine("BatchUpdateException occurred. Message:" + actualExceptionText);
            }
        }
        finally {
            batchStmt.close();
        }
        if (log.isLoggable(Level.FINE)) {
            log.fine("UpdateCounts:");
        }
        for (int updateCount : actualUpdateCounts) {
            log.fine("" + updateCount + ",");
        }
        log.fine("");
        assertTrue(Arrays.equals(actualUpdateCounts, expectedUpdateCounts), "Test interleaved inserts and warnings");

        expectedUpdateCounts = new int[] {-3, 1, 1, 1};
        stmt.addBatch(error);
        stmt.addBatch(insertStmt);
        stmt.addBatch(insertStmt);
        stmt.addBatch(insertStmt);
        try {
            actualUpdateCounts = stmt.executeBatch();
            actualExceptionText = "";
        }
        catch (BatchUpdateException bue) {
            actualUpdateCounts = bue.getUpdateCounts();
            actualExceptionText = bue.getMessage();
        }
        log.fine("UpdateCounts:");
        for (int updateCount : actualUpdateCounts) {
            log.fine("" + updateCount + ",");
        }
        log.fine("");
        assertTrue(Arrays.equals(actualUpdateCounts, expectedUpdateCounts), "Test error followed by inserts");
        // 50280
        expectedUpdateCounts = new int[] {1, -3};
        stmt.addBatch(insertStmt);
        stmt.addBatch(error16);
        try {
            actualUpdateCounts = stmt.executeBatch();
            actualExceptionText = "";
        }
        catch (BatchUpdateException bue) {
            actualUpdateCounts = bue.getUpdateCounts();
            actualExceptionText = bue.getMessage();
        }
        for (int updateCount : actualUpdateCounts) {
            log.fine("" + updateCount + ",");
        }
        log.fine("");
        assertTrue(Arrays.equals(actualUpdateCounts, expectedUpdateCounts), "Test insert followed by non-fatal error (50280)");

        // Test "soft" errors
        conn.setAutoCommit(false);
        stmt.addBatch(select);
        stmt.addBatch(insertStmt);
        stmt.addBatch(select);
        stmt.addBatch(insertStmt);
        try {
            stmt.executeBatch();
            assertEquals(true, false, "Soft error test: executeBatch unexpectedly succeeded");
        }
        catch (BatchUpdateException bue) {
            assertEquals("A result set was generated for update.", bue.getMessage(), "Soft error test: wrong error message in BatchUpdateException");
            assertEquals(Arrays.equals(bue.getUpdateCounts(), new int[] {-3, 1, -3, 1}), true,
                    "Soft error test: wrong update counts in BatchUpdateException");
        }
        conn.rollback();

        // Defect 128801: Rollback (with conversion error) should throw SQLException
        stmt.addBatch(dateConversionError);
        stmt.addBatch(insertStmt);
        stmt.addBatch(insertStmt);
        stmt.addBatch(insertStmt);
        try {
            stmt.executeBatch();
        }
        catch (BatchUpdateException bue) {
            assertThat(bue.getMessage(), containsString("Syntax error converting date"));
            // CTestLog.CompareStartsWith(bue.getMessage(), "Syntax error converting date", "Transaction rollback with conversion error threw wrong
            // BatchUpdateException");
        }
        catch (SQLException e) {
            assertThat(e.getMessage(), containsString("Conversion failed when converting date"));
            // CTestLog.CompareStartsWith(e.getMessage(), "Conversion failed when converting date", "Transaction rollback with conversion error threw
            // wrong SQLException");
        }

        conn.setAutoCommit(true);

        // On SQL Azure, raising FATAL error by RAISERROR() is not supported and there is no way to
        // cut the current connection by a statement inside a SQL batch.
        // Details: Although one can simulate a fatal error (that cuts the connections) by dropping the database,
        // this simulation cannot be written entirely in TSQL (because it needs a new connection),
        // and thus it cannot be put into a TSQL batch and it is useless here.
        // So we have to skip the last scenario of this test case, i.e. "Test Severe (connection-closing) errors"
        // It is worthwhile to still execute the first 5 test scenarios of this test case, in order to have best test coverage.
        if (!DBConnection.isSqlAzure(conn)) {
            // Test Severe (connection-closing) errors
            stmt.addBatch(error);
            stmt.addBatch(insertStmt);
            stmt.addBatch(warning);
            // TODO Removed until ResultSet refactoring task (45832) is complete.
            // stmt.addBatch(select); // error: select not permitted in batch
            stmt.addBatch(insertStmt);
            stmt.addBatch(severe);
            stmt.addBatch(insertStmt);
            stmt.addBatch(insertStmt);
            try {
                stmt.executeBatch();
                assertEquals(false, true, "Test fatal errors batch execution succeeded (should have failed)");
            }
            catch (BatchUpdateException bue) {
                assertEquals(false, true, "Test fatal errors returned BatchUpdateException rather than SQLException");
            }
            catch (SQLException e) {
                actualExceptionText = e.getMessage();

                if (actualExceptionText.endsWith("reset")) {
                    assertTrue(actualExceptionText.equalsIgnoreCase("Connection reset"), "Test fatal errors");
                }
                else {
                    assertTrue(actualExceptionText.equalsIgnoreCase("raiserror level 20"), "Test fatal errors");
                }
            }
        }

        try {
            stmt.executeUpdate("drop table " + tableName);
        }
        catch (Exception ignored) {
        }
        stmt.close();
        conn.close();
    }

    /**
     * Tests large methods, supported in 42
     * 
     * @throws Exception
     */
    @Test
    @DisplayName("Regression test for using 'large' methods")
    public void Repro47239large() throws Exception {

        assumeTrue("JDBC42".equals(Utils.getConfiguredProperty("JDBC_Version")), "Aborting test case as JDBC version is not compatible. ");
        // the DBConnection for detecting whether the server is SQL Azure or SQL Server.
        con = DriverManager.getConnection(connectionString);
        final String warning;
        final String error;
        final String severe;
        if (DBConnection.isSqlAzure(con)) {
            // SQL Azure will throw exception for "raiserror WITH LOG", so the following RAISERROR statements have not "with log" option
            warning = "RAISERROR ('raiserror level 4',4,1)";
            error = "RAISERROR ('raiserror level 11',11,1)";
            // On SQL Azure, raising FATAL error by RAISERROR() is not supported and there is no way to
            // cut the current connection by a statement inside a SQL batch.
            // Details: Although one can simulate a fatal error (that cuts the connections) by dropping the database,
            // this simulation cannot be written entirely in TSQL (because it needs a new connection),
            // and thus it cannot be put into a TSQL batch and it is useless here.
            // So we have to skip the last scenario of this test case, i.e. "Test Severe (connection-closing) errors"
            // It is worthwhile to still execute the first 5 test scenarios of this test case, in order to have best test coverage.
            severe = "--Not executed when testing against SQL Azure";  // this is a dummy statement that never being executed on SQL Azure
        }
        else {
            warning = "RAISERROR ('raiserror level 4',4,1) WITH LOG";
            error = "RAISERROR ('raiserror level 11',11,1) WITH LOG";
            severe = "RAISERROR ('raiserror level 20',20,1) WITH LOG";
        }
        con.close();

        long[] actualUpdateCounts;
        long[] expectedUpdateCounts;
        String actualExceptionText;

        // SQL Server 2005 driver
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection conn = DriverManager.getConnection(connectionString);
        Statement stmt = conn.createStatement();

        try {
            stmt.executeLargeUpdate("drop table " + tableName);
        }
        catch (Exception ignored) {
        }
        try {
            stmt.executeLargeUpdate(
                    "create table " + tableName + " (c1_int int, c2_varchar varchar(20), c3_date datetime, c4_int int identity(1,1) primary key)");
        }
        catch (Exception ignored) {
        }
        // Regular Statement batch update
        expectedUpdateCounts = new long[] {1, -2, 1, -2, 1, -2};
        Statement batchStmt = conn.createStatement();
        batchStmt.addBatch(insertStmt);
        batchStmt.addBatch(warning);
        batchStmt.addBatch(insertStmt);
        batchStmt.addBatch(warning);
        batchStmt.addBatch(insertStmt);
        batchStmt.addBatch(warning);
        try {
            actualUpdateCounts = batchStmt.executeLargeBatch();
            actualExceptionText = "";
        }
        catch (BatchUpdateException bue) {
            actualUpdateCounts = bue.getLargeUpdateCounts();
            actualExceptionText = bue.getMessage();
            log.fine("BatchUpdateException occurred. Message:" + actualExceptionText);
        }
        finally {
            batchStmt.close();
        }
        log.fine("UpdateCounts:");
        for (long updateCount : actualUpdateCounts) {
            log.fine("" + updateCount + ",");
        }
        log.fine("");
        assertTrue(Arrays.equals(actualUpdateCounts, expectedUpdateCounts), "Test interleaved inserts and warnings");

        expectedUpdateCounts = new long[] {-3, 1, 1, 1};
        stmt.addBatch(error);
        stmt.addBatch(insertStmt);
        stmt.addBatch(insertStmt);
        stmt.addBatch(insertStmt);
        try {
            actualUpdateCounts = stmt.executeLargeBatch();
            actualExceptionText = "";
        }
        catch (BatchUpdateException bue) {
            actualUpdateCounts = bue.getLargeUpdateCounts();
            actualExceptionText = bue.getMessage();
        }
        log.fine("UpdateCounts:");
        for (long updateCount : actualUpdateCounts) {
            log.fine("" + updateCount + ",");
        }
        log.fine("");
        assertTrue(Arrays.equals(actualUpdateCounts, expectedUpdateCounts), "Test error followed by inserts");

        // 50280
        expectedUpdateCounts = new long[] {1, -3};
        stmt.addBatch(insertStmt);
        stmt.addBatch(error16);
        try {
            actualUpdateCounts = stmt.executeLargeBatch();
            actualExceptionText = "";
        }
        catch (BatchUpdateException bue) {
            actualUpdateCounts = bue.getLargeUpdateCounts();
            actualExceptionText = bue.getMessage();
        }
        for (long updateCount : actualUpdateCounts) {
            log.fine("" + updateCount + ",");
        }
        log.fine("");
        assertTrue(Arrays.equals(actualUpdateCounts, expectedUpdateCounts), "Test insert followed by non-fatal error (50280)");

        // Test "soft" errors
        conn.setAutoCommit(false);
        stmt.addBatch(select);
        stmt.addBatch(insertStmt);
        stmt.addBatch(select);
        stmt.addBatch(insertStmt);
        try {
            stmt.executeLargeBatch();
            assertEquals(false, true, "Soft error test: executeLargeBatch unexpectedly succeeded");
        }
        catch (BatchUpdateException bue) {
            assertEquals("A result set was generated for update.", bue.getMessage(), "Soft error test: wrong error message in BatchUpdateException");
            assertEquals(Arrays.equals(bue.getLargeUpdateCounts(), new long[] {-3, 1, -3, 1}), true,
                    "Soft error test: wrong update counts in BatchUpdateException");
        }
        conn.rollback();

        // Defect 128801: Rollback (with conversion error) should throw SQLException
        stmt.addBatch(dateConversionError);
        stmt.addBatch(insertStmt);
        stmt.addBatch(insertStmt);
        stmt.addBatch(insertStmt);
        try {
            stmt.executeLargeBatch();
        }
        catch (BatchUpdateException bue) {
            assertThat(bue.getMessage(), containsString("Syntax error converting date"));
        }
        catch (SQLException e) {
            assertThat(e.getMessage(), containsString("Conversion failed when converting date"));
        }

        conn.setAutoCommit(true);

        // On SQL Azure, raising FATAL error by RAISERROR() is not supported and there is no way to
        // cut the current connection by a statement inside a SQL batch.
        // Details: Although one can simulate a fatal error (that cuts the connections) by dropping the database,
        // this simulation cannot be written entirely in TSQL (because it needs a new connection),
        // and thus it cannot be put into a TSQL batch and it is useless here.
        // So we have to skip the last scenario of this test case, i.e. "Test Severe (connection-closing) errors"
        // It is worthwhile to still execute the first 5 test scenarios of this test case, in order to have best test coverage.
        if (!DBConnection.isSqlAzure(DriverManager.getConnection(connectionString))) {
            // Test Severe (connection-closing) errors
            stmt.addBatch(error);
            stmt.addBatch(insertStmt);
            stmt.addBatch(warning);

            stmt.addBatch(insertStmt);
            stmt.addBatch(severe);
            stmt.addBatch(insertStmt);
            stmt.addBatch(insertStmt);
            try {
                stmt.executeLargeBatch();
                assertEquals(false, true, "Test fatal errors batch execution succeeded (should have failed)");
            }
            catch (BatchUpdateException bue) {
                assertEquals(false, true, "Test fatal errors returned BatchUpdateException rather than SQLException");
            }
            catch (SQLException e) {
                actualExceptionText = e.getMessage();

                if (actualExceptionText.endsWith("reset")) {
                    assertTrue(actualExceptionText.equalsIgnoreCase("Connection reset"), "Test fatal errors");
                }
                else {
                    assertTrue(actualExceptionText.equalsIgnoreCase("raiserror level 20"), "Test fatal errors");

                }
            }
        }

        try {
            stmt.executeLargeUpdate("drop table " + tableName);
        }
        catch (Exception ignored) {
        }
        stmt.close();
        conn.close();
    }
}
