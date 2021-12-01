/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.resiliency;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@Tag(Constants.xSQLv11)
public class ResultSetsWithResiliencyTest extends AbstractTest {
    static String tableName = "[" + RandomUtil.getIdentifier("resTable") + "]";
    static int numberOfRows = 10;

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();

        try (Connection c = DriverManager.getConnection(connectionString); Statement s = c.createStatement();) {
            TestUtils.dropTableIfExists(tableName, s);
            createTable(s);
            insertData(s);
        }
    }

    /*
     * Execute statement with adaptive buffering on a broken connection.
     */
    @Test
    public void testAdaptiveBuffering() throws SQLException, InterruptedException {
        verifyResultSetResponseBuffering("adaptive", true);
    }

    /*
     * Execute statement with full buffering on a broken connection.
     */
    @Test
    public void testFullBuffering() throws SQLException, InterruptedException {
        verifyResultSetResponseBuffering("full", true);
    }

    /*
     * Execute statement with adaptive buffering and no strong reference to result set.
     */
    @Test
    public void testAdaptiveBufferingNoStrongReferenceToResultSet() throws SQLException, InterruptedException {
        verifyResultSetResponseBuffering("adaptive", false);
    }

    /*
     * Execute statement with full buffering and no strong reference to result set.
     */
    @Test
    public void testFullBufferingNoStrongReferenceToResultSet() throws SQLException, InterruptedException {
        verifyResultSetResponseBuffering("full", false);
    }

    /*
     * Execute statement when previous result set completely buffered but partially parsed by application.
     */
    @Test
    public void testFullBufferingWithPartiallyParsedResultSet() throws SQLException {
        try (Connection c = ResiliencyUtils.getConnection(connectionString + ";responseBuffering=full");
                Statement s = c.createStatement(); Statement s2 = c.createStatement()) {
            int sessionId = ResiliencyUtils.getSessionId(c);
            try (ResultSet rs = s.executeQuery("SELECT * FROM sys.syslanguages")) {
                // Partially parsed
                rs.next();
                rs.getString(2);
                ResiliencyUtils.killConnection(sessionId, connectionString, c);
                // ResultSet is not completely parsed, connection recovery is disabled.
                s2.execute("SELECT 1");
                fail("Driver should not have succesfully reconnected but it did.");
            }
        } catch (SQLServerException e) {
            if (!e.getMessage().matches(TestUtils.formatErrorMsg("R_crClientUnrecoverable"))) {
                e.printStackTrace();
            }
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_crClientUnrecoverable")));
            
        }
    }

    /*
     * Execute statement when previous result set partially buffered 
     */
    @Test
    public void testAdaptiveBufferingWithPartiallyBufferedResultSet() throws SQLException {
        // The table must contain enough rows to partially buffer the result set.
        try (Connection c = ResiliencyUtils.getConnection(connectionString + ";responseBuffering=adaptive");
                Statement s = c.createStatement(); Statement s2 = c.createStatement()) {
            int sessionId = ResiliencyUtils.getSessionId(c);
            try (ResultSet rs = s.executeQuery("SELECT * FROM " + tableName + " ORDER BY id;")) {
                ResiliencyUtils.killConnection(sessionId, connectionString, c);
                // ResultSet is partially buffered, connection recovery is disabled.
                s2.execute("SELECT 1");
                fail("Driver should not have succesfully reconnected but it did.");
            }
        } catch (SQLServerException e) {
            assertTrue("08S01" == e.getSQLState()
                    || e.getMessage().matches(TestUtils.formatErrorMsg("R_crClientUnrecoverable")));
            
        }
    }

    private static void createTable(Statement s) throws SQLException {
        s.execute("CREATE TABLE " + tableName + " (id int IDENTITY, data varchar(50));");
    }

    private static void insertData(Statement s) throws SQLException {
        for (int i = 1; i <= numberOfRows; i++) {
            s.executeUpdate("INSERT INTO " + tableName + " VALUES ('testData" + i + "');");
        }
    }

    private void verifyResultSet(ResultSet rs) throws SQLException {
        int count = 0;
        while (rs.next()) {
            count++;
            if (!("testData" + count).equals(rs.getString(2))) {
                fail();
            }
        }
        assertEquals(numberOfRows, count);
    }

    private void verifyResultSetResponseBuffering(String responseBuffering,
            boolean strongReferenceToResultSet) throws SQLException, InterruptedException {
        try (Connection c = ResiliencyUtils.getConnection(connectionString + ";responseBuffering=" + responseBuffering);
                Statement s = c.createStatement()) {
            ResiliencyUtils.killConnection(c, connectionString);
            if (strongReferenceToResultSet) {
                try (ResultSet rs = s.executeQuery("SELECT * FROM " + tableName + " ORDER BY id;")) {
                    verifyResultSet(rs);
                }
            } else {
                s.executeQuery("SELECT * FROM " + tableName + " ORDER BY id;");
            }
        }
    }

    @AfterAll
    public static void cleanUp() throws SQLException {
        try (Connection c = DriverManager.getConnection(connectionString); Statement s = c.createStatement()) {
            TestUtils.dropTableIfExists(tableName, s);
        }
    }
}
