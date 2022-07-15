/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.resiliency;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@Tag(Constants.xSQLv11)
@Tag(Constants.xAzureSQLDW)
public class ResultSetsWithResiliencyTest extends AbstractTest {
    static String tableName = AbstractSQLGenerator.escapeIdentifier("resilencyTestTable");
    static int numberOfRows = 10;

    @BeforeAll
    public static void setupTests() throws Exception {
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
                ResiliencyUtils.killConnection(sessionId, connectionString, c, 0);
                // ResultSet is not completely parsed, connection recovery is disabled.
                s2.execute("SELECT 1");
                fail("Driver should not have succesfully reconnected but it did.");
            }
        } catch (SQLServerException e) {
            if (!e.getMessage().matches(TestUtils.formatErrorMsg("R_crClientUnrecoverable"))) {
                e.printStackTrace();
            }
            assertTrue(e.getMessage().matches(TestUtils.formatErrorMsg("R_crClientUnrecoverable")), e.getMessage());

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
                ResiliencyUtils.killConnection(sessionId, connectionString, c, 0);
                // ResultSet is partially buffered, connection recovery is disabled.
                s2.execute("SELECT 1");
                fail("Driver should not have succesfully reconnected but it did.");
            }
        } catch (SQLServerException e) {
            assertTrue(
                    "08S01" == e.getSQLState()
                            || e.getMessage().matches(TestUtils.formatErrorMsg("R_crClientUnrecoverable")),
                    e.getMessage());

        }
    }

    /*
     * Test killing a session while retrieving result set should result in exception thrown
     */
    @Test
    public void testKillSession() throws Exception {
        // setup test with big tables
        String table1 = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("killSessionTestTable1"));
        String table2 = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("killSessionTestTable2"));
        String table3 = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("killSessionTestTable3"));

        try (Connection c = DriverManager.getConnection(connectionString); Statement s = c.createStatement();
                PreparedStatement ps1 = c.prepareStatement("INSERT INTO " + table1 + " values (?)");
                PreparedStatement ps2 = c.prepareStatement("INSERT INTO " + table2 + " values (?)");
                PreparedStatement ps3 = c.prepareStatement("INSERT INTO " + table3 + " values (?)")) {
            TestUtils.dropTableIfExists(table1, s);
            TestUtils.dropTableIfExists(table2, s);
            TestUtils.dropTableIfExists(table3, s);
            s.execute("CREATE TABLE " + table1
                    + " (ID int primary key IDENTITY(1,1) NOT NULL, NAME varchar(255) NOT NULL); CREATE TABLE " + table2
                    + " (ID int primary key IDENTITY(1,1) NOT NULL, NAME varchar(255) NOT NULL); CREATE TABLE " + table3
                    + "( ID int primary key IDENTITY(1,1) NOT NULL, NAME varchar(255) NOT NULL);");

            for (int i = 0; i < 1000; i++) {
                ps1.setString(1, "value" + i);
                ps2.setString(1, "value" + i);
                ps3.setString(1, "value" + i);

                ps1.addBatch();
                ps2.addBatch();
                ps3.addBatch();
            }

            ps1.executeBatch();
            ps2.executeBatch();
            ps3.executeBatch();

            c.commit();

            try (Connection c2 = DriverManager.getConnection(connectionString)) {
                int sessionId = ResiliencyUtils.getSessionId(c2);

                Runnable r1 = () -> {
                    try {
                        ResiliencyUtils.killConnection(sessionId, connectionString, c2, 10);
                    } catch (Exception e) {
                        fail(e.getMessage());;
                    }
                };

                Thread t1 = new Thread(r1);
                t1.start();

                // execute query which takes a long time and kill session in another thread
                try (PreparedStatement ps = c2.prepareStatement("SELECT e1.* FROM " + table1 + " e1, " + table2
                        + " e2, " + table3 + " e3, " + table1
                        + " e4 where e1.name = 'abc' or e2.name = 'def'or e3.name = 'ghi' or e4.name = 'xxx' and e1.name not in (select name  FROM "
                        + table2 + ") and e2.name not in (select name  FROM " + table1
                        + " ) and e3.name not in (SELECT name FROM " + table2
                        + ") and e4.name not in (SELECT name FROM " + table3 + ");");
                        ResultSet rs = ps.executeQuery()) {

                    fail(TestResource.getResource("R_expectedExceptionNotThrown"));
                } catch (SQLException e) {
                    assertTrue(
                            e.getMessage().matches(TestUtils.formatErrorMsg("R_serverError"))
                                    || e.getMessage().contains(TestResource.getResource("R_sessionKilled")),
                            e.getMessage());
                }
                t1.join();

            } finally {
                TestUtils.dropTableIfExists(table1, s);
                TestUtils.dropTableIfExists(table2, s);
                TestUtils.dropTableIfExists(table3, s);
            }
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
            ResiliencyUtils.killConnection(c, connectionString, 0);
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
