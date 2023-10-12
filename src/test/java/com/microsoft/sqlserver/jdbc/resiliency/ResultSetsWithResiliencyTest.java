/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.resiliency;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
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
    static String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("resiliencyTestTable"));
    static int numberOfRows = 10;

    private static String callableStatementICROnDoneTestSp = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CallableStatement_ICROnDoneTest_SP"));
    private static String callableStatementICROnDoneErrorTestSp = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("CallableStatement_ICROnDoneErrorTest_SP"));
    private static String createClientCursorInitTableQuery = "create table %s (col1 int, col2 varchar(8000), col3 int identity(1,1))";
    private static String createFetchBufferTableQuery = "create table %s (col1 int not null)";
    private static String insertIntoFetchBufferTableQuery = "insert into %s (col1) values (%s);";
    private static final String clientCursorInitTable1 = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("clientCursorInitTable1"));
    private static final String clientCursorInitTable2 = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("clientCursorInitTable2"));
    private static final String clientCursorInitTable3 = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("clientCursorInitTable3"));
    private static final String fetchBufferTestTable1 = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("fetchBufferTestTable1"));
    private static final String fetchBufferTestTable2 = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("fetchBufferTestTable2"));
    private static final String clientCursorInitTableQuery1 = "select * from " + clientCursorInitTable1;
    private static final String clientCursorInitTableQuery2 = "select * from " + clientCursorInitTable2;
    private static final String clientCursorInitTableQuery3 = "select * from " + clientCursorInitTable3;
    private static final String fetchBufferTableQuery1 = "select * from " + fetchBufferTestTable1;
    private static final String fetchBufferTableQuery2 = "select * from " + fetchBufferTestTable2;
    private static final String mockErrorMsg = "This is a mock query error.";
    private static final String errorQuery = "RAISERROR('" + mockErrorMsg + "', 16, 1)";

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();

        try (Connection c = DriverManager.getConnection(connectionString); Statement s = c.createStatement();) {
            TestUtils.dropTableIfExists(tableName, s);
            TestUtils.dropTableIfExists(clientCursorInitTable1, s);
            TestUtils.dropTableIfExists(clientCursorInitTable2, s);
            TestUtils.dropTableIfExists(clientCursorInitTable3, s);
            TestUtils.dropTableIfExists(fetchBufferTestTable1, s);
            TestUtils.dropTableIfExists(fetchBufferTestTable2, s);
            TestUtils.dropProcedureIfExists(callableStatementICROnDoneTestSp, s);
            TestUtils.dropProcedureIfExists(callableStatementICROnDoneErrorTestSp, s);

            createTable(s);
            insertData(s);

            createCallableStatementOnDoneTestSp(s);
            createCallableStatementOnDoneErrorTestSp(s);

            createTable(s, String.format(createClientCursorInitTableQuery, clientCursorInitTable1));
            createTable(s, String.format(createClientCursorInitTableQuery, clientCursorInitTable2));
            createTable(s, String.format(createClientCursorInitTableQuery, clientCursorInitTable3));
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

    @Test
    public void testResultSetClientCursorInitializerOnDone() throws SQLException {
        try (Connection con = ResiliencyUtils.getConnection(connectionString); Statement stmt = con.createStatement()) {

            boolean hasResults = stmt.execute(clientCursorInitTableQuery1 + "; " + clientCursorInitTableQuery2);
            while (hasResults) {
                ResultSet rs = stmt.getResultSet();
                while (rs.next()) {}
                hasResults = stmt.getMoreResults();
            }

            ResiliencyUtils.killConnection(con, connectionString, 1);

            try (ResultSet rs = con.createStatement().executeQuery(clientCursorInitTableQuery3)) {
                while (rs.next()) {}
            }
        }
    }

    @Test
    public void testResultSetErrorClientCursorInitializerOnDone() throws SQLException {
        try (Connection con = ResiliencyUtils.getConnection(connectionString); Statement stmt = con.createStatement()) {

            try {
                boolean hasResults = stmt.execute(clientCursorInitTableQuery1 + "; " + errorQuery);
                while (hasResults) {
                    ResultSet rs = stmt.getResultSet();
                    while (rs.next()) {}
                    hasResults = stmt.getMoreResults();
                }
            } catch (SQLServerException se) {
                if (!se.getMessage().equals(mockErrorMsg)) {
                    se.printStackTrace();
                    fail("Mock Sql Server error message was expected.");
                }
            }

            ResiliencyUtils.killConnection(con, connectionString, 1);

            try (ResultSet rs = con.createStatement().executeQuery(clientCursorInitTableQuery3)) {
                while (rs.next()) {}
            }
        }
    }

    @Test
    public void testCallableStatementOnDone() throws SQLException {
        String sql = "{CALL " + callableStatementICROnDoneTestSp + " (?, ?)}";

        try (Connection con = ResiliencyUtils.getConnection(connectionString)) {

            try (CallableStatement cs = con.prepareCall(sql)) {
                cs.registerOutParameter(1, Types.TIMESTAMP);
                cs.registerOutParameter(2, Types.TIMESTAMP);
                cs.execute();
                cs.execute();
                cs.execute();
            }

            ResiliencyUtils.killConnection(con, connectionString, 1);

            try (CallableStatement cs2 = con.prepareCall(sql)) {
                cs2.registerOutParameter(1, Types.TIMESTAMP);
                cs2.registerOutParameter(2, Types.TIMESTAMP);
                cs2.execute();
            }
        }
    }

    @Test
    public void testCallableStatementErrorOnDone() throws SQLException {
        String errorCallableStmt = "{CALL " + callableStatementICROnDoneErrorTestSp + " (?, ?)}";
        String validCallableStmt = "{CALL " + callableStatementICROnDoneTestSp + " (?, ?)}";

        try (Connection con = ResiliencyUtils.getConnection(connectionString)) {

            try (CallableStatement cs = con.prepareCall(errorCallableStmt)) {
                cs.registerOutParameter(1, Types.TIMESTAMP);
                cs.registerOutParameter(2, Types.TIMESTAMP);
                cs.execute();
            } catch (SQLServerException se) {
                if (!se.getMessage().equals(mockErrorMsg)) {
                    se.printStackTrace();
                    fail("Mock Sql Server error message was expected.");
                }
            }

            ResiliencyUtils.killConnection(con, connectionString, 1);

            try (CallableStatement cs2 = con.prepareCall(validCallableStmt)) {
                cs2.registerOutParameter(1, Types.TIMESTAMP);
                cs2.registerOutParameter(2, Types.TIMESTAMP);
                cs2.execute();
            }
        }
    }

    @Test
    public void testResultSetFetchBufferOnDone() throws SQLException {

        try (SQLServerConnection con = (SQLServerConnection) ResiliencyUtils.getConnection(connectionString)) {
            try (Statement stmt = con.createStatement()) {
                TestUtils.dropTableIfExists(fetchBufferTestTable1, stmt);
                createTable(stmt, String.format(createFetchBufferTableQuery, fetchBufferTestTable1));
                insertData(stmt, String.format(insertIntoFetchBufferTableQuery, fetchBufferTestTable1, 1), 10);
            }

            ResiliencyUtils.killConnection(con, connectionString, 1);

            try (Statement stmt = con.createStatement()) {
                TestUtils.dropTableIfExists(fetchBufferTestTable2, stmt);
                createTable(stmt, String.format(createFetchBufferTableQuery, fetchBufferTestTable2));
                insertData(stmt, String.format(insertIntoFetchBufferTableQuery, fetchBufferTestTable2, 1), 10);
            }

            try (Statement stmt = con.createStatement()) {
                boolean hasResults = stmt.execute(fetchBufferTableQuery1 + "; " + fetchBufferTableQuery2);
                while (hasResults) {
                    ResultSet rs = stmt.getResultSet();
                    while (rs.next()) {}
                    hasResults = stmt.getMoreResults();
                }
            }

            ResiliencyUtils.killConnection(con, connectionString, 1);

            try (Statement stmt = con.createStatement()) {
                ResultSet rs = stmt.executeQuery(fetchBufferTableQuery2);
                while (rs.next()) {}
            }
        }
    }

    @Test
    public void testResultSetErrorFetchBufferOnDone() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) ResiliencyUtils.getConnection(connectionString)) {
            try (Statement stmt = con.createStatement()) {
                TestUtils.dropTableIfExists(fetchBufferTestTable1, stmt);
                createTable(stmt, String.format(createFetchBufferTableQuery, fetchBufferTestTable1));
                insertData(stmt, errorQuery, 10);
            } catch (SQLServerException se) {
                if (!se.getMessage().equals(mockErrorMsg)) {
                    se.printStackTrace();
                    fail("Mock Sql Server error message was expected.");
                }
            }

            ResiliencyUtils.killConnection(con, connectionString, 1);

            try (Statement stmt = con.createStatement()) {
                TestUtils.dropTableIfExists(fetchBufferTestTable2, stmt);
                createTable(stmt, String.format(createFetchBufferTableQuery, fetchBufferTestTable2));
                insertData(stmt, String.format(insertIntoFetchBufferTableQuery, fetchBufferTestTable2, 1), 10);
            }

            try (Statement stmt = con.createStatement()) {
                boolean hasResults = stmt.execute(fetchBufferTableQuery1 + "; " + errorQuery);
                while (hasResults) {
                    ResultSet rs = stmt.getResultSet();
                    while (rs.next()) {}
                    hasResults = stmt.getMoreResults();
                }
            } catch (SQLServerException se) {
                if (!se.getMessage().equals(mockErrorMsg)) {
                    se.printStackTrace();
                    fail("Mock Sql Server error message was expected.");
                }
            }

            ResiliencyUtils.killConnection(con, connectionString, 1);

            try (Statement stmt = con.createStatement()) {
                ResultSet rs = stmt.executeQuery(fetchBufferTableQuery2);
                while (rs.next()) {}
            }
        }
    }

    /*
     * Test killing a session while retrieving multiple result sets
     */
    @Test
    public void testMultipleResultSets() throws Exception {
        try (Connection c = ResiliencyUtils.getConnection(connectionString); Statement s = c.createStatement()) {
            boolean results = s.execute("SELECT 1;SELECT 2");
            int rsCount = 0;
            do {
                if (results) {
                    try (ResultSet rs = s.getResultSet()) {
                        rsCount++;

                        while (rs.next()) {
                            ResiliencyUtils.killConnection(c, connectionString, 0);
                            assertTrue(rs.getString(1).equals(String.valueOf(rsCount)));
                        }
                    }
                }
                results = s.getMoreResults();
            } while (results);
        } catch (SQLException e) {
            if (!("08S01" == e.getSQLState()
                    || e.getMessage().matches(TestUtils.formatErrorMsg("R_crClientUnrecoverable")))) {
                e.printStackTrace();
            }
            assertTrue(
                    "08S01" == e.getSQLState()
                            || e.getMessage().matches(TestUtils.formatErrorMsg("R_crClientUnrecoverable")),
                    e.getMessage());
        }
    }

    /*
     * Test killing a session while retrieving result set that causes an exception
     */
    @Test
    public void testResultSetWithException() throws Exception {
        try (Connection c = ResiliencyUtils.getConnection(connectionString); Statement s = c.createStatement();
                ResultSet rs = s.executeQuery("SELECT 1/0")) {

            while (rs.next()) {
                ResiliencyUtils.killConnection(c, connectionString, 0);
                // driver should not have successfully reconnected but it did
                fail(TestResource.getResource("R_expectedFailPassed"));
            }
        } catch (SQLException e) {
            if (!e.getMessage().contains("Divide by zero error")) {
                e.printStackTrace();
            }
        }
    }

    /*
     * Test killing a session while retrieving multiple result sets that causes an exception
     */
    @Test
    public void testMultipleResultSetsWithException() throws Exception {
        try (Connection c = ResiliencyUtils.getConnection(connectionString); Statement s = c.createStatement()) {
            boolean results = s.execute("SELECT 1;SELECT 1/0");
            int rsCount = 0;
            do {
                if (results) {
                    try (ResultSet rs = s.getResultSet()) {
                        rsCount++;

                        while (rs.next()) {
                            ResiliencyUtils.killConnection(c, connectionString, 0);
                            assertTrue(rs.getString(1).equals(String.valueOf(rsCount)));
                        }
                    }
                }
                results = s.getMoreResults();
            } while (results);
        } catch (SQLException e) {
            if (!e.getMessage().contains("Divide by zero error")) {
                e.printStackTrace();
            }
        }
    }

    private static void createTable(Statement s) throws SQLException {
        s.execute("CREATE TABLE " + tableName + " (id int IDENTITY, data varchar(50));");
    }

    private static void createTable(Statement s, String query) throws SQLException {
        s.execute(query);
    }

    private static void insertData(Statement s) throws SQLException {
        for (int i = 1; i <= numberOfRows; i++) {
            s.executeUpdate("INSERT INTO " + tableName + " VALUES ('testData" + i + "');");
        }
    }

    private static void insertData(Statement s, String query, int rows) throws SQLException {
        for (int i = 0; i < rows; i++) {
            s.executeUpdate(query);
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

    private static void createCallableStatementOnDoneTestSp(Statement stmt) throws SQLException {
        String sql = "CREATE PROCEDURE " + callableStatementICROnDoneTestSp
                + "(@p1 datetime2(7) OUTPUT, @p2 datetime2(7) OUTPUT) AS "
                + "SELECT @p1 = '2018-03-11T02:00:00.1234567'; SELECT @p2 = '2022-03-11T02:00:00.1234567';";
        stmt.execute(sql);
    }

    private static void createCallableStatementOnDoneErrorTestSp(Statement stmt) throws SQLException {
        String sql = "CREATE PROCEDURE " + callableStatementICROnDoneErrorTestSp
                + "(@p1 datetime2(7) OUTPUT, @p2 datetime2(7) OUTPUT) AS "
                + "SELECT @p1 = '2018-03-11T02:00:00.1234567'; SELECT @p2 = '2022-03-11T02:00:00.1234567'; "
                + errorQuery;
        stmt.execute(sql);
    }

    @AfterAll
    public static void cleanUp() throws SQLException {
        try (Connection c = DriverManager.getConnection(connectionString); Statement s = c.createStatement()) {
            TestUtils.dropTableIfExists(tableName, s);
            TestUtils.dropTableIfExists(clientCursorInitTable1, s);
            TestUtils.dropTableIfExists(clientCursorInitTable2, s);
            TestUtils.dropTableIfExists(clientCursorInitTable3, s);
            TestUtils.dropTableIfExists(fetchBufferTestTable1, s);
            TestUtils.dropTableIfExists(fetchBufferTestTable2, s);
            TestUtils.dropProcedureIfExists(callableStatementICROnDoneTestSp, s);
            TestUtils.dropProcedureIfExists(callableStatementICROnDoneErrorTestSp, s);
        }
    }
}
