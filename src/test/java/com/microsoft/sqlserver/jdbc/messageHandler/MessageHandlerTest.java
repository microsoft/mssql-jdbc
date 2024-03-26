/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.messageHandler;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import org.junit.jupiter.api.Tag;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerMessage;
import com.microsoft.sqlserver.jdbc.ISQLServerMessageHandler;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
public class MessageHandlerTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /**
     * Helper method to count number of SQLWarnings in a chain
     * 
     * @param str
     *        - Debug String, so we can evaluate from where we called it...
     * @param sqlw
     *        - The SQL Warning chain. (can be null)
     * @return A count of warnings
     */
    private static int getWarningCount(String str, SQLWarning sqlw) {
        int count = 0;
        while (sqlw != null) {
            count++;
            // System.out.println("DEBUG: getWarningCount(): [" + str + "] SQLWarning: Error=" + sqlw.getErrorCode() + ", Severity=" + ((SQLServerWarning)sqlw).getSQLServerError().getErrorSeverity() + ", Text=|" + sqlw.getMessage() + "|.");
            sqlw = sqlw.getNextWarning();
        }
        return count;
    }

    /**
     * Test message handler with normal Statement
     * <ul>
     * <li>Insert duplicate row -- Mapped to Info Message</li>
     * <li>Drop table that do not exist -- Mapped to ignore</li>
     * </ul>
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testMsgHandlerWithStatement() throws Exception {
        try (SQLServerConnection conn = getConnection()) {

            class TestMsgHandler implements ISQLServerMessageHandler {
                int numOfCalls = 0;
                int numOfDowngrades = 0;
                int numOfDiscards = 0;

                @Override
                public ISQLServerMessage messageHandler(ISQLServerMessage srvErrorOrWarning) {
                    numOfCalls++;
                    ISQLServerMessage retObj = srvErrorOrWarning;

                    if (srvErrorOrWarning.isErrorMessage()) {

                        // Downgrade: 2601 -- Cannot insert duplicate key row in object 'dbo.#msghandler_tmp_table' with unique index 'ix_id'. The duplicate key value is (1)
                        if (2601 == srvErrorOrWarning.getErrorNumber()) {
                            retObj = srvErrorOrWarning.getSQLServerMessage().toSQLServerInfoMessage();
                            numOfDowngrades++;
                        }

                        // Discard: 3701 -- Cannot drop the table '#msghandler_tmp_table', because it does not exist or you do not have permission.
                        if (3701 == srvErrorOrWarning.getErrorNumber()) {
                            retObj = null;
                            numOfDiscards++;
                        }
                    }

                    if (srvErrorOrWarning.isInfoMessage()) {

                        // Discard: 3621 -- The statement has been terminated.
                        if (3621 == srvErrorOrWarning.getErrorNumber()) {
                            retObj = null;
                            numOfDiscards++;
                        }
                    }

                    return retObj;
                }
            }
            TestMsgHandler testMsgHandler = new TestMsgHandler();

            // Create a massage handler
            conn.setServerMessageHandler(testMsgHandler);

            try (Statement stmnt = conn.createStatement()) {

                stmnt.executeUpdate("CREATE TABLE #msghandler_tmp_table(id int, c1 varchar(255))");
                assertNull(conn.getWarnings(), "Expecting NO SQLWarnings from 'create table', at Connection.");
                assertNull(stmnt.getWarnings(), "Expecting NO SQLWarnings from 'create table', at Statement.");

                stmnt.executeUpdate("CREATE UNIQUE INDEX ix_id ON #msghandler_tmp_table(id)");
                assertNull(conn.getWarnings(), "Expecting NO SQLWarnings from 'create index', at Connection.");
                assertNull(stmnt.getWarnings(), "Expecting NO SQLWarnings from 'create index', at Statement.");

                stmnt.executeUpdate("INSERT INTO #msghandler_tmp_table VALUES(1, 'row 1')");
                assertNull(conn.getWarnings(), "Expecting NO SQLWarnings from 'first insert', at Connection.");
                assertNull(stmnt.getWarnings(), "Expecting NO SQLWarnings from 'first insert', at Statement.");

                stmnt.executeUpdate(
                        "INSERT INTO #msghandler_tmp_table VALUES(1, 'row 1 - again - msg handler downgrades it')");
                assertNotNull(conn.getWarnings(),
                        "Expecting at least ONE SQLWarnings from 'second insert', which is a duplicate row, at Connection.");
                assertNull(stmnt.getWarnings(),
                        "Expecting NO SQLWarnings from 'second insert', which is a duplicate row, at Statement.");
                conn.clearWarnings(); // Clear Warnings at Connection level

                stmnt.executeUpdate("DROP TABLE #msghandler_tmp_table");
                assertNull(conn.getWarnings(), "Expecting NO SQLWarnings from 'first drop table', at Connection.");
                assertNull(stmnt.getWarnings(), "Expecting NO SQLWarnings from 'first drop table', at Statement.");

                stmnt.executeUpdate("DROP TABLE #msghandler_tmp_table"); // This should be IGNORED by the message handler
                assertNull(conn.getWarnings(),
                        "Expecting NO SQLWarnings from 'second drop table, since it should be IGNORED by the message handler', at Connection.");
                assertNull(stmnt.getWarnings(),
                        "Expecting NO SQLWarnings from 'second drop table, since it should be IGNORED by the message handler', at Statement.");

                // numOfCalls to the message handler should be: 3
                assertEquals(3, testMsgHandler.numOfCalls, "Number of message calls to the message handler.");

                // numOfDowngrades in the message handler should be: 1
                assertEquals(1, testMsgHandler.numOfDowngrades, "Number of message Downgrades in the message handler.");

                // numOfDiscards in the message handler should be: 2
                assertEquals(2, testMsgHandler.numOfDiscards, "Number of message Discards in the message handler.");
            }

        } catch (SQLException ex) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + ex.getMessage());
        }
    }

    /**
     * Test message handler with PreparedStatement
     * <ul>
     * <li>Insert duplicate row -- Mapped to Info Message</li>
     * <li>Drop table that do not exist -- Mapped to ignore</li>
     * </ul>
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testMsgHandlerWithPreparedStatement() throws Exception {
        try (SQLServerConnection conn = getConnection()) {

            class TestMsgHandler implements ISQLServerMessageHandler {
                int numOfCalls = 0;
                int numOfDowngrades = 0;
                int numOfDiscards = 0;

                @Override
                public ISQLServerMessage messageHandler(ISQLServerMessage srvErrorOrWarning) {
                    numOfCalls++;
                    ISQLServerMessage retObj = srvErrorOrWarning;

                    if (srvErrorOrWarning.isErrorMessage()) {

                        // Downgrade: 2601 -- Cannot insert duplicate key row in object 'dbo.#msghandler_tmp_table' with unique index 'ix_id'. The duplicate key value is (1)
                        if (2601 == srvErrorOrWarning.getErrorNumber()) {
                            retObj = srvErrorOrWarning.getSQLServerMessage().toSQLServerInfoMessage();
                            numOfDowngrades++;
                        }

                        // Discard: 3701 -- Cannot drop the table '#msghandler_tmp_table', because it does not exist or you do not have permission.
                        if (3701 == srvErrorOrWarning.getErrorNumber()) {
                            retObj = null;
                            numOfDiscards++;
                        }
                    }

                    if (srvErrorOrWarning.isInfoMessage()) {

                        // Discard: 3621 -- The statement has been terminated.
                        if (3621 == srvErrorOrWarning.getErrorNumber()) {
                            retObj = null;
                            numOfDiscards++;
                        }
                    }

                    return retObj;
                }
            }
            TestMsgHandler testMsgHandler = new TestMsgHandler();

            // Create a massage handler
            conn.setServerMessageHandler(testMsgHandler);

            try (Statement stmnt = conn.createStatement()) {
                stmnt.executeUpdate("CREATE TABLE #msghandler_tmp_table(id int, c1 varchar(255))");
                assertNull(conn.getWarnings(), "Expecting NO SQLWarnings from 'create table', at Connection.");
                assertNull(stmnt.getWarnings(), "Expecting NO SQLWarnings from 'create table', at Statement.");
            }

            try (Statement stmnt = conn.createStatement()) {
                stmnt.executeUpdate("CREATE UNIQUE INDEX ix_id ON #msghandler_tmp_table(id)");
                assertNull(conn.getWarnings(), "Expecting NO SQLWarnings from 'create index', at Connection.");
                assertNull(stmnt.getWarnings(), "Expecting NO SQLWarnings from 'create index', at Statement.");
            }

            try (PreparedStatement stmnt = conn.prepareStatement("INSERT INTO #msghandler_tmp_table VALUES(?, ?)")) {
                stmnt.setInt(1, 1);
                stmnt.setString(2, "row 1");
                stmnt.executeUpdate();
                assertNull(conn.getWarnings(), "Expecting NO SQLWarnings from 'first insert', at Connection.");
                assertNull(stmnt.getWarnings(), "Expecting NO SQLWarnings from 'first insert', at Statement.");
            }

            try (PreparedStatement stmnt = conn.prepareStatement("INSERT INTO #msghandler_tmp_table VALUES(?, ?)")) {
                stmnt.setInt(1, 1);
                stmnt.setString(2, "row 1 - again - msg handler downgrades it");
                stmnt.executeUpdate();
                assertNotNull(conn.getWarnings(),
                        "Expecting at least ONE SQLWarnings from 'second insert', which is a duplicate row, at Connection.");
                assertNull(stmnt.getWarnings(),
                        "Expecting NO SQLWarnings from 'second insert', which is a duplicate row, at Statement.");
                conn.clearWarnings(); // Clear Warnings at Connection level
            }

            try (Statement stmnt = conn.createStatement()) {
                stmnt.executeUpdate("DROP TABLE #msghandler_tmp_table");
                assertNull(conn.getWarnings(), "Expecting NO SQLWarnings from 'first drop table', at Connection.");
                assertNull(stmnt.getWarnings(), "Expecting NO SQLWarnings from 'first drop table', at Statement.");
            }

            try (Statement stmnt = conn.createStatement()) {
                stmnt.executeUpdate("DROP TABLE #msghandler_tmp_table");
                assertNull(conn.getWarnings(), "Expecting NO SQLWarnings from 'first drop table', at Connection.");
                assertNull(stmnt.getWarnings(), "Expecting NO SQLWarnings from 'first drop table', at Statement.");
            }

            // numOfCalls to the message handler should be: 3
            assertEquals(3, testMsgHandler.numOfCalls, "Number of message calls to the message handler.");

            // numOfDowngrades in the message handler should be: 1
            assertEquals(1, testMsgHandler.numOfDowngrades, "Number of message Downgrades in the message handler.");

            // numOfDiscards in the message handler should be: 2
            assertEquals(2, testMsgHandler.numOfDiscards, "Number of message Discards in the message handler.");

        } catch (SQLException ex) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + ex.getMessage());
        }
    }

    /**
     * Test message handler with CallableStatement
     * <ul>
     * <li>Insert duplicate row -- Mapped to Info Message</li>
     * <li>Drop table that do not exist -- Mapped to ignore</li>
     * </ul>
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testMsgHandlerWithCallableStatement() throws Exception {
        try (SQLServerConnection conn = getConnection()) {

            class TestMsgHandler implements ISQLServerMessageHandler {
                int numOfCalls = 0;
                int numOfDowngrades = 0;
                int numOfDiscards = 0;

                @Override
                public ISQLServerMessage messageHandler(ISQLServerMessage srvErrorOrWarning) {
                    numOfCalls++;
                    ISQLServerMessage retObj = srvErrorOrWarning;

                    if (srvErrorOrWarning.isErrorMessage()) {

                        // Downgrade: 2601 -- Cannot insert duplicate key row in object 'dbo.#msghandler_tmp_table' with unique index 'ix_id'. The duplicate key value is (1)
                        if (2601 == srvErrorOrWarning.getErrorNumber()) {
                            retObj = srvErrorOrWarning.getSQLServerMessage().toSQLServerInfoMessage();
                            numOfDowngrades++;
                        }

                        // Downgrade: 3701 -- Cannot drop the table '#msghandler_tmp_table', because it does not exist or you do not have permission.
                        if (3701 == srvErrorOrWarning.getErrorNumber()) {
                            retObj = null;
                            numOfDiscards++;
                        }
                    }

                    if (srvErrorOrWarning.isInfoMessage()) {

                        // Discard: 3621 -- The statement has been terminated.
                        if (3621 == srvErrorOrWarning.getErrorNumber()) {
                            retObj = null;
                            numOfDiscards++;
                        }
                    }

                    return retObj;
                }
            }
            TestMsgHandler testMsgHandler = new TestMsgHandler();

            // Create a massage handler
            conn.setServerMessageHandler(testMsgHandler);

            // SQL to create procedure
            String sqlCreateProc = "" + "CREATE PROCEDURE #msghandler_tmp_proc( \n" + "    @out_row_count INT OUTPUT \n"
                    + ") \n" + "AS \n" + "BEGIN \n" + "    -- Create a dummy table, with index \n"
                    + "    CREATE TABLE #msghandler_tmp_table(id int, c1 varchar(255)) \n"
                    + "    CREATE UNIQUE INDEX ix_id ON #msghandler_tmp_table(id) \n" + "     \n"
                    + "    -- Insert records 1 \n" + "    INSERT INTO #msghandler_tmp_table VALUES(1, 'row 1') \n"
                    + "     \n"
                    + "    -- Insert records 1 -- Again, which will FAIL, but the message handler will downgrade it into a INFO Message \n"
                    + "    INSERT INTO #msghandler_tmp_table VALUES(1, 'row 1 - again - msg handler downgrades it') \n"
                    + "     \n" + "    -- Count records \n" + "    SELECT @out_row_count = count(*) \n"
                    + "    FROM #msghandler_tmp_table \n" + "     \n" + "    -- Drop the table \n"
                    + "    DROP TABLE #msghandler_tmp_table \n" + "     \n"
                    + "    -- Drop the table agin... The message handler will DISCARD the error\n"
                    + "    DROP TABLE #msghandler_tmp_table \n" + "     \n" + "    RETURN 1 \n" + "END \n";

            // Create the proc
            try (Statement stmnt = conn.createStatement()) {
                stmnt.executeUpdate(sqlCreateProc);
                assertEquals(0, getWarningCount("Conn", conn.getWarnings()),
                        "Expecting NO SQLWarnings from 'create proc', at Connection.");
                assertEquals(0, getWarningCount("Stmnt", conn.getWarnings()),
                        "Expecting NO SQLWarnings from 'create proc', at Statement.");
            }

            // Execute the proc
            try (CallableStatement cstmnt = conn.prepareCall("{ ? =call  #msghandler_tmp_proc(?) }")) {
                cstmnt.registerOutParameter(1, Types.INTEGER);
                cstmnt.registerOutParameter(2, Types.INTEGER);
                cstmnt.execute();
                int procReturnCode = cstmnt.getInt(1);
                int procRowCount = cstmnt.getInt(2);

                assertEquals(1, procReturnCode, "Expecting ReturnCode 1 from the temp procedure.");
                assertEquals(1, procRowCount, "Expecting procRowCount 1 from the temp procedure.");

                assertEquals(1, getWarningCount("Conn", conn.getWarnings()),
                        "Expecting NO SQLWarnings from 'exec proc', at Connection.");
                assertEquals(0, getWarningCount("CStmnt", cstmnt.getWarnings()),
                        "Expecting NO SQLWarnings from 'exec proc', at CallableStatement.");
            }

            // numOfCalls to the message handler should be: 3
            assertEquals(3, testMsgHandler.numOfCalls, "Number of message calls to the message handler.");

            // numOfDowngrades in the message handler should be: 1
            assertEquals(1, testMsgHandler.numOfDowngrades, "Number of message Downgrades in the message handler.");

            // numOfDiscards in the message handler should be: 2
            assertEquals(2, testMsgHandler.numOfDiscards, "Number of message Discards in the message handler.");

        } catch (SQLException ex) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + ex.getMessage());
        }
    }

    /**
     * Test message handler with CallableStatement -- and "feedback" messages
     * <p>
     * Do a "long running procedure" and check that the message handler receives the "feedback" messages
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testMsgHandlerWithProcedureFeedback() throws Exception {
        try (SQLServerConnection conn = getConnection()) {

            class TestMsgHandler implements ISQLServerMessageHandler {
                int numOfCalls = 0;
                Map<String, Long> feedbackMsgTs = new LinkedHashMap<>();

                @Override
                public ISQLServerMessage messageHandler(ISQLServerMessage srvErrorOrWarning) {
                    numOfCalls++;

                    if (50_000 == srvErrorOrWarning.getErrorNumber()) {
                        // System.out.println("DEBUG: testMsgHandlerWithProcedureFeedback.messageHandler(): FEEDBACK: " + srvErrorOrWarning.getErrorMessage());
                        // Remember when the message was received
                        feedbackMsgTs.put(srvErrorOrWarning.getErrorMessage(), System.currentTimeMillis());
                    }

                    return srvErrorOrWarning;
                }
            }
            TestMsgHandler testMsgHandler = new TestMsgHandler();

            // Create a massage handler
            conn.setServerMessageHandler(testMsgHandler);

            int doSqlLoopCount = 4;
            // SQL to create procedure
            String sqlCreateProc = "" + "CREATE PROCEDURE #msghandler_feeback_proc \n" + "AS \n" + "BEGIN \n"
                    + "    DECLARE @loop_cnt INT = " + doSqlLoopCount + " \n" + "    DECLARE @feedback VARCHAR(255) \n"
                    + " \n" + "    WHILE (@loop_cnt > 0) \n" + "    BEGIN \n" + "        WAITFOR DELAY '00:00:01' \n"
                    + " \n"
                    + "        SET @feedback = 'In proc, still looping... waiting for loop_count to reach 0. loop_count is now at: ' + convert(varchar(10), @loop_cnt) \n"
                    + "        RAISERROR(@feedback, 0, 1) WITH NOWAIT \n" + " \n"
                    + "        SET @loop_cnt = @loop_cnt - 1 \n" + "    END \n" + " \n" + "    RETURN @loop_cnt \n"
                    + "END \n";

            // Create the proc
            try (Statement stmnt = conn.createStatement()) {
                stmnt.executeUpdate(sqlCreateProc);
                assertEquals(0, getWarningCount("Conn", conn.getWarnings()),
                        "Expecting NO SQLWarnings from 'create proc', at Connection.");
                assertEquals(0, getWarningCount("Stmnt", conn.getWarnings()),
                        "Expecting NO SQLWarnings from 'create proc', at Statement.");
            }

            // Execute the proc
            try (CallableStatement cstmnt = conn.prepareCall("{ ? =call  #msghandler_feeback_proc }")) {
                cstmnt.registerOutParameter(1, Types.INTEGER);
                cstmnt.execute();
                int procReturnCode = cstmnt.getInt(1);

                assertEquals(0, procReturnCode, "Unexpected ReturnCode from the temp procedure.");

                assertEquals(0, getWarningCount("conn", conn.getWarnings()),
                        "Unexpected Number Of SQLWarnings from 'exec proc', at Connection.");
                assertEquals(doSqlLoopCount, getWarningCount("cstmnt", cstmnt.getWarnings()),
                        "Unexpected Number Of SQLWarnings from 'exec proc', at CallableStatement.");
            }

            // numOfCalls to the message handler should be: #
            assertEquals(doSqlLoopCount, testMsgHandler.numOfCalls, "Number of message calls to the message handler.");

            // Loop all received messages and check that they are within a second (+-200ms)
            long prevTime = 0;
            for (Entry<String, Long> entry : testMsgHandler.feedbackMsgTs.entrySet()) {
                if (prevTime == 0) {
                    prevTime = entry.getValue();
                    continue;
                }

                long msDiff = entry.getValue() - prevTime;
                if (msDiff < 800 || msDiff > 1200) {
                    fail("Received Messages is to far apart. They should be approx 1000 ms. msDiff=" + msDiff
                            + " Message=|" + entry.getKey() + "|.");
                }

                prevTime = entry.getValue();
            }

        } catch (SQLException ex) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + ex.getMessage());
        }
    }
}
