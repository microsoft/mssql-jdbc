/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;

@RunWith(JUnitPlatform.class)
public class PreparedStatementTest extends AbstractTest {
    private void executeSQL(SQLServerConnection conn, String sql) throws SQLException {
        Statement stmt = conn.createStatement();
        stmt.execute(sql);
    }

    private int executeSQLReturnFirstInt(SQLServerConnection conn, String sql) throws SQLException {
        Statement stmt = conn.createStatement();
        ResultSet result = stmt.executeQuery(sql);
        
        int returnValue = -1;

        if(result.next())
            returnValue = result.getInt(1);

        return returnValue;
    }

    /**
     * Test handling of unpreparing prepared statements.
     * 
     * @throws SQLException
     */
    @Test
    public void testBatchedUnprepare() throws SQLException {
        SQLServerConnection conOuter = null;

        // Make sure correct settings are used.
        SQLServerConnection.setDefaultPrepareStatementOnFirstCall(SQLServerConnection.INITIAL_DEFAULT_PREPARE_STATEMENT_ON_FIRST_CALL);
        SQLServerConnection.setDefaultPreparedStatementDiscardActionThreshold(SQLServerConnection.INITIAL_DEFAULT_PREPARED_STATEMENT_DISCARD_ACTION_THRESHOLD);

        try (SQLServerConnection con = (SQLServerConnection)DriverManager.getConnection(connectionString)) {
            conOuter = con;

            // Clean-up proc cache
            this.executeSQL(con, "DBCC FREEPROCCACHE;");
            
            String lookupUniqueifier = UUID.randomUUID().toString();

            String queryCacheLookup = String.format("/*unpreparetest_%s%%*/SELECT * FROM sys.tables;", lookupUniqueifier);
            String query = String.format("/*unpreparetest_%s only sp_executesql*/SELECT * FROM sys.tables;", lookupUniqueifier);

            // Verify nothing in cache.
            String verifyTotalCacheUsesQuery = String.format("SELECT CAST(ISNULL(SUM(usecounts), 0) AS INT) FROM sys.dm_exec_cached_plans AS p CROSS APPLY sys.dm_exec_sql_text(p.plan_handle) AS s WHERE s.text LIKE '%%%s'", queryCacheLookup);

            assertSame(0, executeSQLReturnFirstInt(con, verifyTotalCacheUsesQuery));

            int iterations = 25;

            // Verify no prepares for 1 time only uses.
            for(int i = 0; i < iterations; ++i) {
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement)con.prepareStatement(query)) {
                    pstmt.execute();
                } 
                assertSame(0, con.getOutstandingPreparedStatementDiscardActionCount());
            }

            // Verify total cache use.
            assertSame(iterations, executeSQLReturnFirstInt(con, verifyTotalCacheUsesQuery));

            query = String.format("/*unpreparetest_%s, sp_executesql->sp_prepexec->sp_execute- batched sp_unprepare*/SELECT * FROM sys.tables;", lookupUniqueifier);
            int prevDiscardActionCount = 0;
    
            // Now verify unprepares are needed.                 
            for(int i = 0; i < iterations; ++i) {

                // Verify current queue depth is expected.
                assertSame(prevDiscardActionCount, con.getOutstandingPreparedStatementDiscardActionCount());
                
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement)con.prepareStatement(query)) {
                    pstmt.execute(); // sp_executesql
            
                    pstmt.execute(); // sp_prepexec
                    ++prevDiscardActionCount;

                    pstmt.execute(); // sp_execute
                }

                // Verify clean-up is happening as expected.
                if(prevDiscardActionCount > con.getPreparedStatementDiscardActionThreshold()) {
                    prevDiscardActionCount = 0;
                }

                assertSame(prevDiscardActionCount, con.getOutstandingPreparedStatementDiscardActionCount());
            }  

            // Verify total cache use.
            assertSame(iterations * 4, executeSQLReturnFirstInt(con, verifyTotalCacheUsesQuery));              
        } 
        // Verify clean-up happened on connection close.
        assertSame(0, conOuter.getOutstandingPreparedStatementDiscardActionCount());        
    }

    /**
     * Test handling of the two configuration knobs related to prepared statement handling.
     * 
     * @throws SQLException
     */
    @Test
    public void testPreparedStatementExecAndUnprepareConfig() throws SQLException {

        // Verify initial defaults is correct:
        assertTrue(SQLServerConnection.INITIAL_DEFAULT_PREPARED_STATEMENT_DISCARD_ACTION_THRESHOLD > 1);
        assertTrue(false == SQLServerConnection.INITIAL_DEFAULT_PREPARE_STATEMENT_ON_FIRST_CALL);
        assertSame(SQLServerConnection.INITIAL_DEFAULT_PREPARED_STATEMENT_DISCARD_ACTION_THRESHOLD, SQLServerConnection.getDefaultPreparedStatementDiscardActionThreshold());
        assertSame(SQLServerConnection.INITIAL_DEFAULT_PREPARE_STATEMENT_ON_FIRST_CALL, SQLServerConnection.getDefaultPrepareStatementOnFirstCall());

        // Change the defaults and verify change stuck.
        SQLServerConnection.setDefaultPrepareStatementOnFirstCall(!SQLServerConnection.INITIAL_DEFAULT_PREPARE_STATEMENT_ON_FIRST_CALL);
        SQLServerConnection.setDefaultPreparedStatementDiscardActionThreshold(SQLServerConnection.INITIAL_DEFAULT_PREPARED_STATEMENT_DISCARD_ACTION_THRESHOLD - 1);
        assertNotSame(SQLServerConnection.INITIAL_DEFAULT_PREPARED_STATEMENT_DISCARD_ACTION_THRESHOLD, SQLServerConnection.getDefaultPreparedStatementDiscardActionThreshold());
        assertNotSame(SQLServerConnection.INITIAL_DEFAULT_PREPARE_STATEMENT_ON_FIRST_CALL, SQLServerConnection.getDefaultPrepareStatementOnFirstCall());

        // Verify invalid (negative) change does not stick for threshold.
        SQLServerConnection.setDefaultPreparedStatementDiscardActionThreshold(-1);
        assertTrue(0 < SQLServerConnection.getDefaultPreparedStatementDiscardActionThreshold());

        // Verify instance settings.
        SQLServerConnection conn1 = (SQLServerConnection)DriverManager.getConnection(connectionString);
        assertSame(SQLServerConnection.getDefaultPreparedStatementDiscardActionThreshold(), conn1.getPreparedStatementDiscardActionThreshold());        
        assertSame(SQLServerConnection.getDefaultPrepareStatementOnFirstCall(), conn1.getPrepareStatementOnFirstCall());        
        conn1.setPreparedStatementDiscardActionThreshold(SQLServerConnection.getDefaultPreparedStatementDiscardActionThreshold() + 1);
        conn1.setPrepareStatementOnFirstCall(!SQLServerConnection.getDefaultPrepareStatementOnFirstCall());
        assertNotSame(SQLServerConnection.getDefaultPreparedStatementDiscardActionThreshold(), conn1.getPreparedStatementDiscardActionThreshold());        
        assertNotSame(SQLServerConnection.getDefaultPrepareStatementOnFirstCall(), conn1.getPrepareStatementOnFirstCall());        
        
        // Verify new instance not same as changed instance.
        SQLServerConnection conn2 = (SQLServerConnection)DriverManager.getConnection(connectionString);
        assertNotSame(conn1.getPreparedStatementDiscardActionThreshold(), conn2.getPreparedStatementDiscardActionThreshold());        
        assertNotSame(conn1.getPrepareStatementOnFirstCall(), conn2.getPrepareStatementOnFirstCall());        

        // Verify instance setting is followed.
        SQLServerConnection.setDefaultPreparedStatementDiscardActionThreshold(SQLServerConnection.INITIAL_DEFAULT_PREPARED_STATEMENT_DISCARD_ACTION_THRESHOLD);
        try (SQLServerConnection con = (SQLServerConnection)DriverManager.getConnection(connectionString)) {
            try {

                String query = "/*unprepSettingsTest*/SELECT * FROM sys.objects;";

                // Verify initial default is not serial:
                assertTrue(1 < SQLServerConnection.getDefaultPreparedStatementDiscardActionThreshold());

                // Verify first use is batched.
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement)con.prepareStatement(query)) {
                    pstmt.execute();
                }
                // Verify that the un-prepare action was not handled immediately.
                assertSame(1, con.getOutstandingPreparedStatementDiscardActionCount());

                // Force un-prepares.
                con.forcePreparedStatementDiscardActions();

                // Verify that queue is now empty.
                assertSame(0, con.getOutstandingPreparedStatementDiscardActionCount());

                // Set instance setting to serial execution of un-prepare actions.
                con.setPreparedStatementDiscardActionThreshold(1);                

                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement)con.prepareStatement(query)) {
                    pstmt.execute();
                }
                // Verify that the un-prepare action was handled immediately.
                assertSame(0, con.getOutstandingPreparedStatementDiscardActionCount());
            } 
            finally {
            }
        }
    }

}
