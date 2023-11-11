/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.exception;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class ChainedExceptionTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /**
     * Small helper to read/loop all TDS packages from the "wire"
     * <p>
     * hence stmnt.execute() and stmnt.executeUpdate() do not fully read the TDS stream, hence no Exceptions in some cases...
     * @param conn
     * @param sql
     * @return
     * @throws SQLException
     */
    private static int execSql(Connection conn, String sql)
    throws SQLException {

    	try (Statement stmnt = conn.createStatement()) {

            int rowsRead = 0;
            int rowsAffected = 0;
            boolean hasRs = stmnt.execute(sql);

            do {
                if(hasRs) {
                    ResultSet rs = stmnt.getResultSet();

                    while(rs.next()) {
                        // Just read the row... don't care about data
                         rowsRead++; 
                    }

                    // Check for warnings
                    // If warnings found, add them to the LIST
                    for (SQLWarning sqlw = rs.getWarnings(); sqlw != null; sqlw = sqlw.getNextWarning()) {
                        fail("No SQLWarning should be expected here: at ResultSet level.");
                    }

                    rs.close();
                }
                else {
                    rowsAffected = stmnt.getUpdateCount();
                }

                // Check if we have more resultsets
                hasRs = stmnt.getMoreResults();
            }
            while (hasRs || rowsAffected != -1);

            // Check for warnings
            for (SQLWarning sqlw = stmnt.getWarnings(); sqlw != null; sqlw = sqlw.getNextWarning()) {
                fail("No SQLWarning should be expected here: at Statement level.");
            }
            for (SQLWarning sqlw = conn.getWarnings(); sqlw != null; sqlw = sqlw.getNextWarning()) {
                fail("No SQLWarning should be expected here: at Connection level.");
            }

            return rowsRead + rowsAffected;
        }
    }

    

    @Test
    public void testTwoExceptions() throws Exception {
        // The below should yield the following Server Messages:
    	//  1 : Msg 5074, Level 16, State 1: The object 'DF__#chained_exc__c1__AB25243A' is dependent on column 'c1'.
    	//  1 : Msg 4922, Level 16, State 9: ALTER TABLE ALTER COLUMN c1 failed because one or more objects access this column.
        String sql = "CREATE TABLE #chained_exception_test_x1(c1 INT DEFAULT(0)); \n"
                   + "ALTER  TABLE #chained_exception_test_x1 ALTER COLUMN c1 VARCHAR(10); \n"
                   + "DROP   TABLE IF EXISTS #chained_exception_test_x1; \n";	
        try (Connection conn = getConnection()) {

        	// NOTE: stmnt.execute() or executeUpdate() wont work in here since it do not read through the full TDS stream
//        	execSql(con, sql);
        	
        	try (Statement stmnt = conn.createStatement()) { 
        		stmnt.executeUpdate("CREATE TABLE #chained_exception_test_x1(c1 INT DEFAULT(0))"); 
        		stmnt.executeUpdate("ALTER  TABLE #chained_exception_test_x1 ALTER COLUMN c1 VARCHAR(10)"); 
        		stmnt.executeUpdate("DROP   TABLE IF EXISTS #chained_exception_test_x1"); 
        	}

            fail(TestResource.getResource("R_expectedFailPassed"));

        } catch (SQLException ex) {

            // Check the SQLException and the chain
            int exCount     = 0;
            int firstMsgNum = ex.getErrorCode();
            int lastMsgNum  = -1;

            while (ex != null) {
            	exCount++;

                System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>> ex[" + exCount + "].getErrorCode()=" + ex.getErrorCode() + ", Severity=" + ((SQLServerException)ex).getSQLServerError().getErrorSeverity()+ ", Text=|" + ex.getMessage() + "|.");
                lastMsgNum = ex.getErrorCode();

            	ex = ex.getNextException();
            }
            
            // Exception Count should be: 2
        	assertEquals(2, exCount, "Number of SQLExceptions in the SQLException chain");

        	// Check first Msg: 5074
        	assertEquals(5074, firstMsgNum, "First SQL Server Message");

        	// Check last Msg: 4922
        	assertEquals(4922, lastMsgNum, "Last SQL Server Message");
        }
    }
}
