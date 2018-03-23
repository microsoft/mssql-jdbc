/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.exception;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;

@RunWith(JUnitPlatform.class)
public class ExceptionTest extends AbstractTest {
    static String inputFile = "BulkCopyCSVTestInput.csv";

    /**
     * Test the SQLException has the proper cause when encoding is not supported.
     * 
     * @throws Exception
     */
    @Test
    public void testBulkCSVFileRecordExceptionCause() throws Exception {
        String filePath = Utils.getCurrentClassPath();

        try {
            SQLServerBulkCSVFileRecord scvFileRecord = new SQLServerBulkCSVFileRecord(filePath + inputFile, "invalid_encoding", true);
        }
        catch (Exception e) {
            if (!(e instanceof SQLException)) {
                throw e;
            }

            assertTrue(null != e.getCause(), "Cause should not be null.");
            assertTrue(e.getCause() instanceof UnsupportedEncodingException, "Cause should be instance of UnsupportedEncodingException.");
        }
    }

    String waitForDelaySPName = "waitForDelaySP";
    final int waitForDelaySeconds = 10;

    /**
     * Test the SQLException has the proper cause when socket timeout occurs.
     * 
     * @throws Exception
     * 
     */
    @Test
    public void testSocketTimeoutExceptionCause() throws Exception {
        SQLServerConnection conn = null;
        try {
            conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
            
            Utils.dropProcedureIfExists(waitForDelaySPName, conn.createStatement());
            createWaitForDelayPreocedure(conn);

            conn = (SQLServerConnection) DriverManager.getConnection(connectionString + ";socketTimeout=" + (waitForDelaySeconds * 1000 / 2) + ";");

            try {
                conn.createStatement().execute("exec " + waitForDelaySPName);
                throw new Exception("Exception for socketTimeout is not thrown.");
            }
            catch (Exception e) {
                if (!(e instanceof SQLException)) {
                    throw e;
                }

                assertTrue(null != e.getCause(), "Cause should not be null.");
                assertTrue(e.getCause() instanceof SocketTimeoutException, "Cause should be instance of SocketTimeoutException.");
            }
        }
        finally {
            if (null != conn) {
                conn.close();
            }
        }
    }

    private void createWaitForDelayPreocedure(SQLServerConnection conn) throws SQLException {
        String sql = "CREATE PROCEDURE " + waitForDelaySPName + " AS" + " BEGIN" + " WAITFOR DELAY '00:00:" + waitForDelaySeconds + "';" + " END";
        conn.createStatement().execute(sql);
    }
    
    @Test
    public void testResultSetErrorSearch() throws Exception {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);

    	String dropTable_sql = "DROP TABLE IF EXISTS TEST659;";
    	String dropProc_sql = "DROP PROCEDURE IF EXISTS proc_insert_masse_TEST";
    	String createTable_sql = "CREATE TABLE TEST659 (ID INT IDENTITY NOT NULL," +
    												"FIELD1 VARCHAR (255) NOT NULL," +
    												"FIELD2 VARCHAR (255) NOT NULL);";
    	
    	String createProc_sql = "CREATE PROCEDURE proc_insert_masse_TEST @json NVARCHAR(MAX) "
    							+ "AS "
    							+ "BEGIN TRANSACTION "
    							+ "BEGIN TRY "
    							+ "SET NOCOUNT ON; "
    							+ "MERGE INTO TEST659 AS target "
    							+ "USING "
    								+ "(SELECT * FROM OPENJSON(@json) "
    								+ "WITH (FIELD1 VARCHAR(255) 'strict $.FIELD1')) "
								+ "AS src "
    							+ "ON (1 = 0) "
    							+ "WHEN NOT MATCHED THEN "
    								+ "INSERT (FIELD1) VALUES (src.FIELD1) "
								+ "OUTPUT inserted.ID; "
    							+ "COMMIT TRANSACTION; "
    							+ "END TRY "
    							+ "BEGIN CATCH "
    							+ "DECLARE @errorMessage NVARCHAR(4000) = ERROR_MESSAGE(); "
    							+ "ROLLBACK TRANSACTION; "
    							+ "RAISERROR('Error occured during the insert: %s', 16, 1, @errorMessage); "
    							+ "END CATCH;";
    	String proc_sql = "EXECUTE [dbo].proc_insert_masse_TEST N'[{\"FIELD1\" : \"TEST\"}]';";

		Connection conn = ds.getConnection();
		Statement stmt = conn.createStatement();
		stmt.execute(dropTable_sql);
		stmt.execute(createTable_sql);
		stmt.execute(dropProc_sql);
		stmt.execute(createProc_sql);
		stmt.execute(proc_sql);
		ResultSet rs = stmt.getResultSet();
    	try {
    		rs.next();
    		fail("No exceptions caught.");    		
    	} catch (SQLException e) {
    		assertTrue(e.getMessage().contains("Error occured during the insert:"), "Unexpected Error Message:" + e.getMessage());
    	}
    }
}