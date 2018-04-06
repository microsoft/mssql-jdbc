/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.exception;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
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
}
