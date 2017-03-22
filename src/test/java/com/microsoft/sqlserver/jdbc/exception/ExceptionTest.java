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
import java.net.URI;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.testframework.AbstractTest;

@RunWith(JUnitPlatform.class)
public class ExceptionTest extends AbstractTest {
    static String inputFile = "BulkCopyCSVTestInput.csv";

    /**
     * Test the SQLServerException has the proper cause when encoding is not supported.
     * 
     * @throws Exception
     */
    @Test
    public void testBulkCSVFileRecordExceptionCause() throws Exception {
        String filePath = getCurrentClassPath();

        try {
            SQLServerBulkCSVFileRecord scvFileRecord = new SQLServerBulkCSVFileRecord(filePath + inputFile, "invalid_encoding", true);
        }
        catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                throw e;
            }

            assertTrue(null != e.getCause(), "Cause should not be null.");
            assertTrue(e.getCause() instanceof UnsupportedEncodingException, "Cause should be instance of UnsupportedEncodingException.");
        }
    }

    /**
     * 
     * @return location of resource file
     */
    static String getCurrentClassPath() {

        try {
            String className = new Object() {
            }.getClass().getEnclosingClass().getName();
            String location = Class.forName(className).getProtectionDomain().getCodeSource().getLocation().getPath() + "/";
            URI uri = new URI(location.toString());
            return uri.getPath();
        }
        catch (Exception e) {
            fail("Failed to get CSV file path. " + e.getMessage());
        }
        return null;
    }

    String waitForDelaySPName = "waitForDelaySP";
    final int waitForDelaySeconds = 10;

    /**
     * Test the SQLServerException has the proper cause when socket timeout occurs.
     * 
     * @throws Exception
     * 
     */
    @Test
    public void testSocketTimeoutExceptionCause() throws Exception {
        SQLServerConnection conn = null;
        try {
            conn = (SQLServerConnection) DriverManager.getConnection(connectionString);

            dropWaitForDelayProcedure(conn);
            createWaitForDelayPreocedure(conn);

            conn = (SQLServerConnection) DriverManager.getConnection(connectionString + ";socketTimeout=" + (waitForDelaySeconds * 1000 / 2) + ";");

            try {
                conn.createStatement().execute("exec " + waitForDelaySPName);
                throw new Exception("Exception for socketTimeout is not thrown.");
            }
            catch (Exception e) {
                if (!(e instanceof SQLServerException)) {
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

    private void dropWaitForDelayProcedure(SQLServerConnection conn) throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'" + waitForDelaySPName
                + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)" + " DROP PROCEDURE " + waitForDelaySPName;
        conn.createStatement().execute(sql);
    }

    private void createWaitForDelayPreocedure(SQLServerConnection conn) throws SQLException {
        String sql = "CREATE PROCEDURE " + waitForDelaySPName + " AS" + " BEGIN" + " WAITFOR DELAY '00:00:" + waitForDelaySeconds + "';" + " END";
        conn.createStatement().execute(sql);
    }
}