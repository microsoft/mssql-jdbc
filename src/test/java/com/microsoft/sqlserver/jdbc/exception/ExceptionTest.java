/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.exception;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


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
        String filePath = TestUtils.getCurrentClassPath();

        try {
            SQLServerBulkCSVFileRecord scvFileRecord = new SQLServerBulkCSVFileRecord(filePath + inputFile,
                    "invalid_encoding", true);
        } catch (Exception e) {
            if (!(e instanceof SQLException)) {
                throw e;
            }

            assertTrue(null != e.getCause(), TestResource.getResource("R_causeShouldNotBeNull"));
            MessageFormat form = new MessageFormat(TestResource.getResource("R_causeShouldBeInstance"));
            Object[] msgArgs = {"UnsupportedEncodingException"};
            assertTrue(e.getCause() instanceof UnsupportedEncodingException, form.format(msgArgs));
        }
    }

    static String waitForDelaySPName = RandomUtil.getIdentifier("waitForDelaySP");
    final int waitForDelaySeconds = 10;

    /**
     * Test the SQLException has the proper cause when socket timeout occurs.
     * 
     * @throws Exception
     * 
     */
    @Test
    public void testSocketTimeoutExceptionCause() throws Exception {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement()) {
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName), stmt);
            createWaitForDelayPreocedure(conn);
        }

        try (Connection conn = DriverManager
                .getConnection(connectionString + ";socketTimeout=" + (waitForDelaySeconds * 1000 / 2) + ";");
                Statement stmt = conn.createStatement()) {
            stmt.execute("exec " + AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName));
            throw new Exception(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLException)) {
                throw e;
            }

            assertTrue(null != e.getCause(), TestResource.getResource("R_causeShouldNotBeNull"));
            MessageFormat form = new MessageFormat(TestResource.getResource("R_causeShouldBeInstance"));
            Object[] msgArgs = {"SocketTimeoutException"};
            assertTrue(e.getCause() instanceof SocketTimeoutException, form.format(msgArgs));
        }
    }

    private void createWaitForDelayPreocedure(SQLServerConnection conn) throws SQLException {
        String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName) + " AS" + " BEGIN"
                + " WAITFOR DELAY '00:00:" + waitForDelaySeconds + "';" + " END";
        conn.createStatement().execute(sql);
    }

    @AfterAll
    public static void cleanup() throws SQLException {
        try (SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
                Statement stmt = conn.createStatement()) {
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName), stmt);
        }
    }
}
