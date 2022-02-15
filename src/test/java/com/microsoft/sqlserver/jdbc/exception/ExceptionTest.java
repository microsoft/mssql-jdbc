/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.exception;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


@RunWith(JUnitPlatform.class)
public class ExceptionTest extends AbstractTest {
    static String inputFile = "BulkCopyCSVTestInput.csv";

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    /**
     * Test the SQLException has the proper cause when encoding is not supported.
     * 
     * @throws Exception
     */
    @SuppressWarnings("resource")
    @Test
    public void testBulkCSVFileRecordExceptionCause() throws Exception {
        String filePath = TestUtils.getCurrentClassPath();

        try {
            new SQLServerBulkCSVFileRecord(filePath + inputFile, "invalid_encoding", true);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLException)) {
                fail(e.getMessage());
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
    @Tag(Constants.xAzureSQLDW)
    public void testSocketTimeoutExceptionCause() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName), stmt);
            createWaitForDelayPreocedure(conn);
        }

        try (Connection conn = PrepUtil.getConnection(
                connectionString + ";socketTimeout=" + (waitForDelaySeconds * 1000 / 2) + Constants.SEMI_COLON);
                Statement stmt = conn.createStatement()) {
            stmt.execute("exec " + AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName));
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLException)) {
                fail(e.getMessage());
            }

            assertTrue(null != e.getCause(), TestResource.getResource("R_causeShouldNotBeNull"));
            MessageFormat form = new MessageFormat(TestResource.getResource("R_causeShouldBeInstance"));
            Object[] msgArgs = {"SocketTimeoutException"};
            assertTrue(e.getCause() instanceof SocketTimeoutException, form.format(msgArgs));
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testResultSetErrorSearch() throws Exception {
        String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("ISSUE659TABLE"));
        String procName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("ISSUE659PROC"));
        String expectedException = "Error occured during the insert";
        int outputValue = 5;
        String createTableSql = "CREATE TABLE " + tableName + "(FIELD1 VARCHAR (255) NOT NULL);";
        String createProcSql = "CREATE PROCEDURE " + procName
                + " AS BEGIN TRANSACTION\n BEGIN TRY SET NOCOUNT ON; INSERT INTO " + tableName + " (FIELD1) OUTPUT "
                + outputValue + " VALUES ('test'); INSERT INTO" + tableName
                + " (FIELD1) VALUES (NULL); COMMIT TRANSACTION; END TRY BEGIN CATCH DECLARE @errorMessage NVARCHAR(4000) = ERROR_MESSAGE(); ROLLBACK TRANSACTION; RAISERROR('"
                + expectedException + ": %s', 16, 1, @errorMessage); END CATCH;";
        String execProcSql = "EXECUTE " + procName;

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSql);
            stmt.execute(createProcSql);
            stmt.execute(execProcSql);
            try (ResultSet rs = stmt.getResultSet()) {
                // First result set
                while (rs.next()) {
                    assertEquals(outputValue, rs.getInt(1));
                }
            } catch (SQLException e) {
                // First result set should not throw an exception.
                fail(TestResource.getResource("R_unexpectedException"));
            }
            try {
                // Second result set, contains the exception.
                assertTrue(stmt.getMoreResults());
                fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains(expectedException),
                        TestResource.getResource("R_expectedExceptionNotThrown") + e.getMessage());
            }
        } finally {
            try (Statement stmt = connection.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
                TestUtils.dropProcedureIfExists(procName, stmt);
            }
        }
    }

    private void createWaitForDelayPreocedure(Connection conn) throws SQLException {
        String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName) + " AS" + " BEGIN"
                + " WAITFOR DELAY '00:00:" + waitForDelaySeconds + "';" + " END";
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    @AfterAll
    public static void cleanup() throws SQLException {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(waitForDelaySPName), stmt);
        }
    }
}
