/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerError;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class SQLServerErrorTest extends AbstractTest {
    static int loginTimeOutInSeconds = 10;

    @Test
    public void testLoginFailedError() {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        ds.setLoginTimeout(loginTimeOutInSeconds);
        ds.setPassword("");
        try (SQLServerConnection connection = (SQLServerConnection) ds.getConnection()) {
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (SQLServerException e) {
            assertNotNull(e);
            assertNotNull(e.getSQLServerError());

            SQLServerError sse = e.getSQLServerError();
            assertTrue(sse.getErrorMessage().contains(TestResource.getResource("R_loginFailed")));
            assertEquals(18456, sse.getErrorNumber());
            assertEquals(1, sse.getErrorState());
            assertEquals(14, sse.getErrorSeverity());
            assertEquals("", sse.getProcedureName());
            assertEquals(1, sse.getLineNumber());
        }
    }

    @Test
    public void testClosedStatementError() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        try (SQLServerConnection connection = (SQLServerConnection) ds.getConnection();
                Statement stmt = connection.createStatement()) {
            stmt.close();
            try (ResultSet rs = stmt.executeQuery("SELECT 1")) {
                fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (SQLServerException e) {
                assertNotNull(e);
                assertNull(e.getSQLServerError());
            }
        }
    }

    @Test
    public void testClosedResultSetError() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        try (SQLServerConnection connection = (SQLServerConnection) ds.getConnection();
                Statement stmt = connection.createStatement(); ResultSet rs = stmt.executeQuery("SELECT 1")) {
            rs.close();
            rs.next();
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (SQLServerException e) {
            assertNotNull(e);
            assertNull(e.getSQLServerError());
        }
    }

    @Test
    public void testInvalidTableName() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        try (SQLServerConnection connection = (SQLServerConnection) ds.getConnection();
                Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * from INVALID_TABLE_NAME")) {
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (SQLServerException e) {
            assertNotNull(e);
            assertNotNull(e.getSQLServerError());

            SQLServerError sse = e.getSQLServerError();
            assertTrue(sse.getErrorMessage().contains(TestResource.getResource("R_invalidObjectName")));
            assertEquals(208, sse.getErrorNumber());
            assertEquals(1, sse.getErrorState());
            assertEquals(16, sse.getErrorSeverity());
            assertEquals("", sse.getProcedureName());
            assertEquals(1, sse.getLineNumber());
        }
    }
}
