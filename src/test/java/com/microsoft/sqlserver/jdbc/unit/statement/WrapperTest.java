/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;

/**
 * Tests isWrapperFor methods 
 *
 */
@RunWith(JUnitPlatform.class)
public class WrapperTest extends AbstractTest {

    /**
     * Wrapper tests 
     * @throws Exception
     */
    @Test
    public void wrapTest() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection con = DriverManager.getConnection(connectionString);
        Statement stmt = con.createStatement();

        try {
            // First make sure that a statement can be unwrapped
            boolean isWrapper = ((SQLServerStatement) stmt).isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerStatement"));
            assertEquals(isWrapper, true, "SQLServerStatement should be a wrapper for self");
            isWrapper = ((SQLServerStatement) stmt).isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerStatement"));
            assertEquals(isWrapper, true, "SQLServerStatement should be a wrapper for ISQLServerStatement");

            isWrapper = ((SQLServerStatement) stmt).isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection"));
            assertEquals(isWrapper, false, "SQLServerStatement should not be a wrapper for SQLServerConnection");

            // Now make sure that we can unwrap a SQLServerCallableStatement to a SQLServerStatement
            CallableStatement cs = con.prepareCall("{  ? = CALL " + "ProcName" + " (?, ?, ?, ?) }");
            // Test the class first
            isWrapper = ((SQLServerCallableStatement) cs).isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerStatement"));
            assertEquals(isWrapper, true, "SQLServerCallableStatement should be a wrapper for SQLServerStatement");
            // Now unwrap the Callable to a statement and call a SQLServerStatement specific function and make sure it succeeds.
            SQLServerStatement stmt2 = (SQLServerStatement) ((SQLServerCallableStatement) cs)
                    .unwrap(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerStatement"));
            stmt2.setResponseBuffering("adaptive");
            // now test the interface
            isWrapper = ((SQLServerCallableStatement) cs).isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerCallableStatement"));
            assertEquals(isWrapper, true, "SQLServerCallableStatement should be a wrapper for ISQLServerCallableStatement");
            // Now unwrap the Callable to a statement and call a SQLServerStatement specific function and make sure it succeeds.
            ISQLServerPreparedStatement stmt4 = (ISQLServerPreparedStatement) ((SQLServerCallableStatement) cs)
                    .unwrap(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerPreparedStatement"));
            stmt4.setResponseBuffering("adaptive");

            if (isKatmaiServer())
                stmt4.setDateTimeOffset(1, null);

            // Try Unwrapping CallableStatement to a callableStatement
            isWrapper = ((SQLServerCallableStatement) cs).isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerCallableStatement"));
            assertEquals(isWrapper, true, "SQLServerCallableStatement should be a wrapper for SQLServerCallableStatement");
            // Now unwrap the Callable to a SQLServerCallableStatement and call a SQLServerStatement specific function and make sure it succeeds.
            SQLServerCallableStatement stmt3 = (SQLServerCallableStatement) ((SQLServerCallableStatement) cs)
                    .unwrap(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerCallableStatement"));
            stmt3.setResponseBuffering("adaptive");
            if (isKatmaiServer()) {
                stmt3.setDateTimeOffset(1, null);
            }
            if (null != stmt4) {
                stmt4.close();
            }
            if (null != cs) {
                cs.close();
            }

        }
        catch (UnsupportedOperationException e) {
            assertEquals(System.getProperty("java.specification.version"), "1.5", "isWrapperFor should be supported in anything other than 1.5");
            assertTrue(e.getMessage().equalsIgnoreCase("This operation is not supported."), "Wrong exception message");
        }
        finally {
            if (null != stmt) {
                stmt.close();
            }
            if (null != con) {
                con.close();
            }
        }

    }

    /**
     * Tests expected unwrapper failures
     * @throws Exception
     */
    @Test
    public void unWrapFailureTest() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection con = DriverManager.getConnection(connectionString);
        SQLServerStatement stmt = (SQLServerStatement) con.createStatement();

        try {
            String str = "java.lang.String";
            boolean isWrapper = stmt.isWrapperFor(Class.forName(str));
            stmt.unwrap(Class.forName(str));
            assertEquals(isWrapper, false, "SQLServerStatement should not be a wrapper for string");
            stmt.unwrap(Class.forName(str));
            assertTrue(false, "An exception should have been thrown. This code should not be reached");
        }
        catch (SQLException ex) {
            Throwable t = ex.getCause();
            Class exceptionClass = Class.forName("java.lang.ClassCastException");
            assertEquals(t.getClass(), exceptionClass, "The cause in the exception class does not match");
        }
        catch (UnsupportedOperationException e) {
            assertEquals(System.getProperty("java.specification.version"), "1.5", "isWrapperFor should be supported in anything other than 1.5");
            assertEquals(e.getMessage(), "This operation is not supported.", "Wrong exception message");
        }
        finally {
            if (null != stmt) {
                stmt.close();
            }
            if (null != con) {
                con.close();
            }
        }
    }

    private static boolean isKatmaiServer() throws Exception {
        DBConnection conn = new DBConnection(connectionString);
        double version = conn.getServerVersion();
        conn.close();
        return ((version >= 10.0) ? true : false);
    }

}
