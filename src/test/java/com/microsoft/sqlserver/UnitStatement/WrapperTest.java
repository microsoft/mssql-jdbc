/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.UnitStatement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBDriver;

@RunWith(JUnitPlatform.class)
public class WrapperTest extends AbstractTest {

    @Test
    public void WrapTest() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection Connection1 = DriverManager.getConnection(connectionString);
        Statement Statement151 = Connection1.createStatement();

        try {
            // First make sure that a statement can be unwrapped
            boolean isWrapper = ((SQLServerStatement) Statement151).isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerStatement"));
            assertEquals(isWrapper, true, "SQLServerStatement should be a wrapper for self");
            isWrapper = ((SQLServerStatement) Statement151).isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerStatement"));
            assertEquals(isWrapper, true, "SQLServerStatement should be a wrapper for ISQLServerStatement");

            isWrapper = ((SQLServerStatement) Statement151).isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection"));
            assertEquals(isWrapper, false, "SQLServerStatement should not be a wrapper for SQLServerConnection");

            // Now make sure that we can unwrap a SQLServerCallableStatement to a SQLServerStatement
            CallableStatement CallableStatement1 = Connection1.prepareCall("{  ? = CALL " + "ProcName" + " (?, ?, ?, ?) }");
            // Test the class first
            isWrapper = ((SQLServerCallableStatement) CallableStatement1)
                    .isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerStatement"));
            assertEquals(isWrapper, true, "SQLServerCallableStatement should be a wrapper for SQLServerStatement");
            // Now unwrap the Callable to a statement and call a SQLServerStatement specific function and make sure it succeeds.
            SQLServerStatement stmt2 = (SQLServerStatement) ((SQLServerCallableStatement) CallableStatement1)
                    .unwrap(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerStatement"));
            stmt2.setResponseBuffering("adaptive");
            // now test the interface
            isWrapper = ((SQLServerCallableStatement) CallableStatement1)
                    .isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerCallableStatement"));
            assertEquals(isWrapper, true, "SQLServerCallableStatement should be a wrapper for ISQLServerCallableStatement");
            // Now unwrap the Callable to a statement and call a SQLServerStatement specific function and make sure it succeeds.
            ISQLServerPreparedStatement stmt4 = (ISQLServerPreparedStatement) ((SQLServerCallableStatement) CallableStatement1)
                    .unwrap(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerPreparedStatement"));
            stmt4.setResponseBuffering("adaptive");

            if (isKatmaiServer())
                stmt4.setDateTimeOffset(1, null);

            // Try Unwrapping CallableStatement to a callableStatement
            isWrapper = ((SQLServerCallableStatement) CallableStatement1)
                    .isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerCallableStatement"));
            assertEquals(isWrapper, true, "SQLServerCallableStatement should be a wrapper for SQLServerCallableStatement");
            // Now unwrap the Callable to a SQLServerCallableStatement and call a SQLServerStatement specific function and make sure it succeeds.
            SQLServerCallableStatement stmt3 = (SQLServerCallableStatement) ((SQLServerCallableStatement) CallableStatement1)
                    .unwrap(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerCallableStatement"));
            stmt3.setResponseBuffering("adaptive");
            if (isKatmaiServer())
                stmt3.setDateTimeOffset(1, null);

            if (null != CallableStatement1) {
                CallableStatement1.close();
            }

        }
        catch (UnsupportedOperationException e) {
            assertEquals(System.getProperty("java.specification.version"), "1.5", "isWrapperFor should be supported in anything other than 1.5");
            assertTrue(e.getMessage().equalsIgnoreCase("This operation is not supported."), "Wrong exception message");
        }
        finally {
            if (null != Statement151) {
                Statement151.close();
            }
            if (null != Connection1) {
                Connection1.close();
            }
        }

    }

    @Test
    public void UnWrapFailureTest() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection Connection1 = DriverManager.getConnection(connectionString);
        SQLServerStatement stmt1 = (SQLServerStatement) Connection1.createStatement();

        try {
            String str = "java.lang.String";
            boolean isWrapper = stmt1.isWrapperFor(Class.forName(str));
            stmt1.unwrap(Class.forName(str));
            assertEquals(isWrapper, false, "SQLServerStatement should not be a wrapper for string");
            stmt1.unwrap(Class.forName(str));
            assertTrue(false, "An exception should have been thrown. This code should not be reached");
        }
        catch (SQLServerException ex) {
            Throwable t = ex.getCause();
            Class exceptionClass = Class.forName("java.lang.ClassCastException");
            assertEquals(t.getClass(), exceptionClass, "The cause in the exception class does not match");
        }
        catch (UnsupportedOperationException e) {
            assertEquals(System.getProperty("java.specification.version"), "1.5", "isWrapperFor should be supported in anything other than 1.5");
            assertEquals(e.getMessage(), "This operation is not supported.", "Wrong exception message");
        }
        finally {
            if (null != stmt1) {
                stmt1.close();
            }
            if (null != Connection1) {
                Connection1.close();
            }
        }
    }

    private static boolean isKatmaiServer() throws Exception {
        double version = DBDriver.serverversion();
        return ((version >= 10.0) ? true : false);
    }

}
