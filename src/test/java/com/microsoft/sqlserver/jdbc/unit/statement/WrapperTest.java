/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;


/**
 * Tests isWrapperFor methods
 *
 */
@RunWith(JUnitPlatform.class)
@Tag("AzureDWTest")
public class WrapperTest extends AbstractTest {

    /**
     * Wrapper tests
     * 
     * @throws Exception
     */
    @Test
    public void wrapTest() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        MessageFormat form = new MessageFormat(TestResource.getResource("R_shouldBeWrapper"));
        MessageFormat form2 = new MessageFormat(TestResource.getResource("R_shouldNotBeWrapper"));
        Object[][] msgArgs = {{"SQLStatement", "self"}, {"SQLServerStatement", "ISQLServerStatement"},
                {"SQLServerCallableStatement", "SQLServerStatement"},
                {"SQLServerCallableStatement", "SQLServerStatement"}};

        try (Connection con = DriverManager.getConnection(connectionString)) {
            try (Statement stmt = con.createStatement()) {

                // First make sure that a statement can be unwrapped
                boolean isWrapper = ((SQLServerStatement) stmt)
                        .isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerStatement"));

                assertEquals(isWrapper, true, form.format(msgArgs[0]));

                isWrapper = ((SQLServerStatement) stmt)
                        .isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerStatement"));
                assertEquals(isWrapper, true, form.format(msgArgs[1]));

                isWrapper = ((SQLServerStatement) stmt)
                        .isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnection"));
                assertEquals(isWrapper, false, form2.format(msgArgs[1]));

                // Now make sure that we can unwrap a SQLServerCallableStatement to a SQLServerStatement
                try (CallableStatement cs = con.prepareCall("{  ? = CALL " + "ProcName" + " (?, ?, ?, ?) }")) {
                    // Test the class first
                    isWrapper = ((SQLServerCallableStatement) cs)
                            .isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerStatement"));
                    assertEquals(isWrapper, true, form.format(msgArgs[2]));
                    // Now unwrap the Callable to a statement and call a SQLServerStatement specific function and make
                    // sure it succeeds.
                    try (SQLServerStatement stmt2 = (SQLServerStatement) ((SQLServerCallableStatement) cs)
                            .unwrap(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerStatement"))) {
                        stmt2.setResponseBuffering("adaptive");

                        // now test the interface
                        isWrapper = ((SQLServerCallableStatement) cs).isWrapperFor(
                                Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerCallableStatement"));
                        assertEquals(isWrapper, true, form.format(msgArgs[1]));

                        // Now unwrap the Callable to a statement and call a SQLServerStatement specific function and
                        // make sure it succeeds.
                        try (ISQLServerPreparedStatement stmt3 = (ISQLServerPreparedStatement) ((SQLServerCallableStatement) cs)
                                .unwrap(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerPreparedStatement"))) {
                            stmt3.setResponseBuffering("adaptive");

                            if (isKatmaiServer())
                                stmt3.setDateTimeOffset(1, null);

                            // Try Unwrapping CallableStatement to a callableStatement
                            isWrapper = ((SQLServerCallableStatement) cs).isWrapperFor(
                                    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerCallableStatement"));
                            assertEquals(isWrapper, true, form.format(msgArgs[3]));
                            // Now unwrap the Callable to a SQLServerCallableStatement and call a SQLServerStatement
                            // specific function and make sure it succeeds.
                            try (SQLServerCallableStatement stmt4 = (SQLServerCallableStatement) ((SQLServerCallableStatement) cs)
                                    .unwrap(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerCallableStatement"))) {
                                stmt4.setResponseBuffering("adaptive");
                                if (isKatmaiServer()) {
                                    stmt4.setDateTimeOffset(1, null);
                                }
                            }
                        }
                    }
                }
            } catch (UnsupportedOperationException e) {
                assertEquals(System.getProperty("java.specification.version"), "1.5",
                        "isWrapperFor " + TestResource.getResource("R_shouldBeSupported"));
                assertTrue(e.getMessage().equalsIgnoreCase("This operation is not supported."),
                        TestResource.getResource("R_unexpectedExceptionContent"));
            }
        }
    }

    /**
     * Tests expected unwrapper failures
     * 
     * @throws Exception
     */
    @Test
    public void unWrapFailureTest() throws Exception {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        try (Connection con = DriverManager.getConnection(connectionString);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String str = "java.lang.String";
            boolean isWrapper = stmt.isWrapperFor(Class.forName(str));
            stmt.unwrap(Class.forName(str));
            assertEquals(isWrapper, false, "SQLServerStatement should not be a wrapper for string");
            stmt.unwrap(Class.forName(str));
            assertTrue(false, TestResource.getResource("R_exceptionNotThrown"));
        } catch (SQLException ex) {
            Throwable t = ex.getCause();
            Class exceptionClass = Class.forName("java.lang.ClassCastException");
            assertEquals(t.getClass(), exceptionClass, "The cause in the exception class does not match");
        } catch (UnsupportedOperationException e) {
            assertEquals(System.getProperty("java.specification.version"), "1.5",
                    "isWrapperFor " + TestResource.getResource("R_shouldBeSupported"));
            assertEquals(e.getMessage(), "This operation is not supported.",
                    TestResource.getResource("R_unexpectedExceptionContent"));
        }
    }

    private static boolean isKatmaiServer() throws Exception {
        try (DBConnection conn = new DBConnection(connectionString)) {
            double version = conn.getServerVersion();
            return ((version >= 10.0) ? true : false);
        }
    }
}
