/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;

/**
 * Test Poolable statements
 *
 */
@RunWith(JUnitPlatform.class)
public class PoolableTest extends AbstractTest {

    /**
     * Poolable Test
     * 
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    @Test
    @DisplayName("Poolable Test")
    public void poolableTest() throws SQLException, ClassNotFoundException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement statement = conn.createStatement()) {
            try {
                // First get the default values
                boolean isPoolable = ((SQLServerStatement) statement).isPoolable();
                assertEquals(isPoolable, false, "SQLServerStatement should not be Poolable by default");

                try (PreparedStatement prepStmt = connection.prepareStatement("select 1")) {
                    isPoolable = ((SQLServerPreparedStatement) prepStmt).isPoolable();
                    assertEquals(isPoolable, true, "SQLServerPreparedStatement should be Poolable by default");
                }

                try (CallableStatement callableStatement = connection.prepareCall("{  ? = CALL " + "ProcName" + " (?, ?, ?, ?) }");) {
                    isPoolable = ((SQLServerCallableStatement) callableStatement).isPoolable();

                    assertEquals(isPoolable, true, "SQLServerCallableStatement should be Poolable by default");

                    // Now do couple of sets and gets

                    ((SQLServerCallableStatement) callableStatement).setPoolable(false);
                    assertEquals(((SQLServerCallableStatement) callableStatement).isPoolable(), false, "set did not work");
                }

                ((SQLServerStatement) statement).setPoolable(true);
                assertEquals(((SQLServerStatement) statement).isPoolable(), true, "set did not work");
            }
            catch (UnsupportedOperationException e) {
                assertEquals(System.getProperty("java.specification.version"), "1.5", "PoolableTest should be supported in anything other than 1.5");
                assertEquals(e.getMessage(), "This operation is not supported.", "Wrong exception message");
            }
        }
    }

    /**
     * Clean up
     * 
     * @throws Exception
     */
    @AfterAll
    public static void afterAll() throws Exception {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            try {
                Utils.dropProcedureIfExists("ProcName", stmt);
            }
            catch (Exception ex) {
                fail(ex.toString());
            }
        }
    }

}
