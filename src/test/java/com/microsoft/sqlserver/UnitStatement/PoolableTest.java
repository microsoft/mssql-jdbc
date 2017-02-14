/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.UnitStatement;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;

@RunWith(JUnitPlatform.class)
public class PoolableTest extends AbstractTest {

    @Test
    @DisplayName("Poolable Test")
    public  void poolableTest() throws SQLException, ClassNotFoundException {
        Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        Connection Connection1 = DriverManager.getConnection(connectionString);
        Statement Statement151 = Connection1.createStatement();
        try {         
            // First get the default values
            boolean isPoolable = ((SQLServerStatement) Statement151).isPoolable();
            assertEquals(isPoolable, false, "SQLServerStatement should not be Poolable by default");

            PreparedStatement prepStmt = Connection1.prepareStatement("select 1");
            isPoolable = ((SQLServerPreparedStatement) prepStmt).isPoolable();
            assertEquals(isPoolable, true, "SQLServerPreparedStatement should be Poolable by default");


            CallableStatement CallableStatement1 = Connection1.prepareCall("{  ? = CALL " + "ProcName" + " (?, ?, ?, ?) }");
            isPoolable = ((SQLServerCallableStatement) CallableStatement1).isPoolable();

            assertEquals(isPoolable, true, "SQLServerCallableStatement should be Poolable by default");

            // Now do couple of sets and gets

            ((SQLServerCallableStatement) CallableStatement1).setPoolable(false);
            assertEquals(((SQLServerCallableStatement) CallableStatement1).isPoolable(), false, "set did not work");
            CallableStatement1.close();

            ((SQLServerStatement) Statement151).setPoolable(true);
            assertEquals(((SQLServerStatement) Statement151).isPoolable(), true, "set did not work");
            Statement151.close();

        }
        catch (UnsupportedOperationException e) {
            assertEquals(System.getProperty("java.specification.version"), "1.5", "PoolableTest should be supported in anything other than 1.5");
            assertEquals(e.getMessage(), "This operation is not supported.", "Wrong exception message");
        }
    }  
    
}
