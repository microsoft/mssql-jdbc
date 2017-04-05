/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.resultset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerResultSet;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;
import com.microsoft.sqlserver.testframework.util.RandomUtil;

@RunWith(JUnitPlatform.class)
public class ResultSetTest extends AbstractTest {
    private static final String tableName = "[" + RandomUtil.getIdentifier("StatementParam") + "]";

    /**
     * Tests proper exception for unsupported operation
     * 
     * @throws Exception
     */
    @Test
    public void testJdbc41ResultSetMethods() throws Exception {
        Connection con = DriverManager.getConnection(connectionString);
        Statement stmt = con.createStatement();
        try {
            stmt.executeUpdate("create table " + tableName + " (col1 int, col2 text, col3 int identity(1,1) primary key)");

            stmt.executeUpdate("Insert into " + tableName + " values(0, 'hello')");

            stmt.executeUpdate("Insert into " + tableName + " values(0, 'yo')");

            ResultSet rs = stmt.executeQuery("select * from " + tableName);
            rs.next();
            // Both methods throw exceptions
            try {

                int col1 = rs.getObject(1, Integer.class);
            }
            catch (Exception e) {
                // unsupported feature
                assertEquals(e.getClass(), SQLFeatureNotSupportedException.class, "Verify exception type: " + e.getMessage());
            }
            try {
                String col2 = rs.getObject("col2", String.class);
            }
            catch (Exception e) {
                // unsupported feature
                assertEquals(e.getClass(), SQLFeatureNotSupportedException.class, "Verify exception type: " + e.getMessage());
            }
            try {
                stmt.executeUpdate("drop table " + tableName);
            }
            catch (Exception ex) {
                fail(ex.toString());
            }
        }
        finally {
            stmt.close();
            con.close();
        }
    }

    /**
     * Tests ResultSet#isWrapperFor and ResultSet#unwrap.
     * 
     * @throws SQLException
     */
    @Test
    public void testResultSetWrapper() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString);
             Statement stmt = con.createStatement()) {
            
            stmt.executeUpdate("create table " + tableName + " (col1 int, col2 text, col3 int identity(1,1) primary key)");
            
            try (ResultSet rs = stmt.executeQuery("select * from " + tableName)) {
                assertTrue(rs.isWrapperFor(ResultSet.class));
                assertTrue(rs.isWrapperFor(ISQLServerResultSet.class));

                assertSame(rs, rs.unwrap(ResultSet.class));
                assertSame(rs, rs.unwrap(ISQLServerResultSet.class));
            } finally {
                Utils.dropTableIfExists(tableName, stmt);
            }
        }
    }
    
}
