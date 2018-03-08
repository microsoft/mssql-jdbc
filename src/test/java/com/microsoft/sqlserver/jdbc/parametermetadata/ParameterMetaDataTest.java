/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.parametermetadata;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;
import com.microsoft.sqlserver.testframework.util.RandomUtil;

@RunWith(JUnitPlatform.class)
public class ParameterMetaDataTest extends AbstractTest {
    private static final String tableName = "[" + RandomUtil.getIdentifier("StatementParam") + "]";

    /**
     * Test ParameterMetaData#isWrapperFor and ParameterMetaData#unwrap.
     * 
     * @throws SQLException
     */
    @Test
    public void testParameterMetaDataWrapper() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement()) {

            stmt.executeUpdate("create table " + tableName + " (col1 int identity(1,1) primary key)");
            try {
                String query = "SELECT * from " + tableName + " where col1 = ?";

                try (PreparedStatement pstmt = con.prepareStatement(query)) {
                    ParameterMetaData parameterMetaData = pstmt.getParameterMetaData();
                    assertTrue(parameterMetaData.isWrapperFor(ParameterMetaData.class));
                    assertSame(parameterMetaData, parameterMetaData.unwrap(ParameterMetaData.class));
                }
            }
            finally {
                Utils.dropTableIfExists(tableName, stmt);
            }
        }
    }

    /**
     * Test SQLException is not wrapped with another SQLException.
     * 
     * @throws SQLException
     */
    @Test
    public void testSQLServerExceptionNotWrapped() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString);
                PreparedStatement pstmt = connection.prepareStatement("invalid query :)");) {

            pstmt.getParameterMetaData();
        }
        catch (SQLException e) {
            assertTrue(!e.getMessage().contains("com.microsoft.sqlserver.jdbc.SQLException"),
                    "SQLException should not be wrapped by another SQLException.");
        }
    }
    
    /**
     * Test ParameterMetaData when parameter name contains braces
     * 
     * @throws SQLException
     */
    @Test
    public void testNameWithBraces() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement()) {

            stmt.executeUpdate("create table " + tableName + " ([c1_varchar(max)] varchar(max))");
            try {
                String query = "insert into " + tableName + " ([c1_varchar(max)]) values (?)";

                try (PreparedStatement pstmt = con.prepareStatement(query)) {
                    pstmt.getParameterMetaData();
                }
            }
            finally {
                Utils.dropTableIfExists(tableName, stmt);
            }
        }
    }
}
