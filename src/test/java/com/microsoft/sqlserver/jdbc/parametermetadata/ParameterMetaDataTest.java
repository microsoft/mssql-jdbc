/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
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

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class ParameterMetaDataTest extends AbstractTest {
    private static final String tableName = RandomUtil.getIdentifier("StatementParam");

    /**
     * Test ParameterMetaData#isWrapperFor and ParameterMetaData#unwrap.
     * 
     * @throws SQLException
     */
    @Test
    public void testParameterMetaDataWrapper() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement()) {

            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (col1 int identity(1,1) primary key)");
            try {
                String query = "SELECT * from " + AbstractSQLGenerator.escapeIdentifier(tableName) + " where col1 = ?";

                try (PreparedStatement pstmt = con.prepareStatement(query)) {
                    ParameterMetaData parameterMetaData = pstmt.getParameterMetaData();
                    assertTrue(parameterMetaData.isWrapperFor(ParameterMetaData.class));
                    assertSame(parameterMetaData, parameterMetaData.unwrap(ParameterMetaData.class));
                }
            } finally {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
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
        } catch (SQLException e) {
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

            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " ([c1_varchar(max)] varchar(max))");
            try {
                String query = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " ([c1_varchar(max)]) values (?)";

                try (PreparedStatement pstmt = con.prepareStatement(query)) {
                    pstmt.getParameterMetaData();
                }
            } finally {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    /**
     * Test ParameterMetaData when parameter name containing apostrophe
     * 
     * @throws SQLException
     */
    @Test
    public void testParameterMetaData() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement()) {

            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " ([c1_varchar(max)] varchar(max), c2 decimal(38,5))");
            try {
                String query = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " ([c1_varchar(max)], c2) values (?,?)";

                try (PreparedStatement pstmt = con.prepareStatement(query)) {
                    ParameterMetaData metadata = pstmt.getParameterMetaData();
                    assert (metadata.getParameterCount() == 2);
                    assert (metadata.getParameterTypeName(1).equalsIgnoreCase("varchar"));
                    assert (metadata.getParameterTypeName(2).equalsIgnoreCase("decimal"));
                    assert (metadata.getPrecision(2) == 38);
                    assert (metadata.getScale(2) == 5);
                }
            } finally {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }
}
