/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.parametermetadata;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
public class ParameterMetaDataTest extends AbstractTest {
    private static final String tableName = RandomUtil.getIdentifier("StatementParam");

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    /**
     * Test ParameterMetaData#isWrapperFor and ParameterMetaData#unwrap.
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testParameterMetaDataWrapper() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (col1 int identity(1,1) primary key)");
            try {
                String query = "SELECT * from " + AbstractSQLGenerator.escapeIdentifier(tableName) + " where col1 = ?";

                try (PreparedStatement pstmt = connection.prepareStatement(query)) {
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
        try (PreparedStatement pstmt = connection.prepareStatement("invalid query :)");) {
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
    @Tag(Constants.xAzureSQLDW)
    public void testNameWithBraces() throws SQLException {
        try (Statement stmt = connection.createStatement()) {

            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " ([c1_varchar(max)] varchar(max))");
            try {
                String query = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " ([c1_varchar(max)]) values (?)";

                try (PreparedStatement pstmt = connection.prepareStatement(query)) {
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
    @Tag(Constants.xAzureSQLDW)
    public void testParameterMetaData() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {

            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " ([c1_varchar(max)] varchar(max), c2 decimal(38,5))");
            try {
                String query = "insert into " + AbstractSQLGenerator.escapeIdentifier(tableName)
                        + " ([c1_varchar(max)], c2) values (?,?)";

                try (PreparedStatement pstmt = con.prepareStatement(query)) {
                    ParameterMetaData metadata = pstmt.getParameterMetaData();
                    assert (metadata.getParameterCount() == 2);
                    assert (metadata.getParameterType(1) == java.sql.Types.VARCHAR);
                    assert (metadata.getParameterTypeName(1).equalsIgnoreCase("varchar"));
                    assert (metadata.getParameterType(2) == java.sql.Types.DECIMAL);
                    assert (metadata.getParameterTypeName(2).equalsIgnoreCase("decimal"));
                    assert (metadata.getParameterMode(1) == ParameterMetaData.parameterModeIn);
                    assert (metadata.getPrecision(2) == 38);
                    assert (metadata.getScale(2) == 5);
                    assert (!metadata.isSigned(1));
                }
            } finally {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
            }
        }
    }

    /**
     * Test MetaData for Stored procedure sp_help
     * 
     * Stored Procedure reference:
     * https://docs.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sp-help-transact-sql
     * 
     * @throws SQLException
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testParameterMetaDataProc() throws SQLException {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            String query = "exec sp_help (" + AbstractSQLGenerator.escapeIdentifier(tableName) + ")";

            try (PreparedStatement pstmt = con.prepareStatement(query)) {
                ParameterMetaData metadata = pstmt.getParameterMetaData();
                assert (metadata.getParameterCount() == 1);
                assert (metadata.getParameterType(1) == java.sql.Types.NVARCHAR);
                assert (metadata.getParameterTypeName(1).equalsIgnoreCase("nvarchar"));
                assert (metadata.getParameterClassName(1).equalsIgnoreCase(String.class.getName()));
                assert (metadata.getParameterMode(1) == ParameterMetaData.parameterModeIn);
                // Standard value - validate precision of sp_help stored procedure parameter
                assert (metadata.getPrecision(1) == 776);
                assert (metadata.getScale(1) == 0);
                assert (metadata.isNullable(1) == ParameterMetaData.parameterNullable);
                assert (!metadata.isSigned(1));
            }
        }
    }
}
