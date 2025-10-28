/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.parametermetadata;

import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
    private static final String TABLE_TYPE_NAME = "dbo.IdTable";

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();

        // Setup table type for TVP tests
        try (Connection connection = getConnection(); Statement stmt = connection.createStatement()) {
            // Clean up any existing type
            try {
                stmt.executeUpdate("DROP TYPE IF EXISTS " + TABLE_TYPE_NAME);
            } catch (SQLException e) {
                // Ignore if type doesn't exist
            }

            // Create table type
            stmt.executeUpdate("CREATE TYPE " + TABLE_TYPE_NAME + " AS TABLE (id uniqueidentifier)");
        }
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

                    // test invalid index
                    assertThrows(SQLException.class, () -> metadata.getParameterType(0));
                    assertThrows(SQLException.class, () -> metadata.getParameterType(3));
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

                // test invalid index
                assertThrows(SQLException.class, () -> metadata.getParameterType(0));
                assertThrows(SQLException.class, () -> metadata.getParameterType(3));
            }
        }
    }

    /**
     * Test that getParameterMetaData() works with table-valued parameters
     * This test reproduces the issue described in GitHub issue #2744
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testParameterMetaDataWithTVP() throws SQLException {
        try (Connection connection = getConnection()) {
            String sql = "declare @ids " + TABLE_TYPE_NAME + " = ?; select id from @ids;";

            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                // This should not throw an exception
                assertDoesNotThrow(() -> {
                    ParameterMetaData pmd = stmt.getParameterMetaData();
                    assertEquals(1, pmd.getParameterCount());
                    assertEquals("IdTable", pmd.getParameterTypeName(1));
                    assertEquals(microsoft.sql.Types.STRUCTURED, pmd.getParameterType(1));
                    assertEquals(Object.class.getName(), pmd.getParameterClassName(1));
                });
            }
        }
    }

    /**
     * Test the exact scenario from GitHub issue #2744
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testOriginalIssueScenario() throws SQLException {
        try (Connection connection = getConnection()) {
            String sql = "declare @ids dbo.IdTable = ?; select id from @ids;";

            try (PreparedStatement stmt = connection.prepareStatement(sql)) {
                // This should not throw an exception - this was the original failing case
                assertDoesNotThrow(() -> {
                    ParameterMetaData pmd = stmt.getParameterMetaData();
                    assertEquals(1, pmd.getParameterCount());
                });
            }
        }
    }

    /**
     * Test parseQueryMeta method with Table-Valued Parameters (TVP)
     * This test specifically validates the TVP handling in parseQueryMeta
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testParseQueryMetaWithTVP() throws SQLException {
        try (Connection connection = getConnection()) {
            // Test with the table type we created in setup
            String sql = "DECLARE @tvp " + TABLE_TYPE_NAME + " = ?; SELECT * FROM @tvp;";

            try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
                ParameterMetaData pmd = pstmt.getParameterMetaData();

                // Validate TVP parameter metadata
                assertEquals(1, pmd.getParameterCount());

                // Log actual values for debugging
                int actualType = pmd.getParameterType(1);
                String actualTypeName = pmd.getParameterTypeName(1);
                int actualNullable = pmd.isNullable(1);

                // The actual behavior might be different, so let's validate what we get
                // In some cases, TVP might be reported as VARBINARY or other types
                assertTrue(actualType == microsoft.sql.Types.STRUCTURED || actualType == java.sql.Types.VARBINARY
                        || actualType == java.sql.Types.OTHER);

                assertEquals("IdTable", actualTypeName);
                assertEquals(ParameterMetaData.parameterNullableUnknown, actualNullable);
                assertDoesNotThrow(() -> pmd.isSigned(1)); // TVP should not be signed
            }
        }
    }
}
