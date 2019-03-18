/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class TVPSchemaTest extends AbstractTest {

    static SQLServerDataTable tvp = null;
    static String expectecValue1 = "hello";
    static String expectecValue2 = "world";
    static String expectecValue3 = "again";
    private static String schemaName;
    private static String tvpNameWithouSchema;
    private static String tvpNameWithSchema;
    private static String charTable;
    private static String procedureName;

    /**
     * PreparedStatement with storedProcedure
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("TVPSchemaPreparedStatementStoredProcedure()")
    public void testTVPSchemaPreparedStatementStoredProcedure() throws SQLException {

        final String sql = "{call " + procedureName + "(?)}";

        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement();
                SQLServerPreparedStatement P_C_statement = (SQLServerPreparedStatement) conn.prepareStatement(sql);
                ResultSet rs = stmt.executeQuery("select * from " + charTable)) {
            P_C_statement.setStructured(1, tvpNameWithSchema, tvp);
            P_C_statement.execute();

            verify(rs);

        }
    }

    /**
     * callableStatement with StoredProcedure
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("TVPSchemaCallableStatementStoredProcedure()")
    public void testTVPSchemaCallableStatementStoredProcedure() throws SQLException {

        final String sql = "{call " + procedureName + "(?)}";

        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement();
                SQLServerCallableStatement P_C_statement = (SQLServerCallableStatement) conn.prepareCall(sql);
                ResultSet rs = stmt.executeQuery("select * from " + charTable)) {
            P_C_statement.setStructured(1, tvpNameWithSchema, tvp);
            P_C_statement.execute();

            verify(rs);
        }
    }

    /**
     * Prepared with InsertCommand
     * 
     * @throws SQLException
     * @throws IOException
     */
    @Test
    @DisplayName("TVPSchemaPreparedInsertCommand")
    public void testTVPSchemaPreparedInsertCommand() throws SQLException, IOException {

        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement();
                SQLServerPreparedStatement P_C_stmt = (SQLServerPreparedStatement) conn
                        .prepareStatement("INSERT INTO " + charTable + " select * from ? ;");
                ResultSet rs = stmt.executeQuery("select * from " + charTable)) {
            P_C_stmt.setStructured(1, tvpNameWithSchema, tvp);
            P_C_stmt.executeUpdate();

            verify(rs);
        }
    }

    /**
     * Callable with InsertCommand
     * 
     * @throws SQLException
     * @throws IOException
     */
    @Test
    @DisplayName("TVPSchemaCallableInsertCommand()")
    public void testTVPSchemaCallableInsertCommand() throws SQLException, IOException {

        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement();
                SQLServerCallableStatement P_C_stmt = (SQLServerCallableStatement) conn
                        .prepareCall("INSERT INTO " + charTable + " select * from ? ;");
                ResultSet rs = stmt.executeQuery("select * from " + charTable)) {
            P_C_stmt.setStructured(1, tvpNameWithSchema, tvp);
            P_C_stmt.executeUpdate();

            verify(rs);
        }
    }

    @BeforeEach
    public void testSetup() throws SQLException {
        schemaName = RandomUtil.getIdentifier("anotherSchema");
        tvpNameWithouSchema = RandomUtil.getIdentifier("charTVP");
        tvpNameWithSchema = AbstractSQLGenerator.escapeIdentifier(schemaName) + "."
                + AbstractSQLGenerator.escapeIdentifier(tvpNameWithouSchema);

        charTable = AbstractSQLGenerator.escapeIdentifier(schemaName) + "."
                + AbstractSQLGenerator.escapeIdentifier("tvpCharTable");
        procedureName = AbstractSQLGenerator.escapeIdentifier(schemaName) + "."
                + AbstractSQLGenerator.escapeIdentifier("procedureThatCallsTVP");

        dropProcedure();
        dropTables();
        dropTVPS();

        dropAndCreateSchema();

        createTVPS();
        createTables();
        createPreocedure();

        tvp = new SQLServerDataTable();
        tvp.addColumnMetadata("PlainChar", java.sql.Types.CHAR);
        tvp.addColumnMetadata("PlainVarchar", java.sql.Types.VARCHAR);
        tvp.addColumnMetadata("PlainVarcharMax", java.sql.Types.VARCHAR);

        tvp.addRow(expectecValue1, expectecValue2, expectecValue3);
        tvp.addRow(expectecValue1, expectecValue2, expectecValue3);
        tvp.addRow(expectecValue1, expectecValue2, expectecValue3);
        tvp.addRow(expectecValue1, expectecValue2, expectecValue3);
        tvp.addRow(expectecValue1, expectecValue2, expectecValue3);
    }

    private void verify(ResultSet rs) throws SQLException {
        while (rs.next()) {
            String actualValue1 = rs.getString(1);
            String actualValue2 = rs.getString(2);
            String actualValue3 = rs.getString(3);

            assertEquals(actualValue1.trim(), expectecValue1);
            assertEquals(actualValue2.trim(), expectecValue2);
            assertEquals(actualValue3.trim(), expectecValue3);
        }
    }

    private void dropProcedure() throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(procedureName) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + procedureName;

        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private static void dropTables() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("if object_id('" + TestUtils.escapeSingleQuotes(charTable) + "','U') is not null"
                    + " drop table " + charTable);
        }
    }

    private static void dropTVPS() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '"
                    + TestUtils.escapeSingleQuotes(tvpNameWithouSchema) + "') " + " drop type " + tvpNameWithSchema);
        }
    }

    private static void dropAndCreateSchema() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.execute(
                    "if EXISTS (SELECT * FROM sys.schemas where name = '" + TestUtils.escapeSingleQuotes(schemaName)
                            + "') drop schema " + AbstractSQLGenerator.escapeIdentifier(schemaName));
            stmt.execute("CREATE SCHEMA " + AbstractSQLGenerator.escapeIdentifier(schemaName));
        }
    }

    private static void createPreocedure() throws SQLException {
        String sql = "CREATE PROCEDURE " + procedureName + " @InputData " + tvpNameWithSchema + " READONLY " + " AS "
                + " BEGIN " + " INSERT INTO " + charTable + " SELECT * FROM @InputData" + " END";

        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private void createTables() throws SQLException {
        String sql = "create table " + charTable + " (" + "PlainChar char(50) null," + "PlainVarchar varchar(50) null,"
                + "PlainVarcharMax varchar(max) null," + ");";
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
        }
    }

    private void createTVPS() throws SQLException {
        String TVPCreateCmd = "CREATE TYPE " + tvpNameWithSchema + " as table ( " + "PlainChar char(50) null,"
                + "PlainVarchar varchar(50) null," + "PlainVarcharMax varchar(max) null" + ")";
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(TVPCreateCmd);
        }
    }

    @AfterEach
    public void terminateVariation() throws SQLException {
        dropProcedure();
        dropTables();
        dropTVPS();

        if (null != tvp) {
            tvp.clear();
        }
    }
}
