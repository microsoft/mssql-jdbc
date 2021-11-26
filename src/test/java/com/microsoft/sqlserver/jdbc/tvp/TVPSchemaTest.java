/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
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
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class TVPSchemaTest extends AbstractTest {

    static SQLServerDataTable tvp = null;
    static String expectecValue1 = "hello";
    static String expectecValue2 = "world";
    static String expectecValue3 = "again";
    private static String schemaName;
    private static String tvpNameWithoutSchema;
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

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement();
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

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement();
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

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement();
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

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement();
                SQLServerCallableStatement P_C_stmt = (SQLServerCallableStatement) conn
                        .prepareCall("INSERT INTO " + charTable + " select * from ? ;");
                ResultSet rs = stmt.executeQuery("select * from " + charTable)) {
            P_C_stmt.setStructured(1, tvpNameWithSchema, tvp);
            P_C_stmt.executeUpdate();

            verify(rs);
        }
    }

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    @BeforeEach
    public void testSetup() throws SQLException {
        schemaName = RandomUtil.getIdentifier("anotherSchema");
        tvpNameWithoutSchema = RandomUtil.getIdentifier("charTVP");
        tvpNameWithSchema = AbstractSQLGenerator.escapeIdentifier(schemaName) + "."
                + AbstractSQLGenerator.escapeIdentifier(tvpNameWithoutSchema);
        charTable = AbstractSQLGenerator.escapeIdentifier(schemaName) + "."
                + AbstractSQLGenerator.escapeIdentifier("tvpCharTable");
        procedureName = AbstractSQLGenerator.escapeIdentifier(schemaName) + "."
                + AbstractSQLGenerator.escapeIdentifier("procedureThatCallsTVP");

        createSchema();
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

    private void dropObjects() throws SQLException {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            dropProcedure(stmt);
            dropTables(stmt);
            dropTVPS(stmt);
            dropSchema(stmt);
        }
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

    private void dropProcedure(Statement stmt) throws SQLException {
        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'"
                + TestUtils.escapeSingleQuotes(procedureName) + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)"
                + " DROP PROCEDURE " + procedureName;

        stmt.execute(sql);
    }

    private static void dropTables(Statement stmt2) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(charTable, stmt);
        }
    }

    private static void dropTVPS(Statement stmt2) throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate("IF EXISTS (SELECT * FROM sys.types WHERE is_table_type = 1 AND name = '"
                    + TestUtils.escapeSingleQuotes(tvpNameWithoutSchema) + "') " + " drop type " + tvpNameWithSchema);
        }
    }

    private static void createSchema() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            stmt.execute("CREATE SCHEMA " + AbstractSQLGenerator.escapeIdentifier(schemaName));
        }
    }

    private static void createPreocedure() throws SQLException {
        String sql = "CREATE PROCEDURE " + procedureName + " @InputData " + tvpNameWithSchema + " READONLY " + " AS "
                + " BEGIN " + " INSERT INTO " + charTable + " SELECT * FROM @InputData" + " END";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private void createTables() throws SQLException {
        String sql = "create table " + charTable + " (" + "PlainChar char(50) null," + "PlainVarchar varchar(50) null,"
                + "PlainVarcharMax varchar(max) null," + ");";
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(sql);
        }
    }

    private void createTVPS() throws SQLException {
        String TVPCreateCmd = "CREATE TYPE " + tvpNameWithSchema + " as table ( " + "PlainChar char(50) null,"
                + "PlainVarchar varchar(50) null," + "PlainVarcharMax varchar(max) null" + ")";
        try (Statement stmt = connection.createStatement()) {
            stmt.executeUpdate(TVPCreateCmd);
        }
    }

    @AfterEach
    public void terminateVariation() throws SQLException {
        dropObjects();

        if (null != tvp) {
            tvp.clear();
        }
    }

    private void dropSchema(Statement stmt) throws SQLException {
        TestUtils.dropSchemaIfExists(schemaName, stmt);
    }
}
