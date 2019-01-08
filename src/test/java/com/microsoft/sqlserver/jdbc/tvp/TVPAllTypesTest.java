/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.tvp;

import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ComparisonUtil;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;
import com.microsoft.sqlserver.testframework.sqlType.SqlType;


@RunWith(JUnitPlatform.class)
public class TVPAllTypesTest extends AbstractTest {

    private static String tvpName;
    private static String procedureName;
    private static DBTable tableSrc = null;
    private static DBTable tableDest = null;

    /**
     * Test TVP with result set
     * 
     * @throws SQLException
     */
    @Test
    public void testTVPResultSet() throws SQLException {
        testTVPResultSet(false, null, null);
        testTVPResultSet(true, null, null);
        testTVPResultSet(false, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        testTVPResultSet(false, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        testTVPResultSet(false, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        testTVPResultSet(false, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
    }

    private void testTVPResultSet(boolean setSelectMethod, Integer resultSetType,
            Integer resultSetConcurrency) throws SQLException {

        String connString;
        Statement stmt = null;

        if (setSelectMethod) {
            connString = connectionString + ";selectMethod=cursor;";
        } else {
            connString = connectionString;
        }

        try (Connection conn = DriverManager.getConnection(connString)) {
            if (null != resultSetType && null != resultSetConcurrency) {
                stmt = conn.createStatement(resultSetType, resultSetConcurrency);
            } else {
                stmt = conn.createStatement();
            }

            setupVariation(setSelectMethod, stmt);
            try (ResultSet rs = stmt.executeQuery("select * from " + tableSrc.getEscapedTableName());
                    SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn.prepareStatement(
                            "INSERT INTO " + tableDest.getEscapedTableName() + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, rs);
                pstmt.execute();
                ComparisonUtil.compareSrcTableAndDestTableIgnoreRowOrder(new DBConnection(conn), tableSrc,
                        tableDest);
            } catch (Exception e) {
                fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
            } finally {
                stmt.close();
            }
        } finally {
            terminateVariation();
        }
    }

    /**
     * Test TVP with stored procedure and result set
     * 
     * @throws SQLException
     */
    @Test
    public void testTVPStoredProcedureResultSet() throws SQLException {
        testTVPStoredProcedureResultSet(false, null, null);
        testTVPStoredProcedureResultSet(true, null, null);
        testTVPStoredProcedureResultSet(false, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        testTVPStoredProcedureResultSet(false, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
        testTVPStoredProcedureResultSet(false, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);
        testTVPStoredProcedureResultSet(false, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
    }

    private void testTVPStoredProcedureResultSet(boolean setSelectMethod, Integer resultSetType,
            Integer resultSetConcurrency) throws SQLException {
        String connString;
        Statement stmt = null;

        if (setSelectMethod) {
            connString = connectionString + ";selectMethod=cursor;";
        } else {
            connString = connectionString;
        }

        try (Connection conn = DriverManager.getConnection(connString)) {
            if (null != resultSetType && null != resultSetConcurrency) {
                stmt = conn.createStatement(resultSetType, resultSetConcurrency);
            } else {
                stmt = conn.createStatement();
            }

            setupVariation(setSelectMethod, stmt);
            try (ResultSet rs = stmt.executeQuery("select * from " + tableSrc.getEscapedTableName());
                    SQLServerCallableStatement Cstmt = (SQLServerCallableStatement) conn
                            .prepareCall("{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}")) {
                Cstmt.setStructured(1, tvpName, rs);
                Cstmt.execute();
                ComparisonUtil.compareSrcTableAndDestTableIgnoreRowOrder(new DBConnection(conn), tableSrc,
                        tableDest);
            } finally {
                stmt.close();
            }
        } finally {
            terminateVariation();
        }
    }

    /**
     * Test TVP with DataTable
     * 
     * @throws SQLException
     */
    @Test
    public void testTVPDataTable() throws SQLException {

        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            setupVariation(false, stmt);
            SQLServerDataTable dt = new SQLServerDataTable();
            int numberOfColumns = tableDest.getColumns().size();
            Object[] values = new Object[numberOfColumns];

            for (int i = 0; i < numberOfColumns; i++) {
                SqlType sqlType = tableDest.getColumns().get(i).getSqlType();
                dt.addColumnMetadata(tableDest.getColumnName(i), sqlType.getJdbctype().getVendorTypeNumber());
                values[i] = sqlType.createdata();
            }

            int numberOfRows = 10;
            for (int i = 0; i < numberOfRows; i++) {
                dt.addRow(values);
            }

            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn
                    .prepareStatement("INSERT INTO " + tableDest.getEscapedTableName() + " select * from ? ;")) {
                pstmt.setStructured(1, tvpName, dt);
                pstmt.execute();
            }
        } finally {
            terminateVariation();
        }
    }

    private static void createProcedure(String procedureName, String destTable, Statement stmt) throws SQLException {
        String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procedureName) + " @InputData "
                + AbstractSQLGenerator.escapeIdentifier(tvpName) + " READONLY " + " AS " + " BEGIN " + " INSERT INTO "
                + destTable + " SELECT * FROM @InputData" + " END";

        stmt.execute(sql);
    }

    private static void createTVPS(String tvpName, String TVPDefinition, Statement stmt) throws SQLException {
        String TVPCreateCmd = "CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName) + " as table ("
                + TVPDefinition + ");";
        stmt.executeUpdate(TVPCreateCmd);
    }

    private void setupVariation(boolean setSelectMethod, Statement stmt) throws SQLException {

        tvpName = RandomUtil.getIdentifier("TVP");
        procedureName = RandomUtil.getIdentifier("TVP");

        TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(procedureName), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tvpName), stmt);

        try (DBConnection dbConnection = new DBConnection(connectionString);
                DBStatement dbStmt = dbConnection.createStatement()) {

            tableSrc = new DBTable(true);
            tableDest = tableSrc.cloneSchema();

            dbStmt.createTable(tableSrc);
            dbStmt.createTable(tableDest);

            createTVPS(tvpName, tableSrc.getDefinitionOfColumns(), stmt);
            createProcedure(procedureName, tableDest.getEscapedTableName(), stmt);

            dbStmt.populateTable(tableSrc);
        }
    }

    private void terminateVariation() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(procedureName), stmt);
            TestUtils.dropTableIfExists(tableSrc.getEscapedTableName(), stmt);
            TestUtils.dropTableIfExists(tableDest.getEscapedTableName(), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tvpName), stmt);
        }
    }
}
