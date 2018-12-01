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
    private static Connection conn = null;
    static Statement stmt = null;

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
        setupVariation(setSelectMethod, resultSetType, resultSetConcurrency);

        try (ResultSet rs = stmt.executeQuery("select * from " + tableSrc.getEscapedTableName());
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) conn
                        .prepareStatement("INSERT INTO " + tableDest.getEscapedTableName() + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, rs);
            pstmt.execute();

            ComparisonUtil.compareSrcTableAndDestTableIgnoreRowOrder(new DBConnection(connectionString), tableSrc,
                    tableDest);
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
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
        setupVariation(setSelectMethod, resultSetType, resultSetConcurrency);
        try (ResultSet rs = stmt.executeQuery("select * from " + tableSrc.getEscapedTableName());
                SQLServerCallableStatement Cstmt = (SQLServerCallableStatement) conn
                        .prepareCall("{call " + AbstractSQLGenerator.escapeIdentifier(procedureName) + "(?)}")) {
            Cstmt.setStructured(1, tvpName, rs);
            Cstmt.execute();

            ComparisonUtil.compareSrcTableAndDestTableIgnoreRowOrder(new DBConnection(connectionString), tableSrc,
                    tableDest);
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
        setupVariation(false, null, null);

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

        try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) connection
                .prepareStatement("INSERT INTO " + tableDest.getEscapedTableName() + " select * from ? ;")) {
            pstmt.setStructured(1, tvpName, dt);
            pstmt.execute();
        } finally {
            terminateVariation();
        }
    }

    private static void createPreocedure(String procedureName, String destTable) throws SQLException {
        String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procedureName) + " @InputData "
                + AbstractSQLGenerator.escapeIdentifier(tvpName) + " READONLY " + " AS " + " BEGIN " + " INSERT INTO "
                + destTable + " SELECT * FROM @InputData" + " END";

        stmt.execute(sql);
    }

    private static void createTVPS(String tvpName, String TVPDefinition) throws SQLException {
        String TVPCreateCmd = "CREATE TYPE " + AbstractSQLGenerator.escapeIdentifier(tvpName) + " as table ("
                + TVPDefinition + ");";
        stmt.executeUpdate(TVPCreateCmd);
    }

    private void setupVariation(boolean setSelectMethod, Integer resultSetType,
            Integer resultSetConcurrency) throws SQLException {

        tvpName = RandomUtil.getIdentifier("TVP");
        procedureName = RandomUtil.getIdentifier("TVP");

        if (setSelectMethod) {
            conn = DriverManager.getConnection(connectionString + ";selectMethod=cursor;");
        } else {
            conn = DriverManager.getConnection(connectionString);
        }

        if (null != resultSetType || null != resultSetConcurrency) {
            stmt = conn.createStatement(resultSetType, resultSetConcurrency);
        } else {
            stmt = conn.createStatement();
        }

        TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(procedureName), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tvpName), stmt);

        try (DBConnection dbConnection = new DBConnection(connectionString);
                DBStatement dbStmt = dbConnection.createStatement()) {

            tableSrc = new DBTable(true);
            tableDest = tableSrc.cloneSchema();

            dbStmt.createTable(tableSrc);
            dbStmt.createTable(tableDest);

            createTVPS(tvpName, tableSrc.getDefinitionOfColumns());
            createPreocedure(procedureName, tableDest.getEscapedTableName());

            dbStmt.populateTable(tableSrc);
        }
    }

    private void terminateVariation() throws SQLException {
        TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(procedureName), stmt);
        TestUtils.dropTableIfExists(tableSrc.getEscapedTableName(), stmt);
        TestUtils.dropTableIfExists(tableDest.getEscapedTableName(), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tvpName), stmt);

        if (null != stmt) {
            stmt.close();
        }
        if (null != conn) {
            conn.close();
        }
    }
}
