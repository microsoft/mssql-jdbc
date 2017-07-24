/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.parametermetadata;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;
import com.microsoft.sqlserver.testframework.util.RandomUtil;

@RunWith(JUnitPlatform.class)
public class ParameterMetaDataWhiteSpaceTest extends AbstractTest {
    private static final String tableName = "[" + RandomUtil.getIdentifier("ParameterMetaDataWhiteSpaceTest") + "]";

    private static Statement stmt = null;

    @BeforeAll
    public static void BeforeTests() throws SQLException {
        connection = (SQLServerConnection) DriverManager.getConnection(connectionString);
        stmt = connection.createStatement();
        createCharTable();
    }

    @AfterAll
    public static void dropTables() throws SQLException {
        Utils.dropTableIfExists(tableName, stmt);

        if (null != stmt) {
            stmt.close();
        }

        if (null != connection) {
            connection.close();
        }
    }

    private static void createCharTable() throws SQLException {
        stmt.execute("Create table " + tableName + " (c1 int)");
    }

    /**
     * Test regular simple query
     * 
     * @throws SQLException
     */
    @Test
    public void NormalTest() throws SQLException {
        testUpdateWithTwoParameters("update " + tableName + " set c1 = ? where c1 = ?");
        testInsertWithOneParameter("insert into " + tableName + " (c1) values (?)");
    }

    /**
     * Test query with new line character
     * 
     * @throws SQLException
     */
    @Test
    public void NewLineTest() throws SQLException {
        testQueriesWithWhiteSpaces("\n");
    }

    /**
     * Test query with tab character
     * 
     * @throws SQLException
     */
    @Test
    public void TabTest() throws SQLException {
        testQueriesWithWhiteSpaces("\t");
    }

    /**
     * Test query with form feed character
     * 
     * @throws SQLException
     */
    @Test
    public void FormFeedTest() throws SQLException {
        testQueriesWithWhiteSpaces("\f");
    }

    private void testQueriesWithWhiteSpaces(String whiteSpace) throws SQLException {
        testUpdateWithTwoParameters("update" + whiteSpace + tableName + " set c1 = ? where c1 = ?");
        testUpdateWithTwoParameters("update " + tableName + " set" + whiteSpace + "c1 = ? where c1 = ?");
        testUpdateWithTwoParameters("update " + tableName + " set c1 = ? where" + whiteSpace + "c1 = ?");

        testInsertWithOneParameter("insert into " + tableName + "(c1) values (?)"); // no space between table name and column name
        testInsertWithOneParameter("insert into" + whiteSpace + tableName + " (c1) values (?)");
    }

    private void testUpdateWithTwoParameters(String sql) throws SQLException {
        insertTestRow(1);
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, 2);
            ps.setInt(2, 1);
            ps.executeUpdate();
            assertTrue(isIdPresentInTable(2), "Expected ID is not present");
            assertEquals(2, ps.getParameterMetaData().getParameterCount(), "Parameter count mismatch");
        }
    }

    private void testInsertWithOneParameter(String sql) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setInt(1, 1);
            ps.executeUpdate();
            assertTrue(isIdPresentInTable(1), "Insert statement did not work");
            assertEquals(1, ps.getParameterMetaData().getParameterCount(), "Parameter count mismatch");
        }
    }

    private void insertTestRow(int id) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("insert into " + tableName + " (c1) values (?)")) {
            ps.setInt(1, id);
            ps.executeUpdate();
        }
    }

    private boolean isIdPresentInTable(int id) throws SQLException {
        try (PreparedStatement ps = connection.prepareStatement("select c1 from " + tableName + " where c1 = ?")) {
            ps.setInt(1, id);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }
}
