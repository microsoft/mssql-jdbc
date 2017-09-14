/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * Parts Copyright(c) Goldman Sachs All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import java.sql.*;
import java.util.Calendar;
import java.util.TimeZone;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests with sql queries using preparedStatement with parameters
 *
 *
 */
@RunWith(JUnitPlatform.class)
public class SelectWithParameterTest extends AbstractTest {
    static Connection con = null;

    /**
     * Setup before test
     * 
     * @throws SQLException
     */
    @BeforeAll
    public static void setupTest() throws SQLException {
        con = DriverManager.getConnection(connectionString);
        Statement stmt = con.createStatement();
        Utils.dropTableIfExists("paramtest", stmt);
        if (null != stmt) {
            stmt.close();
        }
    }

    /**
     * Tests setTimestamp for datetime and datetime2 columns
     * 
     * @throws SQLException
     */
    @Test
    public void testTimestamp() throws SQLException {
        executeStmt("create table paramtest (col1 datetime, col2 datetime2(3))");
        Timestamp expected = new Timestamp(1167613261999L);  // 1167613261999L is 2007-01-01 01:01:01.999
        // this ensure this test can run in all timezones.
        Calendar c = Calendar.getInstance();
        c.setTimeZone(TimeZone.getTimeZone("UTC"));

        try (PreparedStatement insertStatement = con.prepareStatement("insert into paramtest (col1,col2) values (?,?)")) {
            insertStatement.setTimestamp(1, expected, c);
            insertStatement.setTimestamp(2, expected, c);
            assertEquals(1, insertStatement.executeUpdate());
        }

        try (PreparedStatement psLegacy = con.prepareStatement("select t0.col1, t0.col2 from paramtest t0 where col1 = ?")) {
            psLegacy.setTimestamp(1, expected, c);
            try (ResultSet resultSet = psLegacy.executeQuery()) {
                assertTrue(resultSet.next());
                assertEquals(expected.getTime() + 1, resultSet.getTimestamp(1, c).getTime()); // rounded at the server
                assertEquals(expected, resultSet.getTimestamp(2, c));
            }
        }

        try (PreparedStatement psNew = con.prepareStatement("select t0.col1, t0.col2 from paramtest t0 where col2 = ?")) {
            psNew.setTimestamp(1, expected, c);
            try (ResultSet resultSet = psNew.executeQuery();) {
                assertTrue(resultSet.next());
                assertEquals(expected.getTime() + 1, resultSet.getTimestamp(1, c).getTime()); // rounded at the server
                assertEquals(expected, resultSet.getTimestamp(2, c));
            }
        }
    }

    private void executeStmt(String sql) throws SQLException {
        try (Statement statement = con.createStatement()) {
            statement.execute(sql);
        }
    }

    /**
     * Cleanup after test
     * 
     * @throws SQLException
     */
    @AfterAll
    public static void cleanup() throws SQLException {
        Statement stmt = con.createStatement();
        Utils.dropTableIfExists("paramtest", stmt);
        if (null != stmt) {
            stmt.close();
        }
        if (null != con) {
            con.close();
        }
    }
}
