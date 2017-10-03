/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.resultset;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet42;
import com.microsoft.sqlserver.testframework.AbstractTest;

/**
 * Test SQLServerResultSet42 class
 *
 */
@RunWith(JUnitPlatform.class)
public class ResultSetWrapper42Test extends AbstractTest {
    static Connection connection = null;
    double javaVersion = Double.parseDouble(System.getProperty("java.specification.version"));
    static int major;
    static int minor;

    /**
     * Tests creation of SQLServerResultSet42 object
     * 
     * @throws SQLException
     */
    @Test
    public void SQLServerResultSet42Test() throws SQLException {
        String sql = "SELECT SUSER_SNAME()";

        ResultSet rs = null;
        try {
            rs = connection.createStatement().executeQuery(sql);

            if (1.8d <= javaVersion && 4 == major && 2 == minor) {
                assertTrue(rs instanceof SQLServerResultSet42);
            }
            else {
                assertTrue(rs instanceof SQLServerResultSet);
            }
        }
        finally {
            if (null != rs) {
                rs.close();
            }
        }
    }

    @BeforeAll
    private static void setupConnection() throws SQLException {
        connection = DriverManager.getConnection(connectionString);

        DatabaseMetaData metadata = connection.getMetaData();
        major = metadata.getJDBCMajorVersion();
        minor = metadata.getJDBCMinorVersion();
    }

    @AfterAll
    private static void terminateVariation() throws SQLException {
        if (null != connection) {
            connection.close();
        }
    }

}
