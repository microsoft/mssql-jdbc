/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement42;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement42;
import com.microsoft.sqlserver.testframework.AbstractTest;

/**
 * Test SQLServerPreparedSatement42 and SQLServerCallableSatement42 classes
 *
 */
@RunWith(JUnitPlatform.class)
public class Wrapper42Test extends AbstractTest {
    static Connection connection = null;
    double javaVersion = Double.parseDouble(System.getProperty("java.specification.version"));
    static int major;
    static int minor;

    /**
     * Tests creation of SQLServerPreparedSatement42 object
     * 
     * @throws SQLException
     */
    @Test
    public void PreparedSatement42Test() throws SQLException {
        String sql = "SELECT SUSER_SNAME()";

        PreparedStatement pstmt = connection.prepareStatement(sql);

        if (1.8d <= javaVersion && 4 == major && 2 == minor) {
            assertTrue(pstmt instanceof SQLServerPreparedStatement42);
        }
        else {
            assertTrue(pstmt instanceof SQLServerPreparedStatement);
        }
    }

    /**
     * Tests creation of SQLServerCallableStatement42 object
     * 
     * @throws SQLException
     */
    @Test
    public void CallableStatement42Test() throws SQLException {
        String sql = "SELECT SUSER_SNAME()";

        CallableStatement cstmt = connection.prepareCall(sql);

        if (1.8d <= javaVersion && 4 == major && 2 == minor) {
            assertTrue(cstmt instanceof SQLServerCallableStatement42);
        }
        else {
            assertTrue(cstmt instanceof SQLServerCallableStatement);
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
