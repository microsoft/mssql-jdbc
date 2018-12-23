/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerConnection43;
import com.microsoft.sqlserver.testframework.AbstractTest;


/**
 * Test ConnectionWrapper43Test class
 *
 */
@RunWith(JUnitPlatform.class)
@Tag("AzureDWTest")
public class ConnectionWrapper43Test extends AbstractTest {
    static Connection connection = null;
    double javaVersion = Double.parseDouble(System.getProperty("java.specification.version"));
    static int major;
    static int minor;

    /**
     * Tests creation of SQLServerConnection43Test object
     * 
     * @throws SQLException
     */
    @Test
    public void SQLServerConnection43Test() throws SQLException {
        try {
            if (1.8d <= javaVersion && 4 == major && 2 == minor) {
                assertTrue(connection instanceof SQLServerConnection);
            } else {
                assertTrue(connection instanceof SQLServerConnection43);
            }
        } finally {
            if (null != connection) {
                connection.close();
            }
        }
    }

    @BeforeAll
    public static void setupConnection() throws SQLException {
        connection = DriverManager.getConnection(connectionString);

        DatabaseMetaData metadata = connection.getMetaData();
        major = metadata.getJDBCMajorVersion();
        minor = metadata.getJDBCMinorVersion();
    }

    @AfterAll
    public static void terminateVariation() throws SQLException {
        if (null != connection) {
            connection.close();
        }
    }

}
