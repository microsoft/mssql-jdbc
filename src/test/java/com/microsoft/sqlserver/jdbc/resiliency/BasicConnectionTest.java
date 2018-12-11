/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.resiliency;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;


public class BasicConnectionTest extends AbstractTest {

    @Test
    public void testBasicReconnectDefault() throws SQLException {
        basicReconnect(connectionString);
    }

    @Test
    public void testBasicEncryptedConnection() throws SQLException {
        basicReconnect(connectionString + ";encrypt=true;trustServerCertificate=true;");
    }

    @Test
    public void testGracefulClose() throws SQLException {
        try (Connection c = DriverManager.getConnection(connectionString)) {
            try (Statement s = c.createStatement()) {
                ResiliencyUtils.killConnection(c, connectionString);
                c.close();
                s.executeQuery("SELECT 1");
                fail("Query execution did not throw an exception on a closed execution");
            } catch (SQLException e) {
                assertTrue(e.getMessage().contains("The connection is closed."));
            }
        }
    }
    
    @Test
    public void testSetAttributes() throws SQLException {
        try (Connection c = DriverManager.getConnection(connectionString)) {
            ResiliencyUtils.toggleRandomProperties(c);
            Map<String,String> expected = ResiliencyUtils.getUserOptions(c);
            ResiliencyUtils.killConnection(c, connectionString);
            Map<String,String> recieved = ResiliencyUtils.getUserOptions(c);
            assertTrue("User options do not match", expected.equals(recieved));
        }
    }

    @Test
    public void testCatalog() throws SQLException {
        String expectedDatabaseName = null;
        String actualDatabaseName = null;
        try (Connection c = DriverManager.getConnection(connectionString); Statement s = c.createStatement()) {
            try {
                expectedDatabaseName = RandomUtil.getIdentifier("resDB");
                TestUtils.dropDatabaseIfExists(expectedDatabaseName, s);
                s.execute("CREATE DATABASE [" + expectedDatabaseName + "]");
                try {
                    c.setCatalog(expectedDatabaseName);
                } catch (SQLException e) {
                    // Switching databases is not supported against Azure, skip/
                    return;
                }
                ResiliencyUtils.killConnection(c, connectionString);
                try (ResultSet rs = s.executeQuery("SELECT db_name();")) {
                    while (rs.next()) {
                        actualDatabaseName = rs.getString(1);
                    }
                }
                // Check if the driver reconnected to the expected database. 
                assertEquals(expectedDatabaseName, actualDatabaseName);
            }
            finally {
                TestUtils.dropDatabaseIfExists(expectedDatabaseName, s);
            }
        }
    }

    private void basicReconnect(String connectionString) throws SQLException {
        try (Connection c = DriverManager.getConnection(connectionString)) {
            try (Statement s = c.createStatement()) {
                ResiliencyUtils.killConnection(c, connectionString);
                s.executeQuery("SELECT 1");
            }
        }
    }
}
