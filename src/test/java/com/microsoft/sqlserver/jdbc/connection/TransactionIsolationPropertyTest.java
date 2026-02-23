/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.testframework.AbstractTest;

@RunWith(JUnitPlatform.class)
public class TransactionIsolationPropertyTest extends AbstractTest {

    @Test
    public void testPropertyInfo() throws SQLException {
        SQLServerDriver driver = new SQLServerDriver();
        Properties info = new Properties();
        String url = "jdbc:sqlserver://localhost;transactionIsolation=1";

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, info);
        assertNotNull(propertyInfo);

        boolean found = false;
        for (DriverPropertyInfo prop : propertyInfo) {
            if ("transactionIsolation".equalsIgnoreCase(prop.name)) {
                assertEquals("1", prop.value);
                found = true;
                break;
            }
        }
        assertTrue(found, "Property 'transactionIsolation' not found in DriverPropertyInfo");
    }

    @Test
    public void testInvalidProperty() {
        Properties info = new Properties();
        String url = "jdbc:sqlserver://localhost;transactionIsolation=invalid";

        SQLException exception = assertThrows(SQLException.class, () -> {
            new SQLServerDriver().connect(url, info);
        });

        assertTrue(exception.getMessage().contains("The transactionIsolation value \"invalid\" is not valid"),
                "Exception message should contain the invalid value. Actual message: " + exception.getMessage());
    }

    @Test
    public void testOutOfRangeProperty() {
        Properties info = new Properties();
        String url = "jdbc:sqlserver://localhost;transactionIsolation=100";

        SQLException exception = assertThrows(SQLException.class, () -> {
            new SQLServerDriver().connect(url, info);
        });

        assertTrue(exception.getMessage().contains("The transactionIsolation value \"100\" is not valid"),
                "Exception message should contain the invalid value. Actual message: " + exception.getMessage());
    }

    @Test
    public void testUserSpecificUrl() throws SQLException {
        SQLServerDriver driver = new SQLServerDriver();
        Properties info = new Properties();

        // Use a generic placeholder since this test only validates URL parsing via
        // getPropertyInfo,
        // which does not require a real server.
        String url = "jdbc:sqlserver://generic-server;databaseName=testdb;transactionIsolation=1;";

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, info);
        assertNotNull(propertyInfo);

        boolean found = false;
        for (DriverPropertyInfo prop : propertyInfo) {
            if ("transactionIsolation".equalsIgnoreCase(prop.name)) {
                assertEquals("1", prop.value, "Transaction isolation should be 1");
                found = true;
                break;
            }
        }
        assertTrue(found, "Property 'transactionIsolation' not found in DriverPropertyInfo for user URL");
    }

    @Test
    public void testDataSourceProperty() {
        com.microsoft.sqlserver.jdbc.SQLServerDataSource ds = new com.microsoft.sqlserver.jdbc.SQLServerDataSource();
        ds.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED);
        assertEquals(java.sql.Connection.TRANSACTION_READ_UNCOMMITTED, ds.getTransactionIsolation());
    }

    @Test
    public void testDataSourceDefaultProperty() {
        com.microsoft.sqlserver.jdbc.SQLServerDataSource ds = new com.microsoft.sqlserver.jdbc.SQLServerDataSource();
        assertEquals(java.sql.Connection.TRANSACTION_READ_COMMITTED, ds.getTransactionIsolation());
    }
}
