/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

@RunWith(JUnitPlatform.class)
public class TransactionIsolationPropertyTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        // No additional setup required as AbstractTest handles base configuration.
    }

    @Test
    public void testPropertyInfo() throws SQLException {
        SQLServerDriver driver = new SQLServerDriver();
        Properties info = new Properties();
        // Following PR review feedback: no need to include explicit server URLs in these tests as the test framework
        // handles the base connection string for integration tests. For metadata and parsing tests, 
        // using the minimal Constants.JDBC_PREFIX is sufficient and ensures consistency.
        String url = TestUtils.addOrOverrideProperty(Constants.JDBC_PREFIX,
                "defaultTransactionIsolation", "READ_COMMITTED");

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, info);
        assertNotNull(propertyInfo);

        boolean found = false;
        for (DriverPropertyInfo prop : propertyInfo) {
            if ("defaultTransactionIsolation".equalsIgnoreCase(prop.name)) {
                assertEquals("READ_COMMITTED", prop.value);
                found = true;
                break;
            }
        }
        assertTrue(found, "Property 'defaultTransactionIsolation' not found in DriverPropertyInfo");
    }

    @Test
    public void testInvalidProperty() {
        Properties info = new Properties();
        // Using minimal URL as we are testing property parsing and validation logic.
        String url = TestUtils.addOrOverrideProperty(Constants.JDBC_PREFIX,
                "defaultTransactionIsolation", "invalid");

        SQLException exception = assertThrows(SQLException.class, () -> {
            new SQLServerDriver().connect(url, info);
        });

        assertTrue(exception.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidConnectionSetting")), 
                "Exception message should match R_InvalidConnectionSetting. Actual message: " + exception.getMessage());
        assertTrue(exception.getMessage().contains("defaultTransactionIsolation"), "Message should contain property name 'defaultTransactionIsolation'");
        assertTrue(exception.getMessage().contains("invalid"), "Message should contain the invalid value 'invalid'");
    }

    @Test
    public void testOutOfRangeProperty() {
        Properties info = new Properties();
        // Using minimal URL as we are testing property parsing and validation logic.
        String url = TestUtils.addOrOverrideProperty(Constants.JDBC_PREFIX,
                "defaultTransactionIsolation", "NUMERIC_VALUE_UNSUPPORTED");

        SQLException exception = assertThrows(SQLException.class, () -> {
            new SQLServerDriver().connect(url, info);
        });

        assertTrue(exception.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidConnectionSetting")),
                "Exception message should match R_InvalidConnectionSetting. Actual message: " + exception.getMessage());
        assertTrue(exception.getMessage().contains("defaultTransactionIsolation"), "Message should contain property name 'defaultTransactionIsolation'");
        assertTrue(exception.getMessage().contains("NUMERIC_VALUE_UNSUPPORTED"), "Message should contain the invalid value 'NUMERIC_VALUE_UNSUPPORTED'");
    }

    @Test
    public void testUserSpecificUrl() throws SQLException {
        SQLServerDriver driver = new SQLServerDriver();
        Properties info = new Properties();

        // Testing the parsing of multiple properties from a minimal URL.
        String url = TestUtils.addOrOverrideProperty(Constants.JDBC_PREFIX,
                "defaultTransactionIsolation", "SNAPSHOT");
        url = TestUtils.addOrOverrideProperty(url, "databaseName", "testdb");

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, info);
        assertNotNull(propertyInfo);

        boolean found = false;
        for (DriverPropertyInfo prop : propertyInfo) {
            if ("defaultTransactionIsolation".equalsIgnoreCase(prop.name)) {
                assertEquals("SNAPSHOT", prop.value, "Transaction isolation should be SNAPSHOT");
                found = true;
                break;
            }
        }
        assertTrue(found, "Property 'defaultTransactionIsolation' not found in DriverPropertyInfo for user URL");
    }

    @Test
    public void testDataSourceProperty() {
        com.microsoft.sqlserver.jdbc.SQLServerDataSource ds = new com.microsoft.sqlserver.jdbc.SQLServerDataSource();
        ds.setDefaultTransactionIsolation("READ_UNCOMMITTED");
        assertEquals("READ_UNCOMMITTED", ds.getDefaultTransactionIsolation());
    }

    @Test
    public void testDataSourceDefaultProperty() {
        com.microsoft.sqlserver.jdbc.SQLServerDataSource ds = new com.microsoft.sqlserver.jdbc.SQLServerDataSource();
        // By default, the property is null because no default value is defined in the property metadata.
        // It falls back to driver defaults during connection establishment if not specified.
        assertTrue(ds.getDefaultTransactionIsolation() == null);
    }

    @Test
    public void testCaseInsensitiveProperty() throws SQLException {
        SQLServerDriver driver = new SQLServerDriver();
        Properties info = new Properties();
        // Testing case-insensitivity using the minimal JDBC prefix.
        String url = TestUtils.addOrOverrideProperty(Constants.JDBC_PREFIX,
                "defaultTransactionIsolation", "read_Committed");

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, info);
        assertNotNull(propertyInfo);

        boolean found = false;
        for (DriverPropertyInfo prop : propertyInfo) {
            if ("defaultTransactionIsolation".equalsIgnoreCase(prop.name)) {
                // If the driver normalizes during parsing, it should be READ_COMMITTED.
                // Based on standard driver implementation, it should at least match the choices exactly.
                assertEquals("READ_COMMITTED", prop.value, "Transaction isolation should be normalized to READ_COMMITTED");
                found = true;
                break;
            }
        }
        assertTrue(found, "Property 'defaultTransactionIsolation' not found");
    }

    /**
     * Integration test to verify that the defaultTransactionIsolation property is correctly applied to a real connection.
     * We iterate through all supported levels and verify con.getTransactionIsolation() matches.
     */
    @Test
    public void testTransactionIsolationApplied() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(connectionString != null && connectionString.startsWith(Constants.JDBC_PREFIX),
                "Connection string is not set.");

        String[] levels = {"READ_UNCOMMITTED", "READ_COMMITTED", "REPEATABLE_READ", "SERIALIZABLE", "SNAPSHOT"};
        int[] expectedConstants = {
            Connection.TRANSACTION_READ_UNCOMMITTED,
            Connection.TRANSACTION_READ_COMMITTED,
            Connection.TRANSACTION_REPEATABLE_READ,
            Connection.TRANSACTION_SERIALIZABLE,
            ISQLServerConnection.TRANSACTION_SNAPSHOT
        };

        for (int i = 0; i < levels.length; i++) {
            String url = TestUtils.addOrOverrideProperty(connectionString, "defaultTransactionIsolation", levels[i]);
            try (Connection con = PrepUtil.getConnection(url)) {
                assertEquals(expectedConstants[i], con.getTransactionIsolation(),
                        "Isolation level " + levels[i] + " was not applied correctly.");
            }
        }
    }

    /**
     * Integration test to verify case-insensitivity when applying to a real connection.
     */
    @Test
    public void testMixedCaseTransactionIsolationApplied() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(connectionString != null && connectionString.startsWith(Constants.JDBC_PREFIX),
                "Connection string is not set.");

        String url = TestUtils.addOrOverrideProperty(connectionString, "defaultTransactionIsolation", "reAd_comMitted");
        try (Connection con = PrepUtil.getConnection(url)) {
            assertEquals(Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation(),
                    "Mixed-case isolation level was not applied correctly.");
        }
    }
}
