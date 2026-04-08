/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

@RunWith(JUnitPlatform.class)
public class TransactionIsolationPropertyTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /**
     * Finds a {@link DriverPropertyInfo} by name (case-insensitive) from the given array.
     * Fails the test if the property is not found.
     */
    private static DriverPropertyInfo findProperty(DriverPropertyInfo[] props, String name) {
        for (DriverPropertyInfo prop : props) {
            if (name.equalsIgnoreCase(prop.name)) {
                return prop;
            }
        }
        fail("Property '" + name + "' not found in DriverPropertyInfo");
        return null;
    }

    @Test
    public void testPropertyInfo() throws SQLException {
        SQLServerDriver driver = new SQLServerDriver();
        Properties info = new Properties();
        String url = connectionString + ";defaultTransactionIsolation=READ_COMMITTED";

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, info);
        assertNotNull(propertyInfo);

        DriverPropertyInfo prop = findProperty(propertyInfo, "defaultTransactionIsolation");
        assertEquals("READ_COMMITTED", prop.value);
    }

    @Test
    public void testInvalidProperty() {
        String url = connectionString + ";defaultTransactionIsolation=invalid";

        SQLException exception = assertThrows(SQLException.class, () -> {
            PrepUtil.getConnection(url);
        });

        assertTrue(exception.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidConnectionSetting")), 
                "Exception message should match R_InvalidConnectionSetting. Actual message: " + exception.getMessage());
        assertTrue(exception.getMessage().contains("defaultTransactionIsolation"), "Message should contain property name 'defaultTransactionIsolation'");
        assertTrue(exception.getMessage().contains("invalid"), "Message should contain the invalid value 'invalid'");
    }

    @Test
    public void testOutOfRangeProperty() {
        String url = connectionString + ";defaultTransactionIsolation=NUMERIC_VALUE_UNSUPPORTED";

        SQLException exception = assertThrows(SQLException.class, () -> {
            PrepUtil.getConnection(url);
        });

        assertTrue(exception.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidConnectionSetting")),
                "Exception message should match R_InvalidConnectionSetting. Actual message: " + exception.getMessage());
        assertTrue(exception.getMessage().contains("defaultTransactionIsolation"), "Message should contain property name 'defaultTransactionIsolation'");
        assertTrue(exception.getMessage().contains("NUMERIC_VALUE_UNSUPPORTED"), "Message should contain the invalid value 'NUMERIC_VALUE_UNSUPPORTED'");
    }

    @Test
    public void testSnapshotPropertyInfo() throws SQLException {
        SQLServerDriver driver = new SQLServerDriver();
        Properties info = new Properties();
        String url = connectionString + ";defaultTransactionIsolation=SNAPSHOT";

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, info);
        assertNotNull(propertyInfo);

        DriverPropertyInfo prop = findProperty(propertyInfo, "defaultTransactionIsolation");
        assertEquals("SNAPSHOT", prop.value, "Transaction isolation should be SNAPSHOT");
    }

    @Test
    public void testDataSourceProperty() {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setDefaultTransactionIsolation("READ_UNCOMMITTED");
        assertEquals("READ_UNCOMMITTED", ds.getDefaultTransactionIsolation());
    }

    @Test
    public void testDataSourceDefaultProperty() {
        SQLServerDataSource ds = new SQLServerDataSource();
        // By default, the property is null because no default value is defined in the property metadata.
        // It falls back to driver defaults during connection establishment if not specified.
        assertNull(ds.getDefaultTransactionIsolation());
    }

    @Test
    public void testEdgeCasePropertyValues() throws Exception {
        // Empty value — ignored, falls back to default.
        String emptyUrl = connectionString + ";defaultTransactionIsolation=";
        try (Connection con = PrepUtil.getConnection(emptyUrl)) {
            assertEquals(Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation(),
                    "Empty property value should fall back to default isolation level.");
        }

        // Whitespace — trimmed to empty, also falls back to default.
        String whitespaceUrl = connectionString + ";defaultTransactionIsolation= ";
        try (Connection con = PrepUtil.getConnection(whitespaceUrl)) {
            assertEquals(Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation(),
                    "Whitespace-only property value should fall back to default isolation level.");
        }
    }

    @Test
    public void testCaseInsensitiveProperty() throws SQLException {
        SQLServerDriver driver = new SQLServerDriver();
        Properties info = new Properties();
        String url = connectionString + ";defaultTransactionIsolation=read_Committed";

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, info);
        assertNotNull(propertyInfo);

        DriverPropertyInfo prop = findProperty(propertyInfo, "defaultTransactionIsolation");
        assertEquals("READ_COMMITTED", prop.value, "Transaction isolation should be normalized to READ_COMMITTED");
    }

    /**
     * Integration test to verify that the defaultTransactionIsolation property is correctly applied to a real connection.
     * We iterate through all supported levels and verify con.getTransactionIsolation() matches.
     */
    @Test
    public void testTransactionIsolationApplied() throws Exception {
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
        String url = connectionString + ";defaultTransactionIsolation=reAd_comMitted";
        try (Connection con = PrepUtil.getConnection(url)) {
            assertEquals(Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation(),
                    "Mixed-case isolation level was not applied correctly.");
        }
    }

    /**
     * Verifies that the runtime setTransactionIsolation() overrides the level set by the
     * defaultTransactionIsolation connection property, and vice versa.
     */
    @Test
    public void testRuntimeOverridesConnectionProperty() throws Exception {
        // Connect with SERIALIZABLE, then override to READ_UNCOMMITTED at runtime.
        String url = TestUtils.addOrOverrideProperty(connectionString,
                "defaultTransactionIsolation", "SERIALIZABLE");
        try (Connection con = PrepUtil.getConnection(url)) {
            assertEquals(Connection.TRANSACTION_SERIALIZABLE, con.getTransactionIsolation(),
                    "Connection should start with SERIALIZABLE from the connection property.");

            con.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, con.getTransactionIsolation(),
                    "Runtime setTransactionIsolation should override the connection property level.");
        }

        // Connect with READ_UNCOMMITTED, then override to SERIALIZABLE at runtime.
        url = TestUtils.addOrOverrideProperty(connectionString,
                "defaultTransactionIsolation", "READ_UNCOMMITTED");
        try (Connection con = PrepUtil.getConnection(url)) {
            assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, con.getTransactionIsolation(),
                    "Connection should start with READ_UNCOMMITTED from the connection property.");

            con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            assertEquals(Connection.TRANSACTION_SERIALIZABLE, con.getTransactionIsolation(),
                    "Runtime setTransactionIsolation should override the connection property level.");
        }
    }
}
