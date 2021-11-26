/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import java.sql.Connection;
import java.sql.SQLException;

import com.microsoft.sqlserver.jdbc.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.testframework.AbstractTest;

/*
 * This test is for testing various connection options
 */
@RunWith(JUnitPlatform.class)
public class ConnectionTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    @Test
    public void testConnections() throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        ds.setKeyStoreAuthentication("KeyVaultClientSecret");
        ds.setKeyStorePrincipalId("placeholder");
        ds.setKeyStoreSecret("placeholder");

        // Multiple, successive connections should not fail
        try (Connection con = ds.getConnection()) {
        }

        try (Connection con = ds.getConnection()) {
        }
    }
}
