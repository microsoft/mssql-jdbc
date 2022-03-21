/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;

import com.microsoft.sqlserver.jdbc.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;


/*
 * This test is for testing various connection options
 */
@RunWith(JUnitPlatform.class)
public class ConnectionTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
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
        try (Connection con = ds.getConnection()) {}

        try (Connection con = ds.getConnection()) {}
    }

    @Test
    public void testConnectWithIPAddressPreference () throws SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        ds.setIPAddressPreference("IPv4First");
        try (Connection con = ds.getConnection()) {
        }
        ds.setIPAddressPreference("IPv6First");
        try (Connection con = ds.getConnection()) {
        }
        ds.setIPAddressPreference("UsePlatformDefault");
        try (Connection con = ds.getConnection()) {
        }
        catch (Exception e){
            fail(e.getMessage());
        }
        ds.setIPAddressPreference("Bogus");
        try (Connection con = ds.getConnection()) {
            fail(TestResource.getResource("R_expectedFailPassed"));
        }
        catch (Exception e){
            MessageFormat form = new MessageFormat(TestResource.getResource("R_InvalidIPAddressPreference"));
            Object[] msgArgs1 = {"Bogus"};
            assertTrue (e.getMessage().equals(form.format(msgArgs1)), e.getMessage());
        }
    }
}
