/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.integrated;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.LogManager;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests for Active Directory Integrated authentication
 */
@Tag(Constants.aadIntegrated)
public class AADIntegratedTest extends AbstractTest {
    static String azureServer = null;
    static String azureDatabase = null;
    static String azureUserName = null;
    static String azurePassword = null;
    static String azureGroupUserName = null;

    @BeforeAll
    public static void getConfigs() throws Exception {
        azureServer = getConfiguredProperty("azureServer");
        azureDatabase = getConfiguredProperty("azureDatabase");
        azureUserName = getConfiguredProperty("azureUserName");
        azurePassword = getConfiguredProperty("azurePassword");
        azureGroupUserName = getConfiguredProperty("azureGroupUserName");

        // reset logging to avoid severe logs
        LogManager.getLogManager().reset();

        connectionString = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";Authentication="
                + "ActiveDirectoryIntegrated";

        connectionString = TestUtils.addOrOverrideProperty(connectionString, "trustServerCertificate", "true");
    }

    @Test
    public void testActiveDirectoryIntegrated() throws SQLException {

        java.util.Properties info = new Properties();
        info.put("TrustServerCertificate", "true");
        info.put("Authentication", "ActiveDirectoryIntegrated");

        try (Connection connection = DriverManager.getConnection(connectionString, info)) {} catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testActiveDirectoryIntegratedDS() throws Exception {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setServerName(azureServer);
        ds.setDatabaseName(azureDatabase);
        ds.setAuthentication("ActiveDirectoryIntegrated");

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT SUSER_SNAME()")) {
            rs.next();
            if (isWindows) {
                assertTrue(rs.getString(1).contains(System.getProperty("user.name")), rs.getString(1));
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testInvalidActiveDirectoryIntegrated() throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName("badServer");
            ds.setDatabaseName(azureDatabase);

            ds.setAuthentication("ActiveDirectoryIntegrated");
            ds.setEncrypt(encrypt);
            ds.setTrustServerCertificate(true);

            try (Connection conn = ds.getConnection()) {}
        } catch (Exception e) {
            org.junit.Assert.assertTrue(TestResource.getResource("R_invalidExceptionMessage") + ": " + e.getMessage(),
                    e.getMessage().contains(TestResource.getResource("R_loginFailed"))
                            || e.getMessage().contains(TestResource.getResource("R_failedToAuthenticate")));
        }
    }

    @Test
    public void testActiveDirectoryIntegratedUserName() throws Exception {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setServerName(azureServer);
        ds.setDatabaseName(azureDatabase);
        ds.setAuthentication("ActiveDirectoryIntegrated");

        try (Connection conn = ds.getConnection(); Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT SUSER_SNAME()")) {
            rs.next();
            if (isWindows) {
                assertTrue(rs.getString(1).contains(System.getProperty("user.name")), rs.getString(1));
            } else {
                // cannot verify user in kerberos tickets so just check it's not empty
                assertTrue(!rs.getString(1).isEmpty());
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
