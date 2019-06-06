/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;


@RunWith(JUnitPlatform.class)
public class CertValidationTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Throwable {
        getFedauthInfo();
    }

    @Test
    public void testNotValidate() throws SQLException {
        notValidate("Notspecified", false, false);
        notValidate("Notspecified", false, true);
        notValidate("Notspecified", true, true);
        notValidate("SqlPassword", false, true);
        notValidate("SqlPassword", true, true);
        notValidate("ActiveDirectoryPassword", false, true);
        notValidate("ActiveDirectoryPassword", true, true);
        notValidate("ActiveDirectoryIntegrated", false, true);
        notValidate("ActiveDirectoryIntegrated", true, true);
    }

    public void notValidate(String authentication, boolean encrypt,
            boolean trustServerCertificate) throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            if (!authentication.equalsIgnoreCase("ActiveDirectoryIntegrated")) {
                ds.setUser(azureUserName);
                ds.setPassword("WrongPassword");
            }
            ds.setAuthentication(authentication);
            ds.setEncrypt(encrypt);
            ds.setTrustServerCertificate(trustServerCertificate);

            try (Connection connection = ds.getConnection()) {}
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (authentication.toLowerCase().contains("activedirectory")) {
                assertTrue(e.getMessage().contains(TestResource.getResource("R_loginFailed"))
                        || e.getMessage().contains("Failed to authenticate"));
            } else {
                assertTrue(e.getMessage().contains("Cannot open server"));
            }
        }
    }

    @Test
    public void testValidate() throws SQLException {
        validate("Notspecified", true, false);
        validate("sqlpassword", false, false);
        validate("sqlpassword", true, false);
        validate("ActiveDirectoryPassword", false, false);
        validate("ActiveDirectoryPassword", true, false);
        validate("ActiveDirectoryIntegrated", false, false);
        validate("ActiveDirectoryIntegrated", true, false);
    }

    public void validate(String authentication, boolean encrypt, boolean trustServerCertificate) throws SQLException {
        try {
            SQLServerDataSource ds = new SQLServerDataSource();

            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            if (!authentication.equalsIgnoreCase("ActiveDirectoryIntegrated")) {
                ds.setUser(azureUserName);
                ds.setPassword("WrongPassword");
            }
            ds.setAuthentication(authentication);
            ds.setEncrypt(encrypt);
            ds.setTrustServerCertificate(trustServerCertificate);

            try (Connection connection = ds.getConnection()) {}
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (authentication.toLowerCase().contains("activedirectory")) {
                assertTrue(e.getMessage().contains(TestResource.getResource("R_loginFailed"))
                        || e.getMessage().contains("Failed to authenticate"));
            } else {
                assertTrue(e.getMessage().contains("Cannot open server"));
            }
        }
    }
}
