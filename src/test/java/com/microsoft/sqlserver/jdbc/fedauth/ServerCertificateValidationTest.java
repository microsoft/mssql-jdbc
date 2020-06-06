/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.Constants;

@RunWith(JUnitPlatform.class)
@Tag(Constants.Fedauth)
public class ServerCertificateValidationTest extends FedauthCommon {

    @Test
    public void testNotValidNotSpecifiedNotEncryptedNotTrustServerCert() throws SQLException {
        testNotValid("Notspecified", false, false);
    }

    @Test
    public void testNotValidNotSpecifiedNotEncryptedTrustServerCert() throws SQLException {
        testNotValid("Notspecified", false, true);
    }

    @Test
    public void testNotValidNotSpecifiedEncryptedTrustServerCert() throws SQLException {
        testNotValid("Notspecified", true, true);
    }

    @Test
    public void testNotValidSqlpasswordNotEncrytedTrustServerCert() throws SQLException {
        testNotValid("sqlpassword", false, true);
    }

    @Test
    public void testNotValidSqlpasswordEncrypedTrustServerCert() throws SQLException {
        testNotValid("sqlpassword", true, true);
    }

    @Test
    public void testNotValidADPasswordNotEncrypedTrustServerCert() throws SQLException {
        testNotValid("ActiveDirectoryPassword", false, true);
    }

    @Test
    public void testNotValidADPasswordEncrypedtrustServerCert() throws SQLException {
        testNotValid("ActiveDirectoryPassword", true, true);
    }

    @Test
    public void testNotValidADIntegratedNotEncrypedTrustServerCert() throws SQLException {
        testNotValid("ActiveDirectoryIntegrated", false, true);
    }

    @Test
    public void testNotValidADIntegratedEncrypedrustServerCert() throws SQLException {
        testNotValid("ActiveDirectoryIntegrated", true, true);
    }

    @Test
    public void testValidNotSpecified() throws SQLException {
        testValid("Notspecified", true, false);
    }

    @Test
    public void testValidSqlpasswordNotEncrypted() throws SQLException {
        testValid("sqlpassword", false, false);
    }

    @Test
    public void testValidSqlpasswordEncrypted() throws SQLException {
        testValid("sqlpassword", true, false);
    }

    @Test
    public void testValidADPasswordNotEncryptedNotTrustServerCert() throws SQLException {
        testValid("ActiveDirectoryPassword", false, false);
    }

    @Test
    public void testValidADPasswordEncryptedNotTrustServerCert() throws SQLException {
        testValid("ActiveDirectoryPassword", true, false);
    }

    @Test
    public void testValidADInegratedNotEncryptedNotTrustServerCert() throws SQLException {
        testValid("ActiveDirectoryIntegrated", false, false);
    }

    @Test
    public void testValidADIntegratedEncryptedNotTrustServerCert() throws SQLException {
        testValid("ActiveDirectoryIntegrated", true, false);
    }

    private void testValid(String authentication, boolean encrypt, boolean trustServerCertificate) throws SQLException {
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
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {}
    }

    private void testNotValid(String authentication, boolean encrypt,
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
            fail(TestResource.getResource("R_expectedFailPassed"));
        } catch (Exception e) {
            if (authentication.toLowerCase().contains("activedirectory")) {
                assertTrue(
                        e.getMessage().contains("Login failed for")
                                || e.getMessage().contains("Failed to authenticate"),
                        "Invalid exception message: \n" + e.getMessage());
            } else {
                assertTrue(e.getMessage().contains("Cannot open server"),
                        "Invalid exception message: \n" + e.getMessage());
            }
        }
    }
}
