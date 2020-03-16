/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.clientcertauth;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests client certificate authentication feature
 * The feature is only supported against SQL Server Linux CU2 or higher.
 * 
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xSQLv12)
@Tag(Constants.xSQLv14)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.xAzureSQLDB)
@Tag(Constants.clientCertAuth)
public class ClientCertificateAuthenticationTest extends AbstractTest {

    @Test
    public void pkcs1Test() throws Exception {
        String conStr = connectionString + ";clientCertificate=" + clientCertificate + ".pem;" + "clientKey="
                + clientKey + "1.key;" + "clientKeyPassword=" + clientKeyPassword + ";";
        try (Connection conn = DriverManager.getConnection(conStr); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT @@VERSION AS 'SQL Server Version'");
            rs.next();
            assertTrue(rs.getString(1).contains(TestResource.getResource("R_microsoft")));
        }
    }

    @Test
    public void pkcs8Test() throws Exception {
        String conStr = connectionString + ";clientCertificate=" + clientCertificate + ".pem;" + "clientKey="
                + clientKey + "8.key;" + "clientKeyPassword=" + clientKeyPassword + ";";
        try (Connection conn = DriverManager.getConnection(conStr); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT @@VERSION AS 'SQL Server Version'");
            rs.next();
            assertTrue(rs.getString(1).contains(TestResource.getResource("R_microsoft")));
        }
    }

    @Test
    public void pvkTest() throws Exception {
        String conStr = connectionString + ";clientCertificate=" + clientCertificate + ".cer;" + "clientKey="
                + clientKey + "pvk;" + "clientKeyPassword=" + clientKeyPassword + ";";
        try (Connection conn = DriverManager.getConnection(conStr); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT @@VERSION AS 'SQL Server Version'");
            rs.next();
            assertTrue(rs.getString(1).contains(TestResource.getResource("R_microsoft")));
        }
    }

    @Test
    public void pfxTest() throws Exception {
        String conStr = connectionString + ";clientCertificate=" + clientCertificate + ".pfx;" + "clientKeyPassword="
                + clientKeyPassword + ";";
        try (Connection conn = DriverManager.getConnection(conStr); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT @@VERSION AS 'SQL Server Version'");
            rs.next();
            assertTrue(rs.getString(1).contains(TestResource.getResource("R_microsoft")));
        }
    }

    @Test
    public void invalidCert() throws Exception {
        String conStr = connectionString + ";clientCertificate=invalid_path;" + "clientKeyPassword=" + clientKeyPassword
                + ";";
        try (Connection conn = DriverManager.getConnection(conStr); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT @@VERSION AS 'SQL Server Version'");
            rs.next();
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().contains(TestResource.getResource("R_invalidPath")));
        }
    }

    @Test
    public void invalidCertPassword() throws Exception {
        String conStr = connectionString + ";clientCertificate=" + clientCertificate + ".pfx;"
                + "clientKeyPassword=invalid_password;";
        try (Connection conn = DriverManager.getConnection(conStr); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt.executeQuery("SELECT @@VERSION AS 'SQL Server Version'");
            rs.next();
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().contains(TestResource.getResource("R_keystorePassword")));
        }
    }
}
