/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.Fedauth)
public class ConnectionEncryptionTest extends FedauthCommon {

    static String charTable = TestUtils.escapeSingleQuotes(
            AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("JDBC_ConnectionEncryption")));

    @BeforeAll
    public static void setupTests() throws Throwable {
        FedauthCommon.getFedauthInfo();
    }

    @Test
    public void testCorrectCertificate() throws SQLException {
        String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "userName="
                + azureUserName + ";password=" + azurePassword + ";" + "Authentication=ActiveDirectoryPassword;"
                + "HostNameInCertificate=" + hostNameInCertificate;

        try (Connection connection = DriverManager.getConnection(connectionUrl);
                Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT SUSER_SNAME()")) {
            rs.next();

            String retrievedUserName = rs.getString(1);
            assertTrue(retrievedUserName.equals(azureUserName));
            try {
                TestUtils.dropTableIfExists(charTable, stmt);
                FedauthTest.createTable(stmt, charTable);
                FedauthTest.populateCharTable(connection, charTable);
                FedauthTest.testChar(stmt, charTable);
            } finally {
                TestUtils.dropTableIfExists(charTable, stmt);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testWrongCertificate() throws SQLException {
        String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "userName="
                + azureUserName + ";password=" + azurePassword + ";" + "Authentication=ActiveDirectoryPassword;"
                + "HostNameInCertificate=WrongCertificate";

        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Exception e) {
            if (!(e instanceof SQLServerException)) {
                fail(TestResource.getResource("R_expectedExceptionNotThrown"));
            }

            assertTrue(TestResource.getResource("R_invalidExceptionMessage") + ": " + e.getMessage(),
                    e.getMessage().startsWith(
                            "The driver could not establish a secure connection to SQL Server by using Secure Sockets Layer (SSL) encryption."));
        }
    }

    // set TrustServerCertificate to true, which skips server certificate validation.
    @Test
    public void testWrongCertificateButTrustServerCertificate() throws SQLException {
        String connectionUrl = "jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";" + "userName="
                + azureUserName + ";password=" + azurePassword + ";" + "Authentication=ActiveDirectoryPassword;"
                + "HostNameInCertificate=WrongCertificate;" + "TrustServerCertificate=true";

        try (Connection connection = DriverManager.getConnection(connectionUrl);
                Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT SUSER_SNAME()")) {
            rs.next();

            String retrievedUserName = rs.getString(1);
            assertTrue(retrievedUserName.equals(azureUserName));
            try {
                TestUtils.dropTableIfExists(charTable, stmt);
                FedauthTest.createTable(stmt, charTable);
                FedauthTest.populateCharTable(connection, charTable);
                FedauthTest.testChar(stmt, charTable);
            } finally {
                TestUtils.dropTableIfExists(charTable, stmt);
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @AfterAll
    public static void terminate() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(charTable, stmt);
        }
    }
}
