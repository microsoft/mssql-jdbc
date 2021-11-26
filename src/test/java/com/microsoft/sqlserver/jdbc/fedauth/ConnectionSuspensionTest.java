/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.AfterAll;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;

import static org.junit.Assert.assertTrue;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;


@RunWith(JUnitPlatform.class)
@Tag("slow")
@Tag(Constants.fedAuth)
public class ConnectionSuspensionTest extends FedauthCommon {

    static String charTable = TestUtils.escapeSingleQuotes(
            AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("JDBC_ConnectionSuspension")));

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    @Test
    public void testAccessTokenExpiredThenCreateNewStatementADPassword() throws SQLException {
        testAccessTokenExpiredThenCreateNewStatement(SqlAuthentication.ActiveDirectoryPassword);
    }

    @Test
    public void testAccessTokenExpiredThenCreateNewStatementADIntegrated() throws SQLException {
        org.junit.Assume.assumeTrue(enableADIntegrated);

        testAccessTokenExpiredThenCreateNewStatement(SqlAuthentication.ActiveDirectoryIntegrated);
    }

    private void testAccessTokenExpiredThenCreateNewStatement(SqlAuthentication authentication) throws SQLException {
        long secondsPassed = 0;
        long start = System.currentTimeMillis();
        SQLServerDataSource ds = new SQLServerDataSource();

        if (SqlAuthentication.ActiveDirectoryIntegrated != authentication) {
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setPassword(azurePassword);
            ds.setAuthentication(SqlAuthentication.ActiveDirectoryPassword.toString());
        } else {
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setAuthentication(SqlAuthentication.ActiveDirectoryIntegrated.toString());
        }

        try (Connection connection = (SQLServerConnection) ds.getConnection();
                Statement stmt = connection.createStatement()) {
            testUserName(connection, azureUserName, authentication);

            try {
                TestUtils.dropTableIfExists(charTable, stmt);
                createTable(stmt, charTable);
                populateCharTable(connection, charTable);
                testChar(stmt, charTable);
            } finally {
                TestUtils.dropTableIfExists(charTable, stmt);
            }

            while (secondsPassed < secondsBeforeExpiration) {
                Thread.sleep(TimeUnit.MINUTES.toMillis(5)); // Sleep for 2 minutes

                secondsPassed = (System.currentTimeMillis() - start) / 1000;
                try (Statement stmt1 = connection.createStatement()) {
                    testUserName(connection, azureUserName, authentication);

                    try {
                        TestUtils.dropTableIfExists(charTable, stmt1);
                        createTable(stmt1, charTable);
                        populateCharTable(connection, charTable);
                        testChar(stmt1, charTable);
                    } finally {
                        TestUtils.dropTableIfExists(charTable, stmt1);
                    }
                }
            }
        } catch (Exception e) {
            assertTrue(INVALID_EXCEPTION_MSG + ": " + e.getMessage(),
                    e.getMessage().contains(ERR_MSG_RESULTSET_IS_CLOSED));
        }
    }

    @Test
    public void testAccessTokenExpiredThenExecuteUsingSameStatementADPassword() throws SQLException {
        testAccessTokenExpiredThenExecuteUsingSameStatement(SqlAuthentication.ActiveDirectoryPassword);
    }

    @Test
    public void testAccessTokenExpiredThenExecuteUsingSameStatementADIntegrated() throws SQLException {
        org.junit.Assume.assumeTrue(enableADIntegrated);

        testAccessTokenExpiredThenExecuteUsingSameStatement(SqlAuthentication.ActiveDirectoryIntegrated);
    }

    private void testAccessTokenExpiredThenExecuteUsingSameStatement(
            SqlAuthentication authentication) throws SQLException {
        long secondsPassed = 0;
        long start = System.currentTimeMillis();

        SQLServerDataSource ds = new SQLServerDataSource();

        if (SqlAuthentication.ActiveDirectoryIntegrated != authentication) {
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setUser(azureUserName);
            ds.setPassword(azurePassword);
            ds.setAuthentication(SqlAuthentication.ActiveDirectoryPassword.toString());
        } else {
            ds.setServerName(azureServer);
            ds.setDatabaseName(azureDatabase);
            ds.setAuthentication(SqlAuthentication.ActiveDirectoryIntegrated.toString());
        }

        try {
            try (Connection connection = ds.getConnection(); Statement stmt = connection.createStatement()) {
                testUserName(connection, azureUserName, authentication);

                try {
                    TestUtils.dropTableIfExists(charTable, stmt);
                    createTable(stmt, charTable);
                    populateCharTable(connection, charTable);
                    testChar(stmt, charTable);
                } finally {
                    TestUtils.dropTableIfExists(charTable, stmt);
                }

                while (secondsPassed < secondsBeforeExpiration) {
                    Thread.sleep(TimeUnit.MINUTES.toMillis(5)); // Sleep for 5 minutes

                    secondsPassed = (System.currentTimeMillis() - start) / 1000;
                    testUserName(connection, azureUserName, authentication);
                }

                try {
                    TestUtils.dropTableIfExists(charTable, stmt);
                    createTable(stmt, charTable);
                    populateCharTable(connection, charTable);
                    testChar(stmt, charTable);
                } finally {
                    TestUtils.dropTableIfExists(charTable, stmt);
                }
            }
        } catch (Exception e) {
            assertTrue(INVALID_EXCEPTION_MSG + ": " + e.getMessage(),
                    e.getMessage().contains(ERR_MSG_RESULTSET_IS_CLOSED));
        }
    }

    @AfterAll
    public static void terminate() throws SQLException {
        try (Connection conn = DriverManager.getConnection(adPasswordConnectionStr);
                Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(charTable, stmt);
        }
    }
}
