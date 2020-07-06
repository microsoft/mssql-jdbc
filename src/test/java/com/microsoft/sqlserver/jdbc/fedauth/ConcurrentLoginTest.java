/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.Fedauth)
public class ConcurrentLoginTest extends FedauthCommon {

    @Test
    public void testConcurrentLogin() {
        Random rand = new Random();
        int numberOfThreadsForEachType = rand.nextInt(15) + 1; // 1 to 15

        Runnable r1 = () -> {
            // Access token based authentication
            try {
                SQLServerDataSource ds = new SQLServerDataSource();
                ds.setServerName(azureServer);
                ds.setDatabaseName(azureDatabase);
                ds.setAccessToken(accessToken);

                try (Connection conn = ds.getConnection()) {
                    testUserName(conn, azureUserName, SqlAuthentication.NotSpecified);
                }
            } catch (SQLException e) {
                fail(e.getMessage());
            }
        };

        Runnable r2 = () -> {
            // active directory password
            try {
                SQLServerDataSource ds = new SQLServerDataSource();
                ds.setServerName(azureServer);
                ds.setDatabaseName(azureDatabase);
                ds.setUser(azureUserName);
                ds.setPassword(azurePassword);
                ds.setAuthentication(SqlAuthentication.ActiveDirectoryPassword.toString());

                try (Connection conn = ds.getConnection()) {
                    testUserName(conn, azureUserName, SqlAuthentication.ActiveDirectoryPassword);
                }
            } catch (SQLException e) {
                fail(e.getMessage());
            }
        };

        Runnable r3 = () -> {
            // active directory integrated
            try {
                SQLServerDataSource ds = new SQLServerDataSource();
                ds.setServerName(azureServer);
                ds.setDatabaseName(azureDatabase);
                ds.setAuthentication(SqlAuthentication.ActiveDirectoryIntegrated.toString());

                try (Connection conn = ds.getConnection()) {
                    testUserName(conn, azureUserName, SqlAuthentication.ActiveDirectoryIntegrated);
                }
            } catch (SQLException e) {
                fail(e.getMessage());
            }
        };

        for (int i = 0; i < numberOfThreadsForEachType; i++) {
            new Thread(r1).start();
            new Thread(r2).start();
            if (isWindows && enableADIntegrated)
                new Thread(r3).start();
        }
    }
}
