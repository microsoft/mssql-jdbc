/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.TimeUnit;

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

        FedauthCommon.getFedauthInfo();

        for (int i = 0; i < numberOfThreadsForEachType; i++) {
            // Access token based authentication
            new Thread() {
                public void run() {
                    try {
                        SQLServerDataSource ds = new SQLServerDataSource();
                        ds.setServerName(azureServer);
                        ds.setDatabaseName(azureDatabase);
                        ds.setHostNameInCertificate(hostNameInCertificate);
                        ds.setAccessToken(accessToken);

                        try (Connection connection = ds.getConnection(); Statement st = connection.createStatement();
                                ResultSet rs = st.executeQuery("SELECT SUSER_SNAME()")) {
                            if (rs.next()) {
                                assertTrue(azureUserName.equals(rs.getString(1)));
                            }
                        }
                    } catch (SQLException e) {
                        fail(e.getMessage());
                    }
                }
            }.start();

            // active directory password
            new Thread() {
                public void run() {
                    try {
                        SQLServerDataSource ds = new SQLServerDataSource();
                        ds.setServerName(azureServer);
                        ds.setDatabaseName(azureDatabase);
                        ds.setUser(azureUserName);
                        ds.setPassword(azurePassword);
                        ds.setAuthentication("ActiveDirectoryPassword");
                        ds.setHostNameInCertificate(hostNameInCertificate);

                        try (Connection connection = ds.getConnection(); Statement st = connection.createStatement();
                                ResultSet rs = st.executeQuery("SELECT SUSER_SNAME()")) {
                            if (rs.next()) {
                                assertTrue(azureUserName.equals(rs.getString(1)));
                            }
                        }
                    } catch (SQLException e) {
                        fail(e.getMessage());
                    }
                }
            }.start();

            // active directory integrated
            if (enableADIntegrated) {
                new Thread() {
                    public void run() {
                        try {
                            SQLServerDataSource ds = new SQLServerDataSource();
                            ds.setServerName(azureServer);
                            ds.setDatabaseName(azureDatabase);
                            ds.setAuthentication("ActiveDirectoryIntegrated");
                            ds.setHostNameInCertificate(hostNameInCertificate);

                            try (Connection connection = ds.getConnection();
                                    Statement st = connection.createStatement();
                                    ResultSet rs = st.executeQuery("SELECT SUSER_SNAME()")) {
                                if (rs.next()) {
                                    assertTrue(azureUserName.equals(rs.getString(1)));
                                }
                            }
                        } catch (SQLException e) {
                            fail(e.getMessage());
                        }
                    }
                }.start();
            }
        }

        // sleep in order to catch exception from other threads if tests fail.
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(60));
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
    }
}
