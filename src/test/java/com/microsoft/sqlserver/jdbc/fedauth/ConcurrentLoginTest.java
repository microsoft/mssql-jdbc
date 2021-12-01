/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;

import com.microsoft.sqlserver.jdbc.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.fedAuth)
public class ConcurrentLoginTest extends FedauthCommon {

    final AtomicReference<Throwable> throwableRef = new AtomicReference<Throwable>();
    Thread.UncaughtExceptionHandler handler = new Thread.UncaughtExceptionHandler() {

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            throwableRef.set(e);
        }
    };

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    @Test
    public void testConcurrentLogin() throws Exception {
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
            } catch (Exception e) {
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
            } catch (Exception e) {
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
            } catch (Exception e) {
                fail(e.getMessage());
            }
        };

        for (int i = 0; i < numberOfThreadsForEachType; i++) {
            Thread t1 = new Thread(r1);
            Thread t2 = new Thread(r2);
            t1.setUncaughtExceptionHandler(handler);
            t2.setUncaughtExceptionHandler(handler);

            t1.start();
            t2.start();
            if (enableADIntegrated) {
                Thread t3 = new Thread(r3);
                t3.setUncaughtExceptionHandler(handler);
                t3.start();
                t3.join();
            }
            t1.join();
            t2.join();

            Throwable throwable = (Throwable) throwableRef.get();
            if (throwable != null) {
                fail(throwable.getMessage());
            }

        }
    }
}
