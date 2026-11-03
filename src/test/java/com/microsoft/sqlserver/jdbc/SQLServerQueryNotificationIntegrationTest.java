/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@Tag(Constants.reqExternalSetup)
@Tag(Constants.xAzureSQLDB)
@Tag(Constants.xAzureSQLDW)
public class SQLServerQueryNotificationIntegrationTest extends AbstractTest {
    private static final int NOTIFICATION_TIMEOUT_SECONDS = 30;

    @Test
    public void statementAndPreparedStatementDeliverRenewedNotifications() throws Exception {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String tableName = "QueryNotificationTable_" + suffix;
        String queueName = "QueryNotificationQueue_" + suffix;
        String serviceName = "QueryNotificationService_" + suffix;
        String escapedTableName = "dbo." + Util.escapeSQLId(tableName);
        SQLServerDataSource dataSource =
                (SQLServerDataSource) updateDataSource(connectionString, new SQLServerDataSource());
        SQLServerQueryNotificationListener listener = null;
        boolean brokerObjectsCreated = false;

        try (Connection testConnection = dataSource.getConnection();
                Statement setup = testConnection.createStatement()) {
            setup.executeUpdate("CREATE TABLE " + escapedTableName
                    + " (id int NOT NULL PRIMARY KEY, value int NOT NULL)");
            setup.executeUpdate("INSERT INTO " + escapedTableName + " (id, value) VALUES (1, 0)");
            SQLServerQueryNotificationListener.createServiceBrokerObjects(dataSource, queueName, serviceName);
            brokerObjectsCreated = true;

            String options = "service=" + serviceName + ";local database=" + testConnection.getCatalog();
            AtomicReference<SQLServerException> listenerError = new AtomicReference<>();
            listener = new SQLServerQueryNotificationListener(dataSource, queueName, Runnable::run);

            CountDownLatch statementNotification = new CountDownLatch(1);
            listener.register("statement-subscription", new TestNotificationListener(statementNotification,
                    listenerError));
            listener.start();

            SQLServerQueryNotificationRequest statementRequest = new SQLServerQueryNotificationRequest(
                    "statement-subscription", options, NOTIFICATION_TIMEOUT_SECONDS);
            try (SQLServerStatement statement = (SQLServerStatement) testConnection.createStatement()) {
                statement.setQueryNotificationRequest(statementRequest);
                consume(statement.executeQuery("SELECT id, value FROM " + escapedTableName + " WHERE id = 1"));
            }
            setup.executeUpdate("UPDATE " + escapedTableName + " SET value = value + 1 WHERE id = 1");
            awaitNotification(statementNotification, listenerError, "Statement notification was not delivered");

            CountDownLatch preparedNotifications = new CountDownLatch(2);
            listener.register("prepared-subscription", new TestNotificationListener(preparedNotifications,
                    listenerError));
            SQLServerQueryNotificationRequest preparedRequest = new SQLServerQueryNotificationRequest(
                    "prepared-subscription", options, NOTIFICATION_TIMEOUT_SECONDS);
            String selectSql = "SELECT id, value FROM " + escapedTableName + " WHERE id = ?";
            try (SQLServerPreparedStatement statement =
                    (SQLServerPreparedStatement) testConnection.prepareStatement(selectSql)) {
                statement.setInt(1, 1);
                statement.setQueryNotificationRequest(preparedRequest);
                consume(statement.executeQuery());
                setup.executeUpdate("UPDATE " + escapedTableName + " SET value = value + 1 WHERE id = 1");
                awaitCount(preparedNotifications, 1, listenerError,
                        "PreparedStatement notification was not delivered");

                // Query Notification subscriptions are one-shot. Re-execute the query to renew the subscription.
                consume(statement.executeQuery());
                setup.executeUpdate("UPDATE " + escapedTableName + " SET value = value + 1 WHERE id = 1");
                awaitNotification(preparedNotifications, listenerError,
                        "Renewed PreparedStatement notification was not delivered");
            }
        } finally {
            if (null != listener) {
                listener.stop();
                waitForListenerToStop(listener);
            }
            if (brokerObjectsCreated) {
                SQLServerQueryNotificationListener.dropServiceBrokerObjects(dataSource, queueName, serviceName);
            }
            try (Connection cleanupConnection = dataSource.getConnection();
                    Statement cleanup = cleanupConnection.createStatement()) {
                cleanup.executeUpdate("DROP TABLE IF EXISTS " + escapedTableName);
            }
        }
    }

    private static void consume(ResultSet resultSet) throws Exception {
        try (ResultSet closeableResultSet = resultSet) {
            while (closeableResultSet.next()) {
                // Consume the result so the subscription request completes normally.
            }
        }
    }

    private static void awaitCount(CountDownLatch latch, long expectedRemaining,
            AtomicReference<SQLServerException> error, String message) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(NOTIFICATION_TIMEOUT_SECONDS);
        while (latch.getCount() > expectedRemaining && System.nanoTime() < deadline) {
            if (null != error.get()) {
                fail(error.get());
            }
            Thread.sleep(50);
        }
        assertTrue(latch.getCount() <= expectedRemaining, message);
    }

    private static void awaitNotification(CountDownLatch latch, AtomicReference<SQLServerException> error,
            String message) throws Exception {
        assertTrue(latch.await(NOTIFICATION_TIMEOUT_SECONDS, TimeUnit.SECONDS), message);
        if (null != error.get()) {
            fail(error.get());
        }
    }

    private static void waitForListenerToStop(SQLServerQueryNotificationListener listener) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (listener.isRunning() && System.nanoTime() < deadline) {
            Thread.sleep(50);
        }
        assertTrue(!listener.isRunning(), "Query Notification listener did not stop");
    }

    private static final class TestNotificationListener implements ISQLServerQueryNotificationListener {
        private final CountDownLatch latch;
        private final AtomicReference<SQLServerException> error;

        private TestNotificationListener(CountDownLatch latch, AtomicReference<SQLServerException> error) {
            this.latch = latch;
            this.error = error;
        }

        @Override
        public void onNotification(SQLServerQueryNotification notification) {
            latch.countDown();
        }

        @Override
        public void onError(SQLServerException exception) {
            error.compareAndSet(null, exception);
        }
    }
}
