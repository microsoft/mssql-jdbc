/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;


class SQLServerQueryNotificationListenerTest {
    @Test
    void synchronizesStartupAndShutdown() throws NoSuchMethodException {
        Method start = SQLServerQueryNotificationListener.class.getMethod("start");
        Method stop = SQLServerQueryNotificationListener.class.getMethod("stop");

        assertTrue(Modifier.isSynchronized(start.getModifiers()));
        assertTrue(Modifier.isSynchronized(stop.getModifiers()));
    }

    @Test
    void dispatchesRegisteredNotificationsAndSupportsUnregister() throws SQLServerException {
        DataSource dataSource = Mockito.mock(DataSource.class);
        SQLServerQueryNotificationListener notificationListener =
                new SQLServerQueryNotificationListener(dataSource, "NotificationQueue", Runnable::run);
        AtomicInteger callbackCount = new AtomicInteger();
        ISQLServerQueryNotificationListener callback = notification -> callbackCount.incrementAndGet();
        SQLServerQueryNotification notification =
                new SQLServerQueryNotification("cache-key", "change", "data", "insert", 1, 2);

        notificationListener.register("cache-key", callback);
        notificationListener.dispatch(notification);
        notificationListener.dispatch(notification);
        assertEquals(2, callbackCount.get());

        notificationListener.unregister("cache-key", callback);
        notificationListener.dispatch(notification);
        assertEquals(2, callbackCount.get());
    }

    @Test
    void validatesConstructorAndRegistrationArguments() throws SQLServerException {
        DataSource dataSource = Mockito.mock(DataSource.class);
        assertThrows(SQLServerException.class, () -> new SQLServerQueryNotificationListener(null, "queue"));
        assertThrows(SQLServerException.class, () -> new SQLServerQueryNotificationListener(dataSource, ""));

        SQLServerQueryNotificationListener listener =
                new SQLServerQueryNotificationListener(dataSource, "queue", Runnable::run);
        assertThrows(SQLServerException.class, () -> listener.register(null, notification -> {}));
        assertThrows(SQLServerException.class, () -> listener.register("key", null));
    }

    @Test
    void fallsBackToInlineCallbackWhenExecutorRejects() throws SQLServerException {
        DataSource dataSource = Mockito.mock(DataSource.class);
        Executor rejectingExecutor = command -> {
            throw new RejectedExecutionException("rejected");
        };
        SQLServerQueryNotificationListener listener =
                new SQLServerQueryNotificationListener(dataSource, "queue", rejectingExecutor);
        AtomicInteger callbackCount = new AtomicInteger();
        listener.register("key", notification -> callbackCount.incrementAndGet());

        listener.dispatch(new SQLServerQueryNotification("key", "change", "data", "insert", 1, 2));

        assertEquals(1, callbackCount.get());
    }

    @Test
    void commitsReceiveAndConversationBeforeReturningNotification() throws Exception {
        DataSource dataSource = Mockito.mock(DataSource.class);
        Connection connection = Mockito.mock(Connection.class);
        Statement receiveStatement = Mockito.mock(Statement.class);
        PreparedStatement endConversationStatement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        SQLServerQueryNotificationListener listener =
                new SQLServerQueryNotificationListener(dataSource, "queue", Runnable::run);
        String payload = "<QueryNotification id=\"1\" type=\"change\" source=\"data\" info=\"insert\" "
                + "database_id=\"2\"><Message>key</Message></QueryNotification>";

        Mockito.when(connection.createStatement()).thenReturn(receiveStatement);
        Mockito.when(receiveStatement.executeQuery("receive")).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true);
        Mockito.when(resultSet.getString(1)).thenReturn("conversation");
        Mockito.when(resultSet.getString(2))
                .thenReturn("http://schemas.microsoft.com/SQL/Notifications/QueryNotification");
        Mockito.when(resultSet.getString(3)).thenReturn(payload);
        Mockito.when(connection.prepareStatement("END CONVERSATION ?")).thenReturn(endConversationStatement);

        SQLServerQueryNotification notification = listener.receiveNext(connection, "receive");

        assertEquals("key", notification.getUserData());
        InOrder order = Mockito.inOrder(receiveStatement, endConversationStatement, connection);
        order.verify(receiveStatement).executeQuery("receive");
        Mockito.verify(receiveStatement).setQueryTimeout(0);
        order.verify(connection).prepareStatement("END CONVERSATION ?");
        order.verify(endConversationStatement).executeUpdate();
        order.verify(connection).commit();
    }

    @Test
    void reportsInvalidPayloadWithoutReconnecting() throws Exception {
        DataSource dataSource = Mockito.mock(DataSource.class);
        Connection connection = Mockito.mock(Connection.class);
        Statement receiveStatement = Mockito.mock(Statement.class);
        PreparedStatement endConversationStatement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        ISQLServerQueryNotificationListener callback = Mockito.mock(ISQLServerQueryNotificationListener.class);
        SQLServerQueryNotificationListener listener =
                new SQLServerQueryNotificationListener(dataSource, "queue", Runnable::run);
        listener.register("key", callback);

        Mockito.when(connection.createStatement()).thenReturn(receiveStatement);
        Mockito.when(receiveStatement.executeQuery("receive")).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true);
        Mockito.when(resultSet.getString(1)).thenReturn("conversation");
        Mockito.when(resultSet.getString(2))
                .thenReturn("http://schemas.microsoft.com/SQL/Notifications/QueryNotification");
        Mockito.when(resultSet.getString(3)).thenReturn("<invalid/>");
        Mockito.when(connection.prepareStatement("END CONVERSATION ?")).thenReturn(endConversationStatement);

        SQLServerQueryNotification notification = listener.receiveNext(connection, "receive");

        assertNull(notification);
        Mockito.verify(connection).commit();
        Mockito.verify(callback).onError(Mockito.any(SQLServerException.class));
    }

    @Test
    void stopCancelsActiveReceiveStatement() throws Exception {
        DataSource dataSource = Mockito.mock(DataSource.class);
        Statement statement = Mockito.mock(Statement.class);
        SQLServerQueryNotificationListener listener =
                new SQLServerQueryNotificationListener(dataSource, "queue", Runnable::run);
        Field receiveStatement = SQLServerQueryNotificationListener.class.getDeclaredField("receiveStatement");
        receiveStatement.setAccessible(true);
        receiveStatement.set(listener, statement);

        listener.stop();

        Mockito.verify(statement).cancel();
    }

    @Test
    void stopWaitsForReceiveThreadToTerminate() throws Exception {
        DataSource dataSource = Mockito.mock(DataSource.class);
        SQLServerQueryNotificationListener listener =
                new SQLServerQueryNotificationListener(dataSource, "queue", Runnable::run);
        CountDownLatch started = new CountDownLatch(1);
        Thread receiveThread = new Thread(() -> {
            started.countDown();
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(30));
            } catch (InterruptedException e) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }
        });
        Field receiveThreadField = SQLServerQueryNotificationListener.class.getDeclaredField("receiveThread");
        receiveThreadField.setAccessible(true);
        receiveThreadField.set(listener, receiveThread);
        receiveThread.start();
        assertTrue(started.await(5, TimeUnit.SECONDS));

        listener.stop();

        assertFalse(receiveThread.isAlive());
    }

    @Test
    void rollsBackReceiveWhenConversationCleanupFails() throws Exception {
        DataSource dataSource = Mockito.mock(DataSource.class);
        Connection connection = Mockito.mock(Connection.class);
        Statement receiveStatement = Mockito.mock(Statement.class);
        PreparedStatement endConversationStatement = Mockito.mock(PreparedStatement.class);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        SQLServerQueryNotificationListener listener =
                new SQLServerQueryNotificationListener(dataSource, "queue", Runnable::run);

        Mockito.when(connection.createStatement()).thenReturn(receiveStatement);
        Mockito.when(receiveStatement.executeQuery("receive")).thenReturn(resultSet);
        Mockito.when(resultSet.next()).thenReturn(true);
        Mockito.when(resultSet.getString(1)).thenReturn("conversation");
        Mockito.when(resultSet.getString(2)).thenReturn("other-message-type");
        Mockito.when(connection.prepareStatement("END CONVERSATION ?")).thenReturn(endConversationStatement);
        SQLException cleanupException = new SQLException("cleanup failed");
        Mockito.when(endConversationStatement.executeUpdate()).thenThrow(cleanupException);

        SQLException thrown = assertThrows(SQLException.class, () -> listener.receiveNext(connection, "receive"));

        assertSame(cleanupException, thrown);
        Mockito.verify(connection).rollback();
        Mockito.verify(connection, Mockito.never()).commit();
    }

    @Test
    void dropBrokerObjectsCommitsAndRestoresAutoCommit() throws Exception {
        DataSource dataSource = Mockito.mock(DataSource.class);
        Connection connection = Mockito.mock(Connection.class);
        Statement statement = Mockito.mock(Statement.class);
        PreparedStatement serviceCheck = Mockito.mock(PreparedStatement.class);
        PreparedStatement queueCheck = Mockito.mock(PreparedStatement.class);
        ResultSet serviceResult = Mockito.mock(ResultSet.class);
        ResultSet queueResult = Mockito.mock(ResultSet.class);

        Mockito.when(dataSource.getConnection()).thenReturn(connection);
        Mockito.when(connection.getAutoCommit()).thenReturn(false);
        Mockito.when(connection.createStatement()).thenReturn(statement);
        Mockito.when(connection.prepareStatement("SELECT 1 FROM sys.services WHERE name = ?"))
                .thenReturn(serviceCheck);
        Mockito.when(connection.prepareStatement(
                "SELECT 1 FROM sys.service_queues WHERE object_id = OBJECT_ID(?)"))
                .thenReturn(queueCheck);
        Mockito.when(serviceCheck.executeQuery()).thenReturn(serviceResult);
        Mockito.when(queueCheck.executeQuery()).thenReturn(queueResult);
        Mockito.when(serviceResult.next()).thenReturn(false);
        Mockito.when(queueResult.next()).thenReturn(false);

        SQLServerQueryNotificationListener.dropServiceBrokerObjects(dataSource, "queue", "service");

        InOrder order = Mockito.inOrder(connection);
        order.verify(connection).setAutoCommit(false);
        order.verify(connection).commit();
        order.verify(connection).setAutoCommit(false);
        Mockito.verify(queueCheck).setString(1, "[queue]");
    }
}
