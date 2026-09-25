/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.atomic.AtomicInteger;

import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;


class SQLServerQueryNotificationListenerTest {
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
}
