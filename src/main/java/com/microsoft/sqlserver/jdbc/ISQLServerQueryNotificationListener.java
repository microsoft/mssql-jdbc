/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/** Receives SQL Server Query Notifications and listener errors. */
public interface ISQLServerQueryNotificationListener {
    /**
     * Called when SQL Server delivers a Query Notification.
     *
     * @param notification
     *        the notification
     */
    void onNotification(SQLServerQueryNotification notification);

    /**
     * Called when the background listener encounters an error. The listener will attempt to reconnect until stopped.
     *
     * @param exception
     *        the listener error
     */
    default void onError(SQLServerException exception) {
        // Applications may override this method when listener error reporting is required.
    }
}
