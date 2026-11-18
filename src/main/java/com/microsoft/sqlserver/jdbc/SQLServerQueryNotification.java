/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.Serializable;


/** Represents a Query Notification message delivered by SQL Server Service Broker. */
public final class SQLServerQueryNotification implements Serializable {
    private static final long serialVersionUID = 5273397081452543455L;

    private final String userData;
    private final String type;
    private final String source;
    private final String info;
    private final long id;
    private final int databaseId;

    SQLServerQueryNotification(String userData, String type, String source, String info, long id, int databaseId) {
        this.userData = userData;
        this.type = type;
        this.source = source;
        this.info = info;
        this.id = id;
        this.databaseId = databaseId;
    }

    /** @return the application correlation value supplied with the request */
    public String getUserData() {
        return userData;
    }

    /** @return the notification type, such as {@code change} or {@code subscribe} */
    public String getType() {
        return type;
    }

    /** @return the source that caused the notification, such as {@code data} or {@code object} */
    public String getSource() {
        return source;
    }

    /** @return the notification reason, such as {@code insert}, {@code update}, or {@code delete} */
    public String getInfo() {
        return info;
    }

    /** @return the server notification identifier */
    public long getId() {
        return id;
    }

    /** @return the ID of the database that produced the notification */
    public int getDatabaseId() {
        return databaseId;
    }
}
