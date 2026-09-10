/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.Serializable;
import java.text.MessageFormat;


/**
 * Describes a request for a SQL Server Query Notification.
 * <p>
 * The request is attached to a statement with
 * {@link ISQLServerStatement#setQueryNotificationRequest(SQLServerQueryNotificationRequest)}. SQL Server delivers the
 * resulting one-time notification to the Service Broker service identified by {@code options}.
 */
public final class SQLServerQueryNotificationRequest implements Serializable {
    private static final long serialVersionUID = 5076534975097907508L;

    static final int MAX_STRING_LENGTH = 32767;
    static final int MAX_TIMEOUT_SECONDS = 4294967;

    private final String userData;
    private final String options;
    private final int timeout;

    /**
     * Creates a Query Notification request.
     *
     * @param userData
     *        opaque application data used to identify the notification
     * @param options
     *        Service Broker options, for example
     *        {@code service=ProductNotificationService;local database=Products}
     * @param timeout
     *        subscription timeout in seconds; zero cancels a matching subscription
     * @throws SQLServerException
     *         if an argument is invalid
     */
    public SQLServerQueryNotificationRequest(String userData, String options, int timeout) throws SQLServerException {
        validateString(userData, "userData");
        validateString(options, "options");

        if (timeout < 0 || timeout > MAX_TIMEOUT_SECONDS) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerException.getErrString("R_invalidQueryNotificationTimeout"), null, false);
        }

        this.userData = userData;
        this.options = options;
        this.timeout = timeout;
    }

    private void validateString(String value, String argumentName) throws SQLServerException {
        if (null == value || value.isEmpty() || value.length() > MAX_STRING_LENGTH) {
            MessageFormat form = new MessageFormat(
                    SQLServerException.getErrString("R_invalidQueryNotificationArgument"));
            SQLServerException.makeFromDriverError(null, this,
                    form.format(new Object[] {argumentName, MAX_STRING_LENGTH}), null, false);
        }
    }

    /**
     * Returns the opaque application data used to identify the notification.
     *
     * @return the notification user data
     */
    public String getUserData() {
        return userData;
    }

    /**
     * Returns the Service Broker deployment options.
     *
     * @return the Service Broker options
     */
    public String getOptions() {
        return options;
    }

    /**
     * Returns the subscription timeout in seconds.
     *
     * @return the timeout in seconds
     */
    public int getTimeout() {
        return timeout;
    }

    int getTimeoutMilliseconds() {
        return timeout * 1000;
    }

    int getHeaderLength() {
        // HeaderLength, HeaderType, two string lengths, the UTF-16LE strings, and NotifyTimeout.
        return 14 + 2 * userData.length() + 2 * options.length();
    }
}
