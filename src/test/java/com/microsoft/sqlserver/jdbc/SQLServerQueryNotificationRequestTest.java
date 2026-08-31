/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;


class SQLServerQueryNotificationRequestTest {
    @Test
    void constructorPreservesValues() throws SQLServerException {
        SQLServerQueryNotificationRequest request = new SQLServerQueryNotificationRequest("cache-key",
                "service=NotificationService;local database=TestDatabase", 60);

        assertEquals("cache-key", request.getUserData());
        assertEquals("service=NotificationService;local database=TestDatabase", request.getOptions());
        assertEquals(60, request.getTimeout());
        assertEquals(60000, request.getTimeoutMilliseconds());
        assertEquals(14 + 2 * request.getUserData().length() + 2 * request.getOptions().length(),
                request.getHeaderLength());
    }

    @Test
    void constructorAcceptsProtocolBoundaries() throws SQLServerException {
        String maximumLengthString = repeat('x', SQLServerQueryNotificationRequest.MAX_STRING_LENGTH);
        SQLServerQueryNotificationRequest request = new SQLServerQueryNotificationRequest(maximumLengthString,
                maximumLengthString, SQLServerQueryNotificationRequest.MAX_TIMEOUT_SECONDS);

        assertEquals(SQLServerQueryNotificationRequest.MAX_STRING_LENGTH, request.getUserData().length());
        assertEquals(SQLServerQueryNotificationRequest.MAX_TIMEOUT_SECONDS, request.getTimeout());
        assertEquals(4294967000L, Integer.toUnsignedLong(request.getTimeoutMilliseconds()));
    }

    @Test
    void constructorRejectsInvalidStrings() {
        assertThrows(SQLServerException.class, () -> new SQLServerQueryNotificationRequest(null, "options", 1));
        assertThrows(SQLServerException.class, () -> new SQLServerQueryNotificationRequest("", "options", 1));
        assertThrows(SQLServerException.class, () -> new SQLServerQueryNotificationRequest("data", null, 1));
        assertThrows(SQLServerException.class, () -> new SQLServerQueryNotificationRequest("data", "", 1));
        assertThrows(SQLServerException.class,
                () -> new SQLServerQueryNotificationRequest(
                        repeat('x', SQLServerQueryNotificationRequest.MAX_STRING_LENGTH + 1), "options", 1));
    }

    @Test
    void constructorRejectsInvalidTimeouts() {
        assertThrows(SQLServerException.class, () -> new SQLServerQueryNotificationRequest("data", "options", -1));
        assertThrows(SQLServerException.class,
                () -> new SQLServerQueryNotificationRequest("data", "options",
                        SQLServerQueryNotificationRequest.MAX_TIMEOUT_SECONDS + 1));
    }

    private static String repeat(char value, int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.append(value);
        }
        return builder.toString();
    }
}
