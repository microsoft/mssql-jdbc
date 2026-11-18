/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;


class SQLServerQueryNotificationParserTest {
    @Test
    void parsesNotificationPayload() throws SQLServerException {
        String payload = "<qn:QueryNotification xmlns:qn=\"http://schemas.microsoft.com/SQL/Notifications/"
                + "QueryNotification\" id=\"42\" type=\"change\" source=\"data\" info=\"update\" database_id=\"7\">"
                + "<qn:Message>products-cache</qn:Message></qn:QueryNotification>";

        SQLServerQueryNotification notification = SQLServerQueryNotificationParser.parse(payload);

        assertEquals("products-cache", notification.getUserData());
        assertEquals("change", notification.getType());
        assertEquals("data", notification.getSource());
        assertEquals("update", notification.getInfo());
        assertEquals(42, notification.getId());
        assertEquals(7, notification.getDatabaseId());
    }

    @Test
    void rejectsMalformedAndUnsafePayloads() {
        assertThrows(SQLServerException.class, () -> SQLServerQueryNotificationParser.parse(null));
        assertThrows(SQLServerException.class, () -> SQLServerQueryNotificationParser.parse("<wrong/>"));
        assertThrows(SQLServerException.class,
                () -> SQLServerQueryNotificationParser.parse("<!DOCTYPE x [<!ENTITY e SYSTEM 'file:///tmp/x'>]>"
                        + "<QueryNotification id=\"1\" database_id=\"1\"><Message>&e;</Message></QueryNotification>"));
    }
}
