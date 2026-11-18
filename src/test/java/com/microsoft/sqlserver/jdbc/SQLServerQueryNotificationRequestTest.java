/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;


class SQLServerQueryNotificationRequestTest {
    @Test
    void statementExtensionMethodsRemainBinaryCompatibleDefaults() throws NoSuchMethodException {
        Method setter = ISQLServerStatement.class.getMethod("setQueryNotificationRequest",
                SQLServerQueryNotificationRequest.class);
        Method getter = ISQLServerStatement.class.getMethod("getQueryNotificationRequest");

        assertTrue(setter.isDefault());
        assertTrue(getter.isDefault());
    }

    @Test
    void constructorPreservesValues() throws SQLServerException {
        SQLServerQueryNotificationRequest request = new SQLServerQueryNotificationRequest("cache-key",
                "service=NotificationService;local database=TestDatabase", 60);

        assertEquals("cache-key", request.getUserData());
        assertEquals("service=NotificationService;local database=TestDatabase", request.getOptions());
        assertEquals(60, request.getTimeout());
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

    @Test
    void writesProtocolTimeoutInSeconds() throws Exception {
        SQLServerConnection connection = Mockito.mock(SQLServerConnection.class);
        TDSChannel channel = Mockito.mock(TDSChannel.class);
        Mockito.when(connection.getTDSPacketSize()).thenReturn(4096);
        Mockito.when(connection.getTransactionDescriptor()).thenReturn(new byte[8]);

        SQLServerQueryNotificationRequest request =
                new SQLServerQueryNotificationRequest("cache-key", "service=NotificationService", 60);
        TDSCommand command = new TDSCommand("query-notification-test", 0, 0) {
            private static final long serialVersionUID = 1L;

            @Override
            boolean doExecute() {
                return true;
            }
        };
        command.setQueryNotificationRequest(request);

        TDSWriter writer = new TDSWriter(channel, connection);
        writer.startMessage(command, TDS.PKT_QUERY);

        Field stagingBufferField = TDSWriter.class.getDeclaredField("stagingBuffer");
        stagingBufferField.setAccessible(true);
        ByteBuffer header = ((ByteBuffer) stagingBufferField.get(writer)).duplicate().order(ByteOrder.LITTLE_ENDIAN);
        int timeoutOffset = 22 + 2 * request.getUserData().length() + 2 * request.getOptions().length();
        assertEquals(60, header.getInt(timeoutOffset));
    }

    private static String repeat(char value, int count) {
        StringBuilder builder = new StringBuilder(count);
        for (int i = 0; i < count; i++) {
            builder.append(value);
        }
        return builder.toString();
    }
}
