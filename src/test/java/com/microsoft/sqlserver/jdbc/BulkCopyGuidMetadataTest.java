/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;


public class BulkCopyGuidMetadataTest {
    @ParameterizedTest
    @MethodSource("sourceTypes")
    public void testEffectiveSourceType(SSType nativeSourceType, int jdbcType, SSType destinationType,
            boolean encryptedDestination, boolean allowEncryptedValueModifications, int expected) throws Exception {
        try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(mock(SQLServerConnection.class))) {
            SQLServerBulkCopy.BulkColumnMetaData source = bulkCopy.new BulkColumnMetaData("id", true, 36, 0, jdbcType,
                    null);
            source.ssType = nativeSourceType;
            SQLServerBulkCopy.BulkColumnMetaData destination = bulkCopy.new BulkColumnMetaData("id", true, 36, 0,
                    destinationType.getJDBCType().asJavaSqlType(), null);
            destination.ssType = destinationType;
            destination.encryptionType = encryptedDestination ? "RANDOMIZED" : null;
            Map<Integer, SQLServerBulkCopy.BulkColumnMetaData> sources = new HashMap<>();
            sources.put(1, source);
            Map<Integer, SQLServerBulkCopy.BulkColumnMetaData> destinations = new HashMap<>();
            destinations.put(1, destination);
            setField(bulkCopy, "srcColumnMetadata", sources);
            setField(bulkCopy, "destColumnMetadata", destinations);
            SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
            options.setAllowEncryptedValueModifications(allowEncryptedValueModifications);
            bulkCopy.setBulkCopyOptions(options);

            Method method = SQLServerBulkCopy.class.getDeclaredMethod("getSourceJdbcType", int.class, int.class);
            method.setAccessible(true);
            assertEquals(expected, method.invoke(bulkCopy, 1, 1));
            assertEquals(jdbcType, source.jdbcType, "Resolving one mapping must not change other mappings.");
        }
    }

    private static Stream<Arguments> sourceTypes() {
        int character = java.sql.Types.CHAR;
        int guid = microsoft.sql.Types.GUID;
        return Stream.of(Arguments.of(SSType.GUID, character, SSType.GUID, false, false, guid),
                Arguments.of(SSType.GUID, character, SSType.VARCHAR, false, false, character),
                Arguments.of(SSType.GUID, character, SSType.GUID, true, false, character),
                Arguments.of(SSType.GUID, character, SSType.GUID, false, true, character),
                Arguments.of(SSType.VARBINARY, character, SSType.GUID, false, false, character),
                Arguments.of(SSType.CHAR, character, SSType.GUID, false, false, character),
                Arguments.of(null, character, SSType.GUID, false, false, character),
                Arguments.of(null, guid, SSType.GUID, false, false, guid));
    }

    @Test
    public void testNativeGuidBytesAndNull() throws Exception {
        try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(mock(SQLServerConnection.class))) {
            Method method = SQLServerBulkCopy.class.getDeclaredMethod("writeGuidToTdsWriter", TDSWriter.class,
                    Object.class, int.class);
            method.setAccessible(true);
            UUID value = UUID.fromString("00112233-4455-6677-8899-aabbccddeeff");
            byte[] expected = {0x33, 0x22, 0x11, 0x00, 0x55, 0x44, 0x77, 0x66, (byte) 0x88, (byte) 0x99, (byte) 0xaa,
                    (byte) 0xbb, (byte) 0xcc, (byte) 0xdd, (byte) 0xee, (byte) 0xff};
            for (Object input : new Object[] {value, value.toString(), null}) {
                TDSWriter writer = mock(TDSWriter.class);
                method.invoke(bulkCopy, writer, input, 36);
                if (null == input) {
                    verify(writer).writeByte((byte) 0);
                } else {
                    verify(writer).writeByte((byte) 16);
                    verify(writer).writeBytes(expected);
                }
                verifyNoMoreInteractions(writer);
            }
        }
    }

    private static void setField(SQLServerBulkCopy bulkCopy, String name, Object value) throws Exception {
        Field field = SQLServerBulkCopy.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(bulkCopy, value);
    }
}
