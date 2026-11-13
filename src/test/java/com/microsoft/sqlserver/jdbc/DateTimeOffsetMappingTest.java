/*
 * Microsoft JDBC Driver for SQL Server
 * Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.ResultSet;
import java.time.OffsetDateTime;
import java.util.GregorianCalendar;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import microsoft.sql.DateTimeOffset;


class DateTimeOffsetMappingTest {
    @Test
    void legacyValuesCanBeBoundWithStandardMetadataType() {
        assertTrue(JDBCType.DATETIMEOFFSET.convertsTo(JDBCType.TIMESTAMP_WITH_TIMEZONE));
        assertTrue(
                JavaType.SetterConversionAE.converts(JavaType.DATETIMEOFFSET, JDBCType.TIMESTAMP_WITH_TIMEZONE, true));
    }

    @Test
    void offsetDateTimeCanBeBoundToEncryptedDateTimeOffset() {
        assertTrue(
                JavaType.SetterConversionAE.converts(JavaType.OFFSETDATETIME, JDBCType.TIMESTAMP_WITH_TIMEZONE, true));
    }

    @Test
    void discoveryTypeFilterPreservesOtherMappings() {
        DataTypeFilter filter = new DataTypeFilter();
        assertEquals(microsoft.sql.Types.DATETIMEOFFSET, filter.oneValueToAnother(microsoft.sql.Types.DATETIMEOFFSET));
        assertEquals(java.sql.Types.TIME, filter.oneValueToAnother(-154));
    }

    @Test
    void resultSetMetadataMatchesLegacyObjectForPlainAndEncryptedColumns() throws Exception {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        SQLServerResultSet result = mock(SQLServerResultSet.class);
        Column column = mock(Column.class);
        TypeInfo type = mock(TypeInfo.class);
        when(result.getColumn(1)).thenReturn(column);
        when(column.getTypeInfo()).thenReturn(type);
        when(type.getSSType()).thenReturn(SSType.DATETIMEOFFSET);
        SQLServerResultSetMetaData metadata = new SQLServerResultSetMetaData(connection, result);
        assertEquals(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, metadata.getColumnType(1));
        assertEquals(DateTimeOffset.class.getName(), metadata.getColumnClassName(1));

        CryptoMetadata crypto = mock(CryptoMetadata.class);
        when(column.getCryptoMetadata()).thenReturn(crypto);
        when(crypto.getBaseTypeInfo()).thenReturn(type);
        assertEquals(java.sql.Types.TIMESTAMP_WITH_TIMEZONE, metadata.getColumnType(1));
        assertEquals(DateTimeOffset.class.getName(), metadata.getColumnClassName(1));
    }

    @Test
    void bulkCopyReadsStandardResultSetUsingStandardClass() throws Exception {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(connection);
        ResultSet source = mock(ResultSet.class);
        OffsetDateTime value = OffsetDateTime.parse("2024-01-02T03:04:05.1234567+05:30");
        when(source.getObject(1, OffsetDateTime.class)).thenReturn(value);
        Field sourceField = SQLServerBulkCopy.class.getDeclaredField("sourceResultSet");
        sourceField.setAccessible(true);
        sourceField.set(bulkCopy, source);
        Method read = SQLServerBulkCopy.class.getDeclaredMethod("readColumnFromResultSet", int.class, int.class,
                boolean.class, boolean.class);
        read.setAccessible(true);
        assertEquals(value, read.invoke(bulkCopy, 1, java.sql.Types.TIMESTAMP_WITH_TIMEZONE, false, false));
        verify(source).getObject(1, OffsetDateTime.class);
    }

    @Test
    void bulkCopyEncryptsStandardOffsetDateTimeWithoutLosingPrecision() throws Exception {
        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(mock(SQLServerConnection.class));
        TDSWriter writer = mock(TDSWriter.class);
        OffsetDateTime value = OffsetDateTime.parse("2024-01-02T03:04:05.1234567+05:30");
        byte[] encrypted = {1, 2};
        when(writer.writeEncryptedScaledTemporal(any(GregorianCalendar.class), eq(value.getNano()), eq(7),
                eq(SSType.DATETIMEOFFSET), eq((short) 330), eq(null))).thenReturn(encrypted);
        Method encrypt = SQLServerBulkCopy.class.getDeclaredMethod("getEncryptedTemporalBytes", TDSWriter.class,
                JDBCType.class, Object.class, int.class);
        encrypt.setAccessible(true);
        assertEquals(encrypted, encrypt.invoke(bulkCopy, writer, JDBCType.DATETIMEOFFSET, value, 7));
        ArgumentCaptor<GregorianCalendar> calendar = ArgumentCaptor.forClass(GregorianCalendar.class);
        verify(writer).writeEncryptedScaledTemporal(calendar.capture(), eq(value.getNano()), eq(7),
                eq(SSType.DATETIMEOFFSET), eq((short) 330), eq(null));
        assertEquals(value.toEpochSecond() * 1000, calendar.getValue().getTimeInMillis());
    }

    @Test
    void bulkWriterStandardAndLegacyEncodingsMatch() throws Exception {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        when(connection.isKatmaiOrLater()).thenReturn(true);
        TDSWriter writer = mock(TDSWriter.class);
        Field connectionField = TDSWriter.class.getDeclaredField("con");
        connectionField.setAccessible(true);
        connectionField.set(writer, connection);
        OffsetDateTime value = OffsetDateTime.parse("2024-01-02T03:04:05.0000001+05:30");
        doCallRealMethod().when(writer).writeOffsetDateTimeWithTimezone(value, 7);
        DateTimeOffset legacy = DateTimeOffset.valueOf(value);
        doCallRealMethod().when(writer).writeDateTimeOffset(legacy, 7, SSType.DATETIMEOFFSET);
        writer.writeOffsetDateTimeWithTimezone(value, 7);
        writer.writeDateTimeOffset(legacy, 7, SSType.DATETIMEOFFSET);
        ArgumentCaptor<byte[]> bytes = ArgumentCaptor.forClass(byte[].class);
        verify(writer, times(4)).writeBytes(bytes.capture());
        List<byte[]> encoded = bytes.getAllValues();
        assertArrayEquals(encoded.get(0), encoded.get(2));
        assertArrayEquals(encoded.get(1), encoded.get(3));
        verify(writer, times(2)).writeShort((short) 330);
    }
}
