/*
 * Microsoft JDBC Driver for SQL Server
 * Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
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
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;

import microsoft.sql.DateTimeOffset;


class DateTimeOffsetMappingTest {
    @ParameterizedTest
    @ValueSource(strings = {"0025-03-11T18:57:04.349953-00:33", "0076-07-08T17:34:51.1592513+09:09",
            "0095-05-12T16:15:34.7841496+07:44", "0104-03-03T09:04:05.871-05:13", "0597-09-12T07:38:16.9166545+13:48",
            "1234-12-12T23:59:59.1234567-03:00", "1500-03-01T00:00:00Z", "1582-10-10T12:34:56Z", "1582-10-15T00:00:00Z",
            "0001-01-01T14:00:00+14:00", "9999-12-31T09:59:59.9999999-14:00", "2024-01-02T03:04:05.0000001+05:30"})
    void bulkWriterPreservesGregorianDate(String text) throws Exception {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        when(connection.isKatmaiOrLater()).thenReturn(true);
        TDSWriter writer = mock(TDSWriter.class);
        Field connectionField = TDSWriter.class.getDeclaredField("con");
        connectionField.setAccessible(true);
        connectionField.set(writer, connection);
        OffsetDateTime value = OffsetDateTime.parse(text);
        doCallRealMethod().when(writer).writeOffsetDateTimeWithTimezone(value, 7);
        writer.writeOffsetDateTimeWithTimezone(value, 7);

        ArgumentCaptor<byte[]> bytes = ArgumentCaptor.forClass(byte[].class);
        verify(writer, times(2)).writeBytes(bytes.capture());
        OffsetDateTime utc = value.withOffsetSameInstant(ZoneOffset.UTC);
        long days = utc.toLocalDate().toEpochDay() - LocalDate.of(1, 1, 1).toEpochDay();
        assertArrayEquals(new byte[] {(byte) days, (byte) (days >> 8), (byte) (days >> 16)},
                bytes.getAllValues().get(1));
        long ticks = utc.toLocalTime().toNanoOfDay() / 100;
        assertArrayEquals(new byte[] {(byte) ticks, (byte) (ticks >> 8), (byte) (ticks >> 16), (byte) (ticks >> 24),
                (byte) (ticks >> 32)}, bytes.getAllValues().get(0));
        verify(writer).writeShort((short) (value.getOffset().getTotalSeconds() / 60));
    }

    @ParameterizedTest
    @CsvSource({"2025-05-13T14:30:00Z,2025-05-13 14:30:00 +00:00",
            "1234-12-12T23:59:59.1234567-03:00,1234-12-12 23:59:59.1234567 -03:00",
            "2024-01-02T03:04:05.0000001+05:30,2024-01-02 03:04:05.0000001 +05:30",
            "2024-12-31T23:59:59.999999950Z,2025-01-01 00:00:00 +00:00"})
    void bulkBatchFormatsOffsetDateTimeAsSqlLiteral(String text, String expected) throws Exception {
        Parameter parameter = mock(Parameter.class);
        when(parameter.getSetterValue()).thenReturn(OffsetDateTime.parse(text));
        ArrayList<Parameter[]> batch = new ArrayList<>();
        batch.add(new Parameter[] {parameter});
        SQLServerBulkBatchInsertRecord record = new SQLServerBulkBatchInsertRecord(batch, null,
                new ArrayList<>(Arrays.asList("?")), null, false);
        record.addColumnMetadata(1, "dto", microsoft.sql.Types.DATETIMEOFFSET, 34, 7);
        assertTrue(record.next());
        assertEquals(expected, record.getRowData()[0]);

        SQLServerConnection connection = mock(SQLServerConnection.class);
        when(connection.isKatmaiOrLater()).thenReturn(true);
        TDSWriter writer = mock(TDSWriter.class);
        Field connectionField = TDSWriter.class.getDeclaredField("con");
        connectionField.setAccessible(true);
        connectionField.set(writer, connection);
        doCallRealMethod().when(writer).writeDateTimeOffset(expected, 7, SSType.DATETIMEOFFSET);
        writer.writeDateTimeOffset(record.getRowData()[0], 7, SSType.DATETIMEOFFSET);
        ArgumentCaptor<byte[]> bytes = ArgumentCaptor.forClass(byte[].class);
        verify(writer, times(2)).writeBytes(bytes.capture());
        OffsetDateTime rounded = OffsetDateTime.parse(expected.replaceFirst(" ", "T").replace(" ", ""));
        long days = rounded.withOffsetSameInstant(ZoneOffset.UTC).toLocalDate().toEpochDay()
                - LocalDate.of(1, 1, 1).toEpochDay();
        assertArrayEquals(new byte[] {(byte) days, (byte) (days >> 8), (byte) (days >> 16)},
                bytes.getAllValues().get(1));
        long ticks = rounded.withOffsetSameInstant(ZoneOffset.UTC).toLocalTime().toNanoOfDay() / 100;
        assertArrayEquals(new byte[] {(byte) ticks, (byte) (ticks >> 8), (byte) (ticks >> 16), (byte) (ticks >> 24),
                (byte) (ticks >> 32)}, bytes.getAllValues().get(0));
    }

    @Test
    void bulkBatchPreservesLegacyAndNullDateTimeOffset() throws Exception {
        DateTimeOffset legacy = DateTimeOffset.valueOf(OffsetDateTime.parse("2024-01-02T03:04:05.1234567+05:30"));
        Parameter parameter = mock(Parameter.class);
        when(parameter.getSetterValue()).thenReturn(legacy, (Object) null);
        ArrayList<Parameter[]> batch = new ArrayList<>();
        batch.add(new Parameter[] {parameter});
        batch.add(new Parameter[] {parameter});
        SQLServerBulkBatchInsertRecord record = new SQLServerBulkBatchInsertRecord(batch, null,
                new ArrayList<>(Arrays.asList("?")), null, false);
        record.addColumnMetadata(1, "dto", microsoft.sql.Types.DATETIMEOFFSET, 34, 7);
        assertTrue(record.next());
        assertSame(legacy, record.getRowData()[0]);
        assertTrue(record.next());
        assertNull(record.getRowData()[0]);
    }

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

    @ParameterizedTest
    @ValueSource(strings = {"2024-01-02T03:04:05.1234567+05:30", "1234-12-12T23:59:59.1234567-03:00",
            "1500-03-01T00:00:00.0000001Z", "1582-10-10T12:34:56Z"})
    void bulkCopyEncryptsStandardOffsetDateTimeWithoutLosingPrecision(String text) throws Exception {
        SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(mock(SQLServerConnection.class));
        TDSWriter writer = mock(TDSWriter.class);
        OffsetDateTime value = OffsetDateTime.parse(text);
        short offset = (short) (value.getOffset().getTotalSeconds() / 60);
        byte[] encrypted = {1, 2};
        when(writer.writeEncryptedScaledTemporal(any(GregorianCalendar.class), eq(value.getNano()), eq(7),
                eq(SSType.DATETIMEOFFSET), eq(offset), eq(null))).thenReturn(encrypted);
        Method encrypt = SQLServerBulkCopy.class.getDeclaredMethod("getEncryptedTemporalBytes", TDSWriter.class,
                JDBCType.class, Object.class, int.class);
        encrypt.setAccessible(true);
        assertEquals(encrypted, encrypt.invoke(bulkCopy, writer, JDBCType.DATETIMEOFFSET, value, 7));
        ArgumentCaptor<GregorianCalendar> calendar = ArgumentCaptor.forClass(GregorianCalendar.class);
        verify(writer).writeEncryptedScaledTemporal(calendar.capture(), eq(value.getNano()), eq(7),
                eq(SSType.DATETIMEOFFSET), eq(offset), eq(null));
        assertEquals(value.toEpochSecond() * 1000, calendar.getValue().getTimeInMillis());
        OffsetDateTime utc = value.withOffsetSameInstant(ZoneOffset.UTC);
        assertEquals(utc.getYear(), calendar.getValue().get(Calendar.YEAR));
        assertEquals(utc.getMonthValue() - 1, calendar.getValue().get(Calendar.MONTH));
        assertEquals(utc.getDayOfMonth(), calendar.getValue().get(Calendar.DAY_OF_MONTH));
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
