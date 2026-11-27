/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;


@Tag("Unit")
public class AdaptiveParameterSizingTest {
    @ParameterizedTest
    @EnumSource(value = JDBCType.class, names = {"CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "BINARY", "VARBINARY"})
    public void testFloorGrowthAndNulls(JDBCType type) throws Exception {
        SQLServerConnection connection = connection(false, 10);
        Parameter parameter = new Parameter(false, true);
        String family = type.isBinary() ? "varbinary"
                                        : type == JDBCType.NCHAR || type == JDBCType.NVARCHAR ? "nvarchar" : "varchar";
        set(parameter, connection, type, null, null);
        assertEquals(family + "(10)", parameter.getTypeDefinition(connection, null));
        int[] lengths = {0, 1, 11, 3, 17, 0};
        int[] expected = {10, 10, 11, 11, 17, 17};
        for (int i = 0; i < lengths.length; i++) {
            int length = lengths[i];
            set(parameter, connection, type, type.isBinary() ? new byte[length] : repeated(length), null);
            assertEquals(family + "(" + expected[i] + ")", parameter.getTypeDefinition(connection, null));
        }
        set(parameter, connection, type, null, null);
        assertEquals(family + "(17)", parameter.getTypeDefinition(connection, null));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testEffectiveTypeUnits(boolean unicode) throws Exception {
        SQLServerConnection connection = connection(unicode, 1);
        Parameter parameter = new Parameter(false, true);
        set(parameter, connection, JDBCType.VARCHAR, "\uD83D\uDE00", null);
        assertEquals(unicode ? "nvarchar(2)" : "varchar(4)", parameter.getTypeDefinition(connection, null));
    }

    @Test
    public void testIndependentEffectiveFamilies() throws Exception {
        SQLServerConnection connection = connection(false, 10);
        Parameter parameter = new Parameter(false, true);
        set(parameter, connection, JDBCType.VARCHAR, repeated(21), null);
        assertEquals("varchar(21)", parameter.getTypeDefinition(connection, null));
        set(parameter, connection, JDBCType.NVARCHAR, repeated(13), null);
        assertEquals("nvarchar(13)", parameter.getTypeDefinition(connection, null));
        set(parameter, connection, JDBCType.VARBINARY, new byte[7], null);
        assertEquals("varbinary(10)", parameter.getTypeDefinition(connection, null));
        set(parameter, connection, JDBCType.CHAR, "x", null);
        assertEquals("varchar(21)", parameter.getTypeDefinition(connection, null));
    }

    @Test
    public void testExplicitHintsBypassHistoryAndFloor() throws Exception {
        SQLServerConnection connection = connection(false, 100);
        Parameter parameter = new Parameter(false, true);
        set(parameter, connection, JDBCType.VARCHAR, "x", 5);
        assertEquals("varchar(5)", parameter.getTypeDefinition(connection, null));
        set(parameter, connection, JDBCType.VARCHAR, repeated(213), 5);
        assertEquals("varchar(213)", parameter.getTypeDefinition(connection, null));
        set(parameter, connection, JDBCType.VARCHAR, "x", null);
        assertEquals("varchar(100)", parameter.getTypeDefinition(connection, null));
        parameter.setDefineParameterTypeSqlType(java.sql.Types.VARCHAR);
        parameter.setDefineParameterTypeLengthHint(7);
        set(parameter, connection, JDBCType.VARCHAR, "x", 3);
        assertEquals("varchar(7)", parameter.getTypeDefinition(connection, null));
        set(parameter, connection, JDBCType.VARCHAR, repeated(8), null);
        assertThrows(SQLServerException.class, () -> parameter.getTypeDefinition(connection, null));
    }

    @ParameterizedTest
    @ValueSource(ints = {0, -1})
    public void testUnusableExplicitHintFallsBackToDefault(int hint) throws Exception {
        SQLServerConnection connection = connection(false, 10);
        Parameter parameter = new Parameter(false, true);
        set(parameter, connection, JDBCType.NVARCHAR, repeated(13), hint);
        assertEquals("nvarchar(13)", parameter.getTypeDefinition(connection, null));
    }

    @ParameterizedTest
    @EnumSource(value = JDBCType.class, names = {"VARCHAR", "NVARCHAR", "VARBINARY"})
    public void testLargeDataAndSnapshotMaxHistory(JDBCType type) throws Exception {
        SQLServerConnection connection = connection(false, 10);
        Parameter parameter = new Parameter(false, true);
        int largeLength = type == JDBCType.NVARCHAR ? 4001 : 8001;
        set(parameter, connection, type, type.isBinary() ? new byte[largeLength] : repeated(largeLength), null);
        Parameter large = parameter.cloneForBatch();
        set(parameter, connection, type, type.isBinary() ? new byte[1] : "x", null);
        Parameter small = parameter.cloneForBatch();
        String family = type.toString().toLowerCase(java.util.Locale.ENGLISH);
        assertEquals(family + "(max)", large.getTypeDefinition(connection, null));
        assertEquals(family + "(max)", small.getTypeDefinition(connection, null));
    }

    @Test
    public void testOutputParametersRetainExistingSizing() throws Exception {
        SQLServerConnection connection = connection(false, 10);
        Parameter output = new Parameter(false, true);
        set(output, connection, JDBCType.NVARCHAR, "x", null);
        assertEquals("nvarchar(10)", output.getTypeDefinition(connection, null));
        output.registerForOutput(JDBCType.NVARCHAR, connection);
        assertEquals("nvarchar(4000)", output.getTypeDefinition(connection, null));
        set(output, connection, JDBCType.NVARCHAR, "x", null);
        assertEquals("nvarchar(4000)", output.getTypeDefinition(connection, null));
        assertEquals(10, output.getLengthHistory().resolveLength(JDBCType.NVARCHAR, 10, 0));
    }

    @ParameterizedTest
    @EnumSource(value = JDBCType.class, names = {"CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "BINARY", "VARBINARY"})
    public void testAEDeclarationPassesPreserveLegacySizing(JDBCType type) throws Exception {
        SQLServerConnection connection = connection(false, 10);
        Parameter parameter = new Parameter(true, true);
        Parameter legacy = new Parameter(true);
        int largeLength = type == JDBCType.NCHAR || type == JDBCType.NVARCHAR ? 4001 : 8001;
        for (int length : new int[] {1, 13, 2, largeLength, 2}) {
            Object value = type.isBinary() ? new byte[length] : repeated(length);
            set(parameter, connection, type, value, null);
            set(legacy, connection, type, value, null);
            Parameter row = parameter.cloneForBatch();
            Parameter legacyRow = legacy.cloneForBatch();
            assertEquals(legacyRow.getTypeDefinition(connection, null), row.getTypeDefinition(connection, null));
            row.renewDefinition = legacyRow.renewDefinition = true;
            assertEquals(legacyRow.getTypeDefinition(connection, null), row.getTypeDefinition(connection, null));
            assertNull(row.getLengthHistory());
        }
        assertNull(parameter.getLengthHistory());
    }

    @Test
    public void testExplicitUnboundedTypesAndMaxHintsDoNotGrowDefaultHistory() throws Exception {
        SQLServerConnection connection = connection(false, 10);
        Parameter parameter = new Parameter(false, true);
        set(parameter, connection, JDBCType.LONGNVARCHAR, repeated(31), null);
        assertEquals("nvarchar(max)", parameter.cloneForBatch().getTypeDefinition(connection, null));
        set(parameter, connection, JDBCType.NVARCHAR, repeated(51), 5000);
        assertEquals("nvarchar(max)", parameter.cloneForBatch().getTypeDefinition(connection, null));
        set(parameter, connection, JDBCType.NVARCHAR, "x", null);
        assertEquals("nvarchar(10)", parameter.getTypeDefinition(connection, null));
    }

    @Test
    public void testStreamsAndMissingCollationUseLegacySizing() throws Exception {
        SQLServerConnection connection = connection(false, 10);
        Parameter parameter = new Parameter(false, true);
        parameter.setValue(JDBCType.VARBINARY, new ByteArrayInputStream(new byte[20]), JavaType.INPUTSTREAM,
                new StreamSetterArgs(StreamType.BINARY, 20), null, null, null, connection, false,
                SQLServerStatementColumnEncryptionSetting.DISABLED, 1, "SELECT ?", null, false);
        assertEquals("varbinary(8000)", parameter.getTypeDefinition(connection, null));
        assertEquals(10, parameter.getLengthHistory().resolveLength(JDBCType.VARBINARY, 10, 0));
        when(connection.getDatabaseCollation()).thenReturn(null);
        set(parameter, connection, JDBCType.VARCHAR, "x", null);
        assertEquals("varchar(8000)", parameter.getTypeDefinition(connection, null));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDirectSqlDoesNotGrowHistoryBeforeRpcRestoration(boolean unicode) throws Exception {
        SQLServerConnection connection = connection(unicode, 10);
        Parameter parameter = new Parameter(false, true);
        parameter.setValue(JDBCType.VARCHAR, repeated(31), JavaType.STRING, null, null, null, null, connection, false,
                SQLServerStatementColumnEncryptionSetting.DISABLED, 1, "SELECT ?", null, true);
        parameter.getTypeDefinition(connection, null);
        assertEquals(10, parameter.getLengthHistory().resolveLength(JDBCType.NVARCHAR, 10, 0));
        assertEquals(10, parameter.getLengthHistory().resolveLength(JDBCType.VARCHAR, 10, 0));
        parameter.restoreRpcType(connection);
        assertEquals(unicode ? "nvarchar(31)" : "varchar(31)", parameter.getTypeDefinition(connection, null));
        if (!unicode) {
            set(parameter, connection, JDBCType.NVARCHAR, "x", null);
            assertEquals("nvarchar(10)", parameter.getTypeDefinition(connection, null));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testDirectSqlMaxObservationIsSharedOnlyAfterRpcRestoration(boolean unicode) throws Exception {
        SQLServerConnection connection = connection(unicode, 10);
        for (JDBCType type : new JDBCType[] {JDBCType.VARCHAR, JDBCType.NVARCHAR, JDBCType.VARBINARY}) {
            Parameter parameter = new Parameter(false, true);
            boolean national = type == JDBCType.NVARCHAR || type == JDBCType.VARCHAR && unicode;
            JDBCType effectiveType = national ? JDBCType.NVARCHAR : type;
            int length = national ? 4001 : 8001;
            Object value = type.isBinary() ? new byte[length] : repeated(length);
            parameter.setValue(type, value, JavaType.of(value), null, null, null, null, connection, false,
                    SQLServerStatementColumnEncryptionSetting.DISABLED, 1, "SELECT ?", null, true);
            Parameter large = parameter.cloneForBatch();
            // Single execution resolves declarations before discovering that literal substitution is unavailable.
            large.getTypeDefinition(connection, null);
            assertEquals(10, parameter.getLengthHistory().resolveLength(effectiveType, 10, 0));
            set(parameter, connection, type, type.isBinary() ? new byte[1] : "x", null);
            Parameter small = parameter.cloneForBatch();
            large.restoreRpcType(connection);
            String family = effectiveType.toString().toLowerCase(java.util.Locale.ENGLISH);
            assertEquals(family + "(max)", large.getTypeDefinition(connection, null));
            assertEquals(family + "(max)", small.getTypeDefinition(connection, null));
        }
    }

    @Test
    public void testRpcRestorationUsesEncodedByteLengthAndPreservesHints() throws Exception {
        SQLServerConnection connection = connection(false, 1);
        Parameter parameter = new Parameter(false, true);
        parameter.setValue(JDBCType.VARCHAR, "\uD83D\uDE00", JavaType.STRING, null, null, null, null, connection, false,
                SQLServerStatementColumnEncryptionSetting.DISABLED, 1, "SELECT ?", null, true);
        parameter.getTypeDefinition(connection, null);
        parameter.restoreRpcType(connection);
        assertEquals("varchar(4)", parameter.getTypeDefinition(connection, null));
        assertEquals(1, parameter.getLengthHistory().resolveLength(JDBCType.NVARCHAR, 1, 0));

        parameter.setValue(JDBCType.VARCHAR, repeated(31), JavaType.STRING, null, null, null, 5, connection, false,
                SQLServerStatementColumnEncryptionSetting.DISABLED, 1, "SELECT ?", null, true);
        parameter.getTypeDefinition(connection, null);
        parameter.restoreRpcType(connection);
        assertEquals(Integer.valueOf(5), parameter.getInputDTV().getScale());
        assertEquals("varchar(31)", parameter.getTypeDefinition(connection, null));
        assertEquals(4, parameter.getLengthHistory().resolveLength(JDBCType.VARCHAR, 1, 0));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRpcFallbackUnusableHintsNullsAndUnboundedTypes(boolean unicode) throws Exception {
        SQLServerConnection connection = connection(unicode, 10);
        for (Integer hint : new Integer[] {null, 0, -1}) {
            Parameter parameter = new Parameter(false, true);
            parameter.setValue(JDBCType.NVARCHAR, repeated(13), JavaType.STRING, null, null, null, hint, connection,
                    false, SQLServerStatementColumnEncryptionSetting.DISABLED, 1, "SELECT ?", null, true);
            parameter.getTypeDefinition(connection, null);
            parameter.restoreRpcType(connection);
            assertEquals("nvarchar(13)", parameter.getTypeDefinition(connection, null));
            set(parameter, connection, JDBCType.NVARCHAR, null, null);
            assertEquals("nvarchar(13)", parameter.getTypeDefinition(connection, null));
        }
        Parameter parameter = new Parameter(false, true);
        parameter.setValue(JDBCType.LONGNVARCHAR, repeated(31), JavaType.STRING, null, null, null, null, connection,
                false, SQLServerStatementColumnEncryptionSetting.DISABLED, 1, "SELECT ?", null, true);
        Parameter row = parameter.cloneForBatch();
        row.getTypeDefinition(connection, null);
        row.restoreRpcType(connection);
        assertEquals("nvarchar(max)", row.getTypeDefinition(connection, null));
        assertEquals(10, parameter.getLengthHistory().resolveLength(JDBCType.NVARCHAR, 10, 0));
        set(parameter, connection, JDBCType.NVARCHAR, "", null);
        assertEquals("nvarchar(10)", parameter.getTypeDefinition(connection, null));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testRpcFallbackPreservesAEAndOutputExclusions(boolean ae) throws Exception {
        SQLServerConnection connection = connection(true, 10);
        Parameter parameter = new Parameter(ae, true);
        Parameter legacy = new Parameter(ae);
        for (Parameter input : new Parameter[] {parameter, legacy}) {
            if (!ae) {
                input.registerForOutput(JDBCType.NVARCHAR, connection);
            }
            input.setValue(JDBCType.NVARCHAR, repeated(31), JavaType.STRING, null, null, null, null, connection, false,
                    SQLServerStatementColumnEncryptionSetting.DISABLED, 1, "SELECT ?", null, true);
            input.getTypeDefinition(connection, null);
            input.restoreRpcType(connection);
        }
        assertEquals(legacy.getTypeDefinition(connection, null), parameter.getTypeDefinition(connection, null));
        if (ae) {
            assertNull(parameter.getLengthHistory());
        } else {
            assertEquals(10, parameter.getLengthHistory().resolveLength(JDBCType.NVARCHAR, 10, 0));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testJsonBindingRetainsLegacySizingWithoutCharacterHistory(boolean direct) throws Exception {
        SQLServerConnection connection = connection(true, 10);
        for (String value : new String[] {null, "{}", "{\"v\":\"" + repeated(4001) + "\"}"}) {
            Parameter parameter = new Parameter(false, true);
            Parameter legacy = new Parameter(false);
            for (Parameter input : new Parameter[] {parameter, legacy}) {
                input.setValue(JDBCType.JSON, value, null == value ? JavaType.OBJECT : JavaType.STRING, null, null,
                        null, null, connection, false, SQLServerStatementColumnEncryptionSetting.DISABLED, 1,
                        "SELECT ?", null, direct);
                input.getTypeDefinition(connection, null);
                if (direct) {
                    input.restoreRpcType(connection);
                }
            }
            assertEquals(legacy.getTypeDefinition(connection, null), parameter.getTypeDefinition(connection, null));
            assertEquals(10, parameter.getLengthHistory().resolveLength(JDBCType.NVARCHAR, 10, 0));
            assertEquals(10, parameter.getLengthHistory().resolveLength(JDBCType.VARCHAR, 10, 0));
        }
    }

    private static SQLServerConnection connection(boolean unicode, int floor) throws SQLServerException {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        SQLCollation collation = mock(SQLCollation.class);
        when(collation.getCharset()).thenReturn(StandardCharsets.UTF_8);
        when(connection.getDatabaseCollation()).thenReturn(collation);
        when(connection.sendStringParametersAsUnicode()).thenReturn(unicode);
        when(connection.getStringParameterDefaultLength(any(JDBCType.class))).thenAnswer(invocation -> {
            JDBCType type = invocation.getArgument(0);
            switch (type) {
                case CHAR:
                case VARCHAR:
                case BINARY:
                case VARBINARY:
                    return Math.min(floor, 8000);
                case NCHAR:
                case NVARCHAR:
                    return Math.min(floor, 4000);
                default:
                    return 0;
            }
        });
        return connection;
    }

    private static void set(Parameter parameter, SQLServerConnection connection, JDBCType type, Object value,
            Integer hint) throws SQLServerException {
        parameter.setValue(type, value, null == value ? JavaType.OBJECT : JavaType.of(value), null, null, null, hint,
                connection, false, SQLServerStatementColumnEncryptionSetting.DISABLED, 1, "SELECT ?", null, false);
    }

    private static String repeated(int length) {
        char[] chars = new char[length];
        java.util.Arrays.fill(chars, 'x');
        return new String(chars);
    }
}
