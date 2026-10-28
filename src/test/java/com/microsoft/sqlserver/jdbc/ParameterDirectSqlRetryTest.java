/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.io.StringReader;
import java.nio.charset.Charset;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;


class ParameterDirectSqlRetryTest {
    private static SQLServerConnection connection(boolean unicode) throws SQLServerException {
        SQLServerConnection con = mock(SQLServerConnection.class);
        SQLCollation collation = mock(SQLCollation.class);
        when(collation.getCharset()).thenReturn(Charset.forName("windows-1252"));
        when(con.getDatabaseCollation()).thenReturn(collation);
        when(con.sendStringParametersAsUnicode()).thenReturn(unicode);
        return con;
    }

    private static Parameter parameter(SQLServerConnection con, JDBCType type, Object value, JavaType javaType,
            StreamSetterArgs streamArgs, boolean direct) throws SQLServerException {
        Parameter param = new Parameter(false);
        param.setValue(type, value, javaType, streamArgs, null, null, null, con, false,
                SQLServerStatementColumnEncryptionSetting.DISABLED, 1, "SELECT ?", null, direct);
        return param;
    }

    @ParameterizedTest
    @EnumSource(value = JDBCType.class,
            names = {"CHAR", "VARCHAR", "LONGVARCHAR", "CLOB", "NCHAR", "NVARCHAR", "LONGNVARCHAR", "NCLOB"})
    void testRestoredStringMatchesRpcBinding(JDBCType type) throws SQLServerException {
        SQLServerConnection con = connection(false);
        Parameter direct = parameter(con, type, "abc\u20ac", JavaType.STRING, null, true);
        Parameter rpc = parameter(con, type, "abc\u20ac", JavaType.STRING, null, false);
        direct.getTypeDefinition(con, null);

        direct.restoreRpcType(con);
        assertEquals(rpc.getJdbcType(), direct.getJdbcType());
        assertEquals(rpc.getInputDTV().getJavaType(), direct.getInputDTV().getJavaType());
        assertEquals(rpc.getTypeDefinition(con, null), direct.getTypeDefinition(con, null));
        if (rpc.getSetterValue() instanceof byte[]) {
            assertArrayEquals((byte[]) rpc.getSetterValue(), (byte[]) direct.getSetterValue());
        } else {
            assertEquals(rpc.getSetterValue(), direct.getSetterValue());
        }
        assertFalse(direct.getInputDTV().sendStringParametersAsUnicode);
        DTV restored = direct.getInputDTV();
        direct.restoreRpcType(con);
        assertSame(restored, direct.getInputDTV());
    }

    @Test
    void testReaderUsesRpcEncodingConversion() throws Exception {
        SQLServerConnection con = connection(false);
        Parameter param = parameter(con, JDBCType.VARCHAR, new StringReader("a\u20ac"), JavaType.READER,
                new StreamSetterArgs(StreamType.CHARACTER, 2), true);
        param.restoreRpcType(con);

        assertEquals(JDBCType.VARCHAR, param.getJdbcType());
        assertEquals(JavaType.INPUTSTREAM, param.getInputDTV().getJavaType());
        assertEquals(DataTypes.UNKNOWN_STREAM_LENGTH, param.getInputDTV().getStreamSetterArgs().getLength());
        try (InputStream stream = (InputStream) param.getSetterValue()) {
            byte[] bytes = new byte[2];
            assertEquals(2, stream.read(bytes));
            assertArrayEquals(new byte[] {'a', (byte) 0x80}, bytes);
            assertEquals(-1, stream.read());
        }
    }

    @Test
    void testRestoringBatchCloneDoesNotMutateSharedValue() throws SQLServerException {
        SQLServerConnection con = connection(false);
        Parameter original = parameter(con, JDBCType.VARCHAR, "abc", JavaType.STRING, null, true);
        Parameter batch = original.cloneForBatch();
        batch.restoreRpcType(con);

        assertEquals(JDBCType.NVARCHAR, original.getJdbcType());
        assertEquals("abc", original.getSetterValue());
        original.restoreRpcType(con);
        assertEquals(JDBCType.VARCHAR, original.getJdbcType());
        assertArrayEquals((byte[]) batch.getSetterValue(), (byte[]) original.getSetterValue());
    }

    @Test
    void testNullRetainsOriginalType() throws SQLServerException {
        SQLServerConnection con = connection(false);
        Parameter param = parameter(con, JDBCType.VARCHAR, null, JavaType.OBJECT, null, true);
        param.restoreRpcType(con);
        assertEquals(JDBCType.VARCHAR, param.getJdbcType());
        assertNull(param.getSetterValue());
    }

    @Test
    void testUnicodeConnectionRetainsPromotedValue() throws SQLServerException {
        SQLServerConnection con = connection(true);
        Parameter param = parameter(con, JDBCType.VARCHAR, "abc", JavaType.STRING, null, true);
        DTV value = param.getInputDTV();
        param.restoreRpcType(con);
        assertSame(value, param.getInputDTV());
        assertEquals(JDBCType.NVARCHAR, param.getJdbcType());
    }
}
