/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;

import org.junit.jupiter.api.Test;


/**
 * Verifies the JDBC type used by {@link Column#updateValue} when a textual value is assigned to a column. For Always
 * Encrypted columns the column's physical SSType is VARBINARY, so the type must be derived from the base type.
 */
public class ColumnAlwaysEncryptedUpdateTest {

    private static JDBCType updatedJdbcType(SSType physicalType, SSType baseType) throws Exception {
        SQLServerConnection con = mock(SQLServerConnection.class);
        when(con.sendStringParametersAsUnicode()).thenReturn(false);
        when(con.isKatmaiOrLater()).thenReturn(true);
        when(con.isColumnEncryptionSettingEnabled()).thenReturn(true);

        TypeInfo typeInfo = mock(TypeInfo.class);
        when(typeInfo.getSSType()).thenReturn(physicalType);

        CryptoMetadata cryptoMetadata = null;
        if (null != baseType) {
            TypeInfo baseTypeInfo = mock(TypeInfo.class);
            when(baseTypeInfo.getSSType()).thenReturn(baseType);
            cryptoMetadata = new CryptoMetadata(null, (short) 0, (byte) 0, null, (byte) 0, (byte) 0);
            cryptoMetadata.setBaseTypeInfo(baseTypeInfo);
        }

        Column column = new Column(typeInfo, "col", null, cryptoMetadata);
        column.updateValue(JDBCType.VARCHAR, "0a1b", JavaType.STRING, null, null, null, con,
                SQLServerStatementColumnEncryptionSetting.USE_CONNECTION_SETTING, null, false, 1);

        Field f = Column.class.getDeclaredField("updaterDTV");
        f.setAccessible(true);
        return ((DTV) f.get(column)).getJdbcType();
    }

    /** updateString on an encrypted CHAR/VARCHAR column must not be treated as a hex string for a binary column. */
    @Test
    public void testUpdateStringEncryptedVarcharIsNotRetypedAsBinary() throws Exception {
        assertEquals(JDBCType.VARCHAR, updatedJdbcType(SSType.VARBINARY, SSType.VARCHAR));
        assertEquals(JDBCType.VARCHAR, updatedJdbcType(SSType.VARBINARY, SSType.CHAR));
    }

    /** A plain (non-encrypted) binary column still takes a hex string. */
    @Test
    public void testUpdateStringBinaryColumnStillConvertedFromHex() throws Exception {
        assertEquals(JDBCType.VARBINARY, updatedJdbcType(SSType.VARBINARY, null));
    }

    /** An encrypted binary column also still takes a hex string. */
    @Test
    public void testUpdateStringEncryptedBinaryStillConvertedFromHex() throws Exception {
        assertEquals(JDBCType.VARBINARY, updatedJdbcType(SSType.VARBINARY, SSType.VARBINARY));
    }
}
