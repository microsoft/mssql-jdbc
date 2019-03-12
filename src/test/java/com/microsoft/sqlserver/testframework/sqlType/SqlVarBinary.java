/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

import java.sql.JDBCType;

import com.microsoft.sqlserver.jdbc.TestUtils.DBBinaryStream;
import com.microsoft.sqlserver.jdbc.TestUtils.DBCharacterStream;
import com.microsoft.sqlserver.testframework.DBCoercion;
import com.microsoft.sqlserver.testframework.DBConstants;


/**
 * Contains name, jdbctype, precision, scale for varbinary data type
 */
public class SqlVarBinary extends SqlBinary {

    /**
     * set JDBCType and precision for SqlVarBinary
     */
    public SqlVarBinary() {
        super("varbinary", JDBCType.VARBINARY, 4000);
        coercions
                .add(new DBCoercion(String.class, new int[] {DBConstants.GET_COERCION, DBConstants.GETPARAM_COERCION}));
        coercions.add(new DBCoercion(Object.class,
                new int[] {DBConstants.GET_COERCION, DBConstants.UPDATE_COERCION, DBConstants.UPDATEOBJECT_COERCION,
                        DBConstants.SET_COERCION, DBConstants.SETOBJECT_COERCION, DBConstants.GETPARAM_COERCION,
                        DBConstants.REG_COERCION}));
        coercions.add(new DBCoercion(byte[].class,
                new int[] {DBConstants.GET_COERCION, DBConstants.UPDATE_COERCION, DBConstants.UPDATEOBJECT_COERCION,
                        DBConstants.SET_COERCION, DBConstants.SETOBJECT_COERCION, DBConstants.GETPARAM_COERCION,
                        DBConstants.REG_COERCION}));

        // TODO: Following coercions are not supported by AE. add a check later
        // coercions.remove(String.class);
        coercions.add(new DBCoercion(String.class, new int[] {DBConstants.GET_COERCION, DBConstants.UPDATE_COERCION,
                DBConstants.UPDATEOBJECT_COERCION, DBConstants.SETOBJECT_COERCION, DBConstants.GETPARAM_COERCION}));
        coercions.add(new DBCoercion(DBBinaryStream.class,
                new int[] {DBConstants.GET_COERCION, DBConstants.UPDATE_COERCION, DBConstants.UPDATEOBJECT_COERCION,
                        DBConstants.SET_COERCION, DBConstants.SETOBJECT_COERCION, DBConstants.GETPARAM_COERCION,
                        DBConstants.REG_COERCION, DBConstants.STREAM_COERCION}));
        coercions.add(new DBCoercion(DBCharacterStream.class,
                new int[] {DBConstants.GET_COERCION, DBConstants.UPDATE_COERCION, DBConstants.UPDATEOBJECT_COERCION,
                        DBConstants.SETOBJECT_COERCION, DBConstants.GETPARAM_COERCION, DBConstants.REG_COERCION,
                        DBConstants.STREAM_COERCION}));

    }
}
