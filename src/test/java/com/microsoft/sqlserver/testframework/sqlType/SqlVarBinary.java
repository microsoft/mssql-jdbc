/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

import java.sql.JDBCType;

import com.microsoft.sqlserver.testframework.DBCoercion;
import com.microsoft.sqlserver.testframework.Utils;
import com.microsoft.sqlserver.testframework.Utils.DBBinaryStream;
import com.microsoft.sqlserver.testframework.Utils.DBCharacterStream;

/**
 * Contains name, jdbctype, precision, scale for varbinary data type
 */
public class SqlVarBinary extends SqlBinary {

    /**
     * set JDBCType and precision for SqlVarBinary
     */
    public SqlVarBinary() {
        super("varbinary", JDBCType.VARBINARY, 4000);
        coercions.add(new DBCoercion(String.class, new int[] {DBCoercion.GET, DBCoercion.GETPARAM}));
        coercions.add(new DBCoercion(Object.class, new int[] {DBCoercion.GET, DBCoercion.UPDATE, DBCoercion.UPDATEOBJECT, DBCoercion.SET,
                DBCoercion.SETOBJECT, DBCoercion.GETPARAM, DBCoercion.REG}));
        coercions.add(new DBCoercion(byte[].class, new int[] {DBCoercion.GET, DBCoercion.UPDATE, DBCoercion.UPDATEOBJECT, DBCoercion.SET,
                DBCoercion.SETOBJECT, DBCoercion.GETPARAM, DBCoercion.REG}));

        // TODO: Following coercions are not supported by AE. add a check later
        // coercions.remove(String.class);
        coercions.add(new DBCoercion(String.class,
                new int[] {DBCoercion.GET, DBCoercion.UPDATE, DBCoercion.UPDATEOBJECT, DBCoercion.SETOBJECT, DBCoercion.GETPARAM}));
        coercions.add(new DBCoercion(DBBinaryStream.class, new int[] {DBCoercion.GET, DBCoercion.UPDATE, DBCoercion.UPDATEOBJECT, DBCoercion.SET,
                DBCoercion.SETOBJECT, DBCoercion.GETPARAM, DBCoercion.REG, DBCoercion.STREAM}));
        coercions.add(new DBCoercion(DBCharacterStream.class, new int[] {DBCoercion.GET, DBCoercion.UPDATE, DBCoercion.UPDATEOBJECT,
                DBCoercion.SETOBJECT, DBCoercion.GETPARAM, DBCoercion.REG, DBCoercion.STREAM}));

    }
}