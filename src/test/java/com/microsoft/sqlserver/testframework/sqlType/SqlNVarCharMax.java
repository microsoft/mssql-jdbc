/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework.sqlType;

import java.sql.Clob;
import java.sql.JDBCType;
import java.sql.NClob;

import com.microsoft.sqlserver.testframework.DBCoercion;


public class SqlNVarCharMax extends SqlNVarChar {

    public SqlNVarCharMax() {
        super();
        name = "nvarchar(max)";
        jdbctype = JDBCType.LONGNVARCHAR;
        variableLengthType = variableLengthType.Variable;

        coercions.add(new DBCoercion(Clob.class, new int[] {DBCoercion.GET, DBCoercion.UPDATE, DBCoercion.UPDATEOBJECT, DBCoercion.SET,
                DBCoercion.SETOBJECT, DBCoercion.GETPARAM, DBCoercion.REG, DBCoercion.CHAR}));
        coercions.add(new DBCoercion(NClob.class, new int[] {DBCoercion.GET, DBCoercion.UPDATE, DBCoercion.UPDATEOBJECT, DBCoercion.SET,
                DBCoercion.SETOBJECT, DBCoercion.GETPARAM, DBCoercion.REG, DBCoercion.NCHAR}));

    }

}