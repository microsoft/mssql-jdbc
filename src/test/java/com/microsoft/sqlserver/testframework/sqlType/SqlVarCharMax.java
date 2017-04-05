/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework.sqlType;

import java.sql.Clob;
import java.util.ArrayList;

import com.microsoft.sqlserver.testframework.DBCoercion;
import com.microsoft.sqlserver.testframework.DBCoercions;

public class SqlVarCharMax extends SqlVarChar {

    public SqlVarCharMax() {

        super();
        name = "varchar(max)";
        jdbctype = jdbctype.LONGVARCHAR;
        variableLengthType = variableLengthType.Variable;
        coercions.add(new DBCoercion(Clob.class, new int[] {DBCoercion.GET, DBCoercion.UPDATE, DBCoercion.UPDATEOBJECT, DBCoercion.SET,
                DBCoercion.SETOBJECT, DBCoercion.GETPARAM, DBCoercion.REG, DBCoercion.CHAR}));

    }
}