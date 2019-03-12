/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework.sqlType;

import java.sql.Clob;
import java.sql.JDBCType;
import java.sql.NClob;

import com.microsoft.sqlserver.testframework.DBCoercion;
import com.microsoft.sqlserver.testframework.DBConstants;


public class SqlNVarCharMax extends SqlNVarChar {

    public SqlNVarCharMax() {
        super();
        name = "nvarchar(max)";
        jdbctype = JDBCType.LONGNVARCHAR;
        variableLengthType = VariableLengthType.Variable;

        coercions.add(new DBCoercion(Clob.class,
                new int[] {DBConstants.GET_COERCION, DBConstants.UPDATE_COERCION, DBConstants.UPDATEOBJECT_COERCION,
                        DBConstants.SET_COERCION, DBConstants.SETOBJECT_COERCION, DBConstants.GETPARAM_COERCION,
                        DBConstants.REG_COERCION, DBConstants.CHAR_COERCION}));
        coercions.add(new DBCoercion(NClob.class,
                new int[] {DBConstants.GET_COERCION, DBConstants.UPDATE_COERCION, DBConstants.UPDATEOBJECT_COERCION,
                        DBConstants.SET_COERCION, DBConstants.SETOBJECT_COERCION, DBConstants.GETPARAM_COERCION,
                        DBConstants.REG_COERCION, DBConstants.NCHAR_COERCION}));

    }

}
