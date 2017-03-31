/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

import java.sql.JDBCType;
import java.util.concurrent.ThreadLocalRandom;

import com.microsoft.sqlserver.testframework.DBCoercion;
import com.microsoft.sqlserver.testframework.Utils;

/*
 * Restricting the size of char/binary to 2000 and nchar to 1000 to accommodate SQL Sever limitation of having of having maximum allowable table row
 * size to 8060
 */
public class SqlChar extends SqlType {

    private static String normalCharSet = "1234567890-=!@#$%^&*()_+qwertyuiop[]\\asdfghjkl;zxcvbnm,./QWERTYUIOP{}|ASDFGHJKL:\"ZXCVBNM<>?";

    public SqlChar() {
        this("char", JDBCType.CHAR, 2000);
    }

    SqlChar(String name,
            JDBCType jdbctype,
            int precision) {
        super(name, jdbctype, precision, 0, SqlTypeValue.CHAR.minValue, SqlTypeValue.CHAR.maxValue, SqlTypeValue.CHAR.nullValue,
                VariableLengthType.Precision, String.class);
        generatePrecision();
        coercions.add(new DBCoercion(Object.class, new int[] {DBCoercion.GET, DBCoercion.UPDATE, DBCoercion.UPDATEOBJECT, DBCoercion.SET,
                DBCoercion.SETOBJECT, DBCoercion.GETPARAM, DBCoercion.REG}));
        coercions.add(new DBCoercion(String.class, new int[] {DBCoercion.GET, DBCoercion.UPDATE, DBCoercion.UPDATEOBJECT, DBCoercion.SET,
                DBCoercion.SETOBJECT, DBCoercion.GETPARAM, DBCoercion.REG, DBCoercion.CHAR}));
        coercions.add(new DBCoercion(Utils.DBCharacterStream.class, new int[] {DBCoercion.GET, DBCoercion.UPDATE, DBCoercion.UPDATEOBJECT,
                DBCoercion.SET, DBCoercion.SETOBJECT, DBCoercion.GETPARAM, DBCoercion.REG, DBCoercion.STREAM, DBCoercion.CHAR}));
    }

    public Object createdata() {
        int dataLength = ThreadLocalRandom.current().nextInt(precision);
        return generateCharTypes(dataLength);
    }

    private static String generateCharTypes(int columnLength) {
        String charSet = normalCharSet;
        return buildCharOrNChar(columnLength, charSet);
    }

    /**
     * generate char or nchar values
     * 
     * @param columnLength
     * @param charSet
     * @return
     */
    protected static String buildCharOrNChar(int columnLength,
            String charSet) {
        int columnLengthInt = columnLength;
        return buildRandomString(columnLengthInt, charSet);
    }

    private static String buildRandomString(int length,
            String charSet) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            char c = pickRandomChar(charSet);
            sb.append(c);
        }
        return sb.toString();
    }

    private static char pickRandomChar(String charSet) {
        int charSetLength = charSet.length();
        int randomIndex = ThreadLocalRandom.current().nextInt(charSetLength);
        return charSet.charAt(randomIndex);
    }
}