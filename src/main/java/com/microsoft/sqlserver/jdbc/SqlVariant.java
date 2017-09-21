/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;

/**
 * This class holds information regarding the basetype of a sql_variant data.
 *
 */

/**
 * Enum for valid probBytes for different TDSTypes
 *
 */
enum sqlVariantProbBytes {
    INTN(0),
    INT8(0),
    INT4(0),
    INT2(0),
    INT1(0),
    FLOAT4(0),
    FLOAT8(0),
    DATETIME4(0),
    DATETIME8(0),
    MONEY4(0),
    MONEY8(0),
    BITN(0),
    GUID(0),
    DATEN(0),
    TIMEN(1),
    DATETIME2N(1),
    DECIMALN(2),
    NUMERICN(2),
    BIGBINARY(2),
    BIGVARBINARY(2),
    BIGCHAR(7),
    BIGVARCHAR(7),
    NCHAR(7),
    NVARCHAR(7);

    private final int intValue;

    private static final int MAXELEMENTS = 23;
    private static final sqlVariantProbBytes valuesTypes[] = new sqlVariantProbBytes[MAXELEMENTS];

    private sqlVariantProbBytes(int intValue) {
        this.intValue = intValue;
    }

    int getIntValue() {
        return intValue;
    }

    static sqlVariantProbBytes valueOf(int intValue) {
        sqlVariantProbBytes tdsType;

        if (!(0 <= intValue && intValue < valuesTypes.length) || null == (tdsType = valuesTypes[intValue])) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unknownSSType"));
            Object[] msgArgs = {Integer.valueOf(intValue)};
            throw new IllegalArgumentException(form.format(msgArgs));
        }

        return tdsType;
    }

}

public class SqlVariant {

    private int baseType;
    private int precision;
    private int scale;
    private int maxLength;  // for Character basetypes in sqlVariant
    private SQLCollation collation; // for Character basetypes in sqlVariant
    private boolean isBaseTypeTime = false;  // we need this when we need to read time as timestamp (for instance in bulkcopy)
    private JDBCType baseJDBCType;

    /**
     * Constructor for sqlVariant
     */
    SqlVariant(int baseType) {
        this.baseType = baseType;
    }

    /**
     * Check if the basetype for variant is of time value
     * 
     * @return
     */
    boolean isBaseTypeTimeValue() {
        return this.isBaseTypeTime;
    }

    void setIsBaseTypeTimeValue(boolean isBaseTypeTime) {
        this.isBaseTypeTime = isBaseTypeTime;
    }

    /**
     * store the base type for sql-variant
     * 
     * @param baseType
     */
    void setBaseType(int baseType) {
        this.baseType = baseType;
    }

    /**
     * retrieves the base type for sql-variant
     * 
     * @return
     */
    int getBaseType() {
        return this.baseType;
    }

    /**
     * Store the basetype as jdbc type
     * 
     * @param baseJDBCType
     */
    void setBaseJDBCType(JDBCType baseJDBCType) {
        this.baseJDBCType = baseJDBCType;
    }

    /**
     * retrieves the base type as jdbc type
     * 
     * @return
     */
    JDBCType getBaseJDBCType() {
        return this.baseJDBCType;
    }

    /**
     * stores the scale if applicable
     * 
     * @param scale
     */
    void setScale(int scale) {
        this.scale = scale;
    }

    /**
     * retrieves the scale
     * 
     * @return
     */
    int getScale() {
        return this.scale;
    }

    /**
     * stores the precision if applicable
     * 
     * @param precision
     */
    void setPrecision(int precision) {
        this.precision = precision;
    }

    /**
     * retrieves the precision
     * 
     * @return
     */
    int getPrecision() {
        return this.precision;
    }

    /**
     * stores the collation if applicable
     * 
     * @param collation
     */
    void setCollation(SQLCollation collation) {
        this.collation = collation;
    }

    /**
     * Retrieves the collation
     * 
     * @return
     */
    SQLCollation getCollation() {
        return this.collation;
    }

    /**
     * stores the maximum length
     * 
     * @param maxLength
     */
    void setMaxLength(int maxLength) {
        this.maxLength = maxLength;
    }

    /**
     * retrieves the maximum length
     * 
     * @return
     */
    int getMaxLength() {
        return this.maxLength;
    }
}
