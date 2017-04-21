/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

import java.sql.JDBCType;
import java.util.BitSet;
import java.util.concurrent.ThreadLocalRandom;

import com.microsoft.sqlserver.testframework.DBCoercion;
import com.microsoft.sqlserver.testframework.DBCoercions;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBItems;

public abstract class SqlType extends DBItems {
    // TODO: add seed to generate random data -> will help to reproduce the
    // exact data for debugging
    protected String name = null;       // type name for creating SQL query
    protected JDBCType jdbctype = JDBCType.NULL;
    protected int precision = 0;
    protected int scale = 0;
    protected Object minvalue = null;
    protected Object maxvalue = null;
    protected Object nullvalue = null;  // Primitives have non-null defaults
    protected VariableLengthType variableLengthType;
    protected Class type = null;
    protected BitSet flags = new BitSet();
    protected DBCoercions coercions = new DBCoercions();

    public static final int DEFAULT = 0;
    public static final int NULLABLE = 1;
    public static final int UPDATABLE = 2;
    public static final int NUMERIC = 3;
    public static final int FLOATINGPOINT = 4;
    public static final int FIXED = 5;
    public static final int CREATEPARAMS = 6;
    public static final int CHARACTER = 7;
    public static final int UNICODE = 8;
    public static final int LONG = 9;
    public static final int SEARCHABLE = 10;
    public static final int XML = 11;
    public static final int UDT = 12;
    public static final int BINARY = 13;
    public static final int TEMPORAL = 14;
    public static final int BOOLEAN = 15;
    public static final int PRIMITIVE = 16;
    public static final int COLLATE = 17;
    public static final int GUID = 18;

    /**
     * 
     * @param name
     * @param jdbctype
     * @param precision
     * @param scale
     * @param min
     *            minimum allowed value for the SQL type
     * @param max
     *            maximum allowed value for the SQL type
     * @param nullvalue
     *            default null value for the SQL type
     * @param variableLengthType
     *            {@link VariableLengthType}
     */
    SqlType(String name,
            JDBCType jdbctype,
            int precision,
            int scale,
            Object min,
            Object max,
            Object nullvalue,
            VariableLengthType variableLengthType,
            Class type) {
        this.name = name;
        this.jdbctype = jdbctype;
        this.precision = precision;
        this.scale = scale;
        this.minvalue = min;
        this.maxvalue = max;
        this.nullvalue = nullvalue;
        this.variableLengthType = variableLengthType;
        this.type = type;
    }

    /**
     * 
     * @return valid random value for the SQL type
     */
    public Object createdata() {
        try {
            return null;
        }
        catch (Exception e) {
            // Make this easier to debug
            throw new Error("createdata failed: ", e);
        }
    }

    /**
     * create valid random value for the SQL type
     * @param type
     * @param data
     * @return
     */
    public Object createdata(Class type,
            byte[] data) {
        if (type == String.class)
            return new String(data);
        return data;
    }

    /**
     * 
     * @return JDBCType of SqlType object
     */
    public JDBCType getJdbctype() {
        return jdbctype;
    }

    /**
     * 
     * @return
     */
    public Class getType() {
        return type;
    }

    /**
     * 
     * @param jdbctype
     *            set JDBCType of SqlType object
     */
    public void setJdbctype(JDBCType jdbctype) {
        this.jdbctype = jdbctype;
    }

    /**
     * 
     * @return precision set for the SqlType
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * 
     * @param precision
     *            set precision for SqlType
     */
    public void setPrecision(int precision) {
        this.precision = precision;
    }

    /**
     * 
     * @return scale set for the SqlType
     */
    public int getScale() {
        return scale;
    }

    /**
     * 
     * @param scale
     *            set precision for SqlType
     */
    public void setScale(int scale) {
        this.scale = scale;
    }

    /**
     * 
     * @return string value of SqlType
     */
    public String getName() {
        return name;
    }

    /**
     * 
     * @return null value for the SqlType
     */
    public Object getNullvalue() {
        return nullvalue;
    }

    /**
     * 
     * @return minimum allowed value for the SQL type
     */
    public Object getMinvalue() {
        return minvalue;
    }

    /**
     * 
     * @return maximum allowed value for the SQL type
     */
    public Object getMaxvalue() {
        return maxvalue;
    }

    /**
     * 
     * @return variableLengthType {@link VariableLengthType}
     */
    public Object getVariableLengthType() {
        return variableLengthType;
    }

    /**
     * generates random precision for SQL types with precision
     */
    void generatePrecision() {
        int minPrecision = 1;
        int maxPrecision = this.precision;
        this.precision = ThreadLocalRandom.current().nextInt(minPrecision, maxPrecision + 1);
    }
    
    /**
     * generates random precision for SQL types with scale
     */
    void generateScale() {
        int minScale = 1;
        int maxScale = this.scale;
        this.scale = ThreadLocalRandom.current().nextInt(minScale, maxScale + 1);
    }
    
    /**
     * @return
     */
    public boolean isString() {
        return flags.get(CHARACTER);
    }

    /**
     * 
     * @param target
     * @param flag
     * @param conn
     * @return
     * @throws Exception
     */
    public boolean canConvert(Class target,
            int flag,
            DBConnection conn) throws Exception {
        double serverversion = conn.getServerVersion();

        if (flag == DBCoercion.SET || flag == DBCoercion.SETOBJECT || flag == DBCoercion.UPDATE || flag == DBCoercion.UPDATEOBJECT
                || flag == DBCoercion.REG) {
            // SQL 8 does not allow conversion from string to money
            if (flag != DBCoercion.SETOBJECT && serverversion < 9.0 && this instanceof SqlMoney && target == String.class)
                return false;
            if (flag == DBCoercion.SET || flag == DBCoercion.SETOBJECT) {
                // setTemporal() on textual columns returns unverifiable format
                if (this.isString() && (target == java.sql.Date.class || target == java.sql.Time.class || target == java.sql.Timestamp.class))
                    return false;
            }
        }

        DBCoercion coercion = coercions.find(target);
        if (coercion != null)
            return coercion.flags().get(flag);

        return false;
    }

}