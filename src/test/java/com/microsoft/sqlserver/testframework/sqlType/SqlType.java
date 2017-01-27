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

public abstract class SqlType {
    // TODO: add seed to generate random data -> will help to reproduce the
    // exact data for debugging
    protected String name = null;		// type name for creating SQL query
    protected JDBCType jdbctype = JDBCType.NULL;
    protected int precision = 0;
    protected int scale = 0;
    protected Object minvalue = null;
    protected Object maxvalue = null;
    protected Object nullvalue = null;	// Primitives have non-null defaults
    protected VariableLengthType variableLengthType;
    // protected ThreadLocalRandom r;

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
            VariableLengthType variableLengthType) {
        this.name = name;
        this.jdbctype = jdbctype;
        this.precision = precision;
        this.scale = scale;
        this.minvalue = min;
        this.maxvalue = max;
        this.nullvalue = nullvalue;
        this.variableLengthType = variableLengthType;
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
     * 
     * @return JDBCType of SqlType object
     */
    public JDBCType getJdbctype() {
        return jdbctype;
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

}
