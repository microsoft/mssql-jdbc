// ---------------------------------------------------------------------------------------------------------------------------------
// File: SqlType.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"),
// to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense,
// and / or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.
// ---------------------------------------------------------------------------------------------------------------------------------

package com.microsoft.sqlserver.testframework.sqlType;

import java.sql.JDBCType;
import java.util.Random;
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
    static Random r = new Random();
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
    SqlType(String name, JDBCType jdbctype, int precision, int scale, Object min, Object max,
            Object nullvalue, VariableLengthType variableLengthType) {
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
    
    /**
     * generate char or nchar values
     * @param columnLength
     * @param charSet
     * @return
     */
    protected static String buildCharOrNChar(int columnLength, String charSet) {

        int length;
        
            int columnLengthInt = columnLength;
            length = columnLengthInt;
            return buildRandomString(length, charSet);
        
    }
    
    private static String buildRandomString(int length, String charSet) {
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < length; i++) {
            char c = pickRandomChar(charSet);
            sb.append(c);
        }
        return sb.toString();
    }

    private static char pickRandomChar(String charSet) {
        int charSetLength = charSet.length();
        int randomIndex = r.nextInt(charSetLength);
        return charSet.charAt(randomIndex);
    }
}
