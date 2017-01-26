/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.SQLType;

/**
 * This interface requires all the CallableStatement methods including those are specific to JDBC 4.2
 *
 */
public interface ISQLServerCallableStatement42 extends ISQLServerCallableStatement, ISQLServerPreparedStatement42 {

    public void registerOutParameter(int index,
            SQLType sqlType) throws SQLServerException;

    public void registerOutParameter(int index,
            SQLType sqlType,
            String typeName) throws SQLServerException;

    public void registerOutParameter(int index,
            SQLType sqlType,
            int scale) throws SQLServerException;

    /**
     * Registers the parameter in ordinal position index to be of JDBC type sqlType. All OUT parameters must be registered before a stored procedure
     * is executed.
     * <p>
     * The JDBC type specified by sqlType for an OUT parameter determines the Java type that must be used in the get method to read the value of that
     * parameter.
     * 
     * @param index
     *            the first parameter is 1, the second is 2,...
     * @param sqlType
     *            the JDBC type code defined by SQLType to use to register the OUT Parameter.
     * @param precision
     *            the sum of the desired number of digits to the left and right of the decimal point. It must be greater than or equal to zero.
     * @param scale
     *            the desired number of digits to the right of the decimal point. It must be greater than or equal to zero.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void registerOutParameter(int index,
            SQLType sqlType,
            int precision,
            int scale) throws SQLServerException;

    public void setObject(String sCol,
            Object obj,
            SQLType jdbcType) throws SQLServerException;

    public void setObject(String sCol,
            Object obj,
            SQLType jdbcType,
            int scale) throws SQLServerException;

    /**
     * Sets the value of the designated parameter with the given object.
     * 
     * @param sCol
     *            the name of the parameter
     * @param obj
     *            the object containing the input parameter value
     * @param jdbcType
     *            the SQL type to be sent to the database
     * @param scale
     *            scale the desired number of digits to the right of the decimal point. It must be greater than or equal to zero.
     * @param forceEncrypt
     *            true if force encryption is on, false if force encryption is off
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void setObject(String sCol,
            Object obj,
            SQLType jdbcType,
            int scale,
            boolean forceEncrypt) throws SQLServerException;

    public void registerOutParameter(String parameterName,
            SQLType sqlType,
            String typeName) throws SQLServerException;

    public void registerOutParameter(String parameterName,
            SQLType sqlType,
            int scale) throws SQLServerException;

    /**
     * Registers the parameter in ordinal position index to be of JDBC type sqlType. All OUT parameters must be registered before a stored procedure
     * is executed.
     * <p>
     * The JDBC type specified by sqlType for an OUT parameter determines the Java type that must be used in the get method to read the value of that
     * parameter.
     * 
     * @param parameterName
     *            the name of the parameter
     * @param sqlType
     *            the JDBC type code defined by SQLType to use to register the OUT Parameter.
     * @param precision
     *            the sum of the desired number of digits to the left and right of the decimal point. It must be greater than or equal to zero.
     * @param scale
     *            the desired number of digits to the right of the decimal point. It must be greater than or equal to zero.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void registerOutParameter(String parameterName,
            SQLType sqlType,
            int precision,
            int scale) throws SQLServerException;

    public void registerOutParameter(String parameterName,
            SQLType sqlType) throws SQLServerException;
}
