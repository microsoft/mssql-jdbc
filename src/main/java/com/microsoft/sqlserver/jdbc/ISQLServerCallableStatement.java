/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.SQLException;
import java.sql.SQLType;

/**
 * 
 * This interface is implemented by SQLServerCallableStatement Class.
 *
 */
public interface ISQLServerCallableStatement extends java.sql.CallableStatement, ISQLServerPreparedStatement {
    /**
     * Gets the DateTimeOffset value of parameter with index parameterIndex
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, and so on
     * @return DateTimeOffset value
     * @throws SQLException
     *             if parameterIndex is out of range; if a database access error occurs or this method is called on a closed
     *             <code>CallableStatement</code>
     */
    public microsoft.sql.DateTimeOffset getDateTimeOffset(int parameterIndex) throws SQLException;

    /**
     * Gets the DateTimeOffset value of parameter with name parameterName
     * 
     * @param parameterName
     *            the name of the parameter
     * @return DateTimeOffset value
     * @throws SQLException
     *             if parameterName does not correspond to a named parameter; if a database access error occurs or this method is called on a closed
     *             <code>CallableStatement</code>
     */
    public microsoft.sql.DateTimeOffset getDateTimeOffset(String parameterName) throws SQLException;

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
    
    
}
