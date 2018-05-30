/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.SQLException;

/**
 * 
 * This interface is implemented by SQLServerCallableStatement Class.
 *
 */
public interface ISQLServerCallableStatement extends java.sql.CallableStatement, ISQLServerPreparedStatement {
    /**
     * Sets parameter parameterName to DateTimeOffset x
     * 
     * @param parameterName
     *            the name of the parameter
     * @param x
     *            DateTimeOffset value
     * @throws SQLException
     *             if parameterName does not correspond to a named parameter; if the driver can detect that a data conversion error could occur; if a
     *             database access error occurs or this method is called on a closed <code>CallableStatement</code>
     */
    public void setDateTimeOffset(String parameterName,
            microsoft.sql.DateTimeOffset x) throws SQLException;

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
}
