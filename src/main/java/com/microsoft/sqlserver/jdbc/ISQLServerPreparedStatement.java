/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.SQLException;

public interface ISQLServerPreparedStatement extends java.sql.PreparedStatement, ISQLServerStatement {
    /**
     * Sets the designated parameter to the given <code>microsoft.sql.DateTimeOffset</code> value.
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @throws SQLException
     *             if parameterIndex does not correspond to a parameter marker in the SQL statement; if a database access error occurs or this method
     *             is called on a closed <code>PreparedStatement</code>
     */
    public void setDateTimeOffset(int parameterIndex,
            microsoft.sql.DateTimeOffset x) throws SQLException;
}
