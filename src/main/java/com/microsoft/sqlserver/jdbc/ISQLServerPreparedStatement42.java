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
 * This interface requires all the PreparedStatement methods including those are specific to JDBC 4.2
 *
 */
public interface ISQLServerPreparedStatement42 extends ISQLServerPreparedStatement {

    public void setObject(int index,
            Object obj,
            SQLType jdbcType) throws SQLServerException;

    public void setObject(int parameterIndex,
            Object x,
            SQLType targetSqlType,
            int scaleOrLength) throws SQLServerException;

    /**
     * Sets the value of the designated parameter with the given object.
     *
     * This method is similar to {@link #setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength)}, except that it assumes a
     * scale of zero.
     * <P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the object containing the input parameter value
     * @param targetSqlType
     *            the SQL type to be sent to the database
     * @param precision
     *            the precision of the column
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             if parameterIndex does not correspond to a parameter marker in the SQL statement; if a database access error occurs or this method
     *             is called on a closed {@code PreparedStatement}
     */
    public void setObject(int parameterIndex,
            Object x,
            SQLType targetSqlType,
            Integer precision,
            Integer scale) throws SQLServerException;

    /**
     * Sets the value of the designated parameter with the given object.
     *
     * This method is similar to {@link #setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength)}, except that it assumes a
     * scale of zero.
     * <P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the object containing the input parameter value
     * @param targetSqlType
     *            the SQL type to be sent to the database
     * @param precision
     *            the precision of the column
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             if parameterIndex does not correspond to a parameter marker in the SQL statement; if a database access error occurs or this method
     *             is called on a closed {@code PreparedStatement}
     */
    public void setObject(int parameterIndex,
            Object x,
            SQLType targetSqlType,
            Integer precision,
            Integer scale,
            boolean forceEncrypt) throws SQLServerException;
}
