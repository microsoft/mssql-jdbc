//---------------------------------------------------------------------------------------------------------------------------------
// File: ISQLServerPreparedStatement42.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), 
//  to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//  and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
//  IN THE SOFTWARE.
//---------------------------------------------------------------------------------------------------------------------------------
 

package com.microsoft.sqlserver.jdbc;

import java.sql.SQLType;

/**
 * This interface requires all the PreparedStatement methods including those are specific to JDBC 4.2
 *
 */
public interface ISQLServerPreparedStatement42 extends ISQLServerPreparedStatement{

	void setObject(int index, Object obj, SQLType jdbcType) throws SQLServerException;

	void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLServerException;

	/**
     * Sets the value of the designated parameter with the given object.
     *
     * This method is similar to {@link #setObject(int parameterIndex,
     * Object x, SQLType targetSqlType, int scaleOrLength)},
     * except that it assumes a scale of zero.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the object containing the input parameter value
     * @param targetSqlType the SQL type to be sent to the database
     * @param precision the precision of the column
     * @param scale the scale of the column
     * @throws SQLServerException if parameterIndex does not correspond to a
     * parameter marker in the SQL statement; if a database access error occurs
     * or this method is called on a closed {@code PreparedStatement}
     */
    void setObject(int parameterIndex, Object x, SQLType targetSqlType, Integer precision, Integer scale) throws SQLServerException;

	/**
     * Sets the value of the designated parameter with the given object.
     *
     * This method is similar to {@link #setObject(int parameterIndex,
     * Object x, SQLType targetSqlType, int scaleOrLength)},
     * except that it assumes a scale of zero.
     *<P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @param x the object containing the input parameter value
     * @param targetSqlType the SQL type to be sent to the database
     * @param precision the precision of the column
     * @param scale the scale of the column
     * @param forceEncrypt If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted
     * and Always Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException if parameterIndex does not correspond to a
     * parameter marker in the SQL statement; if a database access error occurs
     * or this method is called on a closed {@code PreparedStatement}
     */
    void setObject(int parameterIndex, Object x, SQLType targetSqlType, Integer precision, Integer scale, boolean forceEncrypt) throws SQLServerException;
}
