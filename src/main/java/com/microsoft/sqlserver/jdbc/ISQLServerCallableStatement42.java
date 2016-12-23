//---------------------------------------------------------------------------------------------------------------------------------
// File: ISQLServerCallableStatement42.java
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
 * This interface requires all the CallableStatement methods including those are specific to JDBC 4.2
 *
 */
public interface ISQLServerCallableStatement42 extends ISQLServerCallableStatement, ISQLServerPreparedStatement42{

	void registerOutParameter(int index, SQLType sqlType) throws SQLServerException;

	void registerOutParameter(int index, SQLType sqlType, String typeName) throws SQLServerException;

	void registerOutParameter(int index, SQLType sqlType, int scale) throws SQLServerException;

	/**
	 * Registers the parameter in ordinal position index to be of JDBC type sqlType. All OUT parameters must be registered before a stored procedure is executed.
	 * <p>
	 * The JDBC type specified by sqlType for an OUT parameter determines the Java type that must be used in the get method to read the value of that parameter.
	 * 
	 * @param index the first parameter is 1, the second is 2,...
	 * @param sqlType the JDBC type code defined by SQLType to use to register the OUT Parameter.
	 * @param precision the sum of the desired number of digits to the left and right of the decimal point. It must be greater than or equal to zero.
	 * @param scale the desired number of digits to the right of the decimal point. It must be greater than or equal to zero.
	 * @throws SQLServerException If any errors occur.
	 */
    void registerOutParameter(int index, SQLType sqlType, int precision, int scale) throws SQLServerException;

	void setObject(String sCol, Object obj, SQLType jdbcType) throws SQLServerException;

	void setObject(String sCol, Object obj, SQLType jdbcType, int scale) throws SQLServerException;

	/**
	 * Sets the value of the designated parameter with the given object.
	 * 
	 * @param sCol the name of the parameter
	 * @param obj the object containing the input parameter value
	 * @param jdbcType the SQL type to be sent to the database
	 * @param scale scale the desired number of digits to the right of the decimal point. It must be greater than or equal to zero.
	 * @param forceEncrypt true if force encryption is on, false if force encryption is off
	 * @throws SQLServerException If any errors occur.
	 */
    void setObject(String sCol, Object obj, SQLType jdbcType, int scale, boolean forceEncrypt) throws SQLServerException;

	void registerOutParameter(String parameterName, SQLType sqlType, String typeName) throws SQLServerException;

	void registerOutParameter(String parameterName, SQLType sqlType, int scale) throws SQLServerException;

	/**
	 * Registers the parameter in ordinal position index to be of JDBC type sqlType. All OUT parameters must be registered before a stored procedure is executed.
	 * <p>
	 * The JDBC type specified by sqlType for an OUT parameter determines the Java type that must be used in the get method to read the value of that parameter.
	 * 
	 * @param parameterName the name of the parameter
	 * @param sqlType the JDBC type code defined by SQLType to use to register the OUT Parameter.
	 * @param precision the sum of the desired number of digits to the left and right of the decimal point. It must be greater than or equal to zero.
	 * @param scale the desired number of digits to the right of the decimal point. It must be greater than or equal to zero.
	 * @throws SQLServerException If any errors occur.
	 */
    void registerOutParameter(String parameterName, SQLType sqlType, int precision, int scale) throws SQLServerException;

	void registerOutParameter(String parameterName, SQLType sqlType) throws SQLServerException;
}
