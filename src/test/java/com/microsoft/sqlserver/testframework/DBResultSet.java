//---------------------------------------------------------------------------------------------------------------------------------
// File: DBResultSet.java
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
 

package com.microsoft.sqlserver.testframework;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * wrapper class for ResultSet 
 * @author Microsoft
 *
 */
public class DBResultSet extends AbstractParentWrapper{

	// TODO: add cursors
	// TODO: add resultSet level holdability 
	// TODO: add concurrency control
	ResultSet resultSet = null;
	
	DBResultSet(DBStatement dbstatement, ResultSet internal){
		super(dbstatement, internal, "resultSet");
		resultSet = internal;
	}
	
	/**
	 * Close the ResultSet object
	 * @throws SQLException 
	 */
	public void close() throws SQLException {
		if (null != resultSet) {
			resultSet.close();
		}
	}
	
	/**
	 * 
	 * @return true new row is valid
	 * @throws SQLException
	 */
	public boolean next() throws SQLException {
		return resultSet.next();
	}
	
	/**
	 * 
	 * @param index
	 * @return Object with the column value
	 * @throws SQLException
	 */
	public Object getObject(int index) throws SQLException {
		// call individual getters based on type
		return resultSet.getObject(index);
	}
	
	/**
	 * 
	 * @param index
	 * @return
	 */
	public void updateObject(int index) throws SQLException {
		//TODO: update object based on cursor type
	}
}
