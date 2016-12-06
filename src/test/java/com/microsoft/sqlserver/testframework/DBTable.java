/**
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation
 * All rights reserved.
 * 
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

/**
 * @author Microsoft
 *
 */
public class DBTable {

	private String tableName;
	
	private DBColumn[] columns;
	
	/**
	 * 
	 * @param tableName Table name
	 * @param columns Array of DBColumn
	 */
	public DBTable(String tableName, DBColumn[] columns) {
		
		assert(tableName != null) : "TableName should not Null";
		assert(columns != null) : "Columns should not be Null";
		assert(columns.length > 0) : "Atleast one column should be there";
		
		this.tableName = tableName;
		this.columns = columns;
	}

	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @return the columns : Array of DBColumn.
	 */
	public DBColumn[] getColumns() {
		return columns;
	}
	
}
