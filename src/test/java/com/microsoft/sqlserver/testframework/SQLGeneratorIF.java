/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation
 * All rights reserved.
 * 
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

import java.util.List;

/**
 * @see  
 * <Li>{@link <a href="https://msdn.microsoft.com/en-us/library/ms365315.aspx"> Understanding the JDBC Driver Data Types </a>}
 * <LI>{@link <a href="https://msdn.microsoft.com/en-us/library/ms187752.aspx"> Data Types (Transact-SQL) </a>}
 * <LI>{@link <a href="https://msdn.microsoft.com/en-us/library/ff848799.aspx"> Data Definition Language (DDL) Statements (Transact-SQL) </a>}
 * <LI>{@link <a href="https://msdn.microsoft.com/en-us/library/aa342341(v=sql.110).aspx"> Working with Data Types (JDBC) </a>}

 * 
 * TODO
 * <LI> https://msdn.microsoft.com/en-us/library/ff630902(v=sql.110).aspx Wrappers and Interfaces
 * <LI> For {@link #createTable(DBTable)} Instead of returning String should we execute and return result? 
 * 			or may be change other two method signature {@link #isColumnExists(String, String)} and {@link #isTableExist(String)}  
 */
public interface SQLGeneratorIF {
	
	/**
	 * Checks if table exists or not. 
	 * @param tableName table name
	 * @return
	 */
    String isTableExist(String tableName);
	
	/**
	 * Checking if column exists or not
	 * @param tableName TableName
	 * @param columnName ColumnName
	 * @return boolean 
	 */
    String isColumnExists(String tableName, String columnName);
	
	/**
	 * This will give you actual query for create table.
	 * @param table {@link DBTable}
	 * @return SQL Query in String
	 */
    String createTable(DBTable table);
	
	/**
	 * This will give you query for Drop Table. 
	 * @param tableName
	 * @return Query in String
	 */
    String dropTable(String tableName);


	/**
	 * Creates query for inserting data. 
	 * @param tableName table name
	 * @param values {@link DBValue}
	 * @return Query in String
	 */
    String insertData(String tableName, DBValue[] values);
	
	/**
	 * Creates query for inserting data
	 * @param tableName  Table name
	 * @param lstValues {@link List}
	 * @return  Query in String
	 */
    String insertData(String tableName, List<Object> lstValues);
}
