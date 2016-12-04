/**
 * File Name: SQLGeneratorIF.java 
 * Created : Dec 3, 2016
 *
 * Microsoft JDBC Driver for SQL Server
 * The MIT License (MIT)
 * Copyright(c) 2016 Microsoft Corporation
 * All rights reserved.
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
 * and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *  
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR 
 * ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH 
 * THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE. 
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
 * @author Microsoft
 *
 */
public interface SQLGeneratorIF {
	
	/**
	 * 
	 * @param tableName table name
	 * @return
	 */
	public String isTableExist(String tableName);
	
	/**
	 * Checking if column exists or not
	 * @param tableName TableName
	 * @param columnName ColumnName
	 * @return boolean 
	 */
	public String isColumnExists(String tableName, String columnName);
	
	/**
	 * This will give you actual query for create table.
	 * @param table {@link DBTable}
	 * @return SQL Query in String
	 */
	public String createTable(DBTable table);
	
	/**
	 * This will give you query for Drop Table. 
	 * @param tableName
	 * @return Query in String
	 */
	public String dropTable(String tableName);


	/**
	 * Creates query for inserting data. 
	 * @param tableName table name
	 * @param values {@link DBValue}
	 * @return Query in String
	 */
	public String insertData(String tableName, DBValue[] values);
	
	/**
	 * Creates query for inserting data
	 * @param tableName  Table name
	 * @param lstValues {@link List}
	 * @return  Query in String
	 */
	public String insertData(String tableName, List<Object> lstValues);
}
