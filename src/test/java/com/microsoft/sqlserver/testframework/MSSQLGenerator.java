/**
 * File Name: MSSQLGenerator.java 
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

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * @author Microsoft
 */
public class MSSQLGenerator extends AbstractSQLGenerator {

	
	public String wrapBySQ(String name) {
		// TODO Auto-generated method stub
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		sb.append(name);
		sb.append("]");
		return sb.toString();
	}
	
	
	/* (non-Javadoc)
	 * @see com.microsoft.sqlserver.test.SQLGeneratorIF#isTableExist(java.lang.String)
	 */
	@Override
	public String isTableExist(String tableName) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not Implememnted");
	}

	/* (non-Javadoc)
	 * @see com.microsoft.sqlserver.test.SQLGeneratorIF#isColumnExists(java.lang.String, java.lang.String)
	 */
	@Override
	public String isColumnExists(String tableName, String columnName) {
		// TODO Auto-generated method stub
		throw new RuntimeException("Not Implememnted");
	}

	/* (non-Javadoc)
	 * @see com.microsoft.sqlserver.test.SQLGeneratorIF#createTable(com.microsoft.sqlserver.test.Table)
	 */
	@Override
	public String createTable(DBTable table) {
		StringJoiner sb = new StringJoiner(SPACE_CHAR);

		//Assuming table is not null.

		DBColumn[] columns = table.getColumns();

		//Should we give option of create table if not exist etc.? 
		sb.add(CREATE_TABLE);
		sb.add(table.getTableName());
		sb.add(OPEN_BRACKET);
		int noColumn = columns.length;

		for (int i = 0; i < noColumn; i++) {
			DBColumn column = columns[i];

			if (i > 0) {
				sb.add(COMMA);
			}

			sb.add(buildColumnString(column).trim());
		}

		sb.add(CLOSE_BRACKET);
		return sb.toString();
	}

	/**
	 * TODO: Need to think about multiple primary keys.
	 * @param column {@link DBColumn}
	 * @return {@link String}
	 */
	private String buildColumnString(DBColumn column) {
		StringJoiner sb = new StringJoiner(SPACE_CHAR);
		sb.add(wrapBySQ(column.getColumnName()));
		int type = column.getColumnType();

		sb.add(getColumnTypeName(type));
		int length = column.getLength();
		if (isColumnLengthRequired(type) && length > 0) {
			sb.add(OPEN_BRACKET);
			//			sb.add(length+"");
			sb.add(Integer.toString(length));
			sb.add(CLOSE_BRACKET);
		}

		if (isPrecisionScaleDataRequired(type) && length > 0) {
			sb.add(OPEN_BRACKET);
			sb.add(length + COMMA + column.getScale());
			sb.add(CLOSE_BRACKET);
		}

		if (!column.isNullable()) {
			sb.add(NOT);
		}

		sb.add(NULL);

		if (!column.isPrimaryKey() && column.getDefaultValue() != null) {
			sb.add(DEFAULT);
			//			sb.add(TICK+column.getDefaultValue()+TICK);
			sb.add(wrapBySQ(column.getDefaultValue().toString()));
		}

		//In case of multiple primary key this is not going to work. THINK...
		if (column.isPrimaryKey()) {
			sb.add(PRIMARY_KEY);
		}

		return sb.toString();
	}

	/**
	 * This will give you query for Drop Table. 
	 * @param tableName
	 * @return Query in String
	 */
	public String dropTable(String tableName) {
		StringJoiner sb = new StringJoiner(SPACE_CHAR);
		sb.add("IF OBJECT_ID");
		sb.add(OPEN_BRACKET);
		sb.add(wrapName(tableName));
		sb.add(",");
		sb.add(wrapName("U"));
		sb.add(CLOSE_BRACKET);
		sb.add("IS NOT NULL");
		sb.add("DROP TABLE");
		sb.add(tableName); //for drop table no need to wrap.
		return sb.toString();
	}

	/**
	 * Creates query for inserting data. 
	 * @param tableName table name
	 * @param values {@link DBValue}
	 * @return Query in String
	 */
	@Override
	public String insertData(String tableName, DBValue[] values) {
		StringJoiner sb = new StringJoiner(SPACE_CHAR);
		StringBuilder strColumns = new StringBuilder();
		StringBuilder strValues = new StringBuilder();
		
		int size = values.length;
		
		//Better to have advance foreach loop.
		for(int i=0; i< size; i++) {
			DBValue dbValue = values[i];
			if(i !=0) {
				strColumns.append(",");
				strValues.append(",");
			}
			strColumns.append(wrapBySQ(dbValue.getColumnName()));
			strValues.append(wrapName(dbValue.getValue().toString()));
		}
		
		//Actual creating Insert Query
		sb.add("INSERT");
		sb.add("INTO");
		sb.add(tableName);
		sb.add(OPEN_BRACKET);
		sb.add(strColumns.toString());
		sb.add(CLOSE_BRACKET);
		sb.add("VALUES");
		sb.add(OPEN_BRACKET);
		sb.add(strValues.toString());
		sb.add(CLOSE_BRACKET);
		return sb.toString();
	}
	
	/**
	 * Creates query for inserting data
	 * @param tableName  Table name
	 * @param lstValues {@link ArrayList}
	 * @return  Query in String
	 */
	@Override
	public String insertData(String tableName, List<Object> lstValues) {
		StringJoiner sb = new StringJoiner(SPACE_CHAR);
		StringBuilder strValues = new StringBuilder();
		
		int size = lstValues.size();
		
		//Better to have advance foreach loop.
		for(int i=0; i< size; i++) {
			Object value = lstValues.get(i);
			if(i !=0) {
				strValues.append(",");
			}
			strValues.append(wrapName(value.toString()));
		}
		
		//Actual creating Insert Query
		sb.add("INSERT");
		sb.add("INTO");
		sb.add(tableName);
		sb.add("VALUES");
		sb.add(OPEN_BRACKET);
		sb.add(strValues.toString());
		sb.add(CLOSE_BRACKET);
		return sb.toString();
	}
	

}
