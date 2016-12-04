/**
 * File Name: DBColumn.java 
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

/**
 * Think about encrypted columns. <B>createCMK code should not add here.</B> 
 * @author Microsoft
 *
 */
public class DBColumn {
	
	private String columnName;
	
	private int columnType;
	
	private int length;
	
	private int scale;
	
	private boolean primaryKey;
	
	private boolean nullable = true;
	
	private boolean unicode;
	
	private Object defaultValue;
	
	private boolean alwaysEncrypted;
	
	/**
	 * 
	 * @param columnName
	 * @param columnType
	 * @param length
	 * @param primaryKey
	 * @param defaultValue
	 */
	public DBColumn(String columnName, int columnType, int length, boolean primaryKey, Object defaultValue) {
		
		assert(columnName != null) : "ColumnName should not Null";
				
		this.columnName = columnName;
		this.columnType = columnType;
		this.length = length;
		this.primaryKey = primaryKey;
		this.defaultValue = defaultValue;
		
		if(primaryKey) {
			nullable = false;
		}
	}
	
	/**
	 * 
	 * @param columnName
	 * @param columnType
	 * @param length
	 */
	public DBColumn(String columnName, int columnType, int length) {
		this(columnName, columnType, length, false, null);
	}
	
	/**
	 * 
	 * @param columnName
	 * @param columnType
	 * @param precision
	 * @param scale
	 */
	public DBColumn(String columnName, int columnType, int precision, int scale) {
		this(columnName, columnType, precision, false, null);
		this.scale = scale;
	}
	
	/**
	 * 
	 * @param columnName
	 * @param columnType
	 * @param length
	 * @param primaryKey
	 */
	public DBColumn(String columnName, int columnType, int length, boolean primaryKey) {
		this(columnName, columnType, length, primaryKey, null);
	}
	
	/**
	 * 
	 * @param columnName
	 * @param columnType
	 */
	public DBColumn(String columnName, int columnType) {
		 this(columnName, columnType, -1, false, null);
	}

	/**
	 * @return the columnName
	 */
	public String getColumnName() {
		return columnName;
	}

	/**
	 * @param columnName the columnName to set
	 */
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	/**
	 * @return the columnType
	 */
	public int getColumnType() {
		return columnType;
	}

	/**
	 * @param columnType the columnType to set
	 */
	public void setColumnType(int columnType) {
		this.columnType = columnType;
	}

	/**
	 * @return the length
	 */
	public int getLength() {
		return length;
	}

	/**
	 * @param length the length to set
	 */
	public void setLength(int length) {
		this.length = length;
	}

	/**
	 * @return the primaryKey
	 */
	public boolean isPrimaryKey() {
		return primaryKey;
	}

	/**
	 * @param primaryKey the primaryKey to set
	 */
	public void setPrimaryKey(boolean primaryKey) {
		this.primaryKey = primaryKey;
		
		if(primaryKey) {
			nullable = false;
		}
	}

	/**
	 * @return the nullable
	 */
	public boolean isNullable() {
		return nullable;
	}

	/**
	 * @param nullable the nullable to set
	 */
	public void setNullable(boolean nullable) {
		if (isPrimaryKey()) {
			nullable = false;
		} else {
			this.nullable = nullable;
		}
	}

	/**
	 * @return the unicode
	 */
	public boolean isUnicode() {
		return unicode;
	}

	/**
	 * @param unicode the unicode to set
	 */
	public void setUnicode(boolean unicode) {
		this.unicode = unicode;
	}

	/**
	 * @return the defaultValue
	 */
	public Object getDefaultValue() {
		return defaultValue;
	}

	/**
	 * @param defaultValue the defaultValue to set
	 */
	public void setDefaultValue(Object defaultValue) {
		this.defaultValue = defaultValue;
	}

	/**
	 * @return the alwaysEncrypted
	 */
	public boolean isAlwaysEncrypted() {
		return alwaysEncrypted;
	}

	/**
	 * @param alwaysEncrypted the alwaysEncrypted to set
	 */
	public void setAlwaysEncrypted(boolean alwaysEncrypted) {
		this.alwaysEncrypted = alwaysEncrypted;
	}
	
	/**
	 * 
	 * @param precision
	 * @param scale
	 */
	public void addPrecision(int precision, int scale) {
		this.length = precision;
		this.scale = scale;
	}
	
	/**
	 * 
	 * @return scale
	 */
	public int getScale() {
		return scale;
	}
	
}
