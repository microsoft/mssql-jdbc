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

import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

/**
 * Common methods needed for any implementation for {@link SQLGeneratorIF}
 */
public abstract class AbstractSQLGenerator implements SQLGeneratorIF {

	protected static final String CREATE_TABLE = "CREATE TABLE";
	protected static final String SPACE_CHAR = " ";
	protected static final String OPEN_BRACKET = "(";
	protected static final String CLOSE_BRACKET = ")";
	protected static final String NOT = "NOT";
	protected static final String NULL = "NULL";
	protected static final String PRIMARY_KEY = "PRIMARY KEY";
	protected static final String DEFAULT = "DEFAULT";
	protected static final String COMMA = ",";

	//FIXME: Find good word for '. Better replaced by wrapIdentifier. 
	protected static final String TICK = "'";

	protected static final String defaultWrapIdentifier = "\'";
	
	private static final Map<Integer, String> dataTypes;

	private static final Map<Integer, String> dataTypesWithLengh;

	private static final Map<Integer, String> dataTypesWithPrecision;
	
	protected static String wrapIdentifier = defaultWrapIdentifier;

	//TODO: Come up with TypeName class which can store default size, precision & scale etc. 
	//TODO: Need to add support for MAX
	static {
		dataTypes = new HashMap<>();
		dataTypes.put(Types.BIGINT, "BIGINT");
		dataTypes.put(Types.BINARY, "BINARY");
		dataTypes.put(Types.BIT, "BIT");
		dataTypes.put(Types.CHAR, "CHAR");
		dataTypes.put(Types.DATE, "DATE");
		dataTypes.put(Types.TIMESTAMP, "DATETIME");
		dataTypes.put(microsoft.sql.Types.DATETIME, "DATETIME2");
		dataTypes.put(microsoft.sql.Types.DATETIMEOFFSET, "DATETIMEOFFSET");
		dataTypes.put(Types.DECIMAL, "DECIMAL");
		dataTypes.put(Types.DOUBLE, "FLOAT");
		dataTypes.put(Types.LONGVARBINARY, "IMAGE");
		dataTypes.put(microsoft.sql.Types.GUID, "GUID");
		dataTypes.put(Types.INTEGER, "INT");
		dataTypes.put(microsoft.sql.Types.MONEY, "MONEY");
		dataTypes.put(Types.NCHAR, "NCHAR");
		dataTypes.put(Types.LONGNVARCHAR, "NTEXT");
		dataTypes.put(Types.NUMERIC, "NUMERIC");
		dataTypes.put(Types.NVARCHAR, "NVARCHAR");
		dataTypes.put(Types.REAL, "REAL");
		dataTypes.put(microsoft.sql.Types.SMALLDATETIME, "SMALLDATETIME");
		dataTypes.put(Types.SMALLINT, "SMALLINT");
		dataTypes.put(microsoft.sql.Types.SMALLMONEY, "SMALLMONEY");
		dataTypes.put(Types.LONGVARCHAR, "TEXT");
		dataTypes.put(Types.TIME, "TIME");
		dataTypes.put(Types.TINYINT, "TINYINT");
		dataTypes.put(Types.VARBINARY, "VARBINARY");
		dataTypes.put(Types.VARCHAR, "VARCHAR");
		dataTypes.put(microsoft.sql.Types.MONEY, "MONEY");

		//Data types with length
		dataTypesWithLengh = new HashMap<>();
		dataTypesWithLengh.put(microsoft.sql.Types.DATETIMEOFFSET, "DATETIMEOFFSET");
		dataTypesWithLengh.put(Types.NCHAR, "NCHAR");
		dataTypesWithLengh.put(Types.LONGNVARCHAR, "NTEXT");
		dataTypesWithLengh.put(Types.NVARCHAR, "NVARCHAR");
		dataTypesWithLengh.put(Types.LONGVARCHAR, "TEXT");
		dataTypesWithLengh.put(Types.VARBINARY, "VARBINARY");
		dataTypesWithLengh.put(Types.VARCHAR, "VARCHAR");
		dataTypesWithLengh.put(Types.CHAR, "CHAR");

		//Data types with precision
		dataTypesWithPrecision = new HashMap<>();
		dataTypesWithPrecision.put(Types.NUMERIC, "NUMERIC");
		dataTypesWithPrecision.put(Types.DECIMAL, "DECIMAL");

	}

	/**
	 * @return the wrapIdentifier
	 */
	public static String getWrapIdentifier() {
		return wrapIdentifier;
	}

	/**
	 * @param wrapIdentifier the wrapIdentifier to set
	 */
	public static void setWrapIdentifier(String wrapIdentifier) {
		AbstractSQLGenerator.wrapIdentifier = wrapIdentifier;
	}

	/**
	 * 
	 * @param type
	 * @return
	 */
	public static String getColumnTypeName(int type) {
		return dataTypes.get(type);
	}

	/**
	 * 
	 * @param type
	 * @return
	 */
	public static boolean isColumnLengthRequired(int type) {
		return dataTypesWithLengh.containsKey(type);
	}

	/**
	 * 
	 * @param type
	 * @return
	 */
	public static boolean isPrecisionScaleDataRequired(int type) {
		return dataTypesWithPrecision.containsKey(type);
	}

	/**
	 * It will wrap provided string with wrap identifier. 
	 * @param name
	 * @return
	 */
	public String wrapName(String name) {
		StringBuffer wrap = new StringBuffer();

		wrap.append(getWrapIdentifier());
		wrap.append(name);
		wrap.append(getWrapIdentifier());

		return wrap.toString();
	}

}
