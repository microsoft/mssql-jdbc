/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * This class represents a column of the in-memory data table represented by SQLServerDataTable.
 */
public final class SQLServerDataColumn {
    String columnName;
    int javaSqlType;
    int precision = 0;
    int scale = 0;
    int numberOfDigitsIntegerPart = 0;

    /**
     * Initializes a new instance of SQLServerDataColumn with the column name and type.
     * 
     * @param columnName
     *            the name of the column
     * @param sqlType
     *            the type of the column
     */
    public SQLServerDataColumn(String columnName,
            int sqlType) {
        this.columnName = columnName;
        this.javaSqlType = sqlType;
    }

    /**
     * Retrieves the column name.
     * 
     * @return the name of the column.
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Retrieves the column type.
     * 
     * @return the column type.
     */
    public int getColumnType() {
        return javaSqlType;
    }
}