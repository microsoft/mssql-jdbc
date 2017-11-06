/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

import java.sql.JDBCType;
import java.util.ArrayList;
import java.util.List;

import com.microsoft.sqlserver.testframework.sqlType.SqlType;

/**
 * This class holds data for Column. Think about encrypted columns. <B>createCMK code should not add here.</B>
 */
public class DBColumn {

    /*
     * TODO: add nullable, defaultValue, alwaysEncrypted
     */
    private String columnName;
    private SqlType sqlType;
    private List<Object> columnValues;

    DBColumn(String columnName,
            SqlType sqlType) {
        this.columnName = columnName;
        this.sqlType = sqlType;
    }

    /**
     * @return the columnName
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * @param columnName
     *            the columnName to set
     */
    void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    /**
     * 
     * @return SqlType for the column
     */
    public SqlType getSqlType() {
        return sqlType;
    }

    /**
     * 
     * @return JDBCType for the column
     */
    JDBCType getJdbctype() {
        return sqlType.getJdbctype();
    }

    /**
     * 
     * @param sqlType
     */
    void setSqlType(SqlType sqlType) {
        this.sqlType = sqlType;
    }

    /**
     * generate value for the column
     * 
     * @param rows
     *            number of rows
     */
    void populateValues(int rows) {
        columnValues = new ArrayList<>();
        for (int i = 0; i < rows; i++)
            columnValues.add(sqlType.createdata());
    }

    /**
     * 
     * @param row
     * @return the value populated for the column
     */
    Object getRowValue(int row) {
        // handle exceptions
        return columnValues.get(row);
    }

}