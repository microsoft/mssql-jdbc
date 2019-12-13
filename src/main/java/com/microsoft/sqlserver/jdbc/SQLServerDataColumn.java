/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Represents a column of the in-memory data table represented by {@link SQLServerDataTable}.
 */
public final class SQLServerDataColumn {
    String columnName;
    int javaSqlType;
    int precision = 0;
    int scale = 0;
    int numberOfDigitsIntegerPart = 0;

    /**
     * Constructs a SQLServerDataColumn with the column name and type.
     * 
     * @param columnName
     *        the name of the column
     * @param sqlType
     *        the type of the column
     */
    public SQLServerDataColumn(String columnName, int sqlType) {
        this.columnName = columnName;
        this.javaSqlType = sqlType;
    }

    /**
     * Returns the column name.
     * 
     * @return the name of the column.
     */
    public String getColumnName() {
        return columnName;
    }

    /**
     * Returns the column type.
     * 
     * @return the column type.
     */
    public int getColumnType() {
        return javaSqlType;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + javaSqlType;
        hash = 31 * hash + precision;
        hash = 31 * hash + scale;
        hash = 31 * hash + numberOfDigitsIntegerPart;
        hash = 31 * hash + (null != columnName ? columnName.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (null != object && object.getClass() == SQLServerDataColumn.class) {
            SQLServerDataColumn aSQLServerDataColumn = (SQLServerDataColumn) object;
            if (hashCode() == aSQLServerDataColumn.hashCode()) {
                // Compare objects to avoid collision
                return ((null == columnName ? null == aSQLServerDataColumn.columnName
                                            : columnName.equals(aSQLServerDataColumn.columnName))
                        && javaSqlType == aSQLServerDataColumn.javaSqlType
                        && numberOfDigitsIntegerPart == aSQLServerDataColumn.numberOfDigitsIntegerPart
                        && precision == aSQLServerDataColumn.precision && scale == aSQLServerDataColumn.scale);
            }
        }
        return false;
    }
}
