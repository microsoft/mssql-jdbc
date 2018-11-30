/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;


/**
 * 
 * Represents metadata for a column. It is used in the ISQLServerDataRecord interface to pass column metadata to the
 * table-valued parameter.
 * 
 */
public class SQLServerMetaData {

    String columnName = null;
    int javaSqlType;
    int precision = 0;
    int scale = 0;
    boolean useServerDefault = false;
    boolean isUniqueKey = false;
    SQLServerSortOrder sortOrder = SQLServerSortOrder.Unspecified;
    int sortOrdinal;
    private SQLCollation collation;

    static final int defaultSortOrdinal = -1;

    /**
     * Constructs a SQLServerMetaData with the column name and SQL type.
     * 
     * @param columnName
     *        the name of the column
     * @param sqlType
     *        the SQL type of the column
     */
    public SQLServerMetaData(String columnName, int sqlType) {
        this.columnName = columnName;
        this.javaSqlType = sqlType;
    }

    /**
     * Constructs a SQLServerMetaData with the column name, SQL type, precision, and scale.
     * 
     * @param columnName
     *        the name of the column
     * @param sqlType
     *        the SQL type of the column
     * @param precision
     *        the precision of the column
     * @param scale
     *        the scale of the column
     */
    public SQLServerMetaData(String columnName, int sqlType, int precision, int scale) {
        this.columnName = columnName;
        this.javaSqlType = sqlType;
        this.precision = precision;
        this.scale = scale;
    }

    /**
     * Constructs a SQLServerMetaData with the column name, SQL type, and length (for String data).
     * The length is used to differentiate large strings from strings with length less than 4000 characters.
     * 
     * @param columnName
     *        the name of the column
     * @param sqlType
     *        the SQL type of the column
     * @param length
     *        the length of the string type
     */
    public SQLServerMetaData(String columnName, int sqlType, int length) {
        this.columnName = columnName;
        this.javaSqlType = sqlType;
        this.precision = length;
    }

    /**
     * Constructs a SQLServerMetaData.
     * 
     * @param columnName
     *        the name of the column
     * @param sqlType
     *        the sql type of the column
     * @param precision
     *        the precision of the column
     * @param scale
     *        the scale of the column
     * @param useServerDefault
     *        specifies if this column should use the default server value; Default value is false.
     * @param isUniqueKey
     *        indicates if the column in the table-valued parameter is unique; Default value is false.
     * @param sortOrder
     *        indicates the sort order for a column; Default value is SQLServerSortOrder.Unspecified.
     * @param sortOrdinal
     *        specifies ordinal of the sort column; sortOrdinal starts from 0; Default value is -1.
     * @throws SQLServerException
     *         when an error occurs
     */
    public SQLServerMetaData(String columnName, int sqlType, int precision, int scale, boolean useServerDefault,
            boolean isUniqueKey, SQLServerSortOrder sortOrder, int sortOrdinal) throws SQLServerException {
        this.columnName = columnName;
        this.javaSqlType = sqlType;
        this.precision = precision;
        this.scale = scale;
        this.useServerDefault = useServerDefault;
        this.isUniqueKey = isUniqueKey;
        this.sortOrder = sortOrder;
        this.sortOrdinal = sortOrdinal;
        validateSortOrder();
    }

    /**
     * Constructs a SQLServerMetaData from another SQLServerMetaData object.
     * 
     * @param sqlServerMetaData
     *        the object passed to initialize a new instance of SQLServerMetaData
     */
    public SQLServerMetaData(SQLServerMetaData sqlServerMetaData) {
        this.columnName = sqlServerMetaData.columnName;
        this.javaSqlType = sqlServerMetaData.javaSqlType;
        this.precision = sqlServerMetaData.precision;
        this.scale = sqlServerMetaData.scale;
        this.useServerDefault = sqlServerMetaData.useServerDefault;
        this.isUniqueKey = sqlServerMetaData.isUniqueKey;
        this.sortOrder = sqlServerMetaData.sortOrder;
        this.sortOrdinal = sqlServerMetaData.sortOrdinal;
    }

    /**
     * Returns the column name.
     * 
     * @return column name
     */
    public String getColumName() {
        return columnName;
    }

    /**
     * Returns the java sql type.
     * 
     * @return java sql type
     */
    public int getSqlType() {
        return javaSqlType;
    }

    /**
     * Returns the precision of the type passed to the column.
     * 
     * @return precision
     */
    public int getPrecision() {
        return precision;
    }

    /**
     * Returns the scale of the type passed to the column.
     * 
     * @return scale
     */
    public int getScale() {
        return scale;
    }

    /**
     * Returns whether the column uses the default server value.
     * 
     * @return whether the column uses the default server value.
     */
    public boolean useServerDefault() {
        return useServerDefault;
    }

    /**
     * Returns whether the column is unique.
     * 
     * @return whether the column is unique.
     */
    public boolean isUniqueKey() {
        return isUniqueKey;
    }

    /**
     * Returns the sort order.
     * 
     * @return sort order
     */
    public SQLServerSortOrder getSortOrder() {
        return sortOrder;
    }

    /**
     * Returns the sort ordinal.
     * 
     * @return sort ordinal
     */
    public int getSortOrdinal() {
        return sortOrdinal;
    }

    /**
     * Returns the collation.
     * 
     * @return SQL collation
     */
    SQLCollation getCollation() {
        return this.collation;
    }

    void validateSortOrder() throws SQLServerException {
        // should specify both sort order and ordinal, or neither
        if ((SQLServerSortOrder.Unspecified == sortOrder) != (defaultSortOrdinal == sortOrdinal)) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_TVPMissingSortOrderOrOrdinal"));
            throw new SQLServerException(form.format(new Object[] {sortOrder, sortOrdinal}), null, 0, null);
        }
    }
}
