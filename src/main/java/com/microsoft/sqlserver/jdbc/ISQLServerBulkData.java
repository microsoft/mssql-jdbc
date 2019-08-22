/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.Serializable;

/**
 * Provides an interface used to create classes that read in data from any source (such as a file) and allows a
 * SQLServerBulkCopy class to write the data to SQL Server tables.
 */
public interface ISQLServerBulkData extends Serializable {

    /**
     * Returns the ordinals for each of the columns represented in this data record.
     *
     * @return Set of ordinals for the columns.
     */
    java.util.Set<Integer> getColumnOrdinals();

    /**
     * Returns the name of the given column.
     *
     * @param column
     *        Column ordinal
     * @return Name of the column
     */
    String getColumnName(int column);

    /**
     * Returns the JDBC data type of the given column.
     *
     * @param column
     *        Column ordinal
     * @return JDBC data type of the column
     */
    int getColumnType(int column);

    /**
     * Returns the precision for the given column.
     *
     * @param column
     *        Column ordinal
     * @return Precision of the column
     */
    int getPrecision(int column);

    /**
     * Returns the scale for the given column.
     *
     * @param column
     *        Column ordinal
     * @return Scale of the column
     */
    int getScale(int column);

    /**
     * Returns the data for the current row as an array of Objects.
     *
     * Each Object must match the Java language Type that is used to represent the indicated JDBC data type for the
     * given column. For more information, see 'Understanding the JDBC Driver Data Types' for the appropriate mappings.
     *
     * @return The data for the row.
     * @throws SQLServerException
     *         If there are any errors in obtaining the data.
     */
    Object[] getRowData() throws SQLServerException;

    /**
     * Advances to the next data row.
     *
     * @return True if rows are available; false if there are no more rows
     * @throws SQLServerException
     *         If there are any errors in advancing to the next row.
     */
    boolean next() throws SQLServerException;
}
