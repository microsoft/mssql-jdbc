/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * The ISQLServerDataRecord interface can be used to create classes that read in data from any source (such as a file) and allow a structured type to
 * be sent to SQL Server tables.
 */

public interface ISQLServerDataRecord {
    /**
     * Get the column meta data
     * 
     * @param column
     *            the first column is 1, the second is 2, and so on
     * @return SQLServerMetaData of column
     */
    public SQLServerMetaData getColumnMetaData(int column);

    /**
     * Get the column count.
     * 
     * @return Set of ordinals for the columns.
     */
    public int getColumnCount();

    /**
     * Gets the data for the current row as an array of Objects.
     * 
     * Each Object must match the Java language Type that is used to represent the indicated JDBC data type for the given column. For more
     * information, see 'Understanding the JDBC Driver Data Types' for the appropriate mappings.
     * 
     * @return The data for the row.
     */
    public Object[] getRowData();

    /**
     * Advances to the next data row.
     * 
     * @return True if rows are available; false if there are no more rows
     */
    public boolean next();
}
