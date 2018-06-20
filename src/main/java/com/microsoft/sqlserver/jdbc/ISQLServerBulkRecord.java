/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.time.format.DateTimeFormatter;

/**
 * The ISQLServerBulkRecord interface can be used to create classes that read in data from any source (such as a file) and allow a SQLServerBulkCopy
 * class to write the data to SQL Server tables.
 * 
 * Implemented by {@link SQLServerBulkCSVFileRecord}
 */
public interface ISQLServerBulkRecord {

    /**
     * Get the ordinals for each of the columns represented in this data record.
     * 
     * @return Set of ordinals for the columns.
     */
    public java.util.Set<Integer> getColumnOrdinals();

    /**
     * Get the name of the given column.
     * 
     * @param column
     *            Column ordinal
     * @return Name of the column
     */
    public String getColumnName(int column);

    /**
     * Get the JDBC data type of the given column.
     * 
     * @param column
     *            Column ordinal
     * @return JDBC data type of the column
     */
    public int getColumnType(int column);

    /**
     * Get the precision for the given column.
     * 
     * @param column
     *            Column ordinal
     * @return Precision of the column
     */
    public int getPrecision(int column);

    /**
     * Get the scale for the given column.
     * 
     * @param column
     *            Column ordinal
     * @return Scale of the column
     */
    public int getScale(int column);

    /**
     * Indicates whether the column represents an identity column.
     * 
     * @param column
     *            Column ordinal
     * @return True if the column is an identity column; false otherwise.
     */
    public boolean isAutoIncrement(int column);

    /**
     * Gets the data for the current row as an array of Objects.
     * 
     * Each Object must match the Java language Type that is used to represent the indicated JDBC data type for the given column. For more
     * information, see 'Understanding the JDBC Driver Data Types' for the appropriate mappings.
     * 
     * @return The data for the row.
     * @throws SQLServerException
     *             If there are any errors in obtaining the data.
     */
    public Object[] getRowData() throws SQLServerException;

    /**
     * Advances to the next data row.
     * 
     * @return True if rows are available; false if there are no more rows
     * @throws SQLServerException
     *             If there are any errors in advancing to the next row.
     */
    public boolean next() throws SQLServerException;

    /**
     * Adds metadata for the given column in the file.
     * 
     * @param positionInFile
     *            Indicates which column the metadata is for. Columns start at 1.
     * @param name
     *            Name for the column (optional if only using column ordinal in a mapping for SQLServerBulkCopy operation)
     * @param jdbcType
     *            JDBC data type of the column
     * @param precision
     *            Precision for the column (ignored for the appropriate data types)
     * @param scale
     *            Scale for the column (ignored for the appropriate data types)
     * @param dateTimeFormatter
     *            format to parse data that is sent
     * @throws SQLServerException
     *             when an error occurs
     */
    public void addColumnMetadata(int positionInFile,
            String name,
            int jdbcType,
            int precision,
            int scale,
            DateTimeFormatter dateTimeFormatter) throws SQLServerException;

    /**
     * Adds metadata for the given column in the file.
     * 
     * @param positionInFile
     *            Indicates which column the metadata is for. Columns start at 1.
     * @param name
     *            Name for the column (optional if only using column ordinal in a mapping for SQLServerBulkCopy operation)
     * @param jdbcType
     *            JDBC data type of the column
     * @param precision
     *            Precision for the column (ignored for the appropriate data types)
     * @param scale
     *            Scale for the column (ignored for the appropriate data types)
     * @throws SQLServerException
     *             when an error occurs
     */
    public void addColumnMetadata(int positionInFile,
            String name,
            int jdbcType,
            int precision,
            int scale) throws SQLServerException;

    /**
     * Set the format for reading in dates from the file.
     * 
     * @param dateTimeFormat
     *            format to parse data sent as java.sql.Types.TIMESTAMP_WITH_TIMEZONE
     */
    public void setTimestampWithTimezoneFormat(String dateTimeFormat);

    /**
     * Set the format for reading in dates from the file.
     * 
     * @param dateTimeFormatter
     *            format to parse data sent as java.sql.Types.TIMESTAMP_WITH_TIMEZONE
     */
    public void setTimestampWithTimezoneFormat(DateTimeFormatter dateTimeFormatter);

    /**
     * Set the format for reading in dates from the file.
     * 
     * @param timeFormat
     *            format to parse data sent as java.sql.Types.TIME_WITH_TIMEZONE
     */
    public void setTimeWithTimezoneFormat(String timeFormat);

    /**
     * Set the format for reading in dates from the file.
     * 
     * @param dateTimeFormatter
     *            format to parse data sent as java.sql.Types.TIME_WITH_TIMEZONE
     */
    public void setTimeWithTimezoneFormat(DateTimeFormatter dateTimeFormatter);

    /**
     * Releases any resources associated with the file reader.
     * 
     * @throws SQLServerException
     *             when an error occurs
     */
    public void close() throws SQLServerException;

    /**
     * Retreives <code>dateTimeFormatter</code> for the given column
     * 
     * @param column
     *            Column ordinal
     * @return dateTimeFormatter
     */
    public DateTimeFormatter getColumnDateTimeFormatter(int column);
}
