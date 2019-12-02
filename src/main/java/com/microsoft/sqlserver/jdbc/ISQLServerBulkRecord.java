/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.time.format.DateTimeFormatter;


/**
 * Provides an interface used to create classes that read in data from any source (such as a file) and allows a
 * SQLServerBulkCopy class to write the data to SQL Server tables.
 * 
 * This interface is implemented by {@link SQLServerBulkRecord} Class
 *
 * @deprecated as of 8.1.0, because the interface contains methods which are not called as part of actual bulk copy
 *             process. Use {@link ISQLServerBulkData}} instead.
 */
@Deprecated
public interface ISQLServerBulkRecord extends ISQLServerBulkData {
    /**
     * Returns whether the column represents an identity column.
     *
     * @param column
     *        Column ordinal
     * @return True if the column is an identity column; false otherwise.
     */
    boolean isAutoIncrement(int column);

    /**
     * Adds metadata for the given column in the file.
     * 
     * @param positionInFile
     *        Indicates which column the metadata is for. Columns start at 1.
     * @param name
     *        Name for the column (optional if only using column ordinal in a mapping for SQLServerBulkCopy operation)
     * @param jdbcType
     *        JDBC data type of the column
     * @param precision
     *        Precision for the column (ignored for the appropriate data types)
     * @param scale
     *        Scale for the column (ignored for the appropriate data types)
     * @param dateTimeFormatter
     *        format to parse data that is sent
     * @throws SQLServerException
     *         when an error occurs
     */
    void addColumnMetadata(int positionInFile, String name, int jdbcType, int precision, int scale,
            DateTimeFormatter dateTimeFormatter) throws SQLServerException;

    /**
     * Adds metadata for the given column in the file.
     * 
     * @param positionInFile
     *        Indicates which column the metadata is for. Columns start at 1.
     * @param name
     *        Name for the column (optional if only using column ordinal in a mapping for SQLServerBulkCopy operation)
     * @param jdbcType
     *        JDBC data type of the column
     * @param precision
     *        Precision for the column (ignored for the appropriate data types)
     * @param scale
     *        Scale for the column (ignored for the appropriate data types)
     * @throws SQLServerException
     *         when an error occurs
     */
    void addColumnMetadata(int positionInFile, String name, int jdbcType, int precision,
            int scale) throws SQLServerException;

    /**
     * Sets the format for reading in dates from the file.
     * 
     * @param dateTimeFormat
     *        format to parse data sent as java.sql.Types.TIMESTAMP_WITH_TIMEZONE
     */
    void setTimestampWithTimezoneFormat(String dateTimeFormat);

    /**
     * Sets the format for reading in dates from the file.
     * 
     * @param dateTimeFormatter
     *        format to parse data sent as java.sql.Types.TIMESTAMP_WITH_TIMEZONE
     */
    void setTimestampWithTimezoneFormat(DateTimeFormatter dateTimeFormatter);

    /**
     * Sets the format for reading in dates from the file.
     * 
     * @param timeFormat
     *        format to parse data sent as java.sql.Types.TIME_WITH_TIMEZONE
     */
    void setTimeWithTimezoneFormat(String timeFormat);

    /**
     * Sets the format for reading in dates from the file.
     * 
     * @param dateTimeFormatter
     *        format to parse data sent as java.sql.Types.TIME_WITH_TIMEZONE
     */
    void setTimeWithTimezoneFormat(DateTimeFormatter dateTimeFormatter);

    /**
     * Returns the <code>dateTimeFormatter</code> for the given column.
     * 
     * @param column
     *        Column ordinal
     * @return dateTimeFormatter
     */
    DateTimeFormatter getColumnDateTimeFormatter(int column);
}
