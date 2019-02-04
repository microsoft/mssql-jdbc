/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Map.Entry;


abstract class SQLServerBulkCommon implements ISQLServerBulkRecord {

    /**
     * Update serialVersionUID when making changes to this file
     */
    private static final long serialVersionUID = -170992637946357449L;

    /*
     * Class to represent the column metadata
     */
    protected class ColumnMetadata {
        String columnName;
        int columnType;
        int precision;
        int scale;
        DateTimeFormatter dateTimeFormatter = null;

        ColumnMetadata(String name, int type, int precision, int scale, DateTimeFormatter dateTimeFormatter) {
            columnName = name;
            columnType = type;
            this.precision = precision;
            this.scale = scale;
            this.dateTimeFormatter = dateTimeFormatter;
        }
    }

    /*
     * Contains all the column names if firstLineIsColumnNames is true
     */
    protected String[] columnNames = null;

    /*
     * Metadata to represent the columns in the batch/file. Each column should be mapped to its corresponding position
     * within the parameter (from position 1 and onwards)
     */
    protected Map<Integer, ColumnMetadata> columnMetadata;

    /*
     * Contains the format that java.sql.Types.TIMESTAMP_WITH_TIMEZONE data should be read in as.
     */
    protected DateTimeFormatter dateTimeFormatter = null;

    /*
     * Contains the format that java.sql.Types.TIME_WITH_TIMEZONE data should be read in as.
     */
    protected DateTimeFormatter timeFormatter = null;

    @Override
    public void addColumnMetadata(int positionInSource, String name, int jdbcType, int precision, int scale,
            DateTimeFormatter dateTimeFormatter) throws SQLServerException {
        addColumnMetadataInternal(positionInSource, name, jdbcType, precision, scale, dateTimeFormatter);
    }

    @Override
    public void addColumnMetadata(int positionInSource, String name, int jdbcType, int precision,
            int scale) throws SQLServerException {
        addColumnMetadataInternal(positionInSource, name, jdbcType, precision, scale, null);
    }

    /**
     * Adds metadata for the given column in the batch/file.
     * 
     * @param positionInSource
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
    void addColumnMetadataInternal(int positionInSource, String name, int jdbcType, int precision, int scale,
            DateTimeFormatter dateTimeFormatter) throws SQLServerException {}

    @Override
    public void setTimestampWithTimezoneFormat(String dateTimeFormat) {
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat);
    }

    @Override
    public void setTimestampWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {
        this.dateTimeFormatter = dateTimeFormatter;
    }

    @Override
    public void setTimeWithTimezoneFormat(String timeFormat) {
        this.timeFormatter = DateTimeFormatter.ofPattern(timeFormat);
    }

    @Override
    public void setTimeWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {
        this.timeFormatter = dateTimeFormatter;
    }

    /*
     * Helper method to throw a SQLServerExeption with the invalidArgument message and given argument.
     */
    protected void throwInvalidArgument(String argument) throws SQLServerException {
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidArgument"));
        Object[] msgArgs = {argument};
        SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, false);
    }

    /*
     * Method to throw a SQLServerExeption for duplicate column names
     */
    protected void checkDuplicateColumnName(int positionInTable, String colName) throws SQLServerException {

        if (null != colName && colName.trim().length() != 0) {
            for (Entry<Integer, ColumnMetadata> entry : columnMetadata.entrySet()) {
                // duplicate check is not performed in case of same
                // positionInTable value
                if (null != entry && entry.getKey() != positionInTable) {
                    if (null != entry.getValue() && colName.trim().equalsIgnoreCase(entry.getValue().columnName)) {
                        throw new SQLServerException(SQLServerException.getErrString("R_BulkDataDuplicateColumn"),
                                null);
                    }
                }
            }
        }
    }
}
