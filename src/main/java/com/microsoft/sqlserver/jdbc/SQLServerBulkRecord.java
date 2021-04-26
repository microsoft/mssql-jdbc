/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

/**
 * Abstract class that implements ISQLServerBulkRecord
 *
 */
abstract class SQLServerBulkRecord implements ISQLServerBulkRecord {

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

    /**
     * Contains all the column names if firstLineIsColumnNames is true
     */
    protected String[] columnNames = null;

    /**
     * Metadata to represent the columns in the batch/file. Each column should be mapped to its corresponding position
     * within the parameter (from position 1 and onwards)
     */
    protected Map<Integer, ColumnMetadata> columnMetadata;

    /**
     * Contains the format that java.sql.Types.TIMESTAMP_WITH_TIMEZONE data should be read in as.
     */
    protected DateTimeFormatter dateTimeFormatter = null;

    /**
     * Contains the format that java.sql.Types.TIME_WITH_TIMEZONE data should be read in as.
     */
    protected DateTimeFormatter timeFormatter = null;

    /*
     * Logger
     */
    String loggerPackageName = "com.microsoft.jdbc.SQLServerBulkRecord";
    static java.util.logging.Logger loggerExternal = java.util.logging.Logger
            .getLogger("com.microsoft.jdbc.SQLServerBulkRecord");

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
        loggerExternal.entering(loggerPackageName, "setTimestampWithTimezoneFormat", dateTimeFormat);
        this.dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat);
        loggerExternal.exiting(loggerPackageName, "setTimestampWithTimezoneFormat");
    }

    @Override
    public void setTimestampWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER)) {
            loggerExternal.entering(loggerPackageName, "setTimestampWithTimezoneFormat",
                    new Object[] {dateTimeFormatter});
        }
        this.dateTimeFormatter = dateTimeFormatter;
        loggerExternal.exiting(loggerPackageName, "setTimestampWithTimezoneFormat");
    }

    @Override
    public void setTimeWithTimezoneFormat(String timeFormat) {
        loggerExternal.entering(loggerPackageName, "setTimeWithTimezoneFormat", timeFormat);
        this.timeFormatter = DateTimeFormatter.ofPattern(timeFormat);
        loggerExternal.exiting(loggerPackageName, "setTimeWithTimezoneFormat");
    }

    @Override
    public void setTimeWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER)) {
            loggerExternal.entering(loggerPackageName, "setTimeWithTimezoneFormat", new Object[] {dateTimeFormatter});
        }
        this.timeFormatter = dateTimeFormatter;
        loggerExternal.exiting(loggerPackageName, "setTimeWithTimezoneFormat");
    }

    /**
     * Helper method to throw a SQLServerExeption with the invalidArgument message and given argument.
     */
    void throwInvalidArgument(String argument) throws SQLServerException {
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidArgument"));
        Object[] msgArgs = {argument};
        SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, false);
    }

    void checkDuplicateColumnName(int positionInTable, String colName) throws SQLServerException {

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

    @Override
    public DateTimeFormatter getColumnDateTimeFormatter(int column) {
        return columnMetadata.get(column).dateTimeFormatter;
    }

    @Override
    public Set<Integer> getColumnOrdinals() {
        return columnMetadata.keySet();
    }

    @Override
    public String getColumnName(int column) {
        return columnMetadata.get(column).columnName;
    }

    @Override
    public int getColumnType(int column) {
        return columnMetadata.get(column).columnType;
    }

    @Override
    public int getPrecision(int column) {
        return columnMetadata.get(column).precision;
    }

    @Override
    public int getScale(int column) {
        return columnMetadata.get(column).scale;
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }
}
