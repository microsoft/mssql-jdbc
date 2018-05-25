/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Map.Entry;

abstract class SQLServerBulkCommon {
    
    /*
     * Class to represent the column metadata
     */
    protected class ColumnMetadata {
        String columnName;
        int columnType;
        int precision;
        int scale;
        DateTimeFormatter dateTimeFormatter = null;

        ColumnMetadata(String name,
                int type,
                int precision,
                int scale,
                DateTimeFormatter dateTimeFormatter) {
            columnName = name;
            columnType = type;
            this.precision = precision;
            this.scale = scale;
            this.dateTimeFormatter = dateTimeFormatter;
        }
    }
    
    /*
     * Class name for logging.
     */
    protected static String loggerClassName;
    
    /*
     * Logger
     */
    protected static final java.util.logging.Logger loggerExternal = java.util.logging.Logger.getLogger(loggerClassName);
    
    /*
     * Contains all the column names if firstLineIsColumnNames is true
     */
    protected String[] columnNames = null;
    
    /*
     * Metadata to represent the columns in the batch/file. Each column should be mapped to its corresponding position within the parameter (from position 1 and
     * onwards)
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

    /**
     * Adds metadata for the given column in the batch/file.
     * 
     * @param positionInSource
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
    public void addColumnMetadata(int positionInSource,
            String name,
            int jdbcType,
            int precision,
            int scale,
            DateTimeFormatter dateTimeFormatter) throws SQLServerException {
        addColumnMetadataInternal(positionInSource, name, jdbcType, precision, scale, dateTimeFormatter);
    }

    /**
     * Adds metadata for the given column in the batch/file.
     * 
     * @param positionInTable
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
    public void addColumnMetadata(int positionInSource,
            String name,
            int jdbcType,
            int precision,
            int scale) throws SQLServerException {
        addColumnMetadataInternal(positionInSource, name, jdbcType, precision, scale, null);
    }

    /**
     * Adds metadata for the given column in the batch/file.
     * 
     * @param positionInSource
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
    void addColumnMetadataInternal(int positionInSource,
            String name,
            int jdbcType,
            int precision,
            int scale,
            DateTimeFormatter dateTimeFormatter) throws SQLServerException {
        loggerExternal.entering(loggerClassName, "addColumnMetadata", new Object[] {positionInSource, name, jdbcType, precision, scale});

        String colName = "";

        if (0 >= positionInSource) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidColumnOrdinal"));
            Object[] msgArgs = {positionInSource};
            throw new SQLServerException(form.format(msgArgs), SQLState.COL_NOT_FOUND, DriverError.NOT_SET, null);
        }

        if (null != name)
            colName = name.trim();
        else if ((columnNames != null) && (columnNames.length >= positionInSource))
            colName = columnNames[positionInSource - 1];

        if ((columnNames != null) && (positionInSource > columnNames.length)) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidColumn"));
            Object[] msgArgs = {positionInSource};
            throw new SQLServerException(form.format(msgArgs), SQLState.COL_NOT_FOUND, DriverError.NOT_SET, null);
        }

        checkDuplicateColumnName(positionInSource, name);
        switch (jdbcType) {
            /*
             * SQL Server supports numerous string literal formats for temporal types, hence sending them as varchar with approximate
             * precision(length) needed to send supported string literals. string literal formats supported by temporal types are available in MSDN
             * page on data types.
             */
            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
            case microsoft.sql.Types.DATETIMEOFFSET:
                if (this instanceof SQLServerBulkCSVFileRecord) {
                    columnMetadata.put(positionInSource, new ColumnMetadata(colName, jdbcType, 50, precision, dateTimeFormatter));
                } else if (this instanceof SQLServerBulkBatchInsertRecord) {
                    columnMetadata.put(positionInSource, new ColumnMetadata(colName, jdbcType, precision, precision, dateTimeFormatter));
                } else {
                    columnMetadata.put(positionInSource, new ColumnMetadata(colName, jdbcType, 50, precision, dateTimeFormatter));
                }
                break;

            // Redirect SQLXML as LONGNVARCHAR
            // SQLXML is not valid type in TDS
            case java.sql.Types.SQLXML:
                columnMetadata.put(positionInSource, new ColumnMetadata(colName, java.sql.Types.LONGNVARCHAR, precision, scale, dateTimeFormatter));
                break;

            // Redirecting Float as Double based on data type mapping
            // https://msdn.microsoft.com/en-us/library/ms378878%28v=sql.110%29.aspx
            case java.sql.Types.FLOAT:
                columnMetadata.put(positionInSource, new ColumnMetadata(colName, java.sql.Types.DOUBLE, precision, scale, dateTimeFormatter));
                break;

            // redirecting BOOLEAN as BIT
            case java.sql.Types.BOOLEAN:
                columnMetadata.put(positionInSource, new ColumnMetadata(colName, java.sql.Types.BIT, precision, scale, dateTimeFormatter));
                break;

            default:
                columnMetadata.put(positionInSource, new ColumnMetadata(colName, jdbcType, precision, scale, dateTimeFormatter));
        }

        loggerExternal.exiting(loggerClassName, "addColumnMetadata");
    }

    /**
     * Set the format for reading in dates from the batch/file.
     * 
     * @param dateTimeFormat
     *            format to parse data sent as java.sql.Types.TIMESTAMP_WITH_TIMEZONE
     */
    public void setTimestampWithTimezoneFormat(String dateTimeFormat) {
        DriverJDBCVersion.checkSupportsJDBC42();
        loggerExternal.entering(loggerClassName, "setTimestampWithTimezoneFormat", dateTimeFormat);

        this.dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimeFormat);

        loggerExternal.exiting(loggerClassName, "setTimestampWithTimezoneFormat");
    }

    /**
     * Set the format for reading in dates from the batch/file.
     * 
     * @param dateTimeFormatter
     *            format to parse data sent as java.sql.Types.TIMESTAMP_WITH_TIMEZONE
     */
    public void setTimestampWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {
        loggerExternal.entering(loggerClassName, "setTimestampWithTimezoneFormat", new Object[] {dateTimeFormatter});

        this.dateTimeFormatter = dateTimeFormatter;

        loggerExternal.exiting(loggerClassName, "setTimestampWithTimezoneFormat");
    }

    /**
     * Set the format for reading in dates from the batch/file.
     * 
     * @param timeFormat
     *            format to parse data sent as java.sql.Types.TIME_WITH_TIMEZONE
     */
    public void setTimeWithTimezoneFormat(String timeFormat) {
        DriverJDBCVersion.checkSupportsJDBC42();
        loggerExternal.entering(loggerClassName, "setTimeWithTimezoneFormat", timeFormat);

        this.timeFormatter = DateTimeFormatter.ofPattern(timeFormat);

        loggerExternal.exiting(loggerClassName, "setTimeWithTimezoneFormat");
    }

    /**
     * Set the format for reading in dates from the batch/file.
     * 
     * @param dateTimeFormatter
     *            format to parse data sent as java.sql.Types.TIME_WITH_TIMEZONE
     */
    public void setTimeWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {
        loggerExternal.entering(loggerClassName, "setTimeWithTimezoneFormat", new Object[] {dateTimeFormatter});

        this.timeFormatter = dateTimeFormatter;

        loggerExternal.exiting(loggerClassName, "setTimeWithTimezoneFormat");
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
    protected void checkDuplicateColumnName(int positionInTable,
            String colName) throws SQLServerException {

        if (null != colName && colName.trim().length() != 0) {
            for (Entry<Integer, ColumnMetadata> entry : columnMetadata.entrySet()) {
                // duplicate check is not performed in case of same positionInTable value
                if (null != entry && entry.getKey() != positionInTable) {
                    if (null != entry.getValue() && colName.trim().equalsIgnoreCase(entry.getValue().columnName)) {
                        throw new SQLServerException(SQLServerException.getErrString("R_BulkCSVDataDuplicateColumn"), null);
                    }
                }

            }
        }
    }
}
