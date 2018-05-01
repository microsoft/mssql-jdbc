/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Types;
import java.text.DecimalFormat;
import java.text.MessageFormat;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * A simple implementation of the ISQLServerBulkRecord interface that can be used to read in the basic Java data types from an ArrayList of
 * Parameters that were provided by pstmt/cstmt.
 */
public class SQLServerBulkBatchInsertRecord implements ISQLServerBulkRecord, java.lang.AutoCloseable {
    /*
     * Class to represent the column metadata
     */
    private class ColumnMetadata {
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
     * Metadata to represent the columns in the batch. Each column should be mapped to its corresponding position within the parameter (from position 1 and
     * onwards)
     */
    private Map<Integer, ColumnMetadata> columnMetadata;

    /*
     * Contains all the column names if firstLineIsColumnNames is true
     */
    private String[] columnNames = null;

    /*
     * Contains the format that java.sql.Types.TIMESTAMP_WITH_TIMEZONE data should be read in as.
     */
    private DateTimeFormatter dateTimeFormatter = null;

    /*
     * Contains the format that java.sql.Types.TIME_WITH_TIMEZONE data should be read in as.
     */
    private DateTimeFormatter timeFormatter = null;
    
    /*
     * Class name for logging.
     */
    private static final String loggerClassName = "com.microsoft.sqlserver.jdbc.SQLServerBulkBatchInsertRecord";
    
    /*
     * Logger
     */
    private static final java.util.logging.Logger loggerExternal = java.util.logging.Logger.getLogger(loggerClassName);

    private ArrayList<Parameter[]> batchParam;
    private int batchParamIndex = -1;
    private ArrayList<String> columnList;
    private ArrayList<String> valueList;

    public SQLServerBulkBatchInsertRecord(ArrayList<Parameter[]> batchParam, ArrayList<String> columnList, 
            ArrayList<String> valueList, String encoding) throws SQLServerException {
        loggerExternal.entering(loggerClassName, "SQLServerBulkBatchInsertRecord",
                new Object[] {batchParam, encoding});
        
        if (null == batchParam) {
            throwInvalidArgument("batchParam");
        }
        
        if (null == valueList) {
            throwInvalidArgument("valueList");
        }
                
        this.batchParam = batchParam;
        this.columnList = columnList;
        this.valueList = valueList;
        columnMetadata = new HashMap<>();

        loggerExternal.exiting(loggerClassName, "SQLServerBulkBatchInsertRecord");
    }

    /**
     * Adds metadata for the given column in the file.
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
     * @param dateTimeFormatter
     *            format to parse data that is sent
     * @throws SQLServerException
     *             when an error occurs
     */
    public void addColumnMetadata(int positionInTable,
            String name,
            int jdbcType,
            int precision,
            int scale,
            DateTimeFormatter dateTimeFormatter) throws SQLServerException {
        addColumnMetadataInternal(positionInTable, name, jdbcType, precision, scale, dateTimeFormatter);
    }

    /**
     * Adds metadata for the given column in the file.
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
    public void addColumnMetadata(int positionInTable,
            String name,
            int jdbcType,
            int precision,
            int scale) throws SQLServerException {
        addColumnMetadataInternal(positionInTable, name, jdbcType, precision, scale, null);
    }

    /**
     * Adds metadata for the given column in the file.
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
     * @param dateTimeFormatter
     *            format to parse data that is sent
     * @throws SQLServerException
     *             when an error occurs
     */
    void addColumnMetadataInternal(int positionInTable,
            String name,
            int jdbcType,
            int precision,
            int scale,
            DateTimeFormatter dateTimeFormatter) throws SQLServerException {
        loggerExternal.entering(loggerClassName, "addColumnMetadata", new Object[] {positionInTable, name, jdbcType, precision, scale});

        String colName = "";

        if (0 >= positionInTable) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidColumnOrdinal"));
            Object[] msgArgs = {positionInTable};
            throw new SQLServerException(form.format(msgArgs), SQLState.COL_NOT_FOUND, DriverError.NOT_SET, null);
        }

        if (null != name)
            colName = name.trim();
        else if ((columnNames != null) && (columnNames.length >= positionInTable))
            colName = columnNames[positionInTable - 1];

        if ((columnNames != null) && (positionInTable > columnNames.length)) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidColumn"));
            Object[] msgArgs = {positionInTable};
            throw new SQLServerException(form.format(msgArgs), SQLState.COL_NOT_FOUND, DriverError.NOT_SET, null);
        }

        checkDuplicateColumnName(positionInTable, name);
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
                // The precision is just a number long enough to hold all types of temporal data, doesn't need to be exact precision.
                columnMetadata.put(positionInTable, new ColumnMetadata(colName, jdbcType, 50, scale, dateTimeFormatter));
                break;

            // Redirect SQLXML as LONGNVARCHAR
            // SQLXML is not valid type in TDS
            case java.sql.Types.SQLXML:
                columnMetadata.put(positionInTable, new ColumnMetadata(colName, java.sql.Types.LONGNVARCHAR, precision, scale, dateTimeFormatter));
                break;

            // Redirecting Float as Double based on data type mapping
            // https://msdn.microsoft.com/en-us/library/ms378878%28v=sql.110%29.aspx
            case java.sql.Types.FLOAT:
                columnMetadata.put(positionInTable, new ColumnMetadata(colName, java.sql.Types.DOUBLE, precision, scale, dateTimeFormatter));
                break;

            // redirecting BOOLEAN as BIT
            case java.sql.Types.BOOLEAN:
                columnMetadata.put(positionInTable, new ColumnMetadata(colName, java.sql.Types.BIT, precision, scale, dateTimeFormatter));
                break;

            default:
                columnMetadata.put(positionInTable, new ColumnMetadata(colName, jdbcType, precision, scale, dateTimeFormatter));
        }

        loggerExternal.exiting(loggerClassName, "addColumnMetadata");
    }

    /**
     * Set the format for reading in dates from the file.
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
     * Set the format for reading in dates from the file.
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
     * Set the format for reading in dates from the file.
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
     * Set the format for reading in dates from the file.
     * 
     * @param dateTimeFormatter
     *            format to parse data sent as java.sql.Types.TIME_WITH_TIMEZONE
     */
    public void setTimeWithTimezoneFormat(DateTimeFormatter dateTimeFormatter) {
        loggerExternal.entering(loggerClassName, "setTimeWithTimezoneFormat", new Object[] {dateTimeFormatter});

        this.timeFormatter = dateTimeFormatter;

        loggerExternal.exiting(loggerClassName, "setTimeWithTimezoneFormat");
    }

    /**
     * Releases any resources associated with the file reader.
     * 
     * @throws SQLServerException
     *             when an error occurs
     */
    public void close() throws SQLServerException {
    }

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

    @Override
    public Object[] getRowData() throws SQLServerException {
        
        Object[] data = new Object[columnMetadata.size()];
        
        if (null == columnList || columnList.size() == 0) {
            int valueIndex = 0;
            for (int i = 0; i < data.length; i++) {
                if (valueList.get(i).equalsIgnoreCase("?")) {
                    data[i] = batchParam.get(batchParamIndex)[valueIndex].getSetterValue();
                    valueIndex++;
                } else {
                    // remove 's at the beginning and end of the value, if it exists.
                    int len = valueList.get(i).length();
                    if (valueList.get(i).charAt(0) == '\'' && valueList.get(i).charAt(len - 1) == '\'') {
                        data[i] = valueList.get(i).substring(1, len - 1);
                    } else {
                        data[i] = valueList.get(i);
                    }
                }
            }
        } else {
            int valueIndex = 0;
            int columnListIndex = 0;
            for (int i = 0; i < data.length; i++) {
                if (columnList.size() > columnListIndex && columnList.get(columnListIndex).equals(columnMetadata.get(i + 1).columnName)) {
                    if (valueList.get(i).equalsIgnoreCase("?")) {
                        data[i] = batchParam.get(i)[valueIndex].getSetterValue();
                        valueIndex++;
                    } else {
                        // remove 's at the beginning and end of the value, if it exists.
                        int len = valueList.get(i).length();
                        if (valueList.get(i).charAt(0) == '\'' && valueList.get(i).charAt(len - 1) == '\'') {
                            data[i] = valueList.get(i).substring(1, len - 1);
                        } else {
                            data[i] = valueList.get(i);
                        }
                    }
                    columnListIndex++;
                } else {
                    data[i] = "";
                }
            }
        }

        // Cannot go directly from String[] to Object[] and expect it to act as an array.
        Object[] dataRow = new Object[data.length];

        for (Entry<Integer, ColumnMetadata> pair : columnMetadata.entrySet()) {
            ColumnMetadata cm = pair.getValue();

            if (data.length < pair.getKey() - 1) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidColumn"));
                Object[] msgArgs = {pair.getKey()};
                throw new SQLServerException(form.format(msgArgs), SQLState.COL_NOT_FOUND, DriverError.NOT_SET, null);
            }

            // Source header has more columns than current param read
            if (columnNames != null && (columnNames.length > data.length)) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_CSVDataSchemaMismatch"));
                Object[] msgArgs = {};
                throw new SQLServerException(form.format(msgArgs), SQLState.COL_NOT_FOUND, DriverError.NOT_SET, null);
            }

            try {
                if (0 == data[pair.getKey() - 1].toString().length()) {
                    dataRow[pair.getKey() - 1] = null;
                    continue;
                }

                switch (cm.columnType) {
                    /*
                     * Both BCP and BULK INSERT considers double quotes as part of the data and throws error if any data (say "10") is to be
                     * inserted into an numeric column. Our implementation does the same.
                     */
                    case Types.INTEGER: {
                        // Formatter to remove the decimal part as SQL Server floors the decimal in integer types
                        DecimalFormat decimalFormatter = new DecimalFormat("#");
                        String formatedfInput = decimalFormatter.format(Double.parseDouble(data[pair.getKey() - 1].toString()));
                        dataRow[pair.getKey() - 1] = Integer.valueOf(formatedfInput);
                        break;
                    }

                    case Types.TINYINT:
                    case Types.SMALLINT: {
                        // Formatter to remove the decimal part as SQL Server floors the decimal in integer types
                        DecimalFormat decimalFormatter = new DecimalFormat("#");
                        String formatedfInput = decimalFormatter.format(Double.parseDouble(data[pair.getKey() - 1].toString()));
                        dataRow[pair.getKey() - 1] = Short.valueOf(formatedfInput);
                        break;
                    }

                    case Types.BIGINT: {
                        BigDecimal bd = new BigDecimal(data[pair.getKey() - 1].toString().trim());
                        try {
                            dataRow[pair.getKey() - 1] = bd.setScale(0, RoundingMode.DOWN).longValueExact();
                        } catch (ArithmeticException ex) {
                            String value = "'" + data[pair.getKey() - 1] + "'";
                            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorConvertingValue"));
                            throw new SQLServerException(form.format(new Object[]{value, JDBCType.of(cm.columnType)}), null, 0, ex);
                        }
                        break;
                    }

                    case Types.DECIMAL:
                    case Types.NUMERIC: {
                        BigDecimal bd = new BigDecimal(data[pair.getKey() - 1].toString().trim());
                        dataRow[pair.getKey() - 1] = bd.setScale(cm.scale, RoundingMode.HALF_UP);
                        break;
                    }

                    case Types.BIT: {
                        // "true" => 1, "false" => 0
                        // Any non-zero value (integer/double) => 1, 0/0.0 => 0
                        try {
                            dataRow[pair.getKey() - 1] = (0 == Double.parseDouble(data[pair.getKey() - 1].toString())) ? Boolean.FALSE : Boolean.TRUE;
                        } catch (NumberFormatException e) {
                            dataRow[pair.getKey() - 1] = Boolean.parseBoolean(data[pair.getKey() - 1].toString());
                        }
                        break;
                    }

                    case Types.REAL: {
                        dataRow[pair.getKey() - 1] = Float.parseFloat(data[pair.getKey() - 1].toString());
                        break;
                    }

                    case Types.DOUBLE: {
                        dataRow[pair.getKey() - 1] = Double.parseDouble(data[pair.getKey() - 1].toString());
                        break;
                    }

                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB: {
                        /*
                         * For binary data, the value in file may or may not have the '0x' prefix. We will try to match our implementation with
                         * 'BULK INSERT' except that we will allow 0x prefix whereas 'BULK INSERT' command does not allow 0x prefix. A BULK INSERT
                         * example: A sample csv file containing data for 2 binary columns and 1 row: 61,62 Table definition: create table t1(c1
                         * varbinary(10), c2 varbinary(10)) BULK INSERT command: bulk insert t1 from 'C:\in.csv'
                         * with(DATAFILETYPE='char',firstrow=1,FIELDTERMINATOR=',') select * from t1 shows 1 row with columns: 0x61, 0x62
                         */
                        // Strip off 0x if present.
                        String binData = data[pair.getKey() - 1].toString().trim();
                        if (binData.startsWith("0x") || binData.startsWith("0X")) {
                            dataRow[pair.getKey() - 1] = binData.substring(2);
                        } else {
                            dataRow[pair.getKey() - 1] = binData;
                        }
                        break;
                    }

                    case 2013:    // java.sql.Types.TIME_WITH_TIMEZONE
                    {
                        DriverJDBCVersion.checkSupportsJDBC42();
                        OffsetTime offsetTimeValue;

                        // The per-column DateTimeFormatter gets priority.
                        if (null != cm.dateTimeFormatter)
                            offsetTimeValue = OffsetTime.parse(data[pair.getKey() - 1].toString(), cm.dateTimeFormatter);
                        else if (timeFormatter != null)
                            offsetTimeValue = OffsetTime.parse(data[pair.getKey() - 1].toString(), timeFormatter);
                        else
                            offsetTimeValue = OffsetTime.parse(data[pair.getKey() - 1].toString());

                        dataRow[pair.getKey() - 1] = offsetTimeValue;
                        break;
                    }

                    case 2014: // java.sql.Types.TIMESTAMP_WITH_TIMEZONE
                    {
                        DriverJDBCVersion.checkSupportsJDBC42();
                        OffsetDateTime offsetDateTimeValue;

                        // The per-column DateTimeFormatter gets priority.
                        if (null != cm.dateTimeFormatter)
                            offsetDateTimeValue = OffsetDateTime.parse(data[pair.getKey() - 1].toString(), cm.dateTimeFormatter);
                        else if (dateTimeFormatter != null)
                            offsetDateTimeValue = OffsetDateTime.parse(data[pair.getKey() - 1].toString(), dateTimeFormatter);
                        else
                            offsetDateTimeValue = OffsetDateTime.parse(data[pair.getKey() - 1].toString());

                        dataRow[pair.getKey() - 1] = offsetDateTimeValue;
                        break;
                    }

                    case Types.NULL: {
                        dataRow[pair.getKey() - 1] = null;
                        break;
                    }

                    case Types.DATE:
                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.CLOB:
                    default: {
                        // The string is copied as is.
                        dataRow[pair.getKey() - 1] = data[pair.getKey() - 1];
                        break;
                    }
                }
            } catch (IllegalArgumentException e) {
                String value = "'" + data[pair.getKey() - 1] + "'";
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorConvertingValue"));
                throw new SQLServerException(form.format(new Object[]{value, JDBCType.of(cm.columnType)}), null, 0, e);
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new SQLServerException(SQLServerException.getErrString("R_CSVDataSchemaMismatch"), e);
            }
            
        }
        return dataRow;
    }
    
    @Override
    public boolean next() throws SQLServerException {
        batchParamIndex++;
        return batchParamIndex < batchParam.size();
    }

    /*
     * Helper method to throw a SQLServerExeption with the invalidArgument message and given argument.
     */
    private void throwInvalidArgument(String argument) throws SQLServerException {
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidArgument"));
        Object[] msgArgs = {argument};
        SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, false);
    }

    /*
     * Method to throw a SQLServerExeption for duplicate column names
     */
    private void checkDuplicateColumnName(int positionInTable,
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
