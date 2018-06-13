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
import java.util.Map.Entry;
import java.util.Set;

/**
 * A simple implementation of the ISQLServerBulkRecord interface that can be used to read in the basic Java data types from an ArrayList of Parameters
 * that were provided by pstmt/cstmt.
 */
public class SQLServerBulkBatchInsertRecord extends SQLServerBulkCommon implements ISQLServerBulkRecord, java.lang.AutoCloseable {

    private ArrayList<Parameter[]> batchParam;
    private int batchParamIndex = -1;
    private ArrayList<String> columnList;
    private ArrayList<String> valueList;

    public SQLServerBulkBatchInsertRecord(ArrayList<Parameter[]> batchParam,
            ArrayList<String> columnList,
            ArrayList<String> valueList,
            String encoding) throws SQLServerException {
        loggerClassName = "com.microsoft.sqlserver.jdbc.SQLServerBulkBatchInsertRecord";
        loggerExternal = java.util.logging.Logger.getLogger(loggerClassName);
        loggerExternal.entering(loggerClassName, "SQLServerBulkBatchInsertRecord", new Object[] {batchParam, encoding});

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
     * Releases any resources associated with the batch.
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

    private Object convertValue(ColumnMetadata cm,
            Object data) throws SQLServerException {
        switch (cm.columnType) {
            /*
             * Both BCP and BULK INSERT considers double quotes as part of the data and throws error if any data (say "10") is to be inserted into an
             * numeric column. Our implementation does the same.
             */
            case Types.INTEGER: {
                // Formatter to remove the decimal part as SQL Server floors the decimal in integer types
                DecimalFormat decimalFormatter = new DecimalFormat("#");
                decimalFormatter.setRoundingMode(RoundingMode.DOWN);
                String formatedfInput = decimalFormatter.format(Double.parseDouble(data.toString()));
                return Integer.valueOf(formatedfInput);
            }

            case Types.TINYINT:
            case Types.SMALLINT: {
                // Formatter to remove the decimal part as SQL Server floors the decimal in integer types
                DecimalFormat decimalFormatter = new DecimalFormat("#");
                decimalFormatter.setRoundingMode(RoundingMode.DOWN);
                String formatedfInput = decimalFormatter.format(Double.parseDouble(data.toString()));
                return Short.valueOf(formatedfInput);
            }

            case Types.BIGINT: {
                BigDecimal bd = new BigDecimal(data.toString().trim());
                try {
                    return bd.setScale(0, RoundingMode.DOWN).longValueExact();
                }
                catch (ArithmeticException ex) {
                    String value = "'" + data + "'";
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorConvertingValue"));
                    throw new SQLServerException(form.format(new Object[] {value, JDBCType.of(cm.columnType)}), null, 0, ex);
                }
            }

            case Types.DECIMAL:
            case Types.NUMERIC: {
                BigDecimal bd = new BigDecimal(data.toString().trim());
                return bd.setScale(cm.scale, RoundingMode.HALF_UP);
            }

            case Types.BIT: {
                // "true" => 1, "false" => 0
                // Any non-zero value (integer/double) => 1, 0/0.0 => 0
                try {
                    return (0 == Double.parseDouble(data.toString())) ? Boolean.FALSE : Boolean.TRUE;
                }
                catch (NumberFormatException e) {
                    return Boolean.parseBoolean(data.toString());
                }
            }

            case Types.REAL: {
                return Float.parseFloat(data.toString());
            }

            case Types.DOUBLE: {
                return Double.parseDouble(data.toString());
            }

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
            case Types.BLOB: {
                // Strip off 0x if present.
                String binData = data.toString().trim();
                if (binData.startsWith("0x") || binData.startsWith("0X")) {
                    return binData.substring(2);
                }
                else {
                    return binData;
                }
            }

            case 2013:    // java.sql.Types.TIME_WITH_TIMEZONE
            {
                OffsetTime offsetTimeValue;

                // The per-column DateTimeFormatter gets priority.
                if (null != cm.dateTimeFormatter)
                    offsetTimeValue = OffsetTime.parse(data.toString(), cm.dateTimeFormatter);
                else if (timeFormatter != null)
                    offsetTimeValue = OffsetTime.parse(data.toString(), timeFormatter);
                else
                    offsetTimeValue = OffsetTime.parse(data.toString());

                return offsetTimeValue;
            }

            case 2014: // java.sql.Types.TIMESTAMP_WITH_TIMEZONE
            {
                OffsetDateTime offsetDateTimeValue;

                // The per-column DateTimeFormatter gets priority.
                if (null != cm.dateTimeFormatter)
                    offsetDateTimeValue = OffsetDateTime.parse(data.toString(), cm.dateTimeFormatter);
                else if (dateTimeFormatter != null)
                    offsetDateTimeValue = OffsetDateTime.parse(data.toString(), dateTimeFormatter);
                else
                    offsetDateTimeValue = OffsetDateTime.parse(data.toString());

                return offsetDateTimeValue;
            }

            case Types.NULL: {
                return null;
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
                return data;
            }
        }
    }

    private String removeSingleQuote(String s) {
        int len = s.length();
        return (s.charAt(0) == '\'' && s.charAt(len - 1) == '\'') ? s.substring(1, len - 1) : s;
    }

    @Override
    public Object[] getRowData() throws SQLServerException {
        Object[] data = new Object[columnMetadata.size()];
        int valueIndex = 0;
        String valueData;
        Object rowData;
        int columnListIndex = 0;
        
        // check if the size of the list of values = size of the list of columns (which is optional)
        if (null != columnList && columnList.size() != valueList.size()) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_DataSchemaMismatch"));
            Object[] msgArgs = {};
            throw new SQLServerException(form.format(msgArgs), SQLState.COL_NOT_FOUND, DriverError.NOT_SET, null);
        }
        
        for (Entry<Integer, ColumnMetadata> pair : columnMetadata.entrySet()) {
            int index = pair.getKey() - 1;

            // To explain what each variable represents:
            // columnMetadata = map containing the ENTIRE list of columns in the table.
            // columnList = the *optional* list of columns the user can provide. For example, the (c1, c3) part of this query: INSERT into t1 (c1, c3) values (?, ?)
            // valueList = the *mandatory* list of columns the user needs provide. This is the (?, ?) part of the previous query. The size of this valueList will always equal the number of
            // the entire columns in the table IF columnList has NOT been provided. If columnList HAS been provided, then this valueList may be smaller than the list of all columns (which is columnMetadata).
            
            // case when the user has not provided the optional list of column names.
            if (null == columnList || columnList.size() == 0) {
                valueData = valueList.get(index);
                // if the user has provided a wildcard for this column, fetch the set value from the batchParam.
                if (valueData.equalsIgnoreCase("?")) {
                    rowData = batchParam.get(batchParamIndex)[valueIndex++].getSetterValue();
                }
                else if (valueData.equalsIgnoreCase("null")) {
                    rowData = null;
                }
                // if the user has provided a hardcoded value for this column, rowData is simply set to the hardcoded value.
                else {
                    rowData = removeSingleQuote(valueData);
                }
            }
            // case when the user has provided the optional list of column names.
            else {
                // columnListIndex is a separate counter we need to keep track of for each time we've processed a column
                // that the user provided.
                // for example, if the user provided an optional columnList of (c1, c3, c5, c7) in a table that has 8 columns (c1~c8),
                // then the columnListIndex would increment only when we're dealing with the four columns inside columnMetadata.
                // compare the list of the optional list of column names to the table's metadata, and match each other, so we assign the correct value to each column.
                if (columnList.size() > columnListIndex && columnList.get(columnListIndex).equalsIgnoreCase(columnMetadata.get(index + 1).columnName)) {
                    valueData = valueList.get(columnListIndex);
                    if (valueData.equalsIgnoreCase("?")) {
                        rowData = batchParam.get(batchParamIndex)[valueIndex++].getSetterValue();
                    }
                    else if (valueData.equalsIgnoreCase("null")) {
                        rowData = null;
                    }
                    else {
                        rowData = removeSingleQuote(valueData);
                    }
                    columnListIndex++;
                }
                else {
                    rowData = null;
                }
            }

            try {
                if (null == rowData) {
                    data[index] = null;
                    continue;
                } else if (0 == rowData.toString().length()) {
                    data[index] = "";
                    continue;
                }
                data[index] = convertValue(pair.getValue(), rowData);
            }
            catch (IllegalArgumentException e) {
                String value = "'" + rowData + "'";
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorConvertingValue"));
                throw new SQLServerException(form.format(new Object[] {value, JDBCType.of(pair.getValue().columnType)}), null, 0, e);
            }
            catch (ArrayIndexOutOfBoundsException e) {
                throw new SQLServerException(SQLServerException.getErrString("R_DataSchemaMismatch"), e);
            }
        }
        return data;
    }

    @Override
    public boolean next() throws SQLServerException {
        batchParamIndex++;
        return batchParamIndex < batchParam.size();
    }
}
