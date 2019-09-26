/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.math.BigDecimal;
import java.text.MessageFormat;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;


/**
 * Represents the data table for SQL Server.
 */
public final class SQLServerDataTable {

    int rowCount = 0;
    int columnCount = 0;
    Map<Integer, SQLServerDataColumn> columnMetadata = null;
    Set<String> columnNames = null;
    Map<Integer, Object[]> rows = null;
    private String tvpName = null;

    /**
     * The constant in the Java programming language, sometimes referred to as a type code, that identifies the type
     * TVP.
     * 
     * @throws SQLServerException
     *         when an error occurs
     */
    // Name used in CREATE TYPE
    public SQLServerDataTable() throws SQLServerException {
        columnMetadata = new LinkedHashMap<>();
        columnNames = new HashSet<>();
        rows = new HashMap<>();
    }

    /**
     * Clears this data table.
     */
    public synchronized void clear() {
        rowCount = 0;
        columnCount = 0;
        columnMetadata.clear();
        columnNames.clear();
        rows.clear();
    }

    /**
     * Returns an iterator on the rows of the data table.
     * 
     * @return an iterator on the rows of the data table.
     */
    public synchronized Iterator<Entry<Integer, Object[]>> getIterator() {
        if ((null != rows) && (null != rows.entrySet())) {
            return rows.entrySet().iterator();
        }
        return null;
    }

    /**
     * Adds meta data for the specified column.
     * 
     * @param columnName
     *        the name of the column
     * @param sqlType
     *        the sql type of the column
     * @throws SQLServerException
     *         when an error occurs
     */
    public synchronized void addColumnMetadata(String columnName, int sqlType) throws SQLServerException {
        // column names must be unique
        Util.checkDuplicateColumnName(columnName, columnNames);
        columnMetadata.put(columnCount++, new SQLServerDataColumn(columnName, sqlType));
    }

    /**
     * Adds meta data for the specified column.
     * 
     * @param column
     *        the name of the column
     * @throws SQLServerException
     *         when an error occurs
     */
    public synchronized void addColumnMetadata(SQLServerDataColumn column) throws SQLServerException {
        // column names must be unique
        Util.checkDuplicateColumnName(column.columnName, columnNames);
        columnMetadata.put(columnCount++, column);
    }

    /**
     * Adds one row of data to the data table.
     * 
     * @param values
     *        values to be added in one row of data to the data table.
     * @throws SQLServerException
     *         when an error occurs
     */
    public synchronized void addRow(Object... values) throws SQLServerException {
        try {
            int columnCount = columnMetadata.size();

            if ((null != values) && values.length > columnCount) {
                MessageFormat form = new MessageFormat(
                        SQLServerException.getErrString("R_moreDataInRowThanColumnInTVP"));
                Object[] msgArgs = {};
                throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
            }

            Iterator<Entry<Integer, SQLServerDataColumn>> columnsIterator = columnMetadata.entrySet().iterator();
            Object[] rowValues = new Object[columnCount];
            int currentColumn = 0;
            while (columnsIterator.hasNext()) {
                Object val = null;

                if ((null != values) && (currentColumn < values.length) && (null != values[currentColumn]))
                    val = values[currentColumn];
                currentColumn++;
                Map.Entry<Integer, SQLServerDataColumn> pair = columnsIterator.next();
                JDBCType jdbcType = JDBCType.of(pair.getValue().javaSqlType);
                internalAddrow(jdbcType, val, rowValues, pair);
            }
            rows.put(rowCount++, rowValues);
        } catch (NumberFormatException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_TVPInvalidColumnValue"), e);
        } catch (ClassCastException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_TVPInvalidColumnValue"), e);
        }

    }

    /**
     * Adding rows one row of data to data table.
     * 
     * @param jdbcType
     *        The jdbcType
     * @param val
     *        The data value
     * @param rowValues
     *        Row of data
     * @param pair
     *        pair to be added to data table
     * @throws SQLServerException
     *         when an error occurs
     */
    private void internalAddrow(JDBCType jdbcType, Object val, Object[] rowValues,
            Map.Entry<Integer, SQLServerDataColumn> pair) throws SQLServerException {
        int key = pair.getKey();

        if (null != val) {
            SQLServerDataColumn currentColumnMetadata = pair.getValue();
            int nValueLen;

            switch (jdbcType) {
                case BIGINT:
                    rowValues[key] = (val instanceof Long) ? val : Long.parseLong(val.toString());
                    break;

                case BIT:
                    if (val instanceof Boolean) {
                        rowValues[key] = val;
                    } else {
                        String valString = val.toString();

                        if ("0".equals(valString) || valString.equalsIgnoreCase(Boolean.FALSE.toString())) {
                            rowValues[key] = Boolean.FALSE;
                        } else if ("1".equals(valString) || valString.equalsIgnoreCase(Boolean.TRUE.toString())) {
                            rowValues[key] = Boolean.TRUE;
                        } else {
                            MessageFormat form = new MessageFormat(
                                    SQLServerException.getErrString("R_TVPInvalidColumnValue"));
                            Object[] msgArgs = {jdbcType};
                            throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
                        }
                    }
                    break;

                case INTEGER:
                    rowValues[key] = (val instanceof Integer) ? val : Integer.parseInt(val.toString());
                    break;

                case SMALLINT:
                case TINYINT:
                    rowValues[key] = (val instanceof Short) ? val : Short.parseShort(val.toString());
                    break;

                case DECIMAL:
                case NUMERIC:
                    BigDecimal bd = null;
                    boolean isColumnMetadataUpdated = false;
                    bd = new BigDecimal(val.toString());
                    // BigDecimal#precision returns number of digits in the unscaled value.
                    // Say, for value 0.01, it returns 1 but the precision should be 3 for SQLServer
                    int precision = Util.getValueLengthBaseOnJavaType(bd, JavaType.of(bd), null, null, jdbcType);
                    if (bd.scale() > currentColumnMetadata.scale) {
                        currentColumnMetadata.scale = bd.scale();
                        isColumnMetadataUpdated = true;
                    }
                    if (precision > currentColumnMetadata.precision) {
                        currentColumnMetadata.precision = precision;
                        isColumnMetadataUpdated = true;
                    }

                    // precision equal: the maximum number of digits in integer part + the maximum scale
                    int numberOfDigitsIntegerPart = precision - bd.scale();
                    if (numberOfDigitsIntegerPart > currentColumnMetadata.numberOfDigitsIntegerPart) {
                        currentColumnMetadata.numberOfDigitsIntegerPart = numberOfDigitsIntegerPart;
                        isColumnMetadataUpdated = true;
                    }

                    if (isColumnMetadataUpdated) {
                        currentColumnMetadata.precision = currentColumnMetadata.scale
                                + currentColumnMetadata.numberOfDigitsIntegerPart;
                        columnMetadata.put(pair.getKey(), currentColumnMetadata);
                    }
                    rowValues[key] = bd;
                    break;

                case DOUBLE:
                    rowValues[key] = (val instanceof Double) ? val : Double.parseDouble(val.toString());
                    break;

                case FLOAT:
                case REAL:
                    rowValues[key] = (val instanceof Float) ? val : Float.parseFloat(val.toString());
                    break;

                case TIMESTAMP_WITH_TIMEZONE:
                case TIME_WITH_TIMEZONE:
                case DATE:
                case TIME:
                case TIMESTAMP:
                case DATETIMEOFFSET:
                case DATETIME:
                case SMALLDATETIME:
                    // Sending temporal types as string. Error from database is thrown if parsing fails
                    // no need to send precision for temporal types, string literal will never exceed
                    // DataTypes.SHORT_VARTYPE_MAX_BYTES

                    // java.sql.Date, java.sql.Time and java.sql.Timestamp are subclass of java.util.Date
                    if (val instanceof java.util.Date || val instanceof microsoft.sql.DateTimeOffset
                            || val instanceof OffsetDateTime || val instanceof OffsetTime)
                        rowValues[key] = val.toString();
                    else
                        rowValues[key] = val;
                    break;

                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                    nValueLen = ((byte[]) val).length;

                    if (nValueLen > currentColumnMetadata.precision) {
                        currentColumnMetadata.precision = nValueLen;
                        columnMetadata.put(pair.getKey(), currentColumnMetadata);
                    }
                    rowValues[key] = val;
                    break;

                case CHAR:
                case VARCHAR:
                case NCHAR:
                case NVARCHAR:
                case LONGVARCHAR:
                case LONGNVARCHAR:
                case SQLXML:
                    if (val instanceof UUID)
                        val = val.toString();
                    nValueLen = (2 * ((String) val).length());

                    if (nValueLen > currentColumnMetadata.precision) {
                        currentColumnMetadata.precision = nValueLen;
                        columnMetadata.put(pair.getKey(), currentColumnMetadata);
                    }
                    rowValues[key] = val;
                    break;

                case SQL_VARIANT:
                    JDBCType internalJDBCType;
                    JavaType javaType = JavaType.of(val);
                    internalJDBCType = javaType.getJDBCType(SSType.UNKNOWN, jdbcType);
                    internalAddrow(internalJDBCType, val, rowValues, pair);
                    break;

                default:
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unsupportedDataTypeTVP"));
                    Object[] msgArgs = {jdbcType};
                    throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
            }
        } else {
            rowValues[key] = null;
            if (jdbcType == JDBCType.SQL_VARIANT) {
                throw new SQLServerException(SQLServerException.getErrString("R_invalidValueForTVPWithSQLVariant"),
                        null);
            }
        }
    }

    /**
     * Returns the <code>java.util.Map</code> object type of columnMetaData for all columns where column indexes are
     * mapped with their respective {@link SQLServerDataColumn} Java object.
     * 
     * @return Map
     */
    public synchronized Map<Integer, SQLServerDataColumn> getColumnMetadata() {
        return columnMetadata;
    }

    /**
     * Returns name of TVP type set by {@link #setTvpName(String)}.
     * 
     * @return tvpName
     */
    public String getTvpName() {
        return tvpName;
    }

    /**
     * Sets the TVP Name.
     * 
     * @param tvpName
     *        the name of TVP
     */
    public void setTvpName(String tvpName) {
        this.tvpName = tvpName;
    }

    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + rowCount;
        hash = 31 * hash + columnCount;
        hash = 31 * hash + (null != columnMetadata ? columnMetadata.hashCode() : 0);
        hash = 31 * hash + (null != columnNames ? columnNames.hashCode() : 0);
        hash = 31 * hash + getRowsHashCode();
        hash = 31 * hash + (null != tvpName ? tvpName.hashCode() : 0);
        return hash;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (null != object && object.getClass() == SQLServerDataTable.class) {
            SQLServerDataTable aSQLServerDataTable = (SQLServerDataTable) object;
            if (hashCode() == aSQLServerDataTable.hashCode()) {

                // Compare objects to avoid collision
                boolean equalColumnMetadata = columnMetadata.equals(aSQLServerDataTable.columnMetadata);
                boolean equalColumnNames = columnNames.equals(aSQLServerDataTable.columnNames);
                boolean equalRowData = compareRows(aSQLServerDataTable.rows);

                return (rowCount == aSQLServerDataTable.rowCount && columnCount == aSQLServerDataTable.columnCount
                        && tvpName == aSQLServerDataTable.tvpName && equalColumnMetadata && equalColumnNames
                        && equalRowData);
            }
        }
        return false;
    }

    private int getRowsHashCode() {
        if (null == rows) {
            return 0;
        }
        int h = 0;
        for (Entry<Integer, Object[]> entry : rows.entrySet()) {
            h += entry.getKey() ^ Arrays.hashCode(entry.getValue());
        }
        return h;
    }

    private boolean compareRows(Map<Integer, Object[]> otherRows) {
        if (rows == otherRows) {
            return true;
        }
        if (rows.size() != otherRows.size()) {
            return false;
        }
        try {
            for (Entry<Integer, Object[]> e : rows.entrySet()) {
                Integer key = e.getKey();
                Object[] value = e.getValue();
                if (null == value) {
                    if (!(null == otherRows.get(key) && otherRows.containsKey(key))) {
                        return false;
                    }
                } else {
                    if (!Arrays.equals(value, otherRows.get(key))) {
                        return false;
                    }
                }
            }
        } catch (ClassCastException unused) {
            return false;
        } catch (NullPointerException unused) {
            return false;
        }
        return true;
    }
}
