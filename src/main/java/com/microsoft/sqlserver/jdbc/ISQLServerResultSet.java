/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.sql.SQLType;

public interface ISQLServerResultSet extends java.sql.ResultSet {

    public static final int TYPE_SS_DIRECT_FORWARD_ONLY = 2003; // TYPE_FORWARD_ONLY + 1000
    public static final int TYPE_SS_SERVER_CURSOR_FORWARD_ONLY = 2004; // TYPE_FORWARD_ONLY + 1001
    public static final int TYPE_SS_SCROLL_STATIC = 1004; // TYPE_SCROLL_INSENSITIVE
    public static final int TYPE_SS_SCROLL_KEYSET = 1005; // TYPE_SCROLL_SENSITIVE
    public static final int TYPE_SS_SCROLL_DYNAMIC = 1006; // TYPE_SCROLL_SENSITIVE + 1

    /* SQL Server concurrency values */
    public static final int CONCUR_SS_OPTIMISTIC_CC = 1008; // CONCUR_UPDATABLE
    public static final int CONCUR_SS_SCROLL_LOCKS = 1009; // CONCUR_UPDATABLE + 1
    public static final int CONCUR_SS_OPTIMISTIC_CCVAL = 1010; // CONCUR_UPDATABLE + 2

    /**
     * Retrieves the value of the designated column as a microsoft.sql.DateTimeOffset object, given a zero-based column ordinal.
     * 
     * @param columnIndex
     *            The zero-based ordinal of a column.
     * @return A DateTimeOffset Class object.
     * @throws SQLException
     *             when an error occurs
     */
    public microsoft.sql.DateTimeOffset getDateTimeOffset(int columnIndex) throws SQLException;

    /**
     * Retrieves the value of the column specified as a microsoft.sql.DateTimeOffset object, given a column name.
     * 
     * @param columnName
     *            The name of a column.
     * @return A DateTimeOffset Class object.
     * @throws SQLException
     *             when an error occurs
     */
    public microsoft.sql.DateTimeOffset getDateTimeOffset(String columnName) throws SQLException;

    /**
     * Updates the value of the column specified to the DateTimeOffset Class value, given a zero-based column ordinal.
     * 
     * @param index
     *            The zero-based ordinal of a column.
     * @param x
     *            A DateTimeOffset Class object.
     * @throws SQLException
     *             when an error occurs
     */
    public void updateDateTimeOffset(int index,
            microsoft.sql.DateTimeOffset x) throws SQLException;

    /**
     * Updates the value of the column specified to the DateTimeOffset Class value, given a column name.
     * 
     * @param columnName
     *            The name of a column.
     * @param x
     *            A DateTimeOffset Class object.
     * @throws SQLException
     *             when an error occurs
     */
    public void updateDateTimeOffset(String columnName,
            microsoft.sql.DateTimeOffset x) throws SQLException;

    /**
     * Updates the designated column with an {@code Object} value.
     *
     * The updater methods are used to update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the {@code updateRow} or {@code insertRow} methods are called to update the database.
     *
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param x
     *            the new column value
     * @param precision
     *            the precision of the column
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             when an error occurs
     */
    public void updateObject(int index,
            Object x,
            int precision,
            int scale) throws SQLServerException;
    
    /**
     * Updates the designated column with an Object value. The updater methods are used to update column values in the current row or the insert row.
     * The updater methods do not update the underlying database; instead the updateRow or insertRow methods are called to update the database. If the
     * second argument is an InputStream then the stream must contain the number of bytes specified by scaleOrLength. If the second argument is a
     * Reader then the reader must contain the number of characters specified by scaleOrLength. If these conditions are not true the driver will
     * generate a SQLException when the statement is executed. The default implementation will throw SQLFeatureNotSupportedException
     * 
     * @param index
     *            the first column is 1, the second is 2, ...
     * @param obj
     *            the new column value
     * @param targetSqlType
     *            the SQL type to be sent to the database
     * @param scale
     *            for an object of java.math.BigDecimal , this is the number of digits after the decimal point. For Java Object types InputStream and
     *            Reader, this is the length of the data in the stream or reader. For all other types, this value will be ignored.
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement.If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateObject(int index,
            Object obj,
            SQLType targetSqlType,
            int scale,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * 
     * Updates the designated column with an Object value. The updater methods are used to update column values in the current row or the insert row.
     * The updater methods do not update the underlying database; instead the updateRow or insertRow methods are called to update the database. If the
     * second argument is an InputStream then the stream must contain the number of bytes specified by scaleOrLength. If the second argument is a
     * Reader then the reader must contain the number of characters specified by scaleOrLength. If these conditions are not true the driver will
     * generate a SQLException when the statement is executed. The default implementation will throw SQLFeatureNotSupportedException
     * 
     * @param columnName
     *            the label for the column specified with the SQL AS clause. If the SQL AS clause was not specified, then the label is the name of the
     *            column
     * @param obj
     *            the new column value
     * @param targetSqlType
     *            the SQL type to be sent to the database
     * @param scale
     *            for an object of java.math.BigDecimal , this is the number of digits after the decimal point. For Java Object types InputStream and
     *            Reader, this is the length of the data in the stream or reader. For all other types, this value will be ignored.
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement.If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             If any errors occur.
     */
    public void updateObject(String columnName,
            Object obj,
            SQLType targetSqlType,
            int scale,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateBoolean(int index,
            boolean x,
            boolean forceEncrypt) throws SQLException;
    
    public void updateByte(int index,
            byte x,
            boolean forceEncrypt) throws SQLException;
    
    public void updateShort(int index,
            short x,
            boolean forceEncrypt) throws SQLException;
    
    public void updateInt(int index,
            int x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateLong(int index,
            long x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateFloat(int index,
            float x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateDouble(int index,
            double x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateMoney(int index,
            BigDecimal x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateMoney(String columnName,
            BigDecimal x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateSmallMoney(int index,
            BigDecimal x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateSmallMoney(String columnName,
            BigDecimal x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateBigDecimal(int index,
            BigDecimal x,
            Integer precision,
            Integer scale) throws SQLServerException;
    
    public void updateBigDecimal(int index,
            BigDecimal x,
            Integer precision,
            Integer scale,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateString(int columnIndex,
            String stringValue,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateNString(int columnIndex,
            String nString,
            boolean forceEncrypt) throws SQLException;
    
    public void updateNString(String columnLabel,
            String nString,
            boolean forceEncrypt) throws SQLException;
    
    public void updateBytes(int index,
            byte x[],
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateDate(int index,
            java.sql.Date x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateTime(int index,
            java.sql.Time x,
            Integer scale) throws SQLServerException;
    
    public void updateTime(int index,
            java.sql.Time x,
            Integer scale,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateTimestamp(int index,
            java.sql.Timestamp x,
            int scale) throws SQLServerException;
    
    public void updateTimestamp(int index,
            java.sql.Timestamp x,
            int scale,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateDateTime(int index,
            java.sql.Timestamp x) throws SQLServerException;
    
    public void updateDateTime(int index,
            java.sql.Timestamp x,
            Integer scale) throws SQLServerException;
    
    public void updateDateTime(int index,
            java.sql.Timestamp x,
            Integer scale,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateSmallDateTime(int index,
            java.sql.Timestamp x) throws SQLServerException;
    
    public void updateSmallDateTime(int index,
            java.sql.Timestamp x,
            Integer scale) throws SQLServerException;
    
    public void updateSmallDateTime(int index,
            java.sql.Timestamp x,
            Integer scale,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateDateTimeOffset(int index,
            microsoft.sql.DateTimeOffset x,
            Integer scale) throws SQLException;
    
    public void updateDateTimeOffset(int index,
            microsoft.sql.DateTimeOffset x,
            Integer scale,
            boolean forceEncrypt) throws SQLException;
    
    public void updateUniqueIdentifier(int index,
            String x) throws SQLException;
    
    public void updateUniqueIdentifier(int index,
            String x,
            boolean forceEncrypt) throws SQLException;
    
    public void updateBoolean(String columnName,
            boolean x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateByte(String columnName,
            byte x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateShort(String columnName,
            short x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateInt(String columnName,
            int x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateLong(String columnName,
            long x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateFloat(String columnName,
            float x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateDouble(String columnName,
            double x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateBigDecimal(String columnName,
            BigDecimal x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateBigDecimal(String columnName,
            BigDecimal x,
            Integer precision,
            Integer scale) throws SQLServerException;
    
    public void updateBigDecimal(String columnName,
            BigDecimal x,
            Integer precision,
            Integer scale,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateString(String columnName,
            String x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateBytes(String columnName,
            byte x[],
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateDate(String columnName,
            java.sql.Date x,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateTime(String columnName,
            java.sql.Time x,
            int scale) throws SQLServerException;
    
    public void updateTime(String columnName,
            java.sql.Time x,
            int scale,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateTimestamp(String columnName,
            java.sql.Timestamp x,
            int scale) throws SQLServerException;
    
    public void updateTimestamp(String columnName,
            java.sql.Timestamp x,
            int scale,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateDateTime(String columnName,
            java.sql.Timestamp x,
            int scale) throws SQLServerException;
    
    public void updateDateTime(String columnName,
            java.sql.Timestamp x,
            int scale,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateSmallDateTime(String columnName,
            java.sql.Timestamp x,
            int scale) throws SQLServerException;
    
    public void updateSmallDateTime(String columnName,
            java.sql.Timestamp x,
            int scale,
            boolean forceEncrypt) throws SQLServerException;
    
    public void updateDateTimeOffset(String columnName,
            microsoft.sql.DateTimeOffset x,
            int scale) throws SQLException;
    
    public void updateDateTimeOffset(String columnName,
            microsoft.sql.DateTimeOffset x,
            int scale,
            boolean forceEncrypt) throws SQLException;
    
    public void updateUniqueIdentifier(String columnName,
            String x,
            boolean forceEncrypt) throws SQLException;
    
    public void updateObject(String columnName,
            Object x,
            int precision,
            int scale) throws SQLServerException;
    
    public void updateObject(String columnName,
            Object x,
            int precision,
            int scale,
            boolean forceEncrypt) throws SQLServerException;
    
    
}
