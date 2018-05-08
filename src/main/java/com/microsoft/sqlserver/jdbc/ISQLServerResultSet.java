/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

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

    public void updateObject(int index,
            Object obj,
            SQLType targetSqlType) throws SQLServerException;

    public void updateObject(int index,
            Object obj,
            SQLType targetSqlType,
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

    public void updateObject(String columnName,
            Object obj,
            SQLType targetSqlType,
            int scale) throws SQLServerException;

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

    public void updateObject(String columnName,
            Object obj,
            SQLType targetSqlType) throws SQLServerException;
}
