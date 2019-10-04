/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.math.BigDecimal;
import java.sql.SQLType;
import java.util.Calendar;

import com.microsoft.sqlserver.jdbc.dataclassification.SensitivityClassification;


/**
 * Provides an interface to the {@link SQLServerResultSet} class.
 */
public interface ISQLServerResultSet extends java.sql.ResultSet {

    int TYPE_SS_DIRECT_FORWARD_ONLY = 2003; // TYPE_FORWARD_ONLY + 1000
    int TYPE_SS_SERVER_CURSOR_FORWARD_ONLY = 2004; // TYPE_FORWARD_ONLY + 1001
    int TYPE_SS_SCROLL_STATIC = 1004; // TYPE_SCROLL_INSENSITIVE
    int TYPE_SS_SCROLL_KEYSET = 1005; // TYPE_SCROLL_SENSITIVE
    int TYPE_SS_SCROLL_DYNAMIC = 1006; // TYPE_SCROLL_SENSITIVE + 1

    /* SQL Server concurrency values */
    int CONCUR_SS_OPTIMISTIC_CC = 1008; // CONCUR_UPDATABLE
    int CONCUR_SS_SCROLL_LOCKS = 1009; // CONCUR_UPDATABLE + 1
    int CONCUR_SS_OPTIMISTIC_CCVAL = 1010; // CONCUR_UPDATABLE + 2

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a
     * com.microsoft.sqlserver.jdbc.Geometry object in the Java programming language.
     * 
     * @param columnIndex
     *        the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    Geometry getGeometry(int columnIndex) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a
     * com.microsoft.sqlserver.jdbc.Geometry object in the Java programming language.
     * 
     * @param columnName
     *        the name of the column
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    Geometry getGeometry(String columnName) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a
     * com.microsoft.sqlserver.jdbc.Geography object in the Java programming language.
     * 
     * @param columnIndex
     *        the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    Geography getGeography(int columnIndex) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a
     * com.microsoft.sqlserver.jdbc.Geography object in the Java programming language.
     * 
     * @param columnName
     *        the name of the column
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    Geography getGeography(String columnName) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a String object in the
     * Java programming language.
     * 
     * @param columnIndex
     *        the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    String getUniqueIdentifier(int columnIndex) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a String object in the
     * Java programming language.
     * 
     * @param columnLabel
     *        the name of the column
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    String getUniqueIdentifier(String columnLabel) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp
     * object in the Java programming language.
     * 
     * @param columnIndex
     *        the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    java.sql.Timestamp getDateTime(int columnIndex) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp
     * object in the Java programming language.
     * 
     * @param columnName
     *        is the name of the column
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         If any errors occur.
     */
    java.sql.Timestamp getDateTime(String columnName) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp
     * object in the Java programming language. This method uses the given calendar to construct an appropriate
     * millisecond value for the timestamp if the underlying database does not store timezone information.
     * 
     * @param columnIndex
     *        the first column is 1, the second is 2, ...
     * @param cal
     *        the java.util.Calendar object to use in constructing the dateTime
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         If any errors occur.
     */
    java.sql.Timestamp getDateTime(int columnIndex, Calendar cal) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp
     * object in the Java programming language. This method uses the given calendar to construct an appropriate
     * millisecond value for the timestamp if the underlying database does not store timezone information.
     * 
     * @param colName
     *        the label for the column specified with the SQL AS clause. If the SQL AS clause was not specified, then
     *        the label is the name of the column
     * @param cal
     *        the java.util.Calendar object to use in constructing the dateTime
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         If any errors occur.
     */
    java.sql.Timestamp getDateTime(String colName, Calendar cal) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp
     * object in the Java programming language.
     * 
     * @param columnIndex
     *        the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    java.sql.Timestamp getSmallDateTime(int columnIndex) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp
     * object in the Java programming language.
     * 
     * @param columnName
     *        is the name of a column.
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         If any errors occur.
     */
    java.sql.Timestamp getSmallDateTime(String columnName) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp
     * object in the Java programming language.
     * 
     * @param columnIndex
     *        the first column is 1, the second is 2, ...
     * @param cal
     *        the java.util.Calendar object to use in constructing the smalldateTime
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         If any errors occur.
     */
    java.sql.Timestamp getSmallDateTime(int columnIndex, Calendar cal) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp
     * object in the Java programming language.
     * 
     * @param colName
     *        The name of a column
     * @param cal
     *        the java.util.Calendar object to use in constructing the smalldateTime
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         If any errors occur.
     */
    java.sql.Timestamp getSmallDateTime(String colName, Calendar cal) throws SQLServerException;

    /**
     * Returns the value of the designated column as a microsoft.sql.DateTimeOffset object, given a zero-based column
     * ordinal.
     * 
     * @param columnIndex
     *        The zero-based ordinal of a column.
     * @return A DateTimeOffset Class object.
     * @throws SQLServerException
     *         when an error occurs
     */
    microsoft.sql.DateTimeOffset getDateTimeOffset(int columnIndex) throws SQLServerException;

    /**
     * Returns the value of the column specified as a microsoft.sql.DateTimeOffset object, given a column name.
     * 
     * @param columnName
     *        The name of a column.
     * @return A DateTimeOffset Class object.
     * @throws SQLServerException
     *         when an error occurs
     */
    microsoft.sql.DateTimeOffset getDateTimeOffset(String columnName) throws SQLServerException;

    /**
     * Returns the value of the column specified as a java.math.BigDecimal object.
     * 
     * @param columnIndex
     *        The zero-based ordinal of a column.
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    BigDecimal getMoney(int columnIndex) throws SQLServerException;

    /**
     * Returns the value of the column specified as a java.math.BigDecimal object.
     * 
     * @param columnName
     *        is the name of a column.
     * @return the column value; if the value is SQL NULL, the value returned is null.
     * @throws SQLServerException
     *         If any errors occur.
     */
    BigDecimal getMoney(String columnName) throws SQLServerException;

    /**
     * Returns the value of the column specified as a java.math.BigDecimal object.
     * 
     * @param columnIndex
     *        The zero-based ordinal of a column.
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         If any errors occur.
     */
    BigDecimal getSmallMoney(int columnIndex) throws SQLServerException;

    /**
     * Returns the value of the column specified as a java.math.BigDecimal object.
     * 
     * @param columnName
     *        is the name of a column.
     * @return the column value; if the value is SQL NULL, the value returned is null.
     * @throws SQLServerException
     *         If any errors occur.
     */
    BigDecimal getSmallMoney(String columnName) throws SQLServerException;

    /**
     * Updates the value of the column specified to the DateTimeOffset Class value, given a zero-based column ordinal.
     * 
     * @param index
     *        The zero-based ordinal of a column.
     * @param x
     *        A DateTimeOffset Class object.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateDateTimeOffset(int index, microsoft.sql.DateTimeOffset x) throws SQLServerException;

    /**
     * Updates the value of the column specified to the DateTimeOffset Class value, given a column name.
     * 
     * @param columnName
     *        The name of a column.
     * @param x
     *        A DateTimeOffset Class object.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateDateTimeOffset(String columnName, microsoft.sql.DateTimeOffset x) throws SQLServerException;

    /**
     * Updates the designated column with an {@code Object} value.
     *
     * The updater methods are used to update column values in the current row or the insert row. The updater methods do
     * not update the underlying database; instead the {@code updateRow} or {@code insertRow} methods are called to
     * update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param precision
     *        the precision of the column
     * @param scale
     *        the scale of the column
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateObject(int index, Object x, int precision, int scale) throws SQLServerException;

    /**
     * Updates the designated column with an Object value. The updater methods are used to update column values in the
     * current row or the insert row. The updater methods do not update the underlying database; instead the updateRow
     * or insertRow methods are called to update the database. If the second argument is an InputStream then the stream
     * must contain the number of bytes specified by scaleOrLength. If the second argument is a Reader then the reader
     * must contain the number of characters specified by scaleOrLength. If these conditions are not true the driver
     * will generate a SQLServerException when the statement is executed. The default implementation will throw
     * SQLFeatureNotSupportedException
     * 
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param obj
     *        the new column value
     * @param targetSqlType
     *        the SQL type to be sent to the database
     * @param scale
     *        for an object of java.math.BigDecimal , this is the number of digits after the decimal point. For Java
     *        Object types InputStream and Reader, this is the length of the data in the stream or reader. For all other
     *        types, this value will be ignored.
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement.If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateObject(int index, Object obj, SQLType targetSqlType, int scale,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * 
     * Updates the designated column with an Object value. The updater methods are used to update column values in the
     * current row or the insert row. The updater methods do not update the underlying database; instead the updateRow
     * or insertRow methods are called to update the database. If the second argument is an InputStream then the stream
     * must contain the number of bytes specified by scaleOrLength. If the second argument is a Reader then the reader
     * must contain the number of characters specified by scaleOrLength. If these conditions are not true the driver
     * will generate a SQLServerException when the statement is executed. The default implementation will throw
     * SQLFeatureNotSupportedException.
     * 
     * @param columnName
     *        the label for the column specified with the SQL AS clause. If the SQL AS clause was not specified, then
     *        the label is the name of the column
     * @param obj
     *        the new column value
     * @param targetSqlType
     *        the SQL type to be sent to the database
     * @param scale
     *        for an object of java.math.BigDecimal , this is the number of digits after the decimal point. For Java
     *        Object types InputStream and Reader, this is the length of the data in the stream or reader. For all other
     *        types, this value will be ignored.
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement.If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateObject(String columnName, Object obj, SQLType targetSqlType, int scale,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>boolean</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateBoolean(int index, boolean x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>byte</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateByte(int index, byte x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>short</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateShort(int index, short x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with an <code>int</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateInt(int index, int x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>long</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateLong(int index, long x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>float</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateFloat(int index, float x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>double</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateDouble(int index, double x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>money</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateMoney(int index, BigDecimal x) throws SQLServerException;

    /**
     * Updates the designated column with a <code>money</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateMoney(int index, BigDecimal x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>money</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        the column name
     * @param x
     *        the new column value
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateMoney(String columnName, BigDecimal x) throws SQLServerException;

    /**
     * Updates the designated column with a <code>money</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        the column name
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateMoney(String columnName, BigDecimal x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>smallmoney</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateSmallMoney(int index, BigDecimal x) throws SQLServerException;

    /**
     * Updates the designated column with a <code>smallmoney</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateSmallMoney(int index, BigDecimal x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>smallmoney</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        the column name
     * @param x
     *        the new column value
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateSmallMoney(String columnName, BigDecimal x) throws SQLServerException;

    /**
     * Updates the designated column with a <code>smallmoney</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        the column name
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateSmallMoney(String columnName, BigDecimal x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.math.BigDecimal</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param precision
     *        the precision of the column
     * @param scale
     *        the scale of the column
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateBigDecimal(int index, BigDecimal x, Integer precision, Integer scale) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.math.BigDecimal</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param precision
     *        the precision of the column
     * @param scale
     *        the scale of the column
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateBigDecimal(int index, BigDecimal x, Integer precision, Integer scale,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>String</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex
     *        the first column is 1, the second is 2, ...
     * @param stringValue
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateString(int columnIndex, String stringValue, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>String</code> value. It is intended for use when updating
     * <code>NCHAR</code>,<code>NVARCHAR</code> and <code>LONGNVARCHAR</code> columns. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnIndex
     *        the first column is 1, the second 2, ...
     * @param nString
     *        the value for the column to be updated
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateNString(int columnIndex, String nString, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>String</code> value. It is intended for use when updating
     * <code>NCHAR</code>,<code>NVARCHAR</code> and <code>LONGNVARCHAR</code> columns. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnLabel
     *        the label for the column specified with the SQL AS clause. If the SQL AS clause was not specified, then
     *        the label is the name of the column
     * @param nString
     *        the value for the column to be updated
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateNString(String columnLabel, String nString, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>byte</code> array value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateBytes(int index, byte x[], boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Date</code> value. The updater methods are used to update
     * column values in the current row or the insert row. The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateDate(int index, java.sql.Date x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Time</code> value. The updater methods are used to update
     * column values in the current row or the insert row. The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param scale
     *        the scale of the column
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateTime(int index, java.sql.Time x, Integer scale) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Time</code> value. The updater methods are used to update
     * column values in the current row or the insert row. The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param scale
     *        the scale of the column
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateTime(int index, java.sql.Time x, Integer scale, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param scale
     *        the scale of the column
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateTimestamp(int index, java.sql.Timestamp x, int scale) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param scale
     *        the scale of the column
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateTimestamp(int index, java.sql.Timestamp x, int scale, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateDateTime(int index, java.sql.Timestamp x) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param scale
     *        the scale of the column
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateDateTime(int index, java.sql.Timestamp x, Integer scale) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param scale
     *        the scale of the column
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateDateTime(int index, java.sql.Timestamp x, Integer scale, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateSmallDateTime(int index, java.sql.Timestamp x) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param scale
     *        the scale of the column
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateSmallDateTime(int index, java.sql.Timestamp x, Integer scale) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param scale
     *        the scale of the column
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateSmallDateTime(int index, java.sql.Timestamp x, Integer scale,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the value of the column specified to the DateTimeOffset Class value, given a zero-based column ordinal.
     * 
     * @param index
     *        The zero-based ordinal of a column.
     * @param x
     *        A DateTimeOffset Class object.
     * @param scale
     *        scale of the column
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateDateTimeOffset(int index, microsoft.sql.DateTimeOffset x, Integer scale) throws SQLServerException;

    /**
     * Updates the value of the column specified to the DateTimeOffset Class value, given a zero-based column ordinal.
     * 
     * @param index
     *        The zero-based ordinal of a column.
     * @param x
     *        A DateTimeOffset Class object.
     * @param scale
     *        scale of the column
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateDateTimeOffset(int index, microsoft.sql.DateTimeOffset x, Integer scale,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>String</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     * 
     * @param index
     *        The zero-based ordinal of a column.
     * @param x
     *        the new column value
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateUniqueIdentifier(int index, String x) throws SQLServerException;

    /**
     * Updates the designated column with a <code>String</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     * 
     * @param index
     *        The zero-based ordinal of a column.
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateUniqueIdentifier(int index, String x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with an {@code Object} value.
     *
     * The updater methods are used to update column values in the current row or the insert row. The updater methods do
     * not update the underlying database; instead the {@code updateRow} or {@code insertRow} methods are called to
     * update the database.
     *
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param x
     *        the new column value
     * @param precision
     *        the precision of the column
     * @param scale
     *        the scale of the column
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateObject(int index, Object x, int precision, int scale, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>boolean</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        the name of the column
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         when an error occurs
     */
    void updateBoolean(String columnName, boolean x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>byte</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     *
     * @param columnName
     *        the name of the column
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateByte(String columnName, byte x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>short</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        the name of the column
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateShort(String columnName, short x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with an <code>int</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateInt(String columnName, int x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>long</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateLong(String columnName, long x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>float </code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateFloat(String columnName, float x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>double</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateDouble(String columnName, double x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.BigDecimal</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateBigDecimal(String columnName, BigDecimal x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.BigDecimal</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column and Always Encrypted is enabled on the connection or on the statement. If the
     *        boolean forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @param x
     *        BigDecimal value
     * @param precision
     *        the precision of the column
     * @param scale
     *        the scale of the column
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateBigDecimal(String columnName, BigDecimal x, Integer precision, Integer scale) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.BigDecimal</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column and Always Encrypted is enabled on the connection or on the statement. If the
     *        boolean forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @param x
     *        BigDecimal value
     * @param precision
     *        the precision of the column
     * @param scale
     *        the scale of the column
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateBigDecimal(String columnName, BigDecimal x, Integer precision, Integer scale,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>String</code> value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateString(String columnName, String x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a byte array value.
     *
     * The updater methods are used to update column values in the current row or the insert row. The updater methods do
     * not update the underlying database; instead the <code>updateRow</code> or <code>insertRow</code> methods are
     * called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateBytes(String columnName, byte x[], boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Date</code> value. The updater methods are used to update
     * column values in the current row or the insert row. The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateDate(String columnName, java.sql.Date x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Time</code> value. The updater methods are used to update
     * column values in the current row or the insert row. The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @param scale
     *        the scale of the column
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateTime(String columnName, java.sql.Time x, int scale) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Time</code> value. The updater methods are used to update
     * column values in the current row or the insert row. The updater methods do not update the underlying database;
     * instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @param scale
     *        the scale of the column
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateTime(String columnName, java.sql.Time x, int scale, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @param scale
     *        the scale of the column
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateTimestamp(String columnName, java.sql.Timestamp x, int scale) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @param scale
     *        the scale of the column
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateTimestamp(String columnName, java.sql.Timestamp x, int scale,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateDateTime(String columnName, java.sql.Timestamp x) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @param scale
     *        the scale of the column
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateDateTime(String columnName, java.sql.Timestamp x, int scale) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @param scale
     *        the scale of the column
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateDateTime(String columnName, java.sql.Timestamp x, int scale,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateSmallDateTime(String columnName, java.sql.Timestamp x) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @param scale
     *        the scale of the column
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateSmallDateTime(String columnName, java.sql.Timestamp x, int scale) throws SQLServerException;

    /**
     * Updates the designated column with a <code>java.sql.Timestamp</code> value. The updater methods are used to
     * update column values in the current row or the insert row. The updater methods do not update the underlying
     * database; instead the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     *
     * @param columnName
     *        is the name of the column
     * @param x
     *        the new column value
     * @param scale
     *        the scale of the column
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateSmallDateTime(String columnName, java.sql.Timestamp x, int scale,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the value of the column specified to the DateTimeOffset Class value, given a column name.
     * 
     * @param columnName
     *        The name of a column.
     * @param x
     *        A DateTimeOffset Class object.
     * @param scale
     *        the scale of the column
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateDateTimeOffset(String columnName, microsoft.sql.DateTimeOffset x, int scale) throws SQLServerException;

    /**
     * Updates the value of the column specified to the DateTimeOffset Class value, given a column name.
     * 
     * @param columnName
     *        The name of a column.
     * @param x
     *        A DateTimeOffset Class object.
     * @param scale
     *        the scale of the column
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateDateTimeOffset(String columnName, microsoft.sql.DateTimeOffset x, int scale,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with a <code>String</code>value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     * 
     * @param columnName
     *        The name of a column.
     * @param x
     *        the new column value
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateUniqueIdentifier(String columnName, String x) throws SQLServerException;

    /**
     * Updates the designated column with a <code>String</code>value. The updater methods are used to update column
     * values in the current row or the insert row. The updater methods do not update the underlying database; instead
     * the <code>updateRow</code> or <code>insertRow</code> methods are called to update the database.
     * 
     * @param columnName
     *        The name of a column.
     * @param x
     *        the new column value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateUniqueIdentifier(String columnName, String x, boolean forceEncrypt) throws SQLServerException;

    /**
     * Updates the designated column with an {@code Object} value.
     *
     * The updater methods are used to update column values in the current row or the insert row. The updater methods do
     * not update the underlying database; instead the {@code updateRow} or {@code insertRow} methods are called to
     * update the database.
     *
     * @param columnName
     *        The name of a column.
     * @param x
     *        the new column value
     * @param precision
     *        the precision of the column
     * @param scale
     *        the scale of the column
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateObject(String columnName, Object x, int precision, int scale) throws SQLServerException;

    /**
     * Updates the designated column with an {@code Object} value.
     *
     * The updater methods are used to update column values in the current row or the insert row. The updater methods do
     * not update the underlying database; instead the {@code updateRow} or {@code insertRow} methods are called to
     * update the database.
     *
     * @param columnName
     *        The name of a column.
     * @param x
     *        the new column value
     * @param precision
     *        the precision of the column
     * @param scale
     *        the scale of the column
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void updateObject(String columnName, Object x, int precision, int scale,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Returns the Data Classification information for the current ResultSet For SQL Servers that do not support Data
     * Classification or results that do not fetch any classified columns, this data can be null.
     * 
     * @return SensitivityClassification
     */
    SensitivityClassification getSensitivityClassification();
}
