/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.math.BigDecimal;
import java.sql.SQLType;
import java.sql.Timestamp;
import java.util.Calendar;


/**
 * Provides an interface to the {@link SQLServerCallableStatement} class.
 */
public interface ISQLServerCallableStatement extends java.sql.CallableStatement, ISQLServerPreparedStatement {

    @Deprecated
    BigDecimal getBigDecimal(String parameterName, int scale) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp
     * object in the Java programming language.
     * 
     * @param index
     *        the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    Timestamp getDateTime(int index) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp
     * object in the Java programming language.
     * 
     * @param parameterName
     *        the label for the column specified with the SQL AS clause. If the SQL AS clause was not specified, then
     *        the label is the name of the column
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    Timestamp getDateTime(String parameterName) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp
     * object in the Java programming language. This method uses the given calendar to construct an appropriate
     * millisecond value for the timestamp if the underlying database does not store timezone information.
     * 
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param cal
     *        the java.util.Calendar object to use in constructing the dateTime
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    Timestamp getDateTime(int index, Calendar cal) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp
     * object in the Java programming language. This method uses the given calendar to construct an appropriate
     * millisecond value for the timestamp if the underlying database does not store timezone information.
     * 
     * @param name
     *        the name of the column
     * @param cal
     *        the java.util.Calendar object to use in constructing the dateTime
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    Timestamp getDateTime(String name, Calendar cal) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp
     * object in the Java programming language.
     * 
     * @param index
     *        the first column is 1, the second is 2, ...
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    Timestamp getSmallDateTime(int index) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp
     * object in the Java programming language.
     * 
     * @param parameterName
     *        The name of a column.
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    Timestamp getSmallDateTime(String parameterName) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp
     * object in the Java programming language.
     * 
     * @param index
     *        the first column is 1, the second is 2, ...
     * @param cal
     *        the java.util.Calendar object to use in constructing the smalldateTime
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    Timestamp getSmallDateTime(int index, Calendar cal) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this ResultSet object as a java.sql.Timestamp
     * object in the Java programming language.
     * 
     * @param name
     *        The name of a column
     * @param cal
     *        the java.util.Calendar object to use in constructing the smalldateTime
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    Timestamp getSmallDateTime(String name, Calendar cal) throws SQLServerException;

    /**
     * Returns the DateTimeOffset value of parameter with index parameterIndex.
     * 
     * @param parameterIndex
     *        the first parameter is 1, the second is 2, and so on
     * @return DateTimeOffset value if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         if parameterIndex is out of range; if a database access error occurs or this method is called on a closed
     *         <code>CallableStatement</code>
     */
    microsoft.sql.DateTimeOffset getDateTimeOffset(int parameterIndex) throws SQLServerException;

    /**
     * Returns the DateTimeOffset value of parameter with name parameterName.
     * 
     * @param parameterName
     *        the name of the parameter
     * @return DateTimeOffset value if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    microsoft.sql.DateTimeOffset getDateTimeOffset(String parameterName) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this <code>ResultSet</code> object as a stream
     * of ASCII characters. The value can then be read in chunks from the stream. This method is particularly suitable
     * for retrieving large <code>LONGVARCHAR</code> values. The JDBC driver will do any necessary conversion from the
     * database format into ASCII.
     *
     * <P>
     * <B>Note:</B> All the data in the returned stream must be read prior to getting the value of any other column. The
     * next call to a getter method implicitly closes the stream. Also, a stream may return <code>0</code> when the
     * method <code>InputStream.available</code> is called whether there is data available or not.
     *
     * @param parameterIndex
     *        the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value as a stream of one-byte ASCII characters; if
     *         the value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws SQLServerException
     *         if the columnIndex is not valid; if a database access error occurs or this method is called on a closed
     *         result set
     */
    java.io.InputStream getAsciiStream(int parameterIndex) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this <code>ResultSet</code> object as a stream
     * of ASCII characters. The value can then be read in chunks from the stream. This method is particularly suitable
     * for retrieving large <code>LONGVARCHAR</code> values. The JDBC driver will do any necessary conversion from the
     * database format into ASCII.
     *
     * <P>
     * <B>Note:</B> All the data in the returned stream must be read prior to getting the value of any other column. The
     * next call to a getter method implicitly closes the stream. Also, a stream may return <code>0</code> when the
     * method <code>available</code> is called whether there is data available or not.
     *
     * @param parameterName
     *        the name of the parameter
     * @return a Java input stream that delivers the database column value as a stream of one-byte ASCII characters. If
     *         the value is SQL <code>NULL</code>, the value returned is <code>null</code>.
     * @throws SQLServerException
     *         if the columnLabel is not valid; if a database access error occurs or this method is called on a closed
     *         result set
     */
    java.io.InputStream getAsciiStream(String parameterName) throws SQLServerException;

    /**
     * Returns the value of the column specified as a java.math.BigDecimal object.
     * 
     * @param parameterIndex
     *        The zero-based ordinal of a column.
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    BigDecimal getMoney(int parameterIndex) throws SQLServerException;

    /**
     * Returns the value of the column specified as a java.math.BigDecimal object.
     * 
     * @param parameterName
     *        The name of a column.
     * @return the column value; if the value is SQL NULL, the value returned is null.
     * @throws SQLServerException
     *         when an error occurs
     */
    BigDecimal getMoney(String parameterName) throws SQLServerException;

    /**
     * Returns the value of the column specified as a java.math.BigDecimal object.
     * 
     * @param parameterIndex
     *        The zero-based ordinal of a column.
     * @return the column value; if the value is SQL NULL, the value returned is null
     * @throws SQLServerException
     *         when an error occurs
     */
    BigDecimal getSmallMoney(int parameterIndex) throws SQLServerException;

    /**
     * Returns the value of the column specified as a java.math.BigDecimal object.
     * 
     * @param parameterName
     *        The name of a column.
     * @return the column value; if the value is SQL NULL, the value returned is null.
     * @throws SQLServerException
     *         when an error occurs
     */
    BigDecimal getSmallMoney(String parameterName) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this <code>ResultSet</code> object as a stream
     * of uninterpreted bytes. The value can then be read in chunks from the stream. This method is particularly
     * suitable for retrieving large <code>LONGVARBINARY</code> values.
     *
     * <P>
     * <B>Note:</B> All the data in the returned stream must be read prior to getting the value of any other column. The
     * next call to a getter method implicitly closes the stream. Also, a stream may return <code>0</code> when the
     * method <code>InputStream.available</code> is called whether there is data available or not.
     *
     * @param parameterIndex
     *        the first column is 1, the second is 2, ...
     * @return a Java input stream that delivers the database column value as a stream of uninterpreted bytes; if the
     *         value is SQL <code>NULL</code>, the value returned is <code>null</code>
     * @throws SQLServerException
     *         if the columnIndex is not valid; if a database access error occurs or this method is called on a closed
     *         result set
     */
    java.io.InputStream getBinaryStream(int parameterIndex) throws SQLServerException;

    /**
     * Returns the value of the designated column in the current row of this <code>ResultSet</code> object as a stream
     * of uninterpreted <code>byte</code>s. The value can then be read in chunks from the stream. This method is
     * particularly suitable for retrieving large <code>LONGVARBINARY</code> values.
     *
     * <P>
     * <B>Note:</B> All the data in the returned stream must be read prior to getting the value of any other column. The
     * next call to a getter method implicitly closes the stream. Also, a stream may return <code>0</code> when the
     * method <code>available</code> is called whether there is data available or not.
     *
     * @param parameterName
     *        the name of the parameter
     * @return a Java input stream that delivers the database column value as a stream of uninterpreted bytes; if the
     *         value is SQL <code>NULL</code>, the result is <code>null</code>
     * @throws SQLServerException
     *         if the columnLabel is not valid; if a database access error occurs or this method is called on a closed
     *         result set
     */
    java.io.InputStream getBinaryStream(String parameterName) throws SQLServerException;

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value. The driver converts this to an
     * SQL <code>TIMESTAMP</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param calendar
     *        a java.util.Calendar
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     * @see #getTimestamp
     */
    void setTimestamp(String parameterName, java.sql.Timestamp value, Calendar calendar,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given <code>java.sql.Time</code> value, using the given
     * <code>Calendar</code> object. The driver uses the <code>Calendar</code> object to construct an SQL
     * <code>TIME</code> value, which the driver then sends to the database. With a a <code>Calendar</code> object, the
     * driver can calculate the time taking into account a custom timezone. If no <code>Calendar</code> object is
     * specified, the driver uses the default timezone, which is that of the virtual machine running the application.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param calendar
     *        the <code>Calendar</code> object the driver will use to construct the time
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     * @see #getTime
     */
    void setTime(String parameterName, java.sql.Time value, Calendar calendar,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given <code>java.sql.Date</code> value, using the given
     * <code>Calendar</code> object. The driver uses the <code>Calendar</code> object to construct an SQL
     * <code>DATE</code> value, which the driver then sends to the database. With a a <code>Calendar</code> object, the
     * driver can calculate the date taking into account a custom timezone. If no <code>Calendar</code> object is
     * specified, the driver uses the default timezone, which is that of the virtual machine running the application.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param calendar
     *        the <code>Calendar</code> object the driver will use to construct the date
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     * @see #getDate
     */
    void setDate(String parameterName, java.sql.Date value, Calendar calendar,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given <code>String</code> object. The driver converts this to a SQL
     * <code>NCHAR</code> or <code>NVARCHAR</code> or <code>LONGNVARCHAR</code>
     * 
     * @param parameterName
     *        the name of the parameter to be set
     * @param value
     *        the parameter value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if the driver does not support national
     *         character sets; if the driver can detect that a data conversion error could occur; if a database access
     *         error occurs or this method is called on a closed <code>CallableStatement</code>
     */
    void setNString(String parameterName, String value, boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the value of the designated parameter with the given object.
     *
     * <p>
     * The given Java object will be converted to the given targetSqlType before being sent to the database.
     *
     * If the object has a custom mapping (is of a class implementing the interface <code>SQLData</code>), the JDBC
     * driver should call the method <code>SQLData.writeSQL</code> to write it to the SQL data stream. If, on the other
     * hand, the object is of a class implementing <code>Ref</code>, <code>Blob</code>, <code>Clob</code>,
     * <code>NClob</code>, <code>Struct</code>, <code>java.net.URL</code>, or <code>Array</code>, the driver should pass
     * it to the database as a value of the corresponding SQL type.
     * <P>
     * Note that this method may be used to pass database- specific abstract data types.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the object containing the input parameter value
     * @param sqlType
     *        the SQL type (as defined in java.sql.Types) to be sent to the database. The scale argument may further
     *        qualify this type.
     * @param decimals
     *        for java.sql.Types.DECIMAL or java.sql.Types.NUMERIC types, this is the number of digits after the decimal
     *        point. For all other types, this value will be ignored.
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     * @see java.sql.Types
     * @see #getObject
     */
    void setObject(String parameterName, Object value, int sqlType, int decimals,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the value of the designated parameter with the given object.
     *
     * <p>
     * The given Java object will be converted to the given targetSqlType before being sent to the database.
     *
     * If the object has a custom mapping (is of a class implementing the interface <code>SQLData</code>), the JDBC
     * driver should call the method <code>SQLData.writeSQL</code> to write it to the SQL data stream. If, on the other
     * hand, the object is of a class implementing <code>Ref</code>, <code>Blob</code>, <code>Clob</code>,
     * <code>NClob</code>, <code>Struct</code>, <code>java.net.URL</code>, or <code>Array</code>, the driver should pass
     * it to the database as a value of the corresponding SQL type.
     * <P>
     * Note that this method may be used to pass datatabase- specific abstract data types.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the object containing the input parameter value
     * @param targetSqlType
     *        the SQL type (as defined in java.sql.Types) to be sent to the database. The scale argument may further
     *        qualify this type.
     * @param precision
     *        the precision of the column.
     * @param scale
     *        the scale of the column.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     * @see java.sql.Types
     * @see #getObject
     */
    void setObject(String parameterName, Object value, int targetSqlType, Integer precision,
            int scale) throws SQLServerException;

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value. The driver converts this to an
     * SQL <code>TIMESTAMP</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param scale
     *        the scale of the parameter
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     * @see #getTimestamp
     */
    void setTimestamp(String parameterName, java.sql.Timestamp value, int scale) throws SQLServerException;

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value. The driver converts this to an
     * SQL <code>TIMESTAMP</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param scale
     *        the scale of the parameter
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     * @see #getTimestamp
     */
    void setTimestamp(String parameterName, java.sql.Timestamp value, int scale,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets parameter parameterName to DateTimeOffset value.
     * 
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        DateTimeOffset value
     * @throws SQLServerException
     *         if an error occurs
     */
    void setDateTimeOffset(String parameterName, microsoft.sql.DateTimeOffset value) throws SQLServerException;

    /**
     * Sets parameter parameterName to DateTimeOffset value.
     * 
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        DateTimeOffset value
     * @param scale
     *        the scale of the parameter
     * @throws SQLServerException
     *         if an error occurs
     */
    void setDateTimeOffset(String parameterName, microsoft.sql.DateTimeOffset value,
            int scale) throws SQLServerException;

    /**
     * Sets parameter parameterName to DateTimeOffset value.
     * 
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        DateTimeOffset value
     * @param scale
     *        the scale of the parameter
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if an error occurs
     */
    void setDateTimeOffset(String parameterName, microsoft.sql.DateTimeOffset value, int scale,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given <code>java.sql.Time</code> value. The driver converts this to an SQL
     * <code>TIME</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param scale
     *        the scale of the column
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     * @see #getTime
     */
    void setTime(String parameterName, java.sql.Time value, int scale) throws SQLServerException;

    /**
     * Sets the designated parameter to the given <code>java.sql.Time</code> value. The driver converts this to an SQL
     * <code>TIME</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param scale
     *        the scale of the column
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     * @see #getTime
     */
    void setTime(String parameterName, java.sql.Time value, int scale, boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value. The driver converts this to an
     * SQL <code>DATETIME</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setDateTime(String parameterName, java.sql.Timestamp value) throws SQLServerException;

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value. The driver converts this to an
     * SQL <code>DATETIME</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setDateTime(String parameterName, java.sql.Timestamp value, boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value. The driver converts this to an
     * SQL <code>SMALLDATETIME</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setSmallDateTime(String parameterName, java.sql.Timestamp value) throws SQLServerException;

    /**
     * Sets the designated parameter to the given <code>java.sql.Timestamp</code> value. The driver converts this to an
     * SQL <code>SMALLDATETIME</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setSmallDateTime(String parameterName, java.sql.Timestamp value,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given <code>String</code> value. The driver converts this to an SQL
     * <code>uniqueIdentifier</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param guid
     *        the parameter value
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setUniqueIdentifier(String parameterName, String guid) throws SQLServerException;

    /**
     * Sets the designated parameter to the given <code>String</code> value. The driver converts this to an SQL
     * <code>uniqueIdentifier</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param guid
     *        the parameter value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setUniqueIdentifier(String parameterName, String guid, boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given Java array of bytes. The driver converts this to an SQL
     * <code>VARBINARY</code> or <code>LONGVARBINARY</code> (depending on the argument's size relative to the driver's
     * limits on <code>VARBINARY</code> values) when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setBytes(String parameterName, byte[] value, boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given Java <code>byte</code> value. The driver converts this to an SQL
     * <code>TINYINT</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setByte(String parameterName, byte value, boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given Java <code>String</code> value. The driver converts this to an SQL
     * <code>VARCHAR</code> or <code>LONGVARCHAR</code> value (depending on the argument's size relative to the driver's
     * limits on <code>VARCHAR</code> values) when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setString(String parameterName, String value, boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given Java <code>java.math.BigDecimal</code> value. The driver converts this
     * to an SQL <code>Money</code> value.
     * 
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setMoney(String parameterName, BigDecimal value) throws SQLServerException;

    /**
     * Sets the designated parameter to the given Java <code>java.math.BigDecimal</code> value. The driver converts this
     * to an SQL <code>Money</code> value.
     * 
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setMoney(String parameterName, BigDecimal value, boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given Java <code>java.math.BigDecimal</code> value. The driver converts this
     * to an SQL <code>smallMoney</code> value.
     * 
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setSmallMoney(String parameterName, BigDecimal value) throws SQLServerException;

    /**
     * Sets the designated parameter to the given Java <code>java.math.BigDecimal</code> value. The driver converts this
     * to an SQL <code>smallMoney</code> value.
     * 
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setSmallMoney(String parameterName, BigDecimal value, boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given <code>java.math.BigDecimal</code> value. The driver converts this to
     * an SQL <code>NUMERIC</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param precision
     *        the precision of the column
     * @param scale
     *        the scale of the column
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setBigDecimal(String parameterName, BigDecimal value, int precision, int scale) throws SQLServerException;

    /**
     * Sets the designated parameter to the given <code>java.math.BigDecimal</code> value. The driver converts this to
     * an SQL <code>NUMERIC</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param precision
     *        the precision of the column
     * @param scale
     *        the scale of the column
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setBigDecimal(String parameterName, BigDecimal value, int precision, int scale,
            boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given Java <code>double</code> value. The driver converts this to an SQL
     * <code>DOUBLE</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setDouble(String parameterName, double value, boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given Java <code>float</code> value. The driver converts this to an SQL
     * <code>FLOAT</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setFloat(String parameterName, float value, boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given Java <code>int</code> value. The driver converts this to an SQL
     * <code>INTEGER</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setInt(String parameterName, int value, boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given Java <code>long</code> value. The driver converts this to an SQL
     * <code>BIGINT</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setLong(String parameterName, long value, boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given Java <code>short</code> value. The driver converts this to an SQL
     * <code>SMALLINT</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setShort(String parameterName, short value, boolean forceEncrypt) throws SQLServerException;

    /**
     * Sets the designated parameter to the given Java <code>boolean</code> value. The driver converts this to an SQL
     * <code>BIT</code> or <code>BOOLEAN</code> value when it sends it to the database.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the parameter value
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     */
    void setBoolean(String parameterName, boolean value, boolean forceEncrypt) throws SQLServerException;

    /**
     * Populates a table valued parameter passed to a stored procedure with a data table.
     * 
     * @param parameterName
     *        the name of the parameter
     * @param tvpName
     *        the name of the type TVP
     * @param tvpDataTable
     *        the data table object
     * @throws SQLServerException
     *         when an error occurs
     */
    void setStructured(String parameterName, String tvpName, SQLServerDataTable tvpDataTable) throws SQLServerException;

    /**
     * Populates a table valued parameter passed to a stored procedure with a ResultSet retrieved from another table
     * 
     * @param parameterName
     *        the name of the parameter
     * @param tvpName
     *        the name of the type TVP
     * @param tvpResultSet
     *        the source result set object
     * @throws SQLServerException
     *         when an error occurs
     */
    void setStructured(String parameterName, String tvpName, java.sql.ResultSet tvpResultSet) throws SQLServerException;

    /**
     * Populates a table valued parameter passed to a stored procedure with an ISQLServerDataRecord object.
     * 
     * @param parameterName
     *        the name of the parameter
     * @param tvpName
     *        the name of the type TVP
     * @param tvpDataRecord
     *        ISQLServerDataRecord is used for streaming data and the user decides how to use it. tvpDataRecord is an
     *        ISQLServerDataRecord object.the source result set object
     * @throws SQLServerException
     *         when an error occurs
     */
    void setStructured(String parameterName, String tvpName,
            ISQLServerDataRecord tvpDataRecord) throws SQLServerException;

    /**
     * Registers the parameter in ordinal position index to be of JDBC type sqlType. All OUT parameters must be
     * registered before a stored procedure is executed.
     * <p>
     * The JDBC type specified by sqlType for an OUT parameter determines the Java type that must be used in the get
     * method to read the value of that parameter.
     * 
     * @param parameterName
     *        the name of the parameter
     * @param sqlType
     *        the JDBC type code defined by SQLType to use to register the OUT Parameter.
     * @param precision
     *        the sum of the desired number of digits to the left and right of the decimal point. It must be greater
     *        than or equal to zero.
     * @param scale
     *        the desired number of digits to the right of the decimal point. It must be greater than or equal to zero.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void registerOutParameter(String parameterName, SQLType sqlType, int precision,
            int scale) throws SQLServerException;

    /**
     * Registers the parameter in ordinal position index to be of JDBC type sqlType. All OUT parameters must be
     * registered before a stored procedure is executed.
     * <p>
     * The JDBC type specified by sqlType for an OUT parameter determines the Java type that must be used in the get
     * method to read the value of that parameter.
     * 
     * @param parameterIndex
     *        the first column is 1, the second is 2, ...
     * @param sqlType
     *        the JDBC type code defined by SQLType to use to register the OUT Parameter.
     * @param precision
     *        the sum of the desired number of digits to the left and right of the decimal point. It must be greater
     *        than or equal to zero.
     * @param scale
     *        the desired number of digits to the right of the decimal point. It must be greater than or equal to zero.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void registerOutParameter(int parameterIndex, SQLType sqlType, int precision, int scale) throws SQLServerException;

    /**
     * Registers the parameter in ordinal position index to be of JDBC type sqlType. All OUT parameters must be
     * registered before a stored procedure is executed.
     * <p>
     * The JDBC type specified by sqlType for an OUT parameter determines the Java type that must be used in the get
     * method to read the value of that parameter.
     * 
     * @param parameterIndex
     *        the first column is 1, the second is 2, ...
     * @param sqlType
     *        the JDBC type code defined by SQLType to use to register the OUT Parameter.
     * @param precision
     *        the sum of the desired number of digits to the left and right of the decimal point. It must be greater
     *        than or equal to zero.
     * @param scale
     *        the desired number of digits to the right of the decimal point. It must be greater than or equal to zero.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void registerOutParameter(int parameterIndex, int sqlType, int precision, int scale) throws SQLServerException;

    /**
     * Registers the parameter in ordinal position index to be of JDBC type sqlType. All OUT parameters must be
     * registered before a stored procedure is executed.
     * <p>
     * The JDBC type specified by sqlType for an OUT parameter determines the Java type that must be used in the get
     * method to read the value of that parameter.
     * 
     * @param parameterName
     *        the name of the parameter
     * @param sqlType
     *        the JDBC type code defined by SQLType to use to register the OUT Parameter.
     * @param precision
     *        the sum of the desired number of digits to the left and right of the decimal point. It must be greater
     *        than or equal to zero.
     * @param scale
     *        the desired number of digits to the right of the decimal point. It must be greater than or equal to zero.
     * @throws SQLServerException
     *         If any errors occur.
     */
    void registerOutParameter(String parameterName, int sqlType, int precision, int scale) throws SQLServerException;

    /**
     * Sets the value of the designated parameter with the given object.
     *
     * <p>
     * The given Java object will be converted to the given targetSqlType before being sent to the database.
     *
     * If the object has a custom mapping (is of a class implementing the interface <code>SQLData</code>), the JDBC
     * driver should call the method <code>SQLData.writeSQL</code> to write it to the SQL data stream. If, on the other
     * hand, the object is of a class implementing <code>Ref</code>, <code>Blob</code>, <code>Clob</code>,
     * <code>NClob</code>, <code>Struct</code>, <code>java.net.URL</code>, or <code>Array</code>, the driver should pass
     * it to the database as a value of the corresponding SQL type.
     * <P>
     * Note that this method may be used to pass datatabase- specific abstract data types.
     *
     * @param parameterName
     *        the name of the parameter
     * @param value
     *        the object containing the input parameter value
     * @param jdbcType
     *        the SQL type (as defined in java.sql.Types) to be sent to the database. The scale argument may further
     *        qualify this type.
     * @param scale
     *        the scale of the column.
     * @param forceEncrypt
     *        If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column
     *        is encrypted and Always Encrypted is enabled on the connection or on the statement. If the boolean
     *        forceEncrypt is set to false, the driver will not force encryption on parameters.
     * @throws SQLServerException
     *         if parameterName does not correspond to a named parameter; if a database access error occurs or this
     *         method is called on a closed <code>CallableStatement</code>
     * @see java.sql.Types
     * @see #getObject
     */
    void setObject(String parameterName, Object value, SQLType jdbcType, int scale,
            boolean forceEncrypt) throws SQLServerException;
}
