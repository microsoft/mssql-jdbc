/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.SQLType;

/**
 * This interface requires all the ResultSet methods including those are specific to JDBC 4.2
 *
 */
public interface ISQLServerResultSet42 extends ISQLServerResultSet {

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
