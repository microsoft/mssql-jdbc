/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLType;

public interface ISQLServerPreparedStatement extends java.sql.PreparedStatement, ISQLServerStatement {
    /**
     * Sets the designated parameter to the given <code>microsoft.sql.DateTimeOffset</code> value.
     * 
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the parameter value
     * @throws SQLException
     *             if parameterIndex does not correspond to a parameter marker in the SQL statement; if a database access error occurs or this method
     *             is called on a closed <code>PreparedStatement</code>
     */
    public void setDateTimeOffset(int parameterIndex,
            microsoft.sql.DateTimeOffset x) throws SQLException;

    /**
     * Sets the value of the designated parameter with the given object.
     *
     * This method is similar to {@link #setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength)}, except that it assumes a
     * scale of zero.
     * <P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the object containing the input parameter value
     * @param targetSqlType
     *            the SQL type to be sent to the database
     * @param precision
     *            the precision of the column
     * @param scale
     *            the scale of the column
     * @throws SQLServerException
     *             if parameterIndex does not correspond to a parameter marker in the SQL statement; if a database access error occurs or this method
     *             is called on a closed {@code PreparedStatement}
     */
    public void setObject(int parameterIndex,
            Object x,
            SQLType targetSqlType,
            Integer precision,
            Integer scale) throws SQLServerException;

    /**
     * Sets the value of the designated parameter with the given object.
     *
     * This method is similar to {@link #setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength)}, except that it assumes a
     * scale of zero.
     * <P>
     * The default implementation will throw {@code SQLFeatureNotSupportedException}
     *
     * @param parameterIndex
     *            the first parameter is 1, the second is 2, ...
     * @param x
     *            the object containing the input parameter value
     * @param targetSqlType
     *            the SQL type to be sent to the database
     * @param precision
     *            the precision of the column
     * @param scale
     *            the scale of the column
     * @param forceEncrypt
     *            If the boolean forceEncrypt is set to true, the query parameter will only be set if the designation column is encrypted and Always
     *            Encrypted is enabled on the connection or on the statement. If the boolean forceEncrypt is set to false, the driver will not force
     *            encryption on parameters.
     * @throws SQLServerException
     *             if parameterIndex does not correspond to a parameter marker in the SQL statement; if a database access error occurs or this method
     *             is called on a closed {@code PreparedStatement}
     */
    public void setObject(int parameterIndex,
            Object x,
            SQLType targetSqlType,
            Integer precision,
            Integer scale,
            boolean forceEncrypt) throws SQLServerException;
    
    public int getPreparedStatementHandle() throws SQLServerException;
    
    public void setBigDecimal(int n,
            BigDecimal x,
            int precision,
            int scale) throws SQLException;
    
    public void setBigDecimal(int n,
            BigDecimal x,
            int precision,
            int scale,
            boolean forceEncrypt) throws SQLException;
    
    public void setMoney(int n,
            BigDecimal x) throws SQLException;
    
    public void setMoney(int n,
            BigDecimal x,
            boolean forceEncrypt) throws SQLException;
    
    public void setSmallMoney(int n,
            BigDecimal x) throws SQLException;
    
    public void setSmallMoney(int n,
            BigDecimal x,
            boolean forceEncrypt) throws SQLException;
    
    public void setBoolean(int n,
            boolean x,
            boolean forceEncrypt) throws SQLException;
    
    public void setByte(int n,
            byte x,
            boolean forceEncrypt) throws SQLException;
    
    public void setBytes(int n,
            byte x[],
            boolean forceEncrypt) throws SQLException;
    
    public void setUniqueIdentifier(int index,
            String guid) throws SQLException;
    
    public void setUniqueIdentifier(int index,
            String guid,
            boolean forceEncrypt) throws SQLException;
    
    public void setDouble(int n,
            double x,
            boolean forceEncrypt) throws SQLException;
    
    public void setFloat(int n,
            float x,
            boolean forceEncrypt) throws SQLException;
    
    public void setGeometry(int n,
            Geometry x) throws SQLException;
    
    public void setGeography(int n,
            Geography x) throws SQLException;
    
    public void setInt(int n,
            int value,
            boolean forceEncrypt) throws SQLException;
    
    public void setLong(int n,
            long x,
            boolean forceEncrypt) throws SQLException;
    
    public void setShort(int index,
            short x,
            boolean forceEncrypt) throws SQLException;
    
    public void setString(int index,
            String str,
            boolean forceEncrypt) throws SQLException;
    
    public void setNString(int parameterIndex,
            String value,
            boolean forceEncrypt) throws SQLException;
    
    public void setTime(int n,
            java.sql.Time x,
            int scale) throws SQLException;
    
    public void setTime(int n,
            java.sql.Time x,
            int scale,
            boolean forceEncrypt) throws SQLException;
    
    public void setTimestamp(int n,
            java.sql.Timestamp x,
            int scale) throws SQLException;
    
    public void setTimestamp(int n,
            java.sql.Timestamp x,
            int scale,
            boolean forceEncrypt) throws SQLException;
    
    public void setDateTimeOffset(int n,
            microsoft.sql.DateTimeOffset x,
            int scale) throws SQLException;
    
    public void setDateTimeOffset(int n,
            microsoft.sql.DateTimeOffset x,
            int scale,
            boolean forceEncrypt) throws SQLException;
    
    public void setDateTime(int n,
            java.sql.Timestamp x) throws SQLException;
    
    public void setDateTime(int n,
            java.sql.Timestamp x,
            boolean forceEncrypt) throws SQLException;
    
    public void setSmallDateTime(int n,
            java.sql.Timestamp x) throws SQLException;
    
    public void setSmallDateTime(int n,
            java.sql.Timestamp x,
            boolean forceEncrypt) throws SQLException;
    
    public void setStructured(int n,
            String tvpName,
            SQLServerDataTable tvpDataTable) throws SQLException;
    
    public void setStructured(int n,
            String tvpName,
            ResultSet tvpResultSet) throws SQLException;
    
    public void setStructured(int n,
            String tvpName,
            ISQLServerDataRecord tvpBulkRecord) throws SQLException;
    
    public void setDate(int n,
            java.sql.Date x,
            java.util.Calendar cal,
            boolean forceEncrypt) throws SQLException;
    
    public void setTime(int n,
            java.sql.Time x,
            java.util.Calendar cal,
            boolean forceEncrypt) throws SQLException;
    
    public void setTimestamp(int n,
            java.sql.Timestamp x,
            java.util.Calendar cal,
            boolean forceEncrypt) throws SQLException;
}
