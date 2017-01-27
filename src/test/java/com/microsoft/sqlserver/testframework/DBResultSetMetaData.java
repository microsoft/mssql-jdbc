/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import com.microsoft.sqlserver.jdbc.SQLServerResultSetMetaData;

/**
 * 
 * Wrapper class for ResultSetMetaData
 */
public class DBResultSetMetaData extends AbstractParentWrapper {

    DBResultSetMetaData dbresultSetMetaData = null;
    ResultSetMetaData resultSetMetaData = null;

    /**
     * @param parent
     * @param internal
     * @param name
     */
    DBResultSetMetaData(AbstractParentWrapper parent,
            Object internal,
            String name) {
        super(parent, internal, name);
        // TODO Auto-generated constructor stub
    }

    /**
     * 
     */
    public DBResultSetMetaData(DBResultSet dbresultset) {
        super(dbresultset, null, "dbresultset");
    }

    DBResultSetMetaData resultSetMetaData() {
        return this;
    }

    /**
     * 
     * @return
     * @throws SQLException
     */
    public DBResultSetMetaData getMetaData() throws SQLException {
        resultSetMetaData = ((ResultSet) parent().product()).getMetaData();
        setInternal(resultSetMetaData);
        return this;
    }

    /**
     * 
     * @throws SQLException
     */
    public void verify() throws SQLException {
        // getColumnCount
        int columns = this.getColumnCount();

        // Loop through the columns
        for (int i = 1; i <= columns; i++) {
            // Note: Just calling these performs the verification, in each method
            this.getColumnName(i);
            this.getColumnType(i);
            this.getColumnTypeName(i);
            this.getScale(i);
            this.isCaseSensitive(i);
            this.isAutoIncrement(i);
            this.isCurrency(i);
            this.isNullable(i);
            this.isSigned(i);
        }
    }

    /**
     * 
     * @return
     * @throws SQLException
     */
    public int getColumnCount() throws SQLException {
        return ((SQLServerResultSetMetaData) product()).getColumnCount();
    }

    /**
     * 
     * @param index
     * @return
     * @throws SQLException
     */
    public String getColumnName(int index) throws SQLException {
        return ((SQLServerResultSetMetaData) product()).getColumnName(index);
    }

    /**
     * 
     * @param index
     * @return
     * @throws SQLException
     */
    public int getColumnType(int index) throws SQLException {
        return ((SQLServerResultSetMetaData) product()).getColumnType(index);
    }

    /**
     * 
     * @param index
     * @return
     * @throws SQLException
     */
    public String getColumnTypeName(int index) throws SQLException {
        return ((SQLServerResultSetMetaData) product()).getColumnTypeName(index);
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public int getPrecision(int x) throws SQLException {
        return ((SQLServerResultSetMetaData) product()).getPrecision(x);
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public int getScale(int x) throws SQLException {
        return ((SQLServerResultSetMetaData) product()).getScale(x);
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public boolean isCaseSensitive(int x) throws SQLException {
        return ((SQLServerResultSetMetaData) product()).isCaseSensitive(x);

    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public boolean isCurrency(int x) throws SQLException {
        return ((SQLServerResultSetMetaData) product()).isCurrency(x);
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public boolean isAutoIncrement(int x) throws SQLException {
        return ((SQLServerResultSetMetaData) product()).isAutoIncrement(x);
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public int isNullable(int x) throws SQLException {
        return ((SQLServerResultSetMetaData) product()).isNullable(x);
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public boolean isSigned(int x) throws SQLException {
        return ((SQLServerResultSetMetaData) product()).isSigned(x);
    }

}
