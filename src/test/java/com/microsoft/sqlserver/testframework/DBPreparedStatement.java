/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 
 * Wrapper class PreparedStatement
 */
public class DBPreparedStatement extends DBStatement {

    PreparedStatement pstmt = null;
    DBResultSet dbresultSet = null;

    /**
     * 
     */
    public DBPreparedStatement(DBConnection dbconnection) {
        super(dbconnection);
    }

    /**
     * set up internal PreparedStatement with query
     * 
     * @param query
     * @return
     * @throws SQLException
     * 
     */
    DBPreparedStatement prepareStatement(String query) throws SQLException {
        pstmt = ((Connection) parent().product()).prepareStatement(query);
        setInternal(pstmt);
        return this;
    }

    /**
     * 
     * @param query
     * @param resultSetType
     * @param resultSetConcurrency
     * @return
     * @throws SQLException
     */
    DBPreparedStatement prepareStatement(String query,
            int resultSetType,
            int resultSetConcurrency) throws SQLException {
        pstmt = ((Connection) parent().product()).prepareStatement(query, resultSetType, resultSetConcurrency);
        setInternal(pstmt);
        return this;
    }

    @Override
    void setInternal(Object internal) {
        this.internal = internal;
    }

    /**
     * 
     * @param parameterIndex
     * @param targetObject
     * @throws SQLException
     */
    public void setObject(int parameterIndex,
            Object targetObject) throws SQLException {

        ((PreparedStatement) product()).setObject(parameterIndex, targetObject);
    }

    /**
     * 
     * @return
     * @throws SQLException
     */
    public DBResultSet executeQuery() throws SQLException {
        ResultSet rs = null;
        rs = pstmt.executeQuery();
        dbresultSet = new DBResultSet(this, rs);
        return dbresultSet;
    }

    /**
     * populate table with values using prepared statement
     * 
     * @param table
     * @return <code>true</code> if table is populated
     */
    public boolean populateTable(DBTable table) {
        return table.populateTableWithPreparedStatement(this);
    }

    /**
     * 
     * @return
     * @throws SQLException
     */
    public boolean execute() throws SQLException {
        return pstmt.execute();
    }
}