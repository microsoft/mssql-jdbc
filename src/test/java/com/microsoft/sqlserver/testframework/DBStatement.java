/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;

/**
 * wrapper method for Statement object
 * 
 * @author Microsoft
 *
 */
public class DBStatement extends AbstractParentWrapper implements AutoCloseable{

    // TODO: support PreparedStatement and CallableStatement
    // TODO: add stmt level holdability
    // TODO: support IDENTITY column and stmt.getGeneratedKeys()
    public int cursortype = ResultSet.TYPE_FORWARD_ONLY;
    public int concurrency = ResultSet.CONCUR_READ_ONLY;
    public int holdability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
    public static final int STATEMENT = 0;
    public static final int PREPAREDSTATEMENT = 1;
    public static final int CALLABLESTATEMENT = 2;
    public static final int ALL = 3;

    Statement statement = null;
    DBResultSet dbresultSet = null;

    DBStatement(DBConnection dbConnection) {
        super(dbConnection, null, "statement");
    }

    DBStatement statement() {
        return this;
    }

    DBStatement createStatement() throws SQLException {
        // TODO: add cursor and holdability
        statement = ((SQLServerConnection) parent().product()).createStatement();
        setInternal(statement);
        return this;
    }

    DBStatement createStatement(int type,
            int concurrency) throws SQLException {
        // TODO: add cursor and holdability
        statement = ((SQLServerConnection) parent().product()).createStatement(type, concurrency);
        setInternal(statement);
        return this;
    }

    /**
     * 
     * @param sql
     *            query to execute
     * @return DBResultSet
     * @throws SQLException
     */
    public DBResultSet executeQuery(String sql) throws SQLException {
        ResultSet rs = null;
        rs = statement.executeQuery(sql);
        dbresultSet = new DBResultSet(this, rs);
        return dbresultSet;
    }

    /**
     * execute 'Select * from ' the table
     * 
     * @param table
     * @return DBResultSet
     * @throws SQLException
     */
    public DBResultSet selectAll(DBTable table) throws SQLException {
        String sql = "SELECT * FROM " + table.getEscapedTableName();
        ResultSet rs = statement.executeQuery(sql);
        dbresultSet = new DBResultSet(this, rs, table);
        return dbresultSet;
    }

    /**
     * 
     * @param sql
     *            query to execute
     * @return <code>true</code> if ResultSet is returned
     * @throws SQLException
     */
    public boolean execute(String sql) throws SQLException {
        return statement.execute(sql);
    }

    /**
     * executes the given sql query
     * 
     * @param sql
     * @return
     * @throws SQLException
     */
    public int executeUpdate(String sql) throws SQLException {
        int updatecount = statement.executeUpdate(sql);
        return updatecount;
    }

    /**
     * Close the <code>Statement</code> and <code>ResultSet</code> associated with it
     * 
     * @throws SQLException
     */
    public void close() throws SQLException {
        if ((null != dbresultSet) && null != ((ResultSet) dbresultSet.product())) {
            ((ResultSet) dbresultSet.product()).close();
        }
        //statement.close();
    }

    /**
     * create table
     * 
     * @param table
     * @return <code>true</code> if table is created
     */
    public boolean createTable(DBTable table) {
        return table.createTable(this);
    }

    /**
     * populate table with values
     * 
     * @param table
     * @return <code>true</code> if table is populated
     */
    public boolean populateTable(DBTable table) {
        return table.populateTable(this);
    }

    /**
     * Drop table from Database
     * 
     * @param dbstatement
     * @return true if table dropped
     */
    public boolean dropTable(DBTable table) {
        return table.dropTable(this);
    }

    @Override
    void setInternal(Object internal) {
        this.internal = internal;
    }

    /**
     * 
     * @return
     * @throws SQLException
     */
    public int getQueryTimeout() throws SQLException {
        return ((Statement) product()).getQueryTimeout();
    }

    /**
     * @return
     * @throws SQLException
     */
    public int getUpdateCount() throws SQLException {
        return ((Statement) product()).getUpdateCount();
    }

    /**
     * 
     * @return
     * @throws SQLException
     */
    public DBResultSet getResultSet() throws SQLException {
        ResultSet rs = ((Statement) product()).getResultSet();
        return dbresultSet = new DBResultSet(this, rs);
    }
}