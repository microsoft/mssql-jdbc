// ---------------------------------------------------------------------------------------------------------------------------------
// File: DBStatement.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"),
// to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense,
// and / or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.
// ---------------------------------------------------------------------------------------------------------------------------------

package com.microsoft.sqlserver.testframework;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;

/**
 * wrapper method for Statement object
 * 
 * @author Microsoft
 *
 */
public class DBStatement extends AbstractParentWrapper {

    // TODO: support PreparedStatement and CallableStatement
    // TODO: add stmt level holdability
    // TODO: support IDENTITY column and stmt.getGeneratedKeys()

    Statement statement = null;
    DBResultSet dbresultSet = null;
    public int _cursortype = ResultSet.TYPE_FORWARD_ONLY;
    public int _concurrency = ResultSet.CONCUR_READ_ONLY;
    public int _holdability = ResultSet.CLOSE_CURSORS_AT_COMMIT;
    
    DBStatement(DBConnection dbConnection) {
        super(dbConnection, null, "statement");
    }
    
    DBStatement(DBConnection parent, Statement internal, int type, int concurrency, int holdability) {
        super(parent, null, "statement");
    }

    DBStatement statement() {
        return this;
    }

    DBStatement createStatement() throws SQLServerException {
        // TODO: add cursor and holdability
        statement = ((SQLServerConnection) parent().product()).createStatement();
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
     * Close the <code>Statement</code> and <code>ResultSet</code> associated
     * with it
     * 
     * @throws SQLException
     */
    public void close() throws SQLException {
        if ((null != dbresultSet) && null != ((ResultSet) dbresultSet.product())) {
            ((ResultSet) dbresultSet.product()).close();
        }
        statement.close();
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
    public int getQueryTimeout() throws SQLException
    {
       int current = ((Statement) product()).getQueryTimeout();     
       return current;
    }
}
