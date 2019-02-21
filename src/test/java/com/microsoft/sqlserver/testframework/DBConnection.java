/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import javax.sql.PooledConnection;
import javax.sql.XAConnection;

import com.microsoft.sqlserver.jdbc.ISQLServerConnection;
import com.microsoft.sqlserver.jdbc.ISQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;


/*
 * Wrapper class for SQLServerConnection
 */
public class DBConnection extends AbstractParentWrapper implements AutoCloseable {
    private double serverversion = 0;

    // TODO: add Isolation Level
    // TODO: add auto commit
    // TODO: add connection Savepoint and rollback
    // TODO: add additional connection properties
    private Connection connection = null;
    private XAConnection xaConnection = null;
    private PooledConnection pooledConnection = null;

    /**
     * establishes connection using the input
     * 
     * @param connectionString
     */
    public DBConnection(String connectionString) {
        super(null, null, "connection");
        getConnection(connectionString);
    }

    public DBConnection(Connection connection) {
        super(null, null, "connection");
        this.connection = connection;
        setInternal(connection);
    }

    /**
     * establishes connection using the input
     * 
     * @param connectionString
     */
    public DBConnection(ISQLServerDataSource dataSource) {
        super(null, null, "connection");
        getConnection(dataSource);
    }

    /**
     * establish connection
     * 
     * @param connectionString
     */
    void getConnection(String connectionString) {
        try {
            connection = PrepUtil.getConnection(connectionString);
            setInternal(connection);
        } catch (SQLException ex) {
            fail(ex.getMessage());
        } catch (ClassNotFoundException ex) {
            fail(ex.getMessage());
        }
    }

    /**
     * establish connection
     * 
     * @param connectionString
     */
    void getConnection(ISQLServerDataSource dataSource) {
        try {
            if (dataSource instanceof SQLServerXADataSource) {
                xaConnection = (XAConnection) ((SQLServerXADataSource) dataSource).getXAConnection();
                connection = (ISQLServerConnection) xaConnection.getConnection();
            } else if (dataSource instanceof SQLServerConnectionPoolDataSource) {
                pooledConnection = (PooledConnection) ((SQLServerConnectionPoolDataSource) dataSource)
                        .getPooledConnection();
                connection = (ISQLServerConnection) pooledConnection.getConnection();
            } else if (dataSource instanceof SQLServerDataSource) {
                connection = (ISQLServerConnection) ((SQLServerDataSource) dataSource).getConnection();
            }
            setInternal(connection);
        } catch (SQLException ex) {
            fail(ex.getMessage());
        }
    }

    @Override
    void setInternal(Object internal) {
        this.internal = internal;
    }

    /**
     * 
     * @return Statement wrapper
     */
    @SuppressWarnings("resource")
    public DBStatement createStatement() {
        try {
            return new DBStatement(this).createStatement();
        } catch (SQLException ex) {
            fail(ex.getMessage());
        }
        return null;
    }

    /**
     * 
     * @param type
     * @param concurrency
     * @return
     * @throws SQLException
     */
    @SuppressWarnings("resource")
    public DBStatement createStatement(int type, int concurrency) throws SQLException {
        return new DBStatement(this).createStatement(type, concurrency);

    }

    /**
     * 
     * @param rsType
     * @return
     * @throws SQLException
     */
    @SuppressWarnings("resource")
    public DBStatement createStatement(DBResultSetTypes rsType) throws SQLException {
        return new DBStatement(this).createStatement(rsType.resultsetCursor, rsType.resultSetConcurrency);
    }

    /**
     * 
     * @param query
     * @return
     * @throws SQLException
     */
    @SuppressWarnings("resource")
    public DBPreparedStatement prepareStatement(String query) throws SQLException {
        return new DBPreparedStatement(this).prepareStatement(query);
    }

    /**
     * 
     * @param query
     * @param type
     * @param concurrency
     * @return
     * @throws SQLException
     */
    @SuppressWarnings("resource")
    public DBPreparedStatement prepareStatement(String query, int type, int concurrency) throws SQLException {
        // Static for fast-forward, limited settings
        if ((type == ResultSet.TYPE_FORWARD_ONLY || type == ResultSet.TYPE_SCROLL_INSENSITIVE))
            concurrency = ResultSet.CONCUR_READ_ONLY;

        return new DBPreparedStatement(this).prepareStatement(query, type, concurrency);
    }

    /**
     * close all open connections
     */
    public void close() {
        try {
            if (null != connection)
                connection.close();
            if (null != xaConnection)
                xaConnection.close();
            if (null != pooledConnection)
                pooledConnection.close();
        } catch (SQLException ex) {
            fail(ex.getMessage());
        }
    }

    /**
     * checks if the connection is closed.
     * 
     * @return true if connection is closed.
     * @throws SQLException
     */
    public boolean isClosed() {
        boolean current = false;
        try {
            current = connection.isClosed();
        } catch (SQLException ex) {
            fail(ex.getMessage());
        }
        return current;
    }

    /**
     * Retrieves metaData
     * 
     * @return
     * @throws SQLException
     */
    public DatabaseMetaData getMetaData() throws SQLException {
        DatabaseMetaData product = connection.getMetaData();
        return product;
    }

    /**
     * @param string
     * @return
     * @throws SQLException
     */
    public DBCallableStatement prepareCall(String query) throws SQLException {
        DBCallableStatement dbcstmt = new DBCallableStatement(this);
        return dbcstmt.prepareCall(query);
    }

    /**
     * Retrieve server version
     * 
     * @return server version
     * @throws Exception
     */
    public double getServerVersion() throws Exception {
        if (0 == serverversion) {
            DBStatement stmt = null;
            DBResultSet rs = null;

            try {
                stmt = this.createStatement(DBResultSet.TYPE_DIRECT_FORWARDONLY, ResultSet.CONCUR_READ_ONLY);
                rs = stmt.executeQuery("SELECT @@VERSION");
                rs.next();

                String version = rs.getString(1);
                // i.e. " - 10.50.1064.0"
                int firstDot = version.indexOf('.');
                assert ((firstDot - 2) > 0);
                int secondDot = version.indexOf('.', (firstDot + 1));
                try {
                    serverversion = Double.parseDouble(version.substring((firstDot - 2), secondDot));
                } catch (NumberFormatException ex) {
                    // for CTP version parsed as P2.3) - 13 throws number format exception
                    serverversion = 16;
                }
            } catch (Exception e) {
                throw new Exception("Unable to get dbms major version", e);
            } finally {
                rs.close();
                stmt.close();
            }
        }
        return serverversion;
    }

}
