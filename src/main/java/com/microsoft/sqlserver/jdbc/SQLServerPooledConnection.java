/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;


/**
 * Represents a physical database connection in a connection pool. If provides methods for the connection pool manager
 * to manage the connection pool. Applications typically do not instantiate these connections directly.
 */

public class SQLServerPooledConnection implements PooledConnection, Serializable {
    /**
     * Always update serialVersionUID when prompted.
     */
    private static final long serialVersionUID = 3492921646187451164L;

    /** listeners */
    private final Vector<ConnectionEventListener> listeners;

    /** factory datasource */
    private SQLServerDataSource factoryDataSource;

    /** physical connection */
    private SQLServerConnection physicalConnection;

    /** last proxy connection */
    private SQLServerConnectionPoolProxy lastProxyConnection;

    /** factory password */
    private String factoryUser, factoryPassword;

    /** logger */
    private transient java.util.logging.Logger pcLogger;

    /** trace ID */
    private final String traceID;

    // Unique id generator for each PooledConnection instance (used for logging).
    static private final AtomicInteger basePooledConnectionID = new AtomicInteger(0);

    /** reentrant lock for connection */
    private final transient Lock lock = new ReentrantLock();

    /** reentrant lock for ConnectionEventListener */
    private final transient Lock listenersLock = new ReentrantLock();

    SQLServerPooledConnection(SQLServerDataSource ds, String user, String password) throws SQLException {
        listeners = new Vector<>();
        traceID = getClass().getSimpleName() + ':' + nextPooledConnectionID();
        // Piggyback SQLServerDataSource logger for now.
        pcLogger = SQLServerDataSource.dsLogger;

        // Create the physical connection.
        factoryDataSource = ds;
        factoryUser = user;
        factoryPassword = password;

        if (pcLogger.isLoggable(Level.FINER))
            pcLogger.finer(toString() + " Start create new connection for pool.");

        physicalConnection = createNewConnection();
        if (pcLogger.isLoggable(Level.FINE))
            pcLogger.fine(toString() + " created by (" + ds.toString() + ")" + " Physical connection " + safeCID()
                    + ", End create new connection for pool");
    }

    /**
     * Provides a helper function to provide an ID string suitable for tracing.
     *
     * @return traceID String
     */
    @Override
    public String toString() {
        return traceID;
    }

    /**
     * Helper function to create a new connection for the pool.
     *
     * @return SQLServerConnection instance
     * @throws SQLException
     */
    private SQLServerConnection createNewConnection() throws SQLException {
        return factoryDataSource.getConnectionInternal(factoryUser, factoryPassword, this);
    }

    /**
     * Returns an object handle for the physical connection that this PooledConnection object represents.
     *
     * @throws SQLException
     *         when an error occurs
     * @return a Connection object that is a handle to this PooledConnection object
     */
    @Override
    public Connection getConnection() throws SQLException {
        if (pcLogger.isLoggable(Level.FINER))
            pcLogger.finer(toString() + " user:(default).");
        lock.lock();
        try {
            // If physical connection is closed, throw exception per spec, this PooledConnection is dead.
            if (physicalConnection == null) {
                SQLServerException.makeFromDriverError(null, this,
                        SQLServerException.getErrString("R_physicalConnectionIsClosed"), "", true);
            }

            /*
             * Check with security manager to insure caller has rights to connect. This will throw a SecurityException
             * if the caller does not have proper rights.
             */
            physicalConnection.doSecurityCheck();
            if (pcLogger.isLoggable(Level.FINE))
                pcLogger.fine(toString() + " Physical connection, " + safeCID());

            if (physicalConnection.needsReconnect()) {
                physicalConnection.close();
                physicalConnection = createNewConnection();
            }

            /*
             * The last proxy connection handle returned will be invalidated (moved to closed state) when getConnection
             * is called.
             */
            if (null != lastProxyConnection) {
                // if there was a last proxy connection send reset
                physicalConnection.resetPooledConnection();

                if (!lastProxyConnection.isClosed()) {
                    if (pcLogger.isLoggable(Level.FINE)) {
                        pcLogger.fine(toString() + "proxy " + lastProxyConnection.toString()
                                + " is not closed before getting the connection.");
                    }
                    /*
                     * use internal close so there wont be an event due to us closing the connection, if not closed
                     * already.
                     */
                    lastProxyConnection.internalClose();
                }
            }

            lastProxyConnection = new SQLServerConnectionPoolProxy(physicalConnection);
            if (pcLogger.isLoggable(Level.FINE) && !lastProxyConnection.isClosed())
                pcLogger.fine(toString() + " proxy " + lastProxyConnection.toString() + " is returned.");

            return lastProxyConnection;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Notifies any interested parties (e.g. pooling managers) of a ConnectionEvent activity on the connection. Calling
     * notifyEvent with null event will place the connection back in the pool. Calling notifyEvent with a non-null event
     * is used to notify the pooling manager that the connection is bad and should be removed from the pool.
     */
    void notifyEvent(SQLServerException e) {
        if (pcLogger.isLoggable(Level.FINER))
            pcLogger.finer(toString() + " Exception:" + e + safeCID());

        // close the proxy on fatal error event. Note exception is null then the event comes from the proxy close.
        if (null != e) {
            lock.lock();
            try {
                if (null != lastProxyConnection) {
                    lastProxyConnection.internalClose();
                    lastProxyConnection = null;
                }
            } finally {
                lock.unlock();
            }
        }

        // A connection handle issued from this pooled connection is closing or an error occurred in the connection
        listenersLock.lock();
        try {
            for (int i = 0; i < listeners.size(); i++) {
                ConnectionEventListener listener = listeners.elementAt(i);

                if (listener == null)
                    continue;

                ConnectionEvent ev = new ConnectionEvent(this, e);
                if (null == e) {
                    if (pcLogger.isLoggable(Level.FINER))
                        pcLogger.finer(toString() + " notifyEvent:connectionClosed " + safeCID());
                    listener.connectionClosed(ev);
                } else {
                    if (pcLogger.isLoggable(Level.FINER))
                        pcLogger.finer(toString() + " notifyEvent:connectionErrorOccurred " + safeCID());
                    listener.connectionErrorOccurred(ev);
                }
            }
        } finally {
            listenersLock.unlock();
        }
    }

    @Override
    public void addConnectionEventListener(ConnectionEventListener listener) {
        if (pcLogger.isLoggable(Level.FINER))
            pcLogger.finer(toString() + safeCID());
        listenersLock.lock();
        try {
            listeners.add(listener);
        } finally {
            listenersLock.unlock();
        }
    }

    @Override
    public void close() throws SQLException {
        if (pcLogger.isLoggable(Level.FINER))
            pcLogger.finer(toString() + " Closing physical connection, " + safeCID());
        lock.lock();
        try {
            // First close the last proxy
            if (null != lastProxyConnection)
                // use internal close so there wont be an event due to us closing the connection, if not closed already.
                lastProxyConnection.internalClose();
            if (null != physicalConnection) {
                physicalConnection.detachFromPool();
                physicalConnection.close();
            }
            physicalConnection = null;
        } finally {
            lock.unlock();
        }
        listenersLock.lock();
        try {
            listeners.clear();
        } finally {
            listenersLock.unlock();
        }
    }

    @Override
    public void removeConnectionEventListener(ConnectionEventListener listener) {
        if (pcLogger.isLoggable(Level.FINER))
            pcLogger.finer(toString() + safeCID());
        listenersLock.lock();
        try {
            listeners.remove(listener);
        } finally {
            listenersLock.unlock();
        }
    }

    @Override
    public void addStatementEventListener(StatementEventListener listener) {
        // Not implemented
        throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));
    }

    @Override
    public void removeStatementEventListener(StatementEventListener listener) {
        // Not implemented
        throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));
    }

    // Returns internal physical connection to caller.
    SQLServerConnection getPhysicalConnection() {
        return physicalConnection;
    }

    // Returns unique id for each PooledConnection instance.
    private static int nextPooledConnectionID() {
        return basePooledConnectionID.incrementAndGet();
    }

    /**
     * Helper function to return connectionID of the physicalConnection in a safe manner for logging. Returns (null) if
     * physicalConnection is null, otherwise returns connectionID.
     **/
    private String safeCID() {
        if (null == physicalConnection)
            return " ConnectionID:(null)";
        return physicalConnection.toString();
    }
}
