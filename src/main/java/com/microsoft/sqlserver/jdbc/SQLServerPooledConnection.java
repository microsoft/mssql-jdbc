/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.PooledConnection;
import javax.sql.StatementEventListener;

/**
 * SQLServerPooledConnection represents a database physical connection in a connection pool. If provides methods for the connection pool manager to
 * manage the connection pool. Applications typically do not instantiate these connections directly.
 */

public class SQLServerPooledConnection implements PooledConnection {
    private final Vector<ConnectionEventListener> listeners;
    private SQLServerDataSource factoryDataSource;
    private SQLServerConnection physicalConnection;
    private SQLServerConnectionPoolProxy lastProxyConnection;
    private String factoryUser, factoryPassword;
    private java.util.logging.Logger pcLogger;
    static private final AtomicInteger basePooledConnectionID = new AtomicInteger(0);	// Unique id generator for each PooledConnection instance
                                                                                     	// (used for logging).
    private final String traceID;

    SQLServerPooledConnection(SQLServerDataSource ds,
            String user,
            String password) throws SQLException {
        listeners = new Vector<>();
        // Piggyback SQLServerDataSource logger for now.
        pcLogger = SQLServerDataSource.dsLogger;

        // Create the physical connection.
        factoryDataSource = ds;
        factoryUser = user;
        factoryPassword = password;

        if (pcLogger.isLoggable(Level.FINER))
            pcLogger.finer(toString() + " Start create new connection for pool.");

        physicalConnection = createNewConnection();
        String nameL = getClass().getName();
        traceID = nameL.substring(1 + nameL.lastIndexOf('.')) + ":" + nextPooledConnectionID();
        if (pcLogger.isLoggable(Level.FINE))
            pcLogger.fine(toString() + " created by (" + ds.toString() + ")" + " Physical connection " + safeCID()
                    + ", End create new connection for pool");
    }

    /**
     * This is a helper function to provide an ID string suitable for tracing.
     * 
     * @return traceID String
     */
    public String toString() {
        return traceID;
    }

    // Helper function to create a new connection for the pool.
    private SQLServerConnection createNewConnection() throws SQLException {
        return factoryDataSource.getConnectionInternal(factoryUser, factoryPassword, this);
    }

    /**
     * Creates an object handle for the physical connection that this PooledConnection object represents.
     * 
     * @throws SQLException
     *             when an error occurs
     * @return a Connection object that is a handle to this PooledConnection object
     */
    public Connection getConnection() throws SQLException {
        if (pcLogger.isLoggable(Level.FINER))
            pcLogger.finer(toString() + " user:(default).");
        synchronized (this) {
            // If physical connection is closed, throw exception per spec, this PooledConnection is dead.
            if (physicalConnection == null) {
                SQLServerException.makeFromDriverError(null, this, SQLServerException.getErrString("R_physicalConnectionIsClosed"), "", true);
            }

            // Check with security manager to insure caller has rights to connect.
            // This will throw a SecurityException if the caller does not have proper rights.
            physicalConnection.doSecurityCheck();
            if (pcLogger.isLoggable(Level.FINE))
                pcLogger.fine(toString() + " Physical connection, " + safeCID());

            if (null != physicalConnection.getAuthenticationResult()) {
                if (Util.checkIfNeedNewAccessToken(physicalConnection)) {
                    physicalConnection = createNewConnection();
                }
            }

            // The last proxy connection handle returned will be invalidated (moved to closed state)
            // when getConnection is called.
            if (null != lastProxyConnection) {
                // if there was a last proxy connection send reset
                physicalConnection.resetPooledConnection();
                if (pcLogger.isLoggable(Level.FINE) && !lastProxyConnection.isClosed())
                    pcLogger.fine(toString() + "proxy " + lastProxyConnection.toString() + " is not closed before getting the connection.");
                // use internal close so there wont be an event due to us closing the connection, if not closed already.
                lastProxyConnection.internalClose();
            }

            lastProxyConnection = new SQLServerConnectionPoolProxy(physicalConnection);
            if (pcLogger.isLoggable(Level.FINE) && !lastProxyConnection.isClosed())
                pcLogger.fine(toString() + " proxy " + lastProxyConnection.toString() + " is returned.");

            return lastProxyConnection;
        }
    }

    // Notify any interested parties (e.g. pooling managers) of a ConnectionEvent activity
    // on the connection. Calling notifyEvent with null event will place the
    // connection back in the pool. Calling notifyEvent with a non-null event is
    // used to notify the pooling manager that the connection is bad and should be removed
    // from the pool.
    void notifyEvent(SQLServerException e) {
        if (pcLogger.isLoggable(Level.FINER))
            pcLogger.finer(toString() + " Exception:" + e + safeCID());

        // close the proxy on fatal error event. Note exception is null then the event comes from the proxy close.
        if (null != e) {
            synchronized (this) {
                if (null != lastProxyConnection) {
                    lastProxyConnection.internalClose();
                    lastProxyConnection = null;
                }
            }
        }

        // A connection handle issued from this pooled connection is closing or an error occurred in the connection
        synchronized (listeners) {
            for (int i = 0; i < listeners.size(); i++) {
                ConnectionEventListener listener = listeners.elementAt(i);

                if (listener == null)
                    continue;

                ConnectionEvent ev = new ConnectionEvent(this, e);
                if (null == e) {
                    if (pcLogger.isLoggable(Level.FINER))
                        pcLogger.finer(toString() + " notifyEvent:connectionClosed " + safeCID());
                    listener.connectionClosed(ev);
                }
                else {
                    if (pcLogger.isLoggable(Level.FINER))
                        pcLogger.finer(toString() + " notifyEvent:connectionErrorOccurred " + safeCID());
                    listener.connectionErrorOccurred(ev);
                }
            }
        }
    }

    public void addConnectionEventListener(ConnectionEventListener listener) {
        if (pcLogger.isLoggable(Level.FINER))
            pcLogger.finer(toString() + safeCID());
        synchronized (listeners) {
            listeners.add(listener);
        }
    }

    public void close() throws SQLException {
        if (pcLogger.isLoggable(Level.FINER))
            pcLogger.finer(toString() + " Closing physical connection, " + safeCID());
        synchronized (this) {
            // First close the last proxy
            if (null != lastProxyConnection)
                // use internal close so there wont be an event due to us closing the connection, if not closed already.
                lastProxyConnection.internalClose();
            if (null != physicalConnection) {
                physicalConnection.DetachFromPool();
                physicalConnection.close();
            }
            physicalConnection = null;
        }
        synchronized (listeners) {
            listeners.clear();
        }

    }

    public void removeConnectionEventListener(ConnectionEventListener listener) {
        if (pcLogger.isLoggable(Level.FINER))
            pcLogger.finer(toString() + safeCID());
        synchronized (listeners) {
            listeners.remove(listener);
        }
    }

    public void addStatementEventListener(StatementEventListener listener) {
        // Not implemented
        throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));
    }

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

    // Helper function to return connectionID of the physicalConnection in a safe manner for logging.
    // Returns (null) if physicalConnection is null, otherwise returns connectionID.
    private String safeCID() {
        if (null == physicalConnection)
            return " ConnectionID:(null)";
        return physicalConnection.toString();
    }
}
