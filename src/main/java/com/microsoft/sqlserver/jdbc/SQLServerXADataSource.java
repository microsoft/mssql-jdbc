/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.SQLException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.Reference;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

/**
 * SQLServerXADataSource provides database connections for use in distributed (XA) transactions. SQLServerXADataSource also supports connection
 * pooling of physical connections.
 *
 * The XADataSource and XAConnection interfaces, which are defined in the package javax.sql, are implemented by sqlserver. An XAConnection object is a
 * pooled connection that can participate in a distributed transaction. More precisely, XAConnection extends the PooledConnection interface by adding
 * the method getXAResource. This method produces an XAResource object that can be used by a transaction manager to coordinate the work done on this
 * connection with the other participants in the distributed transaction.
 *
 * <p>
 * Because they extend the PooledConnection interface, XAConnection objects support all the methods of PooledConnection objects. They are reusable
 * physical connections to an underlying data source and produce logical connection handles that can be passed back to a JDBC application.
 *
 * <p>
 * XAConnection objects are produced by an XADataSource object. There is some similarity between ConnectionPoolDataSource objects and XADataSource
 * objects in that they are both implemented below a DataSource layer that is visible to the JDBC application. This architecture allows sqlserver to
 * support distributed transactions in a way that is transparent to the application.
 *
 * <br>
 * <br>
 * SQLServerXADataSource can be configured to integrate with Microsoft Distributed Transaction Coordinator (DTC) to provide true, distributed
 * transaction processing.
 */

public final class SQLServerXADataSource extends SQLServerConnectionPoolDataSource implements XADataSource {

    static Logger xaLogger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.XA");

    /**
     * Obtain a physical database connection to particate in an XA transaction with the specified user and password. This API should only be called by
     * XA connection pool implementations, not regular JDBC application code.
     *
     * @return A new XAConnection
     * @exception SQLException
     *                The database connection failed.
     */
    /* L0 */ public XAConnection getXAConnection(String user,
            String password) throws SQLException {
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getXAConnection", new Object[] {user, "Password not traced"});
        SQLServerXAConnection pooledXAConnection = new SQLServerXAConnection(this, user, password);

        if (xaLogger.isLoggable(Level.FINER))
            xaLogger.finer(toString() + " user:" + user + pooledXAConnection.toString());

        // Don't start a transaction here but do mark the connection as autocommit false.
        // We cannot start a transaction since XA transaction type 'NEVER' does not start a transaction
        // Autocommit of false is required to ensure that the transaction manager's calls to commit and
        // rollback work correctly.

        if (xaLogger.isLoggable(Level.FINER))
            xaLogger.finer(toString() + " Start get physical connection.");
        SQLServerConnection physicalConnection = pooledXAConnection.getPhysicalConnection();
        if (xaLogger.isLoggable(Level.FINE))
            xaLogger.fine(toString() + " End get physical connection, " + physicalConnection.toString());
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.exiting(getClassNameLogging(), "getXAConnection", pooledXAConnection);
        return pooledXAConnection;
    }

    /**
     * Obtain a physical database connection to particate in an XA transaction. This API should only be called by XA connection pool implementations,
     * not regular JDBC application code.
     *
     * @return A new XAConnection
     * @exception SQLException
     *                The database connection failed.
     */
    /* L0 */ public XAConnection getXAConnection() throws SQLException {
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getXAConnection");
        return getXAConnection(getUser(), getPassword());
    }

    // Implement javax.naming.Referenceable interface methods.

    public Reference getReference() {
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getReference");
        Reference ref = getReferenceInternal("com.microsoft.sqlserver.jdbc.SQLServerXADataSource");
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.exiting(getClassNameLogging(), "getReference", ref);
        return ref;
    }

    private Object writeReplace() throws java.io.ObjectStreamException {
        return new SerializationProxy(this);
    }

    private void readObject(java.io.ObjectInputStream stream) throws java.io.InvalidObjectException {
        // For added security/robustness, the only way to rehydrate a serialized SQLServerXADataSource
        // is to use a SerializationProxy. Direct use of readObject() is not supported.
        throw new java.io.InvalidObjectException("");
    }

    // This is 90% duplicate from the SQLServerDataSource, the serialization proxy pattern does not lend itself to inheritance
    // so the duplication is necessary
    private static class SerializationProxy implements java.io.Serializable {
        private final Reference ref;
        private static final long serialVersionUID = 454661379842314126L;

        SerializationProxy(SQLServerXADataSource ds) {
            // We do not need the class name so pass null, serialization mechanism
            // stores the class info.
            ref = ds.getReferenceInternal(null);
        }

        private Object readResolve() {
            SQLServerXADataSource ds = new SQLServerXADataSource();
            ds.initializeFromReference(ref);
            return ds;
        }
    }

}
