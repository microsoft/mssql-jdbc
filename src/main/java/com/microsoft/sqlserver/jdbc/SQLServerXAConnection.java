/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;

/**
 * SQLServerXAConnection provides JDBC connections that can participate in distributed (XA) transactions.
 */
public final class SQLServerXAConnection extends SQLServerPooledConnection implements XAConnection {

    // NB These instances are not used by applications, only by the app server who is
    // providing the connection pool and transactional processing to the application.
    // That app server is the one who should restrict commit/rollback on the connections
    // it issues to applications, not the driver. These instances can and must commit/rollback
    private SQLServerXAResource XAResource;
    private SQLServerConnection physicalControlConnection;
    private Logger xaLogger;

    /* L0 */ SQLServerXAConnection(SQLServerDataSource ds,
            String user,
            String pwd) throws java.sql.SQLException {
        super(ds, user, pwd);
        // Grab SQLServerXADataSource's static XA logger instance.
        xaLogger = SQLServerXADataSource.xaLogger;
        SQLServerConnection con = getPhysicalConnection();

        Properties controlConnectionProperties = (Properties) con.activeConnectionProperties.clone();
        // Arguments to be sent as unicode always to the server, as the stored procs always write unicode chars as out param.
        controlConnectionProperties.setProperty(SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.toString(), "true");
        controlConnectionProperties.remove(SQLServerDriverStringProperty.SELECT_METHOD.toString());

        if (xaLogger.isLoggable(Level.FINER))
            xaLogger.finer("Creating an internal control connection for" + toString());
        physicalControlConnection = null;
        if (Util.use43Wrapper()) {
            physicalControlConnection = new SQLServerConnection43(toString());
        }
        else {
            physicalControlConnection = new SQLServerConnection(toString());
        }
        physicalControlConnection.connect(controlConnectionProperties, null);
        if (xaLogger.isLoggable(Level.FINER))
            xaLogger.finer("Created an internal control connection" + physicalControlConnection.toString() + " for " + toString()
                    + " Physical connection:" + getPhysicalConnection().toString());

        if (xaLogger.isLoggable(Level.FINER))
            xaLogger.finer(ds.toString() + " user:" + user);
    }

    /* L0 */ public synchronized XAResource getXAResource() throws java.sql.SQLException {
        // All connections handed out from this physical connection have a common XAResource
        // for transaction control. IE the XAResource is one to one with the physical connection.

        if (XAResource == null)
            XAResource = new SQLServerXAResource(getPhysicalConnection(), physicalControlConnection, toString());
        return XAResource;
    }

    /**
     * Closes the physical connection that this PooledConnection object represents.
     */
    public void close() throws SQLException {
        synchronized (this) {
            if (XAResource != null) {
                XAResource.close();
                XAResource = null;
            }
            if (null != physicalControlConnection) {
                physicalControlConnection.close();
                physicalControlConnection = null;
            }
        }
        super.close();
    }

}
