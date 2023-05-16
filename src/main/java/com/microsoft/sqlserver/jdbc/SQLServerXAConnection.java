/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import javax.sql.XAConnection;
import javax.transaction.xa.XAResource;
import java.sql.SQLException;
import java.util.Properties;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Provides JDBC connections that can participate in distributed (XA) transactions.
 */
public final class SQLServerXAConnection extends SQLServerPooledConnection implements XAConnection {

    /**
     * Always update serialVersionUID when prompted.
     */
    private static final long serialVersionUID = -8154621218821899459L;

    /**
     * NB These instances are not used by applications, only by the app server who is providing the connection pool and
     * transactional processing to the application. That app server is the one who should restrict commit/rollback on
     * the connections it issues to applications, not the driver. These instances can and must commit/rollback
     */
    private volatile transient SQLServerXAResource xaResource;

    /** physical connection */
    private SQLServerConnection physicalControlConnection;

    /** logger */
    private transient Logger xaLogger;

    /** reentrant lock */
    private final transient Lock lock = new ReentrantLock();

    SQLServerXAConnection(SQLServerDataSource ds, String user, String pwd) throws java.sql.SQLException {
        super(ds, user, pwd);
        // Grab SQLServerXADataSource's static XA logger instance.
        xaLogger = SQLServerXADataSource.xaLogger;
        SQLServerConnection con = getPhysicalConnection();

        Properties controlConnectionProperties = (Properties) con.activeConnectionProperties.clone();
        // Arguments to be sent as unicode always to the server, as the stored procs always write unicode chars as out
        // param.
        controlConnectionProperties
                .setProperty(SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.toString(), "true");
        controlConnectionProperties.remove(SQLServerDriverStringProperty.SELECT_METHOD.toString());

        // Add password property for NTLM as physical connection had previously removed. This will be removed again
        String auth = controlConnectionProperties
                .getProperty(SQLServerDriverStringProperty.AUTHENTICATION_SCHEME.toString());
        if (null != auth && AuthenticationScheme.NTLM == AuthenticationScheme.valueOfString(auth)) {
            controlConnectionProperties.setProperty(SQLServerDriverStringProperty.PASSWORD.toString(), pwd);
        }

        // Add truststore password property for creating the control connection. This will be removed again
        String trustStorePassword = ds.getTrustStorePassword();
        if (null == trustStorePassword) {
            // trustStorePassword can either come from the connection string or added via
            // SQLServerXADataSource::setTrustStorePassword.
            // if trustStorePassword is null at this point, then check the connection string.
            Properties urlProps = Util.parseUrl(ds.getURL(), xaLogger);
            trustStorePassword = urlProps.getProperty(SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString());
        }

        // if trustStorePassword is still null, it wasn't provided. Do not set the property as null to avoid NPE.
        if (null != trustStorePassword) {
            controlConnectionProperties.setProperty(SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString(),
                    trustStorePassword);
        }

        // Add clientKeyPassword password property for creating the control connection. This will be removed again
        // first check if clientCertificate is there to see if the clientKeyPassword was possibly provided
        String clientCertificate = ds.getClientCertificate();
        if (null != clientCertificate && clientCertificate.length() > 0) {
            Properties urlProps = Util.parseUrl(ds.getURL(), xaLogger);
            String clientKeyPassword = urlProps
                    .getProperty(SQLServerDriverStringProperty.CLIENT_KEY_PASSWORD.toString());

            if (null != clientKeyPassword) {
                controlConnectionProperties.setProperty(SQLServerDriverStringProperty.CLIENT_KEY_PASSWORD.toString(),
                        clientKeyPassword);
            }
        }

        if (xaLogger.isLoggable(Level.FINER))
            xaLogger.finer("Creating an internal control connection for" + toString());
        physicalControlConnection = null;
        physicalControlConnection = DriverJDBCVersion.getSQLServerConnection(toString());
        physicalControlConnection.connect(controlConnectionProperties, null);
        if (xaLogger.isLoggable(Level.FINER))
            xaLogger.finer("Created an internal control connection" + physicalControlConnection.toString() + " for "
                    + toString() + " Physical connection:" + getPhysicalConnection().toString());

        if (xaLogger.isLoggable(Level.FINER))
            xaLogger.finer(ds.toString() + " user:" + user);
    }

    @Override
    public XAResource getXAResource() throws java.sql.SQLException {
        // All connections handed out from this physical connection have a common XAResource
        // for transaction control. IE the XAResource is one to one with the physical connection.
        SQLServerXAResource result = xaResource;
        if (result == null) {
            lock.lock();
            try {
                result = xaResource;
                if (result == null) {
                    xaResource = result = new SQLServerXAResource(getPhysicalConnection(), physicalControlConnection,
                            toString());
                }
            } finally {
                lock.unlock();
            }
        }
        return result;
    }

    /**
     * Closes the physical connection that this PooledConnection object represents.
     */
    @Override
    public void close() throws SQLException {
        lock.lock();
        try {
            if (xaResource != null) {
                xaResource.close();
                xaResource = null;
            }
            if (null != physicalControlConnection) {
                physicalControlConnection.close();
                physicalControlConnection = null;
            }
        } finally {
            lock.unlock();
        }
        super.close();
    }
}
