/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;


/**
 * This class keeps the failover server info and if the mirror has become the primary. For synchronizing better and not
 * to keep a lock in the class through a connection open a placeholder class is used to get the failover info in one
 * shot. This class should never directly expose its members.
 */
final class FailoverInfo {
    private String failoverPartner;
    private int portNumber;
    private String failoverInstance;
    private boolean setUpInfocalled;
    private final Lock lock = new ReentrantLock();

    // This member is exposed outside for reading, we need to know in advance if the
    // failover partner is the currently active server before making a DNS resolution and a connect attempt.
    private boolean useFailoverPartner;

    boolean getUseFailoverPartner() {
        return useFailoverPartner;
    }

    FailoverInfo(String failover, SQLServerConnection con, boolean actualFailoverPartner) {
        failoverPartner = failover;
        useFailoverPartner = actualFailoverPartner;
        portNumber = -1; // init to -1 to make sure that the user of this class calls the failover check before getting
                         // the port number.
    }

    // the members of this class are not exposed so inorder to log we call this function.
    void log(SQLServerConnection con) {
        if (con.getConnectionLogger().isLoggable(Level.FINE))
            con.getConnectionLogger().fine(con.toString() + " Failover server :" + failoverPartner
                    + " Failover partner is primary : " + useFailoverPartner);
    }

    // this function gets the failover server port and sets up the security manager
    // note calls to these should be synchronized or guaranteed to happen from only one thread.
    private void setupInfo(SQLServerConnection con) throws SQLServerException {
        if (setUpInfocalled)
            return;

        if (0 == failoverPartner.length()) {
            portNumber = SQLServerConnection.DEFAULTPORT;
        } else {
            // 3.3006 get the instance name
            int px = failoverPartner.indexOf('\\');
            String instancePort;
            String instanceValue;

            // found the instance name with the severname
            if (px >= 0) {
                if (con.getConnectionLogger().isLoggable(Level.FINE))
                    con.getConnectionLogger().fine(con.toString() + " Failover server :" + failoverPartner);
                instanceValue = failoverPartner.substring(px + 1, failoverPartner.length());
                failoverPartner = failoverPartner.substring(0, px);
                con.validateMaxSQLLoginName(SQLServerDriverStringProperty.INSTANCE_NAME.toString(), instanceValue);
                failoverInstance = instanceValue;
                instancePort = con.getInstancePort(failoverPartner, instanceValue);

                try {
                    portNumber = Integer.parseInt(instancePort);
                } catch (NumberFormatException e) {
                    // Should not get here as the server should give a proper port number anyway.
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPortNumber"));
                    Object[] msgArgs = {instancePort};
                    SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, false);
                }
            } else
                portNumber = SQLServerConnection.DEFAULTPORT;
        }
        setUpInfocalled = true;
    }

    ServerPortPlaceHolder failoverPermissionCheck(SQLServerConnection con, boolean link) throws SQLServerException {
        lock.lock();
        try {
            setupInfo(con);
            return new ServerPortPlaceHolder(failoverPartner, portNumber, failoverInstance, link);
        } finally {
            lock.unlock();
        }
    }

    // Add/replace the failover server,
    void failoverAdd(SQLServerConnection connection, boolean actualUseFailoverPartner, String actualFailoverPartner) {
        lock.lock();
        try {
            if (useFailoverPartner != actualUseFailoverPartner) {
                if (connection.getConnectionLogger().isLoggable(Level.FINE))
                    connection.getConnectionLogger()
                            .fine(connection.toString() + " Failover detected. failover partner=" + actualFailoverPartner);
                useFailoverPartner = actualUseFailoverPartner;
            }
            // The checking for actualUseFailoverPartner may look weird but this is required
            // We only change the failoverpartner info when we connect to the primary
            // if we connect to the secondary and it sends a failover partner
            // we wont store that information.
            if (!actualUseFailoverPartner && !failoverPartner.equals(actualFailoverPartner)) {
                failoverPartner = actualFailoverPartner;
                // new FO partner need to setup again.
                setUpInfocalled = false;
            }
        } finally {
            lock.unlock();
        }
    }
}
