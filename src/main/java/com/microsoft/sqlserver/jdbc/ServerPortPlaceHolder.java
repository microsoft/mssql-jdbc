/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.Serializable;


/**
 * A simple readonly placeholder class to store the current server info. We need this class so during a connection open
 * we can keep a copy of the current failover info stable This is also used to keep the standalone primary server
 * connection information.
 */
final class ServerPortPlaceHolder implements Serializable {
    /**
     * Always update serialVersionUID when prompted.
     */
    private static final long serialVersionUID = 7393779415545731523L;

    private final String serverName;
    private final String parsedServerName;
    private final String fullServerName;
    private final int port;
    private final String instanceName;
    private final boolean checkLink;
    private final transient SQLServerConnectionSecurityManager securityManager;

    ServerPortPlaceHolder(String name, int conPort, String instance, boolean fLink) {
        serverName = name;

        // serverName without named instance
        int px = serverName.indexOf('\\');
        parsedServerName = (px >= 0) ? serverName.substring(0, px) : serverName;

        // serverName with named instance
        fullServerName = (null != instance) ? (serverName + "\\" + instance) : serverName;

        port = conPort;
        instanceName = instance;
        checkLink = fLink;
        securityManager = new SQLServerConnectionSecurityManager(serverName, port);
        doSecurityCheck();
    }

    // accessors
    int getPortNumber() {
        return port;
    }

    String getServerName() {
        return serverName;
    }

    String getInstanceName() {
        return instanceName;
    }

    String getParsedServerName() {
        return parsedServerName;
    }

    String getFullServerName() {
        return fullServerName;
    }

    void doSecurityCheck() {
        securityManager.checkConnect();
        if (checkLink)
            securityManager.checkLink();
    }
}
