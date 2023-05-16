/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.HashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;


final class FailoverMapSingleton {
    private static int initialHashmapSize = 5;
    private static HashMap<String, FailoverInfo> failoverMap = new HashMap<>(initialHashmapSize);
    private static final Lock LOCK = new ReentrantLock();

    private FailoverMapSingleton() {
        /* hide the constructor to stop the instantiation of this class. */}

    private static String concatPrimaryDatabase(String primary, String instance, String database) {
        StringBuilder buf = new StringBuilder();
        buf.append(primary);
        if (null != instance) {
            buf.append("\\");
            buf.append(instance);
        }
        buf.append(";");
        buf.append(database);
        return buf.toString();
    }

    static FailoverInfo getFailoverInfo(SQLServerConnection connection, String primaryServer, String instance,
            String database) {
        LOCK.lock();
        try {
            if (failoverMap.isEmpty()) {
                return null;
            } else {
                String mapKey = concatPrimaryDatabase(primaryServer, instance, database);
                if (connection.getConnectionLogger().isLoggable(Level.FINER))
                    connection.getConnectionLogger()
                            .finer(connection.toString() + " Looking up info in the map using key: " + mapKey);
                FailoverInfo fo = failoverMap.get(mapKey);
                if (null != fo)
                    fo.log(connection);
                return fo;
            }
        } finally {
            LOCK.unlock();
        }
    }

    // The map is populated with primary server, instance name and db as the key (always user info) value is the
    // failover server name provided
    // by the server. The map is only populated if the server sends failover info.
    static void putFailoverInfo(SQLServerConnection connection, String primaryServer, String instance, String database,
            FailoverInfo actualFailoverInfo, boolean actualuseFailover, String failoverPartner) {
        FailoverInfo fo;

        LOCK.lock();
        try {
            // one more check to make sure someone already did not do this
            if (null == (fo = getFailoverInfo(connection, primaryServer, instance, database))) {
                if (connection.getConnectionLogger().isLoggable(Level.FINE))
                    connection.getConnectionLogger().fine(connection.toString() + " Failover map add server: "
                            + primaryServer + "; database:" + database + "; Mirror:" + failoverPartner);
                failoverMap.put(concatPrimaryDatabase(primaryServer, instance, database), actualFailoverInfo);
            } else
                // if the class exists make sure the latest info is updated
                fo.failoverAdd(connection, actualuseFailover, failoverPartner);
        } finally {
            LOCK.unlock();
        }
    }
}
