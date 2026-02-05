/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.resiliency;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import javax.sql.PooledConnection;

import org.junit.Assert;
import org.junit.jupiter.api.Tag;

import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


@Tag(Constants.xSQLv11)
@Tag(Constants.xAzureSQLDW)
final class ResiliencyUtils {

    private static final String[] ON_OFF = new String[] {"ON", "OFF"};
    static final String alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    static final int checkRecoveryAliveInterval = 500;

    private ResiliencyUtils() {};

    enum USER_OPTIONS {
        ANSI_DEFAULTS(ON_OFF),
        ANSI_NULL_DFLT_OFF(ON_OFF),
        ANSI_NULLS(ON_OFF),
        ANSI_PADDING(ON_OFF),
        ANSI_WARNINGS(ON_OFF),
        ARITHABORT(ON_OFF),
        CONCAT_NULL_YIELDS_NULL(ON_OFF),
        // CONTEXT_INFO,
        CURSOR_CLOSE_ON_COMMIT(ON_OFF),
        DATEFIRST(new String[] {"1", "2", "3", "4", "5", "6", "7"}),
        DATEFORMAT(new String[] {"dmy", "dym", "mdy", "myd", "ydm", "ymd"}),
        DEADLOCK_PRIORITY(new String[] {"LOW", "NORMAL", "HIGH", "-10", "-9", "-8", "-7", "-6", "-5", "-4", "-3", "-2",
                "-1", "0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}),
        // FIPS_FLAGGER(new String[] {"ENTRY", "FULL", "INTERMEDIATE", "OFF"}), not picked up by dbcc useroptions
        // FMTONLY fails queries
        FORCEPLAN(ON_OFF),
        // IDENTITY_INSERT(ON_OFF),
        IMPLICIT_TRANSACTIONS(ON_OFF),
        LANGUAGE(new String[] {"English", "German", "French", "Japanese", "Danish", "Spanish", "Italian", "Dutch",
                "Norwegian", "Portuguese", "Finnish", "Swedish", "Czech", "Hungarian", "Polish", "Romanian", "Croatian",
                "Slovak", "Slovenian", "Greek", "Bulgarian", "Russian", "Turkish", "British English", "Estonian",
                "Latvian", "Lithuanian", "Brazilian", "Traditional Chinese", "Korean", "Simplified Chinese", "Arabic",
                "Thai"}) {
            @Override
            String getValue() {
                return "N'" + value + "'";
            }
        },
        LOCK_TIMEOUT(null) {
            @Override
            void init() {
                this.value = String.valueOf(getRandomInt(0, 8000));
            }
        },
        NOCOUNT(ON_OFF),
        // NOEXEC fails queries
        NUMERIC_ROUNDABORT(ON_OFF),
        // PARSEONLY fails queries
        QUERY_GOVERNOR_COST_LIMIT(null) {
            @Override
            void init() {
                this.value = String.valueOf(getRandomInt(0, 8000));
            }
        },
        QUOTED_IDENTIFIER(ON_OFF),
        ROWCOUNT(null) {
            @Override
            void init() {
                this.value = String.valueOf(getRandomInt(0, 8000));
            }
        },
        // SHOWPLAN_ALL/SHOWPLAN_TEXT/SHOWPLAN_XML fails queries
        STATISTICS_IO(ON_OFF) {
            @Override
            public String toString() {
                return "STATISTICS IO";
            }
        },
        STATISTICS_PROFILE(ON_OFF) {
            @Override
            public String toString() {
                return "STATISTICS PROFILE";
            }
        },
        STATISTICS_TIME(ON_OFF) {
            @Override
            public String toString() {
                return "STATISTICS TIME";
            }
        },
        STATISTICS_XML(ON_OFF) {
            @Override
            public String toString() {
                return "STATISTICS XML";
            }
        },
        TEXTSIZE(null) {
            @Override
            void init() {
                this.value = String.valueOf(getRandomInt(0, 8000));
            }
        },
        TRANSACTION_ISOLATION_LEVEL(new String[] {"READ UNCOMMITTED", "READ COMMITTED", "REPEATABLE READ", "SNAPSHOT",
                "SERIALIZABLE"}) {
            @Override
            public String toString() {
                return "TRANSACTION ISOLATION LEVEL";
            }
        },
        XACT_ABORT(ON_OFF);

        private String[] possibleOptions;
        String value;

        private USER_OPTIONS(String[] s) {
            this.possibleOptions = s;
            init();
        }

        void init() {
            this.value = possibleOptions[getRandomInt(0, possibleOptions.length)];
        }

        String getValue() {
            return value;
        }
    }

    class killConnectionThread implements Runnable {

        Connection c;
        String cString;
        int sleepTime = 0;

        public void run() {
            try {
                int sessionID = 0;
                try (Statement s = c.createStatement()) {
                    try (ResultSet rs = s.executeQuery("SELECT @@SPID")) {
                        while (rs.next()) {
                            sessionID = rs.getInt(1);
                        }
                    }
                }
                try (Connection c2 = PrepUtil.getConnection(cString)) {
                    try (Statement s = c2.createStatement()) {
                        Thread.sleep(sleepTime);
                        s.execute("KILL " + sessionID);
                    }
                }
            } catch (SQLException | InterruptedException e) {
                // handle exception
            }
        }

        protected void setProperties(Connection c, String cString) {
            this.c = c;
            this.cString = cString;
        }

        public void setTimer(int millis) {
            this.sleepTime = millis;
        }
    }

    static Connection getPooledConnection(String connectionString) throws SQLException {
        SQLServerConnectionPoolDataSource mds = new SQLServerConnectionPoolDataSource();
        mds.setURL(connectionString);
        PooledConnection pooledConnection = mds.getPooledConnection();
        Connection c = pooledConnection.getConnection();

        minimizeIdleNetworkTracker(c);
        return c;
    }

    static Connection getConnection(String connectionString) throws SQLException {
        Connection c = PrepUtil.getConnection(connectionString);
        minimizeIdleNetworkTracker(c);
        return c;
    }

    static void minimizeIdleNetworkTracker(Connection c) {
        try {
            Connection conn = c;

            // See if we were handed a pooled connection
            for (Field f : c.getClass().getDeclaredFields()) {
                if (f.getName() == "wrappedConnection") {
                    f.setAccessible(true);
                    conn = (Connection) f.get(c);
                    break;
                }
            }

            boolean methodInvoked = false;
            for (Field f : getConnectionFields(conn)) {
                if (f.getName() == "idleNetworkTracker") {
                    f.setAccessible(true);
                    Object idleNetworkTracker = f.get(conn);
                    Method method = idleNetworkTracker.getClass().getDeclaredMethod("setMaxIdleMillis", int.class);
                    method.setAccessible(true);
                    method.invoke(idleNetworkTracker, -1);
                    methodInvoked = true;
                    break;
                }
            }
            if (!methodInvoked) {
                throw new Exception("Failed to find setMaxIdleMillis via reflection to adjust the internal idle time.");
            }
        } catch (Exception e) {
            Assert.fail("Failed to setMaxIdleMillis in Connection's idleNetworkTracker: " + e.getMessage());
        }
    }

    /**
     * Since we are using reflection to the IdleNetworkTracker to immediately deem a connection idle, in certain
     * scenarios (ie testing against Azure SQL DB), it takes too long for the connection to drop from the server side
     * after sending a kill connection command. This method checks to make sure the recovery thread exists and the
     * connection has died before proceeding.
     * 
     * @param c
     *        Connection
     * @return True if recovery thread was started and connection is dead, false if timeout
     */
    static boolean isRecoveryAliveAndConnDead(Connection c) {
        Connection conn = c;
        // See if we were handed a pooled connection
        for (Field f : getConnectionFields(c)) {
            if (f.getName() == "wrappedConnection") {
                f.setAccessible(true);
                try {
                    conn = (Connection) f.get(c);
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    e.printStackTrace();
                }
                break;
            }
        }

        int waits = 0;
        try {
            while (!TestUtils.isConnectionRecoveryNegotiated(conn)) {
                TimeUnit.MILLISECONDS.sleep(ResiliencyUtils.checkRecoveryAliveInterval);
                if (waits++ > 5)
                    return false;
            }
            while (!TestUtils.isConnectionDead(conn)) {
                TimeUnit.MILLISECONDS.sleep(ResiliencyUtils.checkRecoveryAliveInterval);
                if (waits++ > 5)
                    return false;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    static int getSessionId(Connection c) throws SQLException {
        int sessionID = 0;
        try (Statement s = c.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT @@SPID")) {
                while (rs.next()) {
                    sessionID = rs.getInt(1);
                }
            }
        }
        return sessionID;
    }

    static void killConnection(Connection c, String cString, int delayTimeout) throws SQLException {
        killConnection(getSessionId(c), cString, c, delayTimeout);
    }

    /**
     * Running a query on a connection can affect it adversely, this method should not run a query on the connection
     * being passed.
     * 
     * @param sessionID
     *        session id
     * @param cString
     *        connection string
     * @param c
     *        connection
     * @param delayTimeout
     *        delay timeout in seconds
     * @throws SQLException
     */
    static void killConnection(int sessionID, String cString, Connection c, int delayTimeout) throws SQLException {
        try (Connection c2 = PrepUtil.getConnection(cString)) {
            try (Statement s = c2.createStatement()) {
                if (TestUtils.isAzureDW(c2)) {// AzureSQLDW and Synapse uses different syntax
                    if (delayTimeout > 0) {
                        s.execute("KILL '" + sessionID + "'" + "WAITFOR DELAY " + "'00:00:" + delayTimeout + "'");
                    } else {
                        s.execute("KILL '" + sessionID + "'");
                    }
                } else {
                    if (delayTimeout > 0) {
                        s.execute("KILL " + sessionID + "WAITFOR DELAY " + "'00:00:" + delayTimeout + "'");
                    } else {
                        s.execute("KILL " + sessionID);
                    }
                }
            }
        }
        isRecoveryAliveAndConnDead(c);
    }

    // uses reflection to "corrupt" a Connection's server target
    static void blockConnection(Connection c) throws SQLException {
        Field fields[] = getConnectionFields(c);
        for (Field f : fields) {
            if (f.getName() == "activeConnectionProperties" && Properties.class == f.getType()) {
                f.setAccessible(true);
                Properties connectionProps;
                try {
                    connectionProps = (Properties) f.get(c);
                    connectionProps.setProperty("serverName", "bogusServerName");
                    f.set(c, connectionProps);
                    return;
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    Assert.fail("Failed to block connection: " + e.getMessage());
                }
            }
        }
        Assert.fail("Failed to block connection.");
    }

    static Map<String, String> getUserOptions(Connection c) throws SQLException {
        Map<String, String> options = new HashMap<>();
        try (Statement stmt = c.createStatement()) {
            try (ResultSet rs = stmt.executeQuery("DBCC USEROPTIONS")) {
                while (rs.next()) {
                    String key = rs.getString(1);
                    String value = rs.getObject(2).toString();
                    options.put(key, value);
                }
            }
        }
        return options;
    }

    static void toggleRandomProperties(Connection c) throws SQLException {
        try (Statement stmt = c.createStatement()) {
            for (USER_OPTIONS uo : USER_OPTIONS.values()) {
                stmt.execute("SET " + uo.toString() + " " + uo.getValue());
            }
        }
    }

    static int getRandomInt(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max);
    }

    static String getRandomString(String pool, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(String.valueOf(pool.charAt(getRandomInt(0, pool.length()))));
        }
        return sb.toString();
    }

    static String setConnectionProps(String base, Map<String, String> props) {
        StringBuilder sb = new StringBuilder();
        sb.append(base);
        props.forEach((k, v) -> sb.append(k).append("=").append(v).append(";"));
        return sb.toString();
    }

    /**
     * Get declared fields of connection class depending on Java version. Connection class SQLServerConnection43 is
     * returned for Java >=9 and SQLServerConnection or SQLServerConnectPoolProxy for Java 8
     * 
     * @param c
     *        connection class that implements ISQLServerConnection
     * @return declared fields for Connection class
     */
    static Field[] getConnectionFields(Connection c) {
        Class<? extends Connection> cls = c.getClass();
        // SQLServerConnection43 is returned for Java >=9 so need to get super class
        if (cls.getName() == "com.microsoft.sqlserver.jdbc.SQLServerConnection43") {
            return cls.getSuperclass().getDeclaredFields();
        }

        return cls.getDeclaredFields();
    }
}
