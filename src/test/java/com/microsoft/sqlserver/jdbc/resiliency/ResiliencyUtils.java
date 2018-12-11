/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.resiliency;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.Assert;


public final class ResiliencyUtils {

    private static final String[] ON_OFF = new String[] {"ON", "OFF"};
    public static final String alpha = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

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
        REMOTE_PROC_TRANSACTIONS(ON_OFF),
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
                try (Connection c2 = DriverManager.getConnection(cString)) {
                    try (Statement s = c2.createStatement()) {
                        Thread.sleep(sleepTime);
                        s.execute("KILL " + sessionID);
                    }
                }
            } catch (SQLException | InterruptedException e) {
                // handle exception
            }
        }

        public void setProperties(Connection c, String cString) {
            this.c = c;
            this.cString = cString;
        }

        public void setTimer(int millis) {
            this.sleepTime = millis;
        }
    }

    public static void killConnection(Connection c, String cString) throws SQLException {
        int sessionID = 0;
        try (Statement s = c.createStatement()) {
            try (ResultSet rs = s.executeQuery("SELECT @@SPID")) {
                while (rs.next()) {
                    sessionID = rs.getInt(1);
                }
            }
        }
        try (Connection c2 = DriverManager.getConnection(cString)) {
            try (Statement s = c2.createStatement()) {
                s.execute("KILL " + sessionID);
            }
        }
    }

    // uses reflection to "corrupt" a Connection's server target
    public static void blockConnection(Connection c) throws SQLException {
        Field fields[] = c.getClass().getSuperclass().getDeclaredFields();
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

    public static Map<String, String> getUserOptions(Connection c) throws SQLException {
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

    public static void toggleRandomProperties(Connection c) throws SQLException {
        try (Statement stmt = c.createStatement()) {
            for (USER_OPTIONS uo : USER_OPTIONS.values()) {
                stmt.execute("SET " + uo.toString() + " " + uo.getValue());
            }
        }
    }

    public static int getRandomInt(int min, int max) {
        return ThreadLocalRandom.current().nextInt(min, max);
    }

    public static String getRandomString(String pool, int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(String.valueOf(pool.charAt(getRandomInt(0, pool.length()))));
        }
        return sb.toString();
    }
    
    public static String setConnectionProps(String base, Map<String,String> props) {
        StringBuilder sb = new StringBuilder();
        sb.append(base);
        props.forEach((k,v) -> sb.append(k).append("=").append(v).append(";"));
        return sb.toString();
    }
}
