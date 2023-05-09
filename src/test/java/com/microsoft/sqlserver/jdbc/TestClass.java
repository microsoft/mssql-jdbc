package com.microsoft.sqlserver.jdbc;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestClass {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) {
        // Make sure the instance is stopped
        String connectionUrl = "jdbc:sqlserver://localhost;userName=sa;"
                + "password=Moonshine4me;databaseName=TestDb" + ";encrypt=false" + ";trustServerCertificate=true";

        SqlServerConnection connection = new SqlServerConnection(connectionUrl, "Moonshine4me");
        ExecutorService executor = null;
        ScheduledExecutorService scheduledExecutor = null;

        try {
            // A scheduler to print amount of threads created in this jvm
            scheduledExecutor = Executors.newSingleThreadScheduledExecutor(r -> new Thread(r, "ThreadMXBean"));
            scheduledExecutor.scheduleAtFixedRate(() -> log("Amount of threads: %d", ManagementFactory.getThreadMXBean().getThreadCount()), 0, 2, TimeUnit.SECONDS);

            // A second thread for the demo, to simulate multithreaded environment
            executor = Executors.newSingleThreadExecutor(r -> new Thread(r, "DemoThread"));
            executor.submit(() -> {
                safeSleep(5, TimeUnit.SECONDS);
                try {
                    // Second connect
                    connection.connect();
                } catch (SQLException e) {
                    log("Error has occurred during second connect:", e);
                }
            });

            // First connect
            connection.connect();

            safeSleep(5, TimeUnit.SECONDS);
        } catch (Exception e) {
            log("Error has occurred:", e);
        } finally {
            if (executor != null) {
                // Shutdown executor and interrupt the thread.
                executor.shutdownNow();
            }

            try {
                safeSleep(10, TimeUnit.SECONDS);
                connection.disconnect();
            } catch (SQLException e) {
                log("Error has occurred while closing connection:", e);
            }

            safeSleep(10, TimeUnit.SECONDS);

            if (scheduledExecutor != null) {
                scheduledExecutor.shutdownNow();
            }
        }

        log("All Tests Complete. Exiting.");
    }

    private static void safeSleep(int duration, TimeUnit timeUnit) {
        log("Going to sleep for %d %s", duration, timeUnit.name());
        try { Thread.sleep(timeUnit.toMillis(duration)); } catch (InterruptedException ignore) {}
    }

    private static void log(String message, Object... args) {
        log(message, null, args);
    }

    private synchronized static void log(String message, Throwable thrown, Object... args) {
        String msg = message;

        if (args != null) {
            msg = String.format(msg, args);
        }

        if (thrown != null) {
            msg += " " + thrown;
        }

        System.out.printf("%s {%s}: %s%n", dateFormat.format(new Date()), Thread.currentThread().getName(), msg);

        if (thrown != null) {
            System.out.println(getStackTrace(thrown));
        }
    }

    private static String getStackTrace(Throwable thrown) {
        String result = null;
        try (StringWriter sw = new StringWriter(); PrintWriter pw = new PrintWriter(sw)) {
            thrown.printStackTrace(pw);
            result = sw.toString();
        } catch (Exception ignore) { }

        return result;
    }

    private static class SqlServerConnection {
        private final String connectionUrl;
        private final String pwd;
        private Connection connection;

        SqlServerConnection(String connectionUrl, String pwd) {
            this.connectionUrl = connectionUrl;
            this.pwd = pwd;
        }

        public void connect() throws SQLException {
            log("Connecting to SQL Server...");
            if (connection != null) {
                log("Connection is already existing. Disconnecting existing connection...");
                disconnect();
            }
            Properties props = new Properties();
            props.put("password", pwd);

            // Using this, along with synchronization over 'this' instance can overcome the issue,
            // but it will not allow two threads to connect simultaneously...
            //safeSleep(250, TimeUnit.MILLISECONDS);

            connection = DriverManager.getConnection(connectionUrl, props);
            log("Connected.");
        }

        public void disconnect() throws SQLException {
            if (connection != null) {
                log("Disconnecting from SQL Server...");
                connection.close();
                log("Disconnected.");
            }
        }
    }
}
