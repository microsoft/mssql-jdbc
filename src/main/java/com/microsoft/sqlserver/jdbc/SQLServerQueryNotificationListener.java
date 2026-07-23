/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.sql.DataSource;


/**
 * Receives SQL Server Query Notification messages from a Service Broker queue and dispatches them to registered
 * listeners.
 * <p>
 * Query Notification subscriptions are one-time subscriptions. Listener registrations remain active, allowing an
 * application to re-execute its query with the same {@link SQLServerQueryNotificationRequest} after each callback.
 */
public final class SQLServerQueryNotificationListener implements AutoCloseable {
    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.QueryNotificationListener");
    private static final String QUERY_NOTIFICATION_MESSAGE_TYPE =
            "http://schemas.microsoft.com/SQL/Notifications/QueryNotification";
    private static final String QUERY_NOTIFICATION_CONTRACT =
            "http://schemas.microsoft.com/SQL/Notifications/PostQueryNotification";
    private static final int RECEIVE_TIMEOUT_MILLISECONDS = 5000;
    private static final int RECONNECT_DELAY_MILLISECONDS = 1000;

    private final DataSource dataSource;
    private final String queueName;
    private final String escapedQueueName;
    private final Executor callbackExecutor;
    private final ExecutorService ownedCallbackExecutor;
    private final Map<String, CopyOnWriteArrayList<ISQLServerQueryNotificationListener>> listeners =
            new ConcurrentHashMap<>();
    private final AtomicBoolean started = new AtomicBoolean();
    private final AtomicBoolean running = new AtomicBoolean();

    private volatile Connection receiveConnection;
    private volatile Thread receiveThread;
    private volatile SQLServerException lastException;

    /**
     * Creates a listener that dispatches callbacks on a driver-owned daemon thread.
     *
     * @param dataSource
     *        data source used to create the dedicated receive connection
     * @param queueName
     *        Service Broker queue containing Query Notification messages
     * @throws SQLServerException
     *         if an argument is invalid
     */
    public SQLServerQueryNotificationListener(DataSource dataSource, String queueName) throws SQLServerException {
        this(dataSource, queueName, null);
    }

    /**
     * Creates a listener that dispatches callbacks through the supplied executor.
     *
     * @param dataSource
     *        data source used to create the dedicated receive connection
     * @param queueName
     *        Service Broker queue containing Query Notification messages
     * @param callbackExecutor
     *        callback executor, or {@code null} to use a driver-owned daemon thread
     * @throws SQLServerException
     *         if an argument is invalid
     */
    public SQLServerQueryNotificationListener(DataSource dataSource, String queueName, Executor callbackExecutor)
            throws SQLServerException {
        if (null == dataSource || null == queueName || queueName.isEmpty()) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerException.getErrString("R_invalidQueryNotificationListenerArgument"), null, false);
        }

        this.dataSource = dataSource;
        this.queueName = queueName;
        escapedQueueName = Util.escapeSQLId(queueName);

        if (null == callbackExecutor) {
            ownedCallbackExecutor = Executors.newSingleThreadExecutor(new DaemonThreadFactory("mssql-jdbc-qn-callback"));
            this.callbackExecutor = ownedCallbackExecutor;
        } else {
            ownedCallbackExecutor = null;
            this.callbackExecutor = callbackExecutor;
        }
    }

    /** Starts the background Service Broker receive loop. A listener instance can be started only once. */
    public synchronized void start() throws SQLServerException {
        if (started.get()) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerException.getErrString("R_queryNotificationListenerAlreadyStarted"), null, false);
        }

        verifyQueue();
        started.set(true);

        running.set(true);
        receiveThread = new DaemonThreadFactory("mssql-jdbc-qn-receive").newThread(this::receiveLoop);
        receiveThread.start();
    }

    private void verifyQueue() throws SQLServerException {
        try (Connection connection = dataSource.getConnection()) {
            if (!objectExists(connection, "SELECT 1 FROM sys.service_queues WHERE name = ?", queueName)) {
                SQLServerException.makeFromDriverError(null, this,
                        SQLServerException.getErrString("R_queryNotificationQueueNotFound"), null, false);
            }
        } catch (SQLServerException e) {
            throw e;
        } catch (SQLException e) {
            throw toSQLServerException(e);
        }
    }

    /**
     * Registers a callback for a notification request's user data.
     *
     * @param userData
     *        the value supplied to {@link SQLServerQueryNotificationRequest#getUserData()}
     * @param listener
     *        callback
     * @throws SQLServerException
     *         if an argument is invalid
     */
    public void register(String userData, ISQLServerQueryNotificationListener listener) throws SQLServerException {
        if (null == userData || userData.isEmpty() || null == listener) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerException.getErrString("R_invalidQueryNotificationListenerArgument"), null, false);
        }
        listeners.computeIfAbsent(userData, key -> new CopyOnWriteArrayList<>()).addIfAbsent(listener);
    }

    /** Removes a callback registration. */
    public void unregister(String userData, ISQLServerQueryNotificationListener listener) {
        List<ISQLServerQueryNotificationListener> registered = listeners.get(userData);
        if (null != registered) {
            registered.remove(listener);
            if (registered.isEmpty()) {
                listeners.remove(userData, registered);
            }
        }
    }

    public boolean isRunning() {
        return running.get();
    }

    public SQLServerException getLastException() {
        return lastException;
    }

    /** Stops the receive loop and releases its dedicated connection and driver-owned executor. */
    public void stop() {
        if (!running.getAndSet(false)) {
            return;
        }

        closeReceiveConnection();
        Thread thread = receiveThread;
        if (null != thread) {
            thread.interrupt();
        }
        if (null != ownedCallbackExecutor) {
            ownedCallbackExecutor.shutdown();
        }
    }

    @Override
    public void close() {
        stop();
    }

    private void receiveLoop() {
        while (running.get()) {
            try (Connection connection = dataSource.getConnection()) {
                receiveConnection = connection;
                receive(connection);
            } catch (SQLException e) {
                if (running.get()) {
                    reportError(toSQLServerException(e));
                    waitBeforeReconnect();
                }
            } finally {
                receiveConnection = null;
            }
        }
    }

    private void receive(Connection connection) throws SQLException {
        String receiveSql = "WAITFOR (RECEIVE TOP (1) conversation_handle, message_type_name, "
                + "CAST(message_body AS nvarchar(max)) AS message_body FROM " + escapedQueueName + "), TIMEOUT "
                + RECEIVE_TIMEOUT_MILLISECONDS;

        while (running.get()) {
            try (Statement statement = connection.createStatement(); ResultSet resultSet = statement.executeQuery(receiveSql)) {
                if (!resultSet.next()) {
                    continue;
                }

                String conversationHandle = resultSet.getString(1);
                String messageType = resultSet.getString(2);
                String payload = resultSet.getString(3);
                endConversation(connection, conversationHandle);

                if (QUERY_NOTIFICATION_MESSAGE_TYPE.equalsIgnoreCase(messageType)) {
                    try {
                        dispatch(SQLServerQueryNotificationParser.parse(payload));
                    } catch (SQLServerException e) {
                        reportError(e);
                    }
                }
            }
        }
    }

    private void endConversation(Connection connection, String conversationHandle) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement("END CONVERSATION ?")) {
            statement.setString(1, conversationHandle);
            statement.executeUpdate();
        }
    }

    void dispatch(SQLServerQueryNotification notification) {
        List<ISQLServerQueryNotificationListener> registered = listeners.get(notification.getUserData());
        if (null == registered) {
            return;
        }

        for (ISQLServerQueryNotificationListener listener : registered) {
            callbackExecutor.execute(() -> {
                try {
                    listener.onNotification(notification);
                } catch (RuntimeException e) {
                    if (logger.isLoggable(Level.WARNING)) {
                        logger.log(Level.WARNING, "Query Notification callback failed.", e);
                    }
                }
            });
        }
    }

    private void reportError(SQLServerException exception) {
        lastException = exception;
        for (List<ISQLServerQueryNotificationListener> registered : listeners.values()) {
            for (ISQLServerQueryNotificationListener listener : registered) {
                callbackExecutor.execute(() -> {
                    try {
                        listener.onError(exception);
                    } catch (RuntimeException e) {
                        if (logger.isLoggable(Level.WARNING)) {
                            logger.log(Level.WARNING, "Query Notification error callback failed.", e);
                        }
                    }
                });
            }
        }
    }

    private void waitBeforeReconnect() {
        try {
            Thread.sleep(RECONNECT_DELAY_MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void closeReceiveConnection() {
        Connection connection = receiveConnection;
        if (null != connection) {
            try {
                connection.close();
            } catch (SQLException e) {
                if (logger.isLoggable(Level.FINER)) {
                    logger.log(Level.FINER, "Error closing Query Notification receive connection.", e);
                }
            }
        }
    }

    private static SQLServerException toSQLServerException(SQLException exception) {
        if (exception instanceof SQLServerException) {
            return (SQLServerException) exception;
        }
        return new SQLServerException(exception.getMessage(), exception.getSQLState(), exception.getErrorCode(),
                exception);
    }

    /** Explicitly creates a queue and service owned by the calling application. */
    public static void createServiceBrokerObjects(DataSource dataSource, String queueName, String serviceName)
            throws SQLException {
        validateProvisioningArguments(dataSource, queueName, serviceName);
        String escapedQueueName = Util.escapeSQLId(queueName);
        String escapedServiceName = Util.escapeSQLId(serviceName);

        try (Connection connection = dataSource.getConnection()) {
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                if (!objectExists(connection, "SELECT 1 FROM sys.service_queues WHERE name = ?", queueName)) {
                    statement.executeUpdate("CREATE QUEUE " + escapedQueueName);
                }
                if (!objectExists(connection, "SELECT 1 FROM sys.services WHERE name = ?", serviceName)) {
                    statement.executeUpdate("CREATE SERVICE " + escapedServiceName + " ON QUEUE " + escapedQueueName
                            + " ([" + QUERY_NOTIFICATION_CONTRACT + "])");
                }
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(originalAutoCommit);
            }
        }
    }

    /** Explicitly drops a service and queue previously created for the application. */
    public static void dropServiceBrokerObjects(DataSource dataSource, String queueName, String serviceName)
            throws SQLException {
        validateProvisioningArguments(dataSource, queueName, serviceName);
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            if (objectExists(connection, "SELECT 1 FROM sys.services WHERE name = ?", serviceName)) {
                statement.executeUpdate("DROP SERVICE " + Util.escapeSQLId(serviceName));
            }
            if (objectExists(connection, "SELECT 1 FROM sys.service_queues WHERE name = ?", queueName)) {
                statement.executeUpdate("DROP QUEUE " + Util.escapeSQLId(queueName));
            }
        }
    }

    private static boolean objectExists(Connection connection, String sql, String name) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, name);
            try (ResultSet resultSet = statement.executeQuery()) {
                return resultSet.next();
            }
        }
    }

    private static void validateProvisioningArguments(DataSource dataSource, String queueName, String serviceName)
            throws SQLException {
        if (null == dataSource || null == queueName || queueName.isEmpty() || null == serviceName
                || serviceName.isEmpty()) {
            throw new SQLException(SQLServerException.getErrString("R_invalidQueryNotificationListenerArgument"));
        }
    }

    private static final class DaemonThreadFactory implements ThreadFactory {
        private final String name;

        private DaemonThreadFactory(String name) {
            this.name = name;
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setName(name);
            thread.setDaemon(true);
            return thread;
        }
    }
}
