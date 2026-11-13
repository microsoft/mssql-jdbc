# SQL Server Query Notifications

The JDBC driver can attach a SQL Server Query Notification request to a statement and run a background listener that
dispatches Service Broker messages to application callbacks. Query Notifications are one-time subscriptions: after SQL
Server sends a notification, execute the query again to create another subscription. Callback registrations remain
active until explicitly removed.

The database must have Service Broker enabled. Applications can create their own queue and service using SQL, or call
the explicit driver helper during deployment:

```java
SQLServerQueryNotificationListener.createServiceBrokerObjects(
        dataSource, "ProductNotificationQueue", "ProductNotificationService");
```

The helper is never called implicitly. The equivalent SQL using the predefined Query Notification contract is:

```sql
CREATE QUEUE ProductNotificationQueue;

CREATE SERVICE ProductNotificationService
ON QUEUE ProductNotificationQueue
([http://schemas.microsoft.com/SQL/Notifications/PostQueryNotification]);
```

The login registering the query also requires `SUBSCRIBE QUERY NOTIFICATIONS` permission. The login consuming the
messages requires `RECEIVE` permission on the queue.

Start the listener and register the application's correlation key before executing the query:

```java
SQLServerQueryNotificationListener listener =
        new SQLServerQueryNotificationListener(dataSource, "ProductNotificationQueue");

listener.register("products-cache", new ISQLServerQueryNotificationListener() {
    @Override
    public void onNotification(SQLServerQueryNotification notification) {
        System.out.println(notification.getType() + ":" + notification.getSource() + ":" + notification.getInfo());
        // Refresh the cache and execute the monitored query again to renew the one-time subscription.
    }

    @Override
    public void onError(SQLServerException exception) {
        exception.printStackTrace();
    }
});
listener.start();
```

Attach a request to a statement before executing a supported `SELECT` query:

```java
SQLServerQueryNotificationRequest request = new SQLServerQueryNotificationRequest(
        "products-cache",
        "service=ProductNotificationService;local database=Products",
        300);

try (SQLServerPreparedStatement statement = (SQLServerPreparedStatement) connection.prepareStatement(
        "SELECT ProductID, Name FROM dbo.Products")) {
    statement.setQueryNotificationRequest(request);

    try (ResultSet resultSet = statement.executeQuery()) {
        // Populate the cache from resultSet.
    }
}
```

The listener uses a dedicated connection to perform the following Service Broker receive operation and ends every
received conversation:

```sql
WAITFOR (
    RECEIVE TOP (1)
        conversation_handle,
        message_type_name,
        message_body
    FROM ProductNotificationQueue
), TIMEOUT 30000;
```

Close the listener during application shutdown. If the application explicitly owns the Broker objects, it can also
remove them during undeployment:

```java
listener.close();
SQLServerQueryNotificationListener.dropServiceBrokerObjects(
        dataSource, "ProductNotificationQueue", "ProductNotificationService");
```

SQL Server can enqueue an immediate notification when a query does not meet the restrictions for Query Notifications.
Use schema-qualified two-part table names and ensure the required SQL Server `SET` options are enabled. Consult the SQL
Server Query Notifications documentation for the complete query restrictions.
