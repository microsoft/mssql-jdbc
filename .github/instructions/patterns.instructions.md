# Common Code Patterns in mssql-jdbc

This document provides code patterns and examples for common development tasks in the mssql-jdbc driver.

## Table of Contents
- [Exception Handling](#exception-handling)
- [Logging](#logging)
- [Resource Management](#resource-management)
- [Null Validation](#null-validation)
- [Connection Properties](#connection-properties)
- [TDS Protocol](#tds-protocol)
- [Data Type Conversion](#data-type-conversion)
- [Testing Patterns](#testing-patterns)
- [Bulk Copy](#bulk-copy)
- [Always Encrypted](#always-encrypted)

---

## Exception Handling

### Creating Driver Errors

```java
// Standard pattern for driver-generated errors
SQLServerException.makeFromDriverError(
    con,                                                    // SQLServerConnection (null if not available)
    this,                                                   // Source object (usually 'this')
    SQLServerException.getErrString("R_errorResourceKey"),  // Localized error message
    "HY000",                                                // SQL State (null for default)
    false                                                   // true if this is a driver error code
);

// Example: Null parameter validation
if (null == tableName) {
    SQLServerException.makeFromDriverError(con, this,
        SQLServerException.getErrString("R_tableNameNull"), null, false);
}

// Example: Invalid argument
if (timeout < 0) {
    SQLServerException.makeFromDriverError(con, this,
        SQLServerException.getErrString("R_invalidQueryTimeout"), null, false);
}
```

### Wrapping Exceptions

```java
// Wrapping a caught exception as SQLServerException
try {
    // Some operation
} catch (IOException e) {
    SQLServerException.makeFromDriverError(con, this,
        e.getMessage(), SQLServerException.getErrString("R_connectionClosed"), false);
}

// With cause preservation
try {
    // Some operation
} catch (Exception e) {
    throw new SQLServerException(
        SQLServerException.getErrString("R_operationFailed"),
        SQLState.STMT_CLOSED.getSQLStateCode(),
        DriverError.NOT_SET,
        e  // Preserve the cause
    );
}
```

### Adding Error Resources

Error messages are defined in `SQLServerResource.java`:

```java
// In SQLServerResource.java (resource bundle)
{"R_myNewError", "Description of the error with {0} placeholder for parameters."},

// Usage
String msg = MessageFormat.format(
    SQLServerException.getErrString("R_myNewError"), 
    paramValue
);
```

---

## Logging

### Logger Setup

```java
// Standard logger declaration in a class
private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.MyClass");

// For connection-specific logging
private static final Logger connectionLogger = Logger.getLogger("com.microsoft.sqlserver.jdbc.Connection");

// For statement logging
private static final Logger stmtLogger = Logger.getLogger("com.microsoft.sqlserver.jdbc.Statement");
```

### Logging Pattern

```java
// Always check log level before constructing log messages
if (logger.isLoggable(Level.FINER)) {
    logger.finer(toString() + " methodName: param1=" + param1 + " param2=" + param2);
}

// For expensive string operations
if (logger.isLoggable(Level.FINEST)) {
    logger.finest(toString() + " Full SQL: " + buildDebugSqlString());
}

// Entry/Exit logging for public methods
public void myPublicMethod(String param) {
    if (logger.isLoggable(Level.FINER)) {
        logger.entering(getClass().getName(), "myPublicMethod", param);
    }
    
    try {
        // Method implementation
    } finally {
        if (logger.isLoggable(Level.FINER)) {
            logger.exiting(getClass().getName(), "myPublicMethod");
        }
    }
}

// Warning level for recoverable issues
if (logger.isLoggable(Level.WARNING)) {
    logger.warning("Connection retry attempt " + retryCount + " failed");
}
```

### Sensitive Data

```java
// NEVER log sensitive data
// BAD - Don't do this!
logger.fine("Connecting with password: " + password);

// GOOD - Log only safe information
logger.fine("Connecting to server: " + serverName + " as user: " + userName);
```

---

## Resource Management

### Try-With-Resources

```java
// Always use try-with-resources for JDBC objects
public void executeQuery(String sql) throws SQLException {
    try (Connection con = dataSource.getConnection();
         Statement stmt = con.createStatement();
         ResultSet rs = stmt.executeQuery(sql)) {
        
        while (rs.next()) {
            processRow(rs);
        }
    }
    // Resources automatically closed in reverse order
}

// Nested resources
try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
    try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) 
            con.prepareStatement(sql)) {
        pstmt.setString(1, param);
        try (SQLServerResultSet rs = (SQLServerResultSet) pstmt.executeQuery()) {
            // Process results
        }
    }
}
```

### Manual Resource Cleanup

```java
// When try-with-resources isn't suitable
private Statement cachedStatement;

public void cleanup() {
    if (cachedStatement != null) {
        try {
            cachedStatement.close();
        } catch (SQLException e) {
            // Log but don't throw - we're cleaning up
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Error closing statement: " + e.getMessage());
            }
        } finally {
            cachedStatement = null;
        }
    }
}
```

---

## Null Validation

### Parameter Validation

```java
// Validate at method entry
public void setTableName(String tableName) throws SQLServerException {
    if (null == tableName) {
        SQLServerException.makeFromDriverError(null, this,
            SQLServerException.getErrString("R_tableNameNull"), null, false);
    }
    if (tableName.isEmpty()) {
        SQLServerException.makeFromDriverError(null, this,
            SQLServerException.getErrString("R_tableNameEmpty"), null, false);
    }
    this.tableName = tableName;
}

// Using StringUtils
public void setServerName(String serverName) throws SQLServerException {
    if (StringUtils.isNullOrEmpty(serverName)) {
        SQLServerException.makeFromDriverError(null, this,
            SQLServerException.getErrString("R_serverNameNull"), null, false);
    }
    this.serverName = serverName;
}
```

### Defensive Null Checks

```java
// Safe string comparison (null on left)
if (null == inputString || inputString.isEmpty()) {
    return defaultValue;
}

// Safe equals comparison
if ("expectedValue".equals(inputString)) {
    // Process
}

// Array null check
if (null != array && array.length > 0) {
    for (Object item : array) {
        if (null != item) {
            process(item);
        }
    }
}
```

---

## Connection Properties

### Adding a New Boolean Property

```java
// 1. In SQLServerDriverBooleanProperty enum
public enum SQLServerDriverBooleanProperty {
    // ... existing properties ...
    MY_NEW_FEATURE("myNewFeature", false, false, false);  // name, default, required, advanced
    
    // Constructor and methods...
}

// 2. In SQLServerDataSource - add getter/setter
public void setMyNewFeature(boolean value) {
    setBooleanProperty(connectionProps, 
        SQLServerDriverBooleanProperty.MY_NEW_FEATURE.toString(), value);
}

public boolean getMyNewFeature() {
    return getBooleanProperty(connectionProps,
        SQLServerDriverBooleanProperty.MY_NEW_FEATURE.toString(),
        SQLServerDriverBooleanProperty.MY_NEW_FEATURE.getDefaultValue());
}

// 3. In SQLServerConnection - use the property
if (activeConnectionProperties.getBooleanProperty(
        SQLServerDriverBooleanProperty.MY_NEW_FEATURE.toString(), false)) {
    // Feature-specific behavior
}
```

### Adding a New String Property

```java
// 1. In SQLServerDriverStringProperty enum
public enum SQLServerDriverStringProperty {
    // ... existing properties ...
    MY_STRING_PROP("myStringProp", "defaultValue", false);  // name, default, required
}

// 2. In SQLServerDataSource
public void setMyStringProp(String value) {
    setStringProperty(connectionProps,
        SQLServerDriverStringProperty.MY_STRING_PROP.toString(), value);
}

public String getMyStringProp() {
    return getStringProperty(connectionProps,
        SQLServerDriverStringProperty.MY_STRING_PROP.toString(),
        SQLServerDriverStringProperty.MY_STRING_PROP.getDefaultValue());
}
```

---

## TDS Protocol

### Reading TDS Tokens

```java
// Pattern for reading a TDS token
void readToken(TDSReader tdsReader) throws SQLServerException {
    // Read token header
    int tokenType = tdsReader.readUnsignedByte();
    int tokenLength = tdsReader.readUnsignedShort();
    
    // Read token data based on type
    switch (tokenType) {
        case TDS.TDS_COLMETADATA:
            readColumnMetadata(tdsReader);
            break;
        case TDS.TDS_ROW:
            readRow(tdsReader);
            break;
        case TDS.TDS_DONE:
            readDone(tdsReader);
            break;
        default:
            // Skip unknown tokens
            tdsReader.skip(tokenLength);
    }
}
```

### Writing TDS Data

```java
// Pattern for writing data to TDS stream
void writeValue(TDSWriter tdsWriter, Object value, int jdbcType) throws SQLServerException {
    if (null == value) {
        // Write NULL indicator
        tdsWriter.writeByte((byte) 0x00);
        return;
    }
    
    switch (jdbcType) {
        case java.sql.Types.INTEGER:
            tdsWriter.writeByte((byte) 0x04);  // length
            tdsWriter.writeInt((Integer) value);
            break;
        case java.sql.Types.VARCHAR:
            byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
            tdsWriter.writeShort((short) bytes.length);
            tdsWriter.writeBytes(bytes);
            break;
        // ... other types
    }
}
```

---

## Data Type Conversion

### Using DDC (Data Conversion)

```java
// Convert from Java to SQL Server type
Object convertedValue = DDC.convertForward(
    javaValue,           // Source Java value
    jdbcType,            // Target JDBC type
    streamType,          // Stream type for LOBs
    connection           // Connection for settings
);

// Convert from SQL Server to Java type
Object javaValue = DDC.convert(
    tdsValue,            // Value from TDS
    jdbcType,            // Source JDBC type
    targetClass,         // Target Java class
    streamType,          // Stream type
    connection           // Connection
);
```

### Type Checking

```java
// Check type categories
if (Util.isCharType(jdbcType)) {
    // Handle character types (CHAR, VARCHAR, NCHAR, NVARCHAR, etc.)
}

if (Util.isBinaryType(jdbcType)) {
    // Handle binary types (BINARY, VARBINARY, IMAGE, etc.)
}

// Check for specific types
switch (jdbcType) {
    case java.sql.Types.TIMESTAMP:
    case microsoft.sql.Types.DATETIME:
    case microsoft.sql.Types.SMALLDATETIME:
        // Handle datetime types
        break;
}
```

---

## Testing Patterns

### Basic Test Structure

```java
@Tag(Constants.xSQLv12)  // Required SQL Server version
@Test
public void testFeatureName() throws SQLException {
    // Arrange
    String tableName = AbstractSQLGenerator.escapeIdentifier(
        RandomUtil.getIdentifier("testTable"));
    String sql = "CREATE TABLE " + tableName + " (id INT, name NVARCHAR(100))";
    
    try (Connection con = getConnection();
         Statement stmt = con.createStatement()) {
        
        // Setup
        stmt.execute(sql);
        
        try {
            // Act
            stmt.execute("INSERT INTO " + tableName + " VALUES (1, N'Test')");
            
            // Assert
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                assertTrue(rs.next(), "Expected one row");
                assertEquals(1, rs.getInt("id"));
                assertEquals("Test", rs.getString("name"));
            }
        } finally {
            // Cleanup
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }
}
```

### Parameterized Tests

```java
@ParameterizedTest
@MethodSource("dataProvider")
@Tag(Constants.xSQLv12)
public void testWithParameters(String input, String expected) throws SQLException {
    try (Connection con = getConnection();
         PreparedStatement pstmt = con.prepareStatement("SELECT ?")) {
        pstmt.setString(1, input);
        try (ResultSet rs = pstmt.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(expected, rs.getString(1));
        }
    }
}

static Stream<Arguments> dataProvider() {
    return Stream.of(
        Arguments.of("test", "test"),
        Arguments.of("", ""),
        Arguments.of("unicode: 日本語", "unicode: 日本語")
    );
}
```

### Testing with Expected Exceptions

```java
@Test
public void testInvalidInputThrowsException() {
    assertThrows(SQLServerException.class, () -> {
        try (Connection con = getConnection();
             Statement stmt = con.createStatement()) {
            stmt.execute("INVALID SQL SYNTAX HERE");
        }
    });
}

@Test
public void testSpecificErrorMessage() {
    SQLServerException ex = assertThrows(SQLServerException.class, () -> {
        // Code that should throw
    });
    assertTrue(ex.getMessage().contains("expected error text"));
}
```

---

## Bulk Copy

### Basic Bulk Copy Pattern

```java
public void bulkCopyData(Connection sourceConn, Connection destConn, 
        String sourceTable, String destTable) throws SQLException {
    
    try (Statement stmt = sourceConn.createStatement();
         ResultSet rs = stmt.executeQuery("SELECT * FROM " + sourceTable);
         SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(destConn)) {
        
        // Configure options
        SQLServerBulkCopyOptions options = new SQLServerBulkCopyOptions();
        options.setBatchSize(10000);
        options.setBulkCopyTimeout(60);
        options.setKeepIdentity(true);
        bulkCopy.setBulkCopyOptions(options);
        
        // Set destination
        bulkCopy.setDestinationTableName(destTable);
        
        // Execute
        bulkCopy.writeToServer(rs);
    }
}
```

### Custom Bulk Record Implementation

```java
public class MyCustomBulkRecord implements ISQLServerBulkRecord {
    private List<Object[]> data;
    private int currentRow = -1;
    private Map<Integer, ColumnMetadata> columnMetadata;
    
    @Override
    public Set<Integer> getColumnOrdinals() {
        return columnMetadata.keySet();
    }
    
    @Override
    public String getColumnName(int column) {
        return columnMetadata.get(column).name;
    }
    
    @Override
    public int getColumnType(int column) {
        return columnMetadata.get(column).jdbcType;
    }
    
    @Override
    public boolean next() {
        currentRow++;
        return currentRow < data.size();
    }
    
    @Override
    public Object[] getRowData() {
        return data.get(currentRow);
    }
    
    // ... implement other required methods
}
```

---

## Always Encrypted

### Registering Custom Key Store Provider

```java
// Create and register provider
Map<String, SQLServerColumnEncryptionKeyStoreProvider> providers = new HashMap<>();
providers.put("MY_CUSTOM_KEYSTORE", new MyCustomKeyStoreProvider());

SQLServerConnection.registerColumnEncryptionKeyStoreProviders(providers);

// Or register per-connection
try (SQLServerConnection con = (SQLServerConnection) getConnection()) {
    con.registerColumnEncryptionKeyStoreProvidersOnConnection(providers);
}
```

### Custom Key Store Provider Implementation

```java
public class MyCustomKeyStoreProvider extends SQLServerColumnEncryptionKeyStoreProvider {
    
    private String name = "MY_CUSTOM_KEYSTORE";
    
    @Override
    public String getName() {
        return name;
    }
    
    @Override
    public byte[] decryptColumnEncryptionKey(String masterKeyPath, 
            String encryptionAlgorithm, byte[] encryptedColumnEncryptionKey) 
            throws SQLServerException {
        
        // Validate algorithm
        if (!"RSA_OAEP".equalsIgnoreCase(encryptionAlgorithm)) {
            throw new SQLServerException("Unsupported algorithm: " + encryptionAlgorithm);
        }
        
        // Decrypt the key using your custom logic
        return customDecrypt(masterKeyPath, encryptedColumnEncryptionKey);
    }
    
    @Override
    public byte[] encryptColumnEncryptionKey(String masterKeyPath,
            String encryptionAlgorithm, byte[] columnEncryptionKey)
            throws SQLServerException {
        
        // Encrypt the key using your custom logic
        return customEncrypt(masterKeyPath, columnEncryptionKey);
    }
    
    // Implement custom encryption/decryption methods
    private byte[] customDecrypt(String keyPath, byte[] data) { /* ... */ }
    private byte[] customEncrypt(String keyPath, byte[] data) { /* ... */ }
}
```

---

## Anti-Patterns (Avoid These)

### 1. SQL Injection via String Concatenation

```java
// BAD — SQL injection risk
String sql = "SELECT * FROM " + userInput;
stmt.execute(sql);

// GOOD — Use parameterized queries
PreparedStatement pstmt = con.prepareStatement("SELECT * FROM users WHERE id = ?");
pstmt.setInt(1, userId);
```

Note: Calling `executeQuery(String)` on a `PreparedStatement` throws `R_cannotTakeArgumentsPreparedOrCallable`.

### 2. Resource Leaks (Missing try-with-resources)

```java
// BAD — Resources leak if exception occurs between open and close
Connection con = ds.getConnection();
Statement stmt = con.createStatement();
ResultSet rs = stmt.executeQuery(sql);
// ... use rs ...
rs.close(); stmt.close(); con.close();

// GOOD — Auto-close on exception or normal exit
try (Connection con = ds.getConnection();
     Statement stmt = con.createStatement();
     ResultSet rs = stmt.executeQuery(sql)) {
    while (rs.next()) { /* ... */ }
}

// GOOD — SQLServerBulkCopy also implements AutoCloseable
try (SQLServerBulkCopy bc = new SQLServerBulkCopy(con)) {
    bc.setDestinationTableName("target");
    bc.writeToServer(rs);
}
```

### 3. Properties-Only Properties in Connection String

`accessToken`, `accessTokenCallback`, and `gsscredential` **cannot** be set in the connection URL — they must be passed via `java.util.Properties`.

```java
// BAD — accessToken silently ignored in connection string
String url = "jdbc:sqlserver://server;accessToken=eyJ...";

// GOOD — Pass via Properties
Properties props = new Properties();
props.put("accessToken", tokenString);
Connection con = DriverManager.getConnection(url, props);
```

### 4. Sharing Connections Across Threads

`SQLServerConnection`, `SQLServerStatement`, and `SQLServerResultSet` are **NOT** thread-safe.

```java
// BAD — Race condition on shared connection
Connection sharedCon = ds.getConnection();
pool.submit(() -> sharedCon.createStatement().execute(sql1));
pool.submit(() -> sharedCon.createStatement().execute(sql2));

// GOOD — Connection-per-thread or use connection pool
pool.submit(() -> {
    try (Connection con = ds.getConnection()) {
        con.createStatement().execute(sql1);
    }
});
```

Use `SQLServerConnectionPoolDataSource` or `SQLServerXADataSource` for pooling.

### 5. Catching Exception Instead of SQLException

```java
// BAD — Catches NPE, ClassCastException, etc.
try {
    stmt.execute(sql);
} catch (Exception e) {
    logger.warning(e.getMessage());
}

// GOOD — Catch specific exception type
try {
    stmt.execute(sql);
} catch (SQLException e) {
    SQLServerException.makeFromDriverError(con, this, e.getMessage(), null, false);
}
```

### 6. Using isClosed() to Check Connection Validity

`isClosed()` only checks a **local flag** — it does NOT verify the server is reachable.

```java
// BAD — isClosed() doesn't detect broken network connections
if (!con.isClosed()) {
    stmt.execute(sql);  // May still fail if server is unreachable
}

// GOOD — isValid() sends SELECT 1 to the server
if (con.isValid(5)) {  // 5-second timeout
    stmt.execute(sql);
}
```

### 7. Statement Pooling Misconfiguration

Pooling requires **both** `disableStatementPooling=false` AND `statementPoolingCacheSize > 0`. Defaults disable it.

```java
// BAD — Only set one property, pooling still disabled
props.put("statementPoolingCacheSize", "10");
// disableStatementPooling defaults to true → pooling is OFF

// GOOD — Set both
props.put("disableStatementPooling", "false");
props.put("statementPoolingCacheSize", "10");
```

### 8. sendStringParametersAsUnicode Performance Impact

Defaults to `true`. All string params are sent as `nvarchar`, which prevents index seeks on `varchar` columns due to implicit conversion.

```java
// BAD — Default causes nvarchar parameters on varchar columns
// SQL Server sees: WHERE varchar_col = N'value' → index scan, not seek
PreparedStatement pstmt = con.prepareStatement("SELECT * FROM t WHERE varchar_col = ?");
pstmt.setString(1, "value");

// GOOD — For varchar-heavy schemas, disable Unicode sending
props.put("sendStringParametersAsUnicode", "false");
```

### 9. Timeout Unit Confusion

Different timeout properties use **different units**:

| Property | Unit | Default |
|----------|------|---------|
| `loginTimeout` | seconds | 30 |
| `queryTimeout` | seconds | -1 (server default) |
| `cancelQueryTimeout` | seconds | -1 |
| `socketTimeout` | **milliseconds** | 0 (infinite) |
| `lockTimeout` | **milliseconds** | -1 (wait forever) |

```java
// BAD — Wrong units
props.put("socketTimeout", "30");   // 30 milliseconds, NOT 30 seconds!
props.put("queryTimeout", "30000"); // 30,000 seconds (8+ hours), NOT milliseconds!

// GOOD — Correct units
props.put("socketTimeout", "30000");  // 30 seconds in milliseconds
props.put("queryTimeout", "30");      // 30 seconds
```

### 10. Bulk Copy Without Options

```java
// BAD — No batch size, no timeout, no table lock, no try-with-resources
SQLServerBulkCopy bc = new SQLServerBulkCopy(con);
bc.setDestinationTableName("target");
bc.writeToServer(rs);  // Sends ALL rows in one batch

// GOOD — Configure for production use
try (SQLServerBulkCopy bc = new SQLServerBulkCopy(con)) {
    SQLServerBulkCopyOptions opts = new SQLServerBulkCopyOptions();
    opts.setBatchSize(10000);
    opts.setBulkCopyTimeout(120);
    opts.setTableLock(true);  // TABLOCK for performance
    bc.setBulkCopyOptions(opts);
    bc.setDestinationTableName("target");
    bc.writeToServer(rs);
}
```
