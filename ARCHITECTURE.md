# mssql-jdbc Architecture

This document describes the high-level architecture of the Microsoft JDBC Driver for SQL Server.

## Overview

The driver implements the JDBC 4.2/4.3 specification and communicates with SQL Server using the TDS (Tabular Data Stream) protocol. It supports SQL Server 2012 and later, as well as Azure SQL Database.

## Layer Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Application Layer                            │
│                  (User Application Code)                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      JDBC API Layer                              │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────────┐    │
│  │ Connection  │  │  Statement   │  │     ResultSet       │    │
│  │   Pool      │  │   Cache      │  │     Handling        │    │
│  └─────────────┘  └──────────────┘  └─────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Protocol Layer (TDS)                          │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────────┐    │
│  │  IOBuffer   │  │  TDSParser   │  │   Stream Handlers   │    │
│  │  (I/O)      │  │  (Tokens)    │  │   (Data Types)      │    │
│  └─────────────┘  └──────────────┘  └─────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Transport Layer                               │
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────────────┐    │
│  │   Socket    │  │     SSL      │  │   Authentication    │    │
│  │   I/O       │  │   Handler    │  │   (Kerberos/NTLM)   │    │
│  └─────────────┘  └──────────────┘  └─────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      SQL Server                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Package Structure

```
com.microsoft.sqlserver.jdbc/
│
├── Connection Management
│   ├── SQLServerConnection.java         # Main connection implementation
│   ├── SQLServerConnection43.java       # JDBC 4.3 extensions
│   ├── SQLServerDataSource.java         # DataSource implementation
│   ├── SQLServerConnectionPoolDataSource.java
│   ├── SQLServerPooledConnection.java
│   ├── SQLServerConnectionPoolProxy.java
│   ├── SQLServerXAConnection.java       # XA transaction support
│   ├── SQLServerXADataSource.java
│   └── SQLServerXAResource.java
│
├── Statement Execution
│   ├── SQLServerStatement.java          # Basic statements
│   ├── SQLServerPreparedStatement.java  # Parameterized queries
│   ├── SQLServerCallableStatement.java  # Stored procedures
│   ├── Parameter.java                   # Parameter binding
│   ├── ParameterMetaDataCache.java      # Prepared statement cache
│   └── SQLServerParameterMetaData.java
│
├── Result Processing
│   ├── SQLServerResultSet.java          # Result set implementation
│   ├── SQLServerResultSetMetaData.java  # Column metadata
│   ├── SQLServerDatabaseMetaData.java   # Database metadata
│   ├── Column.java                      # Column data handling
│   └── ScrollWindow.java                # Scrollable cursors
│
├── TDS Protocol
│   ├── IOBuffer.java                    # TDS packet I/O
│   ├── tdsparser.java                   # Token stream parsing
│   ├── StreamPacket.java                # Base packet handler
│   ├── StreamColInfo.java               # Column info token
│   ├── StreamColumns.java               # Column metadata token
│   ├── StreamDone.java                  # Done token
│   ├── StreamLoginAck.java              # Login acknowledgment
│   ├── StreamRetStatus.java             # Return status
│   ├── StreamRetValue.java              # Return value
│   └── StreamTabName.java               # Table name token
│
├── Data Types
│   ├── DataTypes.java                   # Type mappings
│   ├── DDC.java                         # Data type conversion
│   ├── dtv.java                         # Data type value handling
│   ├── SQLServerBlob.java               # BLOB support
│   ├── SQLServerClob.java               # CLOB support
│   ├── SQLServerNClob.java              # NCLOB support
│   ├── SQLServerSQLXML.java             # XML support
│   ├── Geography.java                   # Spatial - Geography
│   ├── Geometry.java                    # Spatial - Geometry
│   ├── SQLServerSpatialDatatype.java    # Spatial base class
│   └── SqlVariant.java                  # sql_variant support
│
├── Bulk Operations
│   ├── SQLServerBulkCopy.java           # Bulk copy engine
│   ├── SQLServerBulkCopyOptions.java    # Bulk copy settings
│   ├── SQLServerBulkCSVFileRecord.java  # CSV file reader
│   ├── SQLServerBulkRecord.java         # Base bulk record
│   ├── SQLServerBulkBatchInsertRecord.java
│   ├── ISQLServerBulkData.java          # Bulk data interface
│   ├── ISQLServerBulkRecord.java        # Bulk record interface
│   ├── SQLServerDataTable.java          # In-memory table
│   └── SQLServerDataColumn.java         # Table column definition
│
├── Table-Valued Parameters
│   ├── TVP.java                         # TVP implementation
│   ├── SQLServerMetaData.java           # TVP column metadata
│   └── SQLServerSortOrder.java          # Sort order enum
│
├── Security & Authentication
│   ├── SQLServerSecurityUtility.java    # Security utilities
│   ├── KerbAuthentication.java          # Kerberos auth
│   ├── NTLMAuthentication.java          # NTLM auth
│   ├── SSPIAuthentication.java          # SSPI interface
│   ├── AuthenticationJNI.java           # Native auth bridge
│   ├── SQLServerMSAL4JUtils.java        # Azure AD (MSAL)
│   ├── SqlAuthenticationToken.java      # Auth token handling
│   └── SQLServerAccessTokenCallback.java # Token callback
│
├── Always Encrypted
│   ├── SQLServerColumnEncryptionKeyStoreProvider.java  # Base provider
│   ├── SQLServerColumnEncryptionJavaKeyStoreProvider.java
│   ├── SQLServerColumnEncryptionAzureKeyVaultProvider.java
│   ├── SQLServerColumnEncryptionCertificateStoreProvider.java
│   ├── SQLServerAeadAes256CbcHmac256Algorithm.java
│   ├── SQLServerAeadAes256CbcHmac256EncryptionKey.java
│   ├── SQLServerSymmetricKey.java
│   ├── SQLServerSymmetricKeyCache.java
│   ├── KeyStoreProviderCommon.java
│   └── SQLServerEncryptionType.java
│
├── Secure Enclaves
│   ├── ISQLServerEnclaveProvider.java   # Enclave interface
│   ├── SQLServerAASEnclaveProvider.java # Azure Attestation
│   ├── SQLServerVSMEnclaveProvider.java # Host Guardian
│   └── SQLServerNoneEnclaveProvider.java
│
├── Error Handling
│   ├── SQLServerException.java          # Main exception class
│   ├── SQLServerError.java              # Server error details
│   ├── SQLServerWarning.java            # SQL warnings
│   ├── SQLServerInfoMessage.java        # Info messages
│   ├── ISQLServerMessage.java           # Message interface
│   ├── ISQLServerMessageHandler.java    # Message handler
│   └── SQLServerResource.java           # Error resources
│
├── Utilities
│   ├── Util.java                        # General utilities
│   ├── StringUtils.java                 # String operations
│   ├── ParameterUtils.java              # Parameter parsing
│   ├── SQLServerDriver.java             # JDBC Driver entry
│   ├── ActivityCorrelator.java          # Distributed tracing
│   └── SQLCollation.java                # Collation handling
│
├── Resilience & Retry
│   ├── ConfigurableRetryLogic.java      # Retry configuration
│   ├── ConfigurableRetryRule.java       # Retry rules
│   ├── IdleConnectionResiliency.java    # Connection resilience
│   ├── FailOverInfo.java                # Failover handling
│   └── FailOverMapSingleton.java        # Failover cache
│
└── Supporting Types
    ├── microsoft/sql/DateTimeOffset.java  # DateTimeOffset type
    ├── microsoft/sql/Types.java           # Extended SQL types
    └── mssql/googlecode/cityhash/         # CityHash for hashing
```

## Data Flow

### Query Execution Flow

```
Application                    Driver                         SQL Server
    │                            │                                │
    │  executeQuery(sql)         │                                │
    │ ──────────────────────────>│                                │
    │                            │                                │
    │                            │  Build TDS SQL Batch Packet    │
    │                            │ ──────────────────────────────>│
    │                            │                                │
    │                            │  TDS Response (Tokens)         │
    │                            │ <──────────────────────────────│
    │                            │                                │
    │                            │  Parse COLMETADATA token       │
    │                            │  Parse ROW tokens              │
    │                            │  Parse DONE token              │
    │                            │                                │
    │  ResultSet                 │                                │
    │ <──────────────────────────│                                │
    │                            │                                │
    │  rs.next() / rs.getString()│                                │
    │ ──────────────────────────>│                                │
    │                            │                                │
    │  Data from parsed tokens   │                                │
    │ <──────────────────────────│                                │
```

### Connection Establishment Flow

```
1. SQLServerDriver.connect(url, properties)
2. Parse connection URL and properties
3. Resolve server address (DNS/failover)
4. Establish TCP socket connection
5. SSL/TLS handshake (if encrypted)
6. TDS LOGIN7 packet exchange
7. Authentication (SQL/Windows/Azure AD)
8. TDS LOGINACK response
9. Return SQLServerConnection
```

### Bulk Copy Flow

```
Application                    SQLServerBulkCopy              SQL Server
    │                                 │                            │
    │  writeToServer(records)         │                            │
    │ ───────────────────────────────>│                            │
    │                                 │                            │
    │                                 │  BCP INSERT BULK           │
    │                                 │ ──────────────────────────>│
    │                                 │                            │
    │                                 │  For each batch:           │
    │                                 │    Build TDS ROW packets   │
    │                                 │ ──────────────────────────>│
    │                                 │                            │
    │                                 │  DONE token (rows affected)│
    │                                 │ <──────────────────────────│
    │                                 │                            │
    │  Rows copied count              │                            │
    │ <───────────────────────────────│                            │
```

## Key Design Patterns

### 1. Factory Pattern
- `SQLServerDataSourceObjectFactory` creates DataSource instances
- `SQLServerEncryptionAlgorithmFactoryList` manages encryption algorithms

### 2. Singleton Pattern
- `FailOverMapSingleton` - Global failover partner cache
- `SQLServerSymmetricKeyCache` - Encryption key cache
- `ParameterMetaDataCache` - Prepared statement metadata cache

### 3. Strategy Pattern
- `ISQLServerEnclaveProvider` - Different enclave attestation strategies
- `SQLServerColumnEncryptionKeyStoreProvider` - Different key store implementations

### 4. Builder Pattern
- Connection URL parsing builds connection properties
- `SQLServerBulkCopyOptions` configuration

### 5. Template Method Pattern
- `SQLServerSpatialDatatype` - Base class for Geography/Geometry

## Thread Safety

- `SQLServerConnection` is **NOT** thread-safe (per JDBC spec)
- Multiple statements on same connection must be synchronized
- `SQLServerDataSource` is thread-safe
- Connection pooling should use `SQLServerConnectionPoolDataSource`

## Memory Management

### Adaptive Buffering
- `responseBuffering=adaptive` (default) - Streams large results
- `responseBuffering=full` - Buffers entire result set

### LOB Handling
- LOBs are streamed by default to minimize memory
- `PLPInputStream` handles large binary data
- `ReaderInputStream` handles large character data

## Extension Points

### Custom Bulk Copy Data Sources
```java
public class MyBulkRecord implements ISQLServerBulkRecord {
    // Implement to provide custom data to bulk copy
}
```

### Custom Key Store Providers
```java
public class MyKeyStoreProvider extends SQLServerColumnEncryptionKeyStoreProvider {
    // Implement to provide custom key storage for Always Encrypted
}
```

### Custom Authentication Token Providers
```java
public class MyTokenCallback implements SQLServerAccessTokenCallback {
    // Implement to provide custom Azure AD tokens
}
```

### Message Handlers
```java
public class MyMessageHandler implements ISQLServerMessageHandler {
    // Implement to handle SQL Server messages/warnings
}
```
