# mssql-jdbc Glossary

This glossary defines terms and acronyms commonly used in the Microsoft JDBC Driver for SQL Server codebase.

## Protocol & Communication

| Term | Full Name | Description |
|------|-----------|-------------|
| **TDS** | Tabular Data Stream | Microsoft's proprietary protocol for communication between clients and SQL Server. The driver implements TDS 7.x and 8.0. |
| **TDS Token** | TDS Token | A unit of data in the TDS protocol. Examples: COLMETADATA, ROW, DONE, LOGINACK. |
| **MARS** | Multiple Active Result Sets | Feature allowing multiple active statements on a single connection. |
| **RPC** | Remote Procedure Call | TDS message type for executing stored procedures. |
| **SQL Batch** | SQL Batch | TDS message type for executing SQL statements. |
| **Attention** | Attention Signal | TDS mechanism to cancel an in-progress request. |

## JDBC Concepts

| Term | Description |
|------|-------------|
| **Connection Pool** | A cache of database connections maintained for reuse. |
| **DataSource** | A factory for creating database connections, preferred over DriverManager. |
| **XA Transaction** | Distributed transaction that spans multiple resources (databases). |
| **Savepoint** | A point within a transaction to which you can roll back. |
| **ResultSet Types** | `TYPE_FORWARD_ONLY`, `TYPE_SCROLL_INSENSITIVE`, `TYPE_SCROLL_SENSITIVE` |
| **Concurrency** | `CONCUR_READ_ONLY`, `CONCUR_UPDATABLE` - whether ResultSet can be updated. |
| **Holdability** | Whether cursors remain open after commit. |

## Authentication

### Concepts

| Term | Full Name | Description |
|------|-----------|-------------|
| **SQL Auth** | SQL Server Authentication | Username/password authentication stored in SQL Server. |
| **Windows Auth** | Windows/Integrated Authentication | Uses Windows credentials (SSPI/Kerberos/NTLM). |
| **SSPI** | Security Support Provider Interface | Windows API for authentication. |
| **Kerberos** | Kerberos Protocol | Network authentication protocol using tickets. |
| **NTLM** | NT LAN Manager | Legacy Windows authentication protocol. |
| **Azure AD / Entra ID** | Azure Active Directory (now Microsoft Entra ID) | Microsoft's cloud identity service. |
| **MSAL** | Microsoft Authentication Library | Library for Azure AD authentication (replaced ADAL). |
| **ADAL** | Azure AD Authentication Library | Legacy Azure AD library, deprecated and replaced by MSAL. |
| **SPN** | Service Principal Name | Unique identifier for a service instance in Kerberos. |
| **UPN** | User Principal Name | User identifier format: `user@domain.com`. |
| **MFA** | Multi-Factor Authentication | Authentication requiring multiple verification methods. |
| **MSI** | Managed Service Identity | Azure-managed identity for services (now called Managed Identity). Synonym: `ActiveDirectoryMSI` maps to `ActiveDirectoryManagedIdentity`. |
| **FedAuth** | Federated Authentication | TDS feature extension (`0x02`) for Azure AD token-based authentication flows. |

### `SqlAuthentication` Enum (authentication connection property)

| Value | Description |
|-------|-------------|
| `NotSpecified` | Default. Driver infers auth from other properties (user/password → SQL Auth, integratedSecurity → Windows). |
| `SqlPassword` | SQL Server username/password authentication. |
| `ActiveDirectoryPassword` | Azure AD username/password via MSAL. |
| `ActiveDirectoryIntegrated` | Azure AD integrated auth using Windows credentials (Kerberos/NTLM). |
| `ActiveDirectoryManagedIdentity` | Azure Managed Identity (system-assigned or user-assigned via `msiClientId`). Synonym: `ActiveDirectoryMSI`. |
| `ActiveDirectoryServicePrincipal` | Azure AD service principal using client ID + secret (`AADSecurePrincipalId` / `AADSecurePrincipalSecret`). |
| `ActiveDirectoryServicePrincipalCertificate` | Azure AD service principal using client certificate (`clientCertificate`). |
| `ActiveDirectoryInteractive` | Azure AD interactive browser-based auth (supports MFA). |
| `ActiveDirectoryDefault` | Azure AD `DefaultAzureCredential` chain (environment vars → managed identity → developer tools → etc.). |

### `AuthenticationScheme` Enum (authenticationScheme connection property)

| Value | Description |
|-------|-------------|
| `nativeAuthentication` | Default. Uses Windows SSPI for integrated auth. Only works on Windows. |
| `javaKerberos` | Pure-Java Kerberos via JAAS/GSS-API. Cross-platform. Uses `jaasConfigurationName`, `serverSpn`, `realm`. |
| `NTLM` | NTLM authentication. Uses `domain` connection property. |

### `KeyStoreAuthentication` Enum (keyStoreAuthentication connection property)

| Value | Description |
|-------|-------------|
| `JavaKeyStorePassword` | Authenticate to Java Key Store using `keyStoreSecret` and `keyStoreLocation`. For Always Encrypted CEK storage. |
| `KeyVaultClientSecret` | Authenticate to Azure Key Vault using `keyVaultProviderClientId` and `keyVaultProviderClientKey`. |
| `KeyVaultManagedIdentity` | Authenticate to Azure Key Vault using Managed Identity. |

### Access Token Authentication

| Term | Description |
|------|-------------|
| **accessToken** | Pre-acquired Azure AD token passed via connection properties (not connection string). |
| **SQLServerAccessTokenCallback** | Interface for custom token acquisition. Implement and set via `accessTokenCallback` property or `accessTokenCallbackClass` connection string property. |
| **gsscredential** | Pre-created `GSSCredential` object for Kerberos auth. Pass via connection properties (not connection string). |

## Security & Encryption

| Term | Full Name | Description |
|------|-----------|-------------|
| **TLS/SSL** | Transport Layer Security | Encryption for data in transit. |
| **AE** | Always Encrypted | Column-level encryption feature in SQL Server. |
| **CEK** | Column Encryption Key | Symmetric key that encrypts column data. |
| **CMK** | Column Master Key | Key that protects (encrypts) the CEK. |
| **Deterministic Encryption** | - | Same plaintext always produces same ciphertext. Allows equality comparisons. |
| **Randomized Encryption** | - | Same plaintext produces different ciphertext each time. More secure but no comparisons. |
| **Enclave** | Secure Enclave | Trusted execution environment for Always Encrypted operations. |
| **AAS** | Azure Attestation Service | Service that verifies enclave integrity. |
| **HGS** | Host Guardian Service | On-premises enclave attestation service. |
| **VBS** | Virtualization-Based Security | Windows security feature using Hyper-V. |
| **SGX** | Intel Software Guard Extensions | Hardware-based enclave technology. |

## Data Types

All SQL Server types supported by the driver, mapped from the `SSType` enum in `DataTypes.java`.

### Numeric Types

| SSType | SQL Server Type | Java Type | Description |
|--------|-----------------|-----------|-------------|
| **TINYINT** | tinyint | `short` | Unsigned 8-bit integer (0–255) |
| **BIT** | bit | `boolean` | Boolean (0 or 1) |
| **SMALLINT** | smallint | `short` | Signed 16-bit integer |
| **INTEGER** | int | `int` | Signed 32-bit integer |
| **BIGINT** | bigint | `long` | Signed 64-bit integer |
| **FLOAT** | float | `double` | 64-bit floating point (maps to JDBC `DOUBLE`) |
| **REAL** | real | `float` | 32-bit floating point |
| **DECIMAL** | decimal | `java.math.BigDecimal` | Fixed-precision decimal |
| **NUMERIC** | numeric | `java.math.BigDecimal` | Fixed-precision numeric (synonym for decimal) |
| **SMALLMONEY** | smallmoney | `java.math.BigDecimal` | Currency, 4 decimal places (±214,748.3647) |
| **MONEY** | money | `java.math.BigDecimal` | Currency, 4 decimal places (±922,337,203,685,477.5807) |

### Character Types

| SSType | SQL Server Type | Java Type | Description |
|--------|-----------------|-----------|-------------|
| **CHAR** | char(n) | `String` | Fixed-length non-Unicode (max 8000 bytes) |
| **VARCHAR** | varchar(n) | `String` | Variable-length non-Unicode (max 8000 bytes) |
| **VARCHARMAX** | varchar(max) | `String` | Variable-length non-Unicode (max 2 GB). SSType name differs from SQL type. |
| **TEXT** | text | `String` | Legacy large non-Unicode text (deprecated, use varchar(max)) |
| **NCHAR** | nchar(n) | `String` | Fixed-length Unicode (max 4000 chars) |
| **NVARCHAR** | nvarchar(n) | `String` | Variable-length Unicode (max 4000 chars) |
| **NVARCHARMAX** | nvarchar(max) | `String` | Variable-length Unicode (max 1 GB). SSType name differs from SQL type. |
| **NTEXT** | ntext | `String` | Legacy large Unicode text (deprecated, use nvarchar(max)) |

### Binary Types

| SSType | SQL Server Type | Java Type | Description |
|--------|-----------------|-----------|-------------|
| **BINARY** | binary(n) | `byte[]` | Fixed-length binary (max 8000 bytes) |
| **VARBINARY** | varbinary(n) | `byte[]` | Variable-length binary (max 8000 bytes) |
| **VARBINARYMAX** | varbinary(max) | `byte[]` / `java.sql.Blob` | Variable-length binary (max 2 GB). SSType name differs from SQL type. |
| **IMAGE** | image | `byte[]` / `java.sql.Blob` | Legacy large binary (deprecated, use varbinary(max)) |

### Date/Time Types

| SSType | SQL Server Type | Java Type | Description |
|--------|-----------------|-----------|-------------|
| **DATE** | date | `java.sql.Date` | Date only (0001-01-01 to 9999-12-31) |
| **TIME** | time | `java.sql.Time` | Time only with fractional seconds (0–7 digits) |
| **SMALLDATETIME** | smalldatetime | `java.sql.Timestamp` | Date + time, 1-minute precision |
| **DATETIME** | datetime | `java.sql.Timestamp` | Date + time, 3.33ms precision |
| **DATETIME2** | datetime2 | `java.sql.Timestamp` | Date + time, 100ns precision (0–7 fractional digits) |
| **DATETIMEOFFSET** | datetimeoffset | `microsoft.sql.DateTimeOffset` | datetime2 with timezone offset |

### Special Types

| SSType | SQL Server Type | Java Type | Description |
|--------|-----------------|-----------|-------------|
| **GUID** | uniqueidentifier | `String` | 16-byte globally unique identifier |
| **SQL_VARIANT** | sql_variant | `Object` | Stores values of various data types |
| **XML** | xml | `java.sql.SQLXML` / `String` | XML data type |
| **TIMESTAMP** | timestamp / rowversion | `byte[]` | Auto-generated binary(8) row version number. Not a date/time type. |
| **JSON** | json | `String` | Native JSON data type (SQL Server 2025+). `SSType.JSON`. |

### LOB Abstractions (JDBC)

| Term | SQL Server Type | Java Type | Description |
|------|-----------------|-----------|-------------|
| **BLOB** | varbinary(max), image | `java.sql.Blob` | Binary Large Object |
| **CLOB** | varchar(max), text | `java.sql.Clob` | Character Large Object |
| **NCLOB** | nvarchar(max), ntext | `java.sql.NClob` | National Character LOB (Unicode) |

### User-Defined & Spatial Types

| SSType | SQL Server Type | Java Type | Description |
|--------|-----------------|-----------|-------------|
| **UDT** | CLR user-defined type | `byte[]` | Custom CLR types in SQL Server |
| **Geometry** | geometry | `microsoft.sql.Geometry` | Planar spatial data (UDT category) |
| **Geography** | geography | `microsoft.sql.Geography` | Geodetic spatial data (UDT category) |

### Table-Valued Parameter

| Term | SQL Server Type | Java Type | Description |
|------|-----------------|-----------|-------------|
| **TVP** | Table Type | `SQLServerDataTable` / `ISQLServerDataRecord` | Table-Valued Parameter. TDS type `0xF3`. |

### Vector Type

| SSType | SQL Server Type | Java Type | Description |
|--------|-----------------|-----------|-------------|
| **VECTOR** | vector | `microsoft.sql.Vector` | Fixed-dimension numeric array. FLOAT32 (v1, 4 bytes/dim, max 1998 dims) or FLOAT16 (v2, 2 bytes/dim, max 3996 dims). Max payload 8000 bytes. Header = 8 bytes. |

### hierarchyid

| Term | SQL Server Type | Java Type | Description |
|------|-----------------|-----------|-------------|
| **hierarchyid** | hierarchyid | `byte[]` | Represents position in a tree hierarchy |

## Bulk Operations

| Term | Description |
|------|-------------|
| **Bulk Copy** | High-performance data loading using TDS bulk insert protocol. |
| **BCP** | Bulk Copy Program — command-line utility; also refers to the TDS bulk insert protocol used by `SQLServerBulkCopy`. |
| **SQLServerBulkCopy** | Driver class that bulk-loads data into SQL Server tables. Accepts `ISQLServerBulkData` or `ResultSet` as source. |
| **SQLServerBulkCopyOptions** | Configuration for bulk copy: batch size, timeouts, table lock, check constraints, fire triggers, keep identity, keep nulls, allow encrypted value modifications. |
| **ISQLServerBulkData** | Interface for custom bulk copy data sources. Implemented by `SQLServerBulkCSVFileRecord` and `SQLServerBulkBatchInsertRecord`. |
| **SQLServerBulkCSVFileRecord** | Reads CSV files as a bulk copy data source. |
| **Batch Size** | Number of rows sent to server per batch during bulk copy (`setBatchSize()`). Default 0 = send all rows in one batch. |
| **Table Lock** | `TABLOCK` hint — lock entire table during bulk insert for better performance (`setUseInternalTransaction(false)` + `setTableLock(true)`). |
| **Check Constraints** | Whether to validate constraints during bulk insert (`setCheckConstraints(true)`). |
| **Fire Triggers** | Whether to execute insert triggers during bulk insert (`setFireTriggers(true)`). |
| **Keep Identity** | Preserve source identity values instead of server-generated ones (`setKeepIdentity(true)`). |
| **Keep Nulls** | Preserve null values instead of applying column defaults (`setKeepNulls(true)`). |
| **Bulk Copy Timeout** | Seconds before bulk copy operation times out (`setBulkCopyTimeout()`). Default 60 seconds. |

## Connection Properties

### Server & Database

| Property | Default | Description |
|----------|---------|-------------|
| **serverName** | `""` | SQL Server hostname or IP address. Synonym: `server`. |
| **portNumber** | `1433` | TCP port number. Synonym: `port`. |
| **databaseName** | `""` | Initial database to connect to. Synonym: `database`. |
| **instanceName** | `""` | Named instance (uses SQL Browser to find port). |
| **failoverPartner** | `""` | Failover partner server for database mirroring. |
| **workstationID** | (machine name) | Client workstation name reported to SQL Server. |
| **applicationName** | `"Microsoft JDBC Driver for SQL Server"` | Client application name reported to SQL Server. |
| **iPAddressPreference** | `"IPv4First"` | IP address preference: `IPv4First`, `IPv6First`, `UsePlatformDefault`. |

### Authentication

| Property | Default | Description |
|----------|---------|-------------|
| **user** | `""` | SQL authentication username. Synonym: `userName`. |
| **password** | `""` | SQL authentication password. |
| **integratedSecurity** | `false` | Use Windows authentication. |
| **authentication** | `"NotSpecified"` | Authentication mode: `SqlPassword`, `ActiveDirectoryPassword`, `ActiveDirectoryIntegrated`, `ActiveDirectoryMSI`, `ActiveDirectoryInteractive`, `ActiveDirectoryServicePrincipal`, `ActiveDirectoryServicePrincipalCertificate`, `ActiveDirectoryDefault`, `NotSpecified`. |
| **authenticationScheme** | `"nativeAuthentication"` | Auth scheme: `nativeAuthentication`, `javaKerberos`, `NTLM`. |
| **domain** | `""` | Windows domain for NTLM authentication. Synonym: `domainName`. |
| **serverSpn** | `""` | Service Principal Name for Kerberos auth. |
| **realm** | `""` | Kerberos realm. |
| **jaasConfigurationName** | `"SQLJDBCDriver"` | JAAS configuration entry name for Kerberos. |
| **useDefaultJaasConfig** | `false` | Use default JAAS config instead of driver-provided. |
| **useDefaultGSSCredential** | `false` | Use default GSS credential for Kerberos. |
| **gsscredential** | `null` | Pre-created GSS credential object (Properties only, not connection string). |
| **accessToken** | `""` | Azure AD access token (Properties only, not connection string). |
| **accessTokenCallback** | `null` | Callback object for access token retrieval (Properties only). |
| **accessTokenCallbackClass** | `""` | Class name implementing access token callback. |
| **AADSecurePrincipalId** | `""` | Azure AD service principal ID. |
| **AADSecurePrincipalSecret** | `""` | Azure AD service principal secret. |
| **msiClientId** | `""` | Client ID for Managed Identity authentication. |

### Encryption & TLS

| Property | Default | Description |
|----------|---------|-------------|
| **encrypt** | `"true"` | Enable TLS encryption: `true`, `false`, `strict`. |
| **trustServerCertificate** | `false` | Trust server's SSL certificate without validation. |
| **hostNameInCertificate** | `""` | Expected hostname in server certificate. |
| **sslProtocol** | `"TLS"` | SSL/TLS protocol version: `TLS`, `TLSv1`, `TLSv1.1`, `TLSv1.2`. |
| **trustStoreType** | `"JKS"` | Trust store type (e.g., JKS, PKCS12). |
| **trustStore** | `""` | Path to trust store file. |
| **trustStorePassword** | `""` | Trust store password. |
| **trustManagerClass** | `""` | Custom TrustManager class name. |
| **trustManagerConstructorArg** | `""` | Argument for custom TrustManager constructor. |
| **serverCertificate** | `""` | Server certificate path (for strict encryption). |
| **clientCertificate** | `""` | Client certificate path for mutual authentication. |
| **clientKey** | `""` | Client private key path. |
| **clientKeyPassword** | `""` | Client private key password. |
| **fips** | `false` | Enable FIPS-compliant mode. |

### Always Encrypted

| Property | Default | Description |
|----------|---------|-------------|
| **columnEncryptionSetting** | `"Disabled"` | Always Encrypted: `Enabled` or `Disabled`. |
| **enclaveAttestationUrl** | `""` | URL for secure enclave attestation service. |
| **enclaveAttestationProtocol** | `""` | Protocol for enclave attestation: `HGS`, `AAS`, `NONE`. |
| **keyStoreAuthentication** | `""` | Key store auth type: `JavaKeyStorePassword`. |
| **keyStoreSecret** | `""` | Secret/password for key store. |
| **keyStoreLocation** | `""` | Path to Java Key Store file. |
| **keyStorePrincipalId** | `""` | Principal ID for key store. |
| **keyVaultProviderClientId** | `""` | Client ID for Azure Key Vault provider. |
| **keyVaultProviderClientKey** | `""` | Client key for Azure Key Vault provider. |

### Timeouts & Retries

| Property | Default | Description |
|----------|---------|-------------|
| **loginTimeout** | `30` | Seconds to wait for connection. |
| **socketTimeout** | `0` | Seconds to wait for socket read operations (`0` = infinite). |
| **queryTimeout** | `-1` | Query timeout in seconds (`-1` = use server default). |
| **lockTimeout** | `-1` | Lock timeout in milliseconds (`-1` = wait forever). |
| **cancelQueryTimeout** | `-1` | Seconds before cancelling a query that exceeds queryTimeout. |
| **connectRetryCount** | `1` | Number of connection retry attempts (0–255). |
| **connectRetryInterval** | `10` | Seconds between connection retry attempts (1–60). |
| **retryExec** | `""` | Configurable retry logic rules for statement execution. |
| **retryConn** | `""` | Configurable retry logic rules for connections. |

### Statement & Query Behavior

| Property | Default | Description |
|----------|---------|-------------|
| **prepareMethod** | `"prepexec"` | Prepared statement method: `prepexec`, `prepare`, `none`, `scopeTempTablesToConnection`. |
| **responseBuffering** | `"adaptive"` | `adaptive` (stream results) or `full` (buffer all). |
| **selectMethod** | `"direct"` | `direct` or `cursor` for result retrieval. |
| **sendStringParametersAsUnicode** | `true` | Send string params as Unicode (nvarchar). |
| **sendTimeAsDatetime** | `true` | Send `java.sql.Time` as `datetime` instead of `time`. |
| **datetimeParameterType** | `"datetime2"` | Datetime parameter type: `datetime`, `datetime2`, `datetimeoffset`. |
| **lastUpdateCount** | `true` | Return only the last update count from batch. |
| **useFmtOnly** | `false` | Use `SET FMTONLY` for parameter metadata. |
| **calcBigDecimalPrecision** | `false` | Calculate BigDecimal precision from value. |
| **ignoreOffsetOnDateTimeOffsetConversion** | `false` | Ignore offset when converting DateTimeOffset. |
| **delayLoadingLobs** | `true` | Delay loading LOBs (stream instead of buffer). |
| **quotedIdentifier** | `"ON"` | `SET QUOTED_IDENTIFIER` setting: `ON` / `OFF`. |
| **concatNullYieldsNull** | `"ON"` | `SET CONCAT_NULL_YIELDS_NULL` setting: `ON` / `OFF`. |
| **maxResultBuffer** | `"-1"` | Max result buffer size in bytes (`-1` = unlimited). Can use `p` suffix for percentage of heap. |

### Statement Pooling

| Property | Default | Description |
|----------|---------|-------------|
| **disableStatementPooling** | `true` | Disable prepared statement pooling. |
| **statementPoolingCacheSize** | `0` | Size of prepared statement cache (`0` = disabled). |
| **enablePrepareOnFirstPreparedStatementCall** | `false` | Prepare statements on first execution instead of second. |
| **serverPreparedStatementDiscardThreshold** | `10` | Threshold before batch-unpreparing discarded prepared statements. |

### High Availability & Failover

| Property | Default | Description |
|----------|---------|-------------|
| **multiSubnetFailover** | `false` | Enable fast failover for AlwaysOn Availability Groups. |
| **applicationIntent** | `"ReadWrite"` | `ReadWrite` or `ReadOnly` for read routing. |
| **TransparentNetworkIPResolution** | `true` | Enable Transparent Network IP Resolution. |

### Bulk Copy for Batch Insert

| Property | Default | Description |
|----------|---------|-------------|
| **useBulkCopyForBatchInsert** | `false` | Use bulk copy API for batch inserts. |
| **bulkCopyForBatchInsertBatchSize** | `0` | Batch size for bulk copy batch inserts (`0` = send all at once). |
| **bulkCopyForBatchInsertCheckConstraints** | `false` | Check constraints during bulk copy batch insert. |
| **bulkCopyForBatchInsertFireTriggers** | `false` | Fire triggers during bulk copy batch insert. |
| **bulkCopyForBatchInsertKeepIdentity** | `false` | Keep identity values during bulk copy batch insert. |
| **bulkCopyForBatchInsertKeepNulls** | `false` | Keep null values during bulk copy batch insert. |
| **bulkCopyForBatchInsertTableLock** | `false` | Acquire table lock during bulk copy batch insert. |
| **bulkCopyForBatchInsertAllowEncryptedValueModifications** | `false` | Allow encrypted value modifications in bulk copy batch insert. |
| **cacheBulkCopyMetadata** | `false` | Cache bulk copy metadata for performance. |
| **sendTemporalDataTypesAsStringForBulkCopy** | `true` | Send temporal data types as strings in bulk copy. |

### Networking & Misc

| Property | Default | Description |
|----------|---------|-------------|
| **packetSize** | `8000` | TDS packet size in bytes. |
| **socketFactoryClass** | `""` | Custom socket factory class name. |
| **socketFactoryConstructorArg** | `""` | Argument for custom socket factory constructor. |
| **serverNameAsACE** | `false` | Encode server name using ACE (Punycode) for internationalized domain names. |
| **replication** | `false` | Connection is used for replication. |
| **xopenStates** | `false` | Use X/Open-compliant SQLState codes. |
| **vectorTypeSupport** | `"v1"` | Vector data type support: `off`, `v1`, `v2`. |

### Property Synonyms

| Synonym | Maps To |
|---------|---------|
| `database` | `databaseName` |
| `userName` | `user` |
| `server` | `serverName` |
| `domainName` | `domain` |
| `port` | `portNumber` |

## SQL Server Features

| Term | Description |
|------|-------------|
| **AlwaysOn AG** | AlwaysOn Availability Groups — high availability with readable secondary replicas. TDS feature extension `0x04`. |
| **FCI** | Failover Cluster Instance — clustered SQL Server. |
| **Read Routing** | Routing read-only queries to secondary replicas when `applicationIntent=ReadOnly`. |
| **Idle Connection Resiliency** | Automatic reconnection after transient network failures on idle connections. Uses `TDS_FEATURE_EXT_SESSIONRECOVERY` (`0x01`). Managed by `IdleConnectionResiliency` class. Configured via `connectRetryCount` and `connectRetryInterval`. |
| **Configurable Retry Logic** | Rule-based retry for connections and statements. Implemented in `ConfigurableRetryLogic` / `ConfigurableRetryRule`. Configured via `retryExec` / `retryConn` connection properties. |
| **Always Encrypted (AE)** | Column-level encryption. CEKs/CMKs, key store providers (`JavaKeyStoreProvider`, `AzureKeyVaultProvider`), `TDS_FEATURE_EXT_AE` (`0x04`). |
| **Secure Enclaves** | Server-side trusted execution for Always Encrypted operations. Enclave providers: `SQLServerAASEnclaveProvider`, `SQLServerVSMEnclaveProvider`, `SQLServerNoneEnclaveProvider`. |
| **Federated Authentication (FedAuth)** | Azure AD token-based authentication. `TDS_FEATURE_EXT_FEDAUTH` (`0x02`). Supports `ActiveDirectoryPassword`, `ActiveDirectoryIntegrated`, `ActiveDirectoryMSI`, `ActiveDirectoryInteractive`, `ActiveDirectoryServicePrincipal`, `ActiveDirectoryDefault`. |
| **Data Classification** | SQL Server sensitivity classification metadata. `TDS_FEATURE_EXT_DATACLASSIFICATION` (`0x09`). Classes: `SensitivityClassification`, `ColumnSensitivity`, `Label`, `InformationType`, `SensitivityProperty`. |
| **UTF-8 Support** | Server-side UTF-8 collation support. `TDS_FEATURE_EXT_UTF8SUPPORT` (`0x0A`). |
| **Vector Support** | VECTOR data type for fixed-dimension numeric arrays (FLOAT32/FLOAT16). `TDS_FEATURE_EXT_VECTORSUPPORT` (`0x0E`). |
| **JSON Support** | Native JSON data type. `TDS_FEATURE_EXT_JSONSUPPORT` (`0x0D`). `SSType.JSON`. |
| **Azure SQL DNS Caching** | DNS caching for Azure SQL connections. `TDS_FEATURE_EXT_AZURESQLDNSCACHING` (`0x0B`). |
| **Adaptive Buffering** | Streaming result processing via `responseBuffering=adaptive`. Avoids buffering entire result sets in memory. |
| **Query Notifications** | Notifications when query results would change. |
| **Change Tracking** | Tracking changes to table data. |
| **Sparse Columns** | Optimized storage for columns with many NULLs. |
| **Column Set** | XML representation of all sparse columns. |

## Error Handling

| Term | Description |
|------|-------------|
| **SQLServerException** | Driver's primary exception class extending `java.sql.SQLException`. Created via `makeFromDriverError()` or `makeFromDatabaseError()`. |
| **SQLServerError** | TDS error message class implementing `ISQLServerMessage`. Carries error number, severity, state, line number, procedure name. |
| **SQLServerInfoMessage** | TDS informational message class (severity < 10) implementing `ISQLServerMessage`. |
| **SQLServerWarning** | Driver's `java.sql.SQLWarning` implementation. Created from `SQLServerError` / `SQLServerInfoMessage`. |
| **ISQLServerMessage** | Interface implemented by both `SQLServerError` and `SQLServerInfoMessage`. Methods: `isErrorMessage()`, `isInfoMessage()`, `getErrorNumber()`, `getErrorSeverity()`. |
| **ISQLServerMessageHandler** | Callback interface to customize how the driver handles SQL Server messages. Register via `setServerMessageHandler()`. |
| **TransientError** | Enum nested in `SQLServerError` defining well-known transient error codes (e.g., 4060, 40197) for retry logic. |
| **SQLState** | 5-character ANSI SQL error code. |
| **Error Number** | SQL Server-specific error number. |
| **Severity** | Error severity level (0–25). Levels ≥ 10 are errors; < 10 are informational messages. |
| **State** | Additional error state information. |
| **Line Number** | Line in batch where error occurred. |
| **Procedure** | Stored procedure name if applicable. |

## Logging & Diagnostics

| Term | Description |
|------|-------------|
| **Activity ID** | GUID for distributed tracing across services. |
| **ActivityCorrelator** | Thread-local map of `ActivityId` objects. Methods: `getCurrent()`, `getNext()`. Bridges Activity IDs with logging. |
| **Client Request ID** | Unique identifier for a client request. |
| **Sequence Number** | Order of operations within an activity. |
| **java.util.logging** | Java's built-in logging framework used by the driver. |
| **loggerExternal** | Logger for public API entry/exit tracing. Logged at `FINER` level. Name pattern: `com.microsoft.sqlserver.jdbc.<ClassName>`. |
| **connectionlogger** | Internal logger for protocol-level logging (TDS operations, connection state). Name pattern: `com.microsoft.sqlserver.jdbc.internals.<ClassName>`. |
| **loggerResiliency** | Logger for connection resiliency events. Name: `com.microsoft.sqlserver.jdbc.IdleConnectionResiliency`. |
| **PerformanceLog** | Performance metrics logging infrastructure. Publishes via `PerformanceLogCallback` and/or `java.util.logging`. |
| **PerformanceActivity** | Enum of tracked activities: `CONNECTION`, `PRELOGIN`, `LOGIN`, `TOKEN_ACQUISITION`, `STATEMENT_REQUEST_BUILD`, `STATEMENT_FIRST_SERVER_RESPONSE`, `STATEMENT_PREPARE`, `STATEMENT_PREPEXEC`, `STATEMENT_EXECUTE`. |
| **PerformanceLogCallback** | Callback interface for receiving performance data programmatically. Register via `SQLServerDriver.registerPerformanceLogCallback()`. |
| **Logging Levels** | `FINE` — significant operations (reconnect, failures). `FINER` — API entry/exit, activity IDs. `FINEST` — detailed wire data, internal state. |

## Code Abbreviations

| Abbreviation | Meaning |
|--------------|---------|
| **con** | Connection |
| **stmt** | Statement |
| **pstmt** | PreparedStatement |
| **cstmt** | CallableStatement |
| **rs** | ResultSet |
| **rsmd** | ResultSetMetaData |
| **dbmd** | DatabaseMetaData |
| **ds** | DataSource |
| **tds** | TDS-related operations |
| **DTV** | Data Type Value — core value holder for parameters/columns (`dtv.java`) |
| **SSType** | SQL Server Type — enum mapping SQL Server native types (`DataTypes.java`) |
| **JDBCType** | JDBC Type — driver's internal JDBC type enum (`DataTypes.java`) |
| **DDC** | Data type conversion utilities (`DDC.java`) |
| **AE** | Always Encrypted (`AE.java`) |
| **TVP** | Table-Valued Parameter (`TVP.java`) |
| **PLP** | Partially Length-Prefixed — streaming data format (`PLPInputStream.java`) |
| **DTC** | Distributed Transaction Coordinator |
| **CEK** | Column Encryption Key |
| **CMK** | Column Master Key |
| **FMT** | Format — FMTONLY queries (`SQLServerFMTQuery.java`) |
| **UDT** | User-Defined Type (`UDTTDSHeader.java`) |
| **col** | Column |
| **param** | Parameter |

## File Naming Conventions

| Pattern | Description |
|---------|-------------|
| `SQLServer*.java` | Main JDBC interface implementations |
| `I*.java` | Interfaces (e.g., `ISQLServerConnection`) |
| `*43.java` | JDBC 4.3-specific implementations (e.g., `SQLServerConnection43.java`) |
| `Stream*.java` | TDS token stream handlers |
| `*Utils.java` / `*Util.java` | Utility classes |
| `*Exception.java` | Exception classes (`SQLServerException.java`) |
| `*Error.java` / `*Warning.java` | Error and warning classes |
| `*Provider.java` | Encryption/enclave provider implementations |
| `*Callback.java` | Callback interfaces (`PerformanceLogCallback.java`, `SQLServerAccessTokenCallback.java`) |
| `*Record.java` | Bulk copy record implementations (`SQLServerBulkCSVFileRecord.java`) |
| `*Constants.java` | Constant definitions |
| `*Test.java` | JUnit test classes (predominant pattern) |
| Lowercase files | Some internal files use lowercase: `dtv.java`, `tdsparser.java` |
| Subpackages | Feature-specific: `dataclassification/`, `dns/`, `osgi/`, `spatialdatatypes/` |

## Version-Specific Terms

| Term | Description |
|------|-------------|
| **JDBC 4.2** | Java 8 compatible API. Build profile: `jre8`. |
| **JDBC 4.3** | Java 9+ compatible API. Build profiles: `jre11`, `jre17`, `jre21`, `jre25`. |
| **TDS 7.2 (YUKON)** | Protocol version for SQL Server 2005+. Constant: `VER_YUKON` (`0x72090002`). |
| **TDS 7.3B (KATMAI)** | Protocol version for SQL Server 2008+ (adds null-bit compression). Constant: `VER_KATMAI` (`0x730B0003`). |
| **TDS 7.4 (DENALI)** | Protocol version for SQL Server 2012+. Constant: `VER_DENALI` (`0x74000004`). |
| **TDS 8.0** | Protocol version with strict encryption (SQL Server 2022+). Constant: `VER_TDS80` (`0x08000000`). |
| **SQL Server Codenames** | YUKON = 2005, KATMAI = 2008, DENALI = 2012. Used in TDS version constants and throughout the codebase. |
