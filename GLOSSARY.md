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

| Term | Full Name | Description |
|------|-----------|-------------|
| **SQL Auth** | SQL Server Authentication | Username/password authentication stored in SQL Server. |
| **Windows Auth** | Windows/Integrated Authentication | Uses Windows credentials (SSPI/Kerberos/NTLM). |
| **SSPI** | Security Support Provider Interface | Windows API for authentication. |
| **Kerberos** | Kerberos Protocol | Network authentication protocol using tickets. |
| **NTLM** | NT LAN Manager | Legacy Windows authentication protocol. |
| **Azure AD** | Azure Active Directory | Microsoft's cloud identity service. |
| **MSAL** | Microsoft Authentication Library | Library for Azure AD authentication (replaced ADAL). |
| **SPN** | Service Principal Name | Unique identifier for a service instance in Kerberos. |
| **UPN** | User Principal Name | User identifier format: `user@domain.com`. |
| **MFA** | Multi-Factor Authentication | Authentication requiring multiple verification methods. |
| **MSI** | Managed Service Identity | Azure-managed identity for services (now called Managed Identity). |

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

| Term | SQL Server Type | Java Type | Description |
|------|-----------------|-----------|-------------|
| **BLOB** | varbinary(max), image | `java.sql.Blob` | Binary Large Object |
| **CLOB** | varchar(max), text | `java.sql.Clob` | Character Large Object |
| **NCLOB** | nvarchar(max), ntext | `java.sql.NClob` | National Character LOB (Unicode) |
| **SQLXML** | xml | `java.sql.SQLXML` | XML data type |
| **TVP** | Table Type | `SQLServerDataTable` | Table-Valued Parameter |
| **UDT** | User-Defined Type | byte[] | Custom CLR types in SQL Server |
| **Geometry** | geometry | `Geometry` | Planar spatial data |
| **Geography** | geography | `Geography` | Geodetic spatial data |
| **DateTimeOffset** | datetimeoffset | `DateTimeOffset` | Date/time with timezone offset |
| **sql_variant** | sql_variant | `Object` | Stores values of various data types |
| **hierarchyid** | hierarchyid | byte[] | Represents position in a hierarchy |

## Bulk Operations

| Term | Description |
|------|-------------|
| **Bulk Copy** | High-performance data loading using TDS bulk insert protocol. |
| **BCP** | Bulk Copy Program | Command-line utility; also refers to the protocol. |
| **Batch Size** | Number of rows sent to server in one network round-trip. |
| **Table Lock** | Lock entire table during bulk insert for better performance. |
| **Check Constraints** | Whether to validate constraints during bulk insert. |
| **Fire Triggers** | Whether to execute insert triggers during bulk insert. |

## Connection Properties

| Property | Description |
|----------|-------------|
| **serverName** | SQL Server hostname or IP address. |
| **portNumber** | TCP port (default: 1433). |
| **databaseName** | Initial database to connect to. |
| **instanceName** | Named instance (uses SQL Browser to find port). |
| **encrypt** | Enable TLS encryption (`true`, `false`, `strict`). |
| **trustServerCertificate** | Trust server's SSL certificate without validation. |
| **hostNameInCertificate** | Expected hostname in server certificate. |
| **integratedSecurity** | Use Windows authentication. |
| **authentication** | Authentication mode (SqlPassword, ActiveDirectoryPassword, etc.). |
| **loginTimeout** | Seconds to wait for connection. |
| **socketTimeout** | Seconds to wait for socket operations. |
| **responseBuffering** | `adaptive` (stream results) or `full` (buffer all). |
| **selectMethod** | `direct` or `cursor` for result retrieval. |
| **sendStringParametersAsUnicode** | Send string params as Unicode (nvarchar). |
| **multiSubnetFailover** | Enable fast failover for AlwaysOn Availability Groups. |
| **applicationIntent** | `ReadWrite` or `ReadOnly` for read routing. |

## SQL Server Features

| Term | Description |
|------|-------------|
| **AlwaysOn AG** | AlwaysOn Availability Groups - high availability solution. |
| **FCI** | Failover Cluster Instance - clustered SQL Server. |
| **Read Routing** | Routing read queries to secondary replicas. |
| **Connection Resiliency** | Automatic reconnection after transient failures. |
| **Idle Connection Resiliency** | Keeping idle connections alive. |
| **Query Notifications** | Notifications when query results would change. |
| **Change Tracking** | Tracking changes to table data. |
| **Sparse Columns** | Optimized storage for columns with many NULLs. |
| **Column Set** | XML representation of all sparse columns. |

## Error Handling

| Term | Description |
|------|-------------|
| **SQLState** | 5-character ANSI SQL error code. |
| **Error Number** | SQL Server-specific error number. |
| **Severity** | Error severity level (0-25). |
| **State** | Additional error state information. |
| **Line Number** | Line in batch where error occurred. |
| **Procedure** | Stored procedure name if applicable. |

## Logging & Diagnostics

| Term | Description |
|------|-------------|
| **Activity ID** | GUID for distributed tracing across services. |
| **Client Request ID** | Unique identifier for a client request. |
| **Sequence Number** | Order of operations within an activity. |
| **java.util.logging** | Java's built-in logging framework used by the driver. |

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

## File Naming Conventions

| Pattern | Description |
|---------|-------------|
| `SQLServer*.java` | Main JDBC interface implementations |
| `Stream*.java` | TDS token stream handlers |
| `I*.java` | Interfaces (e.g., `ISQLServerConnection`) |
| `*Utils.java` / `*Util.java` | Utility classes |
| `*Test.java` | JUnit test classes |
| `*Constants.java` | Constant definitions |

## Version-Specific Terms

| Term | Description |
|------|-------------|
| **JDBC 4.2** | Java 8 compatible API (JRE 8 build profile) |
| **JDBC 4.3** | Java 9+ compatible API (JRE 11+ build profiles) |
| **TDS 7.4** | Protocol version for SQL Server 2012+ |
| **TDS 8.0** | Protocol version with strict encryption (SQL Server 2022+) |
