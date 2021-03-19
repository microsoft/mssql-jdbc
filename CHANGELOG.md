# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)

## [9.2.1] HotFix & Stable Release
### Fixed issues
- Fixed an issue with client secret being empty during ActiveDirectoryServicePrincipal authentication in Azure environment. [#1519](https://github.com/microsoft/mssql-jdbc/pull/1519)

## [9.2.0] Stable Release
### Added
- Added logic to handle multi-factor authentication timeouts during ActiveDirectoryInteractive authentication [#1488](https://github.com/microsoft/mssql-jdbc/pull/1488)

### Fixed issues
- Fixed an issue with high memory allocation during bulk copy [#1475](https://github.com/microsoft/mssql-jdbc/pull/1475)

## [9.1.1] Preview Release
### Added
- Added maxResultBuffer connection property [1431](https://github.com/microsoft/mssql-jdbc/pull/1431)
- Added support for Service Principal Authentication [1456](https://github.com/microsoft/mssql-jdbc/pull/1456)
- Added support for Azure Active Directory Interactive Authentication [1464](https://github.com/microsoft/mssql-jdbc/pull/1464)

### Changed
- Enabled useBulkCopyForBatchInsert against non Azure SQL Data Warehouse servers [#1465](https://github.com/microsoft/mssql-jdbc/pull/1465)

## [9.1.0] Preview Release
### Added
- Added support for already connected sockets when using custom socket factory [1420](https://github.com/microsoft/mssql-jdbc/pull/1420)
- Added JAVA 15 support [#1434](https://github.com/microsoft/mssql-jdbc/pull/1434)
- Added LocalDateTime and OffsetDateTime support in CallableStatement [1393](https://github.com/microsoft/mssql-jdbc/pull/1393)
- Added new endpoints to the list of trusted Azure Key Vault endpoints [#1445](https://github.com/microsoft/mssql-jdbc/pull/1445)

### Fixed issues
- Fixed an issue with column ordinal mapping not being sorted when using bulk copy [#1406](https://github.com/microsoft/mssql-jdbc/pull/1406)
- Fixed PooledConnectionTest to catch exceptions from threads [#1409](https://github.com/microsoft/mssql-jdbc/pull/1409)
- Fixed an issue with bulk copy when inserting non-unicode multibyte strings [#1421](https://github.com/microsoft/mssql-jdbc/pull/1421)
- Fixed intermittent deadlock issue in DBMetadatatest [#1423](https://github.com/microsoft/mssql-jdbc/pull/1423)
- Fixed Gradle exclude tags [#1424](https://github.com/microsoft/mssql-jdbc/pull/1424)
- Fixed an issue with SQLServerBulkCSVFileRecord ignoring empty trailing columns when using setEscapeColumnDelimitersCSV() API [#1438](https://github.com/microsoft/mssql-jdbc/pull/1438)

### Changed
- Changed visibility of SQLServerBulkBatchInsertRecord to package-private [#1408](https://github.com/microsoft/mssql-jdbc/pull/1408)
- Upgraded to the latest Azure Key Vault libraries [#1413](https://github.com/microsoft/mssql-jdbc/pull/1413)
- Updated API version when using MSI authentication [#1418](https://github.com/microsoft/mssql-jdbc/pull/1418)
- Enabled Azure Active Directory integrated authentication tests on non-Windows clients [#1425](https://github.com/microsoft/mssql-jdbc/pull/1425)
- Updated the driver to remove clientKeyPassword from memory [#1428](https://github.com/microsoft/mssql-jdbc/pull/1428)
- Updated SQLServerPreparedStatement.getMetaData() to retain exception details [#1430](https://github.com/microsoft/mssql-jdbc/pull/1430)
- Made ADALGetAccessTokenForWindowsIntegrated thread-safe [#1441](https://github.com/microsoft/mssql-jdbc/pull/1441)

## [8.4.1] HotFix & Stable Release
### Fixed issues
- Fixed issue with SQLServerConnectionPoolProxy not being compatible with `delayLoadingLobs`. [#1403](https://github.com/microsoft/mssql-jdbc/pull/1403)
- Fixed a potential `NullPointerException` issue with `delayLoadingLobs`. [#1403](https://github.com/microsoft/mssql-jdbc/pull/1403)
- Fixed issue with decrypting column encryption keys using Windows Certificate Store.

## [8.4.0] Stable Release
### Added
- Added support for sensitivity ranking when using SQL Data Discovery and Classification [#1338](https://github.com/microsoft/mssql-jdbc/pull/1338) [#1373](https://github.com/microsoft/mssql-jdbc/pull/1373)
- Added SQLServerDatabaseMetaData.getDatabaseCompatibilityLevel() API to return the database compatibility level [#1345](https://github.com/microsoft/mssql-jdbc/pull/1345)
- Added support for Azure SQL DNS Caching [#1357](https://github.com/microsoft/mssql-jdbc/pull/1357)

### Fixed issues
- Fixed an issue with DatabaseMetaData.getColumns() intermittently returning table column descriptions in incorrect order [#1348](https://github.com/microsoft/mssql-jdbc/pull/1348)
- Fixed an issue with spatial datatypes casting error when Always Encrypted is enabled [#1353](https://github.com/microsoft/mssql-jdbc/pull/1353)
- Fixed an issue with DatabaseMetaData.getColumns() not returning correct type for IS_AUTOINCREMENT and IS_GENERATEDCOLUMN against Azure Data Warehouse [#1356](https://github.com/microsoft/mssql-jdbc/pull/1356)
- Fixed an issue with Geography.STAsBinary() and Geometry.STAsBinary() returning WKB format instead of CLR format [#1364](https://github.com/microsoft/mssql-jdbc/pull/1364)
- Fixed an issue with allowing non-MSSQL ResultSets to bulk copy DateTimeOffset [#1365](https://github.com/microsoft/mssql-jdbc/pull/1365)
- Fixed issues identified by SonarQube [#1369](https://github.com/microsoft/mssql-jdbc/pull/1369)
- Fixed an issue with batch insertion failing when Always Encrypted is enabled [#1378](https://github.com/microsoft/mssql-jdbc/pull/1378)

### Changed
- Updated Gradle build file to fix Azure pipelines [#1359](https://github.com/microsoft/mssql-jdbc/pull/1359)
- Added database name to Always Encrypted enclave caching key [#1388](https://github.com/microsoft/mssql-jdbc/pull/1388)
- Added functionality to validate both certificate beginning and expiration dates when creating encrypted connection [#1394](https://github.com/microsoft/mssql-jdbc/pull/1394)

## [8.3.1] Preview Release
### Added
- Added delayed durability option to SQLServerConnection.commit() [#1310](https://github.com/microsoft/mssql-jdbc/pull/1310)
- Introduced SQLServerBulkCSVFileRecord.setEscapeColumnDelimitersCSV() to escape delimiters and double quotes when using bulk copy to load from CSV files [#1312](https://github.com/microsoft/mssql-jdbc/pull/1312)
- Added certificate expiry validation when using Always Encrypted with secure enclaves feature [#1321](https://github.com/microsoft/mssql-jdbc/pull/1321)
- Added SQL State to Exception when connection is closed [#1326](https://github.com/microsoft/mssql-jdbc/pull/1326)
- Introduced extended bulk copy support against Azure Data Warehouse [#1331](https://github.com/microsoft/mssql-jdbc/pull/1331)
- Introduced 'delayLoadingLobs' connection property to provide backward compatibility when streaming LOBs [#1336](https://github.com/microsoft/mssql-jdbc/pull/1336)

### Fixed issues
- Fixed an issue with MSI authentication failing due to expiry date format mismatch [#1308](https://github.com/microsoft/mssql-jdbc/pull/1308)
- Fixed an issue with streams not getting closed when using Always Encrypted with secure enclaves feature [#1315](https://github.com/microsoft/mssql-jdbc/pull/1315)
- Fixed an issue with retrieving SQL VARIANT as its underlying type [#1320](https://github.com/microsoft/mssql-jdbc/pull/1320)
- Fixed issues with the driver not being JAVA 8 compliant [#1328](https://github.com/microsoft/mssql-jdbc/pull/1328)
- Fixed an issue with PreparedStatement when inserting large spatial data types [#1337](https://github.com/microsoft/mssql-jdbc/pull/1337)

### Changed
- Updated driver and test dependencies [#1294](https://github.com/microsoft/mssql-jdbc/pull/1294), [#1313](https://github.com/microsoft/mssql-jdbc/pull/1313)
- Improved exception message when connecting to redirection-enabled Azure server [#1311](https://github.com/microsoft/mssql-jdbc/pull/1311)
- Improved performance when parsing connection string [#1317](https://github.com/microsoft/mssql-jdbc/pull/1317)
- Updated the driver to throw a warning when TLS version lower than 1.2 is negotiated [#1322](https://github.com/microsoft/mssql-jdbc/pull/1322)
- Removed unused code [#1330](https://github.com/microsoft/mssql-jdbc/pull/1330)

## [8.3.0] Preview Release
### Added
- Added connection properties to specify custom SocketFactory [#1217](https://github.com/Microsoft/mssql-jdbc/pull/1217)
- Added support for Client Certificate Authentication [#1284](https://github.com/microsoft/mssql-jdbc/pull/1284)
- Added support for JAVA 14 [#1290](https://github.com/microsoft/mssql-jdbc/pull/1290)
- Added support for authentication to Azure Key Vault using Managed Identity [#1286](https://github.com/microsoft/mssql-jdbc/pull/1286)

### Fixed issues
- Fixed an issue with escaping curly brackets in connection string [#1251](https://github.com/microsoft/mssql-jdbc/pull/1251)
- Fixed a warning when retrieving Operating System information from SQL Server Linux when using distributed transactions [#1279](https://github.com/microsoft/mssql-jdbc/pull/1279)

### Changed
- Updated Gradle dependencies [#1244](https://github.com/microsoft/mssql-jdbc/pull/1244)
- Updated SQLServerPreparedStatement.setObject() to retrieve TVP name from SQLServerDataTable [#1282](https://github.com/microsoft/mssql-jdbc/pull/1282)

## [8.2.2] HotFix & Stable Release
### Added
- Added an option to configure the list of trusted Azure Key Vault endpoints [#1285](https://github.com/microsoft/mssql-jdbc/pull/1285)

## [8.2.1] HotFix & Stable Release
### Fixed issues
- Fixed a potential NullPointerException issue when retrieving data as java.time.LocalTime or java.time.LocalDate type with SQLServerResultSet.getObject() [#1250](https://github.com/microsoft/mssql-jdbc/pull/1250) 

## [8.2.0] Stable Release
### Added
- Added new tests for Always Encrypted with secure enclaves feature [#1166](https://github.com/microsoft/mssql-jdbc/pull/1166)
- Added backwards compatibility for calling SQLServerClob.length() on nvarchar columns [#1214](https://github.com/microsoft/mssql-jdbc/pull/1214)

### Fixed issues
- Fixed an issue with potentially creating more enclave sessions than needed [#1208](https://github.com/microsoft/mssql-jdbc/pull/1208)
- Fixed an issue with InputStream closing when calling SQLServerBlob.length() on an `image` column [#1214](https://github.com/microsoft/mssql-jdbc/pull/1214)
- Fixed a potential performance issue created from trailing spaces in PreparedStatement queries [#1215](https://github.com/microsoft/mssql-jdbc/pull/1215)
- Fixed an issue with native Always Encrypted calls not being synchronized [#1220](https://github.com/microsoft/mssql-jdbc/pull/1220)
- Fixed issues identified by SonarQube [#1226](https://github.com/microsoft/mssql-jdbc/pull/1226), Semmle [#1234](https://github.com/microsoft/mssql-jdbc/pull/1234), and CredScanner [#1237](https://github.com/microsoft/mssql-jdbc/pull/1237)

### Changed
- Updated com.microsoft.rest:client-runtime to its latest version [#1235](https://github.com/microsoft/mssql-jdbc/pull/1235)
- Removed shaded jars [#1239](https://github.com/microsoft/mssql-jdbc/pull/1239)

## [8.1.1] Preview Release
### Added
- Added more tests to improve code coverage for Always Encrypted with secure enclaves feature [#1186](https://github.com/microsoft/mssql-jdbc/pull/1186)
- Added certificate and enclave session caching for Always Encrypted with secure enclaves feature [#1189](https://github.com/microsoft/mssql-jdbc/pull/1189)

### Fixed issues
- Fixed a potential NullPointerException in SQLServerDataColumn.equals() [#1168](https://github.com/microsoft/mssql-jdbc/pull/1168)
- Fixed an issue with BulkCopy when source is unicode char/varchar and destination is nchar/nvarchar [#1193](https://github.com/microsoft/mssql-jdbc/pull/1193)
- Fixed an issue with SQLServerDatabaseMetaData.getColumns() only returning the first column against Azure SQL Data Warehouse [#1197](https://github.com/microsoft/mssql-jdbc/pull/1197)
- Fixed an issue with SQLServerDatabaseMetaData.getImportedKeys() failing against Azure SQL Data Warehouse [#1205](https://github.com/microsoft/mssql-jdbc/pull/1205)

### Changed
- Made internal model public for SQLServerSpatialDatatype class [#1169](https://github.com/microsoft/mssql-jdbc/pull/1169)
- Updated ISQLServerBulkData APIs to throw SQLException instead of SQLServerException [#1187](https://github.com/microsoft/mssql-jdbc/pull/1187)
- Changed SQLServerConnection.state to volatile [#1194](https://github.com/microsoft/mssql-jdbc/pull/1194)
- Optimized temporal datatype getter methods by replacing Calendar with LocalDatetime [#1200](https://github.com/microsoft/mssql-jdbc/pull/1200)
- Updated driver and test dependencies [#1203](https://github.com/microsoft/mssql-jdbc/pull/1203)

## [8.1.0] Preview Release
### Added
- Added ISQLServerBulkData to remove implementation details from ISQLServerBulkRecord [#1099](https://github.com/microsoft/mssql-jdbc/pull/1099)
- Added support for Azure national clouds when using Azure Key Vault [#1130](https://github.com/microsoft/mssql-jdbc/pull/1130)
- Implemented hashCode() and equals() APIs for SQLServerDataTable and SQLServerDataColumn [#1146](https://github.com/Microsoft/mssql-jdbc/pull/1146)
- Added support for JAVA 13 [#1151](https://github.com/Microsoft/mssql-jdbc/pull/1151)
- Added support for Always Encrypted with Secure Enclaves [#1155](https://github.com/Microsoft/mssql-jdbc/pull/1155)

### Fixed Issues
- Fixed Geography.STAsBinary() returning null for a single point [#1074](https://github.com/microsoft/mssql-jdbc/pull/1074)
- Fixed DatabaseMetaData.getImportedKeys() returning duplicate rows [#1092](https://github.com/microsoft/mssql-jdbc/pull/1092)
- Fixed issue with truststore password being removed too early for XA connections [#1133](https://github.com/microsoft/mssql-jdbc/pull/1133)
- Fixed issue with SQLServerDatabaseMetada.getColumns() not escaping wildcard characters [#1138](https://github.com/microsoft/mssql-jdbc/pull/1138)
- Removed extra spaces in SQLServerDatabaseMetaData.getNumericFunctions() and SQLServerDatabaseMetaData.getStringFunctions() return values [#1117](https://github.com/microsoft/mssql-jdbc/pull/1117)

### Changed
- Improved performance of column name lookups [#1066](https://github.com/microsoft/mssql-jdbc/pull/1066)
- Test improvements [#1100](https://github.com/microsoft/mssql-jdbc/pull/1100)
- Updated issue templates [#1148](https://github.com/microsoft/mssql-jdbc/pull/1148)
- Improved performance of CallableStatement and ParameterMetaData when using procedure names that contain wildcard characters [#1149](https://github.com/microsoft/mssql-jdbc/pull/1149)
- Updated CI to use SQL Server 2012 instead of 2008R2 [#1153](https://github.com/microsoft/mssql-jdbc/pull/1153)

## [7.4.1] HotFix & Stable Release
### Fixed Issues
- Reverted [#1025](https://github.com/Microsoft/mssql-jdbc/pull/1025) as it contains breaking changes. This removes `hashCode()` and `equals()` APIs from `SQLServerDataTable` and `SQLServerDataColumn`.

## [7.4.0] Stable Release
### Fixed Issues
- Fixed issues reported by Static Analysis Tool - SonarQube [#1077](https://github.com/Microsoft/mssql-jdbc/pull/1077) [#1103](https://github.com/Microsoft/mssql-jdbc/pull/1103)
- Fixed issues with array bound checking in 'useFmtOnly' implementation [#1094](https://github.com/Microsoft/mssql-jdbc/pull/1094)
### Changed
- Performance improvements [#1075](https://github.com/Microsoft/mssql-jdbc/pull/1075)
- Changed NTLM Authentication implementation to not store password in plain text [#1095](https://github.com/Microsoft/mssql-jdbc/pull/1095) [#1108](https://github.com/Microsoft/mssql-jdbc/pull/1108)
- Updated the Maven dependency of 'Java Client Runtime for AutoRest' to 1.6.10 version of the library [#1097](https://github.com/Microsoft/mssql-jdbc/pull/1097)
- Changed NTLM Authentication error strings [#1105](https://github.com/Microsoft/mssql-jdbc/pull/1105)

## [7.3.1] Preview Release
### Added
- Added support for NTLM Authentication [#998](https://github.com/Microsoft/mssql-jdbc/pull/998)
- Added new connection property 'useFmtOnly' to retrieve parameter metadata [#1044](https://github.com/Microsoft/mssql-jdbc/pull/1044)
- Added support for JDK 12 with an additional "jre12" JAR [#1050](https://github.com/Microsoft/mssql-jdbc/pull/1050)
- Added 'keyVaultProviderClientId' and 'keyVaultProviderClientKey' connection properties to enhance Always Encrypted usability [#902](https://github.com/Microsoft/mssql-jdbc/pull/902)
- Implemented `hashCode()` and `equals()` APIs for `SQLServerDataTable` and `SQLServerDataColumn` [#1025](https://github.com/Microsoft/mssql-jdbc/pull/1025)
- Added Maven Shade plugin configuration to package the driver jars in uber-jars [#1043](https://github.com/Microsoft/mssql-jdbc/pull/1043) [#1078](https://github.com/Microsoft/mssql-jdbc/pull/1078) [#1081](https://github.com/Microsoft/mssql-jdbc/pull/1081)

### Fixed Issues
- Fixed `DatabaseMetadata.getColumns()` API to return `ResultSet` as per JDBC 4.3 Specifications [#1016](https://github.com/Microsoft/mssql-jdbc/pull/1016)
- Fixed issue with invalid Spatial data types by marking them valid by default [#1035](https://github.com/microsoft/mssql-jdbc/pull/1035)
- Fixed issues with Login Timeout not getting applied appropriately [#1049](https://github.com/Microsoft/mssql-jdbc/pull/1049)
- Fixed `SharedTimer` implementation to use class level lock for thread safety [#1046](https://github.com/Microsoft/mssql-jdbc/pull/1046)
- Fixed issues with `SQLServerDatabaseMetadata.getMaxConnections()` API query [#1009](https://github.com/Microsoft/mssql-jdbc/pull/1009)
- Fixed issues with next `ResultSet` being consumed when reading warnings [#991](https://github.com/Microsoft/mssql-jdbc/pull/991)
- Fixed exception handling in `SQLServerPreparedStatement` to make it consistent with `SQLServerStatement` [#1003](https://github.com/Microsoft/mssql-jdbc/pull/1003)
- Fixed misleading exception message in `SQLServerCallableStatement` implementation [#1064](https://github.com/Microsoft/mssql-jdbc/pull/1064)
- JUnit Test fixes and improvements [#994](https://github.com/Microsoft/mssql-jdbc/pull/994) [#1004](https://github.com/Microsoft/mssql-jdbc/pull/1004) [#1005](https://github.com/Microsoft/mssql-jdbc/pull/1005) [#1006](https://github.com/Microsoft/mssql-jdbc/pull/1006) [#1008](https://github.com/Microsoft/mssql-jdbc/pull/1008) [#1015](https://github.com/Microsoft/mssql-jdbc/pull/1015) [#1017](https://github.com/Microsoft/mssql-jdbc/pull/1017) [#1019](https://github.com/Microsoft/mssql-jdbc/pull/1019) [#1027](https://github.com/Microsoft/mssql-jdbc/pull/1027) [#1032](https://github.com/Microsoft/mssql-jdbc/pull/1032) [#1034](https://github.com/Microsoft/mssql-jdbc/pull/1034) [#1036](https://github.com/Microsoft/mssql-jdbc/pull/1036) [#1041](https://github.com/Microsoft/mssql-jdbc/pull/1041) [#1047](https://github.com/Microsoft/mssql-jdbc/pull/1047) [#1060](https://github.com/Microsoft/mssql-jdbc/pull/1060)

### Changed
- Improved performance of driver by continuously cleaning up `ActivityIds` stored in internal Map [#1020](https://github.com/Microsoft/mssql-jdbc/pull/1020)
- Improved performance by removing `Enum.values()` calls to avoid unnecessary array cloning [#1065](https://github.com/Microsoft/mssql-jdbc/pull/1065)
- Improved performance of `SQLServerDataTable.internalAddRow()` function [#990](https://github.com/Microsoft/mssql-jdbc/pull/990)

## [7.3.0] Preview Release
### Added
- Added support in SQLServerBulkCopy to allow Pooled/XA Connection instances during object creation [#968](https://github.com/Microsoft/mssql-jdbc/pull/968)
- Added support for FLOAT data type for bulk copy operation when using RowSet [#986](https://github.com/Microsoft/mssql-jdbc/pull/986)

### Fixed Issues
- Fixed a possible Statement leak in SQLServerConnection.isValid() API [#955](https://github.com/Microsoft/mssql-jdbc/pull/955)
- Fixed rounding behavior when inserting datetime values into SQL Server version 2016 and later [#962](https://github.com/Microsoft/mssql-jdbc/pull/962)
- Fixed SQLServerConnection.abort() API behavior to clear resources consistently [#983](https://github.com/Microsoft/mssql-jdbc/pull/983)
- Fixed SQLServerConnection documentation [#984](https://github.com/Microsoft/mssql-jdbc/pull/984)
- Fixed SQL Exception Error State length to respect SQLSTATE Standards [#977](https://github.com/Microsoft/mssql-jdbc/pull/977)

### Changed
- Refactored SELECT_METHOD in SQLServerConnection to not fetch the same connection property twice [#987](https://github.com/Microsoft/mssql-jdbc/pull/987)
- Improved SQLServerParameterMetadata API implementations and code coverage [#973](https://github.com/Microsoft/mssql-jdbc/pull/973)

## [7.2.1] HotFix & Stable Release
### Fixed Issues
- Fixed parsing issues with certain parameterized queries [#950](https://github.com/Microsoft/mssql-jdbc/pull/950)

## [7.2.0] Stable Release
### Added
- Added Azure Pipelines CI configuration to trigger Windows Client testing with SQL Server 2017 and SQL Server 2008 R2 [#940](https://github.com/Microsoft/mssql-jdbc/pull/940)

### Fixed Issues
- Fixed issue with ThreadPoolExecutor thread preventing JVM from exiting [#944](https://github.com/Microsoft/mssql-jdbc/pull/944)
- Fixed issues reported by Static Analysis Tool - SonarQube [#928](https://github.com/Microsoft/mssql-jdbc/pull/928) [#930](https://github.com/Microsoft/mssql-jdbc/pull/930) [#933](https://github.com/Microsoft/mssql-jdbc/pull/933)
- Fixed Timestamp comparison with "Thai" locale in DataTypesTest [#941](https://github.com/Microsoft/mssql-jdbc/pull/941)

### Changed
- Changed timeout request handling implementation to use SharedTimer [#920](https://github.com/Microsoft/mssql-jdbc/pull/920)
- Removed Appveyor CI and updated Travis CI configuration [#940](https://github.com/Microsoft/mssql-jdbc/pull/940)

## [7.1.4] Preview Release
### Added
- Added APIs for DataSourceFactory and OSGI Framework [#700](https://github.com/Microsoft/mssql-jdbc/pull/700)
- Added support for OffsetDateTime to be passed as 'type' in ResultSet.getObject() [#830](https://github.com/Microsoft/mssql-jdbc/pull/830)
- Added support for Active Directory MSI Authentication [#838](https://github.com/Microsoft/mssql-jdbc/pull/838)
- Added more datatype tests to JUnit test suite [#878](https://github.com/Microsoft/mssql-jdbc/pull/878) [#916](https://github.com/Microsoft/mssql-jdbc/pull/916)
- Added an option to perform JUnit testing against Azure Data Warehouse [#903](https://github.com/Microsoft/mssql-jdbc/pull/903)
- Added new APIs to retrieve SQL Server error information received with SQLServerException [#905](https://github.com/Microsoft/mssql-jdbc/pull/905)

### Fixed Issues
- Fixed issue with java.time.OffsetDateTime value sent to the server being affected by the default timezone [#831](https://github.com/Microsoft/mssql-jdbc/pull/831)
- Fixed SSL certificate validation to respect wildcards [#836](https://github.com/Microsoft/mssql-jdbc/pull/836)
- Fixed Bulk Copy for batch insert operation to not error out against specific datatypes [#912](https://github.com/Microsoft/mssql-jdbc/pull/912)

### Changed
- Fixed synchronization on a non-final field [#860](https://github.com/Microsoft/mssql-jdbc/pull/860)
- Removed hardcoded error messages from test file [#904](https://github.com/Microsoft/mssql-jdbc/pull/904)
- Updated Issue and Pull Request templates [#906](https://github.com/Microsoft/mssql-jdbc/pull/906)
- Updated JUnit tests by closing all resources consistently and updated Maven dependency versions to latest [#919](https://github.com/Microsoft/mssql-jdbc/pull/919)

## [7.1.3] Preview Release
### Added
- Added a new SQLServerMetaData constructor for string values of length greater than 4000 [#876](https://github.com/Microsoft/mssql-jdbc/pull/876)

### Fixed Issues
- Fixed an issue with Geography.point() having coordinates reversed [#853](https://github.com/Microsoft/mssql-jdbc/pull/853)
- Fixed intermittent test failures [#854](https://github.com/Microsoft/mssql-jdbc/pull/854)  [#862](https://github.com/Microsoft/mssql-jdbc/pull/862) [#888](https://github.com/Microsoft/mssql-jdbc/pull/888)
- Fixed an issue with setAutoCommit() leaving a transaction open when running against Azure SQL Data Warehouse [#881](https://github.com/Microsoft/mssql-jdbc/pull/881)

### Changed
- Changed query timeout logic to use a single thread [#842](https://github.com/Microsoft/mssql-jdbc/pull/842)
- Code cleanup [#857](https://github.com/Microsoft/mssql-jdbc/pull/857) [#873](https://github.com/Microsoft/mssql-jdbc/pull/873)
- Removed populating Lobs when calling ResultSet.wasNull() [#875](https://github.com/Microsoft/mssql-jdbc/pull/875)
- Improved retry logic for intermittent TLS1.2 issue when establishing a connection [#882](https://github.com/Microsoft/mssql-jdbc/pull/882)

## [7.1.2] Preview Release
### Added
- Added support for JDK 11 [#824](https://github.com/Microsoft/mssql-jdbc/pull/824) [#837](https://github.com/Microsoft/mssql-jdbc/pull/837) [#807](https://github.com/Microsoft/mssql-jdbc/pull/807)
- Updated SQL keywords in DatabaseMetaData [#829](https://github.com/Microsoft/mssql-jdbc/pull/829)
- Improvements in DatabaseMetadata to prevent Statement leaks and enhance Statement caching [#806](https://github.com/Microsoft/mssql-jdbc/pull/806)

### Fixed Issues
- Fixed slf4j warning message in tests [#841](https://github.com/Microsoft/mssql-jdbc/pull/841)
- Fixed potential NullPointerException in logException() [#844](https://github.com/Microsoft/mssql-jdbc/pull/844)
- Fixed intermittent failures in JUnit - LobsTest [#827](https://github.com/Microsoft/mssql-jdbc/pull/827)
- Fixed useBulkCopyForBatchInserts API to respect Statement timeout value [#817](https://github.com/Microsoft/mssql-jdbc/pull/817)

### Changed
- Updated JUnit tests to remove hard-coded names [#809](https://github.com/Microsoft/mssql-jdbc/pull/809)
- Removed illegal reflection access in Kerberos Authentication [#839](https://github.com/Microsoft/mssql-jdbc/pull/839)
- Enabled non-running JUnit tests [#847](https://github.com/Microsoft/mssql-jdbc/pull/847)
- Updated Clobs to use StandardCharsets.US_ASCII instead of hard-coded string [#855](https://github.com/Microsoft/mssql-jdbc/pull/855)
- Code cleanup [#821](https://github.com/Microsoft/mssql-jdbc/pull/821) [#825](https://github.com/Microsoft/mssql-jdbc/pull/825)

## [7.1.1] Preview Release
### Added
- Added streaming capabilities for Clob.getAsciiStream() [#799](https://github.com/Microsoft/mssql-jdbc/pull/799)

### Fixed Issues
- Fixed a bug where calling length() after obtaining a stream would close the stream for Clobs/NClobs [#799](https://github.com/Microsoft/mssql-jdbc/pull/799)
- Fixed Clob/NClob encoding issues [#799](https://github.com/Microsoft/mssql-jdbc/pull/799)
- Fixed issues in Bulk Copy exception handling [#801](https://github.com/Microsoft/mssql-jdbc/pull/801)
- Fixed closeable resource leaks in JUnit tests [#797](https://github.com/Microsoft/mssql-jdbc/pull/797)
- Fixed issues with apostrophe being passed in table name [#780](https://github.com/Microsoft/mssql-jdbc/pull/780)
- Fixed statement leaks and improved exception handling in SQLServerParameterMetadata [#780](https://github.com/Microsoft/mssql-jdbc/pull/780)

### Changed
- Changed error message to be thrown when data out of range for DECIMAL/NUMERIC types [#796](https://github.com/Microsoft/mssql-jdbc/pull/796)

## [7.1.0] Preview Release
### Added
- Added support for LocalDate, LocalTime and LocalDateTime to be passed as 'type' in ResultSet.getObject() [#749](https://github.com/Microsoft/mssql-jdbc/pull/749)
- Added support to read SQL Warnings after ResultSet is read completely [#785](https://github.com/Microsoft/mssql-jdbc/pull/785)

### Fixed Issues
- Fixed Javadoc warnings and removed obselete HTML tags from Javadocs [#786](https://github.com/Microsoft/mssql-jdbc/pull/786)
- Fixed random JUnit failures in framework tests [#762](https://github.com/Microsoft/mssql-jdbc/pull/762)

### Changed
- Improved performance of readLong() function by unrolling loop and using bitwise operators instead of additions [#763](https://github.com/Microsoft/mssql-jdbc/pull/763)
- Removed logging logic which caused performance degradation in AE [#773](https://github.com/Microsoft/mssql-jdbc/pull/773)

## [7.0.0] Stable Release
### Added
- Added 'Automatic-Module-Name' manifest entry to jre10 Jar, allowing JDK 10 users to access driver module 'com.microsoft.sqlserver.jdbc' [#732](https://github.com/Microsoft/mssql-jdbc/pull/732)
- Added setUseBulkCopyForBatchInsert() to request boundary declaration APIs [#739](https://github.com/Microsoft/mssql-jdbc/pull/739)
- Added new test for validation of supported public APIs in request boundary declaration APIs [#746](https://github.com/Microsoft/mssql-jdbc/pull/746)

### Fixed Issues
- Fixed policheck issue with a keyword [#745](https://github.com/Microsoft/mssql-jdbc/pull/745)
- Fixed issues reported by static analysis tools (SonarQube, Fortify) [#747](https://github.com/Microsoft/mssql-jdbc/pull/747)

### Changed
- Reformatted code and updated mssql-jdbc-formatter [#742](https://github.com/Microsoft/mssql-jdbc/pull/742)
- Changed Sha1HashKey to CityHash128Key for generating PreparedStatement handle and metadata cache keys [#717](https://github.com/Microsoft/mssql-jdbc/pull/717)
- Changed order of logic for checking the condition for using Bulk Copy API [#736](https://github.com/Microsoft/mssql-jdbc/pull/736)
- Changed collation name in UTF8SupportTest [#741](https://github.com/Microsoft/mssql-jdbc/pull/741)
- Changed scope of unwanted Public APIs [#757](https://github.com/Microsoft/mssql-jdbc/pull/757)
- Changed behavior of Bulk Copy API for batch inserts to disallow non-parameterized queries [#756](https://github.com/Microsoft/mssql-jdbc/pull/756)
- Changed APIs and JavaDocs for Spatial Datatypes [#752](https://github.com/Microsoft/mssql-jdbc/pull/752)
- Improved Javadoc comments in driver [#754](https://github.com/Microsoft/mssql-jdbc/pull/754), [#760](https://github.com/Microsoft/mssql-jdbc/pull/760)

## [6.5.4] Preview Release
### Added
- Added new connection property "useBulkCopyForBatchInsert" to enable Bulk Copy API support for batch insert operation [#686](https://github.com/Microsoft/mssql-jdbc/pull/686)
- Added implementation for Java 9 introduced Boundary methods APIs on Connection interface [#708](https://github.com/Microsoft/mssql-jdbc/pull/708)
- Added support for "Data Classification Specifications" on fetched resultsets [#709](https://github.com/Microsoft/mssql-jdbc/pull/709)
- Added support for UTF-8 feature extension [#722](https://github.com/Microsoft/mssql-jdbc/pull/722)

### Fixed Issues
- Fixed issue with escaping catalog name when retrieving from database metadata [#718](https://github.com/Microsoft/mssql-jdbc/pull/718)
- Fixed issue with tests requiring additional dependencies [#729](https://github.com/Microsoft/mssql-jdbc/pull/729)

### Changed
- Made driver default compliant to JDBC 4.2 specifications [#711](https://github.com/Microsoft/mssql-jdbc/pull/711)
- Updated ADAL4J dependency version to 1.6.0 [#711](https://github.com/Microsoft/mssql-jdbc/pull/711)
- Cleaned up socket handling implementation to generalize functionality for different JVMs and simplified the logic for single address case [#663](https://github.com/Microsoft/mssql-jdbc/pull/663)

## [6.5.3] Preview Release
### Added
- Added removed constructor back to AKV Provider which supports authentication with a customized method to fetch accessToken [#675](https://github.com/Microsoft/mssql-jdbc/pull/675)
- Added support for JDK 10 for both Maven and Gradle [#691](https://github.com/Microsoft/mssql-jdbc/pull/691)
- Added a resource bundle to handle JUnit error strings [#698](https://github.com/Microsoft/mssql-jdbc/pull/698)

### Fixed Issues
- Fixed the driver disposing user created credentials when using Kerberos Constrained Delegation [#636](https://github.com/Microsoft/mssql-jdbc/pull/636)
- Fixed an issue with HostnameInCertificate when redirected while connected to Azure [#644](https://github.com/Microsoft/mssql-jdbc/pull/644)
- Fixed an intermittent issue with Prepared Statement handle not found [#648](https://github.com/Microsoft/mssql-jdbc/pull/648)
- Fixed a conflict with JDBC Compliance where the driver was returning marked columns as SS_IS_COMPUTED instead of IS_GENERATED [#695](https://github.com/Microsoft/mssql-jdbc/pull/695)
- Fixed maven build warnings and deprecated Java API warnings [#701](https://github.com/Microsoft/mssql-jdbc/pull/701)
- Fixed some Javadoc related warnings [#702](https://github.com/Microsoft/mssql-jdbc/pull/702)

## [6.5.2] Preview Release
### Added
- Added new connection property "cancelQueryTimeout" to cancel QueryTimeout on Connection and Statement [#674](https://github.com/Microsoft/mssql-jdbc/pull/674)

### Fixed Issues
- Improved performance degradation while maintaining JDBC compliance with results from sp_fkeys [#677](https://github.com/Microsoft/mssql-jdbc/pull/677)
- Fixed an issue where ResultSetMetaData instances created by a ResultSet that has been closed were not persisting [#685](https://github.com/Microsoft/mssql-jdbc/pull/685)
- Fixed an issue with PreparedStatement.setBigDecimal when no scale is passed [#684](https://github.com/Microsoft/mssql-jdbc/pull/684)
- Fixed an issue with Clobs/NClobs not persisting after ResultSet/Connection closes [#682](https://github.com/Microsoft/mssql-jdbc/pull/682)

### Changed
- Updated the samples to be usable with Eclipse directly, and updated the driver version used by the samples to 6.4.0.jre9 [#679](https://github.com/Microsoft/mssql-jdbc/pull/679)
- Updated Gradle script for building JDBC Driver [#689](https://github.com/Microsoft/mssql-jdbc/pull/689)
- Updated Maven dependencies for test suite [#676](https://github.com/Microsoft/mssql-jdbc/pull/676)
- Updated multiple Maven dependency and plugin versions [#688](https://github.com/Microsoft/mssql-jdbc/pull/688)

## [6.5.1] Preview Release
### Added
- Test cases for Date, Time, and Datetime2 data types [#558](https://github.com/Microsoft/mssql-jdbc/pull/558)

### Fixed Issues
- Fixed an issue where ResultSetMetadata returned incorrect columnType for Geometry and Geography data types [#657](https://github.com/Microsoft/mssql-jdbc/pull/657)
- Fixed server side CPU Affinity problems caused by uneven connection distribution across NUMA Nodes when multiSubnetFailover is true [#662](https://github.com/Microsoft/mssql-jdbc/pull/662)
- Fixed an issue where Driver wasn't parsing TDS Packets completely to capture exceptions raised inside executed stored procedures [#664](https://github.com/Microsoft/mssql-jdbc/pull/664)
- Fixed an issue where driver throws exception when using setMaxRows() followed by query execution when SHOWPLAN_TEXT is ON [#666](https://github.com/Microsoft/mssql-jdbc/pull/666)

### Changed
- Removed unused imports which forced users to import the ADAL4J library [#652](https://github.com/Microsoft/mssql-jdbc/pull/652)

## [6.5.0] Preview Release
### Added
- Support for spatial datatypes [#642](https://github.com/Microsoft/mssql-jdbc/pull/642)

### Fixed Issues
- Fixed blobs becoming unavailable when the Result Set cursor moves or the Result Set closes [#595](https://github.com/Microsoft/mssql-jdbc/pull/595)
- Fixed an issue when attempting to insert an empty or null value into an encrypted column [#632](https://github.com/Microsoft/mssql-jdbc/pull/632)
- Fixed a misleading error message thrown by the driver when a user doesn't have execute permissions [#635](https://github.com/Microsoft/mssql-jdbc/pull/635)
- Fixed statements throwing SQLServerException instead of java.sql.SQLTimeoutException when the query times out [#641](https://github.com/Microsoft/mssql-jdbc/pull/641)

### Changed
- Unit tests now use SQLException in most cases instead of SQLServerException.

## [6.4.0] Stable Release
### Added
- Support added for AAD Integrated Authentication with ADAL4J on Windows/Linux/Mac OS [#603](https://github.com/Microsoft/mssql-jdbc/pull/603) 
- Enable Recover after MSDTC is restarted [#581](https://github.com/Microsoft/mssql-jdbc/pull/581)
- Added  Version Update configuration rules to project [#541](https://github.com/Microsoft/mssql-jdbc/pull/541)
- JDK 9 Compatibility + JDBC 4.3 API support added to the driver [#601 (https://github.com/Microsoft/mssql-jdbc/pull/601)

### Fixed Issues
- Re-introduced Retry Logic for Prepared Statement Caching implementation and remove detect change context function [#618](https://github.com/Microsoft/mssql-jdbc/pull/618) and [#620](https://github.com/Microsoft/mssql-jdbc/pull/620)
- Fixes for SonarQube Reported issues [#599](https://github.com/Microsoft/mssql-jdbc/pull/599)
- Fixes for Random Assertion Errors [#597](https://github.com/Microsoft/mssql-jdbc/pull/597)

### Changed
- Updated Appveyor to use JDK9 building driver and running tests [#619](https://github.com/Microsoft/mssql-jdbc/pull/619)
- JDK 7 compilation support removed from the driver [#601](https://github.com/Microsoft/mssql-jdbc/pull/601)

## [6.3.6] Preview Release
### Added
- Added support for using database name as part of the key for handle cache [#561](https://github.com/Microsoft/mssql-jdbc/pull/561)
- Updated ADAL4J version to 1.3.0 and also added it into README file [#564](https://github.com/Microsoft/mssql-jdbc/pull/564)

### Fixed Issues 
- Fixed issues with static loggers being set by every constructor invocation  [#563](https://github.com/Microsoft/mssql-jdbc/pull/563)

## [6.3.5] Preview Release
### Added
- Added handle for Account Locked Exception 18486 during login in SQLServerConnection [#522](https://github.com/Microsoft/mssql-jdbc/pull/522)

### Fixed Issues 
- Fixed the issues with Prepared Statement Metadata Caching implementation [#543](https://github.com/Microsoft/mssql-jdbc/pull/543)
- Fixed issues with static logger member in abstract class 'SQLServerClobBase' [#537](https://github.com/Microsoft/mssql-jdbc/pull/537)

## [6.3.4] Preview Release
### Added
- Added new ThreadGroup creation to prevent IllegalThreadStateException if the underlying ThreadGroup has been destroyed [#474](https://github.com/Microsoft/mssql-jdbc/pull/474)
- Added try-with-resources to JUnit tests [#520](https://github.com/Microsoft/mssql-jdbc/pull/520)

### Fixed Issues 
- Fixed the issue with passing parameters names that start with '@' to a CallableStatement [#495](https://github.com/Microsoft/mssql-jdbc/pull/495)
- Fixed SQLServerDataTable creation being O(n^2) issue [#514](https://github.com/Microsoft/mssql-jdbc/pull/514)

### Changed
- Changed some manual array copying to System.arraycopy() [#500](https://github.com/Microsoft/mssql-jdbc/pull/500)
- Removed redundant toString() on String objects [#501](https://github.com/Microsoft/mssql-jdbc/pull/501)
- Replaced literals with constants [#502](https://github.com/Microsoft/mssql-jdbc/pull/502)

## [6.3.3] Preview Release
### Added
- Added connection properties for specifying custom TrustManager [#74](https://github.com/Microsoft/mssql-jdbc/pull/74)

### Fixed Issues 
- Fixed exception thrown by getters on null columns  [#488](https://github.com/Microsoft/mssql-jdbc/pull/488)
- Fixed issue with DatabaseMetaData#getImportedKeys() returns wrong value for DELETE_RULE [#490](https://github.com/Microsoft/mssql-jdbc/pull/490)
- Fixed issue with ActivityCorrelator causing a classloader leak [#465](https://github.com/Microsoft/mssql-jdbc/pull/465)

### Changed
- Removed explicit extends Object [#469](https://github.com/Microsoft/mssql-jdbc/pull/469)
- Removed unnecessary return statements [#471](https://github.com/Microsoft/mssql-jdbc/pull/471)
- Simplified overly complex boolean expressions [#472](https://github.com/Microsoft/mssql-jdbc/pull/472)
- Replaced explicit types with <> (the diamond operator) [#420](https://github.com/Microsoft/mssql-jdbc/pull/420)

## [6.3.2] Preview Release
### Added
- Added new connection property: sslProtocol [#422](https://github.com/Microsoft/mssql-jdbc/pull/422)
- Added "slow" tag to long running tests [#461](https://github.com/Microsoft/mssql-jdbc/pull/461)

### Fixed Issues 
- Fixed some error messages [#452](https://github.com/Microsoft/mssql-jdbc/pull/452) & [#459](https://github.com/Microsoft/mssql-jdbc/pull/459)
- Fixed statement leaks [#455](https://github.com/Microsoft/mssql-jdbc/pull/455)
- Fixed an issue regarding to loginTimeout with TLS [#456](https://github.com/Microsoft/mssql-jdbc/pull/456)
- Fixed sql_variant issue with String type [#442](https://github.com/Microsoft/mssql-jdbc/pull/442)
- Fixed issue with throwing error message for unsupported datatype [#450](https://github.com/Microsoft/mssql-jdbc/pull/450)
- Fixed issue that initial batchException was not thrown [#458](https://github.com/Microsoft/mssql-jdbc/pull/458)

### Changed
- Changed sendStringParameterAsUnicode to impact set/update null [#445](https://github.com/Microsoft/mssql-jdbc/pull/445)
- Removed connection property: fipsProvider [#460](https://github.com/Microsoft/mssql-jdbc/pull/460)
- Replaced for and while loops with foeach loops [#421](https://github.com/Microsoft/mssql-jdbc/pull/421)
- Replaced explicit types with the diamond operator [#468](https://github.com/Microsoft/mssql-jdbc/pull/468) & [#420](https://github.com/Microsoft/mssql-jdbc/pull/420)

## [6.3.1] Preview Release
### Added
- Added support for datetime/smallDatetime in TVP [#435](https://github.com/Microsoft/mssql-jdbc/pull/435)
- Added more JUnit tests for Always Encrypted [#432](https://github.com/Microsoft/mssql-jdbc/pull/432)

### Fixed Issues 
- Fixed getString issue for uniqueIdentifier [#423](https://github.com/Microsoft/mssql-jdbc/pull/423)

### Changed
- Skip long running tests based on Tag [#425](https://github.com/Microsoft/mssql-jdbc/pull/425)
- Removed volatile keyword [#409](https://github.com/Microsoft/mssql-jdbc/pull/409)

## [6.3.0] Preview Release
### Added
- Added support for sql_variant datatype [#387](https://github.com/Microsoft/mssql-jdbc/pull/387)
- Added more JUnit tests for Always Encrypted [#404](https://github.com/Microsoft/mssql-jdbc/pull/404)

### Fixed Issues 
- Fixed Turkey locale issue when lowercasing an "i" [#384](https://github.com/Microsoft/mssql-jdbc/pull/384)
- Fixed issue with incorrect parameter count for INSERT with subquery [#373](https://github.com/Microsoft/mssql-jdbc/pull/373)
- Fixed issue with running DDL in PreparedStatement [#372](https://github.com/Microsoft/mssql-jdbc/pull/372)
- Fixed issue with parameter metadata with whitespace characters [#371](https://github.com/Microsoft/mssql-jdbc/pull/371)
- Fixed handling of explicit boxing and unboxing [#84](https://github.com/Microsoft/mssql-jdbc/pull/84)
- Fixed metadata caching batch query issue [#393](https://github.com/Microsoft/mssql-jdbc/pull/393)
- Fixed javadoc issue for the newest maven version [#385](https://github.com/Microsoft/mssql-jdbc/pull/385)

### Changed
- Updated ADAL4J dependency to version 1.2.0 [#392](https://github.com/Microsoft/mssql-jdbc/pull/392)
- Updated azure-keyvault dependency to version 1.0.0 [#397](https://github.com/Microsoft/mssql-jdbc/pull/397)

## [6.2.2] Hotfix & Stable Release
### Changed
- Updated ADAL4J to version 1.2.0 and AKV to version 1.0.0 [#516](https://github.com/Microsoft/mssql-jdbc/pull/516)

## [6.2.1] Hotfix & Stable Release
### Fixed Issues 
- Fixed queries without parameters using preparedStatement [#372](https://github.com/Microsoft/mssql-jdbc/pull/372)
### Changed
- Removed metadata caching [#377](https://github.com/Microsoft/mssql-jdbc/pull/377)

## [6.2.0] Release Candidate
### Added
- Added TVP and BulkCopy random data test for all data types with server cursor [#319](https://github.com/Microsoft/mssql-jdbc/pull/319)
- Added AE setup and test [#337](https://github.com/Microsoft/mssql-jdbc/pull/337),[328](https://github.com/Microsoft/mssql-jdbc/pull/328)
- Added validation for javadocs for every commit [#338](https://github.com/Microsoft/mssql-jdbc/pull/338)
- Added metdata caching [#345](https://github.com/Microsoft/mssql-jdbc/pull/345)
- Added caching mvn dependencies for Appveyor [#320](https://github.com/Microsoft/mssql-jdbc/pull/320)
- Added caching mvn dependencies for Travis-CI [#322](https://github.com/Microsoft/mssql-jdbc/pull/322)
- Added handle for bulkcopy exceptions [#286](https://github.com/Microsoft/mssql-jdbc/pull/286)
- Added handle for TVP exceptions [#285](https://github.com/Microsoft/mssql-jdbc/pull/285)

### Fixed Issues 
- Fixed metadata caching issue with AE on connection [#361](https://github.com/Microsoft/mssql-jdbc/pull/361)
- Fixed issue with String index out of range parameter metadata [#353](https://github.com/Microsoft/mssql-jdbc/pull/353)
- Fixed javaDocs [#354](https://github.com/Microsoft/mssql-jdbc/pull/354) 
- Fixed javaDocs [#299](https://github.com/Microsoft/mssql-jdbc/pull/299)
- Performance fix from @brettwooldridge [#347](https://github.com/Microsoft/mssql-jdbc/pull/347)
- Get local host name before opening TDSChannel [#324](https://github.com/Microsoft/mssql-jdbc/pull/324)
- Fixed TVP Time issue [#317](https://github.com/Microsoft/mssql-jdbc/pull/317)
- Fixed SonarQube issues [#300](https://github.com/Microsoft/mssql-jdbc/pull/300)
- Fixed SonarQube issues [#301](https://github.com/Microsoft/mssql-jdbc/pull/301)
- Fixed random TDS invalid error [#310](https://github.com/Microsoft/mssql-jdbc/pull/310)
- Fixed password logging [#298](https://github.com/Microsoft/mssql-jdbc/pull/298)
- Fixed bulkcopy cursor issue [#270](https://github.com/Microsoft/mssql-jdbc/pull/270)

### Changed
- Refresh Kerberos configuration [#279](https://github.com/Microsoft/mssql-jdbc/pull/279)

## [6.1.7] Preview Release
### Added
- Added support for data type LONGVARCHAR, LONGNVARCHAR, LONGVARBINARY and SQLXML in TVP [#259](https://github.com/Microsoft/mssql-jdbc/pull/259)
- Added new connection property to accept custom JAAS configuration for Kerberos [#254](https://github.com/Microsoft/mssql-jdbc/pull/254)
- Added support for server cursor with TVP [#234](https://github.com/Microsoft/mssql-jdbc/pull/234) 
- Experimental Feature: Added new connection property to support network timeout [#253](https://github.com/Microsoft/mssql-jdbc/pull/253)
- Added support to authenticate Kerberos with principal and password [#163](https://github.com/Microsoft/mssql-jdbc/pull/163)
- Added temporal types to BulkCopyCSVTestInput.csv [#262](https://github.com/Microsoft/mssql-jdbc/pull/262)
- Added automatic detection of REALM in SPN needed for Cross Domain authentication [#40](https://github.com/Microsoft/mssql-jdbc/pull/40)

### Changed
- Updated minor semantics [#232](https://github.com/Microsoft/mssql-jdbc/pull/232)
- Cleaned up Azure Active Directory (AAD) Authentication methods [#256](https://github.com/Microsoft/mssql-jdbc/pull/256)
- Updated permission check before setting network timeout [#255](https://github.com/Microsoft/mssql-jdbc/pull/255)

### Fixed Issues
- Turn TNIR (TransparentNetworkIPResolution) off for Azure Active Directory (AAD) Authentication and changed TNIR multipliers [#240](https://github.com/Microsoft/mssql-jdbc/pull/240)
- Wrapped ClassCastException in BulkCopy with SQLServerException [#260](https://github.com/Microsoft/mssql-jdbc/pull/260)
- Initialized the XA transaction manager for each XAResource [#257](https://github.com/Microsoft/mssql-jdbc/pull/257)
- Fixed BigDecimal scale rounding issue in BulkCopy [#230](https://github.com/Microsoft/mssql-jdbc/issues/230)
- Fixed the invalid exception thrown when stored procedure does not exist is used with TVP [#265](https://github.com/Microsoft/mssql-jdbc/pull/265)

## [6.1.6] Preview Release
### Added
- Added constrained delegation to connection sample [#188](https://github.com/Microsoft/mssql-jdbc/pull/188)
- Added snapshot to identify nightly/dev builds [#221](https://github.com/Microsoft/mssql-jdbc/pull/221)
- Clarifying public deprecated constructors in LOBs [#226](https://github.com/Microsoft/mssql-jdbc/pull/226)
- Added OSGI Headers in MANIFEST.MF [#218](https://github.com/Microsoft/mssql-jdbc/pull/218)
- Added cause to SQLServerException [#202](https://github.com/Microsoft/mssql-jdbc/pull/202)

### Changed
- Removed java.io.Serializable interface from SQLServerConnectionPoolProxy [#201](https://github.com/Microsoft/mssql-jdbc/pull/201)
- Refactored DROP TABLE and DROP PROCEDURE calls in test code [#222](https://github.com/Microsoft/mssql-jdbc/pull/222/files)
- Removed obsolete methods from DriverJDBCVersion [#187](https://github.com/Microsoft/mssql-jdbc/pull/187)

### Fixed Issues
- Typos in SQLServerConnectionPoolProxy [#189](https://github.com/Microsoft/mssql-jdbc/pull/189)
- Fixed issue where exceptions are thrown if comments are in a SQL string [#157](https://github.com/Microsoft/mssql-jdbc/issues/157)
- Fixed test failures on pre-2016 servers [#215](https://github.com/Microsoft/mssql-jdbc/pull/215)
- Fixed SQLServerExceptions that are wrapped by another SQLServerException [#213](https://github.com/Microsoft/mssql-jdbc/pull/213)
- Fixed a stream isClosed error on LOBs test [#233](https://github.com/Microsoft/mssql-jdbc/pull/223)
- LOBs are fully materialised [#16](https://github.com/Microsoft/mssql-jdbc/issues/16)
- Fix precision issue in TVP [#217](https://github.com/Microsoft/mssql-jdbc/pull/217)
- Re-interrupt the current thread in order to restore the threads interrupt status [#196](https://github.com/Microsoft/mssql-jdbc/issues/196)
- Re-use parameter metadata when using Always Encrypted [#195](https://github.com/Microsoft/mssql-jdbc/issues/195)
- Improved performance for PreparedStatements through minimized server round-trips [#166](https://github.com/Microsoft/mssql-jdbc/issues/166)

## [6.1.5] Preview Release
### Added
- Added socket timeout exception as cause[#180](https://github.com/Microsoft/mssql-jdbc/pull/180)
- Added Constrained delegation support[#178](https://github.com/Microsoft/mssql-jdbc/pull/178)
- Added JUnit test for Statement test[#174](https://github.com/Microsoft/mssql-jdbc/pull/174)
- Added test for statement.cancel() when MultiSubnetFailover is set to true[#173](https://github.com/Microsoft/mssql-jdbc/pull/173)
- Added tests for lobs [#168](https://github.com/Microsoft/mssql-jdbc/pull/168)
- Added badges for License, Maven Central, JavaDocs & gitter chat room [#184](https://github.com/Microsoft/mssql-jdbc/pull/184)

### Changed
- Enabled update counts for SELECT INTO statements[#175](https://github.com/Microsoft/mssql-jdbc/pull/175)
- Use Executor service instead of thread[#162](https://github.com/Microsoft/mssql-jdbc/pull/162)
- Convert socket adaptor to socket[#160](https://github.com/Microsoft/mssql-jdbc/pull/160)

### Fixed Issues
- Fixed local test failures [#179](https://github.com/Microsoft/mssql-jdbc/pull/179) 
- Fixed random failure in BulkCopyColumnMapping test[#165](https://github.com/Microsoft/mssql-jdbc/pull/165)

## [6.1.4] Preview Release
### Added
- Added isWrapperFor methods for MetaData classes[#94](https://github.com/Microsoft/mssql-jdbc/pull/94)
- Added Code Coverage [#136](https://github.com/Microsoft/mssql-jdbc/pull/136)
- Added TVP schema test [#137](https://github.com/Microsoft/mssql-jdbc/pull/137)
- Introduced FIPS boolean property [#135](https://github.com/Microsoft/mssql-jdbc/pull/135)
- Added unit statement test cases [#147](https://github.com/Microsoft/mssql-jdbc/pull/147)

### Changed
- Enabled AAD Authentication with Access Token on Linux [#142](https://github.com/Microsoft/mssql-jdbc/pull/142)
- Enabled AAD Authentication with ActiveDirectoryPassword on Linux [#146](https://github.com/Microsoft/mssql-jdbc/pull/146)
- Made Azure Key Vault and Azure Active Directory Authentication Dependencies optional [#148](https://github.com/Microsoft/mssql-jdbc/pull/148)
- Getting TVP name from ParameterMetaData when using TVP with a stored procedure [#138](https://github.com/Microsoft/mssql-jdbc/pull/138)

### Fixed Issues
- Fixed getBinaryStream issue [#133](https://github.com/Microsoft/mssql-jdbc/pull/133) 
- Fixed an issue of Bulk Copy when AlwaysEncrypted is enabled on connection and destination table is not encrypted [#151](https://github.com/Microsoft/mssql-jdbc/pull/151)


## [6.1.3] Preview Release
### Added
 - Added Binary and Varbinary types to the jUnit test framework [#119](https://github.com/Microsoft/mssql-jdbc/pull/119)
 - Added BulkCopy test cases for csv [#123](https://github.com/Microsoft/mssql-jdbc/pull/123)
 - Added BulkCopy ColumnMapping test cases [#127](https://github.com/Microsoft/mssql-jdbc/pull/127)

### Changed
 - Switched to clean rounding for bigDecimal [#118](https://github.com/Microsoft/mssql-jdbc/pull/118)
 - Updated BVT tests to use jUnit test framework [#120](https://github.com/Microsoft/mssql-jdbc/pull/120)
 - In case of socket timeout occurrence, avoid connection retry [#122](https://github.com/Microsoft/mssql-jdbc/pull/122)
 - Changed ant build file to skip tests [#126](https://github.com/Microsoft/mssql-jdbc/pull/126)

### Fixed Issues
 - Fixed the inconsistent coding style [#4](https://github.com/Microsoft/mssql-jdbc/issues/4) 
 - Fixed NullPointerException in case when SocketTimeout occurs [#65](https://github.com/Microsoft/mssql-jdbc/issues/121) 

 
## [6.1.2] Preview Release
### Added
 - Socket timeout implementation for both connection string and data source [#85](https://github.com/Microsoft/mssql-jdbc/pull/85)
 - Query timeout API for datasource [#88](https://github.com/Microsoft/mssql-jdbc/pull/88)
 - Added connection tests [#95](https://github.com/Microsoft/mssql-jdbc/pull/95)
 - Added Support for FIPS enabled JVM [#97](https://github.com/Microsoft/mssql-jdbc/pull/97)
 - Added additional tests for bulk copy [#110] (https://github.com/Microsoft/mssql-jdbc/pull/110)

### Changed
 - Remove redundant type casts [#63](https://github.com/Microsoft/mssql-jdbc/pull/63) 
 - Read SQL Server error message if status flag has DONE_ERROR set [#73](https://github.com/Microsoft/mssql-jdbc/pull/73)
 - Fix a bug when the value of queryTimeout is bigger than the max value of integer [#78](https://github.com/Microsoft/mssql-jdbc/pull/78) 
 - Add new dependencies to gradle build script [#81](https://github.com/Microsoft/mssql-jdbc/pull/81)
 - Updates to test framework [#90](https://github.com/Microsoft/mssql-jdbc/pull/90)

### Fixed Issues
 - Set the jre8 version as default [#59](https://github.com/Microsoft/mssql-jdbc/issues/59) 
 - Fixed exception SQL Server instance in use does not support column encryption [#65](https://github.com/Microsoft/mssql-jdbc/issues/65) 
 - TVP Handling is causing exception when calling SP with return value [#80](https://github.com/Microsoft/mssql-jdbc/issues/80) 
 - BigDecimal in TVP can incorrectly cause SQLServerException related to invalid precision or scale [#86](https://github.com/Microsoft/mssql-jdbc/issues/86) 
 - Fixed the connection close issue on using variant type [#91] (https://github.com/Microsoft/mssql-jdbc/issues/91)


## [6.1.1] Preview Release
### Added
- Java Docs [#46](https://github.com/Microsoft/mssql-jdbc/pull/46)
- Driver version number in LOGIN7 packet [#43](https://github.com/Microsoft/mssql-jdbc/pull/43)
- Travis- CI Integration [#23](https://github.com/Microsoft/mssql-jdbc/pull/23)
- Appveyor Integration [#23](https://github.com/Microsoft/mssql-jdbc/pull/23)
- Make Ms Jdbc driver more Spring friendly [#9](https://github.com/Microsoft/mssql-jdbc/pull/9)
- Implement Driver#getParentLogger [#8](https://github.com/Microsoft/mssql-jdbc/pull/8)
- Implement missing MetaData #unwrap methods [#12](https://github.com/Microsoft/mssql-jdbc/pull/12)
- Added Gradle build script [#54](https://github.com/Microsoft/mssql-jdbc/pull/54)
- Added a queryTimeout connection parameter [#45](https://github.com/Microsoft/mssql-jdbc/pull/45)
- Added Stored Procedure support for TVP [#47](https://github.com/Microsoft/mssql-jdbc/pull/47)

### Changed
- Use StandardCharsets [#15](https://github.com/Microsoft/mssql-jdbc/pull/15)
- Use Charset throughout [#26](https://github.com/Microsoft/mssql-jdbc/pull/26)
- Upgrade azure-keyvault to 0.9.7 [#50](https://github.com/Microsoft/mssql-jdbc/pull/50)  
- Avoid unnecessary calls to String copy constructor [#14](https://github.com/Microsoft/mssql-jdbc/pull/14)
- make setObject() throw a clear exception for TVP when using with result set [#48](https://github.com/Microsoft/mssql-jdbc/pull/48)
- Few clean-ups like remove wild card imports, unused imports etc.  [#52](https://github.com/Microsoft/mssql-jdbc/pull/52)
- Update Maven Plugin [#55](https://github.com/Microsoft/mssql-jdbc/pull/55)
 

## [6.1.0] Stable Release
### Changed
- Open Sourced.
