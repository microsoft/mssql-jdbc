# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) 

## [6.5.1] Preview Release
### Added
- Test cases for Date, Time, and Datetime2 data types. [#558](https://github.com/Microsoft/mssql-jdbc/pull/558)

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
- Added new ThreadGroup creation to prevent IllegalThreadStateException if the underlying ThreadGroup has been destroyed. [#474](https://github.com/Microsoft/mssql-jdbc/pull/474)
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
- Added more Junit tests for Always Encrypted [#432](https://github.com/Microsoft/mssql-jdbc/pull/432)

### Fixed Issues 
- Fixed getString issue for uniqueIdentifier [#423](https://github.com/Microsoft/mssql-jdbc/pull/423)

### Changed
- Skip long running tests based on Tag [#425](https://github.com/Microsoft/mssql-jdbc/pull/425)
- Removed volatile keyword [#409](https://github.com/Microsoft/mssql-jdbc/pull/409)

## [6.3.0] Preview Release
### Added
- Added support for sql_variant datatype [#387](https://github.com/Microsoft/mssql-jdbc/pull/387)
- Added more Junit tests for Always Encrypted [#404](https://github.com/Microsoft/mssql-jdbc/pull/404)

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
- Added junit test for Statement test[#174](https://github.com/Microsoft/mssql-jdbc/pull/174)
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
 - In case of socket timeout occurance, avoid connection retry [#122](https://github.com/Microsoft/mssql-jdbc/pull/122)
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
