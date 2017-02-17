# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) 

## [6.1.4]
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


## [6.1.3]
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

 
## [6.1.2]
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


## [6.1.1]
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
 

## [6.1.0]
### Changed
- Open Sourced.
