# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) 

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
- Added Store Procedure support for TVP [#47](https://github.com/Microsoft/mssql-jdbc/pull/47)

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



