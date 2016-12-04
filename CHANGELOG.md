# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) 

## [6.1.1]
### Added
- Java Docs
- Driver version number in LOGIN7 packet
- Travis- CI Integration
- Appveyor Integration
- Make Ms Jdbc driver more Spring friendly
- Implement Driver#getParentLogger 
- Implement missing MetaData #unwrap methods 
- Added Gradle build script
- Added a queryTimeout connection parameter
- Added Store Procedure support for TVP

### Changed
- Use StandardCharsets
- Use Charset throughout
- Upgrade azure-keyvault to 0.9.7 
- Avoid unnecessary calls to String copy constructor 
- make setObject() throw a clear exception for TVP when using with result set
- Few clean-ups like remove wild card imports, unused imports etc. 
 

## [6.1.0]
### Changed
- Open Sourced.



