[![GitHub license](https://img.shields.io/badge/license-MIT-blue.svg)](https://raw.githubusercontent.com/Microsoft/mssql-jdbc/master/LICENSE)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.microsoft.sqlserver/mssql-jdbc/badge.svg)](http://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc)
[![codecov.io](http://codecov.io/github/Microsoft/mssql-jdbc/coverage.svg?branch=master)](http://codecov.io/github/Microsoft/mssql-jdbc?branch=master)
[![Javadocs](http://javadoc.io/badge/com.microsoft.sqlserver/mssql-jdbc.svg)](http://javadoc.io/doc/com.microsoft.sqlserver/mssql-jdbc)
[![Gitter](https://img.shields.io/gitter/room/badges/shields.svg)](https://gitter.im/Microsoft/mssql-developers)
</br>
# Microsoft JDBC Driver for SQL Server

Welcome to the Microsoft JDBC Driver for SQL Server project!

The Microsoft JDBC Driver for SQL Server is a Type 4 JDBC driver that provides database connectivity through the standard JDBC application program interfaces (APIs) available in the Java Platform, Enterprise Editions. The Driver provides access to Microsoft SQL Server and Azure SQL Database from any Java application, application server, or Java-enabled applet.

We hope you enjoy using the Microsoft JDBC Driver for SQL Server.

SQL Server Team

## Take our survey

Let us know how you think we're doing.

<a href="https://aka.ms/mssqljdbcsurvey"><img style="float: right;"  height="67" width="156" src="https://sqlchoice.blob.core.windows.net/sqlchoice/static/images/survey.png"></a>

## Status of Most Recent Builds
| Azure Pipelines (Windows) | Azure Pipelines (Linux) | Azure Pipelines (MacOS) |
|--------------------------|--------------------------|--------------------------|
|[![Build Status](https://dev.azure.com/sqlclientdrivers-ci/mssql-jdbc/_apis/build/status/Microsoft.mssql-jdbc.windows?branchName=dev)](https://dev.azure.com/sqlclientdrivers-ci/mssql-jdbc/_build/latest?definitionId=1&branchName=dev) | [![Build Status](https://dev.azure.com/sqlclientdrivers-ci/mssql-jdbc/_apis/build/status/Microsoft.mssql-jdbc.linux?branchName=dev)](https://dev.azure.com/sqlclientdrivers-ci/mssql-jdbc/_build/latest?definitionId=3&branchName=dev) | [![Build Status](https://dev.azure.com/sqlclientdrivers-ci/mssql-jdbc/_apis/build/status/Microsoft.mssql-jdbc.macOS?branchName=dev)](https://dev.azure.com/sqlclientdrivers-ci/mssql-jdbc/_build/latest?definitionId=7&branchName=dev)|

## Announcements
What's coming next?  We will look into adding a more comprehensive set of tests, improving our javadocs, and start developing the next set of features.

## Get Started 
* [**Ubuntu + SQL Server + Java**](https://www.microsoft.com/en-us/sql-server/developer-get-started/java/ubuntu) 
* [**Red Hat + SQL Server + Java**](https://www.microsoft.com/en-us/sql-server/developer-get-started/java/rhel)
* [**Mac + SQL Server + Java**](https://www.microsoft.com/en-us/sql-server/developer-get-started/java/mac)
* [**Windows + SQL Server + Java**](https://www.microsoft.com/en-us/sql-server/developer-get-started/java/windows)

## Build
### Prerequisites
* Java 11+
* [Maven 3.5.0+](http://maven.apache.org/download.cgi)
* An instance of SQL Server or Azure SQL Database that you can connect to. 

### Build the JAR files
Maven builds automatically trigger a set of verification tests to run.  For these tests to pass, you will first need to add an environment variable in your system called `mssql_jdbc_test_connection_properties` to provide the [correct connection properties](https://docs.microsoft.com/en-us/sql/connect/jdbc/building-the-connection-url) for your SQL Server or Azure SQL Database instance.

To build the jar files, you must use minimum version of Java 11 with Maven. You may choose to build JDBC 4.3 compliant jar file (for use with JRE 11 or newer JRE versions) and/or a JDBC 4.2 compliant jar file (for use with JRE 8).

* Maven:
	1. If you have not already done so, add the environment variable `mssql_jdbc_test_connection_properties` in your system with the connection properties for your SQL Server or SQL DB instance.
	2. Run one of the commands below to build a JRE 11 and newer versions compatible jar or JRE 8 compatible jar in the `\target` directory. 
        * Run `mvn install -Pjre15`. This creates JRE 15 compatible jar in `\target` directory which is JDBC 4.3 compliant (Build with JDK 15+).
        * Run `mvn install -Pjre11`. This creates JRE 11 compatible jar in `\target` directory which is JDBC 4.3 compliant (Build with JDK 11+).
        * Run `mvn install -Pjre8`. This creates JRE 8 compatible jar in `\target` directory which is JDBC 4.2 compliant (Build with JDK 11+).

* Gradle:
	1. If you have not already done so, add the environment variable `mssql_jdbc_test_connection_properties` in your system with the connection properties for your SQL Server or SQL DB instance.
	2. Run one of the commands below to build a JRE 11 and newer versions compatible jar or JRE 8 compatible jar in the `\build\libs` directory. 
        * Run `gradle build -PbuildProfile=jre15`. This creates JRE 15 compatible jar in `\build\libs` directory which is JDBC 4.3 compliant (Build with JDK 15+).
        * Run `gradle build -PbuildProfile=jre11`. This creates JRE 11 compatible jar in `\build\libs` directory which is JDBC 4.3 compliant (Build with JDK 11+).
        * Run `gradle build -PbuildProfile=jre8`. This creates JRE 8 compatible jar in `\build\libs` directory which is JDBC 4.2 compliant (Build with JDK 11+).

## Resources

### Documentation
API reference documentation is available in [Javadocs](https://aka.ms/jdbcjavadocs).

This driver is documented on [Microsoft's Documentation web site](https://docs.microsoft.com/en-us/sql/connect/jdbc/getting-started-with-the-jdbc-driver).

### Sample Code
For samples, please see the `src\sample` directory.

### Download the DLLs
For some features (e.g. Integrated Authentication and Distributed Transactions), you may need to use the `sqljdbc_xa` and `mssql-jdbc_auth-<version>.<arch>` DLLs. They can be downloaded from the [Microsoft Download Center](https://docs.microsoft.com/sql/connect/jdbc/download-microsoft-jdbc-driver-for-sql-server). `mssql-jdbc_auth-<version>.<arch>` can also be downloaded from [Maven](https://mvnrepository.com/artifact/com.microsoft.sqlserver/mssql-jdbc_auth).

### Download the driver
Don't want to compile anything?

We're now on the Maven Central Repository. Add the following to your POM file to get the most stable release:

```xml
<dependency>
	<groupId>com.microsoft.sqlserver</groupId>
	<artifactId>mssql-jdbc</artifactId>
	<version>9.2.1.jre15</version>
</dependency>
```
The driver can be downloaded from the [Microsoft Download Center](https://go.microsoft.com/fwlink/?linkid=868287).

To get the latest preview version of the driver, add the following to your POM file: 

```xml
<dependency>
	<groupId>com.microsoft.sqlserver</groupId>
	<artifactId>mssql-jdbc</artifactId>
	<version>9.1.1.jre15-preview</version>
</dependency>
```

### Using driver as Java Module
Starting from version 7.0.0, the driver Jars (jre10 and above) will expose 'Automatic-Module' as **'com.microsoft.sqlserver.jdbc'**. The supporting Jar can now be added to ModulePath to access this module.


## Dependencies
This project has following dependencies: 

Compile Time:
 - `com.azure:azure-security-keyvault-keys` : Microsoft Azure Client Library For KeyVault Keys (optional)
 - `com.azure:azure-identity` : Microsoft Azure Client Library For Identity (optional)
 - `org.bouncycastle:bcprov-jdk15on` : Bouncy Castle Provider for Always Encrypted with secure enclaves feature with JAVA 8 only (optional)
 - `com.google.code.gson:gson` : Gson for Always Encrypted with secure enclaves feature (optional)

Test Time:
 - `junit:jar`   : For Unit Test cases.

### Dependency Tree
One can see all dependencies including Transitive Dependency by executing following command.
``` 
mvn dependency:tree
```

### Azure Key Vault and Azure Active Directory Authentication Dependencies
Projects that require either of the two features need to explicitly declare the dependency in their pom file.

***For Example:*** If you are using *Azure Active Directory Authentication feature* then you need to declare the *azure-identity* dependency in your project's POM file. Please see the following snippet: 

```xml
<dependency>
	<groupId>com.microsoft.sqlserver</groupId>
	<artifactId>mssql-jdbc</artifactId>
	<version>9.2.1.jre15</version>
	<scope>compile</scope>
</dependency>

<dependency>
	<groupId>com.azure</groupId>
	<artifactId>azure-identity</artifactId>
	<version>1.1.3</version>
</dependency>

```

***For Example:*** If you are using *Azure Key Vault feature* then you need to declare the *azure-identity* and *azure-security-keyvault-keys* dependencies in your project's POM file. Please see the following snippet: 

```xml
<dependency>
	<groupId>com.microsoft.sqlserver</groupId>
	<artifactId>mssql-jdbc</artifactId>
	<version>9.2.1.jre15</version>
	<scope>compile</scope>
</dependency>

<dependency>
	<groupId>com.azure</groupId>
	<artifactId>azure-identity</artifactId>
	<version>1.1.3</version>
</dependency>

<dependency>
	<groupId>com.azure</groupId>
	<artifactId>azure-security-keyvault-keys</artifactId>
	<version>4.2.1</version>
</dependency>
```

***Please note*** as of the v6.2.2, the way to construct a `SQLServerColumnEncryptionAzureKeyVaultProvider` object has changed. Please refer to this [Wiki](https://github.com/Microsoft/mssql-jdbc/wiki/New-Constructor-Definition-for-SQLServerColumnEncryptionAzureKeyVaultProvider-after-6.2.2-Release) page for more information.


### 'useFmtOnly' connection property Dependencies
When setting 'useFmtOnly' property to 'true' for establishing a connection or creating a prepared statement, *antlr-runtime* dependency is required to be added in your project's POM file.  Please see the following snippet: 

```xml
<dependency>
	<groupId>com.microsoft.sqlserver</groupId>
	<artifactId>mssql-jdbc</artifactId>
	<version>9.2.1.jre15</version>
</dependency>

<dependency>
	<groupId>org.antlr</groupId>
	<artifactId>antlr4-runtime</artifactId>
	<version>4.7.2</version>
</dependency>
```

## Guidelines for Creating Pull Requests
We love contributions from the community.  To help improve the quality of our code, we encourage you to use the mssql-jdbc_formatter.xml formatter provided on all pull requests.

Thank you!

## Guidelines for Reporting Issues
We appreciate you taking the time to test the driver, provide feedback and report any issues. It would be extremely helpful if you:

- Report each issue as a new issue (but check first if it's already been reported)
- Try to be detailed in your report. Useful information for good bug reports include:
  * What you are seeing and what the expected behavior is
  * Which jar file?
  * Environment details: e.g. Java version, client operating system?
  * Table schema (for some issues the data types make a big difference!)
  * Any other relevant information you want to share
- Try to include a Java sample demonstrating the isolated problem.

Thank you!

### Reporting security issues and security bugs
Security issues and bugs should be reported privately, via email, to the Microsoft Security Response Center (MSRC) [secure@microsoft.com](mailto:secure@microsoft.com). You should receive a response within 24 hours. If for some reason you do not, please follow up via email to ensure we received your original message. Further information, including the MSRC PGP key, can be found in the [Security TechCenter](https://technet.microsoft.com/en-us/security/ff852094.aspx).

## Release roadmap and standards
Our goal is to release regular updates which improve the driver and bring new features to users. Stable, production quality releases happen twice a year, targeting the first and third quarters of the calendar year. They are tested against a comprehensive matrix of supported operating systems, Java versions, and SQL Server versions. Stable releases are accompanied by additional localized packages, which are available on the Microsoft website.

Preview releases happen approximately monthly between stable releases. This gives users an opportunity to try out new features and provide feedback on them before they go into stable releases. Preview releases also include frequent bug fixes for customers to verify without having to wait for a stable release. Preview releases are only available in English. While they are tested, preview releases do not necessarily go through the same rigorous, full test matrix and review process as stable releases.

You can see what is going into a future release by monitoring [Milestones](https://github.com/Microsoft/mssql-jdbc/milestones) in the repository.

### Version conventions
Starting with 6.0, stable versions have an even minor version. For example, 6.0, 6.2, 6.4, 7.0, 7.2, 7.4, 8.2, 8.4, 9.2. Preview versions have an odd minor version. For example, 6.1, 6.3, 6.5, 7.1, 7.3, 8.1 and so on

## Contributors 
Special thanks to everyone who has contributed to the project. 

Up-to-date list of contributors: https://github.com/Microsoft/mssql-jdbc/graphs/contributors

Here are our Top 15 contributors from the community:
- pierresouchay (Pierre Souchay)
- marschall (Philippe Marschall)
- JamieMagee (Jamie Magee)
- sehrope (Sehrope Sarkuni)
- gordthompson (Gord Thompson)
- simon04 (Simon Legner)
- gstojsic
- cosmofrit
- rPraml (Roland Praml)
- nsidhaye (Nikhil Sidhaye)
- tonytamwk
- shayaantx
- mnhubspot
- mfriesen (Mike Friesen)
- harawata (Iwao AVE!)

## License
The Microsoft JDBC Driver for SQL Server is licensed under the MIT license. See the [LICENSE](https://github.com/Microsoft/mssql-jdbc/blob/master/LICENSE) file for more details.

## Code of conduct
This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
