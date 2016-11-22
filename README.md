# Microsoft JDBC Driver for SQL Server

Welcome to the Microsoft JDBC Driver for SQL Server project!

The Microsoft JDBC Driver for SQL Server is a Type 4 JDBC driver that provides database connectivity through the standard JDBC application program interfaces (APIs) available in the Java Platform, Enterprise Editions. The Driver provides access to Microsoft SQL Server and Azure SQL Database from any Java application, application server, or Java-enabled applet.

We hope you enjoy using the Microsoft JDBC Driver for SQL Server.

SQL Server Team


## Announcements
What's coming next?  We will look into adding a more comprehensive set of tests, improving our javadocs, and start developing the next set of features.

## Get Started 
* [**Ubuntu + SQL Server + Java**](https://www.microsoft.com/en-us/sql-server/developer-get-started/java-ubuntu) 
* [**Red Hat + SQL Server + Java**](https://www.microsoft.com/en-us/sql-server/developer-get-started/java-rhel)
* [**Mac + SQL Server + Java**](https://www.microsoft.com/en-us/sql-server/developer-get-started/java-mac)
* [**Windows + SQL Server + Java**](https://www.microsoft.com/en-us/sql-server/developer-get-started/java-windows)


## Build
### Prerequisites
* Java 8
* [Ant](http://ant.apache.org/manual/install.html) (with [Ivy](https://ant.apache.org/ivy/download.cgi)) or [Maven](http://maven.apache.org/download.cgi)
* An instance of SQL Server or Azure SQL Database that you can connect to. 

### Build the JAR files
The build automatically triggers a set of verification tests to run.  For these tests to pass, you will first need to modify the serverConfig.cfg file under .\src\test to provide the correct connection properties for your SQL Server or Azure SQL Database instance.

To build the jar files, you must use Java 8 with either Ant (with Ivy) or Maven.  You can choose to build a JDBC 4.1 compliant jar file (for use with JRE 7) and/or a JDBC 4.2 compliant jar file (for use with JRE 8).

* Ant:
	1. If you have not already done so, update the serverConfig.cfg file under .\src\test with the connection properties for your SQL Server or SQL DB instance.
	2. Run one of the commands below to build a JDBC 4.1 compliant jar, JDBC 4.2 compliant jar, or both in the \build directory. 
		* Run `ant`. This creates both JDBC 4.1 compliant jar and JDBC 4.2 compliant jar in \build directory
    	* Run `ant build41`. This creates JDBC 4.1 compliant jar in \build directory
    	* Run `ant build42`. This creates JDBC 4.2 compliant jar in \build directory

* Maven:
	1. If you have not already done so, update the serverConfig.cfg file under .\src\test with the connection properties for your SQL Server or SQL DB instance.
	2. Run one of the commands below to build a JDBC 4.1 compliant jar or JDBC 4.2 compliant jar in the \build directory. 
    	* Run `mvn install -Pbuild41`. This creates JDBC 4.1 compliant jar in \target directory
    	* Run `mvn install -Pbuild42`. This creates JDBC 4.2 compliant jar in \target directory

## Resources

### Documentation
This driver is documented on [Microsoft's Documentation web site](https://msdn.microsoft.com/en-us/library/mt720657).

### Sample Code
For samples, please see the src\sample directory.

### Download the DLLs
For some features (e.g. Integrated Authentication and Distributed Transactions), you may need to use the `sqljdbc_xa` and `sqljdbc_auth` DLLs. They can be downloaded from the [Microsoft Download Center](https://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=11774)

### Download the driver
Don't want to compile anything?

We're now on the Maven Central Repository. Add the following to your POM file:

```
<dependency>
	<groupId>com.microsoft.sqlserver</groupId>
	<artifactId>mssql-jdbc</artifactId>
	<version>6.1.0.jre8</version>
</dependency>
```

The driver can be downloaded from the [Microsoft Download Center](https://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=11774)

##Dependencies
This project has following dependencies: 

Compile Time:

 - `azure-keyvault`

Test Time:

 - `junit:jar`

If anybody wants to use driver as a run-time dependency but not using Azure Web Services for authentication then you either describe as optional dependency or exclude azure.

```
<dependency>
	<groupId>com.microsoft.sqlserver</groupId>
	<artifactId>mssql-jdbc</artifactId>
	<version>6.1.0.jre8</version>
	<scope>runtime</scope>
	<exclusions>
		<exclusion>
		         <groupId>com.microsoft.azure</groupId>
		         <artifactId>azure-keyvault</artifactId>
		</exclusion>
    </exclusions>
</dependency>
```

###Dependency Tree
```
mvn dependency:tree
[INFO] Scanning for projects...
[INFO]
[INFO] ------------------------------------------------------------------------
[INFO] Building Microsoft JDBC Driver for SQL Server 6.1.0
[INFO] ------------------------------------------------------------------------
[INFO]
[INFO] --- maven-dependency-plugin:2.8:tree (default-cli) @ mssql-jdbc ---
[INFO] com.microsoft.sqlserver:mssql-jdbc:jar:6.1.0
[INFO] +- com.microsoft.azure:azure-keyvault:jar:0.9.3:compile
[INFO] |  +- com.microsoft.azure:azure-core:jar:0.9.3:compile
[INFO] |  |  +- commons-codec:commons-codec:jar:1.10:compile
[INFO] |  |  +- commons-lang:commons-lang:jar:2.6:compile
[INFO] |  |  +- javax.mail:mail:jar:1.4.5:compile
[INFO] |  |  |  \- javax.activation:activation:jar:1.1:compile
[INFO] |  |  +- com.sun.jersey:jersey-client:jar:1.13:compile
[INFO] |  |  |  \- com.sun.jersey:jersey-core:jar:1.13:compile
[INFO] |  |  \- com.sun.jersey:jersey-json:jar:1.13:compile
[INFO] |  |     +- org.codehaus.jettison:jettison:jar:1.1:compile
[INFO] |  |     |  \- stax:stax-api:jar:1.0.1:compile
[INFO] |  |     +- com.sun.xml.bind:jaxb-impl:jar:2.2.3-1:compile
[INFO] |  |     |  \- javax.xml.bind:jaxb-api:jar:2.2.2:compile
[INFO] |  |     |     \- javax.xml.stream:stax-api:jar:1.0-2:compile
[INFO] |  |     +- org.codehaus.jackson:jackson-core-asl:jar:1.9.2:compile
[INFO] |  |     +- org.codehaus.jackson:jackson-mapper-asl:jar:1.9.2:compile
[INFO] |  |     +- org.codehaus.jackson:jackson-jaxrs:jar:1.9.2:compile
[INFO] |  |     \- org.codehaus.jackson:jackson-xc:jar:1.9.2:compile
[INFO] |  +- org.apache.httpcomponents:httpclient:jar:4.3.6:compile
[INFO] |  |  +- org.apache.httpcomponents:httpcore:jar:4.3.3:compile
[INFO] |  |  \- commons-logging:commons-logging:jar:1.1.3:compile
[INFO] |  +- javax.inject:javax.inject:jar:1:compile
[INFO] |  \- com.microsoft.azure:adal4j:jar:1.0.0:compile
[INFO] |     +- com.nimbusds:oauth2-oidc-sdk:jar:4.5:compile
[INFO] |     |  +- net.jcip:jcip-annotations:jar:1.0:compile
[INFO] |     |  +- org.apache.commons:commons-lang3:jar:3.3.1:compile
[INFO] |     |  +- net.minidev:json-smart:jar:1.1.1:compile
[INFO] |     |  +- com.nimbusds:lang-tag:jar:1.4:compile
[INFO] |     |  \- com.nimbusds:nimbus-jose-jwt:jar:3.1.2:compile
[INFO] |     |     \- org.bouncycastle:bcprov-jdk15on:jar:1.51:compile
[INFO] |     +- com.google.code.gson:gson:jar:2.2.4:compile
[INFO] |     \- org.slf4j:slf4j-api:jar:1.7.5:compile
[INFO] \- junit:junit:jar:4.12:test
[INFO]    \- org.hamcrest:hamcrest-core:jar:1.3:test
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
[INFO] ------------------------------------------------------------------------
[INFO] Total time: 1.423 s
[INFO] Finished at: 2016-11-22T13:13:55-08:00
[INFO] Final Memory: 12M/304M
[INFO] ------------------------------------------------------------------------
```


## Guidelines for Reporting Issues
We appreciate you taking the time to test the driver, provide feedback and report any issues.  It would be extremely helpful if you:

- Report each issue as a new issue (but check first if it's already been reported)
- Try to be detailed in your report. Useful information for good bug reports include:
  * What you are seeing and what the expected behaviour is
  *  Which jar file?
  * Environment details: e.g. Java version, client operating system?
  * Table schema (for some issues the data types make a big difference!)
  * Any other relevant information you want to share
- Try to include a Java sample demonstrating the isolated problem.

Thank you!

### Reporting security issues and security bugs
Security issues and bugs should be reported privately, via email, to the Microsoft Security Response Center (MSRC) [secure@microsoft.com](mailto:secure@microsoft.com). You should receive a response within 24 hours. If for some reason you do not, please follow up via email to ensure we received your original message. Further information, including the MSRC PGP key, can be found in the [Security TechCenter](https://technet.microsoft.com/en-us/security/ff852094.aspx).


## License
The Microsoft JDBC Driver for SQL Server is licensed under the MIT license. See the [LICENSE](https://github.com/Microsoft/mssql-jdbc/blob/master/LICENSE) file for more details.



## Code of conduct
This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
