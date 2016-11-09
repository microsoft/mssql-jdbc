# Microsoft JDBC Driver for SQL Server

Welcome to the Microsoft JDBC Driver for SQL Server project!

The Microsoft JDBC Driver for SQL Server is a Type 4 JDBC driver that provides database connectivity through the standard JDBC application program interfaces (APIs) available in the Java Platform, Enterprise Editions. The Driver provides access to Microsoft SQL Server and Azure SQL Database from any Java application, application server, or Java-enabled applet.

We hope you enjoy using the Microsoft JDBC Driver for SQL Server.

SQL Server Team


## Announcements
What's coming next?  We will look into adding a more comprehensive set of tests, improving our javadocs, and start developing the next set of features.

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

### Download the driver
Don't want to compile anything? The driver can be downloaded from the [Microsoft Download Center](https://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=11774)


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
