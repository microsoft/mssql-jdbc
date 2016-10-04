# Microsoft JDBC Driver for SQL Server

Welcome to the Microsoft JDBC Driver for SQL Server project!

The Microsoft JDBC Driver for SQL Server is a Type 4 JDBC driver that provides database connectivity with SQL Server through the standard JDBC application program interfaces (APIs).

We hope you enjoy using the Microsoft JDBC Driver for SQL Server.


## Documentation
This driver is documented on [Microsoft's Documentation web site](https://msdn.microsoft.com/en-us/library/mt720657).


## Changes
For details about the changes included in this release, please see our [blog](https://msdn.microsoft.com/en-us/library/aa342325).


## Known Issues
Please visit the project on GitHub to view outstanding issues.


## Build
To build the jar files, you can use either Ant (with Ivy) or Maven.

* Ant:
    * Run `ant`. This creates both JDBC 4.1 compliant jar and JDBC 4.2 compliant jar in \build directory
    * Run `ant build41`. This creates JDBC 4.1 compliant jar in \build directory
    * Run `ant build42`. This creates JDBC 4.2 compliant jar in \build directory

* Maven:
    * Run `mvn install -Pbuild41`. This creates JDBC 41 compliant jar in \target directory
    * Run `mvn install -Pbuild42`. This creates JDBC 42 compliant jar in \target directory


## Download the driver
The driver can be downloaded from the [Microsoft Download Center](https://www.microsoft.com/en-us/download/details.aspx?displaylang=en&id=11774)


## Notes
Supported operating systems for Microsoft JDBC Driver 6.0 for SQL Server include:
* Windows Server 2008 SP2
* Windows Server 2008 R2 SP1
* Windows Server 2012
* Windows Server 2012 R2
* Windows Vista SP2
* Windows 7 SP1
* Windows 8
* Windows 8.1
* Windows 10
* Linux
* Unix

The list above is an example of some of the supported operating systems. The JDBC driver is designed to work on any operating system that supports the use of a Java Virtual Machine (JVM). However, only Oracle Solaris (x86), SUSE Linux, and Windows Vista Service Pack 2 or later operating systems have been tested.


## License
The Microsoft JDBC Driver for SQL Server is licensed under the MIT license. See the LICENSE file for more details.



