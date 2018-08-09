
# Running Sample Applications

The Microsoft JDBC Driver for SQL Server sample applications demonstrate various features of the JDBC driver. Additionally, they demonstrate good programming practices that you can follow when using the JDBC driver with SQL Server or Azure SQL Database.

All the sample applications are contained in *.java code files that can be compiled and run on your local computer. These code samples are based on the ones found in [Microsoft Docs](https://docs.microsoft.com/en-us/sql/connect/jdbc/sample-jdbc-driver-applications), where you can find additional content with more detailed descriptions.

The following samples are available:

1. adaptive
    * **ExecuteStoredProcedures** - Demonstrates how to retrieve a large OUT parameter from a stored procedure and use adaptive buffering mode.
    * **ReadLargeData** - Demonstrates how to read large data and use adaptive buffering mode. It also demonstrates how to retrieve a large single-column value from by using the getCharacterStream method.
    * **UpdateLargeData** - Demonstrates how to update large data and set the adaptive buffering mode explicitly for updatable result sets.

2. alwaysencrypted
    * **AlwaysEncrypted** - Demonstrates how to create Column Master Key and Column Encryption Key for use with the Java Key Store for Always Encrypted feature.

3. azureactivedirectoryauthentication
    * **AzureActiveDirectoryAuthentication** - Demonstrates how to connect to Azure SQL Databases using identities in Azure Active Directory.

4. connections
    * **ConnectDS** - Demonstrates how to connect using a data source object and retrieve data using a stored procedure.
    * **ConnectURL** - Demonstrates how to connect using a connection URL and retrieve data using an SQL statement.

5. datatypes
    * **BasicDataTypes** - Demonstrates how to retrieve and update basic SQL Server data type values.
    * **SqlXmlDataType** - Demonstrates how to store and retrieve XML data as well as how to parse XML data with the SQLXML Java data type.
    * **SpatialDatatypes** - Demonstrates how to store and retreive Spatial Data as well how to parse data using `Geometry` and `Geography` Java types defined by Microsoft JDBC Driver.

6. resultsets
    * **CacheRS** - Demonstrates how to retrieve a large set of data and control the amount of data that is fetched and cached on the client
    * **RetrieveRS** - Demonstrates how to use a result set to retrieve a basic set of data.
    * **UpdateRS** - Demonstrates how to use an updatable result set to insert, update, and delete a row of data.

7. sparse
    * **SparseColumns** - Demonstrates how to detect column sets. It also shows a technique for parsing a column set's XML output, to get data from the sparse columns.

8. constrained
    * **ConstrainedDelegation** - Demonstrates how to connect with Kerberos constrained delegation using an impersonated credential.

## Running Samples

### Pre-Requisites

* Java 10
* [Maven](http://maven.apache.org/download.cgi)
* An instance of SQL Server or SQL Azure Database that you can connect to.

### Using Maven

To run a sample, you need to provide Maven with the appropriate profile ID so that it knows which sample to run. To find them, open the associated POM file and look for the `<id>` elements within the `<profile>` section. For example, in `\src\samples\adaptive\pom.xml`, you will find: _ExecuteStoredProcedures_, _ReadLargeData_ and _UpdateLargeData_.

To run a specific sample, go to the directory that contains the sample's POM file and run the following commands:

* `mvn install -PprofileID` to compile the sample 
* `mvn exec:java -PprofileID` to run the sample under the current directory.

For example, if you wish to compile and run the _ExecuteStoredProcedures_ sample you can run:

* `mvn install -PExecuteStoredProcedures`
* `mvn exec:java -PExecuteStoredProcedures`

