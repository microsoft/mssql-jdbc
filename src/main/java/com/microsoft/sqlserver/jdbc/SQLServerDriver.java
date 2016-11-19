//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerDriver.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the ""Software""), 
//  to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//  and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
//  IN THE SOFTWARE.
//---------------------------------------------------------------------------------------------------------------------------------
 
 
/*
Version History

18Aug2000 2.03 DPM First Release

31Aug2000 2.04
   Reduced packet size to 512 for SQL Server 6.5 - this is the maxl
   Log reports 2.04 not 2.4
   Log Write and stram made static
   replace SQLException with SQLServerExceptions to make sure they are logged

05Sep2000 DPM 2.05
   New types added to handled 6.5 CHAR() and BINARY() not null definitions
   Columns.java ArrayIndexException deriveType() sent native type negative
   Allow binary, varbinary, image to be returned as result set getString()
   Use correct code page to build string in result set getString()
   All datatypes converted from byte to integer to avoid issues with bytes creating -ve integers during conversion
   All SQLExceptions are SQLServerExceptions which log the exception to the log file with a stack trace
   Callable return values now convert V6 data types and derive data types based on length
   Switch to allow operations with new and old tds comms modules
   Modified processing of return values for SQL 6.5 for StreamRetValue

05Sep2000 DPM 2.06
   More work on new IO architecture and performance
   Used SQL pieces to do prepared statements
   Handle quotes correctly in prepared Statement setString()
   Changed all public classes to SQLServer* otherwise ambiguity compile errors with Optional API
   Dates before 1901 displayed as year 2037
   Property logfile now is independent log file than the one control by the driver manager
    (which does not log multiple connections)

14Sep2000 DPM 2.07
   Prepared stmd and select with a singe ' in SQL executed with createStatement(type, conc) got syntax error - must replace ' with "
   Before first now sets rowset row = -1 and rowset.rows to 0 to force a rowset refresh. The previous setPostions were overwriting the contents of the rowset
   Stmt default fetch size set to 20 (from 2000) since its only used bringing server side rowsets into memory. 2000 is way to high
   Prefix N'..' to sp_cursoropen SQL stmts for V7
   Native type uniqueidentifier could not be returned via result set getXXX()

19Sep2000 DPM 2.08
   Implement connection setCatalog
   Allow TDS_ENV_CHG messages in getMoreResults loop for setCatalog. Just eat them..
   Any messages in getMoreResults are just logged. Previoulsy caused a bad packet error.
   Clear in stream when the last packet of a command is sent out. Set in buffer len and index to 0. Previously this was not being done correctly everywhere
   Dump in buffer when get bad packets
   Removed warning message re JDK 1.2 required

21Sep2000 DPM 2.09
   Finsihed replacement of original IO system.
   Null TEXT, NTEXT and IMAGE in result set were reporting wasNull() as false
   Callable statements did not return any null param back right. Driver hung for char() or varchar()
   Fixed index errors when calling app set and registered out of range callable statment params
   Checked for nulls returning params from callable statments
   Logging moved back to driver since >1 connection needs to write to single log file
   Connection passed into everything so SQLServerException can log the connection ID
   Handle callable syntax like ?=Callxxx(?..)
   Changed server side cursors to manage result set calls to sp_xxx() with JDBC callable statements
   All callable statement output returned as result sets, not return value packets (SQL 2000 does not appear to write them)

02Oct2000 DPM 2.10
   Changed connection pool to not use static vectors. Previously pool returned connections to DB2 from pool2
   Set resultset to null for every doExecute. Previously s1 doing a select then update would report result set generated on the executeUpdate()
   Check NRSType and RSConcur not null in call to stmt.getResultSetType and Concurrency
   Return false for scrolling specified if both concur and type are specified but specified as the default values
   If a command other than select is issued with a statment that specifies concurrency and type then execute it normally - IE not via a cursor_open
   Implemenet stmt.setMaxRows as pre and post append to the sql statement sent. SET ROWCOUNT=x

18Oct2000 DPM 2.11
   PreparedStmt.setNull() and ResultSet.updateString() allow string of null. If so call setNull(), updateNull etc
   Statment.getWarnings() now does not throw not implemented exception, just returns null
   Fixed bug causing invalid packet header. Previously the code tried to form the packet size before it had read enough bytes

31Oct2000 DPM 2.12
   Check all DONE packets for valid hasPacket() status before returning the row count from the packet
   If no result set and no update count is available from getMoreResults set update count to -1 in accordance with JDBC spec
   Support getErrorCode and getSQLState for SQLException
   Support for BLOB and CLOB
   Now deployed obfuscated and in one JAR file (removed 2nd Optional API JAR file)
   Fixed updatable result set meta data returning a column count including the rowstat column.
   Support BIGINT in SQL 2000
   Support Result Set refresh row
   Once the input buffer exceeds its initial size increase it in big chunks to avoid excessive extensions of it for large result sets

27Nov2000 DPM 2.13
   Max concurrent connections for unpaid licenses is down from 2 to 1.
   Rowset Support
   Distributed XA support

01Dec2000 DPM 2.14
  2.13 went out with 2.12 as the embedded version number..
  Used DONEINPROC transaction state flag to correctly handle combinations of result sets and update counts generated from a stored procedure
  Generally reduced number of objects created for many operations. This reduces time to alloc object and garbage collect them. This gave ~15% speed gain. Use OptimizeIt.
  Removed finalizers that only set variables to null. These take time to register and fire and do nothing useful
  IO buffers for input and output merged as one buffer
  An object can specify its original IOBuffer size. Connections required less than statements.
  IOBuffer - Removed temporary char[] used to send commands
  IOBuffer - reduced temporary byte[] used to build StreamXXX packets
  Reduce result set IOBuffer extension size for first extension but then double buffer extension size each time an extension is required. Better for large result sets
  getRowsetField returns Strings, not StringBuffers now.
  Building the String to send for a prepared statement is now done with a common StringBuffer rather than concatenating may strings
  Fixed bug where number of columns returned in ResultSetMetaData was one too few for server side cursors

09Dec2000 DPM 2.15
  Removed indexing of whole result set when it is first read. Now index the current row only
  Upped default fetch size for server side cursors 20->100
  Allow client side cursors to scroll.
  Fix bug where negative shorts were returned positive.
  If a stored proc returns update counts before a result set skip them if the app called executeQuery()

20Dec2000-29Dec2000 DPM 2.16
  Drop the 'N' national character prefix when the length of data > 4000 chars. Else get //More than this get error - 'Error converting data type ntext to text'
  Statement parameters - handle single quote in LONGVARCHAR (CLOB) as well as CHAR (String).
  Prepared params for large binary object - ensure statment string has extra 1K of bytes to avoid realloc of string buffer during conversion to hex
  When sending string command (IE not login) send from the command string itself rather than copying the string into the extendable IO buffer. Previously attempted inserts of >5MB images failed
  Version V7 flag was not propagated to cloned connections during pooling. This caused pooling to SQL 6.5 db's to fail

04Jan2001- DPM 2.17
  Catch no such method on DBComms.setReceiveBufferSize since it fails on some JVM 1.2's
  Result set insert row, update row and delete row now read for TDS_DONEPROC to check successful operation or else throw SQLException - EG data too long
  Obey setMaxRows() for server side cursors in addition to client side cursors.
  Fixed bug that reported zero length TEXT, NTEXT or IMAGE as null
  Make data source property names consistent with JDBC spec serverName instead of host etc.
  Start of JDBC 3.0 features - Savepoints
  Close server cursors - server side cursors were never being closed.

24Jan2001 - DPM 2.18
  Moved command logging to IOBuffer.sendCommand to get log of everything sent
  When get 'exhausted input ..' exception dump the in buffer.
  For output params: if app specifies registerOutParam(CHAR) register it as VARCHAR instead. Its hard to think of cases where this would not be correct and it takes the onus off the app to specify VARCHAR instead of char
  Made connection thread safe. Database comms now synchronized between >1 statement on the connection. NOTE * Result set and statement are not thread safe.
  Fixed rs.getBinaryStream & getAsciiStream which generated a null pointer exception if the column data was null.

5Feb2001 - 20Feb01 DPM 2.19
  For versions >= 7.0 use 'SID', not 'SUID' in schema queries in db meta data. SQL 2000 deprecated SUID.
  Fixed bug returning datetimes's out by an hour in daylight savings hours.
  Fixed bug where a pending transaction on the previous use of a pooled connection handle propagated to the next use of the connection handle

22Feb01 - DPM 2.20
  If a client side cursor hit end with a next() then any subsequent next() must return false
  Stored procs returning multiple result sets. If one of the results sets has 0 rows got 'input exhausted' message. Need to search for DONEINPROC as well as DONEPROC to mark end of result set

07Mar2001 - DPM 2.21
  Datasource setSqlversion changed to setSqlVersion(). Added setDatabase() and getDatabase()
  Support setURL() and getURL() in datasources. Popular (but not mandatory) property..
  Remove parsing for escape  '\'. Parsing a constant string in a prepare() or prepareCall() threw a count parameters exception
  Implemenent JDBC 3.0 pool management in SQLServerPoolingDataSource
  serverName and portNumber were not processed by SQLServerDriver. Previously only worked for data sources, now work via driver manager
  Support setObject(o) where o is a Short.
  getObject() for a smallint should return object type Short. integerToObject returns short for short and tiny types
  Added support for cancel a statement and query timeouts
  Support for Driver Manager and Data source connection login timeout
  Changed Datasource setPort(String) to setPort(int)
  Added support for getInt, Long, Float() etc where the source column is CHAR or VARCHAR

22Mar2001 - DPM 2.22
  Changed sp_cursor to allow update of null on updatable result sets. Now this call uses declare @param=value etc. From SQL 7.0 SP2 and SQL 2000 on, the syntax exec sp_cursor ..., @column1 = null gives 'Incorrect syntax near type'
  Allow result sets from callable statements to use absolute positioning. IE make them not "serverSide()=true"
  Another attempt at getting datestamps to return correctly. Datetimes out by 1 hours as daylight savings changed in some time zones. Use complained some time stamps still out by 60 minutes
  JDBC 3.0 Connector code - start.
  Ensure new JDBC 3.0 connector code does not ship in 2.0 and then get class not found errors at runtime.

03Apr2001 - DPM 2.23
  Fixes to run JDBCCTS certification tests :-
  DBMeta - attempt to change to different catalog if its specified in the call.
  DBMeta - return a non null, empty result instead of null for no tables, columns etc calls
  DBMeta - make connection.getCatalog and dbmeta.getCatalogs() return upper case to match between them
  ResultSet - throw exception for negative setFetchSize.
  ResultSet - allow char to timestamp and char to bit conversion for reads.
  BatchUpdate - If the batch is cleared should return result int[0] to caller, dont throw exception
  BatchUpdate - Propagate the result int[] to any BatchUpdateException
  BatchUpdate - disallow select in batch - throw BatchUpdateException
  EscapeSyntax- return "" to query for functions supported.
  Stmt - throw exceptions for -ve set of query timeout, max column size or max rows
  Stmt - Fix for empty result set generated followed by call to getMoreResults()

  Changed checkLicense - previous method of selecting connections from DB was not thread safe for many threads starting up connections simulataneously
  Bugs in precsion and scale - reviewed and corrected for all data types

19Apr2001 - DPM 2.24
  Renamed SQLServerPoolingDataSource to SQLServerConnectionPool. This class is no longer derived from datasource
  Positions indexes in Blob and CLob must start at 1, not 0 - see JDBC spec
  If auto commit is on and the app calls commit() dont execute a begin transaction.
  Remove difference in logic callable statement vs prepared stmt in getNextResult().
    This is required to ensure update counts are correctly returned for stored procedures executed via a prepared stmt with an "EXEC"
    Summary: 2.23 allows exec via prepared statements, 2.24 returns correct update counts for exec
  DBMeta.getMaxStatements() changed to return 0 unlimited not 1
  Fix 'DBComms.negative data length'. Sometimes SQL Server does not return the full packet length for packets before the last packet
  Licenses are now version specific. IE a 2.x license will fail in a 3.x driver

04May2001 - DPM 2.25
  A single space from a char() column was returned as null. This behaviour should only be true in SQL 6.5 mode
  If updateByte(n) and n is < 0 make n = 256+n. Required to pass inet performance test
  Allow situation with no actual column updates. Required to pass inet performance test
  stmt.indexRow() - fixed bug where a prepared statement was executed with some params to create a result set with rows, then executed with new params which returned a null result set. However the 2nd row of the first result set was returned in error.  checked indexRow does not go beyond end of receieved data from dbms.
  CallableStatement.getObject did not use correct index to retrieve the parameters type if some input only params were in the param list before the out params
  Added new property to specify character set for single byte columns. 'codepage'
  License check for 2.x driver incorrectly checked if the license is version specific. USers installing old licenses (before version aware) into current releases failed the check
  Performance analysis again :
    - make most classes final, those not finalizable - make most methods final
    - replace use of Vector with ArrayList
    - use RPC to execute all sp_cursors to insert, update and delete from server side cursors
    - rewrote the building of SQL cmds to send to db. Minimized use of string concatenation to expand prepared statements. USed global char[] buffer instead
  Catch SocketException if setTcpNoDelay fails on socket creation - this is not supported in Oracle App Server
  Try to set socket KeepAlive on to resolve the numerous reports of 'connection reset by peer'
  Updates to result sets now done with a new temporary statement from the result set. Previously the building of the sp_cursor call and subsequent packets back from the db corrupted the original result set which was using the same io buffer

30May2001 - DPM 2.251
  2.25 was corrupting the result of rs.getString(n) with the value of rs.getString(n-1). Caused read corruption with VARCHAR

30May2001 - DPM 2.26
  Use sp_prepare, column parsing and sp_execute to handle prepared statements
  Generate correct call stmt params numbers for param count 1->99, 2.25 created a syntax error if the param number was > 30
  Setting the "portNumber" via connection properties (therefore datasources etc) was ignored
  Execute callable statements with RPC. Rewrite of large parts of stored procedures
  TDS_DONE and DONEPROC are returned only if they contain an update count or are final. Removed more convoluted logic for processing these packets
  DbComms.receive could read some of the packet's data bytes if it had to read the packet header in >1 chunk. Code was changed to stop this
  Connection setCatalog changed the database but did not set sCatalog that is used to return connection.getCatalog(). Also getTables gave error if the user was connected to any database, EG none specified on the connect URL
  ResultSet.Close() no longer sets its stmt to null. The statement is still required because the result set's statement needs to be able to move to the end of the result set to retrieve more results. Previoulsy application received a null pointer using absolute to move to the end of the result set if the user app closed the result set. See test ResultSetMoreAfterClose.
  Added optimized handling of result set getString() if the default encoding is US Ascii
  Database meta data now creates a new statement and result set instance to handle nested calling of result set functions. Eg get all columns of all tables in the database. User reported this failed with IDE Forte's capture database schema feature.
  Ensure logon failure exceptions contain the specific SQL Server error code and state.
  Codepage setting was not being propagated to pooled connections.
  Allow datasource setXXX() properties to override the values set in datasource.setURL(). Previously if setURL() was used, no other property set with setXXX() was examined
  Commit/Rollback and start new transaction now separate methods.
  XADataSource does not begin a transaction now, XAResource.start begins the transaction since for 'never' setting of container managed transactions app server will never start a transaction. In that case the app server will never call XAResource.start()
  Add JDBC 3.0 pooling parameters to data source and connection pool. Doc with JAva doc for user HTML
  If executeUpdate generates a result set thow the exception but do not display it to System.out
  When a paramter is set clear its bNull flag. Websphere resues prepared statements across bean activations and clears their params after each execute

30May2001 - DPM 2.261
  Converting timestamps to datetime for dates < 1970 created incorrect dates and times since the ms values was < 0
  Allow prepared insert expressions with mixed constants and ? to pass parsing. EG INSERT INTO TABLE (COL1, COL2) VALUES (1, ?)
  Ensure '?' embedded in literals do not get replaced with parameter markers when preparing statements
  Use a smaller char[] to expand parameter  markers for long sql statements. Previously the expanded statement was x4 the original which caused SQL Server to close the connection socket.
  ALlow Types.NUMERIC as a type for registering out params with RPC.

24Jul2001 - DPM 2.262
  Connection property "disablePrePrepare" to disable use of new pre preparation of prepared statements.
  Statement.sendParamsByRPC did not include switch for all the java.sql.Types.* possibly registered with registerOutParam - registerOutPram now maps them to types suported ty preparedStatment.setXXX()
26Jul2001 - DPM 2.263
  Disable sp_prepare until statement pooling is implemented to reduce memory usage
  Add N prefix to char data > 4000 characters for text mode parameter expansion. Previously CHAR or VARCHAR params were sent to DBMS without the N national character prefix when the data was > 4000 chars due to reported "'Error converting data type ntext to text'". Customer complained wrong unicode > 4000 chars and this convert error cannot be recreated..so N is prefixed to all char data now
26Jul2001 - DPM 2.264
  Moved prepared statement preparation to connection for prepared handle pooling
  Ensure all java.sql.Types.* can be set using prepStmt.setNull(n, *);
30Jul2001 - DPM 2.265
  Do not search for TDS_DONE when bOnlyResultSets is true (for executeQuery())
30Jul2001 - DPM 2.266
  Correct 0 index in prepared statement setClob() and setBlob()
  If string data length is > 4000 (not 8000) send as LONGCHAR data
07Aug2001 - DPM 2.267
  Fix fatal comms errors notifications. Previously a connection pool was not notified about a fatal comms error on a connection.
  When a pooled connection is closed only reset it to CLOSED (available) if errors occurred on the connection
  Prepared and callable statement setNull(n, Types.BIGINT) resulted in 0 , not null being passed.
  Pooled connections that have experienced fatal IO errors removed from pool list
  Renamed disablePrePrepare property to disableStatementPooling
  New IOBuffer method to try to extend the buffer (by disk file) in low memory conditions

10Aug 2001 - DPM 2.268
  Dont allow setAutoCommit() to generate more than one level of transaction since a single rollback will leave the transaction uncomitted. IE @@TRANCOUNT should never be > 1
  Leave Debug.class obfuscated in build.
  Allow SQLServerConnectionPool to pool connections for a SQLServerXADataSource, not just SQLServerConnectionPoolDataSource
  Non XA pooled connections now issue a rollback when the connection is closed to dispose of any uncommited transactions
  The use of DBComms.LAST_PKT_NO caused index of out bounds exceptions
  Logging for RPC throws out of bounds exception if the log is on when sending a binary stream (or other large data) whereby the logging expected the length of the procedure name in the first 2 bytes but the 1st packet of the command has already be sent
  Catch a security exception if we cannot read the JVM's character encoding to set bCp1252.

10Aug2001 - DPM 2.269
  Blob.getBinaryStream issued a call for Blob.getBytes with a zero index (the index of the first byte is 1). Ditto for Clob.
  Expand propertyInfo in getPropertyInfo from size 7 to 9. Previously an array index exception was thrown just by calling this method.
  Remove all explicit calls to Runtime garbage collection
  Timeout thread now uses interrupt (not stop) to end a timeout thread. Stop is deprecated because its not threadsafe
  Added SQL_UNICODE_VARCHAR (-96) as a valid type for a null RPC paramter. IBM Persistence builder uses setNull(n, -96). Reported by HP

22Aug2001 - DPM 2.2610
  Dont attempt to send stored procedure calls with static parameters via RPC, send by batch instead. RPC can only be used if all parameters are set by setXXX()
  Connection.getCatalog should not return the db name in upper case. Ensure that if the cat was never set its returned as "master"

23Aug2001 - DPM 2.2611
  Timestamps with >0 times before 1970 were stored wrongly with RPC prepared statements. EG 12/20/1969 13:08:52 was stored 12/21/1969 13:08:52
  Database meta data buildColumns() returned non java.sql.Types values for the data type (col5) for NCHAR, NVARCHAR and NTEXT
  Prepared batch statement with setLong(). The long value was not copied to each batch param set.
  Sending a stream (binary or char/ text or RPC) now checks if less than the requested number bytes was read from the stream. Previously the code expected that all bytes requested were read. IE now the stream may provide the requested number of bytes in chunks

23Aug2001 - DPM 2.2612
  The call to callableStatement.execute() had the static param/output param check commented? The comment was removed. An attempt to call a stored procedure with static params mixed with output params now throws an exception. Static params cannot be sent via RPC and output params cannot be processed via non-RPC stored proc calls. That is because the required RET_VAL's do not appear before the final DONEPROC that the code uses to identify the end of the procedure
  If the file name is null or zero length do not attempt to set the custom log file in logger.setCustomLogger()
  If an unknown column type is reported in connection.buildParamDefinitions abort the preparation of the statement. If the showParseErrors is on display the text of the 'unknown type' exception
  Handle DataTypes.BIT1 (bit not null) when building the parameter types string for the sp_prepare in prepared statement preparation
  If an attempt is made to use setNull with jdbcType 0, use the jdbc type Types.CHAR
  If an attempt is made to set parameters on a closed prepared statement, throw the exception "stmt is closed", dont generate a null pointer exception

10Sep2001 - DPM 2.2613
  Non CallableStatements should not process DONEINPROC's since they may be generated from triggers etc. If the DONEINPROC is processed (rather than the final DONE) we loose the update count in the DONE.
  dbMetaData.getSystemFunctions() reinstated after CTS certification complete. The CTS certification tried to exercise them if they were returned as supported.
  Dont process DONEINPROC for non prepared statements - see code comments. This partially fixes a customer problem getting wrong update counts when triggers are fired.
  Datasource - dont override any username, password set via setURL() if the username/password was not set with setUsername, setPassword etc. Previoulsy a username/pass set in the URL would be overidden with "" if setUsername was not called.

170Sep2001 - DPM 2.2614
  Statement.cancel() did not work.
  Stored procedure that returns say 2 resultsets, the first of which has 0 rows. The first row of the 2nd result set is returned then we get an unknown packet error
  The variable userSQL was moved from PreparedStatement to Statement. Its now used by cachedResultSet to obtain the table name if the rowset is populated by a resultset
  ResultSet.getCharacterStream() - if the data is null return null, dont allow the null pointer exception of the previous version

02Oct2001 - DPM 2.2615
  Revised detection of DONEINPROCs again to allow correct operation of a prepared statement that executes a "execute procname" - IE a prepared statement that cannot be executed with sp_prepare, sp_execute
  Statment.targetColumnType() did not consider the case where the updating statement was a callable statement - attempts to set stored procedure params >4000 chars failed
  Cloning parameters for batcing prepared statements did not copy the values of any binary or character streams, byte arrays or big decimals

10Oct2001 - DPM 2.2616
  BLOB/CLOB and referenced classes Connection, IOBuffer, Packet Requestor etc) all made serializable or included constructors to make them serializalbe. Requested via Thought Inc Coco Base
  RPCAppendDouble did not allocate extend the out buffer by enough bytes for the 8 byte bit pattern. Caused an index out of bounds exception
  Support setObject using java.util.Date as well as java.sql.Date.

15Oct2001 - DPM 2.2617
  Cached rowset - Only report the inability to determine the underlying table name when updating it. For example, the rowset may have been populated from a callable statement and the user just wants to browser (not update) it.
  Prepared Stmt.setObject(timestamp). Move the timestamp test before the java.util.Date test. Timestamps are java.util.Dates but must be processed with timestamp specific code to retain the HH:mm:SS
16Oct2001 - DPM 2.2618
  statement.getMoreResults() call after no result set returned and updatecount=-1 returned caused a "No TDS_RET_STATUS found for stored procedure output params".
  If the syntax for a callable statement is invalid try to execute it as a preparedStatement call. This is the way 2.25 used to work
  Statement.getNextResult() - set update count to -1 if we go beyond the end of the receive buffer
  closeAllConnections() - new API added to connection pool to close connections before the pool is finalized.

16Oct2001 - DPM 2.2619
  StreamError - new mapping of some specific error numbers to state strings.
13Nov2001 - DPM 2.2620
  Some conditional code to allow compile/operation with JDK 1.1.8
27Nov2001 - DPM 2.2621
  Changed rounding arithmetic for sending timestamp paramters via RPC to avoid rounding errors
19Nov2001 - DPM 2.2622
  Changed PreparedStatement.setTimestamp. timestamp.getTime() did not include the millisecs of the timestamp for JDK <= 1.3
  For RPC for < SQL 7.0 send byte array correct tds 6.5 protocol - datatypes, data length size etc.
  Send RPC String params with maximum target type column size, not the size of the actual target column in the table
  For SQLServerJDBCRowset if the username/passw are not set for the rowset use any username/pass specified for the rowset's datasource
13Dec2001 - DPM 2.2623
  Cached rowset - when populating the rowset with a result set rewind the result set before populating the rowset in case the use app was laready at the end of the result set
19Dec2001 - DPM 2.2624
  Propagate the statement's QueryTimeout (if set) to any statements used to fetch result set cursors for server side result sets
  Handle exception if app if driver cannot call System.getProperties() to get the JDK version
02Jan2002 - DPM 2.2625
  Support the setMaxRows() call if the statement is a callable statement. This stopped working when the move was made to RPC for callable statements
07Jan2002 - DPM 2.2626
  Add support for SQLWarnings
  CallableStatement - Allow case where no parenthesis are around the callable statement params, Previously got procedure not found xxx ?,?,?
  Added calendar support for get/set timezone, time and date
  If a decimal is returned as long drop any decimal places, dont throw a number format exception

18Jan2002 - DPM 2.27
  Remove connection finalizer. See notes in code
  Support for JTA via MS DTC
  Handles the use of "{call" escape sequence using a non callable statment.

24Jan2002 - DPM 2.2701
  Corrected count of parameters with an escape sequence in a string
29Jan2002 - DPM 2.2702
  Send prepared stmts > 4000 chars via long strings in RPC
  Dont try to close the connection's prepared handles if the finalizer thread has closed the socket already
06Feb2002 - DPM 2.2703
  Dont use zSQL to build sp_prepare for prepared statements. Previously syntax that could not be parsed was execute as non-prepared.
  Dont close prepared handles when the connection is closed. SQL Server will do it automatically.
  Dont read the entire BLOB/CLOB in PreparedStatement.setBlob/Clob()
  SQLServerDataSourceObjectFactory.stringToBoolean had thrown invalid index exception.
  Calls to SQLServerConnectConnectionPoolDataSource.setManagementCycleTime() now call setPropertyCycle(). THe SQLServerConnect pool is now driven by the propertyCycleTime, not the ManagementCycleTime
06Feb2002 - DPM 2.2704
  Change message when a param value for a prepared stmt is not set. This is generated now by Connection.buildParamTypeDefinitions()
  If the prepared stmt parameter definition > 4000 send it as long text.
  Generate a connection pool notfiy error if there are packet errors in the db comms. Tell the pool to remove the physical connection
06Feb2002 - DPM 2.2705
  If there a >1 LONGVARCHAR (or CLOB) in a single row for insert/update dealy between sending the data for each to the DB
  SQLServerException now serializable
  Added some new state codes mappings for SQL exceptions
06Feb2002 - DPM 3.2706
  Changes to allow JDBC 3 mulitple open result sets
  New connection property to control lock timeouts 'lockTimeout'.
  ResultSet.last() now uses LAST (not absolute) for server side cursors since absolute -1 failed on dynamic cursors.
  Data source support various flavours of setProperty(String, value)
  ResultSet.isLast() caused the field offset tables to be replaced with the value for the last row
  ';' is a connection preoperty delimiter
  String (not byte[]) is now the type returned by getObject() for GUIDs. Required for GUID reading in cached rowsets
  Implemeted connection Failover
20Mar2002 - DPM 3.2707
  Added state code for network failure based exceptions
20Mar2002 - DPM 3.2708
  After ResultSet.updateRow() set all column params to PARAM_NOT_SET so previous row parram values are not applied to the current row to be updated (if they are not explicitly set for that row)
20Mar2002 - DPM 3.2709
  Subtle changes in thread timeout handling, suggested by customer.
  Changed ResultSet.updatedTimestamp. timestamp.getTime() did not include the millisecs of the timestamp for JDK <= 1.3
20Mar2002 - DPM 3.2710
  Reprepare stmt if the execute params values are incompatible with the prepared stmt. EG A long string needs to inserted with a prpeared stmt prepared for a short string
  Once memory is exhausted on receiving large result sets send all subsequent db input to a file buffer.
  Avoid creating another byte[] when reading large BLOBs. Memory saving.
  Check for null stmt in dbmeta.getTables()
  Support property sendStringParametersAsUnicode to allow string params to be sent in native TDS collation format, not UNICODE for performance
  Support all numeric database names by adding single quotes in its "USE" statement
30May2002 - DPM 3.2711
  Map "FOR UPDATE" to HOLDLOCK in a select. This option will lock the select rows
  Report state "08S01" when connection fails because db not up
30May2002 - DPM 3.2712
  SQLServerConnectionPool cycled thru all physical connections in the pool, pinging each one when it should have issued the first eligible connection
  SQLServerConnectionPool did not ping conncections automatically created by the connection pool (EG intial connection or top up connections)
  Detect auto-increment on numerics as well as integer columns
18Jul2002 - DPM 3.2713
  Dont override cached rowset command if its been previously set
  ALlow >1 space between 'call' and procedure name, previously this sytnax caused the call stmt to be sent as batch, not RPC
  Remove attempted mapping of "FOR UPDATE". This generates incorrect syntax if there is a WHERE CLAUSE. SELECT * FROM TEST WHERE COl<x needs to map to SELECT * FROM TEST (HOLDLOKC UPDLOCK) WHERE COl<x. IE Options after table name, not after statement
  Honour sendStringParametersAsUnicode for prepared statments that are sent by batch IE not called by RPC
18Jul2002 - DPM 3.2714
  Auto increment result set metadata columns should return readonly=true, isWritable=false. Same as MS driver
  and JDC-ODBC bridge.
  Change timestamp to database and database to to nanosecond rounding to ensure timestamp handling is identical to MS driver for updates and retrievals
23Aug2002 - DPM 3.2715
  Fixed bug where Cached rowset generated invalid sql syntax if acceptChanges was called on a row where the updates did not change the underlying data.
  Handle -85 packet caused by 'group by' SQL clause when building a servert side cursor.
  Added some support for BLOB.setOutputStream.
23Aug2002 - DPM 3.2716
  //no - Support prepStmt.setObject(stream type..)
  setMaxRows for prepStmt.execute() (not executeQuery()) did not work
08Oct2002 - DPM 3.2717
  Handle group by packets as a a type of TDS_MSG they are and correctly calculate their packet lengths when reading called procedure out values
  Return correct index for a named stored procedure param for return value ?=call(..) syntax. call(?) was OK
  correct rounding/truncation in result set getLong() from a database FLOAT.
  Changed name of license file
08Oct2002 - DPM 3.2718
  Implement param for return last update count paramter
  Implement param for boolean literal translation
  No serverName specified means use localhost
  Remove final from most connection methods  to allow wrapping the connection object.
  New pooling datasource SQLServerPoolingDataSource
  Drop support for Datasource setURL
  Fix getMoreResults. 2 concatenated insert tstatements executed with a single stmt execute never returned -1 in getUpdateCount()
  New data source classes incluidng new SQLServerPoolingDataSource, SQLServerConnectPool deprecated
  New logging common classes
08Oct2002 - DPM 3.2719
  Licensing - must check for old license first
  XA-DTC XAConnection did not report itself as XA causing the logical connection close to issue a rollback every time
  XA-DTC Logging switch on did not work
  XA - changes to handle TMJOIN on xa-start for JBOSS
  Report correct type and class meta data for SQL 2000 BIGINT
08Oct2002 - DPM 3.2720
  XA Connection was causing roll back when logical connection was closed.
  ps.setObject now tries to serialize an object if its not a known JDBC type.
  If a numeric column has scale >0 then return it as big decimal, not bigint
  Stmt.initRowset initialize index tables each call, customer reports null ptr exception in this code but cannot recreate it
  Resultset.insert and update row. Check result set is updatable. Dont throw 'The cursor identifier value provided (0) is not valid'
  Add new test to detected end of resultset for server side cursor. See notes in code

08Oct2002 - DPM 3.2721
  New 7.0 logon packet
  New connection property applicationName
  Allow setDate value to be a null date in cachedrowset

09Dec2002 - DPM 3.3000
  Support for NT Authentication
  Implement TDS 0x71 protocol
  Change BIGINT, NUMERIC, DECIMAL to use TDS protocol 0x71 type values
  connection pre-verify implemented in connection pools
  Datasource print writer can now be enabled and disabled (set to null) during the application running.
  setTransactionIsolation - check connection and socket not closed already.
  Allow prepStmt.setObject(obj) where obj is an input stream
  Add connection/thread context to DBComms network exceptions
  All getObject() now return Long (not BIGINT) for BIGINT db column types. Also applies to any data conversion that previoulsy returned BIGINTs

13Jan2003 - DPM 3.3001
  Must map user's sql to use (case col=null else..) to distinguish null values using textptr's
  Further changes to sending RPC streams to ensure no interim short packets are generated
21Jan2003 - DPM 3.3002
  Change to textptr syntax for Cognos
05Feb2003 - DPM 3.3003
  Discard any result set updates not applied with ResultSet.updateRow() when the application moves off the row.
  Enforce single machine name check. License handled is now version 'A' of license
  An expired license still allowed unlimited connections since the SQL to check existing connections used the wrong 'program name'
24Feb2003 - DPM 3.3004
  Flush db network send for statement cancel API call
  SQLServerLicense.class now required instead of License.class. Required to allow >1 Microsoft product to exist in a JVM without tripping over each others licenses (and failing on license product ID)
  If a TDS_ERR is present at the end of a result set (instead of TDS_DONE) throw the exception. Customer (Nobilis) reported that a deadlock was not reported
  ResultSet getString() from a BIGINT threw a Long() class cast exception.
  DBComms.eatCancel added to remove unprocessed cancelled packets
24Feb2003 - DPM 3.3005
  SQL Type uniqueidentifier (GUID) now returned as a String from getObject(). Previously was byte[].
  ResultSetMetaData and DatabaseMetaData returns GUID column type as Types.CHAR.
  Allow port numbers larger than short() number value.
11Mar2003 - DPM 3.3006
  DBComms.eatCancel() must wait in some cases for arrival of cancel packet.
  Authentication listener now listens on port 1533
  Lookup for named instance port for a specified named instance
  commit/rollback the current trans and start the new one in one (not 2) db round trips
  Added compile time option to synchronize setting of auto commit, see notes in code. BEA project
18Mar2003 - DPM 3.3007
  IoBuffer.bufferAppendRPCInputStream
01Apr2003 - DPM 3.3008
  Trusted connections are now closed via OleDB DLL, not the java socket level. This is required to avoid depletion of network resources ultimately causing a failure to connect.
01Apr2003 - DPM 3.3009
  Trusted connections are now implemented using ODBC. The permanent proxy socket listnener was replaced by a temporary listener on the same thread as the connection attempt
10Apr2003 - 30Apr03 DPM 3.3010
  SQLServerException handling changed to centralize errNum->state mapping
  Support for con property XOPENStates=true
  Check for and throw exception for a statement that is zero length. EG stmt.executeUpdate(""); This causes the recv from the db to wait forever.
  Disabled autoCommit optimization that was added for the BEA transaction tests.
  Trusted authentication reverted back to use OleDB since ODBC did not improve the BMC stability problems
  Trusted connection now made by calling the DLL on the main thread (not a new thread) since BMC changes the credentials of the main thread only
01May2003 - DPM 3.3011
  Password encryption on SQL Server authentication corrected to handle unicode characters
  Allow trusted authetnication with SQL 7.0 which requires two connections to connect. The first is rejected to downstep the TDS version
14May2003 - DPM 3.3012
  notifyEventMethod in connection changed from static to non static. If there are >1 connection objects they may have come from different BasePooledConnections
  notifyEventMethod must be propagated in connection cloning.
21May2003 - DPM 3.3013
  Connection.getTypeMap must not throw exception for WAS 5.0
  Map the "FOR UPDATE" syntax to use an updatable cursor (using sp_cursoropen)
21May2003 - DPM 3.3014
  Ensure that a prepared statement reprepares if a decimal parameter is encounted with a higher scale than the value the statement was prepared with
  Numerous changes to improve efficiency of how a prepared statement can access the original parameters it was prepared with
  Ensure translateBooleanLiterals works with stmt.executeUpdate(). Test for syntax translation was not present in this method
  Boolean literal processing did not correctly handle >1 boolean literal in the SQL syntax. Replace the logic
24Jun2003 - DPM 3.3015
  Only attempt InetAddress.getLocalHost() call if the license specifies a specific machine name. This call has failed with an internal NPE at a customers site
01Jul2003 - DPM 3.3016
  Do not attempt to use server side cursors if the statement is prepared with specifc Result set type and concurrency but the application does not ultimately execute a SELECT. This fix required for use with KODO JDO
01Jul2003 - DPM 3.3017
  Calendar was ignored by some changed code to convert date, timestamp etc to/from MS SQL format.
04Aug2003 - DPM 3.3018
  Start work to handle data type SQL_VARIANT string values EG generated from a EXEC sp_executesql 'some command' within a stored procedure
  Correct database IO bug where the database's packet size is set to a high value EG 32K.
08Aug2003 - DPM 3.3019
  GetPropertyInfo generated an arrayIndex exception. The array must be expanded as new properties are added.
  Changes to cancel logic to avoid driver waiting forever when a statement on a connection was cancelled and that connection was later reissued from the connection pool
03Oct2003 - DPM 3.3020
  Add a connection property to enable failover support. Previously the driver interpreted '%' in a connection propery (eg pwd) as a failover sequence (BMC reported issue)
06Nov2003 - DPM 3.3021
  New licensing structure specifies machine name, max connections and license mode (client or server).
  Discontinue old single connection mode after trial expires. Once trial expires product is now no longer usable.

29Jan2004 - DPM 3.3022 KR
  Code checked into CVS

03Feb2004 - DPM 3.3023 KR
  ProductName-<host-name> instead of just ProductName. The previous version would have failed if multiple
  Group Licenses will try to connect to the same SQLServer.

09 April2004 - 4.0000 KR
  Create a new Driver property called "selectMethod".
  Full support for Blob/Clob classes.
  Improved performance for BatchUpdates -- both Statement and PreparedStatement.
  Changes made in 3.3023 have been reverted back. The new licensing scheme will take
  care of the bug.
  No more SQLServerConnect 2.0 and SQLServerConnect 3.0 -- Just one product with both drivers JDBC 2.0
  and JDBC 3.0 Compliant.
  New Documentation -- All old logos replaced.

23 April2004 - 4.0100 KR
  New property 'iterativeBatching' to support batchUpdate backward compatibility.

27 May2004 - 4.0200 KR
  Fixed a bug for SET Statements.The isSelect() method in SQLServerStatement used to
  determine if a statement is Select or not just by the first letter of the query. Thus
  a set command would be assumed to be a select query. This has caused problems internally where
  isSelect() method is used. Fixed to look for the whole "select" word.
  New Batch implementation never cleared the statements in batch after processing them. Fixed.

18 Jun2004 - 4.0300 KR
  Removed Upper() function in DatabaseMetaData.getCatalogs(). Now return the original case. Refer Bugz-416

29 Oct2004 - 4.0300 DPM
  -Added new properties to allow connection retries. This is introduced to try to solve SBC very rapid creation of connections issue
  -Ensure connection IDs issued in a thread safe manner.
  -Have all log entries show the connection instance ID. Previously connection attempts did not.
  setCursorName. Allow a null value to be compliant with WAS 5.0 adapter com.ibm.websphere.rsadapter.ConnectJDBCDataStoreHelper
  -Added datasource property maxConnectionBacklog to limit the number of concurrent connections in progress. Resolves an SBC issue where many connections where created in a short time period. Typically only used when a server is under very heavy load

09 Nov2004 - 4.0400 DPM - Comments Added by KR
  -Code changed in AuthenticationProcessor run() method and the JDBC3.0 driver will use the
  methods available in JDK 1.4 and up for reusing the ServerSockets that were closed recently.
  -Reusing recently closed sockets by the authentication proxy avoids depletion of the socket pool
  -Added case for error code to state mapping. Error 2627 maps to SQL 92 state "23000" (Violation of primary key constraint)
  -retryConnectionCount > 0 caused >1 connection to be created in error. Now when a connection is made successfully the retry loop exits.

  A connection thread is now guaranteed to not start a new trusted connection proxy listener before
  Reusing recently closed sockets by the authentication proxy avoids depletion of the socket pool
  the proxy instance thread of a previous connection exits
  New property trustedAuthenticationPort was added to enable the trusted authentication proxy to
  listen on a unique port for every JVM instance on a single machine. Otherwise the proxy listener
  is not multi process safe on >1 JVM on a single machine

08 Dec 04 - 4.50 KR
  Got J2EE 1.4 Certified. NTLM code changes have been merged but not made public. Will be made
  public in the next major version. 
  Fixed a bug in CallableStatement and removed stack trace outputs left uncommented inadvertently.

01 Jan 05 - 4.51 KR
  NTLM Authentication implemented.New Connection properties "ntlmAuthentication" and "domain" have been
  added. "domain" property is required whenever ntlmAuthentication is turned on.
  NOTE :- THE SETTERS AND GETTERS IN SQLServerDataSource for these properties are PRIVATE, as they are
  not supposed to be made public in 4.50. AT THE RELEASE OF 5.0 MAKE SURE TO MAKE THEM PUBLIC.
  Added case for error code to state mapping. Error 2627 maps to SQL 92 state "23000" (Violation of primary key constraint)
  retryConnectionCount > 0 caused >1 connection to be created in error. Now when a connection is made successfully the retry loop exits.

02 Feb 05 - 4.52 DPM
  Provide ability to return prepared statement meta data prior to the execution of the statement
  Driver must use max precision of 38 for all decimals for SQL 2000, previously was hard coded to 28.
  prepStmt.setObject(type) added additional code to convert data to target type before sending to db
  A db datetime returned as a JDBC time now has the date component zeroed
  A callable statement param used for IN and OUT may have a different type registered for output versus the input data type supplied by a setXXX().
  DATETIME output params from a stored proc must be converted to the user's registered out type
  Prep statements sp_prepare datatype NTEXT must be modified to TEXT when using ascii string parameters property
  IOBuffer.bufferAppendRPCStringAscii did not preallocate enough bytes causing an IndexArrayOutOfBounds exception periodically
  Added 3 new classes to begin support for SSL via SQLServerConnect.
  For logging of RPC data convert the binary data to hex representation. Existing code could throw a NegativeArrayIndex exception

02 Mar 05 - 4.53 DPM
  Have the SQLServerNTAuthentication.dll issue a unique authentication proxy listening port for the driver in each JVM instance. This is required since there is no inter-JVM synchronization.
*/

package com.microsoft.sqlserver.jdbc;
import java.sql.*;
import java.util.*;
import java.text.*;
import java.util.logging.*;

/**
* SQLServerDriver implements the java.sql.Driver for SQLServerConnect.
*
*/

final class SQLServerDriverPropertyInfo
{
    private final String name;
    final String getName() { return name; }
    private final String description;
    private final String defaultValue;
    private final boolean required;
    private final String[] choices;

    SQLServerDriverPropertyInfo(
        String name,
        String defaultValue,
        boolean required,
        String[] choices)
    {
        this.name = name;
        this.description = SQLServerResource.getResource("R_" + name + "PropertyDescription");
        this.defaultValue = defaultValue;
        this.required = required;
        this.choices = choices;
    }

    DriverPropertyInfo build(Properties connProperties)
    {
        String propValue =
            name.equals(SQLServerDriverStringProperty.PASSWORD.toString()) ? "" : connProperties.getProperty(name);

        if (null == propValue)
            propValue = defaultValue;

        DriverPropertyInfo info = new DriverPropertyInfo(name, propValue);
        info.description = description;
        info.required = required;
        info.choices = choices;

        return info;
    }
}

enum SqlAuthentication {
	NotSpecified ,
	SqlPassword	,
	ActiveDirectoryPassword ,
	ActiveDirectoryIntegrated;
	
	static SqlAuthentication valueOfString(String value) throws SQLServerException
    {
		SqlAuthentication method = null;
		
        if (value.toLowerCase(Locale.US).equalsIgnoreCase(SqlAuthentication.NotSpecified.toString()))
        {
            method = SqlAuthentication.NotSpecified;
        } 
        else if (value.toLowerCase(Locale.US).equalsIgnoreCase(SqlAuthentication.SqlPassword.toString()))
        {
            method = SqlAuthentication.SqlPassword;
        }
        else if (value.toLowerCase(Locale.US).equalsIgnoreCase(SqlAuthentication.ActiveDirectoryPassword.toString()))
        {
            method = SqlAuthentication.ActiveDirectoryPassword;
        }
        else if (value.toLowerCase(Locale.US).equalsIgnoreCase(SqlAuthentication.ActiveDirectoryIntegrated.toString()))
        {
            method = SqlAuthentication.ActiveDirectoryIntegrated;
        }
        else
        {
			MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidConnectionSetting"));
			Object[] msgArgs = {"authentication", value};
			throw new SQLServerException(null , form.format(msgArgs) , null, 0 , false);         	
        }
        return method;
    }
}

enum ColumnEncryptionSetting {
	Enabled,
	Disabled;
	
	static ColumnEncryptionSetting valueOfString(String value) throws SQLServerException
    {
		ColumnEncryptionSetting method = null;
		
        if (value.toLowerCase(Locale.US).equalsIgnoreCase(ColumnEncryptionSetting.Enabled.toString()))
        {
            method = ColumnEncryptionSetting.Enabled;
        } 
        else if (value.toLowerCase(Locale.US).equalsIgnoreCase(ColumnEncryptionSetting.Disabled.toString()))
        {
            method = ColumnEncryptionSetting.Disabled;
        }
        else
        {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidConnectionSetting"));
            Object[] msgArgs = {"columnEncryptionSetting", value};        	
        	throw new SQLServerException(form.format(msgArgs), null);  
        }
        return method;
    }
}

enum KeyStoreAuthentication {
	JavaKeyStorePassword;
	// CertificateStore is not supported for the 6.0 release.
	//CertificateStore;
	
	static KeyStoreAuthentication valueOfString(String value) throws SQLServerException
    {
		KeyStoreAuthentication method = null;
		
        if (value.toLowerCase(Locale.US).equalsIgnoreCase(KeyStoreAuthentication.JavaKeyStorePassword.toString()))
        {
            method = KeyStoreAuthentication.JavaKeyStorePassword;
        } 
//        else if (value.toLowerCase(Locale.US).equalsIgnoreCase(KeyStoreAuthentication.CertificateStore.toString()))
//        {
//            method = KeyStoreAuthentication.CertificateStore;
//        }
        else
        {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidConnectionSetting"));
            Object[] msgArgs = {"keyStoreAuthentication", value};        	
        	throw new SQLServerException(form.format(msgArgs), null);  
        }
        return method;
    }
}

enum AuthenticationScheme
{
    nativeAuthentication,
    javaKerberos;
    static AuthenticationScheme valueOfString(String value) throws SQLServerException
    {
        AuthenticationScheme scheme;
        if (value.toLowerCase(Locale.US).equalsIgnoreCase(AuthenticationScheme.javaKerberos.toString()))
        {
            scheme = AuthenticationScheme.javaKerberos;
        } 
        else
        if (value.toLowerCase(Locale.US).equalsIgnoreCase(AuthenticationScheme.nativeAuthentication.toString()))
        {
            scheme = AuthenticationScheme.nativeAuthentication;
        }
        else
        {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidAuthenticationScheme"));
            Object[] msgArgs = {value};
            throw new SQLServerException(null , form.format(msgArgs) , null, 0 , false);    
        }
        return scheme;
    }
}

enum ApplicationIntent
{
	READ_WRITE("readwrite"),
	READ_ONLY("readonly");

      //the value of the enum 	
	private String value;
	
	//constructor that sets the string value of the enum
	private ApplicationIntent(String value)
	{
		this.value = value;
	}
	
	//returns the string value of enum
	public String toString()
	{
		return value;
	}
	
	static ApplicationIntent valueOfString(String value) throws SQLServerException
    {
		ApplicationIntent applicationIntent = ApplicationIntent.READ_WRITE;
		assert value !=null;
		//handling turkish i issues
		value = value.toUpperCase(Locale.US).toLowerCase(Locale.US);
		if (value.equalsIgnoreCase(ApplicationIntent.READ_ONLY.toString())) 
		{
			applicationIntent = ApplicationIntent.READ_ONLY;
		}
		else if(value.equalsIgnoreCase(ApplicationIntent.READ_WRITE.toString()))
		{
			applicationIntent = ApplicationIntent.READ_WRITE;
		}
		else
		{
			MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidapplicationIntent"));
			Object[] msgArgs = {new String(value)};
			throw new SQLServerException(null , form.format(msgArgs) , null, 0 , false);              
		}     

		return applicationIntent;
    }
}

enum SQLServerDriverStringProperty
{
	APPLICATION_INTENT("applicationIntent",	                            ApplicationIntent.READ_WRITE.toString()),
	APPLICATION_NAME("applicationName",									SQLServerDriver.DEFAULT_APP_NAME),
	DATABASE_NAME("databaseName", 										""),
	FAILOVER_PARTNER("failoverPartner",									""),
	HOSTNAME_IN_CERTIFICATE("hostNameInCertificate", 					""),
	INSTANCE_NAME("instanceName", 										""),
	PASSWORD("password", 					 							""),
	RESPONSE_BUFFERING("responseBuffering",			 					"adaptive"),
	SELECT_METHOD("selectMethod", 				 						"direct"),
	SERVER_NAME("serverName",					 						""),
	SERVER_SPN("serverSpn",						 						""),
	TRUST_STORE("trustStore",					 						""),
	TRUST_STORE_PASSWORD("trustStorePassword",			 				""),
	USER("user", 						 								""),
	WORKSTATION_ID("workstationID",				 						Util.WSIDNotAvailable),
	AUTHENTICATION_SCHEME("authenticationScheme",			 			AuthenticationScheme.nativeAuthentication.toString()),
	AUTHENTICATION("authentication", 									SqlAuthentication.NotSpecified.toString()),
	ACCESS_TOKEN("accessToken", 										""),
	COLUMN_ENCRYPTION("columnEncryptionSetting",					 	ColumnEncryptionSetting.Disabled.toString()),
	KEY_STORE_AUTHENTICATION("keyStoreAuthentication",					""),
	KEY_STORE_SECRET("keyStoreSecret",									""),
	KEY_STORE_LOCATION("keyStoreLocation",								"");

	private String name;
	private String defaultValue;

	private SQLServerDriverStringProperty(String name, String defaultValue)		
	{
		this.name = name;
	 	this.defaultValue = defaultValue;
	}

	String getDefaultValue()
	{
		return defaultValue;				
	}

	public String toString()
    {
    	return name;
	}
}

enum SQLServerDriverIntProperty
{
	PACKET_SIZE("packetSize",											TDS.DEFAULT_PACKET_SIZE),			
	LOCK_TIMEOUT("lockTimeout",					 						-1),
	LOGIN_TIMEOUT("loginTimeout", 				 						15),
	PORT_NUMBER("portNumber",					 						1433);

	private String name;
	private int defaultValue;

	private SQLServerDriverIntProperty(String name, int defaultValue)		
	{
		this.name = name;
		this.defaultValue = defaultValue;
	}
	
	int getDefaultValue()
	{
		return defaultValue;				
	}
	
	public String toString()
	{
		return name;
	}
}

enum SQLServerDriverBooleanProperty
{
	DISABLE_STATEMENT_POOLING("disableStatementPooling", 				true),
	ENCRYPT("encrypt", 													false),	
	INTEGRATED_SECURITY("integratedSecurity",			 				false),
	LAST_UPDATE_COUNT("lastUpdateCount",				 				true),
	MULTI_SUBNET_FAILOVER("multiSubnetFailover",                        false),
	SERVER_NAME_AS_ACE("serverNameAsACE",				 				false),
	SEND_STRING_PARAMETERS_AS_UNICODE("sendStringParametersAsUnicode",	true),
	SEND_TIME_AS_DATETIME("sendTimeAsDatetime", 						true),
	TRANSPARENT_NETWORK_IP_RESOLUTION("TransparentNetworkIPResolution",	true),
	TRUST_SERVER_CERTIFICATE("trustServerCertificate",					false),
	XOPEN_STATES("xopenStates", 										false);

	private String name;
	private boolean defaultValue;

	private SQLServerDriverBooleanProperty(String name, boolean defaultValue)		
	{
		this.name = name;
		this.defaultValue = defaultValue;
	}

	boolean getDefaultValue()
	{
		return defaultValue;				
	}

	public String toString()
	{
		return name;
	}
}

public final class SQLServerDriver implements java.sql.Driver
{
    static final String PRODUCT_NAME = "Microsoft JDBC Driver "+ SQLJdbcVersion.major + "." + SQLJdbcVersion.minor + " for SQL Server";
    static final String DEFAULT_APP_NAME = "Microsoft JDBC Driver for SQL Server";
    
    private static final String[] TRUE_FALSE = {"true", "false"};
    private static final SQLServerDriverPropertyInfo[] DRIVER_PROPERTIES =
    {
        //                                                               													default       																						required    available choices
        //                              property name                    													value        							 															property    (if appropriate)
    	new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.APPLICATION_INTENT.toString(),    				SQLServerDriverStringProperty.APPLICATION_INTENT.getDefaultValue(), 									false,		new String[]{ApplicationIntent.READ_ONLY.toString(), ApplicationIntent.READ_WRITE.toString()}),            	
    	new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.APPLICATION_NAME.toString(),    					SQLServerDriverStringProperty.APPLICATION_NAME.getDefaultValue(), 										false,		null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.COLUMN_ENCRYPTION.toString(),            			SQLServerDriverStringProperty.COLUMN_ENCRYPTION.getDefaultValue(),       								false,      new String[] {ColumnEncryptionSetting.Disabled.toString(), ColumnEncryptionSetting.Enabled.toString()}),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.DATABASE_NAME.toString(),       					SQLServerDriverStringProperty.DATABASE_NAME.getDefaultValue(),       									false,    	null),                        
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.DISABLE_STATEMENT_POOLING.toString(), 			Boolean.toString(SQLServerDriverBooleanProperty.DISABLE_STATEMENT_POOLING.getDefaultValue()),       	false,      new String[] {"true"}),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.ENCRYPT.toString(),                      		Boolean.toString(SQLServerDriverBooleanProperty.ENCRYPT.getDefaultValue()),      						false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.FAILOVER_PARTNER.toString(),              		SQLServerDriverStringProperty.FAILOVER_PARTNER.getDefaultValue(),           							false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.HOSTNAME_IN_CERTIFICATE.toString(),       		SQLServerDriverStringProperty.HOSTNAME_IN_CERTIFICATE.getDefaultValue(),           						false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.INSTANCE_NAME.toString(),                 		SQLServerDriverStringProperty.INSTANCE_NAME.getDefaultValue(),           								false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.INTEGRATED_SECURITY.toString(),          		Boolean.toString(SQLServerDriverBooleanProperty.INTEGRATED_SECURITY.getDefaultValue()),      			false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.KEY_STORE_AUTHENTICATION.toString(),            	SQLServerDriverStringProperty.KEY_STORE_AUTHENTICATION.getDefaultValue(),       						false,      new String[] {KeyStoreAuthentication.JavaKeyStorePassword.toString()}),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.KEY_STORE_SECRET .toString(),            			SQLServerDriverStringProperty.KEY_STORE_SECRET.getDefaultValue(),       								false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.KEY_STORE_LOCATION .toString(),            		SQLServerDriverStringProperty.KEY_STORE_LOCATION.getDefaultValue(),       								false,      null),        
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.LAST_UPDATE_COUNT.toString(),            		Boolean.toString(SQLServerDriverBooleanProperty.LAST_UPDATE_COUNT.getDefaultValue()),       			false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.LOCK_TIMEOUT.toString(),                   			Integer.toString(SQLServerDriverIntProperty.LOCK_TIMEOUT.getDefaultValue()),         					false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.LOGIN_TIMEOUT.toString(),                  			Integer.toString(SQLServerDriverIntProperty.LOGIN_TIMEOUT.getDefaultValue()),         					false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.MULTI_SUBNET_FAILOVER.toString(),            	Boolean.toString(SQLServerDriverBooleanProperty.MULTI_SUBNET_FAILOVER.getDefaultValue()),       		false,      TRUE_FALSE),        
        new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.PACKET_SIZE.toString(),                    			Integer.toString(SQLServerDriverIntProperty.PACKET_SIZE.getDefaultValue()), 							false, 		null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.PASSWORD.toString(),                      		SQLServerDriverStringProperty.PASSWORD.getDefaultValue(),           									true,       null),
        new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.PORT_NUMBER.toString(),                    			Integer.toString(SQLServerDriverIntProperty.PORT_NUMBER.getDefaultValue()),       						false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.RESPONSE_BUFFERING.toString(),            		SQLServerDriverStringProperty.RESPONSE_BUFFERING.getDefaultValue(),   									false,      new String[] {"adaptive", "full"}),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.SELECT_METHOD.toString(),                 		SQLServerDriverStringProperty.SELECT_METHOD.getDefaultValue(),     										false,      new String[] {"direct", "cursor"}),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.toString(), 	Boolean.toString(SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.getDefaultValue()),  	false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.SERVER_NAME_AS_ACE.toString(), 					Boolean.toString(SQLServerDriverBooleanProperty.SERVER_NAME_AS_ACE.getDefaultValue()),  				false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.SERVER_NAME.toString(),                    		SQLServerDriverStringProperty.SERVER_NAME.getDefaultValue(),           									false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.SERVER_SPN.toString(),                    		SQLServerDriverStringProperty.SERVER_SPN.getDefaultValue(),           									false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.TRANSPARENT_NETWORK_IP_RESOLUTION.toString(),    Boolean.toString(SQLServerDriverBooleanProperty.TRANSPARENT_NETWORK_IP_RESOLUTION.getDefaultValue()),   false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.TRUST_SERVER_CERTIFICATE.toString(),        		Boolean.toString(SQLServerDriverBooleanProperty.TRUST_SERVER_CERTIFICATE.getDefaultValue()),      		false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.TRUST_STORE.toString(),                    		SQLServerDriverStringProperty.TRUST_STORE.getDefaultValue(),           									false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString(),            		SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.getDefaultValue(),           						false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.SEND_TIME_AS_DATETIME.toString(),            	Boolean.toString(SQLServerDriverBooleanProperty.SEND_TIME_AS_DATETIME.getDefaultValue()),       		false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.USER.toString(),                          		SQLServerDriverStringProperty.USER.getDefaultValue(),           										true,       null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.WORKSTATION_ID.toString(),                 		SQLServerDriverStringProperty.WORKSTATION_ID.getDefaultValue(), 										false, 		null),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.XOPEN_STATES.toString(),                   		Boolean.toString(SQLServerDriverBooleanProperty.XOPEN_STATES.getDefaultValue()),      					false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.AUTHENTICATION_SCHEME.toString(),          		SQLServerDriverStringProperty.AUTHENTICATION_SCHEME.getDefaultValue(),      			                false,      new String[] {AuthenticationScheme.javaKerberos.toString(),AuthenticationScheme.nativeAuthentication.toString()}),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.AUTHENTICATION.toString(),          		SQLServerDriverStringProperty.AUTHENTICATION.getDefaultValue(),      			                false,      new String[] {SqlAuthentication.NotSpecified.toString(),SqlAuthentication.SqlPassword.toString(),SqlAuthentication.ActiveDirectoryPassword.toString(),SqlAuthentication.ActiveDirectoryIntegrated.toString()}),
    };
    
    //Properties that can only be set by using Properties.
    //Cannot set in connection string
    private static final SQLServerDriverPropertyInfo[] DRIVER_PROPERTIES_PROPERTY_ONLY =
    {
    //                                                               													default       																						required    available choices
    //                              property name                    													value        							 															property    (if appropriate)
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.ACCESS_TOKEN.toString(), 							SQLServerDriverStringProperty.ACCESS_TOKEN.getDefaultValue(), 											false, 		null),
    };

	private static final String driverPropertiesSynonyms[][] = {
		{"database", SQLServerDriverStringProperty.DATABASE_NAME.toString()}, 	
		{"userName",SQLServerDriverStringProperty.USER.toString()},			
		{"server",SQLServerDriverStringProperty.SERVER_NAME.toString()},		
		{"port", SQLServerDriverIntProperty.PORT_NUMBER.toString()}
  };
        static private int baseID = 0;	// Unique id generator for each  instance (used for logging).
        final private int instanceID;							// Unique id for this instance.
        final private String traceID;
        
        // Returns unique id for each instance.
        private synchronized static int nextInstanceID()
        {
            baseID++;
            return baseID;
        }
        final public String toString()
        {
            return traceID;
        }
        static final private java.util.logging.Logger loggerExternal =
            java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.Driver"); 
        final private String loggingClassName;
        String getClassNameLogging()
        {
            return loggingClassName;
        }

    private final static java.util.logging.Logger drLogger = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerDriver");
   // Register with the DriverManager
   static {
      try {
         java.sql.DriverManager.registerDriver(new SQLServerDriver());
      }
      catch (SQLException e) {
         e.printStackTrace();
      }
   }

        public SQLServerDriver()
        {
            instanceID = nextInstanceID();
            traceID = "SQLServerDriver:"  + instanceID;
            loggingClassName = "com.microsoft.sqlserver.jdbc." + "SQLServerDriver:"  + instanceID;
        }

	// Helper function used to fixup the case sensitivity, synonyms and remove unknown tokens from the
	// properties
	static Properties fixupProperties(Properties props) throws SQLServerException
	{
		// assert props !=null 
		Properties fixedup = new Properties();
		Enumeration<?> e = props.keys();
		while (e.hasMoreElements()) 
		{
			String name = (String)e.nextElement();
			String newname = getNormalizedPropertyName(name, drLogger);
			
			if(null == newname){
				newname = getPropertyOnlyName(name, drLogger);
			}
			
			if(null != newname)
			{
				String	val	= props.getProperty(name);
				if(null != val)
				{
					// replace with the driver approved name
					fixedup.setProperty(newname, val);
				}
				else
				{
					MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidpropertyValue"));
					Object[] msgArgs = {name};
					throw new SQLServerException(null , form.format(msgArgs) , null, 0 , false);	
				}
			}
			
		}
		return fixedup;
	}
	// Helper function used to merge together the property set extracted from the url and the 
	// user supplied property set passed in by the caller.  This function is used by both SQLServerDriver.connect 
	// and SQLServerDataSource.getConnectionInternal to centralize this property merging code.
	static Properties mergeURLAndSuppliedProperties(Properties urlProps, Properties suppliedProperties) throws SQLServerException
	{
		if (null == suppliedProperties) return urlProps;
		if (suppliedProperties.isEmpty()) return urlProps;

		Properties suppliedPropertiesFixed = fixupProperties(suppliedProperties);

		// Merge URL properties and supplied properties.
		for (int i=0; i<DRIVER_PROPERTIES.length; i++) 
		{
			String sProp	= DRIVER_PROPERTIES[i].getName();
			String sPropVal = suppliedPropertiesFixed.getProperty(sProp); // supplied properties have precedence
			if (null != sPropVal)
			{
				// overwrite the property in urlprops if already exists. supp prop has more precedence
				urlProps.put(sProp, sPropVal);
			}
		}
		
		//Merge URL properties with property-only properties
		for(int i=0; i<DRIVER_PROPERTIES_PROPERTY_ONLY.length; i++)
		{
			String sProp	= DRIVER_PROPERTIES_PROPERTY_ONLY[i].getName();
			String sPropVal = suppliedPropertiesFixed.getProperty(sProp); // supplied properties have precedence
			if (null != sPropVal)
			{
				// overwrite the property in urlprops if already exists. supp prop has more precedence
				urlProps.put(sProp, sPropVal);
			}
		}

		return urlProps;
	}

   /**
    * normalize the property names
    * @param name name to normalize
	* @param logger
    * @return the normalized property name
    */
	static String getNormalizedPropertyName(String name, Logger logger)
	{
		if(null == name) 
			return name;

		for (int i=0;i<driverPropertiesSynonyms.length; i++)
		{
			if(driverPropertiesSynonyms[i][0].equalsIgnoreCase(name))
			{
				return driverPropertiesSynonyms[i][1];
			}
		}
		for (int i=0;i<DRIVER_PROPERTIES.length; i++)
		{
			if(DRIVER_PROPERTIES[i].getName().equalsIgnoreCase(name))
			{
				return DRIVER_PROPERTIES[i].getName();
			}
		}
		
		if(logger.isLoggable(Level.FINER))
			logger.finer("Unknown property" + name);
		return null;
	}
	
	/**
	    * get property-only names that do not work with connection string
	    * @param name to normalize
		* @param logger
	    * @return the normalized property name
	    */
		static String getPropertyOnlyName(String name, Logger logger)
		{
			if(null == name) 
				return name;

			for(int i=0;i<DRIVER_PROPERTIES_PROPERTY_ONLY.length; i++)
			{
				if(DRIVER_PROPERTIES_PROPERTY_ONLY[i].getName().equalsIgnoreCase(name)){
					return DRIVER_PROPERTIES_PROPERTY_ONLY[i].getName();
				}
			}
			
		if(logger.isLoggable(Level.FINER))
			logger.finer("Unknown property" + name);
		return null;
	}
		
    /*L0*/ public java.sql.Connection connect(String Url, Properties suppliedProperties) throws SQLServerException 
    {
        loggerExternal.entering(getClassNameLogging(),  "connect", "Arguments not traced."); 
        SQLServerConnection result = null;
	
        // Merge connectProperties (from URL) and supplied properties from user.
        Properties connectProperties = parseAndMergeProperties(Url, suppliedProperties);
        if (connectProperties != null)
        {        	        
            result = new SQLServerConnection(toString());
            result.connect(connectProperties, null);
        }
        loggerExternal.exiting(getClassNameLogging(),  "connect", result); 
        return (java.sql.Connection) result;
    }
	

	private final Properties parseAndMergeProperties(String Url, Properties suppliedProperties) throws SQLServerException 
  	{
	if (Url == null)
		{
		throw new SQLServerException(null , SQLServerException.getErrString("R_nullConnection"), null , 0 , false);
		}

	Properties connectProperties = Util.parseUrl(Url, drLogger);
	if (connectProperties == null)
		return null;  // If we are the wrong driver dont throw an exception

	//put the user properties into the connect properties
	int nTimeout = DriverManager.getLoginTimeout();
	if (nTimeout > 0) 
		{
		connectProperties.put(SQLServerDriverIntProperty.LOGIN_TIMEOUT.toString(), new Integer(nTimeout).toString());
		}
	
	// Merge connectProperties (from URL) and supplied properties from user.
	connectProperties = mergeURLAndSuppliedProperties(connectProperties, suppliedProperties);
	return connectProperties;
	}


  /*L0*/ public boolean acceptsURL(String url)  throws SQLServerException 
	{
	    loggerExternal.entering(getClassNameLogging(),  "acceptsURL", "Arguments not traced.");    
	    
	    if (null == url) 
	    {
            throw new SQLServerException(null, SQLServerException.getErrString("R_nullConnection"), null, 0, false);
        }
	    
            boolean result = false;
            try
            {
                result = (Util.parseUrl(url, drLogger) != null);
            }
            catch (SQLServerException e)
            {
                // ignore the exception from the parse URL failure, if we cant parse the URL we do not accept em
                result = false;
            }
            loggerExternal.exiting(getClassNameLogging(),  "acceptsURL", result);     
            return result;
	}

    public DriverPropertyInfo[] getPropertyInfo(String Url, Properties Info) throws SQLServerException
    {
        loggerExternal.entering(getClassNameLogging(),  "getPropertyInfo", "Arguments not traced.");

	Properties 	connProperties = parseAndMergeProperties( Url, Info);
	// This means we are not the right driver throw an exception.
	if(null == connProperties)
		throw new SQLServerException(null , SQLServerException.getErrString("R_invalidConnection"), null , 0 , false);
        DriverPropertyInfo[] properties = getPropertyInfoFromProperties(connProperties);
        loggerExternal.exiting(getClassNameLogging(),  "getPropertyInfo");
        
        return properties;
   }

    static final DriverPropertyInfo[] getPropertyInfoFromProperties(Properties props)
    {
        DriverPropertyInfo[] properties = new DriverPropertyInfo[DRIVER_PROPERTIES.length];

        for (int i=0;i<DRIVER_PROPERTIES.length; i++)
            properties[i] = DRIVER_PROPERTIES[i].build(props);
        return properties;
    }

    public int getMajorVersion()
    {
        loggerExternal.entering(getClassNameLogging(),  "getMajorVersion");
        loggerExternal.exiting(getClassNameLogging(),  "getMajorVersion", new Integer(SQLJdbcVersion.major)); 
        return SQLJdbcVersion.major;
    }

    public int getMinorVersion()
    {
        loggerExternal.entering(getClassNameLogging(),  "getMinorVersion");
        loggerExternal.exiting(getClassNameLogging(),  "getMinorVersion", new Integer(SQLJdbcVersion.minor)); 
        return SQLJdbcVersion.minor;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException{
        DriverJDBCVersion.checkSupportsJDBC41();

    	// The driver currently does not implement JDDBC 4.1 APIs
        throw new SQLFeatureNotSupportedException(SQLServerException.getErrString("R_notSupported"));
     }   
    
   /*L0*/ public boolean jdbcCompliant() {
        loggerExternal.entering(getClassNameLogging(),  "jdbcCompliant");
        loggerExternal.exiting(getClassNameLogging(),  "jdbcCompliant", Boolean.valueOf(true)); 
        return true;
   }
}
