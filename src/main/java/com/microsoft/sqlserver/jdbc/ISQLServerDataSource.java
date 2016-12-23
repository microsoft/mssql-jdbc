//---------------------------------------------------------------------------------------------------------------------------------
// File: ISQLServerDataSource.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), 
//  to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//  and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
//  IN THE SOFTWARE.
//---------------------------------------------------------------------------------------------------------------------------------
 

package com.microsoft.sqlserver.jdbc;
import javax.sql.CommonDataSource;


/**
* A factory to create connections to the data source represented by this object. This interface was added in SQL Server JDBC Driver 3.0.
*/
public interface ISQLServerDataSource extends CommonDataSource
{
	/**
	 * Sets the application intent.
	 * @param applicationIntent A String that contains the application intent.
	 */
    void setApplicationIntent(String applicationIntent);
	
	/**
     * Returns the application intent.
     * @return A String that contains the application intent.
     */
    String getApplicationIntent();
	
	/**
     * Sets the application name.
     * @param applicationName A String that contains the name of the application.
     */
    void setApplicationName(String applicationName);
	
	/**
	 * Returns the application name.
	 * @return A String that contains the application name, or "Microsoft JDBC Driver for SQL Server" if no value is set.
	 */
    String getApplicationName();
	
	/**
     * Sets the database name to connect to.
     * @param databaseName A String that contains the database name.
     */
    void setDatabaseName(String databaseName);
	
	/**
     * Returns the database name.
     * @return A String that contains the database name or null if no value is set.
     */
    String getDatabaseName();
	
	/**
     * Sets the SQL Server instance name.
     * @param instanceName A String that contains the instance name.
     */
    void setInstanceName(String instanceName);
	
	/**
     * Returns the SQL Server instance name.
     * @return A String that contains the instance name, or null if no value is set.
     */
    String getInstanceName();
	
	/**
     * Sets a Boolean value that indicates if the integratedSecurity property is enabled.
     * @param enable true if integratedSecurity is enabled. Otherwise, false.
     */
    void setIntegratedSecurity(boolean enable);
	
	/**
     * Sets a Boolean value that indicates if the lastUpdateCount property is enabled.
     * @param lastUpdateCount true if lastUpdateCount is enabled. Otherwise, false.
     */
    void setLastUpdateCount(boolean lastUpdateCount);
	
	/**
     * Returns a Boolean value that indicates if the lastUpdateCount property is enabled.
     * @return true if lastUpdateCount is enabled. Otherwise, false.
     */
    boolean getLastUpdateCount();
	
	/**
     * Sets a Boolean value that indicates if the encrypt property is enabled.
     * @param encrypt true if the Secure Sockets Layer (SSL) encryption is enabled between the client and the SQL Server. Otherwise, false.
     */
    void setEncrypt(boolean encrypt);
	
	/**
     * Returns a Boolean value that indicates if the encrypt property is enabled.
     * @return true if encrypt is enabled. Otherwise, false.
     */
    boolean getEncrypt();
	
	/**
     * Sets a Boolean value that indicates if the trustServerCertificate property is enabled.
     * @param e true if the server Secure Sockets Layer (SSL) certificate should be automatically trusted when the communication layer is encrypted using SSL. Otherwise, false.
     */
    void setTrustServerCertificate(boolean e);
	
	/**
     * Returns a Boolean value that indicates if the trustServerCertificate property is enabled.
     * @return true if trustServerCertificate is enabled. Otherwise, false.
     */
    boolean getTrustServerCertificate();
	
	/**
     * Sets the path (including file name) to the certificate trustStore file.
     * @param st A String that contains the path (including file name) to the certificate trustStore file.
     */
    void setTrustStore(String st);
	
	/**
     * Returns the path (including file name) to the certificate trustStore file.
     * @return A String that contains the path (including file name) to the certificate trustStore file, or null if no value is set.
     */
    String getTrustStore();
	
	/**
     * Sets the password that is used to check the integrity of the trustStore data.
     * @param p A String that contains the password that is used to check the integrity of the trustStore data.
     */
    void setTrustStorePassword(String p);
	
	/**
     * Sets the host name to be used in validating the SQL Server Secure Sockets Layer (SSL) certificate.
     * @param host A String that contains the host name.
     */
    void setHostNameInCertificate(String host);
	
	/**
     * Returns the host name used in validating the SQL Server Secure Sockets Layer (SSL) certificate.
     * @return A String that contains the host name, or null if no value is set.
     */
    String getHostNameInCertificate();
	
	/**
     * Sets an int value that indicates the number of milliseconds to wait before the database reports a lock time out.
     * @param lockTimeout An int value that contains the number of milliseconds to wait.
     */
    void setLockTimeout(int lockTimeout);
	
	/**
     * Returns an int value that indicates the number of milliseconds that the database will wait before reporting a lock time out.
     * @return An int value that contains the number of milliseconds that the database will wait.
     */
    int getLockTimeout();
	
	/**
     * Sets the password that will be used to connect to SQL Server.
     * @param password A String that contains the password.
     */
    void setPassword(String password);
	
	/**
     * Sets the port number to be used to communicate with SQL Server.
     * @param portNumber An int value that contains the port number.
     */
    void setPortNumber(int portNumber);
	
	/**
     * Returns the current port number that is used to communicate with SQL Server.
     * @return An int value that contains the current port number.
     */
    int getPortNumber();
	
	/**
     * Sets the default cursor type that is used for all result sets that are created by using this SQLServerDataSource object.
     * @param selectMethod A String value that contains the default cursor type.
     */
    void setSelectMethod(String selectMethod);
	
	/**
     * Returns the default cursor type used for all result sets that are created by using this SQLServerDataSource object.
     * @return A String value that contains the default cursor type.
     */
    String getSelectMethod();
	
	/**
     * Sets the response buffering mode for connections created by using this SQLServerDataSource object.
     * @param respo A String that contains the buffering and streaming mode. The valid mode can be one of the following case-insensitive Strings: full or adaptive.
     */
    void setResponseBuffering(String respo);
	
	/**
     * Returns the response buffering mode for this SQLServerDataSource object.
     * @return A String that contains a lower-case full or adaptive.
     */
    String getResponseBuffering();
	
	/**
     * Modifies the setting of the sendTimeAsDatetime connection property.
     * @param sendTimeAsDatetime A Boolean value. When true, causes java.sql.Time values to be sent to the server as SQL Server datetime types.
	 * When false, causes java.sql.Time values to be sent to the server as SQL Server time types.
     */
    void setSendTimeAsDatetime(boolean sendTimeAsDatetime);
	
	/**
     * This method was added in SQL Server JDBC Driver 3.0. Returns the setting of the sendTimeAsDatetime connection property.
     * @return true if java.sql.Time values will be sent to the server as a SQL Server datetime type. false if java.sql.Time values will be sent
	 * to the server as a SQL Server time type.
     */
    boolean getSendTimeAsDatetime();
	
	/**
     * Sets a boolean value that indicates if sending string parameters to the server in UNICODE format is enabled.
     * @param sendStringParametersAsUnicode true if string parameters are sent to the server in UNICODE format. Otherwise, false.
     */
    void setSendStringParametersAsUnicode(boolean sendStringParametersAsUnicode);
	
	/**
     * Returns a boolean value that indicates if sending string parameters to the server in UNICODE format is enabled.
     * @return true if string parameters are sent to the server in UNICODE format. Otherwise, false.
     */
    boolean getSendStringParametersAsUnicode();
	
	/**
     * Sets the name of the computer that is running SQL Server.
     * @param serverName A String that contains the server name.
     */
    void setServerName(String serverName);
	
	/**
     * Returns the name of the SQL Server instance.
     * @return A String that contains the server name or null if no value is set.
     */
    String getServerName();
	
	/**
     * Sets the name of the failover server that is used in a database mirroring configuration.
     * @param serverName A String that contains the failover server name.
     */
    void setFailoverPartner(String serverName);
	
	/**
     * Returns the name of the failover server that is used in a database mirroring configuration.
     * @return A String that contains the name of the failover partner, or null if none is set.
     */
    String getFailoverPartner();
	
	/**
     * Sets the value of the multiSubnetFailover connection property.
     * @param multiSubnetFailover The new value of the multiSubnetFailover connection property.
     */
    void setMultiSubnetFailover(boolean multiSubnetFailover);
	
	/**
     * Returns the value of the multiSubnetFailover connection property.
     * @return Returns true or false, depending on the current setting of the connection property.
     */
    boolean getMultiSubnetFailover();
	
	/**
     * Sets the user name that is used to connect the data source.
     * @param user A String that contains the user name.
     */
    void setUser(String user);
	
	/**
     * Returns the user name that is used to connect the data source.
     * @return A String that contains the user name.
     */
    String getUser();
	
	/**
     * Sets the name of the client computer name that is used to connect to the data source.
     * @param workstationID A String that contains the client computer name.
     */
    void setWorkstationID(String workstationID);
	
	/**
     * Returns the name of the client computer name that is used to connect to the data source.
     * @return A String that contains the client computer name.
     */
    String getWorkstationID();
	
	/**
     * Sets a Boolean value that indicates if converting SQL states to XOPEN compliant states is enabled.
     * @param xopenStates true if converting SQL states to XOPEN compliant states is enabled. Otherwise, false.
     */
    void setXopenStates(boolean xopenStates);
	
	/**
     * Returns a boolean value that indicates if converting SQL states to XOPEN compliant states is enabled.
     * @return true if converting SQL states to XOPEN compliant states is enabled. Otherwise, false.
     */
    boolean getXopenStates();
	
	/**
     * Sets the URL that is used to connect to the data source.
     * @param url A String that contains the URL.
     */
    void setURL(String url);
	
	/**
     * Returns the URL that is used to connect to the data source.
     * @return A String that contains the URL.
     */
    String getURL();
	
	/**
     * Sets the description of the data source.
     * @param description A String that contains the description.
     */
    void setDescription(String description);
	
	/**
     * Returns a description of the data source.
     * @return A String that contains the data source description or null if no value is set.
     */
    String getDescription();
	
	/**
     * Sets the current network packet size used to communicate with SQL Server, specified in bytes.
     * @param packetSize An int value containing the network packet size.
     */
    void setPacketSize(int packetSize);
	
	/**
     * Returns the current network packet size used to communicate with SQL Server, specified in bytes.
     * @return An int value containing the current network packet size.
     */
    int getPacketSize();
	
	/**
     * Indicates the kind of integrated security you want your application to use.
     * @param authenticationScheme Values are "JavaKerberos" and the default "NativeAuthentication".
     */
    void setAuthenticationScheme(String authenticationScheme);
	
	/**
	 * Sets the server spn
	 * @param serverSpn A String that contains the server spn
	 */
    void setServerSpn(String serverSpn);
	
	/**
	 * Returns the server spn
	 * @return A String that contains the server spn
	 */
    String getServerSpn();
}

