/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import javax.sql.CommonDataSource;

/**
 * A factory to create connections to the data source represented by this object. This interface was added in SQL Server JDBC Driver 3.0.
 */
public interface ISQLServerDataSource extends CommonDataSource {
    /**
     * Sets the application intent.
     * 
     * @param applicationIntent
     *            A String that contains the application intent.
     */
    public void setApplicationIntent(String applicationIntent);

    /**
     * Returns the application intent.
     * 
     * @return A String that contains the application intent.
     */
    public String getApplicationIntent();

    /**
     * Sets the application name.
     * 
     * @param applicationName
     *            A String that contains the name of the application.
     */
    public void setApplicationName(String applicationName);

    /**
     * Returns the application name.
     * 
     * @return A String that contains the application name, or "Microsoft JDBC Driver for SQL Server" if no value is set.
     */
    public String getApplicationName();

    /**
     * Sets the database name to connect to.
     * 
     * @param databaseName
     *            A String that contains the database name.
     */
    public void setDatabaseName(String databaseName);

    /**
     * Returns the database name.
     * 
     * @return A String that contains the database name or null if no value is set.
     */
    public String getDatabaseName();

    /**
     * Sets the SQL Server instance name.
     * 
     * @param instanceName
     *            A String that contains the instance name.
     */
    public void setInstanceName(String instanceName);

    /**
     * Returns the SQL Server instance name.
     * 
     * @return A String that contains the instance name, or null if no value is set.
     */
    public String getInstanceName();

    /**
     * Sets a Boolean value that indicates if the integratedSecurity property is enabled.
     * 
     * @param enable
     *            true if integratedSecurity is enabled. Otherwise, false.
     */
    public void setIntegratedSecurity(boolean enable);

    /**
     * Sets a Boolean value that indicates if the lastUpdateCount property is enabled.
     * 
     * @param lastUpdateCount
     *            true if lastUpdateCount is enabled. Otherwise, false.
     */
    public void setLastUpdateCount(boolean lastUpdateCount);

    /**
     * Returns a Boolean value that indicates if the lastUpdateCount property is enabled.
     * 
     * @return true if lastUpdateCount is enabled. Otherwise, false.
     */
    public boolean getLastUpdateCount();

    /**
     * Sets a Boolean value that indicates if the encrypt property is enabled.
     * 
     * @param encrypt
     *            true if the Secure Sockets Layer (SSL) encryption is enabled between the client and the SQL Server. Otherwise, false.
     */
    public void setEncrypt(boolean encrypt);

    /**
     * Returns a Boolean value that indicates if the encrypt property is enabled.
     * 
     * @return true if encrypt is enabled. Otherwise, false.
     */
    public boolean getEncrypt();

    /**
     * Sets a Boolean value that indicates if the trustServerCertificate property is enabled.
     * 
     * @param e
     *            true if the server Secure Sockets Layer (SSL) certificate should be automatically trusted when the communication layer is encrypted
     *            using SSL. Otherwise, false.
     */
    public void setTrustServerCertificate(boolean e);

    /**
     * Returns a Boolean value that indicates if the trustServerCertificate property is enabled.
     * 
     * @return true if trustServerCertificate is enabled. Otherwise, false.
     */
    public boolean getTrustServerCertificate();

    /**
     * Sets the path (including file name) to the certificate trustStore file.
     * 
     * @param st
     *            A String that contains the path (including file name) to the certificate trustStore file.
     */
    public void setTrustStore(String st);

    /**
     * Returns the path (including file name) to the certificate trustStore file.
     * 
     * @return A String that contains the path (including file name) to the certificate trustStore file, or null if no value is set.
     */
    public String getTrustStore();

    /**
     * Sets the password that is used to check the integrity of the trustStore data.
     * 
     * @param p
     *            A String that contains the password that is used to check the integrity of the trustStore data.
     */
    public void setTrustStorePassword(String p);

    /**
     * Sets the host name to be used in validating the SQL Server Secure Sockets Layer (SSL) certificate.
     * 
     * @param host
     *            A String that contains the host name.
     */
    public void setHostNameInCertificate(String host);

    /**
     * Returns the host name used in validating the SQL Server Secure Sockets Layer (SSL) certificate.
     * 
     * @return A String that contains the host name, or null if no value is set.
     */
    public String getHostNameInCertificate();

    /**
     * Sets an int value that indicates the number of milliseconds to wait before the database reports a lock time out.
     * 
     * @param lockTimeout
     *            An int value that contains the number of milliseconds to wait.
     */
    public void setLockTimeout(int lockTimeout);

    /**
     * Returns an int value that indicates the number of milliseconds that the database will wait before reporting a lock time out.
     * 
     * @return An int value that contains the number of milliseconds that the database will wait.
     */
    public int getLockTimeout();

    /**
     * Sets the password that will be used to connect to SQL Server.
     * 
     * @param password
     *            A String that contains the password.
     */
    public void setPassword(String password);

    /**
     * Sets the port number to be used to communicate with SQL Server.
     * 
     * @param portNumber
     *            An int value that contains the port number.
     */
    public void setPortNumber(int portNumber);

    /**
     * Returns the current port number that is used to communicate with SQL Server.
     * 
     * @return An int value that contains the current port number.
     */
    public int getPortNumber();

    /**
     * Sets the default cursor type that is used for all result sets that are created by using this SQLServerDataSource object.
     * 
     * @param selectMethod
     *            A String value that contains the default cursor type.
     */
    public void setSelectMethod(String selectMethod);

    /**
     * Returns the default cursor type used for all result sets that are created by using this SQLServerDataSource object.
     * 
     * @return A String value that contains the default cursor type.
     */
    public String getSelectMethod();

    /**
     * Sets the response buffering mode for connections created by using this SQLServerDataSource object.
     * 
     * @param respo
     *            A String that contains the buffering and streaming mode. The valid mode can be one of the following case-insensitive Strings: full
     *            or adaptive.
     */
    public void setResponseBuffering(String respo);

    /**
     * Returns the response buffering mode for this SQLServerDataSource object.
     * 
     * @return A String that contains a lower-case full or adaptive.
     */
    public String getResponseBuffering();

    /**
     * Modifies the setting of the sendTimeAsDatetime connection property.
     * 
     * @param sendTimeAsDatetime
     *            A Boolean value. When true, causes java.sql.Time values to be sent to the server as SQL Server datetime types. When false, causes
     *            java.sql.Time values to be sent to the server as SQL Server time types.
     */
    public void setSendTimeAsDatetime(boolean sendTimeAsDatetime);

    /**
     * This method was added in SQL Server JDBC Driver 3.0. Returns the setting of the sendTimeAsDatetime connection property.
     * 
     * @return true if java.sql.Time values will be sent to the server as a SQL Server datetime type. false if java.sql.Time values will be sent to
     *         the server as a SQL Server time type.
     */
    public boolean getSendTimeAsDatetime();

    /**
     * Sets a boolean value that indicates if sending string parameters to the server in UNICODE format is enabled.
     * 
     * @param sendStringParametersAsUnicode
     *            true if string parameters are sent to the server in UNICODE format. Otherwise, false.
     */
    public void setSendStringParametersAsUnicode(boolean sendStringParametersAsUnicode);

    /**
     * Returns a boolean value that indicates if sending string parameters to the server in UNICODE format is enabled.
     * 
     * @return true if string parameters are sent to the server in UNICODE format. Otherwise, false.
     */
    public boolean getSendStringParametersAsUnicode();

    /**
     * Sets the name of the computer that is running SQL Server.
     * 
     * @param serverName
     *            A String that contains the server name.
     */
    public void setServerName(String serverName);

    /**
     * Returns the name of the SQL Server instance.
     * 
     * @return A String that contains the server name or null if no value is set.
     */
    public String getServerName();

    /**
     * Sets the name of the failover server that is used in a database mirroring configuration.
     * 
     * @param serverName
     *            A String that contains the failover server name.
     */
    public void setFailoverPartner(String serverName);

    /**
     * Returns the name of the failover server that is used in a database mirroring configuration.
     * 
     * @return A String that contains the name of the failover partner, or null if none is set.
     */
    public String getFailoverPartner();

    /**
     * Sets the value of the multiSubnetFailover connection property.
     * 
     * @param multiSubnetFailover
     *            The new value of the multiSubnetFailover connection property.
     */
    public void setMultiSubnetFailover(boolean multiSubnetFailover);

    /**
     * Returns the value of the multiSubnetFailover connection property.
     * 
     * @return Returns true or false, depending on the current setting of the connection property.
     */
    public boolean getMultiSubnetFailover();

    /**
     * Sets the user name that is used to connect the data source.
     * 
     * @param user
     *            A String that contains the user name.
     */
    public void setUser(String user);

    /**
     * Returns the user name that is used to connect the data source.
     * 
     * @return A String that contains the user name.
     */
    public String getUser();

    /**
     * Sets the name of the client computer name that is used to connect to the data source.
     * 
     * @param workstationID
     *            A String that contains the client computer name.
     */
    public void setWorkstationID(String workstationID);

    /**
     * Returns the name of the client computer name that is used to connect to the data source.
     * 
     * @return A String that contains the client computer name.
     */
    public String getWorkstationID();

    /**
     * Sets a Boolean value that indicates if converting SQL states to XOPEN compliant states is enabled.
     * 
     * @param xopenStates
     *            true if converting SQL states to XOPEN compliant states is enabled. Otherwise, false.
     */
    public void setXopenStates(boolean xopenStates);

    /**
     * Returns a boolean value that indicates if converting SQL states to XOPEN compliant states is enabled.
     * 
     * @return true if converting SQL states to XOPEN compliant states is enabled. Otherwise, false.
     */
    public boolean getXopenStates();

    /**
     * Sets the URL that is used to connect to the data source.
     * 
     * @param url
     *            A String that contains the URL.
     */
    public void setURL(String url);

    /**
     * Returns the URL that is used to connect to the data source.
     * 
     * @return A String that contains the URL.
     */
    public String getURL();

    /**
     * Sets the description of the data source.
     * 
     * @param description
     *            A String that contains the description.
     */
    public void setDescription(String description);

    /**
     * Returns a description of the data source.
     * 
     * @return A String that contains the data source description or null if no value is set.
     */
    public String getDescription();

    /**
     * Sets the current network packet size used to communicate with SQL Server, specified in bytes.
     * 
     * @param packetSize
     *            An int value containing the network packet size.
     */
    public void setPacketSize(int packetSize);

    /**
     * Returns the current network packet size used to communicate with SQL Server, specified in bytes.
     * 
     * @return An int value containing the current network packet size.
     */
    public int getPacketSize();

    /**
     * Indicates the kind of integrated security you want your application to use.
     * 
     * @param authenticationScheme
     *            Values are "JavaKerberos" and the default "NativeAuthentication".
     */
    public void setAuthenticationScheme(String authenticationScheme);

    /**
     * Sets the server spn
     * 
     * @param serverSpn
     *            A String that contains the server spn
     */
    public void setServerSpn(String serverSpn);

    /**
     * Returns the server spn
     * 
     * @return A String that contains the server spn
     */
    public String getServerSpn();
}
