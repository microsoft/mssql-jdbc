/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import org.ietf.jgss.GSSCredential;


/**
 * Provides a factory to create connections to the data source represented by this object. This interface was added in
 * SQL Server JDBC Driver 3.0.
 * 
 * This interface is implemented by {@link SQLServerDataSource} Class.
 */
public interface ISQLServerDataSource extends javax.sql.CommonDataSource {

    /**
     * Sets the application intent.
     * 
     * @param applicationIntent
     *        A String that contains the application intent.
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
     *        A String that contains the name of the application.
     */
    public void setApplicationName(String applicationName);

    /**
     * Returns the application name.
     * 
     * @return A String that contains the application name, or "Microsoft JDBC Driver for SQL Server" if no value is
     *         set.
     */
    public String getApplicationName();

    /**
     * Sets the database name to connect to.
     * 
     * @param databaseName
     *        A String that contains the database name.
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
     *        A String that contains the instance name.
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
     *        true if integratedSecurity is enabled. Otherwise, false.
     */
    public void setIntegratedSecurity(boolean enable);

    /**
     * Sets a Boolean value that indicates if the lastUpdateCount property is enabled.
     * 
     * @param lastUpdateCount
     *        true if lastUpdateCount is enabled. Otherwise, false.
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
     *        true if the Secure Sockets Layer (SSL) encryption is enabled between the client and the SQL Server.
     *        Otherwise, false.
     */
    public void setEncrypt(boolean encrypt);

    /**
     * Returns a Boolean value that indicates if the encrypt property is enabled.
     * 
     * @return true if encrypt is enabled. Otherwise, false.
     */
    public boolean getEncrypt();

    /**
     * Sets the value to enable/disable Transparent Netowrk IP Resolution (TNIR). Beginning in version 6.0 of the
     * Microsoft JDBC Driver for SQL Server, a new connection property transparentNetworkIPResolution (TNIR) is added
     * for transparent connection to Always On availability groups or to a server which has multiple IP addresses
     * associated. When transparentNetworkIPResolution is true, the driver attempts to connect to the first IP address
     * available. If the first attempt fails, the driver tries to connect to all IP addresses in parallel until the
     * timeout expires, discarding any pending connection attempts when one of them succeeds.
     * <p>
     * transparentNetworkIPResolution is ignored if multiSubnetFailover is true
     * <p>
     * transparentNetworkIPResolution is ignored if database mirroring is used
     * <p>
     * transparentNetworkIPResolution is ignored if there are more than 64 IP addresses
     * 
     * @param tnir
     *        if set to true, the driver attempts to connect to the first IP address available. It is true by default.
     */
    public void setTransparentNetworkIPResolution(boolean tnir);

    /**
     * Returns the TransparentNetworkIPResolution value.
     * 
     * @return if enabled, returns true. Otherwise, false.
     */
    public boolean getTransparentNetworkIPResolution();

    /**
     * Sets a Boolean value that indicates if the trustServerCertificate property is enabled.
     * 
     * @param e
     *        true, if the server Secure Sockets Layer (SSL) certificate should be automatically trusted when the
     *        communication layer is encrypted using SSL. Otherwise, false.
     */
    public void setTrustServerCertificate(boolean e);

    /**
     * Returns a Boolean value that indicates if the trustServerCertificate property is enabled.
     * 
     * @return true if trustServerCertificate is enabled. Otherwise, false.
     */
    public boolean getTrustServerCertificate();

    /**
     * Sets the keystore type for the trustStore.
     * 
     * @param trustStoreType
     *        A String that contains the trust store type
     */
    public void setTrustStoreType(String trustStoreType);

    /**
     * Returns the keyStore Type for the trustStore.
     * 
     * @return trustStoreType A String that contains the trust store type
     */
    public String getTrustStoreType();

    /**
     * Sets the path (including file name) to the certificate trustStore file.
     * 
     * @param trustStore
     *        A String that contains the path (including file name) to the certificate trustStore file.
     */
    public void setTrustStore(String trustStore);

    /**
     * Returns the path (including file name) to the certificate trustStore file.
     * 
     * @return trustStore A String that contains the path (including file name) to the certificate trustStore file, or
     *         null if no value is set.
     */
    public String getTrustStore();

    /**
     * Sets the password that is used to check the integrity of the trustStore data.
     * 
     * @param trustStorePassword
     *        A String that contains the password that is used to check the integrity of the trustStore data.
     */
    public void setTrustStorePassword(String trustStorePassword);

    /**
     * Sets the host name to be used in validating the SQL Server Secure Sockets Layer (SSL) certificate.
     * 
     * @param hostName
     *        A String that contains the host name.
     */
    public void setHostNameInCertificate(String hostName);

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
     *        An int value that contains the number of milliseconds to wait.
     */
    public void setLockTimeout(int lockTimeout);

    /**
     * Returns an int value that indicates the number of milliseconds that the database will wait before reporting a
     * lock time out.
     * 
     * @return An int value that contains the number of milliseconds that the database will wait.
     */
    public int getLockTimeout();

    /**
     * Sets the password that will be used to connect to SQL Server.
     * 
     * @param password
     *        A String that contains the password.
     */
    public void setPassword(String password);

    /**
     * Sets the port number to be used to communicate with SQL Server.
     * 
     * @param portNumber
     *        An int value that contains the port number.
     */
    public void setPortNumber(int portNumber);

    /**
     * Returns the current port number that is used to communicate with SQL Server.
     * 
     * @return An int value that contains the current port number.
     */
    public int getPortNumber();

    /**
     * Sets the default cursor type that is used for all result sets that are created by using this SQLServerDataSource
     * object.
     * 
     * @param selectMethod
     *        A String value that contains the default cursor type.
     */
    public void setSelectMethod(String selectMethod);

    /**
     * Returns the default cursor type used for all result sets that are created by using this SQLServerDataSource
     * object.
     * 
     * @return A String value that contains the default cursor type.
     */
    public String getSelectMethod();

    /**
     * Sets the response buffering mode for connections created by using this SQLServerDataSource object.
     * 
     * @param bufferingMode
     *        A String that contains the buffering and streaming mode. The valid mode can be one of the following
     *        case-insensitive Strings: full or adaptive.
     */
    public void setResponseBuffering(String bufferingMode);

    /**
     * Returns the response buffering mode for this SQLServerDataSource object.
     * 
     * @return A String that contains a lower-case full or adaptive.
     */
    public String getResponseBuffering();

    /**
     * Sets the value to enable/disable the sendTimeAsDatetime connection property.
     * 
     * @param sendTimeAsDatetime
     *        A Boolean value. When true, causes java.sql.Time values to be sent to the server as SQL Server datetime
     *        types. When false, causes java.sql.Time values to be sent to the server as SQL Server time types.
     */
    public void setSendTimeAsDatetime(boolean sendTimeAsDatetime);

    /**
     * Returns the value of the sendTimeAsDatetime connection property. This method was added in SQL Server JDBC Driver
     * 3.0. Returns the setting of the sendTimeAsDatetime connection property.
     * 
     * @return true if java.sql.Time values will be sent to the server as a SQL Server datetime type. false if
     *         java.sql.Time values will be sent to the server as a SQL Server time type.
     */
    public boolean getSendTimeAsDatetime();

    /**
     * Sets a boolean value that indicates if sending string parameters to the server in UNICODE format is enabled.
     * 
     * @param sendStringParametersAsUnicode
     *        true if string parameters are sent to the server in UNICODE format. Otherwise, false.
     */
    public void setSendStringParametersAsUnicode(boolean sendStringParametersAsUnicode);

    /**
     * Returns whether sending string parameters to the server in UNICODE format is enabled.
     * 
     * @return true if string parameters are sent to the server in UNICODE format. Otherwise, false.
     */
    public boolean getSendStringParametersAsUnicode();

    /**
     * Sets whether the serverName will be translated from Unicode to ASCII Compatible Encoding (ACE).
     * 
     * @param serverNameAsACE
     *        if enabled the servername will be translated to ASCII Compatible Encoding (ACE)
     */
    public void setServerNameAsACE(boolean serverNameAsACE);

    /**
     * Returns if the serverName should be translated from Unicode to ASCII Compatible Encoding (ACE).
     * 
     * @return if enabled, will return true. Otherwise, false.
     */
    public boolean getServerNameAsACE();

    /**
     * Sets the name of the computer that is running SQL Server.
     * 
     * @param serverName
     *        A String that contains the server name.
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
     *        A String that contains the failover server name.
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
     *        The new value of the multiSubnetFailover connection property.
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
     *        A String that contains the user name.
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
     *        A String that contains the client computer name.
     */
    public void setWorkstationID(String workstationID);

    /**
     * Returns the name of the client computer name that is used to connect to the data source.
     * 
     * @return A String that contains the client computer name.
     */
    public String getWorkstationID();

    /**
     * Sets whether converting SQL states to XOPEN compliant states is enabled.
     * 
     * @param xopenStates
     *        true if converting SQL states to XOPEN compliant states is enabled. Otherwise, false.
     */
    public void setXopenStates(boolean xopenStates);

    /**
     * Returns the value that indicates if converting SQL states to XOPEN compliant states is enabled.
     * 
     * @return true if converting SQL states to XOPEN compliant states is enabled. Otherwise, false.
     */
    public boolean getXopenStates();

    /**
     * Sets the URL that is used to connect to the data source.
     * 
     * @param url
     *        A String that contains the URL.
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
     *        A String that contains the description.
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
     *        An int value containing the network packet size.
     */
    public void setPacketSize(int packetSize);

    /**
     * Returns the current network packet size used to communicate with SQL Server, specified in bytes.
     * 
     * @return An int value containing the current network packet size.
     */
    public int getPacketSize();

    /**
     * Sets the kind of integrated security you want your application to use.
     * 
     * @param authenticationScheme
     *        Values are "JavaKerberos" and the default "NativeAuthentication".
     */
    public void setAuthenticationScheme(String authenticationScheme);

    /**
     * Sets the authentication mode.
     * 
     * @param authentication
     *        the authentication mode
     */
    public void setAuthentication(String authentication);

    /**
     * Returns the authentication mode.
     * 
     * @return the authentication value
     */
    public String getAuthentication();

    /**
     * Sets the server spn.
     * 
     * @param serverSpn
     *        A String that contains the server spn
     */
    public void setServerSpn(String serverSpn);

    /**
     * Returns the server spn.
     * 
     * @return A String that contains the server spn
     */
    public String getServerSpn();

    /**
     * Sets the GSSCredential.
     * 
     * @param userCredential
     *        the credential
     */
    public void setGSSCredentials(GSSCredential userCredential);

    /**
     * Returns the GSSCredential.
     * 
     * @return GSSCredential
     */
    public GSSCredential getGSSCredentials();

    /**
     * Sets the access token.
     * 
     * @param accessToken
     *        to be set in the string property.
     */
    public void setAccessToken(String accessToken);

    /**
     * Returns the access token.
     * 
     * @return the access token.
     */
    public String getAccessToken();

    /**
     * Sets the value to enable/disable Always Encrypted functionality for the data source object. The default is
     * Disabled.
     * 
     * @param columnEncryptionSetting
     *        Enables/disables Always Encrypted functionality for the data source object. The default is Disabled.
     */
    public void setColumnEncryptionSetting(String columnEncryptionSetting);

    /**
     * Returns the Always Encrypted functionality setting for the data source object.
     * 
     * @return the Always Encrypted functionality setting for the data source object.
     */
    public String getColumnEncryptionSetting();

    /**
     * Sets the name that identifies a key store. Only value supported is the "JavaKeyStorePassword" for identifying the
     * Java Key Store. The default is null.
     * 
     * @param keyStoreAuthentication
     *        the name that identifies a key store.
     */
    public void setKeyStoreAuthentication(String keyStoreAuthentication);

    /**
     * Returns the value of the keyStoreAuthentication setting for the data source object.
     * 
     * @return the value of the keyStoreAuthentication setting for the data source object.
     */
    public String getKeyStoreAuthentication();

    /**
     * Sets the password for the Java keystore. Note that, for Java Key Store provider the password for the keystore and
     * the key must be the same. Note that, keyStoreAuthentication must be set with "JavaKeyStorePassword".
     * 
     * @param keyStoreSecret
     *        the password to use for the keystore as well as for the key
     */
    public void setKeyStoreSecret(String keyStoreSecret);

    /**
     * Sets the location including the file name for the Java keystore. Note that, keyStoreAuthentication must be set
     * with "JavaKeyStorePassword".
     * 
     * @param keyStoreLocation
     *        the location including the file name for the Java keystore.
     */
    public void setKeyStoreLocation(String keyStoreLocation);

    /**
     * Returns the keyStoreLocation for the Java Key Store.
     * 
     * @return the keyStoreLocation for the Java Key Store.
     */
    public String getKeyStoreLocation();

    /**
     * Setting the query timeout.
     * 
     * @param queryTimeout
     *        The number of seconds to wait before a timeout has occurred on a query. The default value is 0, which
     *        means infinite timeout.
     */
    public void setQueryTimeout(int queryTimeout);

    /**
     * Returns the query timeout.
     * 
     * @return The number of seconds to wait before a timeout has occurred on a query.
     */
    public int getQueryTimeout();

    /**
     * Sets the cancel timeout.
     * 
     * @param cancelQueryTimeout
     *        The number of seconds to wait before we wait for the query timeout to happen.
     */
    public void setCancelQueryTimeout(int cancelQueryTimeout);

    /**
     * Returns the cancel timeout.
     * 
     * @return the number of seconds to wait before we wait for the query timeout to happen.
     */
    public int getCancelQueryTimeout();

    /**
     * Sets the value that enables/disables whether the first execution of a prepared statement will call sp_executesql
     * and not prepare a statement. If this configuration is false the first execution of a prepared statement will call
     * sp_executesql and not prepare a statement, once the second execution happens it will call sp_prepexec and
     * actually setup a prepared statement handle. Following executions will call sp_execute. This relieves the need for
     * sp_unprepare on prepared statement close if the statement is only executed once.
     * 
     * @param enablePrepareOnFirstPreparedStatementCall
     *        Changes the setting per the description.
     */
    public void setEnablePrepareOnFirstPreparedStatementCall(boolean enablePrepareOnFirstPreparedStatementCall);

    /**
     * Returns the value that indicates whether the first execution of a prepared statement will call sp_executesql and
     * not prepare a statement. If this configuration returns false the first execution of a prepared statement will
     * call sp_executesql and not prepare a statement, once the second execution happens it will call sp_prepexec and
     * actually setup a prepared statement handle. Following executions will call sp_execute. This relieves the need for
     * sp_unprepare on prepared statement close if the statement is only executed once.
     * 
     * @return Returns the current setting per the description.
     */
    public boolean getEnablePrepareOnFirstPreparedStatementCall();

    /**
     * Sets the value that controls how many outstanding prepared statement discard actions (sp_unprepare) can be
     * outstanding per connection before a call to clean-up the outstanding handles on the server is executed. If the
     * setting is {@literal <=} 1 unprepare actions will be executed immedietely on prepared statement close. If it is
     * set to {@literal >} 1 these calls will be batched together to avoid overhead of calling sp_unprepare too often.
     * 
     * @param serverPreparedStatementDiscardThreshold
     *        Changes the setting per the description.
     */
    public void setServerPreparedStatementDiscardThreshold(int serverPreparedStatementDiscardThreshold);

    /**
     * Returns the value of the setting that controls how many outstanding prepared statement discard actions
     * (sp_unprepare) can be outstanding per connection before a call to clean-up the outstanding handles on the server
     * is executed.
     * 
     * @return Returns the current setting per the description.
     */
    public int getServerPreparedStatementDiscardThreshold();

    /**
     * Sets the size of the prepared statement cache for this connection. A value less than 1 means no cache.
     * 
     * @param statementPoolingCacheSize
     *        Changes the setting per the description.
     */
    public void setStatementPoolingCacheSize(int statementPoolingCacheSize);

    /**
     * Returns the size of the prepared statement cache for this connection. A value less than 1 means no cache.
     * 
     * @return Returns the current setting per the description.
     */
    public int getStatementPoolingCacheSize();

    /**
     * Sets the value to disable/enable statement pooling.
     * 
     * @param disableStatementPooling
     *        true to disable statement pooling, false to enable it.
     */
    public void setDisableStatementPooling(boolean disableStatementPooling);

    /**
     * Returns whether statement pooling is disabled.
     * 
     * @return true if statement pooling is disabled, false if it is enabled.
     */
    public boolean getDisableStatementPooling();

    /**
     * Sets the socket timeout value.
     * 
     * @param socketTimeout
     *        The number of milliseconds to wait before a timeout is occurred on a socket read or accept. The default
     *        value is 0, which means infinite timeout.
     */
    public void setSocketTimeout(int socketTimeout);

    /**
     * Returns the socket timeout value.
     * 
     * @return The number of milliseconds to wait before a timeout is occurred on a socket read or accept.
     */
    public int getSocketTimeout();

    /**
     * Sets the login configuration file for Kerberos authentication. This overrides the default configuration <i>
     * SQLJDBCDriver </i>
     * 
     * @param configurationName
     *        the configuration name
     */
    public void setJASSConfigurationName(String configurationName);

    /**
     * Returns the login configuration file for Kerberos authentication.
     * 
     * @return login configuration file name
     */
    public String getJASSConfigurationName();

    /**
     * Sets whether Fips Mode should be enabled/disabled on the connection. For FIPS enabled JVM this property should be
     * true.
     * 
     * @param fips
     *        Boolean property to enable/disable fips
     */
    public void setFIPS(boolean fips);

    /**
     * Returns the value of connection property "fips". For FIPS enabled JVM this property should be true.
     * 
     * @return fips boolean value
     */
    public boolean getFIPS();

    /**
     * Sets the sslProtocol property for connection Set this value to specify TLS protocol keyword.
     * 
     * Acceptable values are: TLS, TLSv1, TLSv1.1, and TLSv1.2.
     * 
     * @param sslProtocol
     *        Value for SSL Protocol to be set.
     */
    public void setSSLProtocol(String sslProtocol);

    /**
     * Returns the value of connection property 'sslProtocol'.
     * 
     * @return sslProtocol property value
     */
    public String getSSLProtocol();

    /**
     * Sets the connection property 'trustManagerClass' on the connection.
     * 
     * @param trustManagerClass
     *        The fully qualified class name of a custom javax.net.ssl.TrustManager.
     */
    public void setTrustManagerClass(String trustManagerClass);

    /**
     * Returns the value for the connection property 'trustManagerClass'.
     * 
     * @return trustManagerClass property value
     */
    public String getTrustManagerClass();

    /**
     * Sets Constructor Arguments to be provided on constructor of 'trustManagerClass'.
     * 
     * @param trustManagerConstructorArg
     *        'trustManagerClass' constructor arguments
     */
    public void setTrustManagerConstructorArg(String trustManagerConstructorArg);

    /**
     * Returns the value for the connection property 'trustManagerConstructorArg'.
     * 
     * @return trustManagerConstructorArg property value
     */
    public String getTrustManagerConstructorArg();

    /**
     * Returns whether the use Bulk Copy API is used for Batch Insert.
     * 
     * @return whether the driver should use Bulk Copy API for Batch Insert operations.
     */
    public boolean getUseBulkCopyForBatchInsert();

    /**
     * Sets whether the use Bulk Copy API should be used for Batch Insert.
     * 
     * @param useBulkCopyForBatchInsert
     *        indicates whether Bulk Copy API should be used for Batch Insert operations.
     */
    public void setUseBulkCopyForBatchInsert(boolean useBulkCopyForBatchInsert);

    /**
     * Sets the client id to be used to retrieve access token from MSI EndPoint.
     * 
     * @param msiClientId
     *        Client ID of User Assigned Managed Identity
     */
    public void setMSIClientId(String msiClientId);

    /**
     * Returns the value for the connection property 'msiClientId'.
     * 
     * @return msiClientId property value
     */
    public String getMSIClientId();
}
