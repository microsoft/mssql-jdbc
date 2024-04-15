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
    void setApplicationIntent(String applicationIntent);

    /**
     * Returns the application intent.
     * 
     * @return A String that contains the application intent.
     */
    String getApplicationIntent();

    /**
     * Sets the application name.
     * 
     * @param applicationName
     *        A String that contains the name of the application.
     */
    void setApplicationName(String applicationName);

    /**
     * Returns the application name.
     * 
     * @return A String that contains the application name, or "Microsoft JDBC Driver for SQL Server" if no value is
     *         set.
     */
    String getApplicationName();

    /**
     * Sets the database name to connect to.
     * 
     * @param databaseName
     *        A String that contains the database name.
     */
    void setDatabaseName(String databaseName);

    /**
     * Returns the database name.
     * 
     * @return A String that contains the database name or null if no value is set.
     */
    String getDatabaseName();

    /**
     * Sets the SQL Server instance name.
     * 
     * @param instanceName
     *        A String that contains the instance name.
     */
    void setInstanceName(String instanceName);

    /**
     * Returns the SQL Server instance name.
     * 
     * @return A String that contains the instance name, or null if no value is set.
     */
    String getInstanceName();

    /**
     * Sets a Boolean value that indicates if the integratedSecurity property is enabled.
     * 
     * @param enable
     *        true if integratedSecurity is enabled. Otherwise, false.
     */
    void setIntegratedSecurity(boolean enable);

    /**
     * Sets a Boolean value that indicates if the lastUpdateCount property is enabled.
     * 
     * @param lastUpdateCount
     *        true if lastUpdateCount is enabled. Otherwise, false.
     */
    void setLastUpdateCount(boolean lastUpdateCount);

    /**
     * Returns a Boolean value that indicates if the lastUpdateCount property is enabled.
     * 
     * @return true if lastUpdateCount is enabled. Otherwise, false.
     */
    boolean getLastUpdateCount();

    /**
     * Sets the option whether TLS encryption is used.
     * 
     * @param encryptOption
     *        TLS encrypt option. Default is "true"
     */
    void setEncrypt(String encryptOption);

    /**
     * Sets the option whether TLS encryption is used.
     * 
     * @param encryptOption
     *        TLS encrypt option. Default is true
     * @deprecated Use {@link ISQLServerDataSource#setEncrypt(String encryptOption)} instead
     */
    @Deprecated(since = "10.1.0", forRemoval = true)
    void setEncrypt(boolean encryptOption);

    /**
     * Returns the TLS encryption option.
     * 
     * @return the TLS encrypt option
     */
    String getEncrypt();

    /**
     * Returns the path to the server certificate.
     *
     * @return serverCertificate property value
     */
    String getServerCertificate();

    /**
     * Sets the connection property 'serverCertificate' on the connection.
     *
     * @param cert
     *        The path to the server certificate.
     */
    void setServerCertificate(String cert);

    /**
     * Sets the value to enable/disable Transparent Network IP Resolution (TNIR). Beginning in version 6.0 of the
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
    void setTransparentNetworkIPResolution(boolean tnir);

    /**
     * Returns the TransparentNetworkIPResolution value.
     * 
     * @return if enabled, returns true. Otherwise, false.
     */
    boolean getTransparentNetworkIPResolution();

    /**
     * Sets a boolean value that indicates if the trustServerCertificate property is enabled.
     * 
     * @param e
     *        true, if the server Secure Sockets Layer (SSL) certificate should be automatically trusted when the
     *        communication layer is encrypted using SSL. false, if server SLL certificate should not be trusted
     *        certificate location, if encrypt=strict
     */
    void setTrustServerCertificate(boolean e);

    /**
     * Returns a boolean value that indicates if the trustServerCertificate property is enabled.
     * 
     * @return true if trustServerCertificate is enabled. Otherwise, false. If encrypt=strict, returns server
     *         certificate location
     */
    boolean getTrustServerCertificate();

    /**
     * Sets the keystore type for the trustStore.
     * 
     * @param trustStoreType
     *        A String that contains the trust store type
     */
    void setTrustStoreType(String trustStoreType);

    /**
     * Returns the keyStore Type for the trustStore.
     * 
     * @return trustStoreType A String that contains the trust store type
     */
    String getTrustStoreType();

    /**
     * Sets the path (including file name) to the certificate trustStore file.
     * 
     * @param trustStore
     *        A String that contains the path (including file name) to the certificate trustStore file.
     */
    void setTrustStore(String trustStore);

    /**
     * Returns the path (including file name) to the certificate trustStore file.
     * 
     * @return trustStore A String that contains the path (including file name) to the certificate trustStore file, or
     *         null if no value is set.
     */
    String getTrustStore();

    /**
     * Sets the password that is used to check the integrity of the trustStore data.
     * 
     * @param trustStorePassword
     *        A String that contains the password that is used to check the integrity of the trustStore data.
     */
    void setTrustStorePassword(String trustStorePassword);

    /**
     * Sets the host name to be used in validating the SQL Server Secure Sockets Layer (SSL) certificate.
     * 
     * @param hostName
     *        A String that contains the host name.
     */
    void setHostNameInCertificate(String hostName);

    /**
     * Returns the host name used in validating the SQL Server Secure Sockets Layer (SSL) certificate.
     * 
     * @return A String that contains the host name, or null if no value is set.
     */
    String getHostNameInCertificate();

    /**
     * Sets an int value that indicates the number of milliseconds to wait before the database reports a lock time out.
     * 
     * @param lockTimeout
     *        An int value that contains the number of milliseconds to wait.
     */
    void setLockTimeout(int lockTimeout);

    /**
     * Returns an int value that indicates the number of milliseconds that the database will wait before reporting a
     * lock time out.
     * 
     * @return An int value that contains the number of milliseconds that the database will wait.
     */
    int getLockTimeout();

    /**
     * Sets the password that will be used to connect to SQL Server.
     * 
     * @param password
     *        A String that contains the password.
     */
    void setPassword(String password);

    /**
     * Sets the port number to be used to communicate with SQL Server.
     * 
     * @param portNumber
     *        An int value that contains the port number.
     */
    void setPortNumber(int portNumber);

    /**
     * Returns the current port number that is used to communicate with SQL Server.
     * 
     * @return An int value that contains the current port number.
     */
    int getPortNumber();

    /**
     * Sets the default cursor type that is used for all result sets that are created by using this SQLServerDataSource
     * object.
     * 
     * @param selectMethod
     *        A String value that contains the default cursor type.
     */
    void setSelectMethod(String selectMethod);

    /**
     * Returns the default cursor type used for all result sets that are created by using this SQLServerDataSource
     * object.
     * 
     * @return A String value that contains the default cursor type.
     */
    String getSelectMethod();

    /**
     * Sets the response buffering mode for connections created by using this SQLServerDataSource object.
     * 
     * @param bufferingMode
     *        A String that contains the buffering and streaming mode. The valid mode can be one of the following
     *        case-insensitive Strings: full or adaptive.
     */
    void setResponseBuffering(String bufferingMode);

    /**
     * Returns the response buffering mode for this SQLServerDataSource object.
     * 
     * @return A String that contains a lower-case full or adaptive.
     */
    String getResponseBuffering();

    /**
     * Sets the value to enable/disable the replication connection property.
     * 
     * @param replication
     *        A Boolean value. When true, tells the server that the connection is used for replication.
     */
    void setReplication(boolean replication);

    /**
     * Returns the value of the replication connection property.
     * 
     * @return true if the connection is to be used for replication. Otherwise false.
     */
    boolean getReplication();

    /**
     * Sets the value to enable/disable the sendTimeAsDatetime connection property.
     * 
     * @param sendTimeAsDatetime
     *        A Boolean value. When true, causes java.sql.Time values to be sent to the server as SQL Server datetime
     *        types. When false, causes java.sql.Time values to be sent to the server as SQL Server time types.
     */
    void setSendTimeAsDatetime(boolean sendTimeAsDatetime);

    /**
     * Returns the value of the sendTimeAsDatetime connection property. This method was added in SQL Server JDBC Driver
     * 3.0. Returns the setting of the sendTimeAsDatetime connection property.
     * 
     * @return true if java.sql.Time values will be sent to the server as a SQL Server datetime type. false if
     *         java.sql.Time values will be sent to the server as a SQL Server time type.
     */
    boolean getSendTimeAsDatetime();

    /**
     * Sets the SQL server datatype to use for Java datetime and timestamp values.
     * 
     * @param datetimeParameterType
     *        The SQL datatype to use when encoding Java dates for SQL Server. Valid values are:
     *        datetime, datetime2 or datetimeoffset.
     */
    void setDatetimeParameterType(String datetimeParameterType);

    /**
     * Returns the value of the datetimeParameterType connection property. This method was added in SQL Server JDBC Driver
     * 12.2. Returns the setting of the datetimeParameterType connection property.
     * 
     * @return Returns the value of the datetimeParameterType property.
     */
    String getDatetimeParameterType();

    /**
     * Sets a boolean value that indicates if sending string parameters to the server in UNICODE format is enabled.
     * 
     * @param sendStringParametersAsUnicode
     *        true if string parameters are sent to the server in UNICODE format. Otherwise, false.
     */
    void setSendStringParametersAsUnicode(boolean sendStringParametersAsUnicode);

    /**
     * Returns whether sending string parameters to the server in UNICODE format is enabled.
     * 
     * @return true if string parameters are sent to the server in UNICODE format. Otherwise, false.
     */
    boolean getSendStringParametersAsUnicode();

    /**
     * Sets whether the serverName will be translated from Unicode to ASCII Compatible Encoding (ACE).
     * 
     * @param serverNameAsACE
     *        if enabled the servername will be translated to ASCII Compatible Encoding (ACE)
     */
    void setServerNameAsACE(boolean serverNameAsACE);

    /**
     * Returns if the serverName should be translated from Unicode to ASCII Compatible Encoding (ACE).
     * 
     * @return if enabled, will return true. Otherwise, false.
     */
    boolean getServerNameAsACE();

    /**
     * Sets the name of the computer that is running SQL Server.
     * 
     * @param serverName
     *        A String that contains the server name.
     */
    void setServerName(String serverName);

    /**
     * Returns the name of the SQL Server instance.
     * 
     * @return A String that contains the server name or null if no value is set.
     */
    String getServerName();

    /**
     * Sets the name of the preferred type of IP Address.
     * 
     * @param iPAddressPreference
     *        A String that contains the preferred type of IP Address.
     */
    void setIPAddressPreference(String iPAddressPreference);

    /**
     * Gets the name of the preferred type of IP Address.
     * 
     * @return IPAddressPreference
     *         A String that contains the preferred type of IP Address.
     */
    String getIPAddressPreference();

    /**
     * Sets the name of the failover server that is used in a database mirroring configuration.
     * 
     * @param serverName
     *        A String that contains the failover server name.
     */
    void setFailoverPartner(String serverName);

    /**
     * Returns the name of the failover server that is used in a database mirroring configuration.
     * 
     * @return A String that contains the name of the failover partner, or null if none is set.
     */
    String getFailoverPartner();

    /**
     * Sets the value of the multiSubnetFailover connection property.
     * 
     * @param multiSubnetFailover
     *        The new value of the multiSubnetFailover connection property.
     */
    void setMultiSubnetFailover(boolean multiSubnetFailover);

    /**
     * Returns the value of the multiSubnetFailover connection property.
     * 
     * @return Returns true or false, depending on the current setting of the connection property.
     */
    boolean getMultiSubnetFailover();

    /**
     * Sets the user name that is used to connect the data source.
     * 
     * @param user
     *        A String that contains the user name.
     */
    void setUser(String user);

    /**
     * Returns the user name that is used to connect the data source.
     * 
     * @return A String that contains the user name.
     */
    String getUser();

    /**
     * Sets the name of the client computer name that is used to connect to the data source.
     * 
     * @param workstationID
     *        A String that contains the client computer name.
     */
    void setWorkstationID(String workstationID);

    /**
     * Returns the name of the client computer name that is used to connect to the data source.
     * 
     * @return A String that contains the client computer name.
     */
    String getWorkstationID();

    /**
     * Sets whether converting SQL states to XOPEN compliant states is enabled.
     * 
     * @param xopenStates
     *        true if converting SQL states to XOPEN compliant states is enabled. Otherwise, false.
     */
    void setXopenStates(boolean xopenStates);

    /**
     * Returns the value that indicates if converting SQL states to XOPEN compliant states is enabled.
     * 
     * @return true if converting SQL states to XOPEN compliant states is enabled. Otherwise, false.
     */
    boolean getXopenStates();

    /**
     * Sets the URL that is used to connect to the data source.
     * 
     * @param url
     *        A String that contains the URL.
     */
    void setURL(String url);

    /**
     * Returns the URL that is used to connect to the data source.
     * 
     * @return A String that contains the URL.
     */
    String getURL();

    /**
     * Sets the description of the data source.
     * 
     * @param description
     *        A String that contains the description.
     */
    void setDescription(String description);

    /**
     * Returns a description of the data source.
     * 
     * @return A String that contains the data source description or null if no value is set.
     */
    String getDescription();

    /**
     * Sets the current network packet size used to communicate with SQL Server, specified in bytes.
     * 
     * @param packetSize
     *        An int value containing the network packet size.
     */
    void setPacketSize(int packetSize);

    /**
     * Returns the current network packet size used to communicate with SQL Server, specified in bytes.
     * 
     * @return An int value containing the current network packet size.
     */
    int getPacketSize();

    /**
     * Sets the kind of integrated security you want your application to use.
     * 
     * @param authenticationScheme
     *        Values are "JavaKerberos" and the default "NativeAuthentication".
     */
    void setAuthenticationScheme(String authenticationScheme);

    /**
     * Sets the authentication mode.
     * 
     * @param authentication
     *        the authentication mode
     */
    void setAuthentication(String authentication);

    /**
     * Returns the authentication mode.
     * 
     * @return the authentication value
     */
    String getAuthentication();

    /**
     * Sets the realm for Kerberos authentication.
     * 
     * @param realm
     *        A String that contains the realm
     */
    void setRealm(String realm);

    /**
     * Returns the realm for Kerberos authentication.
     * 
     * @return A String that contains the realm
     */
    String getRealm();

    /**
     * Sets the server spn.
     * 
     * @param serverSpn
     *        A String that contains the server spn
     */
    void setServerSpn(String serverSpn);

    /**
     * Returns the server spn.
     * 
     * @return A String that contains the server spn
     */
    String getServerSpn();

    /**
     * Sets the value to indicate whether useDefaultGSSCredential is enabled.
     *
     * @param enable
     *        true if useDefaultGSSCredential is enabled. Otherwise, false.
     */
    void setUseDefaultGSSCredential(boolean enable);

    /**
     * Returns the useDefaultGSSCredential.
     *
     * @return if enabled, return true. Otherwise, false.
     */
    boolean getUseDefaultGSSCredential();

    /**
     * Sets whether or not sp_sproc_columns will be used for parameter name lookup.
     *
     * @param useFlexibleCallableStatements
     *        When set to false, sp_sproc_columns is not used for parameter name lookup
     *        in callable statements. This eliminates a round trip to the server but imposes limitations
     *        on how parameters are set. When set to false, applications must either reference
     *        parameters by name or by index, not both. Parameters must also be set in the same
     *        order as the stored procedure definition.
     */
    void setUseFlexibleCallableStatements(boolean useFlexibleCallableStatements);

    /**
     * Returns whether or not sp_sproc_columns is being used for parameter name lookup.
     *
     * @return useFlexibleCallableStatements
     */
    boolean getUseFlexibleCallableStatements();

    /**
     * Sets the GSSCredential.
     *
     * @param userCredential
     *        the credential
     */
    void setGSSCredentials(GSSCredential userCredential);

    /**
     * Returns the GSSCredential.
     *
     * @return GSSCredential
     */
    GSSCredential getGSSCredentials();

    /**
     * Sets the access token.
     * 
     * @param accessToken
     *        to be set in the string property.
     */
    void setAccessToken(String accessToken);

    /**
     * Returns the access token.
     * 
     * @return the access token.
     */
    String getAccessToken();

    /**
     * Sets the value to enable/disable Always Encrypted functionality for the data source object. The default is
     * Disabled.
     * 
     * @param columnEncryptionSetting
     *        Enables/disables Always Encrypted functionality for the data source object. The default is Disabled.
     */
    void setColumnEncryptionSetting(String columnEncryptionSetting);

    /**
     * Returns the Always Encrypted functionality setting for the data source object.
     * 
     * @return the Always Encrypted functionality setting for the data source object.
     */
    String getColumnEncryptionSetting();

    /**
     * Sets the name that identifies a key store. Only value supported is the "JavaKeyStorePassword" for identifying the
     * Java Key Store. The default is null.
     * 
     * @param keyStoreAuthentication
     *        the name that identifies a key store.
     */
    void setKeyStoreAuthentication(String keyStoreAuthentication);

    /**
     * Returns the value of the keyStoreAuthentication setting for the data source object.
     * 
     * @return the value of the keyStoreAuthentication setting for the data source object.
     */
    String getKeyStoreAuthentication();

    /**
     * Sets the password for the Java keystore. Note that, for Java Key Store provider the password for the keystore and
     * the key must be the same. Note that, keyStoreAuthentication must be set with "JavaKeyStorePassword".
     * 
     * @param keyStoreSecret
     *        the password to use for the keystore as well as for the key
     */
    void setKeyStoreSecret(String keyStoreSecret);

    /**
     * Sets the location including the file name for the Java keystore. Note that, keyStoreAuthentication must be set
     * with "JavaKeyStorePassword".
     * 
     * @param keyStoreLocation
     *        the location including the file name for the Java keystore.
     */
    void setKeyStoreLocation(String keyStoreLocation);

    /**
     * Returns the keyStoreLocation for the Java Key Store.
     * 
     * @return the keyStoreLocation for the Java Key Store.
     */
    String getKeyStoreLocation();

    /**
     * Setting the query timeout.
     * 
     * @param queryTimeout
     *        The number of seconds to wait before a timeout has occurred on a query. The default value is 0, which
     *        means infinite timeout.
     */
    void setQueryTimeout(int queryTimeout);

    /**
     * Returns the query timeout.
     * 
     * @return The number of seconds to wait before a timeout has occurred on a query.
     */
    int getQueryTimeout();

    /**
     * Sets the cancel timeout.
     * 
     * @param cancelQueryTimeout
     *        The number of seconds to wait before we wait for the query timeout to happen.
     */
    void setCancelQueryTimeout(int cancelQueryTimeout);

    /**
     * Returns the cancel timeout.
     * 
     * @return the number of seconds to wait before we wait for the query timeout to happen.
     */
    int getCancelQueryTimeout();

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
    void setEnablePrepareOnFirstPreparedStatementCall(boolean enablePrepareOnFirstPreparedStatementCall);

    /**
     * Returns the value that indicates whether the first execution of a prepared statement will call sp_executesql and
     * not prepare a statement. If this configuration returns false the first execution of a prepared statement will
     * call sp_executesql and not prepare a statement, once the second execution happens it will call sp_prepexec and
     * actually setup a prepared statement handle. Following executions will call sp_execute. This relieves the need for
     * sp_unprepare on prepared statement close if the statement is only executed once.
     * 
     * @return Returns the current setting per the description.
     */
    boolean getEnablePrepareOnFirstPreparedStatementCall();

    /**
     * Sets the value that controls how many outstanding prepared statement discard actions (sp_unprepare) can be
     * outstanding per connection before a call to clean-up the outstanding handles on the server is executed. If the
     * setting is {@literal <=} 1 unprepare actions will be executed immedietely on prepared statement close. If it is
     * set to {@literal >} 1 these calls will be batched together to avoid overhead of calling sp_unprepare too often.
     * 
     * @param serverPreparedStatementDiscardThreshold
     *        Changes the setting per the description.
     */
    void setServerPreparedStatementDiscardThreshold(int serverPreparedStatementDiscardThreshold);

    /**
     * Returns the value of the setting that controls how many outstanding prepared statement discard actions
     * (sp_unprepare) can be outstanding per connection before a call to clean-up the outstanding handles on the server
     * is executed.
     * 
     * @return Returns the current setting per the description.
     */
    int getServerPreparedStatementDiscardThreshold();

    /**
     * Sets the size of the prepared statement cache for this connection. A value less than 1 means no cache.
     * 
     * @param statementPoolingCacheSize
     *        Changes the setting per the description.
     */
    void setStatementPoolingCacheSize(int statementPoolingCacheSize);

    /**
     * Returns the size of the prepared statement cache for this connection. A value less than 1 means no cache.
     * 
     * @return Returns the current setting per the description.
     */
    int getStatementPoolingCacheSize();

    /**
     * Sets the value to disable/enable statement pooling.
     * 
     * @param disableStatementPooling
     *        true to disable statement pooling, false to enable it.
     */
    void setDisableStatementPooling(boolean disableStatementPooling);

    /**
     * Returns whether statement pooling is disabled.
     * 
     * @return true if statement pooling is disabled, false if it is enabled.
     */
    boolean getDisableStatementPooling();

    /**
     * Sets the socket timeout value.
     * 
     * @param socketTimeout
     *        The number of milliseconds to wait before a timeout is occurred on a socket read or accept. The default
     *        value is 0, which means infinite timeout.
     */
    void setSocketTimeout(int socketTimeout);

    /**
     * Returns the socket timeout value.
     * 
     * @return The number of milliseconds to wait before a timeout is occurred on a socket read or accept.
     */
    int getSocketTimeout();

    /**
     * Sets the login configuration name for Kerberos authentication. This overrides the default configuration <i>
     * SQLJDBCDriver </i>
     * 
     * @param configurationName
     *        the configuration name
     * @deprecated Use {@link ISQLServerDataSource#setJAASConfigurationName(String configurationName)} instead
     * 
     */
    @Deprecated(since = "9.3.0", forRemoval = true)
    void setJASSConfigurationName(String configurationName);

    /**
     * Returns the login configuration name for Kerberos authentication.
     *
     * 
     * @return login configuration file name
     * @deprecated Use {@link ISQLServerDataSource#getJAASConfigurationName()} instead
     * 
     */
    @Deprecated(since = "9.3.0", forRemoval = true)
    String getJASSConfigurationName();

    /**
     * Sets the login configuration name for Kerberos authentication. This overrides the default configuration <i>
     * SQLJDBCDriver </i>
     * 
     * 
     * @param configurationName
     *        the configuration name
     */
    void setJAASConfigurationName(String configurationName);

    /**
     * Returns the login configuration name for Kerberos authentication.
     * 
     * @return login configuration name
     */
    String getJAASConfigurationName();

    /**
     * Returns whether the default JAAS Configuration should be used
     *
     * @return useDefaultJaasConfig boolean value
     */
    boolean getUseDefaultJaasConfig();

    /**
     * Sets whether the default JAAS Configuration will be used. This means the system-wide JAAS configuration
     * is ignored to avoid conflicts with libraries that override the JAAS configuration.
     *
     * @param useDefaultJaasConfig
     *        boolean property to use the default JAAS configuration
     */
    void setUseDefaultJaasConfig(boolean useDefaultJaasConfig);

    /**
     * Sets whether Fips Mode should be enabled/disabled on the connection. For FIPS enabled JVM this property should be
     * true.
     * 
     * @param fips
     *        Boolean property to enable/disable fips
     */
    void setFIPS(boolean fips);

    /**
     * Returns the value of connection property "fips". For FIPS enabled JVM this property should be true.
     * 
     * @return fips boolean value
     */
    boolean getFIPS();

    /**
     * Sets the sslProtocol property for connection Set this value to specify TLS protocol keyword.
     * 
     * Acceptable values are: TLS, TLSv1, TLSv1.1, and TLSv1.2.
     * 
     * @param sslProtocol
     *        Value for SSL Protocol to be set.
     */
    void setSSLProtocol(String sslProtocol);

    /**
     * Returns the value of connection property 'sslProtocol'.
     * 
     * @return sslProtocol property value
     */
    String getSSLProtocol();

    /**
     * Returns the value for the connection property 'socketFactoryClass'.
     *
     * @return socketFactoryClass property value
     */
    String getSocketFactoryClass();

    /**
     * Sets the connection property 'socketFactoryClass' on the connection.
     *
     * @param socketFactoryClass
     *        The fully qualified class name of a custom javax.net.SocketFactory.
     */
    void setSocketFactoryClass(String socketFactoryClass);

    /**
     * Returns the value for the connection property 'socketFactoryConstructorArg'.
     *
     * @return socketFactoryConstructorArg property value
     */
    String getSocketFactoryConstructorArg();

    /**
     * Sets Constructor Arguments to be provided on constructor of 'socketFactoryClass'.
     *
     * @param socketFactoryConstructorArg
     *        'socketFactoryClass' constructor arguments
     */
    void setSocketFactoryConstructorArg(String socketFactoryConstructorArg);

    /**
     * Sets the connection property 'trustManagerClass' on the connection.
     * 
     * @param trustManagerClass
     *        The fully qualified class name of a custom javax.net.ssl.TrustManager.
     */
    void setTrustManagerClass(String trustManagerClass);

    /**
     * Returns the value for the connection property 'trustManagerClass'.
     * 
     * @return trustManagerClass property value
     */
    String getTrustManagerClass();

    /**
     * Sets Constructor Arguments to be provided on constructor of 'trustManagerClass'.
     * 
     * @param trustManagerConstructorArg
     *        'trustManagerClass' constructor arguments
     */
    void setTrustManagerConstructorArg(String trustManagerConstructorArg);

    /**
     * Returns the value for the connection property 'trustManagerConstructorArg'.
     * 
     * @return trustManagerConstructorArg property value
     */
    String getTrustManagerConstructorArg();

    /**
     * Returns whether the use Bulk Copy API is used for Batch Insert.
     * 
     * @return whether the driver should use Bulk Copy API for Batch Insert operations.
     */
    boolean getUseBulkCopyForBatchInsert();

    /**
     * Sets whether the use Bulk Copy API should be used for Batch Insert.
     * 
     * @param useBulkCopyForBatchInsert
     *        indicates whether Bulk Copy API should be used for Batch Insert operations.
     */
    void setUseBulkCopyForBatchInsert(boolean useBulkCopyForBatchInsert);

    /**
     * Sets the client id to be used to retrieve the access token for a user-assigned Managed Identity.
     * 
     * @param managedIdentityClientId
     *        Client ID of the user-assigned Managed Identity.
     * @deprecated Use {@link ISQLServerDataSource#setUser(String user)} instead.
     */
    @Deprecated(since = "12.1.0", forRemoval = true)
    void setMSIClientId(String managedIdentityClientId);

    /**
     * Returns the value for the connection property 'msiClientId'.
     * 
     * @return msiClientId property value
     * 
     * @deprecated Use {@link ISQLServerDataSource#getUser()} instead.
     */
    @Deprecated(since = "12.1.0", forRemoval = true)
    String getMSIClientId();

    /**
     * Sets the value for the connection property 'keyStorePrincipalId'.
     * 
     * @param keyStorePrincipalId
     * 
     *        <pre>
     *        When keyStoreAuthentication = keyVaultClientSecret, set this value to a valid Azure Active Directory Application Client ID.
     *        When keyStoreAuthentication = keyVaultManagedIdentity, set this value to a valid Azure Active Directory Application Object ID (optional, for user-assigned only).
     *        </pre>
     */
    void setKeyStorePrincipalId(String keyStorePrincipalId);

    /**
     * Returns the value for the connection property 'keyStorePrincipalId'.
     * 
     * @return keyStorePrincipalId
     */
    String getKeyStorePrincipalId();

    /**
     * Sets the Azure Key Vault (AKV) Provider Client Id to provided value to be used for column encryption.
     * 
     * @param keyVaultProviderClientId
     *        Client Id of Azure Key Vault (AKV) Provider to be used for column encryption.
     */
    void setKeyVaultProviderClientId(String keyVaultProviderClientId);

    /**
     * Returns the value for the connection property 'keyVaultProviderClientId'.
     * 
     * @return keyVaultProviderClientId
     */
    String getKeyVaultProviderClientId();

    /**
     * Sets the Azure Key Vault (AKV) Provider Client Key to provided value to be used for column encryption.
     * 
     * @param keyVaultProviderClientKey
     *        Client Key of Azure Key Vault (AKV) Provider to be used for column encryption.
     */
    void setKeyVaultProviderClientKey(String keyVaultProviderClientKey);

    /**
     * Returns the value for the connection property 'domain'.
     * 
     * @return 'domain' property value
     */
    String getDomain();

    /**
     * Sets the 'domain' connection property used for NTLM Authentication.
     *
     * @param domain
     *        Windows domain name
     */
    void setDomain(String domain);

    /**
     * Returns the current flag value for useFmtOnly.
     *
     * @return 'useFmtOnly' property value.
     */
    boolean getUseFmtOnly();

    /**
     * Specifies the flag to use FMTONLY for parameter metadata queries.
     *
     * @param useFmtOnly
     *        boolean value for 'useFmtOnly'.
     */
    void setUseFmtOnly(boolean useFmtOnly);

    /**
     * Returns the enclave attestation URL used in Always Encrypted with Secure Enclaves.
     * 
     * @return enclave attestation URL.
     */
    String getEnclaveAttestationUrl();

    /**
     * Sets the enclave attestation URL used in Always Encrypted with Secure Enclaves.
     * 
     * @param url
     *        Enclave attestation URL.
     */
    void setEnclaveAttestationUrl(String url);

    /**
     * Returns the enclave attestation protocol used in Always Encrypted with Secure Enclaves.
     * 
     * @return Enclave attestation protocol.
     */
    String getEnclaveAttestationProtocol();

    /**
     * Sets the enclave attestation protocol to be used in Always Encrypted with Secure Enclaves.
     * 
     * @param protocol
     *        Enclave attestation protocol.
     */
    void setEnclaveAttestationProtocol(String protocol);

    /**
     * Returns client certificate path for client certificate authentication.
     * 
     * @return Client certificate path.
     */
    String getClientCertificate();

    /**
     * Sets client certificate path for client certificate authentication.
     * 
     * @param certPath
     *        Client certificate path.
     */
    void setClientCertificate(String certPath);

    /**
     * Returns Private key file path for client certificate authentication.
     * 
     * @return Private key file path.
     */
    String getClientKey();

    /**
     * Sets Private key file path for client certificate authentication.
     * 
     * @param keyPath
     *        Private key file path.
     */
    void setClientKey(String keyPath);

    /**
     * Sets the password to be used for Private key provided by the user for client certificate authentication.
     * 
     * @param password
     *        Private key password.
     */
    void setClientKeyPassword(String password);

    /**
     * Specifies the flag to load LOBs instead of streaming them.
     *
     * @param delayLoadingLobs
     *        boolean value for 'delayLoadingLobs'.
     */
    void setDelayLoadingLobs(boolean delayLoadingLobs);

    /**
     * Returns the current flag value for delayLoadingLobs.
     *
     * @return 'delayLoadingLobs' property value.
     */
    boolean getDelayLoadingLobs();

    /**
     * Returns the current flag for value sendTemporalDataTypesAsStringForBulkCopy
     *
     * @return 'sendTemporalDataTypesAsStringForBulkCopy' property value.
     */
    boolean getSendTemporalDataTypesAsStringForBulkCopy();

    /**
     * Specifies the flag to send temporal datatypes as String for Bulk Copy.
     * 
     * @param sendTemporalDataTypesAsStringForBulkCopy
     *        boolean value for 'sendTemporalDataTypesAsStringForBulkCopy'.
     */
    void setSendTemporalDataTypesAsStringForBulkCopy(boolean sendTemporalDataTypesAsStringForBulkCopy);

    /**
     * 
     * Returns the value for the connection property 'AADSecurePrincipalId'.
     * 
     * @return 'AADSecurePrincipalId' property value.
     * @deprecated Use {@link ISQLServerDataSource#getUser()} instead
     */
    @Deprecated(since = "9.4.1", forRemoval = true)
    String getAADSecurePrincipalId();

    /**
     *
     * Sets the 'AADSecurePrincipalId' connection property used for Active Directory Service Principal authentication.
     * 
     * @param aadSecurePrincipalId
     *        Active Directory Service Principal Id.
     * @deprecated Use {@link ISQLServerDataSource#setUser(String user)} instead
     */
    @Deprecated(since = "9.4.1", forRemoval = true)
    void setAADSecurePrincipalId(String aadSecurePrincipalId);

    /**
     * Sets the 'AADSecurePrincipalSecret' connection property used for Active Directory Service Principal
     * authentication.
     * 
     * @param aadSecurePrincipalSecret
     *        Active Directory Service Principal secret.
     * @deprecated Use {@link ISQLServerDataSource#setPassword(String password)} instead
     */
    @Deprecated(since = "9.4.1", forRemoval = true)
    void setAADSecurePrincipalSecret(String aadSecurePrincipalSecret);

    /**
     * Returns value of 'maxResultBuffer' from Connection String.
     *
     * @return 'maxResultBuffer' property.
     */
    String getMaxResultBuffer();

    /**
     * Sets the value for 'maxResultBuffer' property
     *
     * @param maxResultBuffer
     *        String value for 'maxResultBuffer'
     */
    void setMaxResultBuffer(String maxResultBuffer);

    /**
     * Sets the maximum number of attempts to reestablish a broken connection.
     *
     * @param connectRetryCount
     *        maximum number of attempts
     */
    void setConnectRetryCount(int connectRetryCount);

    /**
     * Returns the maximum number of attempts set to reestablish a broken connection.
     *
     * @return maximum number of attempts
     */
    int getConnectRetryCount();

    /**
     * Sets the interval, in seconds, between attempts to reestablish a broken connection.
     *
     * @param connectRetryInterval
     *        interval in seconds
     */
    void setConnectRetryInterval(int connectRetryInterval);

    /**
     * Returns the interval set, in seconds, between attempts to reestablish a broken connection.
     *
     * @return interval in seconds
     */
    int getConnectRetryInterval();

    /**
     * Sets the behavior for the prepare method. {@link PrepareMethod}
     *
     * @param prepareMethod
     *        Changes the setting as per description
     */
    void setPrepareMethod(String prepareMethod);

    /**
     * Returns the value indicating the prepare method. {@link PrepareMethod}
     *
     * @return prepare method
     */
    String getPrepareMethod();

    /**
     * Time-to-live is no longer supported for the cached Managed Identity tokens.
     * This method is a no-op for backwards compatibility only.
     *
     * @param timeToLive
     *        Time-to-live is no longer supported.
     * 
     * @deprecated
     */
    @Deprecated(since = "12.1.0", forRemoval = true)
    void setMsiTokenCacheTtl(int timeToLive);

    /**
     * Time-to-live is no longer supported for the cached Managed Identity tokens.
     * This method will always return 0 and is for backwards compatibility only.
     *
     * @return Method will always return 0.
     * 
     * @deprecated
     */
    @Deprecated(since = "12.1.0", forRemoval = true)
    int getMsiTokenCacheTtl();

    /**
     * Sets the {@link SQLServerAccessTokenCallback} delegate.
     *
     * @param accessTokenCallback
     *        Access token callback delegate.
     */
    void setAccessTokenCallback(SQLServerAccessTokenCallback accessTokenCallback);

    /**
     * Returns a {@link SQLServerAccessTokenCallback}, the access token callback delegate.
     *
     * @return Access token callback delegate.
     */
    SQLServerAccessTokenCallback getAccessTokenCallback();

    /**
     * Returns the fully qualified class name of the implementing class for {@link SQLServerAccessTokenCallback}.
     *
     * @return accessTokenCallbackClass
     */
    String getAccessTokenCallbackClass();

    /**
     * Sets 'accessTokenCallbackClass' to the fully qualified class name
     * of the implementing class for {@link SQLServerAccessTokenCallback}.
     *
     * @param accessTokenCallbackClass
     *        access token callback class
     * 
     */
    void setAccessTokenCallbackClass(String accessTokenCallbackClass);

    /**
     * Returns value of 'calcBigDecimalPrecision' from Connection String.
     *
     * @param calcBigDecimalPrecision
     *        indicates whether the driver should attempt to calculate precision from inputted big decimal values
     */
    void setCalcBigDecimalPrecision(boolean calcBigDecimalPrecision);

    /**
     * Sets the value for 'calcBigDecimalPrecision' property
     *
     * @return calcBigDecimalPrecision boolean value
     */
    boolean getCalcBigDecimalPrecision();

    /**
     * Returns value of 'retryExec' from Connection String.
     *
     * @param retryExec
     */
    void setRetryExec(String retryExec);

    /**
     * Sets the value for 'retryExec' property
     *
     * @return retryExec String value
     */
    String getRetryExec();
}
