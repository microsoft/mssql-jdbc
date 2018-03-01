/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.Reference;
import javax.naming.StringRefAddr;
import javax.sql.DataSource;

import org.ietf.jgss.GSSCredential;

/**
 * This datasource lists properties specific for the SQLServerConnection class.
 */
public class SQLServerDataSource implements ISQLServerDataSource, DataSource, java.io.Serializable, javax.naming.Referenceable {
    // dsLogger is logger used for all SQLServerDataSource instances.
    static final java.util.logging.Logger dsLogger = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerDataSource");
    static final java.util.logging.Logger loggerExternal = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.DataSource");
    static final private java.util.logging.Logger parentLogger = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc");
    final private String loggingClassName;
    private boolean trustStorePasswordStripped = false;
    private static final long serialVersionUID = 654861379544314296L;

    private Properties connectionProps;			// Properties passed to SQLServerConnection class.
    private String dataSourceURL;				// URL for datasource.
    private String dataSourceDescription;		// Description for datasource.
    static private final AtomicInteger baseDataSourceID = new AtomicInteger(0);	// Unique id generator for each DataSource instance (used for
                                                                               	// logging).
    final private String traceID;

    /**
     * Initializes a new instance of the SQLServerDataSource class.
     */
    public SQLServerDataSource() {
        connectionProps = new Properties();
        int dataSourceID = nextDataSourceID();
        String nameL = getClass().getName();
        traceID = nameL.substring(1 + nameL.lastIndexOf('.')) + ":" + dataSourceID;
        loggingClassName = "com.microsoft.sqlserver.jdbc." + nameL.substring(1 + nameL.lastIndexOf('.')) + ":" + dataSourceID;
    }

    String getClassNameLogging() {
        return loggingClassName;
    }

    public String toString() {
        return traceID;
    }

    // DataSource interface public methods

    public Connection getConnection() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getConnection");
        Connection con = getConnectionInternal(null, null, null);
        loggerExternal.exiting(getClassNameLogging(), "getConnection", con);
        return con;
    }

    public Connection getConnection(String username,
            String password) throws SQLServerException {
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getConnection", new Object[] {username, "Password not traced"});
        Connection con = getConnectionInternal(username, password, null);
        loggerExternal.exiting(getClassNameLogging(), "getConnection", con);
        return con;
    }

    // Sets the maximum time in seconds that this data source will wait while
    // attempting to connect to a database. Note default value is 0.
    public void setLoginTimeout(int loginTimeout) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.LOGIN_TIMEOUT.toString(), loginTimeout);
    }

    public int getLoginTimeout() {
        int defaultTimeOut = SQLServerDriverIntProperty.LOGIN_TIMEOUT.getDefaultValue();
        final int logintimeout = getIntProperty(connectionProps, SQLServerDriverIntProperty.LOGIN_TIMEOUT.toString(), defaultTimeOut);
        // even if the user explicitly sets the timeout to zero, convert to 15
        return (logintimeout == 0) ? defaultTimeOut : logintimeout;
    }

    // Sets the log writer for this DataSource.
    // Currently we just hold onto this logWriter and pass it back to callers, nothing else.
    private transient PrintWriter logWriter;

    public void setLogWriter(PrintWriter out) {
        loggerExternal.entering(getClassNameLogging(), "setLogWriter", out);
        logWriter = out;
        loggerExternal.exiting(getClassNameLogging(), "setLogWriter");
    }

    // Retrieves the log writer for this DataSource.
    public PrintWriter getLogWriter() {
        loggerExternal.entering(getClassNameLogging(), "getLogWriter");
        loggerExternal.exiting(getClassNameLogging(), "getLogWriter", logWriter);
        return logWriter;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return parentLogger;
    }

    // Core Connection property setters/getters.

    // applicationName is used to identify the specific application in various SQL Server
    // profiling and logging tools.
    public void setApplicationName(String applicationName) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.APPLICATION_NAME.toString(), applicationName);
    }

    public String getApplicationName() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.APPLICATION_NAME.toString(),
                SQLServerDriverStringProperty.APPLICATION_NAME.getDefaultValue());
    }

    // databaseName is the name of the database to connect to. If databaseName is not set,
    // getDatabaseName returns the default value of null.
    public void setDatabaseName(String databaseName) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.DATABASE_NAME.toString(), databaseName);
    }

    public String getDatabaseName() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.DATABASE_NAME.toString(), null);
    }

    // instanceName is the SQL Server instance name to connect to.
    // If instanceName is not set, getInstanceName returns the default value of null.
    public void setInstanceName(String instanceName) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.INSTANCE_NAME.toString(), instanceName);
    }

    public String getInstanceName() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.INSTANCE_NAME.toString(), null);
    }

    public void setIntegratedSecurity(boolean enable) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.INTEGRATED_SECURITY.toString(), enable);
    }

    public void setAuthenticationScheme(String authenticationScheme) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.AUTHENTICATION_SCHEME.toString(), authenticationScheme);
    }

    /**
     * sets the authentication mode
     * 
     * @param authentication
     *            the authentication mode
     */
    public void setAuthentication(String authentication) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.AUTHENTICATION.toString(), authentication);
    }

    /**
     * Retrieves the authentication mode
     * 
     * @return the authentication value
     */
    public String getAuthentication() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.AUTHENTICATION.toString(),
                SQLServerDriverStringProperty.AUTHENTICATION.getDefaultValue());
    }

    /**
     * sets GSSCredential
     * 
     * @param userCredential the credential
     */
    public void setGSSCredentials(GSSCredential userCredential){
        setObjectProperty(connectionProps,SQLServerDriverObjectProperty.GSS_CREDENTIAL.toString(), userCredential);
    }

    /**
     * Retrieves the GSSCredential
     * 
     * @return GSSCredential
     */
    public GSSCredential getGSSCredentials(){
        return (GSSCredential) getObjectProperty(connectionProps, SQLServerDriverObjectProperty.GSS_CREDENTIAL.toString(),
                SQLServerDriverObjectProperty.GSS_CREDENTIAL.getDefaultValue());
    }
    
    /**
     * Sets the access token.
     * 
     * @param accessToken
     *            to be set in the string property.
     */
    public void setAccessToken(String accessToken) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.ACCESS_TOKEN.toString(), accessToken);
    }

    /**
     * Retrieves the access token.
     * 
     * @return the access token.
     */
    public String getAccessToken() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.ACCESS_TOKEN.toString(), null);
    }

    // If lastUpdateCount is set to true, the driver will return only the last update
    // count from all the update counts returned by a batch. The default of false will
    // return all update counts. If lastUpdateCount is not set, getLastUpdateCount
    // returns the default value of false.
    /**
     * Enables/disables Always Encrypted functionality for the data source object. The default is Disabled.
     * 
     * @param columnEncryptionSetting
     *            Enables/disables Always Encrypted functionality for the data source object. The default is Disabled.
     */
    public void setColumnEncryptionSetting(String columnEncryptionSetting) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.COLUMN_ENCRYPTION.toString(), columnEncryptionSetting);
    }

    /**
     * Retrieves the Always Encrypted functionality setting for the data source object.
     * 
     * @return the Always Encrypted functionality setting for the data source object.
     */
    public String getColumnEncryptionSetting() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.COLUMN_ENCRYPTION.toString(),
                SQLServerDriverStringProperty.COLUMN_ENCRYPTION.getDefaultValue());
    }

    /**
     * Sets the name that identifies a key store. Only value supported is the "JavaKeyStorePassword" for identifying the Java Key Store. The default
     * is null.
     * 
     * @param keyStoreAuthentication
     *            the name that identifies a key store.
     */
    public void setKeyStoreAuthentication(String keyStoreAuthentication) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.KEY_STORE_AUTHENTICATION.toString(), keyStoreAuthentication);
    }

    /**
     * Gets the value of the keyStoreAuthentication setting for the data source object.
     * 
     * @return the value of the keyStoreAuthentication setting for the data source object.
     */
    public String getKeyStoreAuthentication() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.KEY_STORE_AUTHENTICATION.toString(),
                SQLServerDriverStringProperty.KEY_STORE_AUTHENTICATION.getDefaultValue());
    }

    /**
     * Sets the password for the Java keystore. Note that, for Java Key Store provider the password for the keystore and the key must be the same.
     * Note that, keyStoreAuthentication must be set with "JavaKeyStorePassword".
     * 
     * @param keyStoreSecret
     *            the password to use for the keystore as well as for the key
     */
    public void setKeyStoreSecret(String keyStoreSecret) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.KEY_STORE_SECRET.toString(), keyStoreSecret);
    }

    /**
     * Sets the location including the file name for the Java keystore. Note that, keyStoreAuthentication must be set with "JavaKeyStorePassword".
     * 
     * @param keyStoreLocation
     *            the location including the file name for the Java keystore.
     */
    public void setKeyStoreLocation(String keyStoreLocation) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.KEY_STORE_LOCATION.toString(), keyStoreLocation);
    }

    /**
     * Retrieves the keyStoreLocation for the Java Key Store.
     * 
     * @return the keyStoreLocation for the Java Key Store.
     */
    public String getKeyStoreLocation() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.KEY_STORE_LOCATION.toString(),
                SQLServerDriverStringProperty.KEY_STORE_LOCATION.getDefaultValue());
    }

    public void setLastUpdateCount(boolean lastUpdateCount) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.LAST_UPDATE_COUNT.toString(), lastUpdateCount);
    }

    public boolean getLastUpdateCount() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.LAST_UPDATE_COUNT.toString(),
                SQLServerDriverBooleanProperty.LAST_UPDATE_COUNT.getDefaultValue());
    }

    // Encryption
    public void setEncrypt(boolean encrypt) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.ENCRYPT.toString(), encrypt);
    }

    public boolean getEncrypt() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.ENCRYPT.toString(),
                SQLServerDriverBooleanProperty.ENCRYPT.getDefaultValue());
    }

    /**
     * Beginning in version 6.0 of the Microsoft JDBC Driver for SQL Server, a new connection property transparentNetworkIPResolution (TNIR) is added
     * for transparent connection to Always On availability groups or to a server which has multiple IP addresses associated. When
     * transparentNetworkIPResolution is true, the driver attempts to connect to the first IP address available. If the first attempt fails, the
     * driver tries to connect to all IP addresses in parallel until the timeout expires, discarding any pending connection attempts when one of them
     * succeeds.
     * <p>
     * transparentNetworkIPResolution is ignored if multiSubnetFailover is true
     * <p>
     * transparentNetworkIPResolution is ignored if database mirroring is used
     * <p>
     * transparentNetworkIPResolution is ignored if there are more than 64 IP addresses
     * 
     * @param tnir
     *            if set to true, the driver attempts to connect to the first IP address available. It is true by default.
     */
    public void setTransparentNetworkIPResolution(boolean tnir) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.TRANSPARENT_NETWORK_IP_RESOLUTION.toString(), tnir);
    }

    /**
     * Retrieves the TransparentNetworkIPResolution value.
     * 
     * @return if enabled, returns true. Otherwise, false.
     */
    public boolean getTransparentNetworkIPResolution() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.TRANSPARENT_NETWORK_IP_RESOLUTION.toString(),
                SQLServerDriverBooleanProperty.TRANSPARENT_NETWORK_IP_RESOLUTION.getDefaultValue());
    }

    public void setTrustServerCertificate(boolean e) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.TRUST_SERVER_CERTIFICATE.toString(), e);
    }

    public boolean getTrustServerCertificate() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.TRUST_SERVER_CERTIFICATE.toString(),
                SQLServerDriverBooleanProperty.TRUST_SERVER_CERTIFICATE.getDefaultValue());
    }

    public void setTrustStoreType(String trustStoreType) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_STORE_TYPE.toString(), trustStoreType);
    }

    public String getTrustStoreType() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_STORE_TYPE.toString(),
                SQLServerDriverStringProperty.TRUST_STORE_TYPE.getDefaultValue());
    }

    public void setTrustStore(String st) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_STORE.toString(), st);
    }

    public String getTrustStore() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_STORE.toString(), null);
    }

    public void setTrustStorePassword(String p) {
        // if a non value property is set
        if (p != null)
            trustStorePasswordStripped = false;
        setStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString(), p);
    }

    public void setHostNameInCertificate(String host) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.HOSTNAME_IN_CERTIFICATE.toString(), host);
    }

    public String getHostNameInCertificate() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.HOSTNAME_IN_CERTIFICATE.toString(), null);
    }

    // lockTimeout is the number of milliseconds to wait before the database reports
    // a lock timeout. The default value of -1 means wait forever. If specified,
    // this value will be the default for all statements on the connection. Note a
    // value of 0 means no wait. If lockTimeout is not set, getLockTimeout returns
    // the default of -1.
    public void setLockTimeout(int lockTimeout) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.LOCK_TIMEOUT.toString(), lockTimeout);
    }

    public int getLockTimeout() {
        return getIntProperty(connectionProps, SQLServerDriverIntProperty.LOCK_TIMEOUT.toString(),
                SQLServerDriverIntProperty.LOCK_TIMEOUT.getDefaultValue());
    }

    // setPassword sets the password that will be used when connecting to SQL Server.
    // Note getPassword is deliberately declared non-public for security reasons.
    // If the password is not set, getPassword returns the default value of null.
    public void setPassword(String password) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.PASSWORD.toString(), password);
    }

    String getPassword() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.PASSWORD.toString(), null);
    }

    // portNumber is the TCP-IP port number used when opening a socket connection
    // to SQL Server. If portNumber is not set, getPortNumber returns the default
    // of 1433. Note as mentioned above, setPortNumber does not do any range
    // checking on the port value passed in, invalid port numbers like 99999 can
    // be passed in without triggering any error.
    public void setPortNumber(int portNumber) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.PORT_NUMBER.toString(), portNumber);
    }

    public int getPortNumber() {
        return getIntProperty(connectionProps, SQLServerDriverIntProperty.PORT_NUMBER.toString(),
                SQLServerDriverIntProperty.PORT_NUMBER.getDefaultValue());
    }

    // selectMethod is the default cursor type used for the result set. This
    // property is useful when you are dealing with large result sets and don't
    // want to store the whole result set in memory on the client side. By setting
    // the property to "cursor" you will be able to create a server side cursor that
    // can fetch smaller chunks of data at a time. If selectMethod is not set,
    // getSelectMethod returns the default value of "direct".
    public void setSelectMethod(String selectMethod) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.SELECT_METHOD.toString(), selectMethod);
    }

    public String getSelectMethod() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.SELECT_METHOD.toString(),
                SQLServerDriverStringProperty.SELECT_METHOD.getDefaultValue());
    }

    public void setResponseBuffering(String respo) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.RESPONSE_BUFFERING.toString(), respo);
    }

    public String getResponseBuffering() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.RESPONSE_BUFFERING.toString(),
                SQLServerDriverStringProperty.RESPONSE_BUFFERING.getDefaultValue());
    }

    public void setApplicationIntent(String applicationIntent) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.APPLICATION_INTENT.toString(), applicationIntent);
    }

    public String getApplicationIntent() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.APPLICATION_INTENT.toString(),
                SQLServerDriverStringProperty.APPLICATION_INTENT.getDefaultValue());
    }

    public void setSendTimeAsDatetime(boolean sendTimeAsDatetime) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.SEND_TIME_AS_DATETIME.toString(), sendTimeAsDatetime);
    }

    public boolean getSendTimeAsDatetime() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.SEND_TIME_AS_DATETIME.toString(),
                SQLServerDriverBooleanProperty.SEND_TIME_AS_DATETIME.getDefaultValue());
    }

    // If sendStringParametersAsUnicode is set to true (which is the default),
    // string parameters are sent to the server in UNICODE format. If sendStringParametersAsUnicode
    // is set to false, string parameters are sent to the server in the native TDS collation
    // format of the database, not in UNICODE. If sendStringParametersAsUnicode is not set,
    // getSendStringParametersAsUnicode returns the default of true.
    public void setSendStringParametersAsUnicode(boolean sendStringParametersAsUnicode) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.toString(),
                sendStringParametersAsUnicode);
    }

    public boolean getSendStringParametersAsUnicode() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.toString(),
                SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.getDefaultValue());
    }

    /**
     * Translates the serverName from Unicode to ASCII Compatible Encoding (ACE)
     * 
     * @param serverNameAsACE
     *            if enabled the servername will be translated to ASCII Compatible Encoding (ACE)
     */
    public void setServerNameAsACE(boolean serverNameAsACE) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.SERVER_NAME_AS_ACE.toString(), serverNameAsACE);
    }

    /**
     * Retrieves if the serverName should be translated from Unicode to ASCII Compatible Encoding (ACE)
     * 
     * @return if enabled, will return true. Otherwise, false.
     */
    public boolean getServerNameAsACE() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.SERVER_NAME_AS_ACE.toString(),
                SQLServerDriverBooleanProperty.SERVER_NAME_AS_ACE.getDefaultValue());
    }

    // serverName is the host name of the target SQL Server. If serverName is not set,
    // getServerName returns the default value of null is returned.
    public void setServerName(String serverName) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.SERVER_NAME.toString(), serverName);
    }

    public String getServerName() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.SERVER_NAME.toString(), null);
    }

    // Specify an Service Principal Name (SPN) of the target SQL Server.
    // https://msdn.microsoft.com/en-us/library/cc280459.aspx
    public void setServerSpn(String serverSpn) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.SERVER_SPN.toString(), serverSpn);
    }

    public String getServerSpn() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.SERVER_SPN.toString(), null);
    }

    // serverName is the host name of the target SQL Server. If serverName is not set,
    // getServerName returns the default value of null is returned.
    public void setFailoverPartner(String serverName) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.FAILOVER_PARTNER.toString(), serverName);
    }

    public String getFailoverPartner() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.FAILOVER_PARTNER.toString(), null);
    }

    public void setMultiSubnetFailover(boolean multiSubnetFailover) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.MULTI_SUBNET_FAILOVER.toString(), multiSubnetFailover);
    }

    public boolean getMultiSubnetFailover() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.MULTI_SUBNET_FAILOVER.toString(),
                SQLServerDriverBooleanProperty.MULTI_SUBNET_FAILOVER.getDefaultValue());
    }

    // setUser set's the user name that will be used when connecting to SQL Server.
    // If user is not set, getUser returns the default value of null.
    public void setUser(String user) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.USER.toString(), user);
    }

    public String getUser() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.USER.toString(), null);
    }

    // workstationID is the name of the client machine (or client workstation).
    // workstationID is the host name of the client in other words. If workstationID
    // is not set, the default value is constructed by calling InetAddress.getLocalHost().getHostName()
    // or if getHostName() returns blank then getHostAddress().toString().
    public void setWorkstationID(String workstationID) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.WORKSTATION_ID.toString(), workstationID);
    }

    public String getWorkstationID() {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getWorkstationID");
        String getWSID = connectionProps.getProperty(SQLServerDriverStringProperty.WORKSTATION_ID.toString());
        // Per spec, return what the logon will send here if workstationID property is not set.
        if (null == getWSID) {
            getWSID = Util.lookupHostName();
        }
        loggerExternal.exiting(getClassNameLogging(), "getWorkstationID", getWSID);
        return getWSID;
    }

    // If xopenStates is set to true, the driver will convert SQL states to XOPEN
    // compliant states. The default is false which causes the driver to generate SQL 99
    // state codes. If xopenStates is not set, getXopenStates returns the default value
    // of false.
    public void setXopenStates(boolean xopenStates) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.XOPEN_STATES.toString(), xopenStates);
    }

    public boolean getXopenStates() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.XOPEN_STATES.toString(),
                SQLServerDriverBooleanProperty.XOPEN_STATES.getDefaultValue());
    }
    
    public void setFIPS(boolean fips) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.FIPS.toString(), fips);
    }

    public boolean getFIPS() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.FIPS.toString(),
                SQLServerDriverBooleanProperty.FIPS.getDefaultValue());
    }
    
    public void setSSLProtocol(String sslProtocol) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.SSL_PROTOCOL.toString(), sslProtocol);
    }

    public String getSSLProtocol() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.SSL_PROTOCOL.toString(),
                SQLServerDriverStringProperty.SSL_PROTOCOL.getDefaultValue());
    }

    public void setTrustManagerClass(String trustManagerClass) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_MANAGER_CLASS.toString(), trustManagerClass);
    }

    public String getTrustManagerClass() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_MANAGER_CLASS.toString(),
                SQLServerDriverStringProperty.TRUST_MANAGER_CLASS.getDefaultValue());
    }

    public void setTrustManagerConstructorArg(String trustManagerClass) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_MANAGER_CONSTRUCTOR_ARG.toString(), trustManagerClass);
    }

    public String getTrustManagerConstructorArg() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_MANAGER_CONSTRUCTOR_ARG.toString(),
                SQLServerDriverStringProperty.TRUST_MANAGER_CONSTRUCTOR_ARG.getDefaultValue());
    }

    // The URL property is exposed for backwards compatibility reasons. Also, several
    // Java Application servers expect a setURL function on the DataSource and set it
    // by default (JBoss and WebLogic).

    // Note for security reasons we do not recommend that customers include the password
    // in the url supplied to setURL. The reason for this is third-party Java Application
    // Servers will very often display the value set to URL property in their DataSource
    // configuration GUI. We recommend instead that clients use the setPassword method
    // to set the password value. The Java Application Servers will not display a password
    // that is set on the DataSource in the configuration GUI.

    // Note if setURL is not called, getURL returns the default value of "jdbc:sqlserver://".
    public void setURL(String url) {
        loggerExternal.entering(getClassNameLogging(), "setURL", url);
        // URL is not stored in a property set, it is maintained separately.
        dataSourceURL = url;
        loggerExternal.exiting(getClassNameLogging(), "setURL");
    }

    public String getURL() {
        String url = dataSourceURL;
        loggerExternal.entering(getClassNameLogging(), "getURL");

        if (null == dataSourceURL)
            url = "jdbc:sqlserver://";
        loggerExternal.exiting(getClassNameLogging(), "getURL", url);
        return url;
    }

    // DataSource specific property setters/getters.

    // Per JDBC specification 16.1.1 "...the only property that all DataSource
    // implementations are required to support is the description property".
    public void setDescription(String description) {
        loggerExternal.entering(getClassNameLogging(), "setDescription", description);
        dataSourceDescription = description;
        loggerExternal.exiting(getClassNameLogging(), "setDescription");
    }

    public String getDescription() {
        loggerExternal.entering(getClassNameLogging(), "getDescription");
        loggerExternal.exiting(getClassNameLogging(), "getDescription", dataSourceDescription);
        return dataSourceDescription;
    }

    // packetSize is the size (in bytes) to use for the TCP/IP send and receive
    // buffer. It is also the value used for the TDS packet size (SQL Server
    // Network Packet Size). Validity of the value is checked at connect time.
    // If no value is set for this property, its default value is 4KB.
    public void setPacketSize(int packetSize) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.PACKET_SIZE.toString(), packetSize);
    }

    public int getPacketSize() {
        return getIntProperty(connectionProps, SQLServerDriverIntProperty.PACKET_SIZE.toString(),
                SQLServerDriverIntProperty.PACKET_SIZE.getDefaultValue());
    }

    /**
     * Setting the query timeout
     * 
     * @param queryTimeout
     *            The number of seconds to wait before a timeout has occurred on a query. The default value is 0, which means infinite timeout.
     */
    public void setQueryTimeout(int queryTimeout) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.QUERY_TIMEOUT.toString(), queryTimeout);
    }

    /**
     * Getting the query timeout
     * 
     * @return The number of seconds to wait before a timeout has occurred on a query.
     */
    public int getQueryTimeout() {
        return getIntProperty(connectionProps, SQLServerDriverIntProperty.QUERY_TIMEOUT.toString(),
                SQLServerDriverIntProperty.QUERY_TIMEOUT.getDefaultValue());
    }

    /**
     * If this configuration is false the first execution of a prepared statement will call sp_executesql and not prepare 
     * a statement, once the second execution happens it will call sp_prepexec and actually setup a prepared statement handle. Following
     * executions will call sp_execute. This relieves the need for sp_unprepare on prepared statement close if the statement is only
     * executed once.  
     * 
     * @param enablePrepareOnFirstPreparedStatementCall
     *      Changes the setting per the description.
     */
    public void setEnablePrepareOnFirstPreparedStatementCall(boolean enablePrepareOnFirstPreparedStatementCall) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT.toString(), enablePrepareOnFirstPreparedStatementCall);
    }

    /**
     * If this configuration returns false the first execution of a prepared statement will call sp_executesql and not prepare a statement, once the
     * second execution happens it will call sp_prepexec and actually setup a prepared statement handle. Following executions will call sp_execute.
     * This relieves the need for sp_unprepare on prepared statement close if the statement is only executed once.
     * 
     * @return Returns the current setting per the description.
     */
    public boolean getEnablePrepareOnFirstPreparedStatementCall() {
        boolean defaultValue = SQLServerDriverBooleanProperty.ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT.getDefaultValue();
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT.toString(),
                defaultValue);
    }

    /**
     * This setting controls how many outstanding prepared statement discard actions (sp_unprepare) can be outstanding per connection before a call to
     * clean-up the outstanding handles on the server is executed. If the setting is {@literal <=} 1 unprepare actions will be executed immedietely on
     * prepared statement close. If it is set to {@literal >} 1 these calls will be batched together to avoid overhead of calling sp_unprepare too
     * often.
     * 
     * @param serverPreparedStatementDiscardThreshold
     *            Changes the setting per the description.
     */
    public void setServerPreparedStatementDiscardThreshold(int serverPreparedStatementDiscardThreshold) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD.toString(), serverPreparedStatementDiscardThreshold);
    }

    /**
     * This setting controls how many outstanding prepared statement discard actions (sp_unprepare) can be outstanding per connection before a call to
     * clean-up the outstanding handles on the server is executed. If the setting is {@literal <=} 1 unprepare actions will be executed immedietely on
     * prepared statement close. If it is set to {@literal >} 1 these calls will be batched together to avoid overhead of calling sp_unprepare too
     * often.
     * 
     * @return Returns the current setting per the description.
     */
    public int getServerPreparedStatementDiscardThreshold() {
        int defaultSize = SQLServerDriverIntProperty.SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD.getDefaultValue();
        return getIntProperty(connectionProps, SQLServerDriverIntProperty.SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD.toString(), defaultSize);
    }

    /**
     * Specifies the size of the prepared statement cache for this connection. A value less than 1 means no cache.
     * 
     * @param statementPoolingCacheSize
     *            Changes the setting per the description.
     */
    public void setStatementPoolingCacheSize(int statementPoolingCacheSize) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.STATEMENT_POOLING_CACHE_SIZE.toString(), statementPoolingCacheSize);
    }

    /**
     * Returns the size of the prepared statement cache for this connection. A value less than 1 means no cache.
     * 
     * @return Returns the current setting per the description.
     */
    public int getStatementPoolingCacheSize() {
        int defaultSize = SQLServerDriverIntProperty.STATEMENT_POOLING_CACHE_SIZE.getDefaultValue();
        return getIntProperty(connectionProps, SQLServerDriverIntProperty.STATEMENT_POOLING_CACHE_SIZE.toString(), defaultSize);
    }
    
    /**
     * Sets the statement pooling to true or false
     * @param disableStatementPooling
     */
    public void setDisableStatementPooling(boolean disableStatementPooling) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.DISABLE_STATEMENT_POOLING.toString(), disableStatementPooling);       
    }
    
    /**
     * Returns true if statement pooling is disabled.
     * @return
     */
    public boolean getDisableStatementPooling() {
        boolean defaultValue = SQLServerDriverBooleanProperty.DISABLE_STATEMENT_POOLING.getDefaultValue();
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.DISABLE_STATEMENT_POOLING.toString(),
                defaultValue);
    }

    /**
     * Setting the socket timeout
     * 
     * @param socketTimeout
     *            The number of milliseconds to wait before a timeout is occurred on a socket read or accept. The default value is 0, which means
     *            infinite timeout.
     */
    public void setSocketTimeout(int socketTimeout) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.SOCKET_TIMEOUT.toString(), socketTimeout);
    }

    /**
     * Getting the socket timeout
     * 
     * @return The number of milliseconds to wait before a timeout is occurred on a socket read or accept.
     */
    public int getSocketTimeout() {
        int defaultTimeOut = SQLServerDriverIntProperty.SOCKET_TIMEOUT.getDefaultValue();
        return getIntProperty(connectionProps, SQLServerDriverIntProperty.SOCKET_TIMEOUT.toString(), defaultTimeOut);
    }

    /**
     * Sets the login configuration file for Kerberos authentication. This
     * overrides the default configuration <i> SQLJDBCDriver </i>
     * 
     * @param configurationName the configuration name
     */
    public void setJASSConfigurationName(String configurationName) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.JAAS_CONFIG_NAME.toString(),
                configurationName);
    }

    /**
     * Retrieves the login configuration file for Kerberos authentication.
     * 
     * @return login configuration file name
     */
    public String getJASSConfigurationName() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.JAAS_CONFIG_NAME.toString(),
                SQLServerDriverStringProperty.JAAS_CONFIG_NAME.getDefaultValue());
    }
    
    // responseBuffering controls the driver's buffering of responses from SQL Server.
    // Possible values are:
    //
    // "full" - Fully buffer the response at execution time.
    // Advantages:
    // 100% back compat with v1.1 driver
    // Maximizes concurrency on the server
    // Disadvantages:
    // Consumes more client-side memory
    // Client scalability limits with large responses
    // More execute latency
    //
    // "adaptive" - Data Pipe adaptive buffering
    // Advantages:
    // Buffers only when necessary, only as much as necessary
    // Enables handling very large responses, values
    // Disadvantages
    // Reduced concurrency on the server
    // Internal functions for setting/getting property values.

    // Set a string property value.
    // Caller will always supply a non-null props and propKey.
    // Caller may supply a null propValue, in this case no property value is set.
    private void setStringProperty(Properties props,
            String propKey,
            String propValue) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER) && !propKey.contains("password") && !propKey.contains("Password")) {
            loggerExternal.entering(getClassNameLogging(), "set" + propKey, propValue);
        }
        else
            loggerExternal.entering(getClassNameLogging(), "set" + propKey);
        if (null != propValue)
            props.setProperty(propKey, propValue);
        loggerExternal.exiting(getClassNameLogging(), "set" + propKey);
    }

    // Reads property value in String format.
    // Caller will always supply a non-null props and propKey.
    // Returns null if the specific property value is not set.
    private String getStringProperty(Properties props,
            String propKey,
            String defaultValue) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "get" + propKey);
        String propValue = props.getProperty(propKey);
        if (null == propValue)
            propValue = defaultValue;
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER) && !propKey.contains("password") && !propKey.contains("Password"))
            loggerExternal.exiting(getClassNameLogging(), "get" + propKey, propValue);
        return propValue;
    }

    // Set an integer property value.
    // Caller will always supply a non-null props and propKey.
    private void setIntProperty(Properties props,
            String propKey,
            int propValue) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "set" + propKey, propValue);
        props.setProperty(propKey, Integer.valueOf(propValue).toString());
        loggerExternal.exiting(getClassNameLogging(), "set" + propKey);
    }

    // Reads a property value in int format.
    // Caller will always supply a non-null props and propKey.
    // Returns defaultValue if the specific property value is not set.
    private int getIntProperty(Properties props,
            String propKey,
            int defaultValue) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "get" + propKey);
        String propValue = props.getProperty(propKey);
        int value = defaultValue;
        if (null != propValue) {
            try {
                value = Integer.parseInt(propValue);
            }
            catch (NumberFormatException nfe) {
                // This exception cannot occur as all of our properties
                // are set internally by int -> Integer.toString.
                assert false : "Bad portNumber:-" + propValue;
            }
        }
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.exiting(getClassNameLogging(), "get" + propKey, value);
        return value;
    }

    // Set a boolean property value.
    // Caller will always supply a non-null props and propKey.
    private void setBooleanProperty(Properties props,
            String propKey,
            boolean propValue) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "set" + propKey, propValue);
        props.setProperty(propKey, (propValue) ? "true" : "false");
        loggerExternal.exiting(getClassNameLogging(), "set" + propKey);
    }

    // Reads a property value in boolean format.
    // Caller will always supply a non-null props and propKey.
    // Returns defaultValue if the specific property value is not set.
    private boolean getBooleanProperty(Properties props,
            String propKey,
            boolean defaultValue) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "get" + propKey);
        String propValue = props.getProperty(propKey);
        Boolean value;
        if (null == propValue) {
            value = defaultValue;
        }
        else {
            // Since we set the value of the String property ourselves to
            // "true" or "false", we can do this.
            value = Boolean.valueOf(propValue);
        }
        loggerExternal.exiting(getClassNameLogging(), "get" + propKey, value);
        return value;
    }

    private void setObjectProperty(Properties props,
            String propKey,
            Object propValue) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER)) {
            loggerExternal.entering(getClassNameLogging(), "set" + propKey);
        }
        if (null != propValue) {
            props.put(propKey, propValue);
        }
        loggerExternal.exiting(getClassNameLogging(), "set" + propKey);
    }

    private Object getObjectProperty(Properties props,
            String propKey,
            Object defaultValue) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "get" + propKey);
        Object propValue = props.get(propKey);
        if (null == propValue)
            propValue = defaultValue;
        loggerExternal.exiting(getClassNameLogging(), "get" + propKey);
        return propValue;
    }
    
    // Returns a SQLServerConnection given username, password, and pooledConnection.
    // Note that the DataSource properties set to connectionProps are used when creating
    // the connection.

    // Both username and password can be null.

    // If pooledConnection is not null, then connection returned is attached to the pooledConnection
    // and participates in connection pooling.
    SQLServerConnection getConnectionInternal(String username,
            String password,
            SQLServerPooledConnection pooledConnection) throws SQLServerException {
        Properties userSuppliedProps;
        Properties mergedProps;
        // Trust store password stripped and this object got created via Objectfactory referencing.
        if (trustStorePasswordStripped)
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_referencingFailedTSP"), null, true);

        // If username or password is passed in, clone the property set so we
        // don't alter original connectionProps.
        if (null != username || null != password) {
            userSuppliedProps = (Properties) this.connectionProps.clone();

            // Remove user and password from connectionProps if set.
            // We want the user supplied user+password to replace
            // whatever is set in connectionProps.
            userSuppliedProps.remove(SQLServerDriverStringProperty.USER.toString());
            userSuppliedProps.remove(SQLServerDriverStringProperty.PASSWORD.toString());

            if (null != username)
                userSuppliedProps.put(SQLServerDriverStringProperty.USER.toString(), username);
            if (null != password)
                userSuppliedProps.put(SQLServerDriverStringProperty.PASSWORD.toString(), password);
        }
        else {
            userSuppliedProps = connectionProps;
        }

        // Merge in URL properties into userSuppliedProps if URL is set.
        if (null != dataSourceURL) {
            Properties urlProps = Util.parseUrl(dataSourceURL, dsLogger);
            // null returned properties means that the passed in URL is not supported.
            if (null == urlProps)
                SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_errorConnectionString"), null, true);
            // Manually merge URL props and user supplied props.
            mergedProps = SQLServerDriver.mergeURLAndSuppliedProperties(urlProps, userSuppliedProps);
        }
        else {
            mergedProps = userSuppliedProps;
        }

        // Create new connection and connect.
        if (dsLogger.isLoggable(Level.FINER))
            dsLogger.finer(toString() + " Begin create new connection.");
        SQLServerConnection result = null;
        if (Util.use43Wrapper()) {
            result = new SQLServerConnection43(toString());
        }
        else {
            result = new SQLServerConnection(toString());
        }
        result.connect(mergedProps, pooledConnection);
        if (dsLogger.isLoggable(Level.FINER))
            dsLogger.finer(toString() + " End create new connection " + result.toString());
        return result;
    }

    // Implement javax.naming.Referenceable interface methods.

    public Reference getReference() {
        loggerExternal.entering(getClassNameLogging(), "getReference");
        Reference ref = getReferenceInternal("com.microsoft.sqlserver.jdbc.SQLServerDataSource");
        loggerExternal.exiting(getClassNameLogging(), "getReference", ref);
        return ref;
    }

    Reference getReferenceInternal(String dataSourceClassString) {
        if (dsLogger.isLoggable(Level.FINER))
            dsLogger.finer(toString() + " creating reference for " + dataSourceClassString + ".");

        Reference ref = new Reference(this.getClass().getName(), "com.microsoft.sqlserver.jdbc.SQLServerDataSourceObjectFactory", null);
        if (null != dataSourceClassString)
            ref.add(new StringRefAddr("class", dataSourceClassString));

        if (trustStorePasswordStripped)
            ref.add(new StringRefAddr("trustStorePasswordStripped", "true"));

        // Add each property name+value pair found in connectionProps.
        Enumeration<?> e = connectionProps.keys();
        while (e.hasMoreElements()) {
            String propertyName = (String) e.nextElement();
            // If a trustStore password is set, it is omitted and a trustStorePasswordSet flag is set.
            if (propertyName.equals(SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString())) {
                // The property set and the variable set at the same time is not possible
                assert trustStorePasswordStripped == false;
                ref.add(new StringRefAddr("trustStorePasswordStripped", "true"));
            }
            else {
                // do not add passwords to the collection. we have normal password
                if (!propertyName.contains(SQLServerDriverStringProperty.PASSWORD.toString()))
                    ref.add(new StringRefAddr(propertyName, connectionProps.getProperty(propertyName)));
            }
        }

        // Add dataSourceURL and dataSourceDescription as these will not appear in connectionProps.
        if (null != dataSourceURL)
            ref.add(new StringRefAddr("dataSourceURL", dataSourceURL));

        if (null != dataSourceDescription)
            ref.add(new StringRefAddr("dataSourceDescription", dataSourceDescription));

        return ref;
    }

    // Initialize this datasource from properties found inside the reference ref.
    // Called by SQLServerDataSourceObjectFactory to initialize new DataSource instance.
    void initializeFromReference(javax.naming.Reference ref) {
        // Enumerate all the StringRefAddr objects in the Reference and assign properties appropriately.
        Enumeration<?> e = ref.getAll();
        while (e.hasMoreElements()) {
            StringRefAddr addr = (StringRefAddr) e.nextElement();
            String propertyName = addr.getType();
            String propertyValue = (String) addr.getContent();

            // Special case dataSourceURL and dataSourceDescription.
            if ("dataSourceURL".equals(propertyName)) {
                dataSourceURL = propertyValue;
            }
            else if ("dataSourceDescription".equals(propertyName)) {
                dataSourceDescription = propertyValue;
            }
            else if ("trustStorePasswordStripped".equals(propertyName)) {
                trustStorePasswordStripped = true;
            }
            // Just skip "class" StringRefAddr, it does not go into connectionProps
            else if (!"class".equals(propertyName)) {

                connectionProps.setProperty(propertyName, propertyValue);
            }
        }
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "isWrapperFor", iface);
        boolean f = iface.isInstance(this);
        loggerExternal.exiting(getClassNameLogging(), "isWrapperFor", f);
        return f;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "unwrap", iface);
        T t;
        try {
            t = iface.cast(this);
        }
        catch (ClassCastException e) {
            throw new SQLServerException(e.getMessage(), e);
        }
        loggerExternal.exiting(getClassNameLogging(), "unwrap", t);
        return t;
    }


    // Returns unique id for each DataSource instance.
    private static int nextDataSourceID() {
        return baseDataSourceID.incrementAndGet();
    }

    private Object writeReplace() throws java.io.ObjectStreamException {
        return new SerializationProxy(this);
    }

    private void readObject(java.io.ObjectInputStream stream) throws java.io.InvalidObjectException {
        // For added security/robustness, the only way to rehydrate a serialized SQLServerDataSource
        // is to use a SerializationProxy. Direct use of readObject() is not supported.
        throw new java.io.InvalidObjectException("");
    }

    // This code is duplicated in pooled and XA datasource classes.
    private static class SerializationProxy implements java.io.Serializable {
        private final Reference ref;
        private static final long serialVersionUID = 654661379542314226L;

        SerializationProxy(SQLServerDataSource ds) {
            // We do not need the class name so pass null, serialization mechanism
            // stores the class info.
            ref = ds.getReferenceInternal(null);
        }

        private Object readResolve() {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.initializeFromReference(ref);
            return ds;
        }
    }
}
