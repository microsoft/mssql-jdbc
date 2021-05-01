/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.naming.Reference;
import javax.naming.StringRefAddr;

import org.ietf.jgss.GSSCredential;


/**
 * Contains a list of properties specific for the {@link SQLServerConnection} class.
 */
public class SQLServerDataSource
        implements ISQLServerDataSource, javax.sql.DataSource, java.io.Serializable, javax.naming.Referenceable {
    // dsLogger is logger used for all SQLServerDataSource instances.
    static final java.util.logging.Logger dsLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerDataSource");
    static final java.util.logging.Logger loggerExternal = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.DataSource");
    static final private java.util.logging.Logger parentLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc");

    /** logging class name */
    final private String loggingClassName;

    /**
     * trustStorePasswordStripped flag
     */
    private boolean trustStorePasswordStripped = false;

    /**
     * Always refresh SerialVersionUID when prompted
     */
    private static final long serialVersionUID = 654861379544314296L;

    /**
     * Properties passed to SQLServerConnection class
     */
    private Properties connectionProps;

    /**
     * URL for datasource
     */
    private String dataSourceURL;

    /**
     * Description for datasource.
     */
    private String dataSourceDescription;

    /**
     * Unique id generator for each DataSource instance (used for logging).
     */
    static private final AtomicInteger baseDataSourceID = new AtomicInteger(0);

    /**
     * trace ID
     */
    final private String traceID;

    /**
     * Constructs a SQLServerDataSource.
     */
    public SQLServerDataSource() {
        connectionProps = new Properties();
        int dataSourceID = nextDataSourceID();
        String nameL = getClass().getName();
        traceID = nameL.substring(1 + nameL.lastIndexOf('.')) + ":" + dataSourceID;
        loggingClassName = "com.microsoft.sqlserver.jdbc." + nameL.substring(1 + nameL.lastIndexOf('.')) + ":"
                + dataSourceID;
    }

    String getClassNameLogging() {
        return loggingClassName;
    }

    @Override
    public String toString() {
        return traceID;
    }

    // DataSource interface public methods

    @Override
    public Connection getConnection() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getConnection");
        Connection con = getConnectionInternal(null, null, null);
        loggerExternal.exiting(getClassNameLogging(), "getConnection", con);
        return con;
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLServerException {
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getConnection",
                    new Object[] {username, "Password not traced"});
        Connection con = getConnectionInternal(username, password, null);
        loggerExternal.exiting(getClassNameLogging(), "getConnection", con);
        return con;
    }

    /**
     * Sets the maximum time in seconds that this data source will wait while attempting to connect to a database. Note
     * default value is 0.
     */
    @Override
    public void setLoginTimeout(int loginTimeout) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.LOGIN_TIMEOUT.toString(), loginTimeout);
    }

    @Override
    public int getLoginTimeout() {
        int defaultTimeOut = SQLServerDriverIntProperty.LOGIN_TIMEOUT.getDefaultValue();
        final int logintimeout = getIntProperty(connectionProps, SQLServerDriverIntProperty.LOGIN_TIMEOUT.toString(),
                defaultTimeOut);
        // even if the user explicitly sets the timeout to zero, convert to 15
        return (logintimeout == 0) ? defaultTimeOut : logintimeout;
    }

    /**
     * Sets the log writer for this DataSource. Currently we just hold onto this logWriter and pass it back to callers,
     * nothing else.
     */
    private transient PrintWriter logWriter;

    @Override
    public void setLogWriter(PrintWriter out) {
        loggerExternal.entering(getClassNameLogging(), "setLogWriter", out);
        logWriter = out;
        loggerExternal.exiting(getClassNameLogging(), "setLogWriter");
    }

    /**
     * Returns the log writer for this DataSource.
     */
    @Override
    public PrintWriter getLogWriter() {
        loggerExternal.entering(getClassNameLogging(), "getLogWriter");
        loggerExternal.exiting(getClassNameLogging(), "getLogWriter", logWriter);
        return logWriter;
    }

    @Override
    public Logger getParentLogger() throws java.sql.SQLFeatureNotSupportedException {
        return parentLogger;
    }

    // Core Connection property setters/getters.

    /**
     * Sets the specific application in various SQL Server profiling and logging tools.
     */
    @Override
    public void setApplicationName(String applicationName) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.APPLICATION_NAME.toString(), applicationName);
    }

    @Override
    public String getApplicationName() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.APPLICATION_NAME.toString(),
                SQLServerDriverStringProperty.APPLICATION_NAME.getDefaultValue());
    }

    /**
     * Sets the the database to connect to.
     * 
     * @param databaseName
     *        if not set, returns the default value of null.
     */
    @Override
    public void setDatabaseName(String databaseName) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.DATABASE_NAME.toString(), databaseName);
    }

    @Override
    public String getDatabaseName() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.DATABASE_NAME.toString(), null);
    }

    /**
     * Sets the the SQL Server instance name to connect to.
     * 
     * @param instanceName
     *        if not set, returns the default value of null.
     */
    @Override
    public void setInstanceName(String instanceName) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.INSTANCE_NAME.toString(), instanceName);
    }

    @Override
    public String getInstanceName() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.INSTANCE_NAME.toString(), null);
    }

    @Override
    public void setIntegratedSecurity(boolean enable) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.INTEGRATED_SECURITY.toString(), enable);
    }

    @Override
    public void setAuthenticationScheme(String authenticationScheme) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.AUTHENTICATION_SCHEME.toString(),
                authenticationScheme);
    }

    @Override
    public void setAuthentication(String authentication) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.AUTHENTICATION.toString(), authentication);
    }

    @Override
    public String getAuthentication() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.AUTHENTICATION.toString(),
                SQLServerDriverStringProperty.AUTHENTICATION.getDefaultValue());
    }

    @Override
    public void setGSSCredentials(GSSCredential userCredential) {
        setObjectProperty(connectionProps, SQLServerDriverObjectProperty.GSS_CREDENTIAL.toString(), userCredential);
    }

    @Override
    public GSSCredential getGSSCredentials() {
        return (GSSCredential) getObjectProperty(connectionProps,
                SQLServerDriverObjectProperty.GSS_CREDENTIAL.toString(),
                SQLServerDriverObjectProperty.GSS_CREDENTIAL.getDefaultValue());
    }

    @Override
    public void setAccessToken(String accessToken) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.ACCESS_TOKEN.toString(), accessToken);
    }

    @Override
    public String getAccessToken() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.ACCESS_TOKEN.toString(), null);
    }

    /**
     * Sets the Column Encryption setting. If lastUpdateCount is set to true, the driver will return only the last
     * update count from all the update counts returned by a batch. The default of false will return all update counts.
     * If lastUpdateCount is not set, getLastUpdateCount returns the default value of false.
     */
    @Override
    public void setColumnEncryptionSetting(String columnEncryptionSetting) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.COLUMN_ENCRYPTION.toString(),
                columnEncryptionSetting);
    }

    @Override
    public String getColumnEncryptionSetting() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.COLUMN_ENCRYPTION.toString(),
                SQLServerDriverStringProperty.COLUMN_ENCRYPTION.getDefaultValue());
    }

    @Override
    public void setKeyStoreAuthentication(String keyStoreAuthentication) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.KEY_STORE_AUTHENTICATION.toString(),
                keyStoreAuthentication);
    }

    @Override
    public String getKeyStoreAuthentication() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.KEY_STORE_AUTHENTICATION.toString(),
                SQLServerDriverStringProperty.KEY_STORE_AUTHENTICATION.getDefaultValue());
    }

    @Override
    public void setKeyStoreSecret(String keyStoreSecret) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.KEY_STORE_SECRET.toString(), keyStoreSecret);
    }

    @Override
    public void setKeyStoreLocation(String keyStoreLocation) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.KEY_STORE_LOCATION.toString(),
                keyStoreLocation);
    }

    @Override
    public String getKeyStoreLocation() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.KEY_STORE_LOCATION.toString(),
                SQLServerDriverStringProperty.KEY_STORE_LOCATION.getDefaultValue());
    }

    @Override
    public void setLastUpdateCount(boolean lastUpdateCount) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.LAST_UPDATE_COUNT.toString(),
                lastUpdateCount);
    }

    @Override
    public boolean getLastUpdateCount() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.LAST_UPDATE_COUNT.toString(),
                SQLServerDriverBooleanProperty.LAST_UPDATE_COUNT.getDefaultValue());
    }

    @Override
    public void setEncrypt(boolean encrypt) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.ENCRYPT.toString(), encrypt);
    }

    @Override
    public boolean getEncrypt() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.ENCRYPT.toString(),
                SQLServerDriverBooleanProperty.ENCRYPT.getDefaultValue());
    }

    @Override
    public void setTransparentNetworkIPResolution(boolean tnir) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.TRANSPARENT_NETWORK_IP_RESOLUTION.toString(),
                tnir);
    }

    @Override
    public boolean getTransparentNetworkIPResolution() {
        return getBooleanProperty(connectionProps,
                SQLServerDriverBooleanProperty.TRANSPARENT_NETWORK_IP_RESOLUTION.toString(),
                SQLServerDriverBooleanProperty.TRANSPARENT_NETWORK_IP_RESOLUTION.getDefaultValue());
    }

    @Override
    public void setTrustServerCertificate(boolean e) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.TRUST_SERVER_CERTIFICATE.toString(), e);
    }

    @Override
    public boolean getTrustServerCertificate() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.TRUST_SERVER_CERTIFICATE.toString(),
                SQLServerDriverBooleanProperty.TRUST_SERVER_CERTIFICATE.getDefaultValue());
    }

    @Override
    public void setTrustStoreType(String trustStoreType) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_STORE_TYPE.toString(), trustStoreType);
    }

    @Override
    public String getTrustStoreType() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_STORE_TYPE.toString(),
                SQLServerDriverStringProperty.TRUST_STORE_TYPE.getDefaultValue());
    }

    @Override
    public void setTrustStore(String trustStore) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_STORE.toString(), trustStore);
    }

    @Override
    public String getTrustStore() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_STORE.toString(), null);
    }

    @Override
    public void setTrustStorePassword(String trustStorePassword) {
        // if a non value property is set
        if (trustStorePassword != null)
            trustStorePasswordStripped = false;
        setStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString(),
                trustStorePassword);
    }

    String getTrustStorePassword() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString(), null);
    }

    @Override
    public void setHostNameInCertificate(String hostName) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.HOSTNAME_IN_CERTIFICATE.toString(), hostName);
    }

    @Override
    public String getHostNameInCertificate() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.HOSTNAME_IN_CERTIFICATE.toString(),
                null);
    }

    /**
     * Sets the lock timeout value.
     * 
     * @param lockTimeout
     *        the number of milliseconds to wait before the database reports a lock timeout. The default value of -1
     *        means wait forever. If specified, this value will be the default for all statements on the connection.
     *        Note a value of 0 means no wait. If lockTimeout is not set, getLockTimeout returns the default of -1.
     */
    @Override
    public void setLockTimeout(int lockTimeout) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.LOCK_TIMEOUT.toString(), lockTimeout);
    }

    @Override
    public int getLockTimeout() {
        return getIntProperty(connectionProps, SQLServerDriverIntProperty.LOCK_TIMEOUT.toString(),
                SQLServerDriverIntProperty.LOCK_TIMEOUT.getDefaultValue());
    }

    /**
     * Sets the password that will be used when connecting to SQL Server.
     * 
     * @param password
     *        Note getPassword is deliberately declared non-public for security reasons. If the password is not set,
     *        getPassword returns the default value of null.
     */
    @Override
    public void setPassword(String password) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.PASSWORD.toString(), password);
    }

    String getPassword() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.PASSWORD.toString(), null);
    }

    /**
     * Sets the TCP-IP port number used when opening a socket connection to SQL Server.
     * 
     * @param portNumber
     *        if not set, getPortNumber returns the default of 1433. Note as mentioned above, setPortNumber does not do
     *        any range checking on the port value passed in,\ invalid port numbers like 99999 can be passed in without
     *        triggering any error.
     */
    @Override
    public void setPortNumber(int portNumber) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.PORT_NUMBER.toString(), portNumber);
    }

    @Override
    public int getPortNumber() {
        return getIntProperty(connectionProps, SQLServerDriverIntProperty.PORT_NUMBER.toString(),
                SQLServerDriverIntProperty.PORT_NUMBER.getDefaultValue());
    }

    /**
     * Sets the default cursor type used for the result set.
     * 
     * @param selectMethod
     *        This(non-Javadoc) @see com.microsoft.sqlserver.jdbc.ISQLServerDataSource#setSelectMethod(java.lang.String)
     *        property is useful when you are dealing with large result sets and do not want to store the whole result
     *        set in memory on the client side. By setting the property to "cursor" you will be able to create a server
     *        side cursor that can fetch smaller chunks of data at a time. If selectMethod is not set, getSelectMethod
     *        returns the default value of "direct".
     */
    @Override
    public void setSelectMethod(String selectMethod) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.SELECT_METHOD.toString(), selectMethod);
    }

    @Override
    public String getSelectMethod() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.SELECT_METHOD.toString(),
                SQLServerDriverStringProperty.SELECT_METHOD.getDefaultValue());
    }

    @Override
    public void setResponseBuffering(String bufferingMode) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.RESPONSE_BUFFERING.toString(), bufferingMode);
    }

    @Override
    public String getResponseBuffering() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.RESPONSE_BUFFERING.toString(),
                SQLServerDriverStringProperty.RESPONSE_BUFFERING.getDefaultValue());
    }

    @Override
    public void setApplicationIntent(String applicationIntent) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.APPLICATION_INTENT.toString(),
                applicationIntent);
    }

    @Override
    public String getApplicationIntent() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.APPLICATION_INTENT.toString(),
                SQLServerDriverStringProperty.APPLICATION_INTENT.getDefaultValue());
    }

    @Override
    public void setReplication(boolean replication) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.REPLICATION.toString(), replication);
    }

    @Override
    public boolean getReplication() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.REPLICATION.toString(),
                SQLServerDriverBooleanProperty.REPLICATION.getDefaultValue());
    }

    @Override
    public void setSendTimeAsDatetime(boolean sendTimeAsDatetime) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.SEND_TIME_AS_DATETIME.toString(),
                sendTimeAsDatetime);
    }

    @Override
    public boolean getSendTimeAsDatetime() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.SEND_TIME_AS_DATETIME.toString(),
                SQLServerDriverBooleanProperty.SEND_TIME_AS_DATETIME.getDefaultValue());
    }

    @Override
    public void setUseFmtOnly(boolean useFmtOnly) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.USE_FMT_ONLY.toString(), useFmtOnly);
    }

    @Override
    public boolean getUseFmtOnly() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.USE_FMT_ONLY.toString(),
                SQLServerDriverBooleanProperty.USE_FMT_ONLY.getDefaultValue());
    }

    @Override
    public void setDelayLoadingLobs(boolean delayLoadingLobs) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.DELAY_LOADING_LOBS.toString(),
                delayLoadingLobs);
    }

    @Override
    public boolean getDelayLoadingLobs() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.DELAY_LOADING_LOBS.toString(),
                SQLServerDriverBooleanProperty.DELAY_LOADING_LOBS.getDefaultValue());
    }

    /**
     * Sets whether string parameters are sent to the server in UNICODE format.
     * 
     * @param sendStringParametersAsUnicode
     *        if true (default), string parameters are sent to the server in UNICODE format. if false, string parameters
     *        are sent to the server in the native TDS collation format of the database, not in UNICODE. if set, returns
     *        the default of true.
     */
    @Override
    public void setSendStringParametersAsUnicode(boolean sendStringParametersAsUnicode) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.toString(),
                sendStringParametersAsUnicode);
    }

    @Override
    public boolean getSendStringParametersAsUnicode() {
        return getBooleanProperty(connectionProps,
                SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.toString(),
                SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.getDefaultValue());
    }

    @Override
    public void setServerNameAsACE(boolean serverNameAsACE) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.SERVER_NAME_AS_ACE.toString(),
                serverNameAsACE);
    }

    @Override
    public boolean getServerNameAsACE() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.SERVER_NAME_AS_ACE.toString(),
                SQLServerDriverBooleanProperty.SERVER_NAME_AS_ACE.getDefaultValue());
    }

    /**
     * Sets the host name of the target SQL Server.
     * 
     * @param serverName
     *        if not set, returns the default value of null is returned.
     */
    @Override
    public void setServerName(String serverName) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.SERVER_NAME.toString(), serverName);
    }

    @Override
    public String getServerName() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.SERVER_NAME.toString(), null);
    }

    /**
     * Sets the Service Principal Name (SPN) of the target SQL Server.
     * https://msdn.microsoft.com/en-us/library/cc280459.aspx
     * 
     * @param serverSpn
     *        service principal name
     */
    @Override
    public void setServerSpn(String serverSpn) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.SERVER_SPN.toString(), serverSpn);
    }

    @Override
    public String getServerSpn() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.SERVER_SPN.toString(), null);
    }

    /**
     * Sets the fail over partner of the target SQL Server.
     * 
     * @param serverName
     *        if not set, returns the default value of null.
     */
    @Override
    public void setFailoverPartner(String serverName) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.FAILOVER_PARTNER.toString(), serverName);
    }

    @Override
    public String getFailoverPartner() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.FAILOVER_PARTNER.toString(), null);
    }

    @Override
    public void setMultiSubnetFailover(boolean multiSubnetFailover) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.MULTI_SUBNET_FAILOVER.toString(),
                multiSubnetFailover);
    }

    @Override
    public boolean getMultiSubnetFailover() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.MULTI_SUBNET_FAILOVER.toString(),
                SQLServerDriverBooleanProperty.MULTI_SUBNET_FAILOVER.getDefaultValue());
    }

    /**
     * Sets the user name that will be used when connecting to SQL Server.
     * 
     * @param user
     *        if not set, returns the default value of null.
     */
    @Override
    public void setUser(String user) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.USER.toString(), user);
    }

    @Override
    public String getUser() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.USER.toString(), null);
    }

    /**
     * Sets the name of the client machine (or client workstation).
     * 
     * @param workstationID
     *        host name of the client. if not set, the default value is constructed by calling
     *        InetAddress.getLocalHost().getHostName() or if getHostName() returns blank then
     *        getHostAddress().toString().
     */
    @Override
    public void setWorkstationID(String workstationID) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.WORKSTATION_ID.toString(), workstationID);
    }

    @Override
    public String getWorkstationID() {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getWorkstationID");
        String getWSID = connectionProps.getProperty(SQLServerDriverStringProperty.WORKSTATION_ID.toString());
        // Per spec, return what the logon will send here if workstationID
        // property is not set.
        if (null == getWSID) {
            getWSID = Util.lookupHostName();
        }
        loggerExternal.exiting(getClassNameLogging(), "getWorkstationID", getWSID);
        return getWSID;
    }

    /**
     * Sets whether the driver will convert SQL states to XOPEN compliant states.
     * 
     * @param xopenStates
     *        if true, the driver will convert SQL states to XOPEN compliant states. The default is false which causes
     *        the driver to generate SQL 99 state codes. If not set, getXopenStates returns the default value of false.
     */
    @Override
    public void setXopenStates(boolean xopenStates) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.XOPEN_STATES.toString(), xopenStates);
    }

    @Override
    public boolean getXopenStates() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.XOPEN_STATES.toString(),
                SQLServerDriverBooleanProperty.XOPEN_STATES.getDefaultValue());
    }

    @Override
    public void setFIPS(boolean fips) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.FIPS.toString(), fips);
    }

    @Override
    public boolean getFIPS() {
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.FIPS.toString(),
                SQLServerDriverBooleanProperty.FIPS.getDefaultValue());
    }

    @Override
    public String getSocketFactoryClass() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.SOCKET_FACTORY_CLASS.toString(), null);
    }

    @Override
    public void setSocketFactoryClass(String socketFactoryClass) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.SOCKET_FACTORY_CLASS.toString(),
                socketFactoryClass);
    }

    @Override
    public String getSocketFactoryConstructorArg() {
        return getStringProperty(connectionProps,
                SQLServerDriverStringProperty.SOCKET_FACTORY_CONSTRUCTOR_ARG.toString(), null);
    }

    @Override
    public void setSocketFactoryConstructorArg(String socketFactoryConstructorArg) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.SOCKET_FACTORY_CONSTRUCTOR_ARG.toString(),
                socketFactoryConstructorArg);
    }

    @Override
    public void setSSLProtocol(String sslProtocol) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.SSL_PROTOCOL.toString(), sslProtocol);
    }

    @Override
    public String getSSLProtocol() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.SSL_PROTOCOL.toString(),
                SQLServerDriverStringProperty.SSL_PROTOCOL.getDefaultValue());
    }

    @Override
    public void setTrustManagerClass(String trustManagerClass) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_MANAGER_CLASS.toString(),
                trustManagerClass);
    }

    @Override
    public String getTrustManagerClass() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_MANAGER_CLASS.toString(),
                SQLServerDriverStringProperty.TRUST_MANAGER_CLASS.getDefaultValue());
    }

    @Override
    public void setTrustManagerConstructorArg(String trustManagerConstructorArg) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.TRUST_MANAGER_CONSTRUCTOR_ARG.toString(),
                trustManagerConstructorArg);
    }

    @Override
    public String getTrustManagerConstructorArg() {
        return getStringProperty(connectionProps,
                SQLServerDriverStringProperty.TRUST_MANAGER_CONSTRUCTOR_ARG.toString(),
                SQLServerDriverStringProperty.TRUST_MANAGER_CONSTRUCTOR_ARG.getDefaultValue());
    }

    /**
     * Sets the datasource URL.
     * 
     * @param url
     *        The URL property is exposed for backwards compatibility reasons. Also, several Java Application servers
     *        expect a setURL function on the DataSource and set it by default (JBoss and WebLogic) Note for security
     *        reasons we do not recommend that customers include the password in the url supplied to setURL. The reason
     *        for this is third-party Java Application Servers will very often display the value set to URL property in
     *        their DataSource configuration GUI. We recommend instead that clients use the setPassword method to set
     *        the password value. The Java Application Servers will not display a password that is set on the DataSource
     *        in the configuration GUI. Note if setURL is not called, getURL returns the default value of
     *        "jdbc:sqlserver://".
     */
    @Override
    public void setURL(String url) {
        loggerExternal.entering(getClassNameLogging(), "setURL", url);
        // URL is not stored in a property set, it is maintained separately.
        dataSourceURL = url;
        loggerExternal.exiting(getClassNameLogging(), "setURL");
    }

    @Override
    public String getURL() {
        String url = dataSourceURL;
        loggerExternal.entering(getClassNameLogging(), "getURL");

        if (null == dataSourceURL)
            url = "jdbc:sqlserver://";
        loggerExternal.exiting(getClassNameLogging(), "getURL", url);
        return url;
    }

    /**
     * Sets the DataSource description. Per JDBC specification 16.1.1 "...the only property that all DataSource
     * implementations are required to support is the description property".
     */
    @Override
    public void setDescription(String description) {
        loggerExternal.entering(getClassNameLogging(), "setDescription", description);
        dataSourceDescription = description;
        loggerExternal.exiting(getClassNameLogging(), "setDescription");
    }

    /**
     * Returns the DataSource description
     */
    @Override
    public String getDescription() {
        loggerExternal.entering(getClassNameLogging(), "getDescription");
        loggerExternal.exiting(getClassNameLogging(), "getDescription", dataSourceDescription);
        return dataSourceDescription;
    }

    /**
     * Sets the packet size.
     * 
     * @param packetSize
     *        the size (in bytes) to use for the TCP/IP send and receive buffer. It is also the value used for the TDS
     *        packet size (SQL Server Network Packet Size). Validity of the value is checked at connect time. If no
     *        value is set for this property, its default value is 4KB.
     */
    @Override
    public void setPacketSize(int packetSize) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.PACKET_SIZE.toString(), packetSize);
    }

    @Override
    public int getPacketSize() {
        return getIntProperty(connectionProps, SQLServerDriverIntProperty.PACKET_SIZE.toString(),
                SQLServerDriverIntProperty.PACKET_SIZE.getDefaultValue());
    }

    @Override
    public void setQueryTimeout(int queryTimeout) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.QUERY_TIMEOUT.toString(), queryTimeout);
    }

    @Override
    public int getQueryTimeout() {
        return getIntProperty(connectionProps, SQLServerDriverIntProperty.QUERY_TIMEOUT.toString(),
                SQLServerDriverIntProperty.QUERY_TIMEOUT.getDefaultValue());
    }

    @Override
    public void setCancelQueryTimeout(int cancelQueryTimeout) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.CANCEL_QUERY_TIMEOUT.toString(), cancelQueryTimeout);
    }

    @Override
    public int getCancelQueryTimeout() {
        return getIntProperty(connectionProps, SQLServerDriverIntProperty.CANCEL_QUERY_TIMEOUT.toString(),
                SQLServerDriverIntProperty.CANCEL_QUERY_TIMEOUT.getDefaultValue());
    }

    @Override
    public void setEnablePrepareOnFirstPreparedStatementCall(boolean enablePrepareOnFirstPreparedStatementCall) {
        setBooleanProperty(connectionProps,
                SQLServerDriverBooleanProperty.ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT.toString(),
                enablePrepareOnFirstPreparedStatementCall);
    }

    @Override
    public boolean getEnablePrepareOnFirstPreparedStatementCall() {
        boolean defaultValue = SQLServerDriverBooleanProperty.ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT
                .getDefaultValue();
        return getBooleanProperty(connectionProps,
                SQLServerDriverBooleanProperty.ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT.toString(), defaultValue);
    }

    @Override
    public void setServerPreparedStatementDiscardThreshold(int serverPreparedStatementDiscardThreshold) {
        setIntProperty(connectionProps,
                SQLServerDriverIntProperty.SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD.toString(),
                serverPreparedStatementDiscardThreshold);
    }

    @Override
    public int getServerPreparedStatementDiscardThreshold() {
        int defaultSize = SQLServerDriverIntProperty.SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD.getDefaultValue();
        return getIntProperty(connectionProps,
                SQLServerDriverIntProperty.SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD.toString(), defaultSize);
    }

    @Override
    public void setStatementPoolingCacheSize(int statementPoolingCacheSize) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.STATEMENT_POOLING_CACHE_SIZE.toString(),
                statementPoolingCacheSize);
    }

    @Override
    public int getStatementPoolingCacheSize() {
        int defaultSize = SQLServerDriverIntProperty.STATEMENT_POOLING_CACHE_SIZE.getDefaultValue();
        return getIntProperty(connectionProps, SQLServerDriverIntProperty.STATEMENT_POOLING_CACHE_SIZE.toString(),
                defaultSize);
    }

    @Override
    public void setDisableStatementPooling(boolean disableStatementPooling) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.DISABLE_STATEMENT_POOLING.toString(),
                disableStatementPooling);
    }

    @Override
    public boolean getDisableStatementPooling() {
        boolean defaultValue = SQLServerDriverBooleanProperty.DISABLE_STATEMENT_POOLING.getDefaultValue();
        return getBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.DISABLE_STATEMENT_POOLING.toString(),
                defaultValue);
    }

    @Override
    public void setSocketTimeout(int socketTimeout) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.SOCKET_TIMEOUT.toString(), socketTimeout);
    }

    @Override
    public int getSocketTimeout() {
        int defaultTimeOut = SQLServerDriverIntProperty.SOCKET_TIMEOUT.getDefaultValue();
        return getIntProperty(connectionProps, SQLServerDriverIntProperty.SOCKET_TIMEOUT.toString(), defaultTimeOut);
    }

    @Override
    public void setUseBulkCopyForBatchInsert(boolean useBulkCopyForBatchInsert) {
        setBooleanProperty(connectionProps, SQLServerDriverBooleanProperty.USE_BULK_COPY_FOR_BATCH_INSERT.toString(),
                useBulkCopyForBatchInsert);
    }

    @Override
    public boolean getUseBulkCopyForBatchInsert() {
        return getBooleanProperty(connectionProps,
                SQLServerDriverBooleanProperty.USE_BULK_COPY_FOR_BATCH_INSERT.toString(),
                SQLServerDriverBooleanProperty.USE_BULK_COPY_FOR_BATCH_INSERT.getDefaultValue());
    }

    @Override
    public void setJASSConfigurationName(String configurationName) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.JAAS_CONFIG_NAME.toString(),
                configurationName);
    }

    @Override
    public String getJASSConfigurationName() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.JAAS_CONFIG_NAME.toString(),
                SQLServerDriverStringProperty.JAAS_CONFIG_NAME.getDefaultValue());
    }

    @Override
    public void setMSIClientId(String msiClientId) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.MSI_CLIENT_ID.toString(), msiClientId);
    }

    @Override
    public String getMSIClientId() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.MSI_CLIENT_ID.toString(),
                SQLServerDriverStringProperty.MSI_CLIENT_ID.getDefaultValue());
    }

    @Override
    public void setKeyVaultProviderClientId(String keyVaultProviderClientId) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.KEY_VAULT_PROVIDER_CLIENT_ID.toString(),
                keyVaultProviderClientId);
    }

    @Override
    public String getKeyVaultProviderClientId() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.KEY_VAULT_PROVIDER_CLIENT_ID.toString(),
                SQLServerDriverStringProperty.KEY_VAULT_PROVIDER_CLIENT_ID.getDefaultValue());
    }

    @Override
    public void setKeyVaultProviderClientKey(String keyVaultProviderClientKey) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.KEY_VAULT_PROVIDER_CLIENT_KEY.toString(),
                keyVaultProviderClientKey);
    }

    @Override
    public void setKeyStorePrincipalId(String keyStorePrincipalId) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.KEY_STORE_PRINCIPAL_ID.toString(),
                keyStorePrincipalId);
    }

    @Override
    public String getKeyStorePrincipalId() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.KEY_STORE_PRINCIPAL_ID.toString(),
                SQLServerDriverStringProperty.KEY_STORE_PRINCIPAL_ID.getDefaultValue());
    }

    @Override
    public String getDomain() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.DOMAIN.toString(),
                SQLServerDriverStringProperty.DOMAIN.getDefaultValue());
    }

    @Override
    public void setDomain(String domain) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.DOMAIN.toString(), domain);
    }

    @Override
    public String getEnclaveAttestationUrl() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.ENCLAVE_ATTESTATION_URL.toString(),
                SQLServerDriverStringProperty.ENCLAVE_ATTESTATION_URL.getDefaultValue());
    }

    @Override
    public void setEnclaveAttestationUrl(String url) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.ENCLAVE_ATTESTATION_URL.toString(), url);
    }

    @Override
    public String getEnclaveAttestationProtocol() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.ENCLAVE_ATTESTATION_PROTOCOL.toString(),
                SQLServerDriverStringProperty.ENCLAVE_ATTESTATION_PROTOCOL.getDefaultValue());
    }

    @Override
    public void setEnclaveAttestationProtocol(String protocol) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.ENCLAVE_ATTESTATION_PROTOCOL.toString(),
                protocol);
    }

    @Override
    public String getClientCertificate() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.CLIENT_CERTIFICATE.toString(),
                SQLServerDriverStringProperty.CLIENT_CERTIFICATE.getDefaultValue());
    }

    @Override
    public void setClientCertificate(String certPath) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.CLIENT_CERTIFICATE.toString(), certPath);
    }

    @Override
    public String getClientKey() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.CLIENT_KEY.toString(),
                SQLServerDriverStringProperty.CLIENT_KEY.getDefaultValue());
    }

    @Override
    public void setClientKey(String keyPath) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.CLIENT_KEY.toString(), keyPath);
    }

    @Override
    public void setClientKeyPassword(String password) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.CLIENT_KEY_PASSWORD.toString(), password);
    }

    @Override
    public String getAADSecurePrincipalId() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.AAD_SECURE_PRINCIPAL_ID.toString(),
                SQLServerDriverStringProperty.AAD_SECURE_PRINCIPAL_ID.getDefaultValue());
    }

    @Override
    public void setAADSecurePrincipalId(String AADSecurePrincipalId) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.AAD_SECURE_PRINCIPAL_ID.toString(),
                AADSecurePrincipalId);
    }

    @Override
    public String getAADSecurePrincipalSecret() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.AAD_SECURE_PRINCIPAL_SECRET.toString(),
                SQLServerDriverStringProperty.AAD_SECURE_PRINCIPAL_SECRET.getDefaultValue());
    }

    @Override
    public void setAADSecurePrincipalSecret(String AADSecurePrincipalSecret) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.AAD_SECURE_PRINCIPAL_SECRET.toString(),
                AADSecurePrincipalSecret);
    }

    @Override
    public boolean getSendTemporalDataTypesAsStringForBulkCopy() {
        return getBooleanProperty(connectionProps,
                SQLServerDriverBooleanProperty.SEND_TEMPORAL_DATATYPES_AS_STRING_FOR_BULK_COPY.toString(),
                SQLServerDriverBooleanProperty.SEND_TEMPORAL_DATATYPES_AS_STRING_FOR_BULK_COPY.getDefaultValue());
    }

    @Override
    public void setSendTemporalDataTypesAsStringForBulkCopy(boolean sendTemporalDataTypesAsStringForBulkCopy) {
        setBooleanProperty(connectionProps,
                SQLServerDriverBooleanProperty.SEND_TEMPORAL_DATATYPES_AS_STRING_FOR_BULK_COPY.toString(),
                sendTemporalDataTypesAsStringForBulkCopy);
    }

    @Override
    public String getMaxResultBuffer() {
        return getStringProperty(connectionProps, SQLServerDriverStringProperty.MAX_RESULT_BUFFER.toString(),
                SQLServerDriverStringProperty.MAX_RESULT_BUFFER.getDefaultValue());
    }

    @Override
    public void setMaxResultBuffer(String maxResultBuffer) {
        setStringProperty(connectionProps, SQLServerDriverStringProperty.MAX_RESULT_BUFFER.toString(), maxResultBuffer);
    }

    @Override
    public void setConnectRetryCount(int count) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.CONNECT_RETRY_COUNT.toString(), count);
    }

    @Override
    public int getConnectRetryCount() {
        return getIntProperty(connectionProps, SQLServerDriverIntProperty.CONNECT_RETRY_COUNT.toString(),
                SQLServerDriverIntProperty.CONNECT_RETRY_COUNT.getDefaultValue());
    }

    @Override
    public void setConnectRetryInterval(int interval) {
        setIntProperty(connectionProps, SQLServerDriverIntProperty.CONNECT_RETRY_INTERVAL.toString(), interval);
    }

    @Override
    public int getConnectRetryInterval() {
        return getIntProperty(connectionProps, SQLServerDriverIntProperty.CONNECT_RETRY_INTERVAL.toString(),
                SQLServerDriverIntProperty.CONNECT_RETRY_INTERVAL.getDefaultValue());
    }

    /**
     * Sets a property string value.
     * 
     * @param props
     * @param propKey
     * @param propValue
     *        Caller will always supply a non-null props and propKey. Caller may supply a null propValue, in this case
     *        no property value is set.
     */
    private void setStringProperty(Properties props, String propKey, String propValue) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER) && !propKey.contains("password")
                && !propKey.contains("Password")) {
            loggerExternal.entering(getClassNameLogging(), "set" + propKey, propValue);
        } else
            loggerExternal.entering(getClassNameLogging(), "set" + propKey);
        if (null != propValue)
            props.setProperty(propKey, propValue);
        loggerExternal.exiting(getClassNameLogging(), "set" + propKey);
    }

    /**
     * Returns a property value in String format.
     * 
     * @param props
     * @param propKey
     * @param defaultValue
     * @return Caller will always supply a non-null props and propKey. Returns null if the specific property value is
     *         not set.
     */
    private String getStringProperty(Properties props, String propKey, String defaultValue) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "get" + propKey);
        String propValue = props.getProperty(propKey);
        if (null == propValue)
            propValue = defaultValue;
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER) && !propKey.contains("password")
                && !propKey.contains("Password"))
            loggerExternal.exiting(getClassNameLogging(), "get" + propKey, propValue);
        return propValue;
    }

    /**
     * Sets an integer property value.
     * 
     * @param props
     * @param propKey
     * @param propValue
     *        Caller will always supply a non-null props and propKey.
     */
    private void setIntProperty(Properties props, String propKey, int propValue) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "set" + propKey, propValue);
        props.setProperty(propKey, Integer.valueOf(propValue).toString());
        loggerExternal.exiting(getClassNameLogging(), "set" + propKey);
    }

    /**
     * Returns a property value in int format. Caller will always supply a non-null props and propKey. Returns
     * defaultValue if the specific property value is not set.
     */
    private int getIntProperty(Properties props, String propKey, int defaultValue) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "get" + propKey);
        String propValue = props.getProperty(propKey);
        int value = defaultValue;
        if (null != propValue) {
            try {
                value = Integer.parseInt(propValue);
            } catch (NumberFormatException nfe) {
                // This exception cannot occur as all of our properties
                // are set internally by int -> Integer.toString.
                assert false : "Bad portNumber:-" + propValue;
            }
        }
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.exiting(getClassNameLogging(), "get" + propKey, value);
        return value;
    }

    /**
     * Set a boolean property value. Caller will always supply a non-null props and propKey.
     */
    private void setBooleanProperty(Properties props, String propKey, boolean propValue) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "set" + propKey, propValue);
        props.setProperty(propKey, (propValue) ? "true" : "false");
        loggerExternal.exiting(getClassNameLogging(), "set" + propKey);
    }

    /**
     * Returns a property value in boolean format. Caller will always supply a non-null props and propKey. Returns
     * defaultValue if the specific property value is not set.
     */
    private boolean getBooleanProperty(Properties props, String propKey, boolean defaultValue) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "get" + propKey);
        String propValue = props.getProperty(propKey);
        boolean value;
        if (null == propValue) {
            value = defaultValue;
        } else {
            // Since we set the value of the String property ourselves to
            // "true" or "false", we can do this.
            value = Boolean.valueOf(propValue);
        }
        loggerExternal.exiting(getClassNameLogging(), "get" + propKey, value);
        return value;
    }

    private void setObjectProperty(Properties props, String propKey, Object propValue) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER)) {
            loggerExternal.entering(getClassNameLogging(), "set" + propKey);
        }
        if (null != propValue) {
            props.put(propKey, propValue);
        }
        loggerExternal.exiting(getClassNameLogging(), "set" + propKey);
    }

    private Object getObjectProperty(Properties props, String propKey, Object defaultValue) {
        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "get" + propKey);
        Object propValue = props.get(propKey);
        if (null == propValue)
            propValue = defaultValue;
        loggerExternal.exiting(getClassNameLogging(), "get" + propKey);
        return propValue;
    }

    /**
     * Returns a SQLServerConnection given username, password, and pooledConnection. Note that the DataSource properties
     * set to connectionProps are used when creating the connection. Both username and password can be null. If
     * pooledConnection is not null, then connection returned is attached to the pooledConnection and participates in
     * connection pooling.
     */
    SQLServerConnection getConnectionInternal(String username, String password,
            SQLServerPooledConnection pooledConnection) throws SQLServerException {
        Properties userSuppliedProps;
        Properties mergedProps;
        // Trust store password stripped and this object got created via
        // Objectfactory referencing.
        if (trustStorePasswordStripped)
            SQLServerException.makeFromDriverError(null, null,
                    SQLServerException.getErrString("R_referencingFailedTSP"), null, true);

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
        } else {
            userSuppliedProps = connectionProps;
        }

        // Merge in URL properties into userSuppliedProps if URL is set.
        if (null != dataSourceURL) {
            Properties urlProps = Util.parseUrl(dataSourceURL, dsLogger);
            // null returned properties means that the passed in URL is not
            // supported.
            if (null == urlProps)
                SQLServerException.makeFromDriverError(null, null,
                        SQLServerException.getErrString("R_errorConnectionString"), null, true);
            // Manually merge URL props and user supplied props.
            mergedProps = SQLServerDriver.mergeURLAndSuppliedProperties(urlProps, userSuppliedProps);
        } else {
            mergedProps = userSuppliedProps;
        }

        // Create new connection and connect.
        if (dsLogger.isLoggable(Level.FINER))
            dsLogger.finer(toString() + " Begin create new connection.");
        SQLServerConnection result = null;
        if (Util.use43Wrapper()) {
            result = new SQLServerConnection43(toString());
        } else {
            result = new SQLServerConnection(toString());
        }
        result.connect(mergedProps, pooledConnection);
        if (dsLogger.isLoggable(Level.FINER))
            dsLogger.finer(toString() + " End create new connection " + result.toString());
        return result;
    }

    // Implement javax.naming.Referenceable interface methods.

    @Override
    public Reference getReference() {
        loggerExternal.entering(getClassNameLogging(), "getReference");
        Reference ref = getReferenceInternal("com.microsoft.sqlserver.jdbc.SQLServerDataSource");
        loggerExternal.exiting(getClassNameLogging(), "getReference", ref);
        return ref;
    }

    Reference getReferenceInternal(String dataSourceClassString) {
        if (dsLogger.isLoggable(Level.FINER))
            dsLogger.finer(toString() + " creating reference for " + dataSourceClassString + ".");

        Reference ref = new Reference(this.getClass().getName(),
                "com.microsoft.sqlserver.jdbc.SQLServerDataSourceObjectFactory", null);
        if (null != dataSourceClassString)
            ref.add(new StringRefAddr("class", dataSourceClassString));

        if (trustStorePasswordStripped)
            ref.add(new StringRefAddr("trustStorePasswordStripped", "true"));

        // Add each property name+value pair found in connectionProps.
        Enumeration<?> e = connectionProps.keys();
        while (e.hasMoreElements()) {
            String propertyName = (String) e.nextElement();
            // If a trustStore password is set, it is omitted and a
            // trustStorePasswordSet flag is set.
            if (propertyName.equals(SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString())) {
                // The property set and the variable set at the same time is not
                // possible
                assert !trustStorePasswordStripped;
                ref.add(new StringRefAddr("trustStorePasswordStripped", "true"));
            } else {
                // do not add passwords to the collection. we have normal
                // password
                if (!propertyName.contains(SQLServerDriverStringProperty.PASSWORD.toString()))
                    ref.add(new StringRefAddr(propertyName, connectionProps.getProperty(propertyName)));
            }
        }

        // Add dataSourceURL and dataSourceDescription as these will not appear
        // in connectionProps.
        if (null != dataSourceURL)
            ref.add(new StringRefAddr("dataSourceURL", dataSourceURL));

        if (null != dataSourceDescription)
            ref.add(new StringRefAddr("dataSourceDescription", dataSourceDescription));

        return ref;
    }

    /**
     * Initializes the datasource from properties found inside the reference
     * 
     * @param ref
     *        Called by SQLServerDataSourceObjectFactory to initialize new DataSource instance.
     */
    void initializeFromReference(javax.naming.Reference ref) {
        // Enumerate all the StringRefAddr objects in the Reference and assign
        // properties appropriately.
        Enumeration<?> e = ref.getAll();
        while (e.hasMoreElements()) {
            StringRefAddr addr = (StringRefAddr) e.nextElement();
            String propertyName = addr.getType();
            String propertyValue = (String) addr.getContent();

            // Special case dataSourceURL and dataSourceDescription.
            if ("dataSourceURL".equals(propertyName)) {
                dataSourceURL = propertyValue;
            } else if ("dataSourceDescription".equals(propertyName)) {
                dataSourceDescription = propertyValue;
            } else if ("trustStorePasswordStripped".equals(propertyName)) {
                trustStorePasswordStripped = true;
            }
            // Just skip "class" StringRefAddr, it does not go into
            // connectionProps
            else if (!"class".equals(propertyName)) {

                connectionProps.setProperty(propertyName, propertyValue);
            }
        }
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "isWrapperFor", iface);
        boolean f = iface.isInstance(this);
        loggerExternal.exiting(getClassNameLogging(), "isWrapperFor", f);
        return f;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "unwrap", iface);
        T t;
        try {
            t = iface.cast(this);
        } catch (ClassCastException e) {
            throw new SQLServerException(e.getMessage(), e);
        }
        loggerExternal.exiting(getClassNameLogging(), "unwrap", t);
        return t;
    }

    // Returns unique id for each DataSource instance.
    private static int nextDataSourceID() {
        return baseDataSourceID.incrementAndGet();
    }

    /**
     * writeReplace
     * 
     * @return serialization proxy
     * @throws java.io.ObjectStreamException
     *         if error
     */
    private Object writeReplace() throws java.io.ObjectStreamException {
        return new SerializationProxy(this);
    }

    /**
     * For added security/robustness, the only way to rehydrate a serialized SQLServerDataSource is to use a
     * SerializationProxy. Direct use of readObject() is not supported.
     * 
     * @param stream
     *        input stream object
     * @throws java.io.InvalidObjectException
     *         if error
     */
    private void readObject(java.io.ObjectInputStream stream) throws java.io.InvalidObjectException {
        throw new java.io.InvalidObjectException("");
    }

    /**
     * This code is duplicated in pooled and XA datasource classes.
     */
    private static class SerializationProxy implements java.io.Serializable {
        private final Reference ref;
        private static final long serialVersionUID = 654661379542314226L;

        SerializationProxy(SQLServerDataSource ds) {
            // We do not need the class name so pass null, serialization
            // mechanism
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
