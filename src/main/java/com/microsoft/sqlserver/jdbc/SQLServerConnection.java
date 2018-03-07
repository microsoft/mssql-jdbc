/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_16LE;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.IDN;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLPermission;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import javax.sql.XAConnection;

import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;

import mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap.Builder;
import mssql.googlecode.concurrentlinkedhashmap.EvictionListener;

/**
 * SQLServerConnection implements a JDBC connection to SQL Server. SQLServerConnections support JDBC connection pooling and may be either physical
 * JDBC connections or logical JDBC connections.
 * <p>
 * SQLServerConnection manages transaction control for all statements that were created from it. SQLServerConnection may participate in XA distributed
 * transactions managed via an XAResource adapter.
 * <p>
 * SQLServerConnection instantiates a new TDSChannel object for use by itself and all statement objects that are created under this connection.
 * SQLServerConnection is thread safe.
 * <p>
 * SQLServerConnection manages a pool of prepared statement handles. Prepared statements are prepared once and typically executed many times with
 * different data values for their parameters. Prepared statements are also maintained across logical (pooled) connection closes.
 * <p>
 * SQLServerConnection is not thread safe, however multiple statements created from a single connection can be processing simultaneously in concurrent
 * threads.
 * <p>
 * This class's public functions need to be kept identical to the SQLServerConnectionPoolProxy's.
 * <p>
 * The API javadoc for JDBC API methods that this class implements are not repeated here. Please see Sun's JDBC API interfaces javadoc for those
 * details.
 */

// Note all the public functions in this class also need to be defined in SQLServerConnectionPoolProxy.
public class SQLServerConnection implements ISQLServerConnection {

    long timerExpire;
    boolean attemptRefreshTokenLocked = false;

    // Threasholds related to when prepared statement handles are cleaned-up. 1 == immediately.
    /**
     * The default for the prepared statement clean-up action threshold (i.e. when sp_unprepare is called). 
     */    
    static final int DEFAULT_SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD = 10; // Used to set the initial default, can be changed later.
    private int serverPreparedStatementDiscardThreshold = -1; // Current limit for this particular connection.

    /**
     * The default for if prepared statements should execute sp_executesql before following the prepare, unprepare pattern. 
     */    
    static final boolean DEFAULT_ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT_CALL = false; // Used to set the initial default, can be changed later. false == use sp_executesql -> sp_prepexec -> sp_execute -> batched -> sp_unprepare pattern, true == skip sp_executesql part of pattern.
    private Boolean enablePrepareOnFirstPreparedStatementCall = null; // Current limit for this particular connection.

    // Handle the actual queue of discarded prepared statements.
    private ConcurrentLinkedQueue<PreparedStatementHandle> discardedPreparedStatementHandles = new ConcurrentLinkedQueue<>();
    private AtomicInteger discardedPreparedStatementHandleCount = new AtomicInteger(0);

    private boolean fedAuthRequiredByUser = false;
    private boolean fedAuthRequiredPreLoginResponse = false;
    private boolean federatedAuthenticationAcknowledged = false;
    private boolean federatedAuthenticationRequested = false;
    private boolean federatedAuthenticationInfoRequested = false; // Keep this distinct from _federatedAuthenticationRequested, since some fedauth
                                                                  // library types may not need more info

    private FederatedAuthenticationFeatureExtensionData fedAuthFeatureExtensionData = null;
    private String authenticationString = null;
    private byte[] accessTokenInByte = null;

    private SqlFedAuthToken fedAuthToken = null;

    static class Sha1HashKey {
        private byte[] bytes;

        Sha1HashKey(String sql,
                String parametersDefinition,
                String dbName) {
            this(String.format("%s%s%s", sql, parametersDefinition, dbName));
        }

        Sha1HashKey(String s) {
        	bytes = getSha1Digest().digest(s.getBytes());
        }

        public boolean equals(Object obj) {
            if (!(obj instanceof Sha1HashKey))
                return false;

            return java.util.Arrays.equals(bytes, ((Sha1HashKey)obj).bytes);
        }

        public int hashCode() {
            return java.util.Arrays.hashCode(bytes);
        }

        private java.security.MessageDigest getSha1Digest() {
            try {
                return java.security.MessageDigest.getInstance("SHA-1");
            }
            catch (final java.security.NoSuchAlgorithmException e) {
                // This is not theoretically possible, but we're forced to catch it anyway
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Used to keep track of an individual prepared statement handle.
     */
    class PreparedStatementHandle  {
        private int handle = 0;
        private final AtomicInteger handleRefCount = new AtomicInteger();
        private boolean isDirectSql;
        private volatile boolean evictedFromCache; 
        private volatile boolean explicitlyDiscarded; 
        private Sha1HashKey key;

        PreparedStatementHandle(Sha1HashKey key, int handle, boolean isDirectSql, boolean isEvictedFromCache) {
        	this.key = key;
            this.handle = handle;
            this.isDirectSql = isDirectSql;
            this.setIsEvictedFromCache(isEvictedFromCache);
            handleRefCount.set(1);
        }

        /** Has the statement been evicted from the statement handle cache. */
        private boolean isEvictedFromCache() {
            return evictedFromCache;
        }

        /** Specify whether the statement been evicted from the statement handle cache. */
        private void setIsEvictedFromCache(boolean isEvictedFromCache) {
            this.evictedFromCache = isEvictedFromCache;
        }

        /** Specify that this statement has been explicitly discarded from being used by the cache. */
        void setIsExplicitlyDiscarded() {
        	this.explicitlyDiscarded = true;
        	
    		evictCachedPreparedStatementHandle(this);
        }

        /** Has the statement been explicitly discarded. */
        private boolean isExplicitlyDiscarded() {
            return explicitlyDiscarded;
        }

        /** Get the actual handle. */
        int getHandle() {
            return handle;
        }

        /** Get the cache key. */
        Sha1HashKey getKey() {
            return key;
        }

        boolean isDirectSql() {
            return isDirectSql;
        }

        /** Make sure handle cannot be re-used. 
         * 
         * @return 
         *      false: Handle could not be discarded, it is in use.
         *      true: Handle was successfully put on path for discarding.
        */
        private boolean tryDiscardHandle() {
            return handleRefCount.compareAndSet(0, -999);
        }

        /** Returns whether this statement has been discarded and can no longer be re-used. */
        private boolean isDiscarded() {
            return 0 > handleRefCount.intValue();
        }

        /** Adds a new reference to this handle, i.e. re-using it. 
         * 
         * @return 
         *      false: Reference could not be added, statement has been discarded or does not have a handle associated with it.
         *      true: Reference was successfully added.
        */
        boolean tryAddReference() {
            if (isDiscarded() || isExplicitlyDiscarded())
                return false;
            else {
                int refCount = handleRefCount.incrementAndGet();
                return refCount > 0;
            }
        }

        /** Remove a reference from this handle*/ 
        void removeReference() {
            handleRefCount.decrementAndGet();
        }
    }

    /** Size of the parsed SQL-text metadata cache */
    static final private int PARSED_SQL_CACHE_SIZE = 100;

    /** Cache of parsed SQL meta data */
    static private ConcurrentLinkedHashMap<Sha1HashKey, ParsedSQLCacheItem> parsedSQLCache;

    static {
        parsedSQLCache = new Builder<Sha1HashKey, ParsedSQLCacheItem>()
	        .maximumWeightedCapacity(PARSED_SQL_CACHE_SIZE)
            .build();
    }

    /** Get prepared statement cache entry if exists, if not parse and create a new one */
    static ParsedSQLCacheItem  getCachedParsedSQL(Sha1HashKey key) {
        return parsedSQLCache.get(key);
    }

    /** Parse and create a information about parsed SQL text */
    static ParsedSQLCacheItem  parseAndCacheSQL(Sha1HashKey key, String sql) throws SQLServerException {
        JDBCSyntaxTranslator translator = new JDBCSyntaxTranslator();

        String parsedSql = translator.translate(sql);
        String procName = translator.getProcedureName(); // may return null        
        boolean returnValueSyntax = translator.hasReturnValueSyntax();
        int paramCount = countParams(parsedSql);

        ParsedSQLCacheItem  cacheItem = new ParsedSQLCacheItem (parsedSql, paramCount, procName, returnValueSyntax);
        parsedSQLCache.putIfAbsent(key, cacheItem);
        return cacheItem;
    }
 
    /** Default size for prepared statement caches */
    static final int DEFAULT_STATEMENT_POOLING_CACHE_SIZE = 0; 
    
    /** Size of the  prepared statement handle cache */
    private int statementPoolingCacheSize = DEFAULT_STATEMENT_POOLING_CACHE_SIZE;

    /** Cache of prepared statement handles */
    private ConcurrentLinkedHashMap<Sha1HashKey, PreparedStatementHandle> preparedStatementHandleCache;
    /** Cache of prepared statement parameter metadata */
    private ConcurrentLinkedHashMap<Sha1HashKey, SQLServerParameterMetaData> parameterMetadataCache;
    /**
     * Checks whether statement pooling is enabled or disabled. The default is set to true;
     */
    private boolean disableStatementPooling = true;

     /**
      * Find statement parameters.
      * 
      * @param sql
      *          SQL text to parse for number of parameters to intialize.
      */
     private static int countParams(String sql) {
         int nParams = 0;
 
         // Figure out the expected number of parameters by counting the
         // parameter placeholders in the SQL string.
         int offset = -1;
         while ((offset = ParameterUtils.scanSQLForChar('?', sql, ++offset)) < sql.length())
             ++nParams;
 
         return nParams;
     }

    SqlFedAuthToken getAuthenticationResult() {
        return fedAuthToken;
    }

    /**
     * Struct encapsulating the data to be sent to the server as part of Federated Authentication Feature Extension.
     */
    class FederatedAuthenticationFeatureExtensionData {
        boolean fedAuthRequiredPreLoginResponse;
        int libraryType = -1;
        byte[] accessToken = null;
        SqlAuthentication authentication = null;

        FederatedAuthenticationFeatureExtensionData(int libraryType,
                String authenticationString,
                boolean fedAuthRequiredPreLoginResponse) throws SQLServerException {
            this.libraryType = libraryType;
            this.fedAuthRequiredPreLoginResponse = fedAuthRequiredPreLoginResponse;

            switch (authenticationString.toUpperCase(Locale.ENGLISH).trim()) {
                case "ACTIVEDIRECTORYPASSWORD":
                    this.authentication = SqlAuthentication.ActiveDirectoryPassword;
                    break;
                case "ACTIVEDIRECTORYINTEGRATED":
                    this.authentication = SqlAuthentication.ActiveDirectoryIntegrated;
                    break;
                default:
                    assert (false);
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidConnectionSetting"));
                    Object[] msgArgs = {"authentication", authenticationString};
                    throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
            }
        }

        FederatedAuthenticationFeatureExtensionData(int libraryType,
                boolean fedAuthRequiredPreLoginResponse,
                byte[] accessToken) {
            this.libraryType = libraryType;
            this.fedAuthRequiredPreLoginResponse = fedAuthRequiredPreLoginResponse;
            this.accessToken = accessToken;
        }
    }

    class SqlFedAuthInfo {
        String spn;
        String stsurl;

        @Override
        public String toString() {
            return "STSURL: " + stsurl + ", SPN: " + spn;
        }
    }

    

    class ActiveDirectoryAuthentication {
        static final String JDBC_FEDAUTH_CLIENT_ID = "7f98cb04-cd1e-40df-9140-3bf7e2cea4db";
        static final String ADAL_GET_ACCESS_TOKEN_FUNCTION_NAME = "ADALGetAccessToken";
        static final int GET_ACCESS_TOKEN_SUCCESS = 0;
        static final int GET_ACCESS_TOKEN_INVALID_GRANT = 1;
        static final int GET_ACCESS_TOKEN_TANSISENT_ERROR = 2;
        static final int GET_ACCESS_TOKEN_OTHER_ERROR = 3;
    }

    /**
     * denotes the state of the SqlServerConnection
     */
    private enum State {
        Initialized,// default value on calling SQLServerConnection constructor
        Connected,  // indicates that the TCP connection has completed
        Opened,     // indicates that the prelogin, login have completed, the database session established and the connection is ready for use.
        Closed      // indicates that the connection has been closed.
    }

    private final static float TIMEOUTSTEP = 0.08F;    // fraction of timeout to use for fast failover connections
    private final static float TIMEOUTSTEP_TNIR = 0.125F;
    final static int TnirFirstAttemptTimeoutMs = 500;    // fraction of timeout to use for fast failover connections

    /*
     * Connection state variables. NB If new state is added then logical connections derived from a physical connection must inherit the same state.
     * If state variables are added they must be added also in connection cloning method clone()
     */

    private final static int INTERMITTENT_TLS_MAX_RETRY = 5;

    // Indicates if we received a routing ENVCHANGE in the current connection attempt
    private boolean isRoutedInCurrentAttempt = false;

    // Contains the routing info received from routing ENVCHANGE
    private ServerPortPlaceHolder routingInfo = null;

    ServerPortPlaceHolder getRoutingInfo() {
        return routingInfo;
    }

    // Permission targets
    private static final String callAbortPerm = "callAbort";
    
    private static final String SET_NETWORK_TIMEOUT_PERM = "setNetworkTimeout";

    private boolean sendStringParametersAsUnicode = SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.getDefaultValue();        // see
                                                                                                                                               // connection
                                                                                                                                               // properties
                                                                                                                                               // doc
                                                                                                                                               // (default
                                                                                                                                               // is
                                                                                                                                               // false).

    private String hostName = null;
    
    boolean sendStringParametersAsUnicode() {
        return sendStringParametersAsUnicode;
    }

    private boolean lastUpdateCount;              // see connection properties doc

    final boolean useLastUpdateCount() {
        return lastUpdateCount;
    }

    // Translates the serverName from Unicode to ASCII Compatible Encoding (ACE), as defined by the ToASCII operation of RFC 3490
    private boolean serverNameAsACE = SQLServerDriverBooleanProperty.SERVER_NAME_AS_ACE.getDefaultValue();

    boolean serverNameAsACE() {
        return serverNameAsACE;
    }

    // see feature_connection_director_multi_subnet_JDBC.docx
    private boolean multiSubnetFailover;

    final boolean getMultiSubnetFailover() {
        return multiSubnetFailover;
    }

    private boolean transparentNetworkIPResolution;

    final boolean getTransparentNetworkIPResolution() {
        return transparentNetworkIPResolution;
    }

    private ApplicationIntent applicationIntent = null;

    final ApplicationIntent getApplicationIntent() {
        return applicationIntent;
    }

    private int nLockTimeout;                     // see connection properties doc
    private String selectMethod;         // see connection properties doc 4.0 new property

    final String getSelectMethod() {
        return selectMethod;
    }

    private String responseBuffering;

    final String getResponseBuffering() {
        return responseBuffering;
    }

    private int queryTimeoutSeconds;

    final int getQueryTimeoutSeconds() {
        return queryTimeoutSeconds;
    }

    private int socketTimeoutMilliseconds;

    final int getSocketTimeoutMilliseconds() {
        return socketTimeoutMilliseconds;
    }
    
    boolean userSetTNIR = true;

    private boolean sendTimeAsDatetime = SQLServerDriverBooleanProperty.SEND_TIME_AS_DATETIME.getDefaultValue();

    /**
     * Checks the sendTimeAsDatetime property.
     * 
     * @return boolean value of sendTimeAsDatetime
     */
    public synchronized final boolean getSendTimeAsDatetime() {
        return !isKatmaiOrLater() || sendTimeAsDatetime;
    }

    final int baseYear() {
        return getSendTimeAsDatetime() ? TDS.BASE_YEAR_1970 : TDS.BASE_YEAR_1900;
    }

    private byte requestedEncryptionLevel = TDS.ENCRYPT_INVALID;

    final byte getRequestedEncryptionLevel() {
        assert TDS.ENCRYPT_INVALID != requestedEncryptionLevel;
        return requestedEncryptionLevel;
    }

    private boolean trustServerCertificate;

    final boolean trustServerCertificate() {
        return trustServerCertificate;
    }

    private byte negotiatedEncryptionLevel = TDS.ENCRYPT_INVALID;

    final byte getNegotiatedEncryptionLevel() {
        assert TDS.ENCRYPT_INVALID != negotiatedEncryptionLevel;
        return negotiatedEncryptionLevel;
    }

    private String trustManagerClass = null;

    final String getTrustManagerClass() {
        assert TDS.ENCRYPT_INVALID != requestedEncryptionLevel;
        return trustManagerClass;
    }

    private String trustManagerConstructorArg = null;

    final String getTrustManagerConstructorArg() {
        assert TDS.ENCRYPT_INVALID != requestedEncryptionLevel;
        return trustManagerConstructorArg;
    }

    static final String RESERVED_PROVIDER_NAME_PREFIX = "MSSQL_";
    String columnEncryptionSetting = null;

    boolean isColumnEncryptionSettingEnabled() {
        return (columnEncryptionSetting.equalsIgnoreCase(ColumnEncryptionSetting.Enabled.toString()));
    }

    String keyStoreAuthentication = null;
    String keyStoreSecret = null;
    String keyStoreLocation = null;

    private boolean serverSupportsColumnEncryption = false;

    boolean getServerSupportsColumnEncryption() {
        return serverSupportsColumnEncryption;
    }

    static boolean isWindows;
    static Map<String, SQLServerColumnEncryptionKeyStoreProvider> globalSystemColumnEncryptionKeyStoreProviders = new HashMap<>();
    static {
        if (System.getProperty("os.name").toLowerCase(Locale.ENGLISH).startsWith("windows")) {
            isWindows = true;
            SQLServerColumnEncryptionCertificateStoreProvider provider = new SQLServerColumnEncryptionCertificateStoreProvider();
            globalSystemColumnEncryptionKeyStoreProviders.put(provider.getName(), provider);
        }
        else {
            isWindows = false;
        }
    }
    static Map<String, SQLServerColumnEncryptionKeyStoreProvider> globalCustomColumnEncryptionKeyStoreProviders = null;
    // This is a per-connection store provider. It can be JKS or AKV.
    Map<String, SQLServerColumnEncryptionKeyStoreProvider> systemColumnEncryptionKeyStoreProvider = new HashMap<>();

    /**
     * Registers key store providers in the globalCustomColumnEncryptionKeyStoreProviders.
     * 
     * @param clientKeyStoreProviders
     *            a map containing the store providers information.
     * @throws SQLServerException
     *             when an error occurs
     */
    public static synchronized void registerColumnEncryptionKeyStoreProviders(
            Map<String, SQLServerColumnEncryptionKeyStoreProvider> clientKeyStoreProviders) throws SQLServerException {
        loggerExternal.entering(SQLServerConnection.class.getName(), "registerColumnEncryptionKeyStoreProviders",
                "Registering Column Encryption Key Store Providers");

        if (null == clientKeyStoreProviders) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_CustomKeyStoreProviderMapNull"), null, 0, false);
        }

        if (null != globalCustomColumnEncryptionKeyStoreProviders) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_CustomKeyStoreProviderSetOnce"), null, 0, false);
        }

        globalCustomColumnEncryptionKeyStoreProviders = new HashMap<>();

        for (Map.Entry<String, SQLServerColumnEncryptionKeyStoreProvider> entry : clientKeyStoreProviders.entrySet()) {
            String providerName = entry.getKey();
            if (null == providerName || 0 == providerName.length()) {
                throw new SQLServerException(null, SQLServerException.getErrString("R_EmptyCustomKeyStoreProviderName"), null, 0, false);
            }
            if ((providerName.substring(0, 6).equalsIgnoreCase(RESERVED_PROVIDER_NAME_PREFIX))) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidCustomKeyStoreProviderName"));
                Object[] msgArgs = {providerName, RESERVED_PROVIDER_NAME_PREFIX};
                throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
            }
            if (null == entry.getValue()) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_CustomKeyStoreProviderValueNull"));
                Object[] msgArgs = {providerName, RESERVED_PROVIDER_NAME_PREFIX};
                throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
            }
            globalCustomColumnEncryptionKeyStoreProviders.put(entry.getKey(), entry.getValue());
        }

        loggerExternal.exiting(SQLServerConnection.class.getName(), "registerColumnEncryptionKeyStoreProviders",
                "Number of Key store providers that are registered:" + globalCustomColumnEncryptionKeyStoreProviders.size());
    }

    static synchronized SQLServerColumnEncryptionKeyStoreProvider getGlobalSystemColumnEncryptionKeyStoreProvider(String providerName) {
        if (null != globalSystemColumnEncryptionKeyStoreProviders && globalSystemColumnEncryptionKeyStoreProviders.containsKey(providerName)) {
            return globalSystemColumnEncryptionKeyStoreProviders.get(providerName);
        }
        return null;
    }

    static synchronized String getAllGlobalCustomSystemColumnEncryptionKeyStoreProviders() {
        if (null != globalCustomColumnEncryptionKeyStoreProviders)
            return globalCustomColumnEncryptionKeyStoreProviders.keySet().toString();
        else
            return null;
    }

    synchronized String getAllSystemColumnEncryptionKeyStoreProviders() {
        String keyStores = "";
        if (0 != systemColumnEncryptionKeyStoreProvider.size())
            keyStores = systemColumnEncryptionKeyStoreProvider.keySet().toString();
        if (0 != SQLServerConnection.globalSystemColumnEncryptionKeyStoreProviders.size())
            keyStores += "," + SQLServerConnection.globalSystemColumnEncryptionKeyStoreProviders.keySet().toString();
        return keyStores;
    }

    static synchronized SQLServerColumnEncryptionKeyStoreProvider getGlobalCustomColumnEncryptionKeyStoreProvider(String providerName) {
        if (null != globalCustomColumnEncryptionKeyStoreProviders && globalCustomColumnEncryptionKeyStoreProviders.containsKey(providerName)) {
            return globalCustomColumnEncryptionKeyStoreProviders.get(providerName);
        }
        return null;
    }

    synchronized SQLServerColumnEncryptionKeyStoreProvider getSystemColumnEncryptionKeyStoreProvider(String providerName) {
        if ((null != systemColumnEncryptionKeyStoreProvider) && (systemColumnEncryptionKeyStoreProvider.containsKey(providerName))) {
            return systemColumnEncryptionKeyStoreProvider.get(providerName);
        }
        else {
            return null;
        }
    }

    private String trustedServerNameAE = null;
    private static Map<String, List<String>> columnEncryptionTrustedMasterKeyPaths = new HashMap<>();

    /**
     * Sets Trusted Master Key Paths in the columnEncryptionTrustedMasterKeyPaths.
     * 
     * @param trustedKeyPaths
     *            all master key paths that are trusted
     */
    public static synchronized void setColumnEncryptionTrustedMasterKeyPaths(Map<String, List<String>> trustedKeyPaths) {
        loggerExternal.entering(SQLServerConnection.class.getName(), "setColumnEncryptionTrustedMasterKeyPaths", "Setting Trusted Master Key Paths");

        // Use upper case for server and instance names.
        columnEncryptionTrustedMasterKeyPaths.clear();
        for (Map.Entry<String, List<String>> entry : trustedKeyPaths.entrySet()) {
            columnEncryptionTrustedMasterKeyPaths.put(entry.getKey().toUpperCase(), entry.getValue());
        }

        loggerExternal.exiting(SQLServerConnection.class.getName(), "setColumnEncryptionTrustedMasterKeyPaths",
                "Number of Trusted Master Key Paths: " + columnEncryptionTrustedMasterKeyPaths.size());
    }

    /**
     * Updates the columnEncryptionTrustedMasterKeyPaths with the new Server and trustedKeyPaths.
     * 
     * @param server
     *            String server name
     * @param trustedKeyPaths
     *            all master key paths that are trusted
     */
    public static synchronized void updateColumnEncryptionTrustedMasterKeyPaths(String server,
            List<String> trustedKeyPaths) {
        loggerExternal.entering(SQLServerConnection.class.getName(), "updateColumnEncryptionTrustedMasterKeyPaths",
                "Updating Trusted Master Key Paths");

        // Use upper case for server and instance names.
        columnEncryptionTrustedMasterKeyPaths.put(server.toUpperCase(), trustedKeyPaths);

        loggerExternal.exiting(SQLServerConnection.class.getName(), "updateColumnEncryptionTrustedMasterKeyPaths",
                "Number of Trusted Master Key Paths: " + columnEncryptionTrustedMasterKeyPaths.size());
    }

    /**
     * Removes the trusted Master key Path from the columnEncryptionTrustedMasterKeyPaths.
     * 
     * @param server
     *            String server name
     */
    public static synchronized void removeColumnEncryptionTrustedMasterKeyPaths(String server) {
        loggerExternal.entering(SQLServerConnection.class.getName(), "removeColumnEncryptionTrustedMasterKeyPaths",
                "Removing Trusted Master Key Paths");

        // Use upper case for server and instance names.
        columnEncryptionTrustedMasterKeyPaths.remove(server.toUpperCase());

        loggerExternal.exiting(SQLServerConnection.class.getName(), "removeColumnEncryptionTrustedMasterKeyPaths",
                "Number of Trusted Master Key Paths: " + columnEncryptionTrustedMasterKeyPaths.size());
    }

    /**
     * Retrieves the Trusted Master Key Paths.
     * 
     * @return columnEncryptionTrustedMasterKeyPaths.
     */
    public static synchronized Map<String, List<String>> getColumnEncryptionTrustedMasterKeyPaths() {
        loggerExternal.entering(SQLServerConnection.class.getName(), "getColumnEncryptionTrustedMasterKeyPaths", "Getting Trusted Master Key Paths");

        Map<String, List<String>> masterKeyPathCopy = new HashMap<>();

        for (Map.Entry<String, List<String>> entry : columnEncryptionTrustedMasterKeyPaths.entrySet()) {
            masterKeyPathCopy.put(entry.getKey(), entry.getValue());
        }

        loggerExternal.exiting(SQLServerConnection.class.getName(), "getColumnEncryptionTrustedMasterKeyPaths",
                "Number of Trusted Master Key Paths: " + masterKeyPathCopy.size());

        return masterKeyPathCopy;
    }

    static synchronized List<String> getColumnEncryptionTrustedMasterKeyPaths(String server,
            Boolean[] hasEntry) {
        if (columnEncryptionTrustedMasterKeyPaths.containsKey(server)) {
            hasEntry[0] = true;
            return columnEncryptionTrustedMasterKeyPaths.get(server);
        }
        else {
            hasEntry[0] = false;
            return null;
        }
    }

    Properties activeConnectionProperties; // the active set of connection properties
    private boolean integratedSecurity = SQLServerDriverBooleanProperty.INTEGRATED_SECURITY.getDefaultValue();
    private AuthenticationScheme intAuthScheme = AuthenticationScheme.nativeAuthentication;
    private GSSCredential ImpersonatedUserCred ;
    // This is the current connect place holder this should point one of the primary or failover place holder
    ServerPortPlaceHolder currentConnectPlaceHolder = null;

    String sqlServerVersion;              // SQL Server version string
    boolean xopenStates;                  // XOPEN or SQL 92 state codes?
    private boolean databaseAutoCommitMode;
    private boolean inXATransaction = false; // Set to true when in an XA transaction.
    private byte[] transactionDescriptor = new byte[8];

    // Flag (Yukon and later) set to true whenever a transaction is rolled back.
    // The flag's value is reset to false when a new transaction starts or when the autoCommit mode changes.
    private boolean rolledBackTransaction;

    final boolean rolledBackTransaction() {
        return rolledBackTransaction;
    }

    private State state = State.Initialized;                      // connection state

    private void setState(State state) {
        this.state = state;
    }

    // This function actually represents whether a database session is not open.
    // The session is not available before the session is established and
    // after the session is closed.
    final boolean isSessionUnAvailable() {
        return !(state.equals(State.Opened));
    }

    final static int maxDecimalPrecision = 38;         // @@max_precision for SQL 2000 and 2005 is 38.
    final static int defaultDecimalPrecision = 18;
    final String traceID;

    /** Limit for the size of data (in bytes) returned for value on this connection */
    private int maxFieldSize; // default: 0 --> no limit

    final void setMaxFieldSize(int limit) throws SQLServerException {
        // assert limit >= 0;
        if (maxFieldSize != limit) {
            if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
                loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
            }
            // If no limit on field size, set text size to max (2147483647), NOT default (0 --> 4K)
            connectionCommand("SET TEXTSIZE " + ((0 == limit) ? 2147483647 : limit), "setMaxFieldSize");
            maxFieldSize = limit;
        }
    }

    // This function is used both to init the values on creation of connection
    // and resetting the values after the connection is released to the pool for reuse.
    final void initResettableValues() {
        rolledBackTransaction = false;
        transactionIsolationLevel = Connection.TRANSACTION_READ_COMMITTED;// default isolation level
        maxFieldSize = 0;  // default: 0 --> no limit
        maxRows = 0; // default: 0 --> no limit
        nLockTimeout = -1;
        databaseAutoCommitMode = true;// auto commit mode
        holdability = ResultSet.HOLD_CURSORS_OVER_COMMIT;
        sqlWarnings = null;
        sCatalog = originalCatalog;
        databaseMetaData = null;
    }

    /** Limit for the maximum number of rows returned from queries on this connection */
    private int maxRows; // default: 0 --> no limit

    final void setMaxRows(int limit) throws SQLServerException {
        // assert limit >= 0;
        if (maxRows != limit) {
            if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
                loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
            }
            connectionCommand("SET ROWCOUNT " + limit, "setMaxRows");
            maxRows = limit;
        }
    }

    private SQLCollation databaseCollation;	// Default database collation read from ENVCHANGE_SQLCOLLATION token.

    final SQLCollation getDatabaseCollation() {
        return databaseCollation;
    }

    static private final AtomicInteger baseConnectionID = new AtomicInteger(0);       // connection id dispenser
    // This is the current catalog
    private String sCatalog = "master";                     // the database catalog
    // This is the catalog immediately after login.
    private String originalCatalog = "master";

    private int transactionIsolationLevel;
    private SQLServerPooledConnection pooledConnectionParent;
    private DatabaseMetaData databaseMetaData; // the meta data for this connection
    private int nNextSavePointId = 10000;      // first save point id

    static final private java.util.logging.Logger connectionlogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerConnection");
    static final private java.util.logging.Logger loggerExternal = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.Connection");
    private final String loggingClassName;

    // there are three ways to get a failover partner
    // connection string, from the failover map, the connecting server returned
    // the following variable only stores the serverReturned failver information.
    private String failoverPartnerServerProvided = null;

    private int holdability;

    final int getHoldabilityInternal() {
        return holdability;
    }

    // Default TDS packet size used after logon if no other value was set via
    // the packetSize connection property. The value was chosen to take maximum
    // advantage of SQL Server's default page size.
    private int tdsPacketSize = TDS.INITIAL_PACKET_SIZE;
    private int requestedPacketSize = TDS.DEFAULT_PACKET_SIZE;

    final int getTDSPacketSize() {
        return tdsPacketSize;
    }

    private TDSChannel tdsChannel;

    private TDSCommand currentCommand = null;

    private int tdsVersion = TDS.VER_UNKNOWN;

    final boolean isKatmaiOrLater() {
        assert TDS.VER_UNKNOWN != tdsVersion;
        assert tdsVersion >= TDS.VER_YUKON;
        return tdsVersion >= TDS.VER_KATMAI;
    }

    final boolean isDenaliOrLater() {
        return tdsVersion >= TDS.VER_DENALI;
    }

    private int serverMajorVersion;

    int getServerMajorVersion() {
        return serverMajorVersion;
    }

    private SQLServerConnectionPoolProxy proxy;

    private UUID clientConnectionId = null;

    /**
     * Retrieves the clientConnectionID.
     * 
     * @throws SQLServerException
     *             when an error occurs
     */
    public UUID getClientConnectionId() throws SQLServerException {
        // If the connection is closed, we do not allow external application to get
        // ClientConnectionId.
        checkClosed();
        return clientConnectionId;
    }

    // This function is called internally, e.g. when login process fails, we
    // need to append the ClientConnectionId to error string.
    final UUID getClientConIdInternal() {
        return clientConnectionId;
    }

    final boolean attachConnId() {
        return state.equals(State.Connected);
    }

    @SuppressWarnings("unused")
    SQLServerConnection(String parentInfo) throws SQLServerException {
        int connectionID = nextConnectionID();                   // sequential connection id
        traceID = "ConnectionID:" + connectionID;
        loggingClassName = "com.microsoft.sqlserver.jdbc.SQLServerConnection:" + connectionID;
        if (connectionlogger.isLoggable(Level.FINE))
            connectionlogger.fine(toString() + " created by (" + parentInfo + ")");
        initResettableValues();

        // JDBC 3 driver only works with 1.5 JRE
        if (3 == DriverJDBCVersion.major && !"1.5".equals(Util.SYSTEM_SPEC_VERSION)) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unsupportedJREVersion"));
            Object[] msgArgs = {Util.SYSTEM_SPEC_VERSION};
            String message = form.format(msgArgs);
            connectionlogger.severe(message);
            throw new UnsupportedOperationException(message);
        }
        
        // Caching turned on?
        if (!this.getDisableStatementPooling() && 0 < this.getStatementPoolingCacheSize()) {
            prepareCache();
        }
    }

    void setFailoverPartnerServerProvided(String partner) {
        failoverPartnerServerProvided = partner;
        // after login this info should be added to the map
    }

    final void setAssociatedProxy(SQLServerConnectionPoolProxy proxy) {
        this.proxy = proxy;
    }

    /*
     * This function is used by the functions that return a connection object to outside world. E.g. stmt.getConnection, these functions should return
     * the proxy not the actual physical connection when the physical connection is pooled and the user should be accessing the connection functions
     * via the proxy object.
     */
    final Connection getConnection() {
        if (null != proxy)
            return proxy;
        else
            return this;
    }

    final void resetPooledConnection() {
        tdsChannel.resetPooledConnection();
        initResettableValues();
    }

    /**
     * Generate the next unique connection id.
     * 
     * @return the next conn id
     */
    /* L0 */ private static int nextConnectionID() {
        return baseConnectionID.incrementAndGet(); // 4.04 Ensure thread safe id allocation
    }

    java.util.logging.Logger getConnectionLogger() {
        return connectionlogger;
    }

    String getClassNameLogging() {
        return loggingClassName;
    }

    /**
     * This is a helper function to provide an ID string suitable for tracing.
     */
    public String toString() {
        if (null != clientConnectionId)
            return traceID + " ClientConnectionId: " + clientConnectionId.toString();
        else
            return traceID;
    }

    /**
     * Throw a not implemeneted exception.
     * 
     * @throws SQLServerException
     */
    /* L0 */ void NotImplemented() throws SQLServerException {
        SQLServerException.makeFromDriverError(this, this, SQLServerException.getErrString("R_notSupported"), null, false);
    }

    /**
     * Check if the connection is closed Create a new connection if it's a fedauth connection and the access token is going to expire.
     * 
     * @throws SQLServerException
     */
    /* L0 */ void checkClosed() throws SQLServerException {
        if (isSessionUnAvailable()) {
            SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_connectionIsClosed"), null, false);
        }

        if (null != fedAuthToken) {
            if (Util.checkIfNeedNewAccessToken(this)) {
                connect(this.activeConnectionProperties, null);
            }
        }
    }

    /**
     * Check if a string property is enabled.
     * 
     * @param propName
     *            the string property name
     * @param propValue
     *            the string property value.
     * @return false if p == null (meaning take default).
     * @return true if p == "true" (case-insensitive).
     * @return false if p == "false" (case-insensitive).
     * @exception SQLServerException
     *                thrown if value is not recognized.
     */
    /* L0 */ private boolean booleanPropertyOn(String propName,
            String propValue) throws SQLServerException {
        // Null means take the default of false.
        if (null == propValue)
            return false;
        String lcpropValue = propValue.toLowerCase(Locale.US);

        if ("true".equals(lcpropValue))
            return true;
        if ("false".equals(lcpropValue))
            return false;
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidBooleanValue"));
        Object[] msgArgs = {propName};
        SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
        // Adding return false here for compiler's sake, this code is unreachable.
        return false;
    }

    // Maximum number of wide characters for a SQL login record name (such as instance name, application name, etc...).
    // See TDS specification, "Login Data Validation Rules" section.
    final static int MAX_SQL_LOGIN_NAME_WCHARS = 128;

    /**
     * Validates propName against maximum allowed length MAX_SQL_LOGIN_NAME_WCHARS. Throws exception if name length exceeded.
     * 
     * @param propName
     *            the name of the property.
     * @param propValue
     *            the value of the property.
     * @throws SQLServerException
     */
    void ValidateMaxSQLLoginName(String propName,
            String propValue) throws SQLServerException {
        if (propValue != null && propValue.length() > MAX_SQL_LOGIN_NAME_WCHARS) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_propertyMaximumExceedsChars"));
            Object[] msgArgs = {propName, Integer.toString(MAX_SQL_LOGIN_NAME_WCHARS)};
            SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
        }
    }

    Connection connect(Properties propsIn,
            SQLServerPooledConnection pooledConnection) throws SQLServerException {
        int loginTimeoutSeconds = 0; // Will be set during the first retry attempt.
        long start = System.currentTimeMillis();

        for (int retryAttempt = 0;;) {
            try {
                return connectInternal(propsIn, pooledConnection);
            }
            catch (SQLServerException e) {
                // Catch only the TLS 1.2 specific intermittent error.
                if (SQLServerException.DRIVER_ERROR_INTERMITTENT_TLS_FAILED != e.getDriverErrorCode()) {
                    // Re-throw all other exceptions.
                    throw e;
                }
                else {
                    // Special handling of the retry logic for TLS 1.2 intermittent issue.

                    // If timeout is not set yet, set it once.
                    if (0 == retryAttempt) {
                        // We do not need to check for exceptions here, as the connection properties are already
                        // verified during the first try. Also, we would like to do this calculation
                        // only for the TLS 1.2 exception case.
                        loginTimeoutSeconds = SQLServerDriverIntProperty.LOGIN_TIMEOUT.getDefaultValue(); // if the user does not specify a default
                                                                                                          // timeout, default is 15 per spec
                        String sPropValue = propsIn.getProperty(SQLServerDriverIntProperty.LOGIN_TIMEOUT.toString());
                        if (null != sPropValue && sPropValue.length() > 0) {
                            int sPropValueInt = Integer.parseInt(sPropValue);
                            if (0 != sPropValueInt) { // Use the default timeout in case of a zero value
                                loginTimeoutSeconds = sPropValueInt;
                            }
                        }
                    }

                    retryAttempt++;
                    long elapsedSeconds = ((System.currentTimeMillis() - start) / 1000L);

                    if (INTERMITTENT_TLS_MAX_RETRY < retryAttempt) {
                        // Re-throw the exception if we have reached the maximum retry attempts.
                        if (connectionlogger.isLoggable(Level.FINE)) {
                            connectionlogger.fine(
                                    "Connection failed during SSL handshake. Maximum retry attempt (" + INTERMITTENT_TLS_MAX_RETRY + ") reached.  ");
                        }
                        throw e;
                    }
                    else if (elapsedSeconds >= loginTimeoutSeconds) {
                        // Re-throw the exception if we do not have any time left to retry.
                        if (connectionlogger.isLoggable(Level.FINE)) {
                            connectionlogger.fine("Connection failed during SSL handshake. Not retrying as timeout expired.");
                        }
                        throw e;
                    }
                    else {
                        // Retry the connection.
                        if (connectionlogger.isLoggable(Level.FINE)) {
                            connectionlogger
                                    .fine("Connection failed during SSL handshake. Retrying due to an intermittent TLS 1.2 failure issue. Retry attempt = "
                                            + retryAttempt + ".");
                        }
                    }
                }
            }
        }
    }

    private void registerKeyStoreProviderOnConnection(String keyStoreAuth,
            String keyStoreSecret,
            String keyStoreLocation) throws SQLServerException {
        if (null == keyStoreAuth) {
            // secret and location must be null too.
            if ((null != keyStoreSecret)) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_keyStoreAuthenticationNotSet"));
                Object[] msgArgs = {"keyStoreSecret"};
                throw new SQLServerException(form.format(msgArgs), null);
            }
            if (null != keyStoreLocation) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_keyStoreAuthenticationNotSet"));
                Object[] msgArgs = {"keyStoreLocation"};
                throw new SQLServerException(form.format(msgArgs), null);
            }
        }
        else {
            KeyStoreAuthentication keyStoreAuthentication = KeyStoreAuthentication.valueOfString(keyStoreAuth);
            switch (keyStoreAuthentication) {
                case JavaKeyStorePassword:
                    // both secret and location must be set for JKS.
                    if ((null == keyStoreSecret) || (null == keyStoreLocation)) {
                        throw new SQLServerException(SQLServerException.getErrString("R_keyStoreSecretOrLocationNotSet"), null);
                    }
                    else {
                        SQLServerColumnEncryptionJavaKeyStoreProvider provider = new SQLServerColumnEncryptionJavaKeyStoreProvider(keyStoreLocation,
                                keyStoreSecret.toCharArray());
                        systemColumnEncryptionKeyStoreProvider.put(provider.getName(), provider);
                    }
                    break;

                default:
                    // valueOfString would throw an exception if the keyStoreAuthentication is not valid.
                    break;
            }
        }
    }

    /**
     * Establish a physical database connection based on the user specified connection properties. Logon to the database.
     *
     * @param propsIn
     *            the connection properties
     * @param pooledConnection
     *            a parent pooled connection if this is a logical connection
     * @throws SQLServerException
     * @return the database connection
     */
    Connection connectInternal(Properties propsIn,
            SQLServerPooledConnection pooledConnection) throws SQLServerException {
        try {
            activeConnectionProperties = (Properties) propsIn.clone();

            pooledConnectionParent = pooledConnection;

            String sPropKey;
            String sPropValue;

            sPropKey = SQLServerDriverStringProperty.USER.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (sPropValue == null) {
                sPropValue = SQLServerDriverStringProperty.USER.getDefaultValue();
                activeConnectionProperties.setProperty(sPropKey, sPropValue);
            }
            ValidateMaxSQLLoginName(sPropKey, sPropValue);

            sPropKey = SQLServerDriverStringProperty.PASSWORD.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (sPropValue == null) {
                sPropValue = SQLServerDriverStringProperty.PASSWORD.getDefaultValue();
                activeConnectionProperties.setProperty(sPropKey, sPropValue);
            }
            ValidateMaxSQLLoginName(sPropKey, sPropValue);

            sPropKey = SQLServerDriverStringProperty.DATABASE_NAME.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            ValidateMaxSQLLoginName(sPropKey, sPropValue);

            int loginTimeoutSeconds = SQLServerDriverIntProperty.LOGIN_TIMEOUT.getDefaultValue(); // if the user does not specify a default timeout,
                                                                                                  // default is 15 per spec
            sPropValue = activeConnectionProperties.getProperty(SQLServerDriverIntProperty.LOGIN_TIMEOUT.toString());
            if (null != sPropValue && sPropValue.length() > 0) {
                try {
                    loginTimeoutSeconds = Integer.parseInt(sPropValue);
                }
                catch (NumberFormatException e) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidTimeOut"));
                    Object[] msgArgs = {sPropValue};
                    SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
                }

                if (loginTimeoutSeconds < 0 || loginTimeoutSeconds > 65535) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidTimeOut"));
                    Object[] msgArgs = {sPropValue};
                    SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
                }
            }

            // Translates the serverName from Unicode to ASCII Compatible Encoding (ACE), as defined by the ToASCII operation of RFC 3490.
            sPropKey = SQLServerDriverBooleanProperty.SERVER_NAME_AS_ACE.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (sPropValue == null) {
                sPropValue = Boolean.toString(SQLServerDriverBooleanProperty.SERVER_NAME_AS_ACE.getDefaultValue());
                activeConnectionProperties.setProperty(sPropKey, sPropValue);
            }
            serverNameAsACE = booleanPropertyOn(sPropKey, sPropValue);

            // get the server name from the properties if it has instance name in it, getProperty the instance name
            // if there is a port number specified do not get the port number from the instance name
            sPropKey = SQLServerDriverStringProperty.SERVER_NAME.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);

            if (sPropValue == null) {
                sPropValue = "localhost";
            }

            String sPropKeyPort = SQLServerDriverIntProperty.PORT_NUMBER.toString();
            String sPropValuePort = activeConnectionProperties.getProperty(sPropKeyPort);

            int px = sPropValue.indexOf('\\');

            String instanceValue = null;

            String instanceNameProperty = SQLServerDriverStringProperty.INSTANCE_NAME.toString();
            // found the instance name with the severname
            if (px >= 0) {
                instanceValue = sPropValue.substring(px + 1, sPropValue.length());
                ValidateMaxSQLLoginName(instanceNameProperty, instanceValue);
                sPropValue = sPropValue.substring(0, px);
            }
            trustedServerNameAE = sPropValue;

            if (true == serverNameAsACE) {
                try {
                    sPropValue = IDN.toASCII(sPropValue);
                }
                catch (IllegalArgumentException ex) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidConnectionSetting"));
                    Object[] msgArgs = {"serverNameAsACE", sPropValue};
                    throw new SQLServerException(form.format(msgArgs), ex);
                }
            }
            activeConnectionProperties.setProperty(sPropKey, sPropValue);

            String instanceValueFromProp = activeConnectionProperties.getProperty(instanceNameProperty);
            // property takes precedence
            if (null != instanceValueFromProp)
                instanceValue = instanceValueFromProp;

            if (instanceValue != null) {
                ValidateMaxSQLLoginName(instanceNameProperty, instanceValue);
                // only get port if the port is not specified
                activeConnectionProperties.setProperty(instanceNameProperty, instanceValue);
                trustedServerNameAE += "\\" + instanceValue;
            }

            if (null != sPropValuePort) {
                trustedServerNameAE += ":" + sPropValuePort;
            }

            sPropKey = SQLServerDriverStringProperty.APPLICATION_NAME.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (sPropValue != null)
                ValidateMaxSQLLoginName(sPropKey, sPropValue);
            else
                activeConnectionProperties.setProperty(sPropKey, SQLServerDriver.DEFAULT_APP_NAME);

            sPropKey = SQLServerDriverBooleanProperty.LAST_UPDATE_COUNT.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (sPropValue == null) {
                sPropValue = Boolean.toString(SQLServerDriverBooleanProperty.LAST_UPDATE_COUNT.getDefaultValue());
                activeConnectionProperties.setProperty(sPropKey, sPropValue);
            }

            sPropKey = SQLServerDriverStringProperty.COLUMN_ENCRYPTION.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (null == sPropValue) {
                sPropValue = SQLServerDriverStringProperty.COLUMN_ENCRYPTION.getDefaultValue();
                activeConnectionProperties.setProperty(sPropKey, sPropValue);
            }
            columnEncryptionSetting = ColumnEncryptionSetting.valueOfString(sPropValue).toString();

            sPropKey = SQLServerDriverStringProperty.KEY_STORE_AUTHENTICATION.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (null != sPropValue) {
                keyStoreAuthentication = KeyStoreAuthentication.valueOfString(sPropValue).toString();
            }

            sPropKey = SQLServerDriverStringProperty.KEY_STORE_SECRET.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (null != sPropValue) {
                keyStoreSecret = sPropValue;
            }

            sPropKey = SQLServerDriverStringProperty.KEY_STORE_LOCATION.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (null != sPropValue) {
                keyStoreLocation = sPropValue;
            }

            registerKeyStoreProviderOnConnection(keyStoreAuthentication, keyStoreSecret, keyStoreLocation);

            sPropKey = SQLServerDriverBooleanProperty.MULTI_SUBNET_FAILOVER.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (sPropValue == null) {
                sPropValue = Boolean.toString(SQLServerDriverBooleanProperty.MULTI_SUBNET_FAILOVER.getDefaultValue());
                activeConnectionProperties.setProperty(sPropKey, sPropValue);
            }
            multiSubnetFailover = booleanPropertyOn(sPropKey, sPropValue);

            sPropKey = SQLServerDriverBooleanProperty.TRANSPARENT_NETWORK_IP_RESOLUTION.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (sPropValue == null) {
                userSetTNIR = false;
                sPropValue = Boolean.toString(SQLServerDriverBooleanProperty.TRANSPARENT_NETWORK_IP_RESOLUTION.getDefaultValue());
                activeConnectionProperties.setProperty(sPropKey, sPropValue);
            }
            transparentNetworkIPResolution = booleanPropertyOn(sPropKey, sPropValue);

            sPropKey = SQLServerDriverBooleanProperty.ENCRYPT.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (sPropValue == null) {
                sPropValue = Boolean.toString(SQLServerDriverBooleanProperty.ENCRYPT.getDefaultValue());
                activeConnectionProperties.setProperty(sPropKey, sPropValue);
            }

            // Set requestedEncryptionLevel according to the value of the encrypt connection property
            requestedEncryptionLevel = booleanPropertyOn(sPropKey, sPropValue) ? TDS.ENCRYPT_ON : TDS.ENCRYPT_OFF;

            sPropKey = SQLServerDriverBooleanProperty.TRUST_SERVER_CERTIFICATE.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (sPropValue == null) {
                sPropValue = Boolean.toString(SQLServerDriverBooleanProperty.TRUST_SERVER_CERTIFICATE.getDefaultValue());
                activeConnectionProperties.setProperty(sPropKey, sPropValue);
            }

            trustServerCertificate = booleanPropertyOn(sPropKey, sPropValue);

            trustManagerClass = activeConnectionProperties.getProperty(SQLServerDriverStringProperty.TRUST_MANAGER_CLASS.toString());
            trustManagerConstructorArg = activeConnectionProperties.getProperty(SQLServerDriverStringProperty.TRUST_MANAGER_CONSTRUCTOR_ARG.toString());

            sPropKey = SQLServerDriverStringProperty.SELECT_METHOD.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (sPropValue == null)
                sPropValue = SQLServerDriverStringProperty.SELECT_METHOD.getDefaultValue();
            if ("cursor".equalsIgnoreCase(sPropValue) || "direct".equalsIgnoreCase(sPropValue)) {
                activeConnectionProperties.setProperty(sPropKey, sPropValue.toLowerCase(Locale.ENGLISH));
            }
            else {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidselectMethod"));
                Object[] msgArgs = {sPropValue};
                SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
            }

            sPropKey = SQLServerDriverStringProperty.RESPONSE_BUFFERING.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (sPropValue == null)
                sPropValue = SQLServerDriverStringProperty.RESPONSE_BUFFERING.getDefaultValue();
            if ("full".equalsIgnoreCase(sPropValue) || "adaptive".equalsIgnoreCase(sPropValue)) {
                activeConnectionProperties.setProperty(sPropKey, sPropValue.toLowerCase(Locale.ENGLISH));
            }
            else {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidresponseBuffering"));
                Object[] msgArgs = {sPropValue};
                SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
            }

            sPropKey = SQLServerDriverStringProperty.APPLICATION_INTENT.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (sPropValue == null)
                sPropValue = SQLServerDriverStringProperty.APPLICATION_INTENT.getDefaultValue();
            applicationIntent = ApplicationIntent.valueOfString(sPropValue);
            activeConnectionProperties.setProperty(sPropKey, applicationIntent.toString());

            sPropKey = SQLServerDriverBooleanProperty.SEND_TIME_AS_DATETIME.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (sPropValue == null) {
                sPropValue = Boolean.toString(SQLServerDriverBooleanProperty.SEND_TIME_AS_DATETIME.getDefaultValue());
                activeConnectionProperties.setProperty(sPropKey, sPropValue);
            }

            sendTimeAsDatetime = booleanPropertyOn(sPropKey, sPropValue);

            // Must be set before DISABLE_STATEMENT_POOLING
            sPropKey = SQLServerDriverIntProperty.STATEMENT_POOLING_CACHE_SIZE.toString();
            if (activeConnectionProperties.getProperty(sPropKey) != null && activeConnectionProperties.getProperty(sPropKey).length() > 0) {
                try {
                    int n = Integer.parseInt(activeConnectionProperties.getProperty(sPropKey));
                    this.setStatementPoolingCacheSize(n);
                }
                catch (NumberFormatException e) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_statementPoolingCacheSize"));
                    Object[] msgArgs = {activeConnectionProperties.getProperty(sPropKey)};
                    SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
                }
            }

            // Must be set after STATEMENT_POOLING_CACHE_SIZE
            sPropKey = SQLServerDriverBooleanProperty.DISABLE_STATEMENT_POOLING.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (null != sPropValue) {
                setDisableStatementPooling(booleanPropertyOn(sPropKey, sPropValue));
            } 

            sPropKey = SQLServerDriverBooleanProperty.INTEGRATED_SECURITY.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (sPropValue != null) {
                integratedSecurity = booleanPropertyOn(sPropKey, sPropValue);
            }

            // Ignore authenticationScheme setting if integrated authentication not specified
            if (integratedSecurity) {
                sPropKey = SQLServerDriverStringProperty.AUTHENTICATION_SCHEME.toString();
                sPropValue = activeConnectionProperties.getProperty(sPropKey);
                if (sPropValue != null) {
                    intAuthScheme = AuthenticationScheme.valueOfString(sPropValue);
                }
            }

            if(intAuthScheme == AuthenticationScheme.javaKerberos){
                sPropKey = SQLServerDriverObjectProperty.GSS_CREDENTIAL.toString();
                if(activeConnectionProperties.containsKey(sPropKey))
                    ImpersonatedUserCred = (GSSCredential) activeConnectionProperties.get(sPropKey);
            }
            
            sPropKey = SQLServerDriverStringProperty.AUTHENTICATION.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (sPropValue == null) {
                sPropValue = SQLServerDriverStringProperty.AUTHENTICATION.getDefaultValue();
            }
            authenticationString = SqlAuthentication.valueOfString(sPropValue).toString();

            if ((true == integratedSecurity) && (!authenticationString.equalsIgnoreCase(SqlAuthentication.NotSpecified.toString()))) {
                if (connectionlogger.isLoggable(Level.SEVERE)) {
                    connectionlogger.severe(toString() + " " + SQLServerException.getErrString("R_SetAuthenticationWhenIntegratedSecurityTrue"));
                }
                throw new SQLServerException(SQLServerException.getErrString("R_SetAuthenticationWhenIntegratedSecurityTrue"), null);
            }

            if (authenticationString.equalsIgnoreCase(SqlAuthentication.ActiveDirectoryIntegrated.toString())
                    && ((!activeConnectionProperties.getProperty(SQLServerDriverStringProperty.USER.toString()).isEmpty())
                            || (!activeConnectionProperties.getProperty(SQLServerDriverStringProperty.PASSWORD.toString()).isEmpty()))) {
                if (connectionlogger.isLoggable(Level.SEVERE)) {
                    connectionlogger.severe(toString() + " " + SQLServerException.getErrString("R_IntegratedAuthenticationWithUserPassword"));
                }
                throw new SQLServerException(SQLServerException.getErrString("R_IntegratedAuthenticationWithUserPassword"), null);
            }

            if (authenticationString.equalsIgnoreCase(SqlAuthentication.ActiveDirectoryPassword.toString())
                    && ((activeConnectionProperties.getProperty(SQLServerDriverStringProperty.USER.toString()).isEmpty())
                            || (activeConnectionProperties.getProperty(SQLServerDriverStringProperty.PASSWORD.toString()).isEmpty()))) {
                if (connectionlogger.isLoggable(Level.SEVERE)) {
                    connectionlogger.severe(toString() + " " + SQLServerException.getErrString("R_NoUserPasswordForActivePassword"));
                }
                throw new SQLServerException(SQLServerException.getErrString("R_NoUserPasswordForActivePassword"), null);
            }

            if (authenticationString.equalsIgnoreCase(SqlAuthentication.SqlPassword.toString())
                    && ((activeConnectionProperties.getProperty(SQLServerDriverStringProperty.USER.toString()).isEmpty())
                            || (activeConnectionProperties.getProperty(SQLServerDriverStringProperty.PASSWORD.toString()).isEmpty()))) {
                if (connectionlogger.isLoggable(Level.SEVERE)) {
                    connectionlogger.severe(toString() + " " + SQLServerException.getErrString("R_NoUserPasswordForSqlPassword"));
                }
                throw new SQLServerException(SQLServerException.getErrString("R_NoUserPasswordForSqlPassword"), null);
            }

            sPropKey = SQLServerDriverStringProperty.ACCESS_TOKEN.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (null != sPropValue) {
                accessTokenInByte = sPropValue.getBytes(UTF_16LE);
            }

            if ((null != accessTokenInByte) && 0 == accessTokenInByte.length) {
                if (connectionlogger.isLoggable(Level.SEVERE)) {
                    connectionlogger.severe(toString() + " " + SQLServerException.getErrString("R_AccessTokenCannotBeEmpty"));
                }
                throw new SQLServerException(SQLServerException.getErrString("R_AccessTokenCannotBeEmpty"), null);
            }

            if ((true == integratedSecurity) && (null != accessTokenInByte)) {
                if (connectionlogger.isLoggable(Level.SEVERE)) {
                    connectionlogger.severe(toString() + " " + SQLServerException.getErrString("R_SetAccesstokenWhenIntegratedSecurityTrue"));
                }
                throw new SQLServerException(SQLServerException.getErrString("R_SetAccesstokenWhenIntegratedSecurityTrue"), null);
            }

            if ((!authenticationString.equalsIgnoreCase(SqlAuthentication.NotSpecified.toString())) && (null != accessTokenInByte)) {
                if (connectionlogger.isLoggable(Level.SEVERE)) {
                    connectionlogger.severe(toString() + " " + SQLServerException.getErrString("R_SetBothAuthenticationAndAccessToken"));
                }
                throw new SQLServerException(SQLServerException.getErrString("R_SetBothAuthenticationAndAccessToken"), null);
            }

            if ((null != accessTokenInByte) && ((!activeConnectionProperties.getProperty(SQLServerDriverStringProperty.USER.toString()).isEmpty())
                    || (!activeConnectionProperties.getProperty(SQLServerDriverStringProperty.PASSWORD.toString()).isEmpty()))) {
                if (connectionlogger.isLoggable(Level.SEVERE)) {
                    connectionlogger.severe(toString() + " " + SQLServerException.getErrString("R_AccessTokenWithUserPassword"));
                }
                throw new SQLServerException(SQLServerException.getErrString("R_AccessTokenWithUserPassword"), null);
            }

            // Turn off TNIR for FedAuth if user does not set TNIR explicitly
            if (!userSetTNIR) {
                if ((!authenticationString.equalsIgnoreCase(SqlAuthentication.NotSpecified.toString())) || (null != accessTokenInByte)) {
                    transparentNetworkIPResolution = false;
                }
            }

            sPropKey = SQLServerDriverStringProperty.WORKSTATION_ID.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            ValidateMaxSQLLoginName(sPropKey, sPropValue);

            int nPort = 0;
            sPropKey = SQLServerDriverIntProperty.PORT_NUMBER.toString();
            try {
                String strPort = activeConnectionProperties.getProperty(sPropKey);
                if (null != strPort) {
                    nPort = Integer.parseInt(strPort);

                    if ((nPort < 0) || (nPort > 65535)) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPortNumber"));
                        Object[] msgArgs = {Integer.toString(nPort)};
                        SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
                    }
                }
            }
            catch (NumberFormatException e) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPortNumber"));
                Object[] msgArgs = {activeConnectionProperties.getProperty(sPropKey)};
                SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
            }

            // Handle optional packetSize property
            sPropKey = SQLServerDriverIntProperty.PACKET_SIZE.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (null != sPropValue && sPropValue.length() > 0) {
                try {
                    requestedPacketSize = Integer.parseInt(sPropValue);

                    // -1 --> Use server default
                    if (-1 == requestedPacketSize)
                        requestedPacketSize = TDS.SERVER_PACKET_SIZE;

                    // 0 --> Use maximum size
                    else if (0 == requestedPacketSize)
                        requestedPacketSize = TDS.MAX_PACKET_SIZE;
                }
                catch (NumberFormatException e) {
                    // Ensure that an invalid prop value results in an invalid packet size that
                    // is not acceptable to the server.
                    requestedPacketSize = TDS.INVALID_PACKET_SIZE;
                }

                if (TDS.SERVER_PACKET_SIZE != requestedPacketSize) {
                    // Complain if the packet size is not in the range acceptable to the server.
                    if (requestedPacketSize < TDS.MIN_PACKET_SIZE || requestedPacketSize > TDS.MAX_PACKET_SIZE) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPacketSize"));
                        Object[] msgArgs = {sPropValue};
                        SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
                    }
                }
            }

            // Note booleanPropertyOn will throw exception if parsed value is not valid.

            // have to check for null before calling booleanPropertyOn, because booleanPropertyOn
            // assumes that the null property defaults to false.
            sPropKey = SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.toString();
            if (null == activeConnectionProperties.getProperty(sPropKey)) {
                sendStringParametersAsUnicode = SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.getDefaultValue();
            }
            else {
                sendStringParametersAsUnicode = booleanPropertyOn(sPropKey, activeConnectionProperties.getProperty(sPropKey));
            }

            sPropKey = SQLServerDriverBooleanProperty.LAST_UPDATE_COUNT.toString();
            lastUpdateCount = booleanPropertyOn(sPropKey, activeConnectionProperties.getProperty(sPropKey));
            sPropKey = SQLServerDriverBooleanProperty.XOPEN_STATES.toString();
            xopenStates = booleanPropertyOn(sPropKey, activeConnectionProperties.getProperty(sPropKey));

            sPropKey = SQLServerDriverStringProperty.SELECT_METHOD.toString();
            selectMethod = null;
            if (activeConnectionProperties.getProperty(sPropKey) != null && activeConnectionProperties.getProperty(sPropKey).length() > 0) {
                selectMethod = activeConnectionProperties.getProperty(sPropKey);
            }

            sPropKey = SQLServerDriverStringProperty.RESPONSE_BUFFERING.toString();
            responseBuffering = null;
            if (activeConnectionProperties.getProperty(sPropKey) != null && activeConnectionProperties.getProperty(sPropKey).length() > 0) {
                responseBuffering = activeConnectionProperties.getProperty(sPropKey);
            }

            sPropKey = SQLServerDriverIntProperty.LOCK_TIMEOUT.toString();
            int defaultLockTimeOut = SQLServerDriverIntProperty.LOCK_TIMEOUT.getDefaultValue();
            nLockTimeout = defaultLockTimeOut; // Wait forever
            if (activeConnectionProperties.getProperty(sPropKey) != null && activeConnectionProperties.getProperty(sPropKey).length() > 0) {
                try {
                    int n = Integer.parseInt(activeConnectionProperties.getProperty(sPropKey));
                    if (n >= defaultLockTimeOut)
                        nLockTimeout = n;
                    else {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidLockTimeOut"));
                        Object[] msgArgs = {activeConnectionProperties.getProperty(sPropKey)};
                        SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
                    }
                }
                catch (NumberFormatException e) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidLockTimeOut"));
                    Object[] msgArgs = {activeConnectionProperties.getProperty(sPropKey)};
                    SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
                }
            }

            sPropKey = SQLServerDriverIntProperty.QUERY_TIMEOUT.toString();
            int defaultQueryTimeout = SQLServerDriverIntProperty.QUERY_TIMEOUT.getDefaultValue();
            queryTimeoutSeconds = defaultQueryTimeout; // Wait forever
            if (activeConnectionProperties.getProperty(sPropKey) != null && activeConnectionProperties.getProperty(sPropKey).length() > 0) {
                try {
                    int n = Integer.parseInt(activeConnectionProperties.getProperty(sPropKey));
                    if (n >= defaultQueryTimeout) {
                        queryTimeoutSeconds = n;
                    }
                    else {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidQueryTimeout"));
                        Object[] msgArgs = {activeConnectionProperties.getProperty(sPropKey)};
                        SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
                    }
                }
                catch (NumberFormatException e) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidQueryTimeout"));
                    Object[] msgArgs = {activeConnectionProperties.getProperty(sPropKey)};
                    SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
                }
            }

            sPropKey = SQLServerDriverIntProperty.SOCKET_TIMEOUT.toString();
            int defaultSocketTimeout = SQLServerDriverIntProperty.SOCKET_TIMEOUT.getDefaultValue();
            socketTimeoutMilliseconds = defaultSocketTimeout; // Wait forever
            if (activeConnectionProperties.getProperty(sPropKey) != null && activeConnectionProperties.getProperty(sPropKey).length() > 0) {
                try {
                    int n = Integer.parseInt(activeConnectionProperties.getProperty(sPropKey));
                    if (n >= defaultSocketTimeout) {
                        socketTimeoutMilliseconds = n;
                    }
                    else {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidSocketTimeout"));
                        Object[] msgArgs = {activeConnectionProperties.getProperty(sPropKey)};
                        SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
                    }
                }
                catch (NumberFormatException e) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidSocketTimeout"));
                    Object[] msgArgs = {activeConnectionProperties.getProperty(sPropKey)};
                    SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
                }
            }
            
            sPropKey = SQLServerDriverIntProperty.SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD.toString();
            if (activeConnectionProperties.getProperty(sPropKey) != null && activeConnectionProperties.getProperty(sPropKey).length() > 0) {
                try {
                    int n = Integer.parseInt(activeConnectionProperties.getProperty(sPropKey));
                    setServerPreparedStatementDiscardThreshold(n);
                }
                catch (NumberFormatException e) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_serverPreparedStatementDiscardThreshold"));
                    Object[] msgArgs = {activeConnectionProperties.getProperty(sPropKey)};
                    SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
                }
            }

            sPropKey = SQLServerDriverBooleanProperty.ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (null != sPropValue) {
                setEnablePrepareOnFirstPreparedStatementCall(booleanPropertyOn(sPropKey, sPropValue));
            }

            sPropKey = SQLServerDriverStringProperty.SSL_PROTOCOL.toString();
            sPropValue = activeConnectionProperties.getProperty(sPropKey);
            if (null == sPropValue) {
                sPropValue = SQLServerDriverStringProperty.SSL_PROTOCOL.getDefaultValue();
                activeConnectionProperties.setProperty(sPropKey, sPropValue);
            }
            else {
                activeConnectionProperties.setProperty(sPropKey, SSLProtocol.valueOfString(sPropValue).toString());
            }
            
            FailoverInfo fo = null;
            String databaseNameProperty = SQLServerDriverStringProperty.DATABASE_NAME.toString();
            String serverNameProperty = SQLServerDriverStringProperty.SERVER_NAME.toString();
            String failOverPartnerProperty = SQLServerDriverStringProperty.FAILOVER_PARTNER.toString();
            String failOverPartnerPropertyValue = activeConnectionProperties.getProperty(failOverPartnerProperty);

            // failoverPartner and multiSubnetFailover=true cannot be used together
            if (multiSubnetFailover && failOverPartnerPropertyValue != null) {
                SQLServerException.makeFromDriverError(this, this, SQLServerException.getErrString("R_dbMirroringWithMultiSubnetFailover"), null,
                        false);
            }

            // transparentNetworkIPResolution is ignored if multiSubnetFailover or DBMirroring is true and user does not set TNIR explicitly
            if (multiSubnetFailover || (null != failOverPartnerPropertyValue)) {
                if (!userSetTNIR) {
                    transparentNetworkIPResolution = false;
                }
            }

            // failoverPartner and applicationIntent=ReadOnly cannot be used together
            if ((applicationIntent != null) && applicationIntent.equals(ApplicationIntent.READ_ONLY) && failOverPartnerPropertyValue != null) {
                SQLServerException.makeFromDriverError(this, this, SQLServerException.getErrString("R_dbMirroringWithReadOnlyIntent"), null, false);
            }

            // check to see failover specified without DB error here if not.
            if (null != activeConnectionProperties.getProperty(databaseNameProperty)) {
                // look to see if there exists a failover
                fo = FailoverMapSingleton.getFailoverInfo(this, activeConnectionProperties.getProperty(serverNameProperty),
                        activeConnectionProperties.getProperty(instanceNameProperty), activeConnectionProperties.getProperty(databaseNameProperty));
            }
            else {
                // it is an error to specify failover without db.
                if (null != failOverPartnerPropertyValue)
                    SQLServerException.makeFromDriverError(this, this, SQLServerException.getErrString("R_failoverPartnerWithoutDB"), null, true);
            }

            String mirror = null;
            if (null == fo)
                mirror = failOverPartnerPropertyValue;

            long startTime = System.currentTimeMillis();
            login(activeConnectionProperties.getProperty(serverNameProperty), instanceValue, nPort, mirror, fo, loginTimeoutSeconds, startTime);

            // If SSL is to be used for the duration of the connection, then make sure
            // that the final negotiated TDS packet size is no larger than the SSL record size.
            if (TDS.ENCRYPT_ON == negotiatedEncryptionLevel || TDS.ENCRYPT_REQ == negotiatedEncryptionLevel) {
                // IBM (Websphere) security provider uses 8K SSL record size. All others use 16K.
                int sslRecordSize = Util.isIBM() ? 8192 : 16384;

                if (tdsPacketSize > sslRecordSize) {
                    if (connectionlogger.isLoggable(Level.FINER)) {
                        connectionlogger.finer(toString() + " Negotiated tdsPacketSize " + tdsPacketSize + " is too large for SSL with JRE "
                                + Util.SYSTEM_JRE + " (max size is " + sslRecordSize + ")");
                    }
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_packetSizeTooBigForSSL"));
                    Object[] msgArgs = {Integer.toString(sslRecordSize)};
                    terminate(SQLServerException.DRIVER_ERROR_UNSUPPORTED_CONFIG, form.format(msgArgs));
                }
            }

            state = State.Opened;

            if (connectionlogger.isLoggable(Level.FINER)) {
                connectionlogger.finer(toString() + " End of connect");
            }
        }
        finally {
            // once we exit the connect function, the connection can be only in one of two
            // states, Opened or Closed(if an exception occurred)
            if (!state.equals(State.Opened)) {
                // if connection is not closed, close it
                if (!state.equals(State.Closed))
                    this.close();
            }
        }

        return this;

    }

    // This function is used by non failover and failover cases. Even when we make a standard connection the server can provide us with its
    // FO partner.
    // If no FO information is available a standard connection is made.
    // If the server returns a failover upon connection, we shall store the FO in our cache.
    //
    private void login(String primary,
            String primaryInstanceName,
            int primaryPortNumber,
            String mirror,
            FailoverInfo foActual,
            int timeout,
            long timerStart) throws SQLServerException {
        // standardLogin would be false only for db mirroring scenarios. It would be true
        // for all other cases, including multiSubnetFailover
        final boolean isDBMirroring = null != mirror || null != foActual;
        int sleepInterval = 100;  // milliseconds to sleep (back off) between attempts.
        long timeoutUnitInterval;

        boolean useFailoverHost = false;
        FailoverInfo tempFailover = null;
        // This is the failover server info place holder
        ServerPortPlaceHolder currentFOPlaceHolder = null;
        // This is the primary server placeHolder
        ServerPortPlaceHolder currentPrimaryPlaceHolder = null;

        if (null != foActual) {
            tempFailover = foActual;
            useFailoverHost = foActual.getUseFailoverPartner();
        }
        else {
            if (isDBMirroring)
                // Create a temporary class with the mirror info from the user
                tempFailover = new FailoverInfo(mirror, this, false);
        }

        // useParallel is set to true only for the first connection
        // when multiSubnetFailover is set to true. In all other cases, it is set
        // to false.
        boolean useParallel = getMultiSubnetFailover();
        boolean useTnir = getTransparentNetworkIPResolution();

        long intervalExpire;

        if (0 == timeout) {
            timeout = SQLServerDriverIntProperty.LOGIN_TIMEOUT.getDefaultValue();
        }
        long timerTimeout = timeout * 1000L;   // ConnectTimeout is in seconds, we need timer millis
        timerExpire = timerStart + timerTimeout;

        // For non-dbmirroring, non-tnir and non-multisubnetfailover scenarios, full time out would be used as time slice.
        if (isDBMirroring || useParallel) {
            timeoutUnitInterval = (long) (TIMEOUTSTEP * timerTimeout);
        }
        else if (useTnir) {
            timeoutUnitInterval = (long) (TIMEOUTSTEP_TNIR * timerTimeout);
        }
        else {
            timeoutUnitInterval = timerTimeout;
        }
        intervalExpire = timerStart + timeoutUnitInterval;
        // This is needed when the host resolves to more than 64 IP addresses. In that case, TNIR is ignored
        // and the original timeout is used instead of the timeout slice.
        long intervalExpireFullTimeout = timerStart + timerTimeout;

        if (connectionlogger.isLoggable(Level.FINER)) {
            connectionlogger.finer(
                    toString() + " Start time: " + timerStart + " Time out time: " + timerExpire + " Timeout Unit Interval: " + timeoutUnitInterval);
        }

        // Initialize loop variables
        int attemptNumber = 0;

        // indicates the no of times the connection was routed to a different server
        int noOfRedirections = 0;

        // Only three ways out of this loop:
        // 1) Successfully connected
        // 2) Parser threw exception while main timer was expired
        // 3) Parser threw logon failure-related exception (LOGON_FAILED, PASSWORD_EXPIRED, etc)
        //
        // Of these methods, only #1 exits normally. This preserves the call stack on the exception
        // back into the parser for the error cases.
        while (true) {
            clientConnectionId = null;
            state = State.Initialized;

            try {
                if (isDBMirroring && useFailoverHost)

                {
                    if (null == currentFOPlaceHolder) {
                        // integrated security flag passed here to verify that the linked dll can be loaded
                        currentFOPlaceHolder = tempFailover.failoverPermissionCheck(this, integratedSecurity);
                    }
                    currentConnectPlaceHolder = currentFOPlaceHolder;
                }
                else {
                    if (routingInfo != null) {
                        currentPrimaryPlaceHolder = routingInfo;
                        routingInfo = null;
                    }
                    else if (null == currentPrimaryPlaceHolder) {
                        currentPrimaryPlaceHolder = primaryPermissionCheck(primary, primaryInstanceName, primaryPortNumber);
                    }
                    currentConnectPlaceHolder = currentPrimaryPlaceHolder;
                }

                // logging code
                if (connectionlogger.isLoggable(Level.FINE)) {
                    connectionlogger.fine(toString() + " This attempt server name: " + currentConnectPlaceHolder.getServerName() + " port: "
                            + currentConnectPlaceHolder.getPortNumber() + " InstanceName: " + currentConnectPlaceHolder.getInstanceName()
                            + " useParallel: " + useParallel);
                    connectionlogger.fine(toString() + " This attempt endtime: " + intervalExpire);
                    connectionlogger.fine(toString() + " This attempt No: " + attemptNumber);
                }
                // end logging code

                // Attempt login.
                // use Place holder to make sure that the failoverdemand is done.

                connectHelper(currentConnectPlaceHolder, TimerRemaining(intervalExpire), timeout, useParallel, useTnir, (0 == attemptNumber), // Is
                                                                                                                                              // this
                                                                                                                                              // the
                                                                                                                                              // TNIR
                                                                                                                                              // first
                                                                                                                                              // attempt
                        TimerRemaining(intervalExpireFullTimeout)); // Only used when host resolves to >64 IPs

                if (isRoutedInCurrentAttempt) {
                    // we ignore the failoverpartner ENVCHANGE, if we got routed.
                    // So, no error needs to be thrown for that case.
                    if (isDBMirroring) {
                        String msg = SQLServerException.getErrString("R_invalidRoutingInfo");
                        terminate(SQLServerException.DRIVER_ERROR_UNSUPPORTED_CONFIG, msg);
                    }

                    noOfRedirections++;

                    if (noOfRedirections > 1) {
                        String msg = SQLServerException.getErrString("R_multipleRedirections");
                        terminate(SQLServerException.DRIVER_ERROR_UNSUPPORTED_CONFIG, msg);
                    }

                    // close tds channel
                    if (tdsChannel != null)
                        tdsChannel.close();

                    initResettableValues();

                    // reset all params that could have been changed due to ENVCHANGE tokens
                    // to defaults, excluding those changed due to routing ENVCHANGE token
                    resetNonRoutingEnvchangeValues();

                    // increase the attempt number. This is not really necessary
                    // (in fact it does not matter whether we increase it or not) as
                    // we do not use any timeslicing for multisubnetfailover. However, this
                    // is done just to be consistent with the rest of the logic.
                    attemptNumber++;

                    // set isRoutedInCurrentAttempt to false for the next attempt
                    isRoutedInCurrentAttempt = false;

                    // useParallel and useTnir should be set to false once we get routed
                    useParallel = false;
                    useTnir = false;

                    // When connection is routed for read only application, remaining timer duration is used as a one full interval
                    intervalExpire = timerExpire;

                    // if timeout expired, throw.
                    if (timerHasExpired(timerExpire)) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_tcpipConnectionFailed"));
                        Object[] msgArgs = {currentConnectPlaceHolder.getServerName(), Integer.toString(currentConnectPlaceHolder.getPortNumber()),
                                SQLServerException.getErrString("R_timedOutBeforeRouting")};
                        String msg = form.format(msgArgs);
                        terminate(SQLServerException.DRIVER_ERROR_UNSUPPORTED_CONFIG, msg);
                    }
                    else {
                        continue;
                    }
                }
                else
                    break; // leave the while loop -- we've successfully connected
            }
            catch (SQLServerException sqlex) {
                if ((SQLServerException.LOGON_FAILED == sqlex.getErrorCode()) // actual logon failed, i.e. bad password
                        || (SQLServerException.PASSWORD_EXPIRED == sqlex.getErrorCode()) // actual logon failed, i.e. password isExpired
                        || (SQLServerException.USER_ACCOUNT_LOCKED == sqlex.getErrorCode()) // actual logon failed, i.e. user account locked
                        || (SQLServerException.DRIVER_ERROR_INVALID_TDS == sqlex.getDriverErrorCode()) // invalid TDS received from server
                        || (SQLServerException.DRIVER_ERROR_SSL_FAILED == sqlex.getDriverErrorCode()) // failure negotiating SSL
                        || (SQLServerException.DRIVER_ERROR_INTERMITTENT_TLS_FAILED == sqlex.getDriverErrorCode()) // failure TLS1.2
                        || (SQLServerException.DRIVER_ERROR_UNSUPPORTED_CONFIG == sqlex.getDriverErrorCode()) // unsupported configuration (e.g.
                                                                                                              // Sphinx, invalid packet size, etc.)
                        || (SQLServerException.ERROR_SOCKET_TIMEOUT == sqlex.getDriverErrorCode()) // socket timeout ocurred
                        || timerHasExpired(timerExpire)// no more time to try again
                        || (state.equals(State.Connected) && !isDBMirroring)
                // for non-dbmirroring cases, do not retry after tcp socket connection succeeds
                ) {

                    // close the connection and throw the error back
                    close();
                    throw sqlex;
                }
                else {
                    // Close the TDS channel from the failed connection attempt so that we don't
                    // hold onto network resources any longer than necessary.
                    if (null != tdsChannel)
                        tdsChannel.close();
                }

                // For standard connections and MultiSubnetFailover connections, change the sleep interval after every attempt.
                // For DB Mirroring, we only sleep after every other attempt.
                if (!isDBMirroring || 1 == attemptNumber % 2) {
                    // Check sleep interval to make sure we won't exceed the timeout
                    // Do this in the catch block so we can re-throw the current exception
                    long remainingMilliseconds = TimerRemaining(timerExpire);
                    if (remainingMilliseconds <= sleepInterval) {
                        throw sqlex;
                    }
                }
            }

            // We only get here when we failed to connect, but are going to re-try
            // After trying to connect to both servers fails, sleep for a bit to prevent clogging
            // the network with requests, then update sleep interval for next iteration (max 1 second interval)
            // We have to sleep for every attempt in case of non-dbMirroring scenarios (including multisubnetfailover),
            // Whereas for dbMirroring, we sleep for every two attempts as each attempt is to a different server.
            if (!isDBMirroring || (1 == attemptNumber % 2)) {
                if (connectionlogger.isLoggable(Level.FINE)) {
                    connectionlogger.fine(toString() + " sleeping milisec: " + sleepInterval);
                }
                try {
                    Thread.sleep(sleepInterval);
                }
                catch (InterruptedException e) {
                    // re-interrupt the current thread, in order to restore the thread's interrupt status.
                    Thread.currentThread().interrupt();
                }
                sleepInterval = (sleepInterval < 500) ? sleepInterval * 2 : 1000;
            }

            // Update timeout interval (but no more than the point where we're supposed to fail: timerExpire)
            attemptNumber++;

            if (useParallel) {
                intervalExpire = System.currentTimeMillis() + (timeoutUnitInterval * (attemptNumber + 1));
            }
            else if (isDBMirroring) {
                intervalExpire = System.currentTimeMillis() + (timeoutUnitInterval * ((attemptNumber / 2) + 1));
            }
            else if (useTnir) {
                long timeSlice = timeoutUnitInterval * (1 << attemptNumber);

                // In case the timeout for the first slice is less than 500 ms then bump it up to 500 ms
                if ((1 == attemptNumber) && (500 > timeSlice)) {
                    timeSlice = 500;
                }

                intervalExpire = System.currentTimeMillis() + timeSlice;
            }
            else
                intervalExpire = timerExpire;
            // Due to the below condition and the timerHasExpired check in catch block,
            // the multiSubnetFailover case or any other standardLogin case where timeOutInterval is full timeout would also be handled correctly.
            if (intervalExpire > timerExpire) {
                intervalExpire = timerExpire;
            }
            // try again, this time swapping primary/secondary servers
            if (isDBMirroring)
                useFailoverHost = !useFailoverHost;
        }

        // If we get here, connection/login succeeded! Just a few more checks & record-keeping
        // if connected to failover host, but said host doesn't have DbMirroring set up, throw an error
        if (useFailoverHost && null == failoverPartnerServerProvided) {
            String curserverinfo = currentConnectPlaceHolder.getServerName();
            if (null != currentFOPlaceHolder.getInstanceName()) {
                curserverinfo = curserverinfo + "\\";
                curserverinfo = curserverinfo + currentFOPlaceHolder.getInstanceName();
            }
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPartnerConfiguration"));
            Object[] msgArgs = {activeConnectionProperties.getProperty(SQLServerDriverStringProperty.DATABASE_NAME.toString()), curserverinfo};
            terminate(SQLServerException.DRIVER_ERROR_UNSUPPORTED_CONFIG, form.format(msgArgs));
        }

        if (null != failoverPartnerServerProvided) {
            // if server returns failoverPartner when multiSubnetFailover keyword is used, fail
            if (multiSubnetFailover) {
                String msg = SQLServerException.getErrString("R_dbMirroringWithMultiSubnetFailover");
                terminate(SQLServerException.DRIVER_ERROR_UNSUPPORTED_CONFIG, msg);
            }

            // if server returns failoverPartner and applicationIntent=ReadOnly, fail
            if ((applicationIntent != null) && applicationIntent.equals(ApplicationIntent.READ_ONLY)) {
                String msg = SQLServerException.getErrString("R_dbMirroringWithReadOnlyIntent");
                terminate(SQLServerException.DRIVER_ERROR_UNSUPPORTED_CONFIG, msg);
            }

            if (null == tempFailover)
                tempFailover = new FailoverInfo(failoverPartnerServerProvided, this, false);
            // if the failover is not from the map already out this in the map, if it is from the map just make sure that we change the
            if (null != foActual) {
                // We must wait for CompleteLogin to finish for to have the
                // env change from the server to know its designated failover
                // partner; saved in failoverPartnerServerProvided
                foActual.failoverAdd(this, useFailoverHost, failoverPartnerServerProvided);
            }
            else {
                String databaseNameProperty = SQLServerDriverStringProperty.DATABASE_NAME.toString();
                String instanceNameProperty = SQLServerDriverStringProperty.INSTANCE_NAME.toString();
                String serverNameProperty = SQLServerDriverStringProperty.SERVER_NAME.toString();

                if (connectionlogger.isLoggable(Level.FINE)) {
                    connectionlogger
                            .fine(toString() + " adding new failover info server: " + activeConnectionProperties.getProperty(serverNameProperty)
                                    + " instance: " + activeConnectionProperties.getProperty(instanceNameProperty) + " database: "
                                    + activeConnectionProperties.getProperty(databaseNameProperty) + " server provided failover: "
                                    + failoverPartnerServerProvided);
                }

                tempFailover.failoverAdd(this, useFailoverHost, failoverPartnerServerProvided);
                FailoverMapSingleton.putFailoverInfo(this, primary, activeConnectionProperties.getProperty(instanceNameProperty),
                        activeConnectionProperties.getProperty(databaseNameProperty), tempFailover, useFailoverHost, failoverPartnerServerProvided);
            }
        }
    }

    // reset all params that could have been changed due to ENVCHANGE tokens to defaults,
    // excluding those changed due to routing ENVCHANGE token
    void resetNonRoutingEnvchangeValues() {
        tdsPacketSize = TDS.INITIAL_PACKET_SIZE;
        databaseCollation = null;
        rolledBackTransaction = false;
        Arrays.fill(getTransactionDescriptor(), (byte) 0);
        sCatalog = originalCatalog;
        failoverPartnerServerProvided = null;
    }

    static final int DEFAULTPORT = SQLServerDriverIntProperty.PORT_NUMBER.getDefaultValue();

    // This code should be similar to the code in FailOverInfo class's failoverPermissionCheck
    // Only difference is that this gets the instance port if the port number is zero where as failover
    // does not have port number available.
    ServerPortPlaceHolder primaryPermissionCheck(String primary,
            String primaryInstanceName,
            int primaryPortNumber) throws SQLServerException {
        String instancePort;
        // look to see primary port number is specified
        if (0 == primaryPortNumber) {
            if (null != primaryInstanceName) {
                instancePort = getInstancePort(primary, primaryInstanceName);
                if (connectionlogger.isLoggable(Level.FINER))
                    connectionlogger.fine(toString() + " SQL Server port returned by SQL Browser: " + instancePort);
                try {
                    if (null != instancePort) {
                        primaryPortNumber = Integer.parseInt(instancePort);

                        if ((primaryPortNumber < 0) || (primaryPortNumber > 65535)) {
                            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPortNumber"));
                            Object[] msgArgs = {Integer.toString(primaryPortNumber)};
                            SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
                        }
                    }
                    else
                        primaryPortNumber = DEFAULTPORT;
                }

                catch (NumberFormatException e) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidPortNumber"));
                    Object[] msgArgs = {primaryPortNumber};
                    SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
                }
            }
            else
                primaryPortNumber = DEFAULTPORT;
        }

        // now we have determined the right port set the connection property back
        activeConnectionProperties.setProperty(SQLServerDriverIntProperty.PORT_NUMBER.toString(), String.valueOf(primaryPortNumber));
        return new ServerPortPlaceHolder(primary, primaryPortNumber, primaryInstanceName, integratedSecurity);
    }

    static boolean timerHasExpired(long timerExpire) {
        boolean result = System.currentTimeMillis() > timerExpire;
        return result;
    }

    static int TimerRemaining(long timerExpire) {
        long timerNow = System.currentTimeMillis();
        long result = timerExpire - timerNow;
        // maximum timeout the socket takes is int max.
        if (result > Integer.MAX_VALUE)
            result = Integer.MAX_VALUE;
        // we have to make sure that we return atleast one ms
        // we want atleast one attempt to happen with a positive timeout passed by the user.
        if (result <= 0)
            result = 1;
        return (int) result;
    }

    /**
     * This is a helper function to connect this gets the port of the server to connect and the server name to connect and the timeout This function
     * achieves one connection attempt Create a prepared statement for internal use by the driver.
     * 
     * @param serverInfo
     * @param timeOutSliceInMillis
     *            -timeout value in milli seconds for one try
     * @param timeOutFullInSeconds
     *            - whole timeout value specified by the user in seconds
     * @param useParallel
     *            - It is used to indicate whether a parallel algorithm should be tried or not for resolving a hostName. Note that useParallel is set
     *            to false for a routed connection even if multiSubnetFailover is set to true.
     * @param useTnir
     * @param isTnirFirstAttempt
     * @param timeOutsliceInMillisForFullTimeout
     * @throws SQLServerException
     */
    private void connectHelper(ServerPortPlaceHolder serverInfo,
            int timeOutsliceInMillis,
            int timeOutFullInSeconds,
            boolean useParallel,
            boolean useTnir,
            boolean isTnirFirstAttempt,
            int timeOutsliceInMillisForFullTimeout) throws SQLServerException {
        // Make the initial tcp-ip connection.

        if (connectionlogger.isLoggable(Level.FINE)) {
            connectionlogger.fine(toString() + " Connecting with server: " + serverInfo.getServerName() + " port: " + serverInfo.getPortNumber()
                    + " Timeout slice: " + timeOutsliceInMillis + " Timeout Full: " + timeOutFullInSeconds);
        }
        
        // Before opening the TDSChannel, calculate local hostname
        // as the InetAddress.getLocalHost() takes more than usual time in certain OS and JVM combination, it avoids connection loss
        hostName = activeConnectionProperties.getProperty(SQLServerDriverStringProperty.WORKSTATION_ID.toString());
        if (StringUtils.isEmpty(hostName)) {
            hostName = Util.lookupHostName();
        }
        
        // if the timeout is infinite slices are infinite too.
        tdsChannel = new TDSChannel(this);
        if (0 == timeOutFullInSeconds)
            tdsChannel.open(serverInfo.getServerName(), serverInfo.getPortNumber(), 0, useParallel, useTnir, isTnirFirstAttempt,
                    timeOutsliceInMillisForFullTimeout);
        else
            tdsChannel.open(serverInfo.getServerName(), serverInfo.getPortNumber(), timeOutsliceInMillis, useParallel, useTnir, isTnirFirstAttempt,
                    timeOutsliceInMillisForFullTimeout);

        setState(State.Connected);

        clientConnectionId = UUID.randomUUID();
        assert null != clientConnectionId;

        Prelogin(serverInfo.getServerName(), serverInfo.getPortNumber());

        // If prelogin negotiated SSL encryption then, enable it on the TDS channel.
        if (TDS.ENCRYPT_NOT_SUP != negotiatedEncryptionLevel) {
            tdsChannel.enableSSL(serverInfo.getServerName(), serverInfo.getPortNumber());
        }

        // We have successfully connected, now do the login. logon takes seconds timeout
        executeCommand(new LogonCommand());
    }

    /**
     * Negotiates prelogin information with the server
     */
    void Prelogin(String serverName,
            int portNumber) throws SQLServerException {
        // Build a TDS Pre-Login packet to send to the server.
        if ((!authenticationString.trim().equalsIgnoreCase(SqlAuthentication.NotSpecified.toString())) || (null != accessTokenInByte)) {
            fedAuthRequiredByUser = true;
        }

        // Message length (incl. header)
        final byte messageLength;
        final byte fedAuthOffset;
        if (fedAuthRequiredByUser) {
            messageLength = TDS.B_PRELOGIN_MESSAGE_LENGTH_WITH_FEDAUTH;
            requestedEncryptionLevel = TDS.ENCRYPT_ON;

            // since we added one more line for prelogin option with fedauth,
            // we also needed to modify the offsets above, by adding 5 to each offset,
            // since the data session of each option is push 5 bytes behind.
            fedAuthOffset = 5;
        }
        else {
            messageLength = TDS.B_PRELOGIN_MESSAGE_LENGTH;
            fedAuthOffset = 0;
        }

        final byte[] preloginRequest = new byte[messageLength];

        int preloginRequestOffset = 0;

        byte[] bufferHeader = {
                // Buffer Header
                TDS.PKT_PRELOGIN, // Message Type
                TDS.STATUS_BIT_EOM, 0, messageLength, 0, 0,                 // SPID (not used)
                0,                    // Packet (not used)
                0,                    // Window (not used)
        };

        System.arraycopy(bufferHeader, 0, preloginRequest, preloginRequestOffset, bufferHeader.length);
        preloginRequestOffset = preloginRequestOffset + bufferHeader.length;

        byte[] preloginOptionsBeforeFedAuth = {
                // OPTION_TOKEN (BYTE), OFFSET (USHORT), LENGTH (USHORT)
                TDS.B_PRELOGIN_OPTION_VERSION, 0, (byte) (16 + fedAuthOffset), 0, 6, // UL_VERSION + US_SUBBUILD
                TDS.B_PRELOGIN_OPTION_ENCRYPTION, 0, (byte) (22 + fedAuthOffset), 0, 1, // B_FENCRYPTION
                TDS.B_PRELOGIN_OPTION_TRACEID, 0, (byte) (23 + fedAuthOffset), 0, 36, // ClientConnectionId + ActivityId
        };
        System.arraycopy(preloginOptionsBeforeFedAuth, 0, preloginRequest, preloginRequestOffset, preloginOptionsBeforeFedAuth.length);
        preloginRequestOffset = preloginRequestOffset + preloginOptionsBeforeFedAuth.length;

        if (fedAuthRequiredByUser) {
            byte[] preloginOptions2 = {TDS.B_PRELOGIN_OPTION_FEDAUTHREQUIRED, 0, 64, 0, 1,};
            System.arraycopy(preloginOptions2, 0, preloginRequest, preloginRequestOffset, preloginOptions2.length);
            preloginRequestOffset = preloginRequestOffset + preloginOptions2.length;
        }

        preloginRequest[preloginRequestOffset] = TDS.B_PRELOGIN_OPTION_TERMINATOR;
        preloginRequestOffset++;

        // PL_OPTION_DATA
        byte[] preloginOptionData = {
                // - Server version -
                // (out param, filled in by the server in the prelogin response).
                0, 0, 0, 0, 0, 0,

                // - Encryption -
                requestedEncryptionLevel,

                // TRACEID Data Session (ClientConnectionId + ActivityId) - Initialize to 0
                0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,};
        System.arraycopy(preloginOptionData, 0, preloginRequest, preloginRequestOffset, preloginOptionData.length);
        preloginRequestOffset = preloginRequestOffset + preloginOptionData.length;

        // If the client’s PRELOGIN request message contains the FEDAUTHREQUIRED option,
        // the client MUST specify 0x01 as the B_FEDAUTHREQUIRED value
        if (fedAuthRequiredByUser) {
            preloginRequest[preloginRequestOffset] = 1;
            preloginRequestOffset = preloginRequestOffset + 1;
        }

        final byte[] preloginResponse = new byte[TDS.INITIAL_PACKET_SIZE];
        String preloginErrorLogString = " Prelogin error: host " + serverName + " port " + portNumber;

        ActivityId activityId = ActivityCorrelator.getNext();
        final byte[] actIdByteArray = Util.asGuidByteArray(activityId.getId());
        final byte[] conIdByteArray = Util.asGuidByteArray(clientConnectionId);

        int offset;

        if (fedAuthRequiredByUser) {
            offset = preloginRequest.length - 36 - 1; // point to the TRACEID Data Session (one more byte for fedauth data session)
        }
        else {
            offset = preloginRequest.length - 36; // point to the TRACEID Data Session
        }

        // copy ClientConnectionId
        System.arraycopy(conIdByteArray, 0, preloginRequest, offset, conIdByteArray.length);
        offset += conIdByteArray.length;

        // copy ActivityId
        System.arraycopy(actIdByteArray, 0, preloginRequest, offset, actIdByteArray.length);
        offset += actIdByteArray.length;

        long seqNum = activityId.getSequence();
        Util.writeInt((int) seqNum, preloginRequest, offset);
        offset += 4;

        if (connectionlogger.isLoggable(Level.FINER)) {
            connectionlogger.finer(toString() + " Requesting encryption level:" + TDS.getEncryptionLevel(requestedEncryptionLevel));
            connectionlogger.finer(toString() + " ActivityId " + activityId.toString());
        }

        // Write the entire prelogin request
        if (tdsChannel.isLoggingPackets())
            tdsChannel.logPacket(preloginRequest, 0, preloginRequest.length, toString() + " Prelogin request");

        try {
            tdsChannel.write(preloginRequest, 0, preloginRequest.length);
            tdsChannel.flush();
        }
        catch (SQLServerException e) {
            connectionlogger.warning(toString() + preloginErrorLogString + " Error sending prelogin request: " + e.getMessage());
            throw e;
        }

        ActivityCorrelator.setCurrentActivityIdSentFlag();  // indicate current ActivityId is sent

        // Read the entire prelogin response
        int responseLength = preloginResponse.length;
        int responseBytesRead = 0;
        boolean processedResponseHeader = false;
        while (responseBytesRead < responseLength) {
            int bytesRead;

            try {
                bytesRead = tdsChannel.read(preloginResponse, responseBytesRead, responseLength - responseBytesRead);
            }
            catch (SQLServerException e) {
                connectionlogger.warning(toString() + preloginErrorLogString + " Error reading prelogin response: " + e.getMessage());
                throw e;
            }

            // If we reached EOF before the end of the prelogin response then something is wrong.
            //
            // Special case: If there was no response at all (i.e. the server closed the connection),
            // then maybe we are just trying to talk to an older server that doesn't support prelogin
            // (and that we don't support with this driver).
            if (-1 == bytesRead) {
                if (connectionlogger.isLoggable(Level.WARNING)) {
                    connectionlogger.warning(
                            toString() + preloginErrorLogString + " Unexpected end of prelogin response after " + responseBytesRead + " bytes read");
                }
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_tcpipConnectionFailed"));
                Object[] msgArgs = {serverName, Integer.toString(portNumber), SQLServerException.getErrString("R_notSQLServer")};
                terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, form.format(msgArgs));
            }

            // Otherwise, we must have read some bytes...
            assert bytesRead >= 0;
            assert bytesRead <= responseLength - responseBytesRead;

            if (tdsChannel.isLoggingPackets())
                tdsChannel.logPacket(preloginResponse, responseBytesRead, bytesRead, toString() + " Prelogin response");

            responseBytesRead += bytesRead;

            // Validate the response header if we haven't already done so and
            // we've read enough of the response to do it.
            if (!processedResponseHeader && responseBytesRead >= TDS.PACKET_HEADER_SIZE) {
                // Verify that the response is actually a response...
                if (TDS.PKT_REPLY != preloginResponse[0]) {
                    if (connectionlogger.isLoggable(Level.WARNING)) {
                        connectionlogger.warning(toString() + preloginErrorLogString + " Unexpected response type:" + preloginResponse[0]);
                    }
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_tcpipConnectionFailed"));
                    Object[] msgArgs = {serverName, Integer.toString(portNumber), SQLServerException.getErrString("R_notSQLServer")};
                    terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, form.format(msgArgs));
                }

                // Verify that the response claims to only be one TDS packet long.
                // In theory, it can be longer, but in current practice it isn't, as all of the
                // prelogin response items easily fit into a single 4K packet.
                if (TDS.STATUS_BIT_EOM != (TDS.STATUS_BIT_EOM & preloginResponse[1])) {
                    if (connectionlogger.isLoggable(Level.WARNING)) {
                        connectionlogger.warning(toString() + preloginErrorLogString + " Unexpected response status:" + preloginResponse[1]);
                    }
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_tcpipConnectionFailed"));
                    Object[] msgArgs = {serverName, Integer.toString(portNumber), SQLServerException.getErrString("R_notSQLServer")};
                    terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, form.format(msgArgs));
                }

                // Verify that the length of the response claims to be small enough to fit in the allocated area
                responseLength = Util.readUnsignedShortBigEndian(preloginResponse, 2);
                assert responseLength >= 0;

                if (responseLength >= preloginResponse.length) {
                    if (connectionlogger.isLoggable(Level.WARNING)) {
                        connectionlogger.warning(toString() + preloginErrorLogString + " Response length:" + responseLength
                                + " is greater than allowed length:" + preloginResponse.length);
                    }
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_tcpipConnectionFailed"));
                    Object[] msgArgs = {serverName, Integer.toString(portNumber), SQLServerException.getErrString("R_notSQLServer")};
                    terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, form.format(msgArgs));
                }

                processedResponseHeader = true;
            }
        }

        // Walk the response for prelogin options received. We expect at least to get
        // back the server version and the encryption level.
        boolean receivedVersionOption = false;
        negotiatedEncryptionLevel = TDS.ENCRYPT_INVALID;

        int responseIndex = TDS.PACKET_HEADER_SIZE;
        while (true) {
            // Get the option token
            if (responseIndex >= responseLength) {
                if (connectionlogger.isLoggable(Level.WARNING)) {
                    connectionlogger.warning(toString() + " Option token not found");
                }
                throwInvalidTDS();
            }
            byte optionToken = preloginResponse[responseIndex++];

            // When we reach the option terminator, we're done processing option tokens
            if (TDS.B_PRELOGIN_OPTION_TERMINATOR == optionToken)
                break;

            // Get the offset and length that follows the option token
            if (responseIndex + 4 >= responseLength) {
                if (connectionlogger.isLoggable(Level.WARNING)) {
                    connectionlogger.warning(toString() + " Offset/Length not found for option:" + optionToken);
                }
                throwInvalidTDS();
            }

            int optionOffset = Util.readUnsignedShortBigEndian(preloginResponse, responseIndex) + TDS.PACKET_HEADER_SIZE;
            responseIndex += 2;
            assert optionOffset >= 0;

            int optionLength = Util.readUnsignedShortBigEndian(preloginResponse, responseIndex);
            responseIndex += 2;
            assert optionLength >= 0;

            if (optionOffset + optionLength > responseLength) {
                if (connectionlogger.isLoggable(Level.WARNING)) {
                    connectionlogger.warning(
                            toString() + " Offset:" + optionOffset + " and length:" + optionLength + " exceed response length:" + responseLength);
                }
                throwInvalidTDS();
            }

            switch (optionToken) {
                case TDS.B_PRELOGIN_OPTION_VERSION:
                    if (receivedVersionOption) {
                        if (connectionlogger.isLoggable(Level.WARNING)) {
                            connectionlogger.warning(toString() + " Version option already received");
                        }
                        throwInvalidTDS();
                    }

                    if (6 != optionLength) {
                        if (connectionlogger.isLoggable(Level.WARNING)) {
                            connectionlogger.warning(toString() + " Version option length:" + optionLength + " is incorrect.  Correct value is 6.");
                        }
                        throwInvalidTDS();
                    }

                    serverMajorVersion = preloginResponse[optionOffset];
                    if (serverMajorVersion < 9) {
                        if (connectionlogger.isLoggable(Level.WARNING)) {
                            connectionlogger
                                    .warning(toString() + " Server major version:" + serverMajorVersion + " is not supported by this driver.");
                        }
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unsupportedServerVersion"));
                        Object[] msgArgs = {Integer.toString(preloginResponse[optionOffset])};
                        terminate(SQLServerException.DRIVER_ERROR_UNSUPPORTED_CONFIG, form.format(msgArgs));
                    }

                    if (connectionlogger.isLoggable(Level.FINE))
                        connectionlogger.fine(toString() + " Server returned major version:" + preloginResponse[optionOffset]);

                    receivedVersionOption = true;
                    break;

                case TDS.B_PRELOGIN_OPTION_ENCRYPTION:
                    if (TDS.ENCRYPT_INVALID != negotiatedEncryptionLevel) {
                        if (connectionlogger.isLoggable(Level.WARNING)) {
                            connectionlogger.warning(toString() + " Encryption option already received");
                        }
                        throwInvalidTDS();
                    }

                    if (1 != optionLength) {
                        if (connectionlogger.isLoggable(Level.WARNING)) {
                            connectionlogger
                                    .warning(toString() + " Encryption option length:" + optionLength + " is incorrect.  Correct value is 1.");
                        }
                        throwInvalidTDS();
                    }

                    negotiatedEncryptionLevel = preloginResponse[optionOffset];

                    // If the server did not return a valid encryption level, terminate the connection.
                    if (TDS.ENCRYPT_OFF != negotiatedEncryptionLevel && TDS.ENCRYPT_ON != negotiatedEncryptionLevel
                            && TDS.ENCRYPT_REQ != negotiatedEncryptionLevel && TDS.ENCRYPT_NOT_SUP != negotiatedEncryptionLevel) {
                        if (connectionlogger.isLoggable(Level.WARNING)) {
                            connectionlogger.warning(toString() + " Server returned " + TDS.getEncryptionLevel(negotiatedEncryptionLevel));
                        }
                        throwInvalidTDS();
                    }

                    if (connectionlogger.isLoggable(Level.FINER))
                        connectionlogger.finer(toString() + " Negotiated encryption level:" + TDS.getEncryptionLevel(negotiatedEncryptionLevel));

                    // If we requested SSL encryption and the server does not support it, then terminate the connection.
                    if (TDS.ENCRYPT_ON == requestedEncryptionLevel && TDS.ENCRYPT_ON != negotiatedEncryptionLevel
                            && TDS.ENCRYPT_REQ != negotiatedEncryptionLevel) {
                        terminate(SQLServerException.DRIVER_ERROR_SSL_FAILED, SQLServerException.getErrString("R_sslRequiredNoServerSupport"));
                    }

                    // If we say we don't support SSL and the server doesn't accept unencrypted connections,
                    // then terminate the connection.
                    if (TDS.ENCRYPT_NOT_SUP == requestedEncryptionLevel && TDS.ENCRYPT_NOT_SUP != negotiatedEncryptionLevel) {
                        // If the server required an encrypted connection then terminate with an appropriate error.
                        if (TDS.ENCRYPT_REQ == negotiatedEncryptionLevel)
                            terminate(SQLServerException.DRIVER_ERROR_SSL_FAILED, SQLServerException.getErrString("R_sslRequiredByServer"));

                        if (connectionlogger.isLoggable(Level.WARNING)) {
                            connectionlogger
                                    .warning(toString() + " Client requested encryption level: " + TDS.getEncryptionLevel(requestedEncryptionLevel)
                                            + " Server returned unexpected encryption level: " + TDS.getEncryptionLevel(negotiatedEncryptionLevel));
                        }
                        throwInvalidTDS();
                    }
                    break;

                case TDS.B_PRELOGIN_OPTION_FEDAUTHREQUIRED:
                    // Only 0x00 and 0x01 are accepted values from the server.
                    if (0 != preloginResponse[optionOffset] && 1 != preloginResponse[optionOffset]) {
                        if (connectionlogger.isLoggable(Level.SEVERE)) {
                            connectionlogger.severe(toString() + " Server sent an unexpected value for FedAuthRequired PreLogin Option. Value was "
                                    + preloginResponse[optionOffset]);
                        }
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_FedAuthRequiredPreLoginResponseInvalidValue"));
                        throw new SQLServerException(form.format(new Object[] {preloginResponse[optionOffset]}), null);
                    }

                    // We must NOT use the response for the FEDAUTHREQUIRED PreLogin option, if the connection string option
                    // was not using the new Authentication keyword or in other words, if Authentication=NotSpecified
                    // Or AccessToken is not null, mean token based authentication is used.
                    if (((null != authenticationString) && (!authenticationString.equalsIgnoreCase(SqlAuthentication.NotSpecified.toString())))
                            || (null != accessTokenInByte)) {
                        fedAuthRequiredPreLoginResponse = (preloginResponse[optionOffset] == 1);
                    }
                    break;

                default:
                    if (connectionlogger.isLoggable(Level.FINER))
                        connectionlogger.finer(toString() + " Ignoring prelogin response option:" + optionToken);
                    break;
            }
        }

        if (!receivedVersionOption || TDS.ENCRYPT_INVALID == negotiatedEncryptionLevel) {
            if (connectionlogger.isLoggable(Level.WARNING)) {
                connectionlogger.warning(toString() + " Prelogin response is missing version and/or encryption option.");
            }
            throwInvalidTDS();
        }
    }

    final void throwInvalidTDS() throws SQLServerException {
        terminate(SQLServerException.DRIVER_ERROR_INVALID_TDS, SQLServerException.getErrString("R_invalidTDS"));
    }

    final void throwInvalidTDSToken(String tokenName) throws SQLServerException {
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unexpectedToken"));
        Object[] msgArgs = {tokenName};
        String message = SQLServerException.getErrString("R_invalidTDS") + form.format(msgArgs);
        terminate(SQLServerException.DRIVER_ERROR_INVALID_TDS, message);
    }

    /**
     * Terminates the connection and throws an exception detailing the reason for termination.
     *
     * This method is similar to SQLServerException.makeFromDriverError, except that it always terminates the connection, and does so with the
     * appropriate state code.
     */
    final void terminate(int driverErrorCode,
            String message) throws SQLServerException {
        terminate(driverErrorCode, message, null);
    }

    final void terminate(int driverErrorCode,
            String message,
            Throwable throwable) throws SQLServerException {
        String state = this.state.equals(State.Opened) ? SQLServerException.EXCEPTION_XOPEN_CONNECTION_FAILURE
                : SQLServerException.EXCEPTION_XOPEN_CONNECTION_CANT_ESTABLISH;

        if (!xopenStates)
            state = SQLServerException.mapFromXopen(state);

        SQLServerException ex = new SQLServerException(this, SQLServerException.checkAndAppendClientConnId(message, this), state,  // X/Open or SQL99
                                                                                                                                   // SQLState
                0,      // database error number (0 -> driver error)
                true);  // include stack trace in log

        if (null != throwable)
            ex.initCause(throwable);

        ex.setDriverErrorCode(driverErrorCode);

        notifyPooledConnection(ex);

        close();

        throw ex;
    }

    private final Object schedulerLock = new Object();

    /**
     * Executes a command through the scheduler.
     *
     * @param newCommand
     *            the command to execute
     */
    boolean executeCommand(TDSCommand newCommand) throws SQLServerException {
        synchronized (schedulerLock) {
            // Detach (buffer) the response from any previously executing
            // command so that we can execute the new command.
            //
            // Note that detaching the response does not process it. Detaching just
            // buffers the response off of the wire to clear the TDS channel.
            if (null != currentCommand) {
                currentCommand.detach();
                currentCommand = null;
            }

            // The implementation of this scheduler is pretty simple...
            // Since only one command at a time may use a connection
            // (to avoid TDS protocol errors), just synchronize to
            // serialize command execution.
            boolean commandComplete = false;
            try {
                commandComplete = newCommand.execute(tdsChannel.getWriter(), tdsChannel.getReader(newCommand));
            }
            finally {
                // We should never displace an existing currentCommand
                // assert null == currentCommand;

                // If execution of the new command left response bytes on the wire
                // (e.g. a large ResultSet or complex response with multiple results)
                // then remember it as the current command so that any subsequent call
                // to executeCommand will detach it before executing another new command.
                if (!commandComplete && !isSessionUnAvailable())
                    currentCommand = newCommand;
            }

            return commandComplete;
        }
    }

    void resetCurrentCommand() throws SQLServerException {
        if (null != currentCommand) {
            currentCommand.detach();
            currentCommand = null;
        }
    }

    /*
     * Executes a connection-level command
     */
    private void connectionCommand(String sql,
            String logContext) throws SQLServerException {
        final class ConnectionCommand extends UninterruptableTDSCommand {
            final String sql;

            ConnectionCommand(String sql,
                    String logContext) {
                super(logContext);
                this.sql = sql;
            }

            final boolean doExecute() throws SQLServerException {
                startRequest(TDS.PKT_QUERY).writeString(sql);
                TDSParser.parse(startResponse(), getLogContext());
                return true;
            }
        }

        executeCommand(new ConnectionCommand(sql, logContext));
    }

    /**
     * Build the syntax to initialize the connection at the database side.
     * 
     * @return the syntax string
     */
    /* L0 */ private String sqlStatementToInitialize() {
        String s = null;
        if (nLockTimeout > -1)
            s = " set lock_timeout " + nLockTimeout;
        return s;
    }

    /**
     * Return the syntax to set the database calatog to use.
     * 
     * @param sDB
     *            the new catalog
     * @return the required syntax
     */
    /* L0 */ void setCatalogName(String sDB) {
        if (sDB != null) {
            if (sDB.length() > 0) {
                sCatalog = sDB;
            }
        }
    }

    /**
     * Return the syntax to set the database isolation level.
     * 
     * @return the required syntax
     */
    /* L0 */ String sqlStatementToSetTransactionIsolationLevel() throws SQLServerException {
        String sql = "set transaction isolation level ";

        switch (transactionIsolationLevel) {
            case Connection.TRANSACTION_READ_UNCOMMITTED: {
                sql = sql + " read uncommitted ";
                break;
            }
            case Connection.TRANSACTION_READ_COMMITTED: {
                sql = sql + " read committed ";
                break;
            }
            case Connection.TRANSACTION_REPEATABLE_READ: {
                sql = sql + " repeatable read ";
                break;
            }
            case Connection.TRANSACTION_SERIALIZABLE: {
                sql = sql + " serializable ";
                break;
            }
            case SQLServerConnection.TRANSACTION_SNAPSHOT: {
                sql = sql + " snapshot ";
                break;
            }
            default: {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidTransactionLevel"));
                Object[] msgArgs = {Integer.toString(transactionIsolationLevel)};
                SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
            }
        }
        return sql;
    }

    /**
     * Return the syntax to set the database commit mode.
     * 
     * @return the required syntax
     */
    static String sqlStatementToSetCommit(boolean autoCommit) {
        return (true == autoCommit) ? "set implicit_transactions off " : "set implicit_transactions on ";
    }

    /* L0 */ public Statement createStatement() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "createStatement");
        Statement st = createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        loggerExternal.exiting(getClassNameLogging(), "createStatement", st);
        return st;
    }

    /* L0 */ public PreparedStatement prepareStatement(String sql) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "prepareStatement", sql);
        PreparedStatement pst = prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        loggerExternal.exiting(getClassNameLogging(), "prepareStatement", pst);
        return pst;
    }

    /* L0 */ public CallableStatement prepareCall(String sql) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "prepareCall", sql);
        CallableStatement st = prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        loggerExternal.exiting(getClassNameLogging(), "prepareCall", st);
        return st;
    }

    /* L0 */ public String nativeSQL(String sql) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "nativeSQL", sql);
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "nativeSQL", sql);
        return sql;
    }

    public void setAutoCommit(boolean newAutoCommitMode) throws SQLServerException {
        if (loggerExternal.isLoggable(Level.FINER)) {
            loggerExternal.entering(getClassNameLogging(), "setAutoCommit", newAutoCommitMode);
            if (Util.IsActivityTraceOn())
                loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        String commitPendingTransaction = "";
        checkClosed();

        if (newAutoCommitMode == databaseAutoCommitMode) // No Change
            return;

        // When changing to auto-commit from inside an existing transaction,
        // commit that transaction first.
        if (newAutoCommitMode == true)
            commitPendingTransaction = "IF @@TRANCOUNT > 0 COMMIT TRAN ";

        if (connectionlogger.isLoggable(Level.FINER)) {
            connectionlogger.finer(toString() + " Autocommitmode current :" + databaseAutoCommitMode + " new: " + newAutoCommitMode);
        }

        rolledBackTransaction = false;
        connectionCommand(commitPendingTransaction + sqlStatementToSetCommit(newAutoCommitMode), "setAutoCommit");
        databaseAutoCommitMode = newAutoCommitMode;
        loggerExternal.exiting(getClassNameLogging(), "setAutoCommit");
    }

    /* L0 */ public boolean getAutoCommit() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getAutoCommit");
        checkClosed();
        boolean res = !inXATransaction && databaseAutoCommitMode;
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.exiting(getClassNameLogging(), "getAutoCommit", res);
        return res;
    }

    /* LO */ final byte[] getTransactionDescriptor() {
        return transactionDescriptor;
    }

    /**
     * Commit a transcation. Per our transaction spec, see also SDT#410729, a commit in autocommit mode = true is a NO-OP.
     *
     * @throws SQLServerException
     *             if no transaction exists.
     */
    public void commit() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "commit");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }

        checkClosed();
        if (!databaseAutoCommitMode)
            connectionCommand("IF @@TRANCOUNT > 0 COMMIT TRAN", "Connection.commit");
        loggerExternal.exiting(getClassNameLogging(), "commit");
    }

    /**
     * Rollback a transcation.
     *
     * @throws SQLServerException
     *             if no transaction exists or if the connection is in auto-commit mode.
     */
    public void rollback() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "rollback");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();

        if (databaseAutoCommitMode) {
            SQLServerException.makeFromDriverError(this, this, SQLServerException.getErrString("R_cantInvokeRollback"), null, true);
        }
        else
            connectionCommand("IF @@TRANCOUNT > 0 ROLLBACK TRAN", "Connection.rollback");
        loggerExternal.exiting(getClassNameLogging(), "rollback");
    }

    public void abort(Executor executor) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "abort", executor);

        // nop if connection is closed
        if (isClosed())
            return;

        if (null == executor) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidArgument"));
            Object[] msgArgs = {"executor"};
            SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, false);
        }

        // check for callAbort permission
        SecurityManager secMgr = System.getSecurityManager();
        if (secMgr != null) {
            try {
                SQLPermission perm = new SQLPermission(callAbortPerm);
                secMgr.checkPermission(perm);
            }
            catch (SecurityException ex) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_permissionDenied"));
                Object[] msgArgs = {callAbortPerm};
                SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, true);
            }
        }

        setState(State.Closed);

        executor.execute(new Runnable() {
            public void run() {
                if (null != tdsChannel) {
                    tdsChannel.close();
                }
            }
        });

        loggerExternal.exiting(getClassNameLogging(), "abort");
    }

    public void close() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "close");

        // Always report the connection as closed for any further use, no matter
        // what happens when we try to clean up the physical resources associated
        // with the connection.
        setState(State.Closed);

        // Close the TDS channel. When the channel is closed, the server automatically
        // rolls back any pending transactions and closes associated resources like
        // prepared handles.
        if (null != tdsChannel) {
            tdsChannel.close();
        }

        // Invalidate statement caches.
        if (null != preparedStatementHandleCache)
            preparedStatementHandleCache.clear();

        if (null != parameterMetadataCache)
            parameterMetadataCache.clear();

        // Clean-up queue etc. related to batching of prepared statement discard actions (sp_unprepare).
        cleanupPreparedStatementDiscardActions();
        
        ActivityCorrelator.cleanupActivityId();

        loggerExternal.exiting(getClassNameLogging(), "close");
    }

    // This function is used by the proxy for notifying the pool manager that this connection proxy is closed
    // This event will pool the connection
    final void poolCloseEventNotify() throws SQLServerException {
        if (state.equals(State.Opened) && null != pooledConnectionParent) {
            // autocommit = true => nothing to do when app closes connection
            // XA = true => the transaction manager is the only one who can invoke transactional APIs

            // Non XA and autocommit off =>
            // If there is a pending BEGIN TRAN from the last commit or rollback, dont propagate it to
            // the next allocated connection.
            // Also if the app closes a connection handle before committing or rolling back the uncompleted
            // transaction may lock other updates/queries so close the transaction now.
            if (!databaseAutoCommitMode && !(pooledConnectionParent instanceof XAConnection)) {
                connectionCommand("IF @@TRANCOUNT > 0 ROLLBACK TRAN" /* +close connection */, "close connection");
            }
            notifyPooledConnection(null);
            ActivityCorrelator.cleanupActivityId();
            if (connectionlogger.isLoggable(Level.FINER)) {
                connectionlogger.finer(toString() + " Connection closed and returned to connection pool");
            }
        }

    }

    /* L0 */ public boolean isClosed() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "isClosed");
        loggerExternal.exiting(getClassNameLogging(), "isClosed", isSessionUnAvailable());
        return isSessionUnAvailable();
    }

    /* L0 */ public DatabaseMetaData getMetaData() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getMetaData");
        checkClosed();
        if (databaseMetaData == null) {
            databaseMetaData = new SQLServerDatabaseMetaData(this);
        }
        loggerExternal.exiting(getClassNameLogging(), "getMetaData", databaseMetaData);
        return databaseMetaData;
    }

    /* L0 */ public void setReadOnly(boolean readOnly) throws SQLServerException {
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "setReadOnly", readOnly);
        checkClosed();
        // do nothing per spec
        loggerExternal.exiting(getClassNameLogging(), "setReadOnly");
    }

    /* L0 */ public boolean isReadOnly() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "isReadOnly");
        checkClosed();
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.exiting(getClassNameLogging(), "isReadOnly", Boolean.FALSE);
        return false;
    }

    /* L0 */ public void setCatalog(String catalog) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "setCatalog", catalog);
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        if (catalog != null) {
            connectionCommand("use " + Util.escapeSQLId(catalog), "setCatalog");
            sCatalog = catalog;
        }
        loggerExternal.exiting(getClassNameLogging(), "setCatalog");
    }

    /* L0 */ public String getCatalog() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getCatalog");
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "getCatalog", sCatalog);
        return sCatalog;
    }

    String getSCatalog() throws SQLServerException {
        return sCatalog;
    }

    /* L0 */ public void setTransactionIsolation(int level) throws SQLServerException {
        if (loggerExternal.isLoggable(Level.FINER)) {
            loggerExternal.entering(getClassNameLogging(), "setTransactionIsolation", level);
            if (Util.IsActivityTraceOn()) {
                loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
            }
        }

        checkClosed();
        if (level == Connection.TRANSACTION_NONE)
            return;
        String sql;
        transactionIsolationLevel = level;
        sql = sqlStatementToSetTransactionIsolationLevel();
        connectionCommand(sql, "setTransactionIsolation");
        loggerExternal.exiting(getClassNameLogging(), "setTransactionIsolation");
    }

    /* L0 */ public int getTransactionIsolation() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getTransactionIsolation");
        checkClosed();
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.exiting(getClassNameLogging(), "getTransactionIsolation", transactionIsolationLevel);
        return transactionIsolationLevel;
    }

    volatile SQLWarning sqlWarnings; // the SQL warnings chain
    Object warningSynchronization = new Object();

    // Think about returning a copy when we implement additional warnings.
    /* L0 */ public SQLWarning getWarnings() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getWarnings");
        checkClosed();
        // check null warn wont crash
        loggerExternal.exiting(getClassNameLogging(), "getWarnings", sqlWarnings);
        return sqlWarnings;
    }

    // Any changes to SQLWarnings should be synchronized.
    private void addWarning(String warningString) {
        synchronized (warningSynchronization) {
            SQLWarning warning = new SQLWarning(warningString);

            if (null == sqlWarnings) {
                sqlWarnings = warning;
            }
            else {
                sqlWarnings.setNextWarning(warning);
            }
        }
    }

    /* L2 */ public void clearWarnings() throws SQLServerException {
        synchronized (warningSynchronization) {
            loggerExternal.entering(getClassNameLogging(), "clearWarnings");
            checkClosed();
            sqlWarnings = null;
            loggerExternal.exiting(getClassNameLogging(), "clearWarnings");
        }
    }

    // --------------------------JDBC 2.0-----------------------------
    public Statement createStatement(int resultSetType,
            int resultSetConcurrency) throws SQLServerException {
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "createStatement",
                    new Object[] {resultSetType, resultSetConcurrency});
        checkClosed();
        Statement st = new SQLServerStatement(this, resultSetType, resultSetConcurrency,
                SQLServerStatementColumnEncryptionSetting.UseConnectionSetting);
        loggerExternal.exiting(getClassNameLogging(), "createStatement", st);
        return st;
    }

    public PreparedStatement prepareStatement(String sql,
            int resultSetType,
            int resultSetConcurrency) throws SQLServerException {
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "prepareStatement",
                    new Object[] {sql, resultSetType, resultSetConcurrency});
        checkClosed();

        PreparedStatement st;
        
        // Make sure SQLServerPreparedStatement42 is used for 4.2 and above. 
        if (Util.use42Wrapper() || Util.use43Wrapper()) {
            st = new SQLServerPreparedStatement42(this, sql, resultSetType, resultSetConcurrency,
                    SQLServerStatementColumnEncryptionSetting.UseConnectionSetting);
        }
        else {
            st = new SQLServerPreparedStatement(this, sql, resultSetType, resultSetConcurrency,
                    SQLServerStatementColumnEncryptionSetting.UseConnectionSetting);
        }

        loggerExternal.exiting(getClassNameLogging(), "prepareStatement", st);
        return st;
    }

    private PreparedStatement prepareStatement(String sql,
            int resultSetType,
            int resultSetConcurrency,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "prepareStatement",
                    new Object[] {sql, resultSetType, resultSetConcurrency, stmtColEncSetting});
        checkClosed();

        PreparedStatement st;

        // Make sure SQLServerPreparedStatement42 is used for 4.2 and above. 
        if (Util.use42Wrapper() || Util.use43Wrapper()) {
            st = new SQLServerPreparedStatement42(this, sql, resultSetType, resultSetConcurrency, stmtColEncSetting);
        }
        else {
            st = new SQLServerPreparedStatement(this, sql, resultSetType, resultSetConcurrency, stmtColEncSetting);
        }

        loggerExternal.exiting(getClassNameLogging(), "prepareStatement", st);
        return st;
    }

    public CallableStatement prepareCall(String sql,
            int resultSetType,
            int resultSetConcurrency) throws SQLServerException {
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "prepareCall",
                    new Object[] {sql, resultSetType, resultSetConcurrency});
        checkClosed();

        CallableStatement st;

        // Make sure SQLServerCallableStatement42 is used for 4.2 and above. 
        if (Util.use42Wrapper() || Util.use43Wrapper()) {
            st = new SQLServerCallableStatement42(this, sql, resultSetType, resultSetConcurrency,
                    SQLServerStatementColumnEncryptionSetting.UseConnectionSetting);
        }
        else {
            st = new SQLServerCallableStatement(this, sql, resultSetType, resultSetConcurrency,
                    SQLServerStatementColumnEncryptionSetting.UseConnectionSetting);
        }

        loggerExternal.exiting(getClassNameLogging(), "prepareCall", st);
        return st;
    }

    /* L2 */ public void setTypeMap(java.util.Map<String, Class<?>> map) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "setTypeMap", map);
        checkClosed();
        if (map != null && (map instanceof java.util.HashMap)) {
            // we return an empty Hash map if the user gives this back make sure we accept it.
            if (map.isEmpty()) {
                loggerExternal.exiting(getClassNameLogging(), "setTypeMap");
                return;
            }

        }
        NotImplemented();
    }

    public java.util.Map<String, Class<?>> getTypeMap() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getTypeMap");
        checkClosed();
        java.util.Map<String, Class<?>> mp = new java.util.HashMap<>();
        loggerExternal.exiting(getClassNameLogging(), "getTypeMap", mp);
        return mp;
    }

    /* ---------------------- Logon --------------------------- */

    int writeAEFeatureRequest(boolean write,
            TDSWriter tdsWriter) throws SQLServerException /* if false just calculates the length */
    {
        // This includes the length of the terminator byte. If there are other extension features, re-adjust accordingly.
        int len = 6; // (1byte = featureID, 4bytes = featureData length, 1 bytes = Version)

        if (write) {
            tdsWriter.writeByte((byte) TDS.TDS_FEATURE_EXT_AE); // FEATUREEXT_TCE
            tdsWriter.writeInt(1);
            tdsWriter.writeByte((byte) TDS.MAX_SUPPORTED_TCE_VERSION);
        }
        return len;
    }

    int writeFedAuthFeatureRequest(boolean write,
            TDSWriter tdsWriter,
            FederatedAuthenticationFeatureExtensionData fedAuthFeatureExtensionData) throws SQLServerException { /*
                                                                                                                  * if false just calculates the
                                                                                                                  * length
                                                                                                                  */

        assert (fedAuthFeatureExtensionData.libraryType == TDS.TDS_FEDAUTH_LIBRARY_ADAL
                || fedAuthFeatureExtensionData.libraryType == TDS.TDS_FEDAUTH_LIBRARY_SECURITYTOKEN);

        int dataLen = 0;

        // set dataLen and totalLen
        switch (fedAuthFeatureExtensionData.libraryType) {
            case TDS.TDS_FEDAUTH_LIBRARY_ADAL:
                dataLen = 2;  // length of feature data = 1 byte for library and echo + 1 byte for workflow
                break;
            case TDS.TDS_FEDAUTH_LIBRARY_SECURITYTOKEN:
                assert null != fedAuthFeatureExtensionData.accessToken;
                dataLen = 1 + 4 + fedAuthFeatureExtensionData.accessToken.length; // length of feature data = 1 byte for library and echo, security
                                                                                  // token length and sizeof(int) for token lengh itself
                break;
            default:
                assert (false);	// Unrecognized library type for fedauth feature extension request"
                break;
        }

        int totalLen = dataLen + 5; // length of feature id (1 byte), data length field (4 bytes), and feature data (dataLen)

        // write feature id
        if (write) {
            tdsWriter.writeByte((byte) TDS.TDS_FEATURE_EXT_FEDAUTH); // FEATUREEXT_TCE

            // set options
            byte options = 0x00;

            // set upper 7 bits of options to indicate fed auth library type
            switch (fedAuthFeatureExtensionData.libraryType) {
                case TDS.TDS_FEDAUTH_LIBRARY_ADAL:
                    assert federatedAuthenticationInfoRequested == true;
                    options |= TDS.TDS_FEDAUTH_LIBRARY_ADAL << 1;
                    break;
                case TDS.TDS_FEDAUTH_LIBRARY_SECURITYTOKEN:
                    assert federatedAuthenticationRequested == true;
                    options |= TDS.TDS_FEDAUTH_LIBRARY_SECURITYTOKEN << 1;
                    break;
                default:
                    assert (false);	// Unrecognized library type for fedauth feature extension request
                    break;
            }

            options |= (byte) (fedAuthFeatureExtensionData.fedAuthRequiredPreLoginResponse == true ? 0x01 : 0x00);

            // write FeatureDataLen
            tdsWriter.writeInt(dataLen);

            // write FeatureData
            // write option
            tdsWriter.writeByte(options);

            // write workflow for FedAuthLibrary.ADAL
            // write accessToken for FedAuthLibrary.SecurityToken
            switch (fedAuthFeatureExtensionData.libraryType) {
                case TDS.TDS_FEDAUTH_LIBRARY_ADAL:
                    byte workflow = 0x00;
                    switch (fedAuthFeatureExtensionData.authentication) {
                        case ActiveDirectoryPassword:
                            workflow = TDS.ADALWORKFLOW_ACTIVEDIRECTORYPASSWORD;
                            break;
                        case ActiveDirectoryIntegrated:
                            workflow = TDS.ADALWORKFLOW_ACTIVEDIRECTORYINTEGRATED;
                            break;
                        default:
                            assert (false);	// Unrecognized Authentication type for fedauth ADAL request
                            break;
                    }

                    tdsWriter.writeByte(workflow);
                    break;
                case TDS.TDS_FEDAUTH_LIBRARY_SECURITYTOKEN:
                    tdsWriter.writeInt(fedAuthFeatureExtensionData.accessToken.length);
                    tdsWriter.writeBytes(fedAuthFeatureExtensionData.accessToken, 0, fedAuthFeatureExtensionData.accessToken.length);
                    break;
                default:
                    assert (false);	// Unrecognized FedAuthLibrary type for feature extension request
                    break;
            }
        }
        return totalLen;
    }

    private final class LogonCommand extends UninterruptableTDSCommand {
        LogonCommand() {
            super("logon");
        }

        final boolean doExecute() throws SQLServerException {
            logon(this);
            return true;
        }
    }

    /* L0 */ private void logon(LogonCommand command) throws SQLServerException {
        SSPIAuthentication authentication = null;
        if (integratedSecurity && AuthenticationScheme.nativeAuthentication == intAuthScheme)
            authentication = new AuthenticationJNI(this, currentConnectPlaceHolder.getServerName(), currentConnectPlaceHolder.getPortNumber());
        if (integratedSecurity && AuthenticationScheme.javaKerberos == intAuthScheme) {
            if (null != ImpersonatedUserCred)
                authentication = new KerbAuthentication(this, currentConnectPlaceHolder.getServerName(), currentConnectPlaceHolder.getPortNumber(),
                        ImpersonatedUserCred);
            else
                authentication = new KerbAuthentication(this, currentConnectPlaceHolder.getServerName(), currentConnectPlaceHolder.getPortNumber());
        }

        // If the workflow being used is Active Directory Password or Active Directory Integrated and server's prelogin response
        // for FEDAUTHREQUIRED option indicates Federated Authentication is required, we have to insert FedAuth Feature Extension
        // in Login7, indicating the intent to use Active Directory Authentication Library for SQL Server.
        if (authenticationString.trim().equalsIgnoreCase(SqlAuthentication.ActiveDirectoryPassword.toString())
                || (authenticationString.trim().equalsIgnoreCase(SqlAuthentication.ActiveDirectoryIntegrated.toString())
                        && fedAuthRequiredPreLoginResponse)) {
            federatedAuthenticationInfoRequested = true;
            fedAuthFeatureExtensionData = new FederatedAuthenticationFeatureExtensionData(TDS.TDS_FEDAUTH_LIBRARY_ADAL, authenticationString,
                    fedAuthRequiredPreLoginResponse);
        }

        if (null != accessTokenInByte) {
            fedAuthFeatureExtensionData = new FederatedAuthenticationFeatureExtensionData(TDS.TDS_FEDAUTH_LIBRARY_SECURITYTOKEN,
                    fedAuthRequiredPreLoginResponse, accessTokenInByte);
            // No need any further info from the server for token based authentication. So set _federatedAuthenticationRequested to true
            federatedAuthenticationRequested = true;
        }

        try {
            sendLogon(command, authentication, fedAuthFeatureExtensionData);

            // If we got routed in the current attempt,
            // the server closes the connection. So, we should not
            // be sending anymore commands to the server in that case.
            if (!isRoutedInCurrentAttempt) {
                originalCatalog = sCatalog;
                String sqlStmt = sqlStatementToInitialize();
                if (sqlStmt != null) {
                    connectionCommand(sqlStmt, "Change Settings");
                }
            }
        }
        finally {
            if (integratedSecurity) {
                if (null != authentication)
                    authentication.ReleaseClientContext();
                authentication = null;
                
                if (null != ImpersonatedUserCred) {
                    try {
                        ImpersonatedUserCred.dispose();
                    }
                    catch (GSSException e) {
                        if (connectionlogger.isLoggable(Level.FINER))
                            connectionlogger.finer(toString() + " Release of the credentials failed GSSException: " + e);
                    }
                }
            }
        }
    }

    private static final int ENVCHANGE_DATABASE = 1;
    private static final int ENVCHANGE_LANGUAGE = 2;
    private static final int ENVCHANGE_CHARSET = 3;
    private static final int ENVCHANGE_PACKETSIZE = 4;
    private static final int ENVCHANGE_SORTLOCALEID = 5;
    private static final int ENVCHANGE_SORTFLAGS = 6;
    private static final int ENVCHANGE_SQLCOLLATION = 7;
    private static final int ENVCHANGE_XACT_BEGIN = 8;
    private static final int ENVCHANGE_XACT_COMMIT = 9;
    private static final int ENVCHANGE_XACT_ROLLBACK = 10;
    private static final int ENVCHANGE_DTC_ENLIST = 11;
    private static final int ENVCHANGE_DTC_DEFECT = 12;
    private static final int ENVCHANGE_CHANGE_MIRROR = 13;
    private static final int ENVCHANGE_UNUSED_14 = 14;
    private static final int ENVCHANGE_DTC_PROMOTE = 15;
    private static final int ENVCHANGE_DTC_MGR_ADDR = 16;
    private static final int ENVCHANGE_XACT_ENDED = 17;
    private static final int ENVCHANGE_RESET_COMPLETE = 18;
    private static final int ENVCHANGE_USER_INFO = 19;
    private static final int ENVCHANGE_ROUTING = 20;

    final void processEnvChange(TDSReader tdsReader) throws SQLServerException {
        tdsReader.readUnsignedByte(); // token type
        final int envValueLength = tdsReader.readUnsignedShort();

        TDSReaderMark mark = tdsReader.mark();
        int envchange = tdsReader.readUnsignedByte();
        switch (envchange) {
            case ENVCHANGE_PACKETSIZE:
                // Set NEW value as new TDS packet size
                try {
                    tdsPacketSize = Integer.parseInt(tdsReader.readUnicodeString(tdsReader.readUnsignedByte()));
                }
                catch (NumberFormatException e) {
                    tdsReader.throwInvalidTDS();
                }
                if (connectionlogger.isLoggable(Level.FINER))
                    connectionlogger.finer(toString() + " Network packet size is " + tdsPacketSize + " bytes");
                break;

            case ENVCHANGE_SQLCOLLATION:
                if (SQLCollation.tdsLength() != tdsReader.readUnsignedByte())
                    tdsReader.throwInvalidTDS();

                try {
                    databaseCollation = new SQLCollation(tdsReader);
                }
                catch (UnsupportedEncodingException e) {
                    terminate(SQLServerException.DRIVER_ERROR_INVALID_TDS, e.getMessage(), e);
                }

                break;

            case ENVCHANGE_DTC_ENLIST:
            case ENVCHANGE_XACT_BEGIN:
                rolledBackTransaction = false;
                byte[] transactionDescriptor = getTransactionDescriptor();

                if (transactionDescriptor.length != tdsReader.readUnsignedByte())
                    tdsReader.throwInvalidTDS();

                tdsReader.readBytes(transactionDescriptor, 0, transactionDescriptor.length);

                if (connectionlogger.isLoggable(Level.FINER)) {
                    String op;
                    if (ENVCHANGE_XACT_BEGIN == envchange)
                        op = " started";
                    else
                        op = " enlisted";

                    connectionlogger.finer(toString() + op);
                }
                break;

            case ENVCHANGE_XACT_ROLLBACK:
                rolledBackTransaction = true;

                if (inXATransaction) {
                    if (connectionlogger.isLoggable(Level.FINER))
                        connectionlogger.finer(toString() + " rolled back. (DTC)");

                    // Do not clear the transaction descriptor if the connection is in DT.
                    // For a DTC transaction, a ENV_ROLLBACKTRAN token won't cleanup the xactID previously cached on the connection
                    // because user is required to explicitly un-enlist/defect a connection from a DTC.
                    // A ENV_DEFECTTRAN token though will clean the DTC xactID on the connection.
                }
                else {
                    if (connectionlogger.isLoggable(Level.FINER))
                        connectionlogger.finer(toString() + " rolled back");

                    Arrays.fill(getTransactionDescriptor(), (byte) 0);
                }

                break;

            case ENVCHANGE_XACT_COMMIT:
                if (connectionlogger.isLoggable(Level.FINER))
                    connectionlogger.finer(toString() + " committed");

                Arrays.fill(getTransactionDescriptor(), (byte) 0);

                break;

            case ENVCHANGE_DTC_DEFECT:
                if (connectionlogger.isLoggable(Level.FINER))
                    connectionlogger.finer(toString() + " defected");

                Arrays.fill(getTransactionDescriptor(), (byte) 0);

                break;

            case ENVCHANGE_DATABASE:
                setCatalogName(tdsReader.readUnicodeString(tdsReader.readUnsignedByte()));
                break;

            case ENVCHANGE_CHANGE_MIRROR:
                setFailoverPartnerServerProvided(tdsReader.readUnicodeString(tdsReader.readUnsignedByte()));
                break;
            // Skip unsupported, ENVCHANGES
            case ENVCHANGE_LANGUAGE:
            case ENVCHANGE_CHARSET:
            case ENVCHANGE_SORTLOCALEID:
            case ENVCHANGE_SORTFLAGS:
            case ENVCHANGE_DTC_PROMOTE:
            case ENVCHANGE_DTC_MGR_ADDR:
            case ENVCHANGE_XACT_ENDED:
            case ENVCHANGE_RESET_COMPLETE:
            case ENVCHANGE_USER_INFO:
                if (connectionlogger.isLoggable(Level.FINER))
                    connectionlogger.finer(toString() + " Ignored env change: " + envchange);
                break;
            case ENVCHANGE_ROUTING:

                // initialize to invalid values
                int routingDataValueLength, routingProtocol, routingPortNumber, routingServerNameLength;
                routingDataValueLength = routingProtocol = routingPortNumber = routingServerNameLength = -1;

                String routingServerName = null;

                try {
                    routingDataValueLength = tdsReader.readUnsignedShort();
                    if (routingDataValueLength <= 5)// (5 is the no of bytes in protocol + port number+ length field of server name)
                    {
                        throwInvalidTDS();
                    }

                    routingProtocol = tdsReader.readUnsignedByte();
                    if (routingProtocol != 0) {
                        throwInvalidTDS();
                    }

                    routingPortNumber = tdsReader.readUnsignedShort();
                    if (routingPortNumber <= 0 || routingPortNumber > 65535) {
                        throwInvalidTDS();
                    }

                    routingServerNameLength = tdsReader.readUnsignedShort();
                    if (routingServerNameLength <= 0 || routingServerNameLength > 1024) {
                        throwInvalidTDS();
                    }

                    routingServerName = tdsReader.readUnicodeString(routingServerNameLength);
                    assert routingServerName != null;

                }
                finally {
                    if (connectionlogger.isLoggable(Level.FINER)) {
                        connectionlogger.finer(toString() + " Received routing ENVCHANGE with the following values." + " routingDataValueLength:"
                                + routingDataValueLength + " protocol:" + routingProtocol + " portNumber:" + routingPortNumber + " serverNameLength:"
                                + routingServerNameLength + " serverName:" + ((routingServerName != null) ? routingServerName : "null"));
                    }
                }

                // Check if the hostNameInCertificate needs to be updated to handle the rerouted subdomain in Azure
                String currentHostName = activeConnectionProperties.getProperty("hostNameInCertificate");
                if (null != currentHostName && currentHostName.startsWith("*")
                        && (null != routingServerName) /* skip the check for hostNameInCertificate if routingServerName is null */
                        && routingServerName.indexOf('.') != -1) {
                    char[] currentHostNameCharArray = currentHostName.toCharArray();
                    char[] routingServerNameCharArray = routingServerName.toCharArray();
                    boolean hostNameNeedsUpdate = true;

                    /*
                     * Check if routingServerName and hostNameInCertificate are from same domain by verifying each character in currentHostName from
                     * last until it reaches the character before the wildcard symbol (i.e. currentHostNameCharArray[1])
                     */
                    for (int i = currentHostName.length() - 1, j = routingServerName.length() - 1; i > 0 && j > 0; i--, j--) {
                        if (routingServerNameCharArray[j] != currentHostNameCharArray[i]) {
                            hostNameNeedsUpdate = false;
                            break;
                        }
                    }

                    if (hostNameNeedsUpdate) {
                        String newHostName = "*" + routingServerName.substring(routingServerName.indexOf('.'));
                        activeConnectionProperties.setProperty("hostNameInCertificate", newHostName);

                        if (connectionlogger.isLoggable(Level.FINER)) {
                            connectionlogger.finer(toString() + "Using new host to validate the SSL certificate");
                        }
                    }
                }

                isRoutedInCurrentAttempt = true;
                routingInfo = new ServerPortPlaceHolder(routingServerName, routingPortNumber, null, integratedSecurity);

                break;

            // Error on unrecognized, unused ENVCHANGES
            default:
                if (connectionlogger.isLoggable(Level.WARNING)) {
                    connectionlogger.warning(toString() + " Unknown environment change: " + envchange);
                }
                throwInvalidTDS();
                break;
        }

        // After extracting whatever value information we need, skip over whatever is left
        // that we're not interested in.
        tdsReader.reset(mark);
        tdsReader.readBytes(new byte[envValueLength], 0, envValueLength);
    }

    final void processFedAuthInfo(TDSReader tdsReader,
            TDSTokenHandler tdsTokenHandler) throws SQLServerException {
        SqlFedAuthInfo sqlFedAuthInfo = new SqlFedAuthInfo();

        tdsReader.readUnsignedByte(); // token type, 0xEE

        // TdsParser.TryGetTokenLength, for FEDAUTHINFO, it uses TryReadInt32()
        int tokenLen = tdsReader.readInt();

        if (connectionlogger.isLoggable(Level.FINER)) {
            connectionlogger.fine(toString() + " FEDAUTHINFO token stream length = " + tokenLen);
        }

        if (tokenLen < 4) {
            // the token must at least contain a DWORD(length is 4 bytes) indicating the number of info IDs
            if (connectionlogger.isLoggable(Level.SEVERE)) {
                connectionlogger.severe(toString() + "FEDAUTHINFO token stream length too short for CountOfInfoIDs.");
            }
            throw new SQLServerException(SQLServerException.getErrString("R_FedAuthInfoLengthTooShortForCountOfInfoIds"), null);
        }

        // read how many FedAuthInfo options there are
        int optionsCount = tdsReader.readInt();

        tokenLen = tokenLen - 4; // remaining length is shortened since we read optCount, 4 is the size of int

        if (connectionlogger.isLoggable(Level.FINER)) {
            connectionlogger.fine(toString() + " CountOfInfoIDs = " + optionsCount);
        }

        if (tokenLen > 0) {
            // read the rest of the token
            byte[] tokenData = new byte[tokenLen];

            tdsReader.readBytes(tokenData, 0, tokenLen);

            if (connectionlogger.isLoggable(Level.FINER)) {
                connectionlogger.fine(toString() + " Read rest of FEDAUTHINFO token stream: " + Arrays.toString(tokenData));
            }

            // each FedAuthInfoOpt is 9 bytes:
            // 1 byte for FedAuthInfoID
            // 4 bytes for FedAuthInfoDataLen
            // 4 bytes for FedAuthInfoDataOffset
            // So this is the index in tokenData for the i-th option
            final int optionSize = 9;

            // the total number of bytes for all FedAuthInfoOpts together
            int totalOptionsSize = optionsCount * optionSize;

            for (int i = 0; i < optionsCount; i++) {
                int currentOptionOffset = i * optionSize;

                byte id = tokenData[currentOptionOffset];
                byte[] buffer = new byte[4];
                buffer[3] = tokenData[currentOptionOffset + 1];
                buffer[2] = tokenData[currentOptionOffset + 2];
                buffer[1] = tokenData[currentOptionOffset + 3];
                buffer[0] = tokenData[currentOptionOffset + 4];
                java.nio.ByteBuffer wrapped = java.nio.ByteBuffer.wrap(buffer); // big-endian by default
                int dataLen = wrapped.getInt();

                buffer = new byte[4];
                buffer[3] = tokenData[currentOptionOffset + 5];
                buffer[2] = tokenData[currentOptionOffset + 6];
                buffer[1] = tokenData[currentOptionOffset + 7];
                buffer[0] = tokenData[currentOptionOffset + 8];
                wrapped = java.nio.ByteBuffer.wrap(buffer); // big-endian by default
                int dataOffset = wrapped.getInt();

                if (connectionlogger.isLoggable(Level.FINER)) {
                    connectionlogger.fine(toString() + " FedAuthInfoOpt: ID=" + id + ", DataLen=" + dataLen + ", Offset=" + dataOffset);
                }

                // offset is measured from optCount, so subtract to make offset measured
                // from the beginning of tokenData, 4 is the size of int
                dataOffset = dataOffset - 4;

                // if dataOffset points to a region within FedAuthInfoOpt or after the end of the token, throw
                if (dataOffset < totalOptionsSize || dataOffset >= tokenLen) {
                    if (connectionlogger.isLoggable(Level.SEVERE)) {
                        connectionlogger.severe(toString() + "FedAuthInfoDataOffset points to an invalid location.");
                    }
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_FedAuthInfoInvalidOffset"));
                    throw new SQLServerException(form.format(new Object[] {dataOffset}), null);
                }

                // try to read data and throw if the arguments are bad, meaning the server sent us a bad token
                String data = null;
                try {
                    byte[] dataArray = new byte[dataLen];
                    System.arraycopy(tokenData, dataOffset, dataArray, 0, dataLen);
                    data = new String(dataArray, UTF_16LE);
                }
                catch (Exception e) {
                    connectionlogger.severe(toString() + "Failed to read FedAuthInfoData.");
                    throw new SQLServerException(SQLServerException.getErrString("R_FedAuthInfoFailedToReadData"), e);
                }

                if (connectionlogger.isLoggable(Level.FINER)) {
                    connectionlogger.fine(toString() + " FedAuthInfoData: " + data);
                }

                // store data in tempFedAuthInfo
                switch (id) {
                    case TDS.FEDAUTH_INFO_ID_SPN:
                        sqlFedAuthInfo.spn = data;
                        break;
                    case TDS.FEDAUTH_INFO_ID_STSURL:
                        sqlFedAuthInfo.stsurl = data;
                        break;
                    default:
                        if (connectionlogger.isLoggable(Level.FINER)) {
                            connectionlogger.fine(toString() + " Ignoring unknown federated authentication info option: " + id);
                        }
                        break;
                }
            }
        }
        else {
            if (connectionlogger.isLoggable(Level.SEVERE)) {
                connectionlogger.severe(toString() + "FEDAUTHINFO token stream is not long enough to contain the data it claims to.");
            }
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_FedAuthInfoLengthTooShortForData"));
            throw new SQLServerException(form.format(new Object[] {tokenLen}), null);
        }

        if (null == sqlFedAuthInfo.spn || null == sqlFedAuthInfo.stsurl || sqlFedAuthInfo.spn.trim().isEmpty()
                || sqlFedAuthInfo.stsurl.trim().isEmpty()) {
            // We should be receiving both stsurl and spn
            if (connectionlogger.isLoggable(Level.SEVERE)) {
                connectionlogger.severe(toString() + "FEDAUTHINFO token stream does not contain both STSURL and SPN.");
            }
            throw new SQLServerException(SQLServerException.getErrString("R_FedAuthInfoDoesNotContainStsurlAndSpn"), null);
        }

        onFedAuthInfo(sqlFedAuthInfo, tdsTokenHandler);
    }

    final class FedAuthTokenCommand extends UninterruptableTDSCommand {
        TDSTokenHandler tdsTokenHandler = null;
        SqlFedAuthToken fedAuthToken = null;

        FedAuthTokenCommand(SqlFedAuthToken fedAuthToken,
                TDSTokenHandler tdsTokenHandler) {
            super("FedAuth");
            this.tdsTokenHandler = tdsTokenHandler;
            this.fedAuthToken = fedAuthToken;
        }

        final boolean doExecute() throws SQLServerException {
            sendFedAuthToken(this, fedAuthToken, tdsTokenHandler);
            return true;
        }
    }

    /**
     * Generates (if appropriate) and sends a Federated Authentication Access token to the server, using the Federated Authentication Info.
     */
    void onFedAuthInfo(SqlFedAuthInfo fedAuthInfo,
            TDSTokenHandler tdsTokenHandler) throws SQLServerException {
        assert (null != activeConnectionProperties.getProperty(SQLServerDriverStringProperty.USER.toString())
                && null != activeConnectionProperties.getProperty(SQLServerDriverStringProperty.PASSWORD.toString()))
                || ((authenticationString.trim().equalsIgnoreCase(SqlAuthentication.ActiveDirectoryIntegrated.toString())
                        && fedAuthRequiredPreLoginResponse));
        assert null != fedAuthInfo;

        attemptRefreshTokenLocked = true;
        fedAuthToken = getFedAuthToken(fedAuthInfo);
        attemptRefreshTokenLocked = false;

        // fedAuthToken cannot be null.
        assert null != fedAuthToken;

        TDSCommand fedAuthCommand = new FedAuthTokenCommand(fedAuthToken, tdsTokenHandler);
        fedAuthCommand.execute(tdsChannel.getWriter(), tdsChannel.getReader(fedAuthCommand));
    }

    private SqlFedAuthToken getFedAuthToken(SqlFedAuthInfo fedAuthInfo) throws SQLServerException {
        SqlFedAuthToken fedAuthToken = null;

        // fedAuthInfo should not be null.
        assert null != fedAuthInfo;

        String user = activeConnectionProperties.getProperty(SQLServerDriverStringProperty.USER.toString());
        String password = activeConnectionProperties.getProperty(SQLServerDriverStringProperty.PASSWORD.toString());

        // No:of milliseconds to sleep for the inital back off.
        int sleepInterval = 100;
        
        while (true) {
            if (authenticationString.trim().equalsIgnoreCase(SqlAuthentication.ActiveDirectoryPassword.toString())) {
                fedAuthToken = SQLServerADAL4JUtils.getSqlFedAuthToken(fedAuthInfo, user, password, authenticationString);

                // Break out of the retry loop in successful case.
                break;
            }
            else if (authenticationString.trim().equalsIgnoreCase(SqlAuthentication.ActiveDirectoryIntegrated.toString())) {
                
                // If operating system is windows and sqljdbc_auth is loaded then choose the DLL authentication.
                if (System.getProperty("os.name").toLowerCase(Locale.ENGLISH).startsWith("windows") && AuthenticationJNI.isDllLoaded()) {
                    try {
                        long expirationFileTime = 0;
                        FedAuthDllInfo dllInfo = AuthenticationJNI.getAccessTokenForWindowsIntegrated(fedAuthInfo.stsurl, fedAuthInfo.spn,
                                clientConnectionId.toString(), ActiveDirectoryAuthentication.JDBC_FEDAUTH_CLIENT_ID, expirationFileTime);

                        // AccessToken should not be null.
                        assert null != dllInfo.accessTokenBytes;

                        byte[] accessTokenFromDLL = dllInfo.accessTokenBytes;

                        String accessToken = new String(accessTokenFromDLL, UTF_16LE);

                        fedAuthToken = new SqlFedAuthToken(accessToken, dllInfo.expiresIn);

                        // Break out of the retry loop in successful case.
                        break;
                    }
                    catch (DLLException adalException) {

                        // the sqljdbc_auth.dll return -1 for errorCategory, if unable to load the adalsql.dll
                        int errorCategory = adalException.GetCategory();
                        if (-1 == errorCategory) {
                            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UnableLoadADALSqlDll"));
                            Object[] msgArgs = {Integer.toHexString(adalException.GetState())};
                            throw new SQLServerException(form.format(msgArgs), null);
                        }

                        int millisecondsRemaining = TimerRemaining(timerExpire);
                        if (ActiveDirectoryAuthentication.GET_ACCESS_TOKEN_TANSISENT_ERROR != errorCategory || timerHasExpired(timerExpire)
                                || (sleepInterval >= millisecondsRemaining)) {

                            String errorStatus = Integer.toHexString(adalException.GetStatus());

                            if (connectionlogger.isLoggable(Level.FINER)) {
                                connectionlogger.fine(toString() + " SQLServerConnection.getFedAuthToken.AdalException category:" + errorCategory
                                        + " error: " + errorStatus);
                            }

                            MessageFormat form1 = new MessageFormat(SQLServerException.getErrString("R_ADALAuthenticationMiddleErrorMessage"));
                            String errorCode = Integer.toHexString(adalException.GetStatus()).toUpperCase();
                            Object[] msgArgs1 = {errorCode, adalException.GetState()};
                            SQLServerException middleException = new SQLServerException(form1.format(msgArgs1), adalException);

                            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_ADALExecution"));
                            Object[] msgArgs = {user, authenticationString};
                            throw new SQLServerException(form.format(msgArgs), null, 0, middleException);
                        }

                        if (connectionlogger.isLoggable(Level.FINER)) {
                            connectionlogger.fine(toString() + " SQLServerConnection.getFedAuthToken sleeping: " + sleepInterval + " milliseconds.");
                            connectionlogger
                                    .fine(toString() + " SQLServerConnection.getFedAuthToken remaining: " + millisecondsRemaining + " milliseconds.");
                        }

                        try {
                            Thread.sleep(sleepInterval);
                        }
                        catch (InterruptedException e1) {
                            // re-interrupt the current thread, in order to restore the thread's interrupt status.
                            Thread.currentThread().interrupt();
                        }
                        sleepInterval = sleepInterval * 2;
                    }
                }
                // else choose ADAL4J for integrated authentication. This option is supported for both windows and unix, so we don't need to check the
                // OS version here.
                else {
                    fedAuthToken = SQLServerADAL4JUtils.getSqlFedAuthTokenIntegrated(fedAuthInfo, authenticationString);
                }
                // Break out of the retry loop in successful case.
                break;
            }
        }

        return fedAuthToken;
    }

    /**
     * Send the access token to the server.
     */
    private void sendFedAuthToken(FedAuthTokenCommand fedAuthCommand,
            SqlFedAuthToken fedAuthToken,
            TDSTokenHandler tdsTokenHandler) throws SQLServerException {
        assert null != fedAuthToken;
        assert null != fedAuthToken.accessToken;

        if (connectionlogger.isLoggable(Level.FINER)) {
            connectionlogger.fine(toString() + " Sending federated authentication token.");
        }

        TDSWriter tdsWriter = fedAuthCommand.startRequest(TDS.PKT_FEDAUTH_TOKEN_MESSAGE);

        byte[] accessToken = fedAuthToken.accessToken.getBytes(UTF_16LE);

        // Send total length (length of token plus 4 bytes for the token length field)
        // If we were sending a nonce, this would include that length as well
        tdsWriter.writeInt(accessToken.length + 4);

        // Send length of token
        tdsWriter.writeInt(accessToken.length);

        // Send federated authentication access token.
        tdsWriter.writeBytes(accessToken, 0, accessToken.length);

        TDSReader tdsReader;
        tdsReader = fedAuthCommand.startResponse();

        federatedAuthenticationRequested = true;

        TDSParser.parse(tdsReader, tdsTokenHandler);
    }

    final void processFeatureExtAck(TDSReader tdsReader) throws SQLServerException {
        tdsReader.readUnsignedByte();	// Reading FEATUREEXTACK_TOKEN 0xAE

        // read feature ID
        byte featureId;
        do {
            featureId = (byte) tdsReader.readUnsignedByte();

            if (featureId != TDS.FEATURE_EXT_TERMINATOR) {
                int dataLen;
                dataLen = tdsReader.readInt();

                byte[] data = new byte[dataLen];
                if (dataLen > 0) {
                    tdsReader.readBytes(data, 0, dataLen);
                }
                onFeatureExtAck(featureId, data);
            }
        }
        while (featureId != TDS.FEATURE_EXT_TERMINATOR);
    }

    private void onFeatureExtAck(int featureId,
            byte[] data) throws SQLServerException {
        if (null != routingInfo) {
            return;
        }

        switch (featureId) {
            case TDS.TDS_FEATURE_EXT_FEDAUTH: {
                if (connectionlogger.isLoggable(Level.FINER)) {
                    connectionlogger.fine(toString() + " Received feature extension acknowledgement for federated authentication.");
                }

                if (!federatedAuthenticationRequested) {
                    if (connectionlogger.isLoggable(Level.SEVERE)) {
                        connectionlogger.severe(toString() + " Did not request federated authentication.");
                    }
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UnrequestedFeatureAckReceived"));
                    Object[] msgArgs = {featureId};
                    throw new SQLServerException(form.format(msgArgs), null);
                }

                // _fedAuthFeatureExtensionData must not be null when _federatedAuthenticatonRequested == true
                assert null != fedAuthFeatureExtensionData;

                switch (fedAuthFeatureExtensionData.libraryType) {
                    case TDS.TDS_FEDAUTH_LIBRARY_ADAL:
                    case TDS.TDS_FEDAUTH_LIBRARY_SECURITYTOKEN:
                        // The server shouldn't have sent any additional data with the ack (like a nonce)
                        if (0 != data.length) {
                            if (connectionlogger.isLoggable(Level.SEVERE)) {
                                connectionlogger.severe(toString()
                                        + " Federated authentication feature extension ack for ADAL and Security Token includes extra data.");
                            }
                            throw new SQLServerException(SQLServerException.getErrString("R_FedAuthFeatureAckContainsExtraData"), null);
                        }
                        break;

                    default:
                        assert false;	// Unknown _fedAuthLibrary type
                        if (connectionlogger.isLoggable(Level.SEVERE)) {
                            connectionlogger.severe(toString() + " Attempting to use unknown federated authentication library.");
                        }
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_FedAuthFeatureAckUnknownLibraryType"));
                        Object[] msgArgs = {fedAuthFeatureExtensionData.libraryType};
                        throw new SQLServerException(form.format(msgArgs), null);
                }
                federatedAuthenticationAcknowledged = true;

                break;
            }
            case TDS.TDS_FEATURE_EXT_AE: {
                if (connectionlogger.isLoggable(Level.FINER)) {
                    connectionlogger.fine(toString() + " Received feature extension acknowledgement for AE.");
                }

                if (1 > data.length) {
                    if (connectionlogger.isLoggable(Level.SEVERE)) {
                        connectionlogger.severe(toString() + " Unknown version number for AE.");
                    }
                    throw new SQLServerException(SQLServerException.getErrString("R_InvalidAEVersionNumber"), null);
                }

                byte supportedTceVersion = data[0];
                if (0 == supportedTceVersion || supportedTceVersion > TDS.MAX_SUPPORTED_TCE_VERSION) {
                    if (connectionlogger.isLoggable(Level.SEVERE)) {
                        connectionlogger.severe(toString() + " Invalid version number for AE.");
                    }
                    throw new SQLServerException(SQLServerException.getErrString("R_InvalidAEVersionNumber"), null);
                }

                assert supportedTceVersion == TDS.MAX_SUPPORTED_TCE_VERSION; // Client support TCE version 1
                serverSupportsColumnEncryption = true;
                break;
            }

            default: {
                // Unknown feature ack
                if (connectionlogger.isLoggable(Level.SEVERE)) {
                    connectionlogger.severe(toString() + " Unknown feature ack.");
                }
                throw new SQLServerException(SQLServerException.getErrString("R_UnknownFeatureAck"), null);
            }
        }
    }

    /*
     * Executes a DTC command
     */
    private void executeDTCCommand(int requestType,
            byte[] payload,
            String logContext) throws SQLServerException {
        final class DTCCommand extends UninterruptableTDSCommand {
            private final int requestType;
            private final byte[] payload;

            DTCCommand(int requestType,
                    byte[] payload,
                    String logContext) {
                super(logContext);
                this.requestType = requestType;
                this.payload = payload;
            }

            final boolean doExecute() throws SQLServerException {
                TDSWriter tdsWriter = startRequest(TDS.PKT_DTC);

                tdsWriter.writeShort((short) requestType);
                if (null == payload) {
                    tdsWriter.writeShort((short) 0);
                }
                else {
                    assert payload.length <= Short.MAX_VALUE;
                    tdsWriter.writeShort((short) payload.length);
                    tdsWriter.writeBytes(payload);
                }

                TDSParser.parse(startResponse(), getLogContext());
                return true;
            }
        }

        executeCommand(new DTCCommand(requestType, payload, logContext));
    }

    /**
     * Unenlist the local transaction with DTC.
     * 
     * @throws SQLServerException
     */
    final void JTAUnenlistConnection() throws SQLServerException {
        // Unenlist the connection
        executeDTCCommand(TDS.TM_PROPAGATE_XACT, null, "MS_DTC unenlist connection");
        inXATransaction = false;
    }

    /**
     * Enlist this connection's local transaction with MS DTC
     * 
     * @param cookie
     *            the cookie identifying the transaction
     * @throws SQLServerException
     */
    final void JTAEnlistConnection(byte cookie[]) throws SQLServerException {
        // Enlist the connection
        executeDTCCommand(TDS.TM_PROPAGATE_XACT, cookie, "MS_DTC enlist connection");

        // DTC sets the enlisted connection's isolation level to SERIALIZABLE by default.
        // Set the isolation level the way the app wants it.
        connectionCommand(sqlStatementToSetTransactionIsolationLevel(), "JTAEnlistConnection");
        inXATransaction = true;
    }

    /**
     * Convert to a String UCS16 encoding.
     * 
     * @param s
     *            the string
     * @throws SQLServerException
     * @return the encoded data
     */
    /* L0 */ private byte[] toUCS16(String s) throws SQLServerException {
        if (s == null)
            return new byte[0];
        int l = s.length();
        byte data[] = new byte[l * 2];
        int offset = 0;
        for (int i = 0; i < l; i++) {
            int c = s.charAt(i);
            byte b1 = (byte) (c & 0xFF);
            data[offset++] = b1;
            data[offset++] = (byte) ((c >> 8) & 0xFF); // Unicode MSB
        }
        return data;
    }

    /**
     * Encrypt a password for the SQL Server logon.
     * 
     * @param pwd
     *            the password
     * @return the encryption
     */
    /* L0 */ private byte[] encryptPassword(String pwd) {
        // Changed to handle non ascii passwords
        if (pwd == null)
            pwd = "";
        int len = pwd.length();
        byte data[] = new byte[len * 2];
        for (int i1 = 0; i1 < len; i1++) {
            int j1 = pwd.charAt(i1) ^ 0x5a5a;
            j1 = (j1 & 0xf) << 4 | (j1 & 0xf0) >> 4 | (j1 & 0xf00) << 4 | (j1 & 0xf000) >> 4;
            byte b1 = (byte) ((j1 & 0xFF00) >> 8);
            data[(i1 * 2) + 1] = b1;
            byte b2 = (byte) ((j1 & 0x00FF));
            data[(i1 * 2) + 0] = b2;
        }
        return data;
    }

    /**
     * Send a TDS 7.x logon packet.
     * 
     * @param secsTimeout
     *            (optional) if non-zero, seconds to wait for logon to be sent.
     * @throws SQLServerException
     */
    private void sendLogon(LogonCommand logonCommand,
            SSPIAuthentication authentication,
            FederatedAuthenticationFeatureExtensionData fedAuthFeatureExtensionData) throws SQLServerException {
        // TDS token handler class for processing logon responses.
        //
        // Note:
        // As a local inner class, LogonProcessor implicitly has access to private
        // members of SQLServerConnection. Certain JVM implementations generate
        // package scope accessors to any private members touched by this class,
        // effectively changing visibility of such members from private to package.
        // Therefore, it is IMPORTANT then for this class not to touch private
        // member variables in SQLServerConnection that contain secure information.
        final class LogonProcessor extends TDSTokenHandler {
            private final SSPIAuthentication auth;
            private byte[] secBlobOut = null;
            StreamLoginAck loginAckToken;

            LogonProcessor(SSPIAuthentication auth) {
                super("logon");
                this.auth = auth;
                this.loginAckToken = null;
            }

            boolean onSSPI(TDSReader tdsReader) throws SQLServerException {
                StreamSSPI ack = new StreamSSPI();
                ack.setFromTDS(tdsReader);

                // Extract SSPI data from the response. If another round trip is
                // required then we will start it after we finish processing the
                // rest of this response.
                boolean[] done = {false};
                secBlobOut = auth.GenerateClientContext(ack.sspiBlob, done);
                return true;
            }

            boolean onLoginAck(TDSReader tdsReader) throws SQLServerException {
                loginAckToken = new StreamLoginAck();
                loginAckToken.setFromTDS(tdsReader);
                sqlServerVersion = loginAckToken.sSQLServerVersion;
                tdsVersion = loginAckToken.tdsVersion;
                return true;
            }

            final boolean complete(LogonCommand logonCommand,
                    TDSReader tdsReader) throws SQLServerException {
                // If we have the login ack already then we're done processing.
                if (null != loginAckToken)
                    return true;

                // No login ack yet. Check if there is more SSPI handshake to do...
                if (null != secBlobOut && 0 != secBlobOut.length) {
                    // Yes, there is. So start the next SSPI round trip and indicate to
                    // our caller that it needs to keep the processing loop going.
                    logonCommand.startRequest(TDS.PKT_SSPI).writeBytes(secBlobOut, 0, secBlobOut.length);
                    return false;
                }

                // The login ack comes in its own complete TDS response message.
                // So integrated auth effectively receives more response messages from
                // the server than it sends request messages from the driver.
                // To ensure that the rest of the response can be read, fake another
                // request to the server so that the channel sees int auth login
                // as a symmetric conversation.
                logonCommand.startRequest(TDS.PKT_SSPI);
                logonCommand.onRequestComplete();
                ++tdsChannel.numMsgsSent;

                TDSParser.parse(tdsReader, this);
                return true;
            }
        }

        // Cannot use SSPI when server has responded 0x01 for FedAuthRequired PreLogin Option.
        assert !(integratedSecurity && fedAuthRequiredPreLoginResponse);
        // Cannot use both SSPI and FedAuth
        assert (!integratedSecurity) || !(federatedAuthenticationInfoRequested || federatedAuthenticationRequested);
        // fedAuthFeatureExtensionData provided without fed auth feature request
        assert (null == fedAuthFeatureExtensionData) || (federatedAuthenticationInfoRequested || federatedAuthenticationRequested);
        // Fed Auth feature requested without specifying fedAuthFeatureExtensionData.
        assert (null != fedAuthFeatureExtensionData || !(federatedAuthenticationInfoRequested || federatedAuthenticationRequested));

        String sUser = activeConnectionProperties.getProperty(SQLServerDriverStringProperty.USER.toString());
        String sPwd = activeConnectionProperties.getProperty(SQLServerDriverStringProperty.PASSWORD.toString());
        String appName = activeConnectionProperties.getProperty(SQLServerDriverStringProperty.APPLICATION_NAME.toString());
        String interfaceLibName = "Microsoft JDBC Driver " + SQLJdbcVersion.major + "." + SQLJdbcVersion.minor;
        String databaseName = activeConnectionProperties.getProperty(SQLServerDriverStringProperty.DATABASE_NAME.toString());
        String serverName;
        // currentConnectPlaceHolder should not be null here. Still doing the check for extra security.
        if (null != currentConnectPlaceHolder) {
            serverName = currentConnectPlaceHolder.getServerName();
        }
        else {
            serverName = activeConnectionProperties.getProperty(SQLServerDriverStringProperty.SERVER_NAME.toString());
        }

        if (serverName != null && serverName.length() > 128)
            serverName = serverName.substring(0, 128);

        byte[] secBlob = new byte[0];
        boolean[] done = {false};
        if (null != authentication) {
            secBlob = authentication.GenerateClientContext(secBlob, done);
            sUser = null;
            sPwd = null;
        }

        byte hostnameBytes[] = toUCS16(hostName);
        byte userBytes[] = toUCS16(sUser);
        byte passwordBytes[] = encryptPassword(sPwd);
        int passwordLen = passwordBytes != null ? passwordBytes.length : 0;
        byte appNameBytes[] = toUCS16(appName);
        byte serverNameBytes[] = toUCS16(serverName);
        byte interfaceLibNameBytes[] = toUCS16(interfaceLibName);
        byte interfaceLibVersionBytes[] = {(byte) SQLJdbcVersion.build, (byte) SQLJdbcVersion.patch, (byte) SQLJdbcVersion.minor,
                (byte) SQLJdbcVersion.major};
        byte databaseNameBytes[] = toUCS16(databaseName);
        byte netAddress[] = new byte[6];
        int dataLen = 0;

        final int TDS_LOGIN_REQUEST_BASE_LEN = 94;

        if (serverMajorVersion >= 11) // Denali --> TDS 7.4
        {
            tdsVersion = TDS.VER_DENALI;
        }
        else if (serverMajorVersion >= 10) // Katmai (10.0) & later 7.3B
        {
            tdsVersion = TDS.VER_KATMAI;
        }
        else if (serverMajorVersion >= 9) // Yukon (9.0) --> TDS 7.2 // Prelogin disconnects anything older
        {
            tdsVersion = TDS.VER_YUKON;
        }
        else // Shiloh (8.x) --> TDS 7.1
        {
            assert false : "prelogin did not disconnect for the old version: " + serverMajorVersion;
        }

        TDSWriter tdsWriter = logonCommand.startRequest(TDS.PKT_LOGON70);

        int len2 = TDS_LOGIN_REQUEST_BASE_LEN + hostnameBytes.length + appNameBytes.length + serverNameBytes.length + interfaceLibNameBytes.length
                + databaseNameBytes.length + secBlob.length + 4;// AE is always on;

        // only add lengths of password and username if not using SSPI or requesting federated authentication info
        if (!integratedSecurity && !(federatedAuthenticationInfoRequested || federatedAuthenticationRequested)) {
            len2 = len2 + passwordLen + userBytes.length;
        }

        int aeOffset = len2;
        // AE is always ON
        len2 += writeAEFeatureRequest(false, tdsWriter);
        if (federatedAuthenticationInfoRequested || federatedAuthenticationRequested) {
            len2 = len2 + writeFedAuthFeatureRequest(false, tdsWriter, fedAuthFeatureExtensionData);
        }

        len2 = len2 + 1; // add 1 to length becaue of FeatureEx terminator

        // Length of entire Login 7 packet
        tdsWriter.writeInt(len2);
        tdsWriter.writeInt(tdsVersion);
        tdsWriter.writeInt(requestedPacketSize);
        tdsWriter.writeBytes(interfaceLibVersionBytes); // writeBytes() is little endian
        tdsWriter.writeInt(0); // Client process ID (0 = ??)
        tdsWriter.writeInt(0); // Primary server connection ID

        tdsWriter.writeByte((byte) (          // OptionFlags1:
        TDS.LOGIN_OPTION1_ORDER_X86 | // X86 byte order for numeric & datetime types
                TDS.LOGIN_OPTION1_CHARSET_ASCII | // ASCII character set
                TDS.LOGIN_OPTION1_FLOAT_IEEE_754 | // IEEE 754 floating point representation
                TDS.LOGIN_OPTION1_DUMPLOAD_ON | // Require dump/load BCP capabilities
                TDS.LOGIN_OPTION1_USE_DB_OFF | // No ENVCHANGE after USE DATABASE
                TDS.LOGIN_OPTION1_INIT_DB_FATAL | // Fail connection if initial database change fails
                TDS.LOGIN_OPTION1_SET_LANG_ON      // Warn on SET LANGUAGE stmt
        ));

        tdsWriter.writeByte((byte) (           // OptionFlags2:
        TDS.LOGIN_OPTION2_INIT_LANG_FATAL | // Fail connection if initial language change fails
                TDS.LOGIN_OPTION2_ODBC_ON | // Use ODBC defaults (ANSI_DEFAULTS ON, IMPLICIT_TRANSACTIONS OFF, TEXTSIZE inf, ROWCOUNT inf)
                (integratedSecurity ?               // Use integrated security if requested
                        TDS.LOGIN_OPTION2_INTEGRATED_SECURITY_ON : TDS.LOGIN_OPTION2_INTEGRATED_SECURITY_OFF)));

        // TypeFlags
        tdsWriter.writeByte((byte) (TDS.LOGIN_SQLTYPE_DEFAULT | (applicationIntent != null && applicationIntent.equals(ApplicationIntent.READ_ONLY)
                ? TDS.LOGIN_READ_ONLY_INTENT : TDS.LOGIN_READ_WRITE_INTENT)));

        // OptionFlags3
        byte colEncSetting;
        // AE is always ON
        {
            colEncSetting = TDS.LOGIN_OPTION3_FEATURE_EXTENSION;
        }
        tdsWriter.writeByte(
                (byte) (TDS.LOGIN_OPTION3_DEFAULT | colEncSetting | ((serverMajorVersion >= 10) ? TDS.LOGIN_OPTION3_UNKNOWN_COLLATION_HANDLING : 0) // Accept
                                                                                                                                                    // unknown
                                                                                                                                                    // collations
                                                                                                                                                    // from
                                                                                                                                                    // Katmai
                                                                                                                                                    // &
                                                                                                                                                    // later
                                                                                                                                                    // servers
                ));

        tdsWriter.writeInt((byte) 0); // Client time zone
        tdsWriter.writeInt((byte) 0); // Client LCID

        tdsWriter.writeShort((short) TDS_LOGIN_REQUEST_BASE_LEN);

        // Hostname
        tdsWriter.writeShort((short) ((hostName != null && !hostName.isEmpty()) ? hostName.length() : 0));
        dataLen += hostnameBytes.length;

        // Only send user/password over if not fSSPI or fed auth ADAL... If both user/password and SSPI are in login
        // rec, only SSPI is used.
        if (!integratedSecurity && !(federatedAuthenticationInfoRequested || federatedAuthenticationRequested)) {
            // User and Password
            tdsWriter.writeShort((short) (TDS_LOGIN_REQUEST_BASE_LEN + dataLen));
            tdsWriter.writeShort((short) (sUser == null ? 0 : sUser.length()));
            dataLen += userBytes.length;

            tdsWriter.writeShort((short) (TDS_LOGIN_REQUEST_BASE_LEN + dataLen));
            tdsWriter.writeShort((short) (sPwd == null ? 0 : sPwd.length()));
            dataLen += passwordLen;

        }
        else {
            // User and Password are null
            tdsWriter.writeShort((short) (0));
            tdsWriter.writeShort((short) (0));
            tdsWriter.writeShort((short) (0));
            tdsWriter.writeShort((short) (0));
        }

        // App name
        tdsWriter.writeShort((short) (TDS_LOGIN_REQUEST_BASE_LEN + dataLen));
        tdsWriter.writeShort((short) (appName == null ? 0 : appName.length()));
        dataLen += appNameBytes.length;

        // Server name
        tdsWriter.writeShort((short) (TDS_LOGIN_REQUEST_BASE_LEN + dataLen));
        tdsWriter.writeShort((short) (serverName == null ? 0 : serverName.length()));
        dataLen += serverNameBytes.length;

        // Unused
        tdsWriter.writeShort((short) (TDS_LOGIN_REQUEST_BASE_LEN + dataLen));
        // AE is always ON
        {
            tdsWriter.writeShort((short) 4);
            dataLen += 4;
        }

        // Interface library name
        assert null != interfaceLibName;
        tdsWriter.writeShort((short) (TDS_LOGIN_REQUEST_BASE_LEN + dataLen));
        tdsWriter.writeShort((short) (interfaceLibName.length()));
        dataLen += interfaceLibNameBytes.length;

        // Language
        tdsWriter.writeShort((short) 0);
        tdsWriter.writeShort((short) 0);

        // Database
        tdsWriter.writeShort((short) (TDS_LOGIN_REQUEST_BASE_LEN + dataLen));
        tdsWriter.writeShort((short) (databaseName == null ? 0 : databaseName.length()));
        dataLen += databaseNameBytes.length;

        // Client ID (from MAC addr)
        tdsWriter.writeBytes(netAddress);

        final int USHRT_MAX = 65535;
        // SSPI data
        if (!integratedSecurity) {
            tdsWriter.writeShort((short) 0);
            tdsWriter.writeShort((short) 0);
        }
        else {
            tdsWriter.writeShort((short) (TDS_LOGIN_REQUEST_BASE_LEN + dataLen));
            if (USHRT_MAX <= secBlob.length) {
                tdsWriter.writeShort((short) (USHRT_MAX));
            }
            else
                tdsWriter.writeShort((short) (secBlob.length));
        }

        // Database to attach during connection process
        tdsWriter.writeShort((short) 0);
        tdsWriter.writeShort((short) 0);

        if (tdsVersion >= TDS.VER_YUKON) {
            // TDS 7.2: Password change
            tdsWriter.writeShort((short) 0);
            tdsWriter.writeShort((short) 0);

            // TDS 7.2: 32-bit SSPI byte count (used if 16 bits above were not sufficient)
            if (USHRT_MAX <= secBlob.length)
                tdsWriter.writeInt(secBlob.length);
            else
                tdsWriter.writeInt((short) 0);
        }

        tdsWriter.writeBytes(hostnameBytes);

        // Don't allow user credentials to be logged
        tdsWriter.setDataLoggable(false);

        // if we are using SSPI or fed auth ADAL, do not send over username/password, since we will use SSPI instead
        if (!integratedSecurity && !(federatedAuthenticationInfoRequested || federatedAuthenticationRequested)) {
            tdsWriter.writeBytes(userBytes); // Username
            tdsWriter.writeBytes(passwordBytes); // Password (encrapted)
        }
        tdsWriter.setDataLoggable(true);

        tdsWriter.writeBytes(appNameBytes);	// application name
        tdsWriter.writeBytes(serverNameBytes);	// server name

        // AE is always ON
        {
            tdsWriter.writeInt(aeOffset);
        }

        tdsWriter.writeBytes(interfaceLibNameBytes);	// interfaceLibName
        tdsWriter.writeBytes(databaseNameBytes);	// databaseName

        // Don't allow user credentials to be logged
        tdsWriter.setDataLoggable(false);
        if (integratedSecurity)
            tdsWriter.writeBytes(secBlob, 0, secBlob.length);

        // AE is always ON
        {
            writeAEFeatureRequest(true, tdsWriter);
        }

        if (federatedAuthenticationInfoRequested || federatedAuthenticationRequested) {
            writeFedAuthFeatureRequest(true, tdsWriter, fedAuthFeatureExtensionData);
        }

        tdsWriter.writeByte((byte) TDS.FEATURE_EXT_TERMINATOR);
        tdsWriter.setDataLoggable(true);

        LogonProcessor logonProcessor = new LogonProcessor(authentication);
        TDSReader tdsReader;
        do {
            tdsReader = logonCommand.startResponse();
            TDSParser.parse(tdsReader, logonProcessor);
        }
        while (!logonProcessor.complete(logonCommand, tdsReader));
    }

    /* --------------- JDBC 3.0 ------------- */

    /**
     * Checks that the holdability argument is one of the values allowed by the JDBC spec and by this driver.
     */
    private void checkValidHoldability(int holdability) throws SQLServerException {
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT && holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidHoldability"));
            SQLServerException.makeFromDriverError(this, this, form.format(new Object[] {holdability}), null, true);
        }
    }

    /**
     * Checks that the proposed statement holdability matches this connection's current holdability.
     *
     * SQL Server doesn't support per-statement holdability, so the statement's proposed holdability must match its parent connection's. Note that
     * this doesn't stop anyone from changing the holdability of the connection after creating the statement. Apps should always call
     * Statement.getResultSetHoldability to check the holdability of ResultSets that would be created, and/or ResultSet.getHoldability to check the
     * holdability of an existing ResultSet.
     */
    private void checkMatchesCurrentHoldability(int resultSetHoldability) throws SQLServerException {
        if (resultSetHoldability != this.holdability) {
            SQLServerException.makeFromDriverError(this, this, SQLServerException.getErrString("R_sqlServerHoldability"), null, false);
        }
    }

    public Statement createStatement(int nType,
            int nConcur,
            int resultSetHoldability) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "createStatement",
                new Object[] {nType, nConcur, resultSetHoldability});
        Statement st = createStatement(nType, nConcur, resultSetHoldability, SQLServerStatementColumnEncryptionSetting.UseConnectionSetting);
        loggerExternal.exiting(getClassNameLogging(), "createStatement", st);
        return st;
    }

    public Statement createStatement(int nType,
            int nConcur,
            int resultSetHoldability,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "createStatement",
                new Object[] {nType, nConcur, resultSetHoldability, stmtColEncSetting});
        checkClosed();
        checkValidHoldability(resultSetHoldability);
        checkMatchesCurrentHoldability(resultSetHoldability);
        Statement st = new SQLServerStatement(this, nType, nConcur, stmtColEncSetting);
        loggerExternal.exiting(getClassNameLogging(), "createStatement", st);
        return st;
    }

    /* L3 */ public PreparedStatement prepareStatement(java.lang.String sql,
            int nType,
            int nConcur,
            int resultSetHoldability) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "prepareStatement",
                new Object[] {nType, nConcur, resultSetHoldability});
        PreparedStatement st = prepareStatement(sql, nType, nConcur, resultSetHoldability,
                SQLServerStatementColumnEncryptionSetting.UseConnectionSetting);
        loggerExternal.exiting(getClassNameLogging(), "prepareStatement", st);
        return st;
    }

    /**
     * Creates a <code>PreparedStatement</code> object that will generate <code>ResultSet</code> objects with the given type, concurrency, and
     * holdability.
     * <P>
     * This method is the same as the <code>prepareStatement</code> method above, but it allows the default result set type, concurrency, and
     * holdability to be overridden.
     *
     * @param sql
     *            a <code>String</code> object that is the SQL statement to be sent to the database; may contain one or more '?' IN parameters
     * @param nType
     *            one of the following <code>ResultSet</code> constants: <code>ResultSet.TYPE_FORWARD_ONLY</code>,
     *            <code>ResultSet.TYPE_SCROLL_INSENSITIVE</code>, or <code>ResultSet.TYPE_SCROLL_SENSITIVE</code>
     * @param nConcur
     *            one of the following <code>ResultSet</code> constants: <code>ResultSet.CONCUR_READ_ONLY</code> or
     *            <code>ResultSet.CONCUR_UPDATABLE</code>
     * @param resultSetHoldability
     *            one of the following <code>ResultSet</code> constants: <code>ResultSet.HOLD_CURSORS_OVER_COMMIT</code> or
     *            <code>ResultSet.CLOSE_CURSORS_AT_COMMIT</code>
     * @param stmtColEncSetting
     *            Specifies how data will be sent and received when reading and writing encrypted columns.
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled SQL statement, that will generate <code>ResultSet</code>
     *         objects with the given type, concurrency, and holdability
     * @throws SQLServerException
     *             if a database access error occurs, this method is called on a closed connection or the given parameters are not
     *             <code>ResultSet</code> constants indicating type, concurrency, and holdability
     */
    public PreparedStatement prepareStatement(java.lang.String sql,
            int nType,
            int nConcur,
            int resultSetHoldability,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "prepareStatement",
                new Object[] {nType, nConcur, resultSetHoldability, stmtColEncSetting});
        checkClosed();
        checkValidHoldability(resultSetHoldability);
        checkMatchesCurrentHoldability(resultSetHoldability);

        PreparedStatement st;

        // Make sure SQLServerPreparedStatement42 is used for 4.2 and above.
        if (Util.use42Wrapper() || Util.use43Wrapper()) {
            st = new SQLServerPreparedStatement42(this, sql, nType, nConcur, stmtColEncSetting);
        }
        else {
            st = new SQLServerPreparedStatement(this, sql, nType, nConcur, stmtColEncSetting);
        }

        loggerExternal.exiting(getClassNameLogging(), "prepareStatement", st);
        return st;
    }

    /* L3 */ public CallableStatement prepareCall(String sql,
            int nType,
            int nConcur,
            int resultSetHoldability) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "prepareStatement",
                new Object[] {nType, nConcur, resultSetHoldability});
        CallableStatement st = prepareCall(sql, nType, nConcur, resultSetHoldability, SQLServerStatementColumnEncryptionSetting.UseConnectionSetting);
        loggerExternal.exiting(getClassNameLogging(), "prepareCall", st);
        return st;
    }

    public CallableStatement prepareCall(String sql,
            int nType,
            int nConcur,
            int resultSetHoldability,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetiing) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "prepareStatement",
                new Object[] {nType, nConcur, resultSetHoldability, stmtColEncSetiing});
        checkClosed();
        checkValidHoldability(resultSetHoldability);
        checkMatchesCurrentHoldability(resultSetHoldability);

        CallableStatement st;

        // Make sure SQLServerCallableStatement42 is used for 4.2 and above
        if (Util.use42Wrapper() || Util.use43Wrapper()) {
            st = new SQLServerCallableStatement42(this, sql, nType, nConcur, stmtColEncSetiing);
        }
        else {
            st = new SQLServerCallableStatement(this, sql, nType, nConcur, stmtColEncSetiing);
        }

        loggerExternal.exiting(getClassNameLogging(), "prepareCall", st);
        return st;
    }

    /* JDBC 3.0 Auto generated keys */

    /* L3 */ public PreparedStatement prepareStatement(String sql,
            int flag) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "prepareStatement", new Object[] {sql, flag});

        SQLServerPreparedStatement ps = (SQLServerPreparedStatement) prepareStatement(sql, flag,
                SQLServerStatementColumnEncryptionSetting.UseConnectionSetting);

        loggerExternal.exiting(getClassNameLogging(), "prepareStatement", ps);
        return ps;
    }

    /**
     * Creates a default <code>PreparedStatement</code> object that has the capability to retrieve auto-generated keys. The given constant tells the
     * driver whether it should make auto-generated keys available for retrieval. This parameter is ignored if the SQL statement is not an
     * <code>INSERT</code> statement, or an SQL statement able to return auto-generated keys (the list of such statements is vendor-specific).
     * <P>
     * <B>Note:</B> This method is optimized for handling parametric SQL statements that benefit from precompilation. If the driver supports
     * precompilation, the method <code>prepareStatement</code> will send the statement to the database for precompilation. Some drivers may not
     * support precompilation. In this case, the statement may not be sent to the database until the <code>PreparedStatement</code> object is
     * executed. This has no direct effect on users; however, it does affect which methods throw certain SQLExceptions.
     * <P>
     * Result sets created using the returned <code>PreparedStatement</code> object will by default be type <code>TYPE_FORWARD_ONLY</code> and have a
     * concurrency level of <code>CONCUR_READ_ONLY</code>. The holdability of the created result sets can be determined by calling
     * {@link #getHoldability}.
     *
     * @param sql
     *            an SQL statement that may contain one or more '?' IN parameter placeholders
     * @param flag
     *            a flag indicating whether auto-generated keys should be returned; one of <code>Statement.RETURN_GENERATED_KEYS</code> or
     *            <code>Statement.NO_GENERATED_KEYS</code>
     * @param stmtColEncSetting
     *            Specifies how data will be sent and received when reading and writing encrypted columns.
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled SQL statement, that will have the capability of returning
     *         auto-generated keys
     * @throws SQLServerException
     *             if a database access error occurs, this method is called on a closed connection or the given parameter is not a
     *             <code>Statement</code> constant indicating whether auto-generated keys should be returned
     */
    public PreparedStatement prepareStatement(String sql,
            int flag,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "prepareStatement", new Object[] {sql, flag, stmtColEncSetting});
        checkClosed();
        SQLServerPreparedStatement ps = (SQLServerPreparedStatement) prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                stmtColEncSetting);
        ps.bRequestedGeneratedKeys = (flag == Statement.RETURN_GENERATED_KEYS);
        loggerExternal.exiting(getClassNameLogging(), "prepareStatement", ps);
        return ps;
    }

    /* L3 */ public PreparedStatement prepareStatement(String sql,
            int[] columnIndexes) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "prepareStatement", new Object[] {sql, columnIndexes});
        SQLServerPreparedStatement ps = (SQLServerPreparedStatement) prepareStatement(sql, columnIndexes,
                SQLServerStatementColumnEncryptionSetting.UseConnectionSetting);

        loggerExternal.exiting(getClassNameLogging(), "prepareStatement", ps);
        return ps;
    }

    /**
     * Creates a default <code>PreparedStatement</code> object capable of returning the auto-generated keys designated by the given array. This array
     * contains the indexes of the columns in the target table that contain the auto-generated keys that should be made available. The driver will
     * ignore the array if the SQL statement is not an <code>INSERT</code> statement, or an SQL statement able to return auto-generated keys (the list
     * of such statements is vendor-specific).
     * <p>
     * An SQL statement with or without IN parameters can be pre-compiled and stored in a <code>PreparedStatement</code> object. This object can then
     * be used to efficiently execute this statement multiple times.
     * <P>
     * <B>Note:</B> This method is optimized for handling parametric SQL statements that benefit from precompilation. If the driver supports
     * precompilation, the method <code>prepareStatement</code> will send the statement to the database for precompilation. Some drivers may not
     * support precompilation. In this case, the statement may not be sent to the database until the <code>PreparedStatement</code> object is
     * executed. This has no direct effect on users; however, it does affect which methods throw certain SQLExceptions.
     * <P>
     * Result sets created using the returned <code>PreparedStatement</code> object will by default be type <code>TYPE_FORWARD_ONLY</code> and have a
     * concurrency level of <code>CONCUR_READ_ONLY</code>. The holdability of the created result sets can be determined by calling
     * {@link #getHoldability}.
     *
     * @param sql
     *            an SQL statement that may contain one or more '?' IN parameter placeholders
     * @param columnIndexes
     *            an array of column indexes indicating the columns that should be returned from the inserted row or rows
     * @param stmtColEncSetting
     *            Specifies how data will be sent and received when reading and writing encrypted columns.
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled statement, that is capable of returning the auto-generated
     *         keys designated by the given array of column indexes
     * @throws SQLServerException
     *             if a database access error occurs or this method is called on a closed connection
     */
    public PreparedStatement prepareStatement(String sql,
            int[] columnIndexes,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "prepareStatement", new Object[] {sql, columnIndexes, stmtColEncSetting});

        checkClosed();
        if (columnIndexes == null || columnIndexes.length != 1) {
            SQLServerException.makeFromDriverError(this, this, SQLServerException.getErrString("R_invalidColumnArrayLength"), null, false);
        }
        SQLServerPreparedStatement ps = (SQLServerPreparedStatement) prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                stmtColEncSetting);
        ps.bRequestedGeneratedKeys = true;
        loggerExternal.exiting(getClassNameLogging(), "prepareStatement", ps);
        return ps;
    }

    /* L3 */ public PreparedStatement prepareStatement(String sql,
            String[] columnNames) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "prepareStatement", new Object[] {sql, columnNames});

        SQLServerPreparedStatement ps = (SQLServerPreparedStatement) prepareStatement(sql, columnNames,
                SQLServerStatementColumnEncryptionSetting.UseConnectionSetting);

        loggerExternal.exiting(getClassNameLogging(), "prepareStatement", ps);
        return ps;
    }

    /**
     * Creates a default <code>PreparedStatement</code> object capable of returning the auto-generated keys designated by the given array. This array
     * contains the names of the columns in the target table that contain the auto-generated keys that should be returned. The driver will ignore the
     * array if the SQL statement is not an <code>INSERT</code> statement, or an SQL statement able to return auto-generated keys (the list of such
     * statements is vendor-specific).
     * <P>
     * An SQL statement with or without IN parameters can be pre-compiled and stored in a <code>PreparedStatement</code> object. This object can then
     * be used to efficiently execute this statement multiple times.
     * <P>
     * <B>Note:</B> This method is optimized for handling parametric SQL statements that benefit from precompilation. If the driver supports
     * precompilation, the method <code>prepareStatement</code> will send the statement to the database for precompilation. Some drivers may not
     * support precompilation. In this case, the statement may not be sent to the database until the <code>PreparedStatement</code> object is
     * executed. This has no direct effect on users; however, it does affect which methods throw certain SQLExceptions.
     * <P>
     * Result sets created using the returned <code>PreparedStatement</code> object will by default be type <code>TYPE_FORWARD_ONLY</code> and have a
     * concurrency level of <code>CONCUR_READ_ONLY</code>. The holdability of the created result sets can be determined by calling
     * {@link #getHoldability}.
     *
     * @param sql
     *            an SQL statement that may contain one or more '?' IN parameter placeholders
     * @param columnNames
     *            an array of column names indicating the columns that should be returned from the inserted row or rows
     * @param stmtColEncSetting
     *            Specifies how data will be sent and received when reading and writing encrypted columns.
     * @return a new <code>PreparedStatement</code> object, containing the pre-compiled statement, that is capable of returning the auto-generated
     *         keys designated by the given array of column names
     * @throws SQLServerException
     *             if a database access error occurs or this method is called on a closed connection
     */
    public PreparedStatement prepareStatement(String sql,
            String[] columnNames,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "prepareStatement", new Object[] {sql, columnNames, stmtColEncSetting});
        checkClosed();
        if (columnNames == null || columnNames.length != 1) {
            SQLServerException.makeFromDriverError(this, this, SQLServerException.getErrString("R_invalidColumnArrayLength"), null, false);
        }
        SQLServerPreparedStatement ps = (SQLServerPreparedStatement) prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                stmtColEncSetting);
        ps.bRequestedGeneratedKeys = true;
        loggerExternal.exiting(getClassNameLogging(), "prepareStatement", ps);
        return ps;
    }

    /* JDBC 3.0 Savepoints */

    public void releaseSavepoint(Savepoint savepoint) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "releaseSavepoint", savepoint);
        NotImplemented();
    }

    final private Savepoint setNamedSavepoint(String sName) throws SQLServerException {
        if (true == databaseAutoCommitMode) {
            SQLServerException.makeFromDriverError(this, this, SQLServerException.getErrString("R_cantSetSavepoint"), null, false);
        }

        SQLServerSavepoint s = new SQLServerSavepoint(this, sName);

        // Create the named savepoint. Note that we explicitly start a transaction if we
        // are not already in one. This is to allow the savepoint to be created even if
        // setSavepoint() is called before executing any other implicit-transaction-starting
        // statements. Also note that the way we create this transaction is rather weird.
        // This is because the server creates a nested transaction (@@TRANCOUNT = 2) rather
        // than just the outer transaction (@@TRANCOUNT = 1). Should this limitation ever
        // change, the T-SQL below should still work.
        connectionCommand("IF @@TRANCOUNT = 0 BEGIN BEGIN TRAN IF @@TRANCOUNT = 2 COMMIT TRAN END SAVE TRAN " + Util.escapeSQLId(s.getLabel()),
                "setSavepoint");

        return s;
    }

    /* L3 */ public Savepoint setSavepoint(String sName) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "setSavepoint", sName);
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        Savepoint pt = setNamedSavepoint(sName);
        loggerExternal.exiting(getClassNameLogging(), "setSavepoint", pt);
        return pt;
    }

    /* L3 */ public Savepoint setSavepoint() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "setSavepoint");
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        Savepoint pt = setNamedSavepoint(null);
        loggerExternal.exiting(getClassNameLogging(), "setSavepoint", pt);
        return pt;
    }

    /* L3 */ public void rollback(Savepoint s) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "rollback", s);
        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkClosed();
        if (true == databaseAutoCommitMode) {
            SQLServerException.makeFromDriverError(this, this, SQLServerException.getErrString("R_cantInvokeRollback"), null, false);
        }
        connectionCommand("IF @@TRANCOUNT > 0 ROLLBACK TRAN " + Util.escapeSQLId(((SQLServerSavepoint) s).getLabel()), "rollbackSavepoint");
        loggerExternal.exiting(getClassNameLogging(), "rollback");
    }

    /* L3 */ public int getHoldability() throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getHoldability");
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.exiting(getClassNameLogging(), "getHoldability", holdability);
        return holdability;
    }

    public void setHoldability(int holdability) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "setHoldability", holdability);

        if (loggerExternal.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
            loggerExternal.finer(toString() + " ActivityId: " + ActivityCorrelator.getNext().toString());
        }
        checkValidHoldability(holdability);
        checkClosed();

        if (this.holdability != holdability) {
            assert ResultSet.HOLD_CURSORS_OVER_COMMIT == holdability || ResultSet.CLOSE_CURSORS_AT_COMMIT == holdability : "invalid holdability "
                    + holdability;

            connectionCommand((holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT) ? "SET CURSOR_CLOSE_ON_COMMIT ON" : "SET CURSOR_CLOSE_ON_COMMIT OFF",
                    "setHoldability");

            this.holdability = holdability;
        }

        loggerExternal.exiting(getClassNameLogging(), "setHoldability");
    }

    public int getNetworkTimeout() throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getNetworkTimeout");

        checkClosed();

        int timeout = 0;
        try {
            timeout = tdsChannel.getNetworkTimeout();
        }
        catch (IOException ioe) {
            terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, ioe.getMessage(), ioe);
        }

        loggerExternal.exiting(getClassNameLogging(), "getNetworkTimeout");
        return timeout;
    }

    public void setNetworkTimeout(Executor executor,
            int timeout) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "setNetworkTimeout", timeout);

        if (timeout < 0) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidSocketTimeout"));
            Object[] msgArgs = {timeout};
            SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, false);
        }

        checkClosed();
        
        // check for setNetworkTimeout permission
        SecurityManager secMgr = System.getSecurityManager();
        if (secMgr != null) {
            try {
                SQLPermission perm = new SQLPermission(SET_NETWORK_TIMEOUT_PERM);
                secMgr.checkPermission(perm);
            }
            catch (SecurityException ex) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_permissionDenied"));
                Object[] msgArgs = {SET_NETWORK_TIMEOUT_PERM};
                SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, true);
            }
        }

        try {
            tdsChannel.setNetworkTimeout(timeout);
        }
        catch (IOException ioe) {
            terminate(SQLServerException.DRIVER_ERROR_IO_FAILED, ioe.getMessage(), ioe);
        }

        loggerExternal.exiting(getClassNameLogging(), "setNetworkTimeout");
    }

    public String getSchema() throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getSchema");

        checkClosed();

        SQLServerStatement stmt = null;
        SQLServerResultSet resultSet = null;

        try {
            stmt = (SQLServerStatement) this.createStatement();
            resultSet = stmt.executeQueryInternal("SELECT SCHEMA_NAME()");
            if (resultSet != null) {
                resultSet.next();
                return resultSet.getString(1);
            }
            else {
                SQLServerException.makeFromDriverError(this, this, SQLServerException.getErrString("R_getSchemaError"), null, true);
            }
        }
        catch (SQLException e) {
            if (isSessionUnAvailable()) {
                throw e;
            }

            SQLServerException.makeFromDriverError(this, this, SQLServerException.getErrString("R_getSchemaError"), null, true);
        }
        finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (stmt != null) {
                stmt.close();
            }
        }

        loggerExternal.exiting(getClassNameLogging(), "getSchema");
        return null;
    }

    public void setSchema(String schema) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "setSchema", schema);
        checkClosed();
        addWarning(SQLServerException.getErrString("R_setSchemaWarning"));

        loggerExternal.exiting(getClassNameLogging(), "setSchema");
    }

    /**
     * Modifies the setting of the sendTimeAsDatetime connection property. When true, java.sql.Time values will be sent to the server as SQL
     * Serverdatetime values. When false, java.sql.Time values will be sent to the server as SQL Servertime values. sendTimeAsDatetime can also be
     * modified programmatically with SQLServerDataSource.setSendTimeAsDatetime. The default value for this property may change in a future release.
     * 
     * @param sendTimeAsDateTimeValue
     *            enables/disables setting the sendTimeAsDatetime connection property. For more information about how the Microsoft JDBC Driver for
     *            SQL Server configures java.sql.Time values before sending them to the server, see
     *            <a href="https://msdn.microsoft.com/en-us/library/ff427224(v=sql.110).aspx">Configuring How java.sql.Time Values are Sent to the
     *            Server</a>.
     */
    public synchronized void setSendTimeAsDatetime(boolean sendTimeAsDateTimeValue) {
        sendTimeAsDatetime = sendTimeAsDateTimeValue;
    }

    public java.sql.Array createArrayOf(String typeName,
            Object[] elements) throws SQLException {
        // Not implemented
        throw new SQLFeatureNotSupportedException(SQLServerException.getErrString("R_notSupported"));
    }

    public Blob createBlob() throws SQLException {
        checkClosed();
        return new SQLServerBlob(this);
    }

    public Clob createClob() throws SQLException {
        checkClosed();
        return new SQLServerClob(this);
    }

    public NClob createNClob() throws SQLException {
        checkClosed();
        return new SQLServerNClob(this);
    }

    public SQLXML createSQLXML() throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "createSQLXML");
         SQLXML sqlxml = new SQLServerSQLXML(this);

        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.exiting(getClassNameLogging(), "createSQLXML", sqlxml);
        return sqlxml;
    }

    public Struct createStruct(String typeName,
            Object[] attributes) throws SQLException {
        // Not implemented
        throw new SQLFeatureNotSupportedException(SQLServerException.getErrString("R_notSupported"));
    }

    String getTrustedServerNameAE() throws SQLServerException {
        return trustedServerNameAE.toUpperCase();
    }

    public Properties getClientInfo() throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getClientInfo");
        checkClosed();
        Properties p = new Properties();
        loggerExternal.exiting(getClassNameLogging(), "getClientInfo", p);
        return p;
    }

    public String getClientInfo(String name) throws SQLException {
        loggerExternal.entering(getClassNameLogging(), "getClientInfo", name);
        checkClosed();
        loggerExternal.exiting(getClassNameLogging(), "getClientInfo", null);
        return null;
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        loggerExternal.entering(getClassNameLogging(), "setClientInfo", properties);
        // This function is only marked as throwing only SQLClientInfoException so the conversion is necessary
        try {
            checkClosed();
        }
        catch (SQLServerException ex) {
            SQLClientInfoException info = new SQLClientInfoException();
            info.initCause(ex);
            throw info;
        }

        if (!properties.isEmpty()) {
            Enumeration<?> e = properties.keys();
            while (e.hasMoreElements()) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidProperty"));
                Object[] msgArgs = {e.nextElement()};
                addWarning(form.format(msgArgs));
            }
        }
        loggerExternal.exiting(getClassNameLogging(), "setClientInfo");
    }

    public void setClientInfo(String name,
            String value) throws SQLClientInfoException {
        loggerExternal.entering(getClassNameLogging(), "setClientInfo", new Object[] {name, value});
        // This function is only marked as throwing only SQLClientInfoException so the conversion is necessary
        try {
            checkClosed();
        }
        catch (SQLServerException ex) {
            SQLClientInfoException info = new SQLClientInfoException();
            info.initCause(ex);
            throw info;
        }
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidProperty"));
        Object[] msgArgs = {name};
        addWarning(form.format(msgArgs));
        loggerExternal.exiting(getClassNameLogging(), "setClientInfo");
    }

    /**
     * Determine whether the connection is still valid.
     *
     * The driver shall submit a query on the connection or use some other mechanism that positively verifies the connection is still valid when this
     * method is called.
     *
     * The query submitted by the driver to validate the connection shall be executed in the context of the current transaction.
     *
     * @param timeout
     *            The time in seconds to wait for the database operation used to validate the connection to complete. If the timeout period expires
     *            before the operation completes, this method returns false. A value of 0 indicates a timeout is not applied to the database
     *            operation. Note that if the value is 0, the call to isValid may block indefinitely if the connection is not valid...
     *
     * @return true if the connection has not been closed and is still valid.
     *
     * @throws SQLException
     *             if the value supplied for the timeout is less than 0.
     */
    public boolean isValid(int timeout) throws SQLException {
        boolean isValid = false;

        loggerExternal.entering(getClassNameLogging(), "isValid", timeout);

        // Throw an exception if the timeout is invalid
        if (timeout < 0) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidQueryTimeOutValue"));
            Object[] msgArgs = {timeout};
            SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), null, true);
        }

        // Return false if the connection is closed
        if (isSessionUnAvailable())
            return false;

        try {
            SQLServerStatement stmt = new SQLServerStatement(this, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                    SQLServerStatementColumnEncryptionSetting.UseConnectionSetting);

            // If asked, limit the time to wait for the query to complete.
            if (0 != timeout)
                stmt.setQueryTimeout(timeout);

            // Try to execute the query. If this succeeds, then the connection is valid.
            // If it fails (throws an exception), then the connection is not valid.
            // If a timeout was provided, execution throws an "query timed out" exception
            // if the query fails to execute in that time.
            stmt.executeQueryInternal("SELECT 1");
            stmt.close();
            isValid = true;
        }
        catch (SQLException e) {
            // Do not propagate SQLExceptions from query execution or statement closure.
            // The connection is considered to be invalid if the statement fails to close,
            // even though query execution succeeded.
            connectionlogger.fine(toString() + " Exception checking connection validity: " + e.getMessage());
        }

        loggerExternal.exiting(getClassNameLogging(), "isValid", isValid);
        return isValid;
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

            SQLServerException newe = new SQLServerException(e.getMessage(), e);
            throw newe;
        }
        loggerExternal.exiting(getClassNameLogging(), "unwrap", t);
        return t;
    }

    public void beginRequest() throws SQLFeatureNotSupportedException {
        DriverJDBCVersion.checkSupportsJDBC43();
        throw new SQLFeatureNotSupportedException("beginRequest not implemented");
    }

    public void endRequest() throws SQLFeatureNotSupportedException {
        DriverJDBCVersion.checkSupportsJDBC43();
        throw new SQLFeatureNotSupportedException("endRequest not implemented");
    }

    /**
     * Replace JDBC syntax parameter markets '?' with SQL Server paramter markers @p1, @p2 etc...
     * 
     * @param sql
     *            the user's SQL
     * @throws SQLServerException
     * @return the returned syntax
     */
    static final char[] OUT = {' ', 'O', 'U', 'T'};

    /* L0 */ String replaceParameterMarkers(String sqlSrc,
            Parameter[] params,
            boolean isReturnValueSyntax) throws SQLServerException {
        final int MAX_PARAM_NAME_LEN = 6;
        char[] sqlDst = new char[sqlSrc.length() + params.length * (MAX_PARAM_NAME_LEN + OUT.length)];
        int dstBegin = 0;
        int srcBegin = 0;
        int nParam = 0;

        int paramIndex = 0;
        while (true) {
            int srcEnd = ParameterUtils.scanSQLForChar('?', sqlSrc, srcBegin);
            sqlSrc.getChars(srcBegin, srcEnd, sqlDst, dstBegin);
            dstBegin += srcEnd - srcBegin;

            if (sqlSrc.length() == srcEnd)
                break;

            dstBegin += makeParamName(nParam++, sqlDst, dstBegin);
            srcBegin = srcEnd + 1;

            if (params[paramIndex++].isOutput()) {
                if (!isReturnValueSyntax || paramIndex > 1) {
                    System.arraycopy(OUT, 0, sqlDst, dstBegin, OUT.length);
                    dstBegin += OUT.length;
                }
            }
        }

        while (dstBegin < sqlDst.length)
            sqlDst[dstBegin++] = ' ';

        return new String(sqlDst);
    }

    /**
     * Make a SQL Server style parameter name.
     * 
     * @param nParam
     *            the parameter number
     * @param name
     *            the paramter name
     * @param offset
     * @return int
     */
    /* L0 */ static int makeParamName(int nParam,
            char[] name,
            int offset) {
        name[offset + 0] = '@';
        name[offset + 1] = 'P';
        if (nParam < 10) {
            name[offset + 2] = (char) ('0' + nParam);
            return 3;
        }
        else {
            if (nParam < 100) {
                int nBase = 2;
                while (true) { // make a char[] representation of the param number 2.26
                    if (nParam < nBase * 10) {
                        name[offset + 2] = (char) ('0' + (nBase - 1));
                        name[offset + 3] = (char) ('0' + (nParam - ((nBase - 1) * 10)));
                        return 4;
                    }
                    nBase++;
                }
            }
            else {
                String sParam = "" + nParam;
                sParam.getChars(0, sParam.length(), name, offset + 2);
                return 2 + sParam.length();
            }
        }
    }

    // Notify any interested parties (e.g. pooling managers) of a ConnectionEvent activity
    // on the connection. Calling notifyPooledConnection with null event will place this
    // connection back in the pool. Calling notifyPooledConnection with a non-null event is
    // used to notify the pooling manager that the connection is bad and should be removed
    // from the pool.
    void notifyPooledConnection(SQLServerException e) {
        synchronized (this) {
            if (null != pooledConnectionParent) {
                pooledConnectionParent.notifyEvent(e);
            }
        }

    }

    // Detaches this connection from connection pool.
    void DetachFromPool() {
        synchronized (this) {
            pooledConnectionParent = null;
        }
    }

    /**
     * Determine the listening port of a named SQL Server instance.
     * 
     * @param server
     *            the server name
     * @param instanceName
     *            the instance
     * @throws SQLServerException
     * @return the instance's port
     */
    private static final int BROWSER_PORT = 1434;

    String getInstancePort(String server,
            String instanceName) throws SQLServerException {
        String browserResult = null;
        DatagramSocket datagramSocket = null;
        String lastErrorMessage = null;

        try {
            lastErrorMessage = "Failed to determine instance for the : " + server + " instance:" + instanceName;

            // First we create a datagram socket
            try {
                datagramSocket = new DatagramSocket();
                datagramSocket.setSoTimeout(1000);
            }
            catch (SocketException socketException) {
                // Errors creating a local socket
                // Log the error and bail.
                lastErrorMessage = "Unable to create local datagram socket";
                throw socketException;
            }

            // Second, we need to get the IP address of the server to which we'll send the UDP request.
            // This may require a DNS lookup, which may fail due to transient conditions, so retry after logging the first time.

            // send UDP packet
            assert null != datagramSocket;
            try {
                if (multiSubnetFailover) {
                    // If instance name is specified along with multiSubnetFailover, we get all IPs resolved by server name
                    InetAddress[] inetAddrs = InetAddress.getAllByName(server);
                    assert null != inetAddrs;
                    for (InetAddress inetAddr : inetAddrs) {
                        // Send the UDP request
                        try {
                            byte sendBuffer[] = (" " + instanceName).getBytes();
                            sendBuffer[0] = 4;
                            DatagramPacket udpRequest = new DatagramPacket(sendBuffer, sendBuffer.length, inetAddr, BROWSER_PORT);
                            datagramSocket.send(udpRequest);
                        }
                        catch (IOException ioException) {
                            lastErrorMessage = "Error sending SQL Server Browser Service UDP request to address: " + inetAddr + ", port: "
                                    + BROWSER_PORT;
                            throw ioException;
                        }
                    }
                }
                else {
                    // If instance name is not specified along with multiSubnetFailover, we resolve only the first IP for server name
                    InetAddress inetAddr = InetAddress.getByName(server);

                    assert null != inetAddr;
                    // Send the UDP request
                    try {
                        byte sendBuffer[] = (" " + instanceName).getBytes();
                        sendBuffer[0] = 4;
                        DatagramPacket udpRequest = new DatagramPacket(sendBuffer, sendBuffer.length, inetAddr, BROWSER_PORT);
                        datagramSocket.send(udpRequest);
                    }
                    catch (IOException ioException) {
                        lastErrorMessage = "Error sending SQL Server Browser Service UDP request to address: " + inetAddr + ", port: " + BROWSER_PORT;
                        throw ioException;
                    }
                }
            }
            catch (UnknownHostException unknownHostException) {
                lastErrorMessage = "Unable to determine IP address of host: " + server;
                throw unknownHostException;
            }

            // Receive the UDP response
            try {
                byte receiveBuffer[] = new byte[4096];
                DatagramPacket udpResponse = new DatagramPacket(receiveBuffer, receiveBuffer.length);
                datagramSocket.receive(udpResponse);
                browserResult = new String(receiveBuffer, 3, receiveBuffer.length - 3);
                if (connectionlogger.isLoggable(Level.FINER))
                    connectionlogger.fine(
                            toString() + " Received SSRP UDP response from IP address: " + udpResponse.getAddress().getHostAddress());
            }
            catch (IOException ioException) {
                // Warn and retry
                lastErrorMessage = "Error receiving SQL Server Browser Service UDP response from server: " + server;
                throw ioException;
            }
        }
        catch (IOException ioException) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_sqlBrowserFailed"));
            Object[] msgArgs = {server, instanceName, ioException.toString()};
            connectionlogger.log(Level.FINE, toString() + " " + lastErrorMessage, ioException);
            SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), SQLServerException.EXCEPTION_XOPEN_CONNECTION_CANT_ESTABLISH,
                    false);
        }
        finally {
            if (null != datagramSocket)
                datagramSocket.close();
        }
        assert null != browserResult;
        // If the server isn't configured for TCP then say so and fail
        int p = browserResult.indexOf("tcp;");
        if (-1 == p) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_notConfiguredToListentcpip"));
            Object[] msgArgs = {instanceName};
            SQLServerException.makeFromDriverError(this, this, form.format(msgArgs), SQLServerException.EXCEPTION_XOPEN_CONNECTION_CANT_ESTABLISH,
                    false);
        }
        // All went well, so return the TCP port of the SQL Server instance
        int p1 = p + 4;
        int p2 = browserResult.indexOf(';', p1);
        return browserResult.substring(p1, p2);
    }

    /* L0 */ int getNextSavepointId() {
        nNextSavePointId++; // Make them unique for this connection
        return nNextSavePointId;
    }

    // Returns this connection's SQLServerConnectionSecurityManager class to caller.
    // Used by SQLServerPooledConnection to verify security when passing out Connection objects.
    void doSecurityCheck() {
        assert null != currentConnectPlaceHolder;
        currentConnectPlaceHolder.doSecurityCheck();
    }

    // ColumnEncryptionKeyCache sets time-to-live for column encryption key entries in
    // the column encryption key cache for the Always Encrypted feature. The default value is 2 hours.
    // This variable holds the value in seconds.
    private static long columnEncryptionKeyCacheTtl = TimeUnit.SECONDS.convert(2, TimeUnit.HOURS);

    /**
     * Sets time-to-live for column encryption key entries in the column encryption key cache for the Always Encrypted feature. The default value is 2
     * hours. This variable holds the value in seconds.
     * 
     * @param columnEncryptionKeyCacheTTL
     *            The timeunit in seconds
     * @param unit
     *            The Timeunit.
     * @throws SQLServerException
     *             when an error occurs
     */
    public static synchronized void setColumnEncryptionKeyCacheTtl(int columnEncryptionKeyCacheTTL,
            TimeUnit unit) throws SQLServerException {
        if (columnEncryptionKeyCacheTTL < 0 || unit.equals(TimeUnit.MILLISECONDS) || unit.equals(TimeUnit.MICROSECONDS)
                || unit.equals(TimeUnit.NANOSECONDS)) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_invalidCEKCacheTtl"), null, 0, false);
        }

        columnEncryptionKeyCacheTtl = TimeUnit.SECONDS.convert(columnEncryptionKeyCacheTTL, unit);
    }

    static synchronized long getColumnEncryptionKeyCacheTtl() {
        return columnEncryptionKeyCacheTtl;
    }
    

    /**
     * Enqueue a discarded prepared statement handle to be clean-up on the server.
     * 
     * @param statementHandle
     *      The prepared statement handle that should be scheduled for unprepare.
     */
    final void enqueueUnprepareStatementHandle(PreparedStatementHandle statementHandle) {
        if(null == statementHandle)
            return;

        if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
            loggerExternal.finer(this + ": Adding PreparedHandle to queue for un-prepare:" + statementHandle.getHandle());

        // Add the new handle to the discarding queue and find out current # enqueued.
        this.discardedPreparedStatementHandles.add(statementHandle);
        this.discardedPreparedStatementHandleCount.incrementAndGet();
    }


    /**
     * Returns the number of currently outstanding prepared statement un-prepare actions.
     * 
     * @return Returns the current value per the description.
     */
    public int getDiscardedServerPreparedStatementCount() {
        return this.discardedPreparedStatementHandleCount.get();
    }

    /**
     * Forces the un-prepare requests for any outstanding discarded prepared statements to be executed.
     */
    public void closeUnreferencedPreparedStatementHandles() {
        this.unprepareUnreferencedPreparedStatementHandles(true);
    }

    /**
     * Remove references to outstanding un-prepare requests. Should be run when connection is closed.
     */
    private final void cleanupPreparedStatementDiscardActions() {
        discardedPreparedStatementHandles.clear();
        discardedPreparedStatementHandleCount.set(0);
    }

    /**
     * Returns the behavior for a specific connection instance. If false the first execution will call sp_executesql and not prepare 
     * a statement, once the second execution happens it will call sp_prepexec and actually setup a prepared statement handle. Following
     * executions will call sp_execute. This relieves the need for sp_unprepare on prepared statement close if the statement is only
     * executed once. The default for this option can be changed by calling setDefaultEnablePrepareOnFirstPreparedStatementCall(). 
     * 
     * @return Returns the current setting per the description.
     */
    public boolean getEnablePrepareOnFirstPreparedStatementCall() {
        if(null == this.enablePrepareOnFirstPreparedStatementCall)
            return DEFAULT_ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT_CALL;
        else
            return this.enablePrepareOnFirstPreparedStatementCall;        
    }

    /**
     * Specifies the behavior for a specific connection instance. If value is false the first execution will call sp_executesql and not prepare 
     * a statement, once the second execution happens it will call sp_prepexec and actually setup a prepared statement handle. Following
     * executions will call sp_execute. This relieves the need for sp_unprepare on prepared statement close if the statement is only
     * executed once.  
     * 
     * @param value
     *      Changes the setting per the description.
     */
    public void setEnablePrepareOnFirstPreparedStatementCall(boolean value) {
        this.enablePrepareOnFirstPreparedStatementCall = value;
    }

    /**
     * Returns the behavior for a specific connection instance. This setting controls how many outstanding prepared statement discard actions
     * (sp_unprepare) can be outstanding per connection before a call to clean-up the outstanding handles on the server is executed. If the setting is
     * {@literal <=} 1, unprepare actions will be executed immedietely on prepared statement close. If it is set to {@literal >} 1, these calls
     * will be batched together to avoid overhead of calling sp_unprepare too often. The default for this option can be changed by calling
     * getDefaultServerPreparedStatementDiscardThreshold().
     * 
     * @return Returns the current setting per the description.
     */
    public int getServerPreparedStatementDiscardThreshold() {
        if (0 > this.serverPreparedStatementDiscardThreshold)
            return DEFAULT_SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD;
        else
            return this.serverPreparedStatementDiscardThreshold;
    }

    /**
     * Specifies the behavior for a specific connection instance. This setting controls how many outstanding prepared statement discard actions
     * (sp_unprepare) can be outstanding per connection before a call to clean-up the outstanding handles on the server is executed. If the setting is
     * {@literal <=} 1 unprepare actions will be executed immedietely on prepared statement close. If it is set to {@literal >} 1 these calls will be
     * batched together to avoid overhead of calling sp_unprepare too often.
     * 
     * @param value
     *            Changes the setting per the description.
     */
    public void setServerPreparedStatementDiscardThreshold(int value) {
        this.serverPreparedStatementDiscardThreshold = Math.max(0, value);
    }

    final boolean isPreparedStatementUnprepareBatchingEnabled() {
    	return 1 < getServerPreparedStatementDiscardThreshold();
    }

    /**
     * Cleans-up discarded prepared statement handles on the server using batched un-prepare actions if the batching threshold has been reached.
     * 
     * @param force 
     *      When force is set to true we ignore the current threshold for if the discard actions should run and run them anyway.
     */
    final void unprepareUnreferencedPreparedStatementHandles(boolean force) {
        // Skip out if session is unavailable to adhere to previous non-batched behavior.
        if (isSessionUnAvailable()) 
            return;

        final int threshold = getServerPreparedStatementDiscardThreshold();

        // Met threshold to clean-up?
        if (force || threshold < getDiscardedServerPreparedStatementCount()) {

            // Create batch of sp_unprepare statements.
            StringBuilder sql = new StringBuilder(threshold  * 32/*EXEC sp_cursorunprepare++;*/);

            // Build the string containing no more than the # of handles to remove.
            // Note that sp_unprepare can fail if the statement is already removed. 
            // However, the server will only abort that statement and continue with 
            // the remaining clean-up.
            int handlesRemoved = 0;
            PreparedStatementHandle statementHandle = null;

            while (null != (statementHandle = discardedPreparedStatementHandles.poll())){
                ++handlesRemoved;
                
                sql.append(statementHandle.isDirectSql() ? "EXEC sp_unprepare " : "EXEC sp_cursorunprepare ")
                    .append(statementHandle.getHandle())
                    .append(';');
            }

            try {
                // Execute the batched set.
                try(Statement stmt = this.createStatement()) {
                    stmt.execute(sql.toString());
                }

                if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
                    loggerExternal.finer(this + ": Finished un-preparing handle count:" + handlesRemoved);
            }
            catch(SQLException e) {
                if (loggerExternal.isLoggable(java.util.logging.Level.FINER))
                    loggerExternal.log(Level.FINER, this + ": Error batch-closing at least one prepared handle", e);
            }

            // Decrement threshold counter
            discardedPreparedStatementHandleCount.addAndGet(-handlesRemoved);
        }
    }

    /**
     * Returns true if statement pooling is disabled.
     * 
     * @return
     */
    public boolean getDisableStatementPooling() {
        return this.disableStatementPooling;
    }

    /**
     * Sets statement pooling to true or false;
     * 
     * @param value
     */
    public void setDisableStatementPooling(boolean value) {
        this.disableStatementPooling = value;
        if (!value && 0 < this.getStatementPoolingCacheSize()) {
            prepareCache();
        }
    }
    
    /**
     * Returns the size of the prepared statement cache for this connection. A value less than 1 means no cache.
     * @return Returns the current setting per the description.
     */
    public int getStatementPoolingCacheSize() {
        return statementPoolingCacheSize;
    }   

    /**
     * Returns the current number of pooled prepared statement handles.
     * @return Returns the current setting per the description.
     */
    public int getStatementHandleCacheEntryCount() {
        if(!isStatementPoolingEnabled())
            return 0;
        else
            return this.preparedStatementHandleCache.size();
    }

    /**
     * Whether statement pooling is enabled or not for this connection.
     * @return Returns the current setting per the description.
     */
    public boolean isStatementPoolingEnabled() {
        return null != preparedStatementHandleCache && 0 < this.getStatementPoolingCacheSize() && !this.getDisableStatementPooling();
    }

    /**
     * Specifies the size of the prepared statement cache for this connection. A value less than 1 means no cache.
     * 
     * @param value
     *            The new cache size.
     * 
     */
    public void setStatementPoolingCacheSize(int value) {
        value = Math.max(0, value);
        statementPoolingCacheSize = value;

        if (!this.disableStatementPooling && value > 0) {
            prepareCache();
        }
        if (null != preparedStatementHandleCache)
            preparedStatementHandleCache.setCapacity(value);

        if (null != parameterMetadataCache)
            parameterMetadataCache.setCapacity(value);
    }

    /**
     * Internal method to prepare the cache handle
     * @param value
     */
    private void prepareCache() {
        preparedStatementHandleCache = new Builder<Sha1HashKey, PreparedStatementHandle>().maximumWeightedCapacity(getStatementPoolingCacheSize())
                .listener(new PreparedStatementCacheEvictionListener()).build();

        parameterMetadataCache = new Builder<Sha1HashKey, SQLServerParameterMetaData>().maximumWeightedCapacity(getStatementPoolingCacheSize())
                .build();
    }

    /** Get a parameter metadata cache entry if statement pooling is enabled */
    final SQLServerParameterMetaData getCachedParameterMetadata(Sha1HashKey key) {
        if(!isStatementPoolingEnabled())
            return null;
        
        return parameterMetadataCache.get(key);
    }

    /** Register a parameter metadata cache entry if statement pooling is enabled */
    final void registerCachedParameterMetadata(Sha1HashKey key, SQLServerParameterMetaData pmd) {
        if(!isStatementPoolingEnabled() || null == pmd)
            return;
        
        parameterMetadataCache.put(key, pmd);
    }

    /** Get or create prepared statement handle cache entry if statement pooling is enabled */
    final PreparedStatementHandle getCachedPreparedStatementHandle(Sha1HashKey key) {
        if(!isStatementPoolingEnabled())
            return null;
        
        return preparedStatementHandleCache.get(key);
    }

    /** Get or create prepared statement handle cache entry if statement pooling is enabled */
    final PreparedStatementHandle registerCachedPreparedStatementHandle(Sha1HashKey key, int handle, boolean isDirectSql) {
        if(!isStatementPoolingEnabled() || null == key)
            return null;
        
        PreparedStatementHandle cacheItem = new PreparedStatementHandle(key, handle, isDirectSql, false);
        preparedStatementHandleCache.putIfAbsent(key, cacheItem);
        return cacheItem;
    }

    /** Return prepared statement handle cache entry so it can be un-prepared. */
    final void returnCachedPreparedStatementHandle(PreparedStatementHandle handle) {
        handle.removeReference();

        if (handle.isEvictedFromCache() && handle.tryDiscardHandle())
            enqueueUnprepareStatementHandle(handle);
    }

    /** Force eviction of prepared statement handle cache entry. */
    final void evictCachedPreparedStatementHandle(PreparedStatementHandle handle) {
    	if(null == handle || null == handle.getKey())
    		return;
    	
    	preparedStatementHandleCache.remove(handle.getKey());
    }

    // Handle closing handles when removed from cache.
    final class PreparedStatementCacheEvictionListener implements EvictionListener<Sha1HashKey, PreparedStatementHandle> {
        public void onEviction(Sha1HashKey key, PreparedStatementHandle handle) {
            if(null != handle) {
                handle.setIsEvictedFromCache(true); // Mark as evicted from cache.

                // Only discard if not referenced.
                if(handle.tryDiscardHandle()) {
                    enqueueUnprepareStatementHandle(handle);
                    // Do not run discard actions here! Can interfere with executing statement.
                }                    
            }
        }
    }
}

// Helper class for security manager functions used by SQLServerConnection class.
final class SQLServerConnectionSecurityManager {
    static final String dllName = "sqljdbc_auth.dll";
    String serverName;
    int portNumber;

    SQLServerConnectionSecurityManager(String serverName,
            int portNumber) {
        this.serverName = serverName;
        this.portNumber = portNumber;
    }

    /**
     * checkConnect will throws a SecurityException if the calling thread is not allowed to open a socket connection to the specified serverName and
     * portNumber.
     * 
     * @throws SecurityException
     *             when an error occurs
     */
    public void checkConnect() throws SecurityException {
        SecurityManager security = System.getSecurityManager();
        if (null != security) {
            security.checkConnect(serverName, portNumber);
        }
    }

    /**
     * Throws a <code>SecurityException</code> if the calling thread is not allowed to dynamic link the library code.
     * 
     * @throws SecurityException
     *             when an error occurs
     */
    public void checkLink() throws SecurityException {
        SecurityManager security = System.getSecurityManager();
        if (null != security) {
            security.checkLink(dllName);
        }
    }
}
