/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.ietf.jgss.GSSCredential;

/**
 * SQLServerDriver implements the java.sql.Driver for SQLServerConnect.
 *
 */

final class SQLServerDriverPropertyInfo {
    private final String name;

    final String getName() {
        return name;
    }

    private final String description;
    private final String defaultValue;
    private final boolean required;
    private final String[] choices;

    SQLServerDriverPropertyInfo(String name,
            String defaultValue,
            boolean required,
            String[] choices) {
        this.name = name;
        this.description = SQLServerResource.getResource("R_" + name + "PropertyDescription");
        this.defaultValue = defaultValue;
        this.required = required;
        this.choices = choices;
    }

    DriverPropertyInfo build(Properties connProperties) {
        String propValue = name.equals(SQLServerDriverStringProperty.PASSWORD.toString()) ? "" : connProperties.getProperty(name);

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
    NotSpecified,
    SqlPassword,
    ActiveDirectoryPassword,
    ActiveDirectoryIntegrated;

    static SqlAuthentication valueOfString(String value) throws SQLServerException {
        SqlAuthentication method = null;

        if (value.toLowerCase(Locale.US).equalsIgnoreCase(SqlAuthentication.NotSpecified.toString())) {
            method = SqlAuthentication.NotSpecified;
        }
        else if (value.toLowerCase(Locale.US).equalsIgnoreCase(SqlAuthentication.SqlPassword.toString())) {
            method = SqlAuthentication.SqlPassword;
        }
        else if (value.toLowerCase(Locale.US).equalsIgnoreCase(SqlAuthentication.ActiveDirectoryPassword.toString())) {
            method = SqlAuthentication.ActiveDirectoryPassword;
        }
        else if (value.toLowerCase(Locale.US).equalsIgnoreCase(SqlAuthentication.ActiveDirectoryIntegrated.toString())) {
            method = SqlAuthentication.ActiveDirectoryIntegrated;
        }
        else {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidConnectionSetting"));
            Object[] msgArgs = {"authentication", value};
            throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
        }
        return method;
    }
}

enum ColumnEncryptionSetting {
    Enabled,
    Disabled;

    static ColumnEncryptionSetting valueOfString(String value) throws SQLServerException {
        ColumnEncryptionSetting method = null;

        if (value.toLowerCase(Locale.US).equalsIgnoreCase(ColumnEncryptionSetting.Enabled.toString())) {
            method = ColumnEncryptionSetting.Enabled;
        }
        else if (value.toLowerCase(Locale.US).equalsIgnoreCase(ColumnEncryptionSetting.Disabled.toString())) {
            method = ColumnEncryptionSetting.Disabled;
        }
        else {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidConnectionSetting"));
            Object[] msgArgs = {"columnEncryptionSetting", value};
            throw new SQLServerException(form.format(msgArgs), null);
        }
        return method;
    }
}

enum SSLProtocol {
    TLS("TLS"),
    TLS_V10("TLSv1"),
    TLS_V11("TLSv1.1"),
    TLS_V12("TLSv1.2"),;

    private final String name;

    private SSLProtocol(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }

    static SSLProtocol valueOfString(String value) throws SQLServerException {
        SSLProtocol protocol = null;

        if (value.toLowerCase(Locale.ENGLISH).equalsIgnoreCase(SSLProtocol.TLS.toString())) {
            protocol = SSLProtocol.TLS;
        }
        else if (value.toLowerCase(Locale.ENGLISH).equalsIgnoreCase(SSLProtocol.TLS_V10.toString())) {
            protocol = SSLProtocol.TLS_V10;
        }
        else if (value.toLowerCase(Locale.ENGLISH).equalsIgnoreCase(SSLProtocol.TLS_V11.toString())) {
            protocol = SSLProtocol.TLS_V11;
        }
        else if (value.toLowerCase(Locale.ENGLISH).equalsIgnoreCase(SSLProtocol.TLS_V12.toString())) {
            protocol = SSLProtocol.TLS_V12;
        }
        else {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidSSLProtocol"));
            Object[] msgArgs = {value};
            throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
        }
        return protocol;
    }
}

enum KeyStoreAuthentication {
    JavaKeyStorePassword;

    static KeyStoreAuthentication valueOfString(String value) throws SQLServerException {
        KeyStoreAuthentication method = null;

        if (value.toLowerCase(Locale.US).equalsIgnoreCase(KeyStoreAuthentication.JavaKeyStorePassword.toString())) {
            method = KeyStoreAuthentication.JavaKeyStorePassword;
        }
        else {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidConnectionSetting"));
            Object[] msgArgs = {"keyStoreAuthentication", value};
            throw new SQLServerException(form.format(msgArgs), null);
        }
        return method;
    }
}

enum AuthenticationScheme {
    nativeAuthentication,
    javaKerberos;
    
    static AuthenticationScheme valueOfString(String value) throws SQLServerException {
        AuthenticationScheme scheme;
        if (value.toLowerCase(Locale.US).equalsIgnoreCase(AuthenticationScheme.javaKerberos.toString())) {
            scheme = AuthenticationScheme.javaKerberos;
        }
        else if (value.toLowerCase(Locale.US).equalsIgnoreCase(AuthenticationScheme.nativeAuthentication.toString())) {
            scheme = AuthenticationScheme.nativeAuthentication;
        }
        else {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidAuthenticationScheme"));
            Object[] msgArgs = {value};
            throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
        }
        return scheme;
    }
}

enum ApplicationIntent {
    READ_WRITE("readwrite"),
    READ_ONLY("readonly");

    // the value of the enum
    private final String value;

    // constructor that sets the string value of the enum
    private ApplicationIntent(String value) {
        this.value = value;
    }

    // returns the string value of enum
    public String toString() {
        return value;
    }

    static ApplicationIntent valueOfString(String value) throws SQLServerException {
        ApplicationIntent applicationIntent = ApplicationIntent.READ_WRITE;
        assert value != null;
        // handling turkish i issues
        value = value.toUpperCase(Locale.US).toLowerCase(Locale.US);
        if (value.equalsIgnoreCase(ApplicationIntent.READ_ONLY.toString())) {
            applicationIntent = ApplicationIntent.READ_ONLY;
        }
        else if (value.equalsIgnoreCase(ApplicationIntent.READ_WRITE.toString())) {
            applicationIntent = ApplicationIntent.READ_WRITE;
        }
        else {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidapplicationIntent"));
            Object[] msgArgs = {value};
            throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
        }

        return applicationIntent;
    }
}

enum SQLServerDriverObjectProperty {
    GSS_CREDENTIAL("gsscredential", null);
    private final String name;
    private final String defaultValue;

    private SQLServerDriverObjectProperty(String name,
            String defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    /**
     * returning string due to structure of DRIVER_PROPERTIES_PROPERTY_ONLY 
     * @return
     */
    public String getDefaultValue() {
        return defaultValue;
    }
    
    public String toString() {
        return name;
    }
}



enum SQLServerDriverStringProperty
{
	APPLICATION_INTENT         ("applicationIntent",       ApplicationIntent.READ_WRITE.toString()),
	APPLICATION_NAME           ("applicationName",         SQLServerDriver.DEFAULT_APP_NAME),
	DATABASE_NAME              ("databaseName",            ""),
	FAILOVER_PARTNER           ("failoverPartner",         ""),
	HOSTNAME_IN_CERTIFICATE    ("hostNameInCertificate",   ""),
	INSTANCE_NAME              ("instanceName",            ""),
	JAAS_CONFIG_NAME           ("jaasConfigurationName",   "SQLJDBCDriver"),
	PASSWORD                   ("password",                ""),
	RESPONSE_BUFFERING         ("responseBuffering",       "adaptive"),
	SELECT_METHOD              ("selectMethod",            "direct"),
	SERVER_NAME                ("serverName",              ""),
	SERVER_SPN                 ("serverSpn",               ""),
	TRUST_STORE_TYPE           ("trustStoreType",          "JKS"),
	TRUST_STORE                ("trustStore",              ""),
	TRUST_STORE_PASSWORD       ("trustStorePassword",      ""),
	TRUST_MANAGER_CLASS        ("trustManagerClass",       ""),
	TRUST_MANAGER_CONSTRUCTOR_ARG("trustManagerConstructorArg", ""),
	USER                       ("user",                    ""),
	WORKSTATION_ID             ("workstationID",           Util.WSIDNotAvailable),
	AUTHENTICATION_SCHEME      ("authenticationScheme",    AuthenticationScheme.nativeAuthentication.toString()),
	AUTHENTICATION             ("authentication",          SqlAuthentication.NotSpecified.toString()),
	ACCESS_TOKEN               ("accessToken",             ""),
	COLUMN_ENCRYPTION          ("columnEncryptionSetting", ColumnEncryptionSetting.Disabled.toString()),
	KEY_STORE_AUTHENTICATION   ("keyStoreAuthentication",  ""),
	KEY_STORE_SECRET           ("keyStoreSecret",          ""),
	KEY_STORE_LOCATION         ("keyStoreLocation",        ""),
	SSL_PROTOCOL               ("sslProtocol",             SSLProtocol.TLS.toString()),
	;

    private final String name;
    private final String defaultValue;

    private SQLServerDriverStringProperty(String name,
            String defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    String getDefaultValue() {
        return defaultValue;
    }

    public String toString() {
        return name;
    }
}

enum SQLServerDriverIntProperty {
    PACKET_SIZE                                ("packetSize",                              TDS.DEFAULT_PACKET_SIZE),            
    LOCK_TIMEOUT                               ("lockTimeout",                             -1),
    LOGIN_TIMEOUT                              ("loginTimeout",                            15),
    QUERY_TIMEOUT                              ("queryTimeout",                            -1),
    PORT_NUMBER                                ("portNumber",                              1433),
    SOCKET_TIMEOUT                             ("socketTimeout",                           0),
    SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD("serverPreparedStatementDiscardThreshold", SQLServerConnection.DEFAULT_SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD),
    STATEMENT_POOLING_CACHE_SIZE               ("statementPoolingCacheSize",               SQLServerConnection.DEFAULT_STATEMENT_POOLING_CACHE_SIZE),
    ;  
    
    private final String name;
    private final int defaultValue;

    private SQLServerDriverIntProperty(String name,
            int defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    int getDefaultValue() {
        return defaultValue;
    }

    public String toString() {
        return name;
    }
}

enum SQLServerDriverBooleanProperty 
{
    DISABLE_STATEMENT_POOLING                 ("disableStatementPooling",                   true),
    ENCRYPT                                   ("encrypt",                                   false), 
    INTEGRATED_SECURITY                       ("integratedSecurity",                        false),
    LAST_UPDATE_COUNT                         ("lastUpdateCount",                           true),
    MULTI_SUBNET_FAILOVER                     ("multiSubnetFailover",                       false),
    SERVER_NAME_AS_ACE                        ("serverNameAsACE",                           false),
    SEND_STRING_PARAMETERS_AS_UNICODE         ("sendStringParametersAsUnicode",             true),
    SEND_TIME_AS_DATETIME                     ("sendTimeAsDatetime",                        true),
    TRANSPARENT_NETWORK_IP_RESOLUTION         ("TransparentNetworkIPResolution",            true),
    TRUST_SERVER_CERTIFICATE                  ("trustServerCertificate",                    false),
    XOPEN_STATES                              ("xopenStates",                               false),
    FIPS                                      ("fips",                                      false),
    ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT("enablePrepareOnFirstPreparedStatementCall", SQLServerConnection.DEFAULT_ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT_CALL);

    private final String name;
    private final boolean defaultValue;

    private SQLServerDriverBooleanProperty(String name,
            boolean defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    boolean getDefaultValue() {
        return defaultValue;
    }

    public String toString() {
        return name;
    }
}

public final class SQLServerDriver implements java.sql.Driver {
    static final String PRODUCT_NAME = "Microsoft JDBC Driver " + SQLJdbcVersion.major + "." + SQLJdbcVersion.minor + " for SQL Server";
    static final String DEFAULT_APP_NAME = "Microsoft JDBC Driver for SQL Server";

    private static final String[] TRUE_FALSE = {"true", "false"};
    private static final SQLServerDriverPropertyInfo[] DRIVER_PROPERTIES =
    {
        //                                                               													  default       																						  required    available choices
        //  property name                                               													  value        							 															      property    (if appropriate)
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.APPLICATION_INTENT.toString(),    				      SQLServerDriverStringProperty.APPLICATION_INTENT.getDefaultValue(), 									  false,	  new String[]{ApplicationIntent.READ_ONLY.toString(), ApplicationIntent.READ_WRITE.toString()}),            	
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.APPLICATION_NAME.toString(),    					      SQLServerDriverStringProperty.APPLICATION_NAME.getDefaultValue(), 									  false,	  null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.COLUMN_ENCRYPTION.toString(),            			      SQLServerDriverStringProperty.COLUMN_ENCRYPTION.getDefaultValue(),       							      false,      new String[] {ColumnEncryptionSetting.Disabled.toString(), ColumnEncryptionSetting.Enabled.toString()}),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.DATABASE_NAME.toString(),       					      SQLServerDriverStringProperty.DATABASE_NAME.getDefaultValue(),       								      false,      null),                        
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.DISABLE_STATEMENT_POOLING.toString(), 			      Boolean.toString(SQLServerDriverBooleanProperty.DISABLE_STATEMENT_POOLING.getDefaultValue()),       	  false,      new String[] {"true"}),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.ENCRYPT.toString(),                      		      Boolean.toString(SQLServerDriverBooleanProperty.ENCRYPT.getDefaultValue()),        					  false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.FAILOVER_PARTNER.toString(),              		      SQLServerDriverStringProperty.FAILOVER_PARTNER.getDefaultValue(),           							  false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.HOSTNAME_IN_CERTIFICATE.toString(),       		      SQLServerDriverStringProperty.HOSTNAME_IN_CERTIFICATE.getDefaultValue(),           					  false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.INSTANCE_NAME.toString(),                 		      SQLServerDriverStringProperty.INSTANCE_NAME.getDefaultValue(),           							      false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.INTEGRATED_SECURITY.toString(),          		      Boolean.toString(SQLServerDriverBooleanProperty.INTEGRATED_SECURITY.getDefaultValue()),      		      false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.KEY_STORE_AUTHENTICATION.toString(),            	      SQLServerDriverStringProperty.KEY_STORE_AUTHENTICATION.getDefaultValue(),       						  false,      new String[] {KeyStoreAuthentication.JavaKeyStorePassword.toString()}),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.KEY_STORE_SECRET .toString(),            			      SQLServerDriverStringProperty.KEY_STORE_SECRET.getDefaultValue(),       								  false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.KEY_STORE_LOCATION .toString(),            		      SQLServerDriverStringProperty.KEY_STORE_LOCATION.getDefaultValue(),       							  false,      null),        
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.LAST_UPDATE_COUNT.toString(),            		      Boolean.toString(SQLServerDriverBooleanProperty.LAST_UPDATE_COUNT.getDefaultValue()),       			  false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.LOCK_TIMEOUT.toString(),                   			      Integer.toString(SQLServerDriverIntProperty.LOCK_TIMEOUT.getDefaultValue()),         				      false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.LOGIN_TIMEOUT.toString(),                  			      Integer.toString(SQLServerDriverIntProperty.LOGIN_TIMEOUT.getDefaultValue()),         				  false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.MULTI_SUBNET_FAILOVER.toString(),            	      Boolean.toString(SQLServerDriverBooleanProperty.MULTI_SUBNET_FAILOVER.getDefaultValue()),       		  false,      TRUE_FALSE),        
        new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.PACKET_SIZE.toString(),                    			      Integer.toString(SQLServerDriverIntProperty.PACKET_SIZE.getDefaultValue()), 							  false, 	  null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.PASSWORD.toString(),                      		      SQLServerDriverStringProperty.PASSWORD.getDefaultValue(),           									  true,       null),
        new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.PORT_NUMBER.toString(),                    			      Integer.toString(SQLServerDriverIntProperty.PORT_NUMBER.getDefaultValue()),       					  false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.QUERY_TIMEOUT.toString(),                                  Integer.toString(SQLServerDriverIntProperty.QUERY_TIMEOUT.getDefaultValue()),                           false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.RESPONSE_BUFFERING.toString(),            		      SQLServerDriverStringProperty.RESPONSE_BUFFERING.getDefaultValue(),   								  false,      new String[] {"adaptive", "full"}),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.SELECT_METHOD.toString(),                 		      SQLServerDriverStringProperty.SELECT_METHOD.getDefaultValue(),     									  false,      new String[] {"direct", "cursor"}),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.toString(), 	      Boolean.toString(SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.getDefaultValue()),   false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.SERVER_NAME_AS_ACE.toString(), 					      Boolean.toString(SQLServerDriverBooleanProperty.SERVER_NAME_AS_ACE.getDefaultValue()),  				  false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.SERVER_NAME.toString(),                    		      SQLServerDriverStringProperty.SERVER_NAME.getDefaultValue(),           								  false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.SERVER_SPN.toString(),                    		      SQLServerDriverStringProperty.SERVER_SPN.getDefaultValue(),           								  false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.TRANSPARENT_NETWORK_IP_RESOLUTION.toString(),          Boolean.toString(SQLServerDriverBooleanProperty.TRANSPARENT_NETWORK_IP_RESOLUTION.getDefaultValue()),   false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.TRUST_SERVER_CERTIFICATE.toString(),        		      Boolean.toString(SQLServerDriverBooleanProperty.TRUST_SERVER_CERTIFICATE.getDefaultValue()),      	  false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.TRUST_STORE_TYPE.toString(),                    	      SQLServerDriverStringProperty.TRUST_STORE_TYPE.getDefaultValue(),           							  false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.TRUST_STORE.toString(),                    		      SQLServerDriverStringProperty.TRUST_STORE.getDefaultValue(),           								  false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString(),            		      SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.getDefaultValue(),           						  false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.TRUST_MANAGER_CLASS.toString(),                         SQLServerDriverStringProperty.TRUST_MANAGER_CLASS.getDefaultValue(),                                    false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.TRUST_MANAGER_CONSTRUCTOR_ARG.toString(),               SQLServerDriverStringProperty.TRUST_MANAGER_CONSTRUCTOR_ARG.getDefaultValue(),                          false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.SEND_TIME_AS_DATETIME.toString(),            	      Boolean.toString(SQLServerDriverBooleanProperty.SEND_TIME_AS_DATETIME.getDefaultValue()),       		  false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.USER.toString(),                          		      SQLServerDriverStringProperty.USER.getDefaultValue(),           										  true,       null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.WORKSTATION_ID.toString(),                 		      SQLServerDriverStringProperty.WORKSTATION_ID.getDefaultValue(), 										  false, 	  null),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.XOPEN_STATES.toString(),                   		      Boolean.toString(SQLServerDriverBooleanProperty.XOPEN_STATES.getDefaultValue()),      				  false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.AUTHENTICATION_SCHEME.toString(),          		      SQLServerDriverStringProperty.AUTHENTICATION_SCHEME.getDefaultValue(),      			            	  false,      new String[] {AuthenticationScheme.javaKerberos.toString(),AuthenticationScheme.nativeAuthentication.toString()}),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.AUTHENTICATION.toString(),          				      SQLServerDriverStringProperty.AUTHENTICATION.getDefaultValue(),      			                		  false,      new String[] {SqlAuthentication.NotSpecified.toString(),SqlAuthentication.SqlPassword.toString(),SqlAuthentication.ActiveDirectoryPassword.toString(),SqlAuthentication.ActiveDirectoryIntegrated.toString()}),
        new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.SOCKET_TIMEOUT.toString(),                   		      Integer.toString(SQLServerDriverIntProperty.SOCKET_TIMEOUT.getDefaultValue()),         				  false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.FIPS.toString(),                                       Boolean.toString(SQLServerDriverBooleanProperty.FIPS.getDefaultValue()),                          	  false,      TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT.toString(), Boolean.toString(SQLServerDriverBooleanProperty.ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT.getDefaultValue()), false,TRUE_FALSE),
        new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD.toString(),    Integer.toString(SQLServerDriverIntProperty.SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD.getDefaultValue()), false,  null),
        new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.STATEMENT_POOLING_CACHE_SIZE.toString(),                   Integer.toString(SQLServerDriverIntProperty.STATEMENT_POOLING_CACHE_SIZE.getDefaultValue()),            false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.JAAS_CONFIG_NAME.toString(),                            SQLServerDriverStringProperty.JAAS_CONFIG_NAME.getDefaultValue(),                                       false,      null),
        new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.SSL_PROTOCOL.toString(),                                SQLServerDriverStringProperty.SSL_PROTOCOL.getDefaultValue(),                                           false,      new String[] {SSLProtocol.TLS.toString(), SSLProtocol.TLS_V10.toString(), SSLProtocol.TLS_V11.toString(), SSLProtocol.TLS_V12.toString()}),
    };

    // Properties that can only be set by using Properties.
    // Cannot set in connection string
    private static final SQLServerDriverPropertyInfo[] DRIVER_PROPERTIES_PROPERTY_ONLY = {
            //                                                                                                                  default                                                                                         required    available choices
            //                          property name                                                                           value                                                                                           property    (if appropriate)
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.ACCESS_TOKEN.toString(),                      SQLServerDriverStringProperty.ACCESS_TOKEN.getDefaultValue(),                                           false,      null),
            new SQLServerDriverPropertyInfo(SQLServerDriverObjectProperty.GSS_CREDENTIAL.toString(),                    SQLServerDriverObjectProperty.GSS_CREDENTIAL.getDefaultValue(),                                         false,      null),        
    };

	private static final String driverPropertiesSynonyms[][] = {
		{"database", SQLServerDriverStringProperty.DATABASE_NAME.toString()}, 	
		{"userName",SQLServerDriverStringProperty.USER.toString()},			
		{"server",SQLServerDriverStringProperty.SERVER_NAME.toString()},		
		{"port", SQLServerDriverIntProperty.PORT_NUMBER.toString()}
    };
    static private final AtomicInteger baseID = new AtomicInteger(0);	// Unique id generator for each instance (used for logging).
    final private int instanceID;							// Unique id for this instance.
    final private String traceID;

    // Returns unique id for each instance.
    private static int nextInstanceID() {
        return baseID.incrementAndGet();
    }

    final public String toString() {
        return traceID;
    }

    static final private java.util.logging.Logger loggerExternal = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.Driver");
    static final private java.util.logging.Logger parentLogger = java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc");
    final private String loggingClassName;

    String getClassNameLogging() {
        return loggingClassName;
    }

    private final static java.util.logging.Logger drLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerDriver");
    // Register with the DriverManager
    static {
        try {
            java.sql.DriverManager.registerDriver(new SQLServerDriver());
        }
        catch (SQLException e) {
            if (drLogger.isLoggable(Level.FINER) && Util.IsActivityTraceOn()) {
                drLogger.finer("Error registering driver: " + e);
            }
        }
    }

    public SQLServerDriver() {
        instanceID = nextInstanceID();
        traceID = "SQLServerDriver:" + instanceID;
        loggingClassName = "com.microsoft.sqlserver.jdbc." + "SQLServerDriver:" + instanceID;
    }

    // Helper function used to fixup the case sensitivity, synonyms and remove unknown tokens from the
    // properties
    static Properties fixupProperties(Properties props) throws SQLServerException {
        // assert props !=null
        Properties fixedup = new Properties();
        Enumeration<?> e = props.keys();
        while (e.hasMoreElements()) {
            String name = (String) e.nextElement();
            String newname = getNormalizedPropertyName(name, drLogger);

            if (null == newname) {
                newname = getPropertyOnlyName(name, drLogger);
            }

            if (null != newname) {
                String val = props.getProperty(name);
                if (null != val) {
                    // replace with the driver approved name
                    fixedup.setProperty(newname, val);
                }
                else if(newname.equalsIgnoreCase("gsscredential") && (props.get(name) instanceof GSSCredential)){
                	fixedup.put(newname, props.get(name));
                }
                else {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidpropertyValue"));
                    Object[] msgArgs = {name};
                    throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
                }
            }

        }
        return fixedup;
    }

    // Helper function used to merge together the property set extracted from the url and the
    // user supplied property set passed in by the caller. This function is used by both SQLServerDriver.connect
    // and SQLServerDataSource.getConnectionInternal to centralize this property merging code.
    static Properties mergeURLAndSuppliedProperties(Properties urlProps,
            Properties suppliedProperties) throws SQLServerException {
        if (null == suppliedProperties)
            return urlProps;
        if (suppliedProperties.isEmpty())
            return urlProps;
        Properties suppliedPropertiesFixed = fixupProperties(suppliedProperties);
        // Merge URL properties and supplied properties.
        for (SQLServerDriverPropertyInfo DRIVER_PROPERTY : DRIVER_PROPERTIES) {
            String sProp = DRIVER_PROPERTY.getName();
            String sPropVal = suppliedPropertiesFixed.getProperty(sProp); // supplied properties have precedence
            if (null != sPropVal) {
                // overwrite the property in urlprops if already exists. supp prop has more precedence
                urlProps.put(sProp, sPropVal);
            }
        }

        // Merge URL properties with property-only properties
        for (SQLServerDriverPropertyInfo aDRIVER_PROPERTIES_PROPERTY_ONLY : DRIVER_PROPERTIES_PROPERTY_ONLY) {
            String sProp = aDRIVER_PROPERTIES_PROPERTY_ONLY.getName();
            Object oPropVal = suppliedPropertiesFixed.get(sProp); // supplied properties have precedence
            if (null != oPropVal) {
                // overwrite the property in urlprops if already exists. supp prop has more precedence
                urlProps.put(sProp, oPropVal);
            }
        }

        return urlProps;
    }

    /**
     * normalize the property names
     * 
     * @param name
     *            name to normalize
     * @param logger
     * @return the normalized property name
     */
    static String getNormalizedPropertyName(String name,
            Logger logger) {
        if (null == name)
            return name;

        for (String[] driverPropertiesSynonym : driverPropertiesSynonyms) {
            if (driverPropertiesSynonym[0].equalsIgnoreCase(name)) {
                return driverPropertiesSynonym[1];
            }
        }
        for (SQLServerDriverPropertyInfo DRIVER_PROPERTY : DRIVER_PROPERTIES) {
            if (DRIVER_PROPERTY.getName().equalsIgnoreCase(name)) {
                return DRIVER_PROPERTY.getName();
            }
        }

        if (logger.isLoggable(Level.FINER))
            logger.finer("Unknown property" + name);
        return null;
    }

    /**
     * get property-only names that do not work with connection string
     * 
     * @param name
     *            to normalize
     * @param logger
     * @return the normalized property name
     */
    static String getPropertyOnlyName(String name,
            Logger logger) {
        if (null == name)
            return name;

        for (SQLServerDriverPropertyInfo aDRIVER_PROPERTIES_PROPERTY_ONLY : DRIVER_PROPERTIES_PROPERTY_ONLY) {
            if (aDRIVER_PROPERTIES_PROPERTY_ONLY.getName().equalsIgnoreCase(name)) {
                return aDRIVER_PROPERTIES_PROPERTY_ONLY.getName();
            }
        }

        if (logger.isLoggable(Level.FINER))
            logger.finer("Unknown property" + name);
        return null;
    }

    /* L0 */ public java.sql.Connection connect(String Url,
            Properties suppliedProperties) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "connect", "Arguments not traced.");
        SQLServerConnection result = null;

        // Merge connectProperties (from URL) and supplied properties from user.
        Properties connectProperties = parseAndMergeProperties(Url, suppliedProperties);
        if (connectProperties != null) {
            if (Util.use43Wrapper()) {
                result = new SQLServerConnection43(toString());
            }
            else {
                result = new SQLServerConnection(toString());
            }
            result.connect(connectProperties, null);
        }
        loggerExternal.exiting(getClassNameLogging(), "connect", result);
        return result;
    }

    private Properties parseAndMergeProperties(String Url,
            Properties suppliedProperties) throws SQLServerException {
        if (Url == null) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_nullConnection"), null, 0, false);
        }

        Properties connectProperties = Util.parseUrl(Url, drLogger);
        if (connectProperties == null)
            return null;  // If we are the wrong driver dont throw an exception

        // put the user properties into the connect properties
        int nTimeout = DriverManager.getLoginTimeout();
        if (nTimeout > 0) {
            connectProperties.put(SQLServerDriverIntProperty.LOGIN_TIMEOUT.toString(), Integer.valueOf(nTimeout).toString());
        }

        // Merge connectProperties (from URL) and supplied properties from user.
        connectProperties = mergeURLAndSuppliedProperties(connectProperties, suppliedProperties);
        return connectProperties;
    }

    /* L0 */ public boolean acceptsURL(String url) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "acceptsURL", "Arguments not traced.");

        if (null == url) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_nullConnection"), null, 0, false);
        }

        boolean result = false;
        try {
            result = (Util.parseUrl(url, drLogger) != null);
        }
        catch (SQLServerException e) {
            // ignore the exception from the parse URL failure, if we cant parse the URL we do not accept em
            result = false;
        }
        loggerExternal.exiting(getClassNameLogging(), "acceptsURL", result);
        return result;
    }

    public DriverPropertyInfo[] getPropertyInfo(String Url,
            Properties Info) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "getPropertyInfo", "Arguments not traced.");

        Properties connProperties = parseAndMergeProperties(Url, Info);
        // This means we are not the right driver throw an exception.
        if (null == connProperties)
            throw new SQLServerException(null, SQLServerException.getErrString("R_invalidConnection"), null, 0, false);
        DriverPropertyInfo[] properties = getPropertyInfoFromProperties(connProperties);
        loggerExternal.exiting(getClassNameLogging(), "getPropertyInfo");

        return properties;
    }

    static final DriverPropertyInfo[] getPropertyInfoFromProperties(Properties props) {
        DriverPropertyInfo[] properties = new DriverPropertyInfo[DRIVER_PROPERTIES.length];

        for (int i = 0; i < DRIVER_PROPERTIES.length; i++)
            properties[i] = DRIVER_PROPERTIES[i].build(props);
        return properties;
    }

    public int getMajorVersion() {
        loggerExternal.entering(getClassNameLogging(), "getMajorVersion");
        loggerExternal.exiting(getClassNameLogging(), "getMajorVersion", SQLJdbcVersion.major);
        return SQLJdbcVersion.major;
    }

    public int getMinorVersion() {
        loggerExternal.entering(getClassNameLogging(), "getMinorVersion");
        loggerExternal.exiting(getClassNameLogging(), "getMinorVersion", SQLJdbcVersion.minor);
        return SQLJdbcVersion.minor;
    }

    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return parentLogger;
    }

    /* L0 */ public boolean jdbcCompliant() {
        loggerExternal.entering(getClassNameLogging(), "jdbcCompliant");
        loggerExternal.exiting(getClassNameLogging(), "jdbcCompliant", Boolean.TRUE);
        return true;
    }
}
