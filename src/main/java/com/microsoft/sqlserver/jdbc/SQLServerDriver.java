/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Enumeration;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.ietf.jgss.GSSCredential;


/**
 * Implements the java.sql.Driver for SQLServerConnect.
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

    SQLServerDriverPropertyInfo(String name, String defaultValue, boolean required, String[] choices) {
        this.name = name;
        this.description = SQLServerResource.getResource("R_" + name + "PropertyDescription");
        this.defaultValue = defaultValue;
        this.required = required;
        this.choices = choices;
    }

    DriverPropertyInfo build(Properties connProperties) {
        String propValue = name.equals(SQLServerDriverStringProperty.PASSWORD.toString()) ? "" : connProperties
                .getProperty(name);

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
    ActiveDirectoryIntegrated,
    ActiveDirectoryMSI,
    ActiveDirectoryManagedIdentity,
    ActiveDirectoryServicePrincipal,
    ActiveDirectoryInteractive,
    DefaultAzureCredential;

    static SqlAuthentication valueOfString(String value) throws SQLServerException {
        SqlAuthentication method = null;

        if (value.toLowerCase(Locale.US).equalsIgnoreCase(SqlAuthentication.NotSpecified.toString())) {
            method = SqlAuthentication.NotSpecified;
        } else if (value.toLowerCase(Locale.US).equalsIgnoreCase(SqlAuthentication.SqlPassword.toString())) {
            method = SqlAuthentication.SqlPassword;
        } else if (value.toLowerCase(Locale.US)
                .equalsIgnoreCase(SqlAuthentication.ActiveDirectoryPassword.toString())) {
            method = SqlAuthentication.ActiveDirectoryPassword;
        } else if (value.toLowerCase(Locale.US)
                .equalsIgnoreCase(SqlAuthentication.ActiveDirectoryIntegrated.toString())) {
            method = SqlAuthentication.ActiveDirectoryIntegrated;
        } else if (value.toLowerCase(Locale.US)
                .equalsIgnoreCase(SqlAuthentication.ActiveDirectoryManagedIdentity.toString())
                || SQLServerDriver.getNormalizedPropertyValueName(value).toLowerCase(Locale.US)
                        .equalsIgnoreCase(SqlAuthentication.ActiveDirectoryManagedIdentity.toString())) {
            method = SqlAuthentication.ActiveDirectoryManagedIdentity;
        } else if (value.toLowerCase(Locale.US)
                .equalsIgnoreCase(SqlAuthentication.ActiveDirectoryServicePrincipal.toString())) {
            method = SqlAuthentication.ActiveDirectoryServicePrincipal;
        } else if (value.toLowerCase(Locale.US)
                .equalsIgnoreCase(SqlAuthentication.ActiveDirectoryInteractive.toString())) {
            method = SqlAuthentication.ActiveDirectoryInteractive;
        } else if (value.toLowerCase(Locale.US).equalsIgnoreCase(SqlAuthentication.DefaultAzureCredential.toString())) {
            method = SqlAuthentication.DefaultAzureCredential;
        } else {
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
        } else if (value.toLowerCase(Locale.US).equalsIgnoreCase(ColumnEncryptionSetting.Disabled.toString())) {
            method = ColumnEncryptionSetting.Disabled;
        } else {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidConnectionSetting"));
            Object[] msgArgs = {"columnEncryptionSetting", value};
            throw new SQLServerException(form.format(msgArgs), null);
        }
        return method;
    }
}


enum EncryptOption {
    False,
    No,
    Optional,
    True,
    Mandatory,
    Strict;

    static EncryptOption valueOfString(String value) throws SQLServerException {
        EncryptOption option = null;

        String val = value.toLowerCase(Locale.US);
        if (val.equalsIgnoreCase(EncryptOption.False.toString()) || val.equalsIgnoreCase(EncryptOption.No.toString())
                || val.equalsIgnoreCase(EncryptOption.Optional.toString())) {
            option = EncryptOption.False;
        } else if (val.equalsIgnoreCase(EncryptOption.True.toString())
                || val.equalsIgnoreCase(EncryptOption.Mandatory.toString())) {
            option = EncryptOption.True;
        } else if (val.equalsIgnoreCase(EncryptOption.Strict.toString())) {
            option = EncryptOption.Strict;
        } else {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidConnectionSetting"));
            Object[] msgArgs = {"EncryptOption", value};
            throw new SQLServerException(form.format(msgArgs), null);
        }
        return option;
    }

    static boolean isValidEncryptOption(String option) {
        for (EncryptOption t : EncryptOption.values()) {
            if (option.equalsIgnoreCase(t.toString())) {
                return true;
            }
        }
        return false;
    }
}


enum AttestationProtocol {
    HGS("HGS"),
    AAS("AAS"),
    NONE("NONE");

    private final String protocol;

    AttestationProtocol(String protocol) {
        this.protocol = protocol;
    }

    static boolean isValidAttestationProtocol(String protocol) {
        for (AttestationProtocol p : AttestationProtocol.values()) {
            if (protocol.equalsIgnoreCase(p.toString())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return protocol;
    }
}


enum EnclaveType {
    VBS("VBS"),
    SGX("SGX");

    private final String type;

    EnclaveType(String type) {
        this.type = type;
    }

    public int getValue() {
        return ordinal() + 1;
    }

    static boolean isValidEnclaveType(String type) {
        for (EnclaveType t : EnclaveType.values()) {
            if (type.equalsIgnoreCase(t.toString())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return type;
    }
}


enum SSLProtocol {
    TLS("TLS"),
    TLS_V10("TLSv1"),
    TLS_V11("TLSv1.1"),
    TLS_V12("TLSv1.2"),
    TLS_V13("TLSv1.3"),;

    private final String name;

    private SSLProtocol(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    static SSLProtocol valueOfString(String value) throws SQLServerException {
        SSLProtocol protocol = null;

        if (value.toLowerCase(Locale.ENGLISH).equalsIgnoreCase(SSLProtocol.TLS.toString())) {
            protocol = SSLProtocol.TLS;
        } else if (value.toLowerCase(Locale.ENGLISH).equalsIgnoreCase(SSLProtocol.TLS_V10.toString())) {
            protocol = SSLProtocol.TLS_V10;
        } else if (value.toLowerCase(Locale.ENGLISH).equalsIgnoreCase(SSLProtocol.TLS_V11.toString())) {
            protocol = SSLProtocol.TLS_V11;
        } else if (value.toLowerCase(Locale.ENGLISH).equalsIgnoreCase(SSLProtocol.TLS_V12.toString())) {
            protocol = SSLProtocol.TLS_V12;
        } else if (value.toLowerCase(Locale.ENGLISH).equalsIgnoreCase(SSLProtocol.TLS_V13.toString())) {
            protocol = SSLProtocol.TLS_V13;
        } else {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidSSLProtocol"));
            Object[] msgArgs = {value};
            throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
        }
        return protocol;
    }
}


enum IPAddressPreference {
    IPv4First("IPv4First"),
    IPv6First("IPv6First"),
    UsePlatformDefault("UsePlatformDefault");

    private final String name;

    IPAddressPreference(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }

    static IPAddressPreference valueOfString(String value) throws SQLServerException {
        IPAddressPreference iptype = null;

        if (value.toLowerCase(Locale.US).equalsIgnoreCase(IPAddressPreference.IPv4First.toString())) {
            iptype = IPAddressPreference.IPv4First;
        } else if (value.toLowerCase(Locale.US).equalsIgnoreCase(IPAddressPreference.IPv6First.toString())) {
            iptype = IPAddressPreference.IPv6First;
        } else if (value.toLowerCase(Locale.US).equalsIgnoreCase(IPAddressPreference.UsePlatformDefault.toString())) {
            iptype = IPAddressPreference.UsePlatformDefault;

        } else {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidIPAddressPreference"));
            Object[] msgArgs = {value};
            throw new SQLServerException(form.format(msgArgs), null);
        }
        return iptype;
    }
}


enum KeyStoreAuthentication {
    JavaKeyStorePassword,
    KeyVaultClientSecret,
    KeyVaultManagedIdentity;

    static KeyStoreAuthentication valueOfString(String value) throws SQLServerException {
        KeyStoreAuthentication method = null;

        if (value.toLowerCase(Locale.US).equalsIgnoreCase(KeyStoreAuthentication.JavaKeyStorePassword.toString())) {
            method = KeyStoreAuthentication.JavaKeyStorePassword;
        } else if (value.toLowerCase(Locale.US)
                .equalsIgnoreCase(KeyStoreAuthentication.KeyVaultClientSecret.toString())) {
            method = KeyStoreAuthentication.KeyVaultClientSecret;
        } else if (value.toLowerCase(Locale.US)
                .equalsIgnoreCase(KeyStoreAuthentication.KeyVaultManagedIdentity.toString())) {
            method = KeyStoreAuthentication.KeyVaultManagedIdentity;

        } else {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidConnectionSetting"));
            Object[] msgArgs = {"keyStoreAuthentication", value};
            throw new SQLServerException(form.format(msgArgs), null);
        }
        return method;
    }
}


enum AuthenticationScheme {
    nativeAuthentication,
    ntlm,
    javaKerberos;

    static AuthenticationScheme valueOfString(String value) throws SQLServerException {
        AuthenticationScheme scheme;
        if (value.toLowerCase(Locale.US).equalsIgnoreCase(AuthenticationScheme.javaKerberos.toString())) {
            scheme = AuthenticationScheme.javaKerberos;
        } else if (value.toLowerCase(Locale.US)
                .equalsIgnoreCase(AuthenticationScheme.nativeAuthentication.toString())) {
            scheme = AuthenticationScheme.nativeAuthentication;
        } else if (value.toLowerCase(Locale.US).equalsIgnoreCase(AuthenticationScheme.ntlm.toString())) {
            scheme = AuthenticationScheme.ntlm;
        } else {
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

    /**
     * Constructs a ApplicationIntent that sets the string value of the enum.
     */
    private ApplicationIntent(String value) {
        this.value = value;
    }

    /**
     * Returns the string value of enum.
     */
    @Override
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
        } else if (value.equalsIgnoreCase(ApplicationIntent.READ_WRITE.toString())) {
            applicationIntent = ApplicationIntent.READ_WRITE;
        } else {
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

    private SQLServerDriverObjectProperty(String name, String defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    /**
     * Returns string due to structure of DRIVER_PROPERTIES_PROPERTY_ONLY.
     * 
     * @return
     */
    public String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String toString() {
        return name;
    }
}


enum PrepareMethod {
    PREPEXEC("prepexec"), // sp_prepexec, default prepare method
    PREPARE("prepare");

    private final String value;

    private PrepareMethod(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }

    static PrepareMethod valueOfString(String value) throws SQLServerException {
        assert value != null;

        for (PrepareMethod prepareMethod : PrepareMethod.values()) {
            if (prepareMethod.toString().equalsIgnoreCase(value)) {
                return prepareMethod;
            }
        }

        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidConnectionSetting"));
        Object[] msgArgs = {SQLServerDriverStringProperty.PREPARE_METHOD.toString(), value};
        throw new SQLServerException(form.format(msgArgs), null);
    }
}


enum SQLServerDriverStringProperty {
    APPLICATION_INTENT("applicationIntent", ApplicationIntent.READ_WRITE.toString()),
    APPLICATION_NAME("applicationName", SQLServerDriver.DEFAULT_APP_NAME),
    PREPARE_METHOD("prepareMethod", PrepareMethod.PREPEXEC.toString()),
    DATABASE_NAME("databaseName", ""),
    FAILOVER_PARTNER("failoverPartner", ""),
    HOSTNAME_IN_CERTIFICATE("hostNameInCertificate", ""),
    INSTANCE_NAME("instanceName", ""),
    JAAS_CONFIG_NAME("jaasConfigurationName", "SQLJDBCDriver"),
    PASSWORD("password", ""),
    RESPONSE_BUFFERING("responseBuffering", "adaptive"),
    SELECT_METHOD("selectMethod", "direct"),
    DOMAIN("domain", ""),
    SERVER_NAME("serverName", ""),
    IPADDRESS_PREFERENCE("iPAddressPreference", IPAddressPreference.IPv4First.toString()),
    SERVER_SPN("serverSpn", ""),
    REALM("realm", ""),
    SOCKET_FACTORY_CLASS("socketFactoryClass", ""),
    SOCKET_FACTORY_CONSTRUCTOR_ARG("socketFactoryConstructorArg", ""),
    TRUST_STORE_TYPE("trustStoreType", "JKS"),
    TRUST_STORE("trustStore", ""),
    TRUST_STORE_PASSWORD("trustStorePassword", ""),
    TRUST_MANAGER_CLASS("trustManagerClass", ""),
    TRUST_MANAGER_CONSTRUCTOR_ARG("trustManagerConstructorArg", ""),
    USER("user", ""),
    WORKSTATION_ID("workstationID", Util.WSIDNotAvailable),
    AUTHENTICATION_SCHEME("authenticationScheme", AuthenticationScheme.nativeAuthentication.toString()),
    AUTHENTICATION("authentication", SqlAuthentication.NotSpecified.toString()),
    ACCESS_TOKEN("accessToken", ""),
    COLUMN_ENCRYPTION("columnEncryptionSetting", ColumnEncryptionSetting.Disabled.toString()),
    ENCLAVE_ATTESTATION_URL("enclaveAttestationUrl", ""),
    ENCLAVE_ATTESTATION_PROTOCOL("enclaveAttestationProtocol", ""),
    KEY_STORE_AUTHENTICATION("keyStoreAuthentication", ""),
    KEY_STORE_SECRET("keyStoreSecret", ""),
    KEY_STORE_LOCATION("keyStoreLocation", ""),
    SSL_PROTOCOL("sslProtocol", SSLProtocol.TLS.toString()),
    MSI_CLIENT_ID("msiClientId", ""),
    KEY_VAULT_PROVIDER_CLIENT_ID("keyVaultProviderClientId", ""),
    KEY_VAULT_PROVIDER_CLIENT_KEY("keyVaultProviderClientKey", ""),
    KEY_STORE_PRINCIPAL_ID("keyStorePrincipalId", ""),
    CLIENT_CERTIFICATE("clientCertificate", ""),
    CLIENT_KEY("clientKey", ""),
    CLIENT_KEY_PASSWORD("clientKeyPassword", ""),
    AAD_SECURE_PRINCIPAL_ID("AADSecurePrincipalId", ""),
    AAD_SECURE_PRINCIPAL_SECRET("AADSecurePrincipalSecret", ""),
    MAX_RESULT_BUFFER("maxResultBuffer", "-1"),
    ENCRYPT("encrypt", EncryptOption.True.toString()),
    SERVER_CERTIFICATE("serverCertificate", "");

    private final String name;
    private final String defaultValue;

    private SQLServerDriverStringProperty(String name, String defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    String getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String toString() {
        return name;
    }
}


enum SQLServerDriverIntProperty {
    PACKET_SIZE("packetSize", TDS.DEFAULT_PACKET_SIZE),
    LOCK_TIMEOUT("lockTimeout", -1),
    LOGIN_TIMEOUT("loginTimeout", 30, 0, 65535),
    QUERY_TIMEOUT("queryTimeout", -1),
    PORT_NUMBER("portNumber", 1433),
    SOCKET_TIMEOUT("socketTimeout", 0),
    SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD("serverPreparedStatementDiscardThreshold", SQLServerConnection.DEFAULT_SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD),
    STATEMENT_POOLING_CACHE_SIZE("statementPoolingCacheSize", SQLServerConnection.DEFAULT_STATEMENT_POOLING_CACHE_SIZE),
    CANCEL_QUERY_TIMEOUT("cancelQueryTimeout", -1),
    CONNECT_RETRY_COUNT("connectRetryCount", 1, 0, 255),
    CONNECT_RETRY_INTERVAL("connectRetryInterval", 10, 1, 60);

    private final String name;
    private final int defaultValue;
    private int minValue = -1; // not assigned
    private int maxValue = -1; // not assigned

    private SQLServerDriverIntProperty(String name, int defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    private SQLServerDriverIntProperty(String name, int defaultValue, int minValue, int maxValue) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }

    int getDefaultValue() {
        return defaultValue;
    }

    boolean isValidValue(int value) {
        return (minValue == -1 && maxValue == -1) || (value >= minValue && value <= maxValue);
    }

    @Override
    public String toString() {
        return name;
    }
}


enum SQLServerDriverBooleanProperty {
    DISABLE_STATEMENT_POOLING("disableStatementPooling", true),
    INTEGRATED_SECURITY("integratedSecurity", false),
    LAST_UPDATE_COUNT("lastUpdateCount", true),
    MULTI_SUBNET_FAILOVER("multiSubnetFailover", false),
    REPLICATION("replication", false),
    SERVER_NAME_AS_ACE("serverNameAsACE", false),
    SEND_STRING_PARAMETERS_AS_UNICODE("sendStringParametersAsUnicode", true),
    SEND_TIME_AS_DATETIME("sendTimeAsDatetime", true),
    TRANSPARENT_NETWORK_IP_RESOLUTION("TransparentNetworkIPResolution", true),
    TRUST_SERVER_CERTIFICATE("trustServerCertificate", false),
    XOPEN_STATES("xopenStates", false),
    FIPS("fips", false),
    ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT("enablePrepareOnFirstPreparedStatementCall", SQLServerConnection.DEFAULT_ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT_CALL),
    USE_BULK_COPY_FOR_BATCH_INSERT("useBulkCopyForBatchInsert", false),
    USE_FMT_ONLY("useFmtOnly", false),
    SEND_TEMPORAL_DATATYPES_AS_STRING_FOR_BULK_COPY("sendTemporalDataTypesAsStringForBulkCopy", true),
    DELAY_LOADING_LOBS("delayLoadingLobs", true);

    private final String name;
    private final boolean defaultValue;

    private SQLServerDriverBooleanProperty(String name, boolean defaultValue) {
        this.name = name;
        this.defaultValue = defaultValue;
    }

    boolean getDefaultValue() {
        return defaultValue;
    }

    @Override
    public String toString() {
        return name;
    }
}


/**
 * Provides methods to connect to a SQL Server database and to obtain information about the JDBC driver.
 */
public final class SQLServerDriver implements java.sql.Driver {
    static final String PRODUCT_NAME = "Microsoft JDBC Driver " + SQLJdbcVersion.major + "." + SQLJdbcVersion.minor
            + " for SQL Server";
    static final String AUTH_DLL_NAME = "mssql-jdbc_auth-" + SQLJdbcVersion.major + "." + SQLJdbcVersion.minor + "."
            + SQLJdbcVersion.patch + "." + Util.getJVMArchOnWindows() + SQLJdbcVersion.releaseExt;
    static final String DEFAULT_APP_NAME = "Microsoft JDBC Driver for SQL Server";

    private static final String[] TRUE_FALSE = {"true", "false"};

    private static final SQLServerDriverPropertyInfo[] DRIVER_PROPERTIES = {
            // default required available choices
            // property name value property (if appropriate)
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.APPLICATION_INTENT.toString(),
                    SQLServerDriverStringProperty.APPLICATION_INTENT.getDefaultValue(), false,
                    new String[] {ApplicationIntent.READ_ONLY.toString(), ApplicationIntent.READ_WRITE.toString()}),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.APPLICATION_NAME.toString(),
                    SQLServerDriverStringProperty.APPLICATION_NAME.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.COLUMN_ENCRYPTION.toString(),
                    SQLServerDriverStringProperty.COLUMN_ENCRYPTION.getDefaultValue(), false,
                    new String[] {ColumnEncryptionSetting.Disabled.toString(),
                            ColumnEncryptionSetting.Enabled.toString()}),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.ENCLAVE_ATTESTATION_URL.toString(),
                    SQLServerDriverStringProperty.ENCLAVE_ATTESTATION_URL.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.ENCLAVE_ATTESTATION_PROTOCOL.toString(),
                    SQLServerDriverStringProperty.ENCLAVE_ATTESTATION_PROTOCOL.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.DATABASE_NAME.toString(),
                    SQLServerDriverStringProperty.DATABASE_NAME.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.DISABLE_STATEMENT_POOLING.toString(),
                    Boolean.toString(SQLServerDriverBooleanProperty.DISABLE_STATEMENT_POOLING.getDefaultValue()), false,
                    new String[] {"true"}),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.ENCRYPT.toString(),
                    SQLServerDriverStringProperty.ENCRYPT.getDefaultValue(), false,
                    new String[] {EncryptOption.False.toString(), EncryptOption.No.toString(),
                            EncryptOption.Optional.toString(), EncryptOption.True.toString(),
                            EncryptOption.Mandatory.toString(), EncryptOption.Strict.toString()}),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.SERVER_CERTIFICATE.toString(),
                    SQLServerDriverStringProperty.SERVER_CERTIFICATE.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.PREPARE_METHOD.toString(),
                    SQLServerDriverStringProperty.PREPARE_METHOD.getDefaultValue(), false,
                    new String[] {PrepareMethod.PREPEXEC.toString(), PrepareMethod.PREPARE.toString()}),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.FAILOVER_PARTNER.toString(),
                    SQLServerDriverStringProperty.FAILOVER_PARTNER.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.HOSTNAME_IN_CERTIFICATE.toString(),
                    SQLServerDriverStringProperty.HOSTNAME_IN_CERTIFICATE.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.INSTANCE_NAME.toString(),
                    SQLServerDriverStringProperty.INSTANCE_NAME.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.INTEGRATED_SECURITY.toString(),
                    Boolean.toString(SQLServerDriverBooleanProperty.INTEGRATED_SECURITY.getDefaultValue()), false,
                    TRUE_FALSE),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.KEY_STORE_AUTHENTICATION.toString(),
                    SQLServerDriverStringProperty.KEY_STORE_AUTHENTICATION.getDefaultValue(), false,
                    new String[] {KeyStoreAuthentication.JavaKeyStorePassword.toString()}),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.KEY_STORE_SECRET.toString(),
                    SQLServerDriverStringProperty.KEY_STORE_SECRET.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.KEY_STORE_LOCATION.toString(),
                    SQLServerDriverStringProperty.KEY_STORE_LOCATION.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.LAST_UPDATE_COUNT.toString(),
                    Boolean.toString(SQLServerDriverBooleanProperty.LAST_UPDATE_COUNT.getDefaultValue()), false,
                    TRUE_FALSE),
            new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.LOCK_TIMEOUT.toString(),
                    Integer.toString(SQLServerDriverIntProperty.LOCK_TIMEOUT.getDefaultValue()), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.LOGIN_TIMEOUT.toString(),
                    Integer.toString(SQLServerDriverIntProperty.LOGIN_TIMEOUT.getDefaultValue()), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.MULTI_SUBNET_FAILOVER.toString(),
                    Boolean.toString(SQLServerDriverBooleanProperty.MULTI_SUBNET_FAILOVER.getDefaultValue()), false,
                    TRUE_FALSE),
            new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.PACKET_SIZE.toString(),
                    Integer.toString(SQLServerDriverIntProperty.PACKET_SIZE.getDefaultValue()), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.PASSWORD.toString(),
                    SQLServerDriverStringProperty.PASSWORD.getDefaultValue(), true, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.PORT_NUMBER.toString(),
                    Integer.toString(SQLServerDriverIntProperty.PORT_NUMBER.getDefaultValue()), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.QUERY_TIMEOUT.toString(),
                    Integer.toString(SQLServerDriverIntProperty.QUERY_TIMEOUT.getDefaultValue()), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.RESPONSE_BUFFERING.toString(),
                    SQLServerDriverStringProperty.RESPONSE_BUFFERING.getDefaultValue(), false,
                    new String[] {"adaptive", "full"}),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.SELECT_METHOD.toString(),
                    SQLServerDriverStringProperty.SELECT_METHOD.getDefaultValue(), false,
                    new String[] {"direct", "cursor"}),
            new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.toString(),
                    Boolean.toString(
                            SQLServerDriverBooleanProperty.SEND_STRING_PARAMETERS_AS_UNICODE.getDefaultValue()),
                    false, TRUE_FALSE),
            new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.SERVER_NAME_AS_ACE.toString(),
                    Boolean.toString(SQLServerDriverBooleanProperty.SERVER_NAME_AS_ACE.getDefaultValue()), false,
                    TRUE_FALSE),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.DOMAIN.toString(),
                    SQLServerDriverStringProperty.DOMAIN.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.SERVER_NAME.toString(),
                    SQLServerDriverStringProperty.SERVER_NAME.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.IPADDRESS_PREFERENCE.toString(),
                    SQLServerDriverStringProperty.IPADDRESS_PREFERENCE.getDefaultValue(), false,
                    new String[] {IPAddressPreference.IPv4First.toString(), IPAddressPreference.IPv6First.toString(),
                            IPAddressPreference.UsePlatformDefault.toString()}),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.SERVER_SPN.toString(),
                    SQLServerDriverStringProperty.SERVER_SPN.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.REALM.toString(),
                    SQLServerDriverStringProperty.REALM.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.SOCKET_FACTORY_CLASS.toString(),
                    SQLServerDriverStringProperty.SOCKET_FACTORY_CLASS.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.SOCKET_FACTORY_CONSTRUCTOR_ARG.toString(),
                    SQLServerDriverStringProperty.SOCKET_FACTORY_CONSTRUCTOR_ARG.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.TRANSPARENT_NETWORK_IP_RESOLUTION.toString(),
                    Boolean.toString(
                            SQLServerDriverBooleanProperty.TRANSPARENT_NETWORK_IP_RESOLUTION.getDefaultValue()),
                    false, TRUE_FALSE),
            new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.TRUST_SERVER_CERTIFICATE.toString(),
                    Boolean.toString(SQLServerDriverBooleanProperty.TRUST_SERVER_CERTIFICATE.getDefaultValue()), false,
                    TRUE_FALSE),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.TRUST_STORE_TYPE.toString(),
                    SQLServerDriverStringProperty.TRUST_STORE_TYPE.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.TRUST_STORE.toString(),
                    SQLServerDriverStringProperty.TRUST_STORE.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.toString(),
                    SQLServerDriverStringProperty.TRUST_STORE_PASSWORD.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.TRUST_MANAGER_CLASS.toString(),
                    SQLServerDriverStringProperty.TRUST_MANAGER_CLASS.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.TRUST_MANAGER_CONSTRUCTOR_ARG.toString(),
                    SQLServerDriverStringProperty.TRUST_MANAGER_CONSTRUCTOR_ARG.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.REPLICATION.toString(),
                    Boolean.toString(SQLServerDriverBooleanProperty.REPLICATION.getDefaultValue()), false, TRUE_FALSE),
            new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.SEND_TIME_AS_DATETIME.toString(),
                    Boolean.toString(SQLServerDriverBooleanProperty.SEND_TIME_AS_DATETIME.getDefaultValue()), false,
                    TRUE_FALSE),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.USER.toString(),
                    SQLServerDriverStringProperty.USER.getDefaultValue(), true, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.WORKSTATION_ID.toString(),
                    SQLServerDriverStringProperty.WORKSTATION_ID.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.XOPEN_STATES.toString(),
                    Boolean.toString(SQLServerDriverBooleanProperty.XOPEN_STATES.getDefaultValue()), false, TRUE_FALSE),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.AUTHENTICATION_SCHEME.toString(),
                    SQLServerDriverStringProperty.AUTHENTICATION_SCHEME.getDefaultValue(), false,
                    new String[] {AuthenticationScheme.javaKerberos.toString(),
                            AuthenticationScheme.nativeAuthentication.toString(),
                            AuthenticationScheme.ntlm.toString()}),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.AUTHENTICATION.toString(),
                    SQLServerDriverStringProperty.AUTHENTICATION.getDefaultValue(), false,
                    new String[] {SqlAuthentication.NotSpecified.toString(), SqlAuthentication.SqlPassword.toString(),
                            SqlAuthentication.ActiveDirectoryPassword.toString(),
                            SqlAuthentication.ActiveDirectoryIntegrated.toString(),
                            SqlAuthentication.ActiveDirectoryManagedIdentity.toString(),
                            SqlAuthentication.ActiveDirectoryServicePrincipal.toString(),
                            SqlAuthentication.ActiveDirectoryInteractive.toString()}),
            new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.SOCKET_TIMEOUT.toString(),
                    Integer.toString(SQLServerDriverIntProperty.SOCKET_TIMEOUT.getDefaultValue()), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.FIPS.toString(),
                    Boolean.toString(SQLServerDriverBooleanProperty.FIPS.getDefaultValue()), false, TRUE_FALSE),
            new SQLServerDriverPropertyInfo(
                    SQLServerDriverBooleanProperty.ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT.toString(),
                    Boolean.toString(SQLServerDriverBooleanProperty.ENABLE_PREPARE_ON_FIRST_PREPARED_STATEMENT
                            .getDefaultValue()),
                    false, TRUE_FALSE),
            new SQLServerDriverPropertyInfo(
                    SQLServerDriverIntProperty.SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD.toString(),
                    Integer.toString(
                            SQLServerDriverIntProperty.SERVER_PREPARED_STATEMENT_DISCARD_THRESHOLD.getDefaultValue()),
                    false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.STATEMENT_POOLING_CACHE_SIZE.toString(),
                    Integer.toString(SQLServerDriverIntProperty.STATEMENT_POOLING_CACHE_SIZE.getDefaultValue()), false,
                    null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.JAAS_CONFIG_NAME.toString(),
                    SQLServerDriverStringProperty.JAAS_CONFIG_NAME.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.SSL_PROTOCOL.toString(),
                    SQLServerDriverStringProperty.SSL_PROTOCOL.getDefaultValue(), false,
                    new String[] {SSLProtocol.TLS.toString(), SSLProtocol.TLS_V10.toString(),
                            SSLProtocol.TLS_V11.toString(), SSLProtocol.TLS_V12.toString()}),
            new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.CANCEL_QUERY_TIMEOUT.toString(),
                    Integer.toString(SQLServerDriverIntProperty.CANCEL_QUERY_TIMEOUT.getDefaultValue()), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.USE_BULK_COPY_FOR_BATCH_INSERT.toString(),
                    Boolean.toString(SQLServerDriverBooleanProperty.USE_BULK_COPY_FOR_BATCH_INSERT.getDefaultValue()),
                    false, TRUE_FALSE),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.MSI_CLIENT_ID.toString(),
                    SQLServerDriverStringProperty.MSI_CLIENT_ID.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.KEY_VAULT_PROVIDER_CLIENT_ID.toString(),
                    SQLServerDriverStringProperty.KEY_VAULT_PROVIDER_CLIENT_ID.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.KEY_VAULT_PROVIDER_CLIENT_KEY.toString(),
                    SQLServerDriverStringProperty.KEY_VAULT_PROVIDER_CLIENT_KEY.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.USE_FMT_ONLY.toString(),
                    Boolean.toString(SQLServerDriverBooleanProperty.USE_FMT_ONLY.getDefaultValue()), false, TRUE_FALSE),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.KEY_STORE_PRINCIPAL_ID.toString(),
                    SQLServerDriverStringProperty.KEY_STORE_PRINCIPAL_ID.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.CLIENT_CERTIFICATE.toString(),
                    SQLServerDriverStringProperty.CLIENT_CERTIFICATE.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.CLIENT_KEY.toString(),
                    SQLServerDriverStringProperty.CLIENT_KEY.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.CLIENT_KEY_PASSWORD.toString(),
                    SQLServerDriverStringProperty.CLIENT_KEY_PASSWORD.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverBooleanProperty.DELAY_LOADING_LOBS.toString(),
                    Boolean.toString(SQLServerDriverBooleanProperty.DELAY_LOADING_LOBS.getDefaultValue()), false,
                    TRUE_FALSE),
            new SQLServerDriverPropertyInfo(
                    SQLServerDriverBooleanProperty.SEND_TEMPORAL_DATATYPES_AS_STRING_FOR_BULK_COPY.toString(),
                    Boolean.toString(SQLServerDriverBooleanProperty.SEND_TEMPORAL_DATATYPES_AS_STRING_FOR_BULK_COPY
                            .getDefaultValue()),
                    false, TRUE_FALSE),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.AAD_SECURE_PRINCIPAL_ID.toString(),
                    SQLServerDriverStringProperty.AAD_SECURE_PRINCIPAL_ID.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.AAD_SECURE_PRINCIPAL_SECRET.toString(),
                    SQLServerDriverStringProperty.AAD_SECURE_PRINCIPAL_SECRET.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.MAX_RESULT_BUFFER.toString(),
                    SQLServerDriverStringProperty.MAX_RESULT_BUFFER.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.CONNECT_RETRY_COUNT.toString(),
                    Integer.toString(SQLServerDriverIntProperty.CONNECT_RETRY_COUNT.getDefaultValue()), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverIntProperty.CONNECT_RETRY_INTERVAL.toString(),
                    Integer.toString(SQLServerDriverIntProperty.CONNECT_RETRY_INTERVAL.getDefaultValue()), false,
                    null),};

    /**
     * Properties that can only be set by using Properties. Cannot set in connection string
     */
    private static final SQLServerDriverPropertyInfo[] DRIVER_PROPERTIES_PROPERTY_ONLY = {
            // default required available choices
            // property name value property (if appropriate)
            new SQLServerDriverPropertyInfo(SQLServerDriverStringProperty.ACCESS_TOKEN.toString(),
                    SQLServerDriverStringProperty.ACCESS_TOKEN.getDefaultValue(), false, null),
            new SQLServerDriverPropertyInfo(SQLServerDriverObjectProperty.GSS_CREDENTIAL.toString(),
                    SQLServerDriverObjectProperty.GSS_CREDENTIAL.getDefaultValue(), false, null),};

    private static final String driverPropertiesSynonyms[][] = {
            {"database", SQLServerDriverStringProperty.DATABASE_NAME.toString()},
            {"userName", SQLServerDriverStringProperty.USER.toString()},
            {"server", SQLServerDriverStringProperty.SERVER_NAME.toString()},
            {"domainName", SQLServerDriverStringProperty.DOMAIN.toString()},
            {"port", SQLServerDriverIntProperty.PORT_NUMBER.toString()}};

    private static final String driverPropertyValuesSynonyms[][] = {
            {"ActiveDirectoryMSI", SqlAuthentication.ActiveDirectoryManagedIdentity.toString()}};

    static private final AtomicInteger baseID = new AtomicInteger(0); // Unique id generator for each instance (used for
                                                                      // logging

    final private int instanceID; // Unique id for this instance.
    final private String traceID;

    // Returns unique id for each instance.
    private static int nextInstanceID() {
        return baseID.incrementAndGet();
    }

    @Override
    final public String toString() {
        return traceID;
    }

    static final private java.util.logging.Logger loggerExternal = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.Driver");
    static final private java.util.logging.Logger parentLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc");
    final private String loggingClassName;

    String getClassNameLogging() {
        return loggingClassName;
    }

    private final static java.util.logging.Logger drLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerDriver");
    private static java.sql.Driver mssqlDriver = null;
    // Register with the DriverManager
    static {
        try {
            register();
        } catch (SQLException e) {
            if (drLogger.isLoggable(Level.FINER) && Util.isActivityTraceOn()) {
                drLogger.finer("Error registering driver: " + e);
            }
        }
    }

    /**
     * Registers the driver with DriverManager. No-op if driver is already registered.
     * 
     * @throws SQLException
     *         if error
     */
    public static void register() throws SQLException {
        if (!isRegistered()) {
            mssqlDriver = new SQLServerDriver();
            DriverManager.registerDriver(mssqlDriver);
        }
    }

    /**
     * De-registers the driver with the DriverManager. No-op if the driver is not registered.
     * 
     * @throws SQLException
     *         if error
     */
    public static void deregister() throws SQLException {
        if (isRegistered()) {
            DriverManager.deregisterDriver(mssqlDriver);
            mssqlDriver = null;
        }
    }

    /**
     * Checks whether the driver has been registered with the driver manager.
     * 
     * @return if the driver has been registered with the driver manager
     */
    public static boolean isRegistered() {
        return mssqlDriver != null;
    }

    /**
     * Creates a SQLServerDriver object
     */
    public SQLServerDriver() {
        instanceID = nextInstanceID();
        traceID = "SQLServerDriver:" + instanceID;
        loggingClassName = "com.microsoft.sqlserver.jdbc." + "SQLServerDriver:" + instanceID;
    }

    /**
     * Provides Helper function used to fix the case sensitivity, synonyms and remove unknown tokens from the
     * properties.
     */
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
                } else if ("gsscredential".equalsIgnoreCase(newname) && (props.get(name) instanceof GSSCredential)) {
                    fixedup.put(newname, props.get(name));
                } else {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidpropertyValue"));
                    Object[] msgArgs = {name};
                    throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
                }
            }

        }
        return fixedup;
    }

    /**
     * Provides Helper function used to merge together the property set extracted from the url and the user supplied
     * property set passed in by the caller. This function is used by both SQLServerDriver.connect and
     * SQLServerDataSource.getConnectionInternal to centralize this property merging code.
     */
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
     * Returns the normalized the property names.
     * 
     * @param name
     *        name to normalize
     * @param logger
     * @return the normalized property name
     */
    static String getNormalizedPropertyName(String name, Logger logger) {
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
     * Returns the normalized the property value name.
     *
     * @param name
     *        name to normalize
     *
     * @return the normalized property value name
     */
    static String getNormalizedPropertyValueName(String name) {
        if (null == name)
            return name;

        for (String[] driverPropertyValueSynonym : driverPropertyValuesSynonyms) {
            if (driverPropertyValueSynonym[0].equalsIgnoreCase(name)) {
                return driverPropertyValueSynonym[1];
            }
        }

        if (parentLogger.isLoggable(Level.FINER)) {
            parentLogger.finer("Unknown property value: " + name);
        }

        return "";
    }

    /**
     * Returns the property-only names that do not work with connection string.
     * 
     * @param name
     *        to normalize
     * @param logger
     * @return the normalized property name
     */
    static String getPropertyOnlyName(String name, Logger logger) {
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

    private final static String[] systemPropertiesToLog = new String[] {"java.specification.vendor",
            "java.specification.version", "java.class.path", "java.class.version", "java.runtime.name",
            "java.runtime.version", "java.vendor", "java.version", "java.vm.name", "java.vm.vendor", "java.vm.version",
            "java.vm.specification.vendor", "java.vm.specification.version", "os.name", "os.version", "os.arch"};

    @Override
    public java.sql.Connection connect(String Url, Properties suppliedProperties) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "connect", "Arguments not traced.");
        SQLServerConnection result = null;

        if (loggerExternal.isLoggable(Level.FINE)) {
            loggerExternal.log(Level.FINE,
                    "Microsoft JDBC Driver " + SQLJdbcVersion.major + "." + SQLJdbcVersion.minor + "."
                            + SQLJdbcVersion.patch + "." + SQLJdbcVersion.build + SQLJdbcVersion.releaseExt
                            + " for SQL Server");
            if (loggerExternal.isLoggable(Level.FINER)) {
                for (String propertyKeyName : systemPropertiesToLog) {
                    String propertyValue = System.getProperty(propertyKeyName);
                    if (propertyValue != null && !propertyValue.isEmpty()) {
                        loggerExternal.log(Level.FINER, "System Property: " + propertyKeyName + " Value: "
                                + System.getProperty(propertyKeyName));
                    }
                }
            }
        }

        // Merge connectProperties (from URL) and supplied properties from user.
        Properties connectProperties = parseAndMergeProperties(Url, suppliedProperties);
        if (connectProperties != null) {
            result = DriverJDBCVersion.getSQLServerConnection(toString());
            result.connect(connectProperties, null);
        }
        loggerExternal.exiting(getClassNameLogging(), "connect", result);
        return result;
    }

    private Properties parseAndMergeProperties(String Url, Properties suppliedProperties) throws SQLServerException {
        if (Url == null) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_nullConnection"), null, 0, false);
        }

        // Pull the URL properties into the connection properties
        Properties connectProperties = Util.parseUrl(Url, drLogger);
        if (null == connectProperties)
            return null; // If we are the wrong driver dont throw an exception

        String loginTimeoutProp = connectProperties.getProperty(SQLServerDriverIntProperty.LOGIN_TIMEOUT.toString());
        int dmLoginTimeout = DriverManager.getLoginTimeout();

        // Use Driver Manager's login timeout if it exceeds 0 and loginTimeout connection property is not provided.
        if (dmLoginTimeout > 0 && null == loginTimeoutProp) {
            connectProperties.setProperty(SQLServerDriverIntProperty.LOGIN_TIMEOUT.toString(),
                    String.valueOf(dmLoginTimeout));
        }

        // Merge connectProperties (from URL) and supplied properties from user.
        connectProperties = mergeURLAndSuppliedProperties(connectProperties, suppliedProperties);

        return connectProperties;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLServerException {
        loggerExternal.entering(getClassNameLogging(), "acceptsURL", "Arguments not traced.");

        if (null == url) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_nullConnection"), null, 0, false);
        }

        boolean result = false;
        try {
            result = (Util.parseUrl(url, drLogger) != null);
        } catch (SQLServerException e) {
            // ignore the exception from the parse URL failure, if we cant parse the URL we do not accept em
            result = false;
        }
        loggerExternal.exiting(getClassNameLogging(), "acceptsURL", result);
        return result;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String Url, Properties Info) throws SQLServerException {
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

    @Override
    public int getMajorVersion() {
        loggerExternal.entering(getClassNameLogging(), "getMajorVersion");
        loggerExternal.exiting(getClassNameLogging(), "getMajorVersion", SQLJdbcVersion.major);
        return SQLJdbcVersion.major;
    }

    @Override
    public int getMinorVersion() {
        loggerExternal.entering(getClassNameLogging(), "getMinorVersion");
        loggerExternal.exiting(getClassNameLogging(), "getMinorVersion", SQLJdbcVersion.minor);
        return SQLJdbcVersion.minor;
    }

    @Override
    public Logger getParentLogger() {
        return parentLogger;
    }

    @Override
    public boolean jdbcCompliant() {
        loggerExternal.entering(getClassNameLogging(), "jdbcCompliant");
        loggerExternal.exiting(getClassNameLogging(), "jdbcCompliant", Boolean.TRUE);
        return true;
    }
}
