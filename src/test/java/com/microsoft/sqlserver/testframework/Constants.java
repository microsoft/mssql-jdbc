package com.microsoft.sqlserver.testframework;

import java.sql.Date;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;


public final class Constants {
    private Constants() {}

    /**
     * Use below tags for tests to exclude them from test group:
     * 
     * <pre>
     * xJDBC42 - - - - - - For tests not compatible with JDBC 42 Specifications
     * xGradle - - - - - - For tests not compatible with Gradle Script (e.g. Manifest File)
     * xSQLv11 - - - - - - For tests not compatible with SQL Server 2012
     * xSQLv12 - - - - - - For tests not compatible with SQL Server 2008 R2 - 2014
     * xSQLv14 - - - - - - For tests not compatible with SQL Server 2016 - 2017
     * xSQLv15 - - - - - - For tests not compatible with SQL Server 2019
     * xSQLv16 - - - - - - For tests not compatible with SQL Server 2022
     * xAzureSQLDB - - - - For tests not compatible with Azure SQL Database
     * xAzureSQLDW - - - - For tests not compatible with Azure Data Warehouse
     * xAzureSQLMI - - - - For tests not compatible with Azure SQL Managed Instance
     * NTLM  - - - - - - - For NTLM tests
     * Kerberos  - - - - - For Kerberos tests
     * reqExternalSetup  - For tests requiring external setup
     * clientCertAuth  - - For tests requiring client certificate authentication setup
     * Fedauth - - - - - - For Fedauth tests
     * JSONTest -  - - - - For tests requiring JSON setup
     * </pre>
     */
    public static final String xJDBC42 = "xJDBC42";
    public static final String xGradle = "xGradle";
    public static final String xSQLv11 = "xSQLv11";
    public static final String xSQLv12 = "xSQLv12";
    public static final String xSQLv14 = "xSQLv14";
    public static final String xSQLv15 = "xSQLv15";
    public static final String xSQLv16 = "xSQLv16";
    public static final String xAzureSQLDB = "xAzureSQLDB";
    public static final String xAzureSQLDW = "xAzureSQLDW";
    public static final String xAzureSQLMI = "xAzureSQLMI";
    public static final String NTLM = "NTLM";
    public static final String kerberos = "kerberos";
    public static final String MSI = "MSI";
    public static final String reqExternalSetup = "reqExternalSetup";
    public static final String clientCertAuth = "clientCertAuth";
    public static final String fedAuth = "fedAuth";
    public static final String requireSecret = "requireSecret";
    public static final String JSONTest = "JSONTest";

    public static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();
    public static final Logger LOGGER = Logger.getLogger("AbstractTest");
    public static final String JKS_SECRET_STRING = "changeit";
    public static final String JDBC_PREFIX = "jdbc:sqlserver://";
    public static final String DEFAULT_DRIVER_LOG = "Driver.log";
    public static final String MSSQL_JDBC_PACKAGE = "com.microsoft.sqlserver.jdbc";
    public static final String MSSQL_JDBC_PROPERTIES = "mssql-jdbc.properties";
    public static final String AKV_TRUSTED_ENDPOINTS_KEYWORD = "AKVTrustedEndpoints";

    public static final String DEFAULT_WRAP_IDENTIFIER = "\'";
    public static final String CREATE_TABLE = "CREATE TABLE";
    public static final String SPACE_CHAR = " ";
    public static final String OPEN_BRACKET = "(";
    public static final String CLOSE_BRACKET = ")";
    public static final String NOT = "NOT";
    public static final String NULL = "NULL";
    public static final String PRIMARY_KEY = "PRIMARY KEY";
    public static final String DEFAULT = "DEFAULT";
    public static final String COMMA = ",";
    public static final String QUESTION_MARK = "?";
    public static final String SINGLE_QUOTE = "'";
    public static final String SEMI_COLON = ";";
    public static final String COLON = ":";
    public static final String EQUAL_TO = "=";
    public static final String BACK_SLASH = "\\";

    public static final String ENABLED = "Enabled";

    // Environment properties
    public static final String FIPS_ENV = "FIPS_ENV";
    public static final String MSSQL_JDBC_TEST_CONNECTION_PROPERTIES = "mssql_jdbc_test_connection_properties";
    public static final String MSSQL_JDBC_LOGGING = "mssql_jdbc_logging";
    public static final String MSSQL_JDBC_LOGGING_HANDLER = "mssql_jdbc_logging_handler";

    public static final String LOGGING_HANDLER_FILE = "file";
    public static final String LOGGING_HANDLER_CONSOLE = "console";
    public static final String LOGGING_HANDLER_STREAM = "stream";

    public static final int ENGINE_EDITION_FOR_SQL_AZURE = 5;
    public static final int ENGINE_EDITION_FOR_SQL_AZURE_DW = 6;

    public static final Date DATE = new Date(new java.util.Date().getTime());
    public static final LocalDateTime NOW = LocalDateTime.now();

    public static final int LOB_ARRAY_SIZE = 500; // number of rows to insert into the table and compare
    public static final int LOB_LENGTH_MIN = 8000;
    public static final int LOB_LENGTH_MAX = 32000;
    public static final String ASCII_CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()-=_+,./;'[]<>?:{}|`~\"\\";
    public static final String UNICODE_CHARACTERS = ASCII_CHARACTERS
            + "Ǥ⚌c♮ƺåYèĢù⚏Ȓ★ǌäõpƸŃōoƝĤßuÙőƆE♹gǇÜŬȺǱ!Û☵ŦãǁĸNQŰǚǻTÖC]ǶýåÉbɉ☩=\\ȍáźŗǃĻýű☓☄¸T☑ö^k☏I:x☑⚀läiȉ☱☚⚅ǸǎãÂ";

    // AE Constants
    public static final String WINDOWS_KEY_STORE_NAME = "MSSQL_CERTIFICATE_STORE";
    public static final String AZURE_KEY_VAULT_NAME = "AZURE_KEY_VAULT";
    public static final String JAVA_KEY_STORE_NAME = "MSSQL_JAVA_KEYSTORE";
    public static final String CUSTOM_KEYSTORE_NAME = "CUSTOM_KEYSTORE";
    public static final String DUMMY_KEYSTORE_NAME = "DUMMY_PROVIDER";
    public static final String JAVA_KEY_STORE_FILENAME = "JavaKeyStore.txt";
    public static final String JAVA_KEY_STORE_SECRET = "JavaKeyStorePassword";
    public static final String JKS = "JKS";
    public static final String JKS_NAME = "clientcert.jks";
    public static final String JKS_SECRET = "password";
    public static final String PKCS12 = "PKCS12";

    public static final String UID = UUID.randomUUID().toString();
    public static final Long RANDOM_LONG = Long.valueOf((long) (RANDOM.nextDouble() * Math.pow(10, 10)));
    public static final String CMK_NAME = "JDBC_CMK_" + RANDOM_LONG;

    public static final String CMK_SIGNATURE_WIN = "0x625F3105C77DF6881D11B9D5A64491452F288AA9CE66189F19AF9591FAF2525B90CE580DD29CF897F6809EC30AF583CF1603477C5591D785A89DA3CE60E03D9CC494926322E0EF15A4EA38C0FFA0B90D29D051DA037631E6D3B435F87BAB46927ECF366AE96C5259D03A227A2BC04B15E09794CA1ABD1B34CB117BF06962225D4157DD1FBEF998D547EC7E201C00ACF2D2B8A746D1FD3FAEED791132D97EA396CABE9C472C8B559C5EF35342EE9D5635C7C18C1BD168647E3BA52B674626D9F8C3CFAC150300EDA157DE3D1B1D7FFB946D1CCE37ACFD28F62D57365BA89088627EE9469B04142A5F5505B71C94999A8D4A1269F15842DF3DA3CC5B41DDD7B115";

    public static final String CEK_NAME = "JDBC_CEK_" + RANDOM_LONG;
    public static final String CEK_ALGORITHM = "RSA_OAEP";
    public static final String CEK_STRING = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    public static final String CEK_ENCRYPTED_VALUE = "0x016E000001630075007200720065006E00740075007300650072002F006D0079002F006100320032003600650031003800310034003300350063003300370065003800660035003500330035003500660066006100340037003400340062003600330035003600610031006200650035003500A83E74C244AD62ED7D83FDEAED3923B4D843BBB8FDE1ECFEE11DB5275144BC936FD3485D157B4921A6CF12EEEAD52F6BAB5822383C7101508523C51858A1487FD10173CD945159418B70DD9FF432D28B453146FA72F90CFA90810B0E905C95FD99D1CA4009BA3D56C783853751EA74482C53C49667C62DD588452473FFECA835B5233B1BDACD4C461560635204DF8EE674C0A3AD63E4C63D787B6602B0902306DC145354C05B7DD74EDED34D5DE4F05379851E36612C9D3BA3B8551BC558FCF7F705711D22AD4C7CD99931662B73DD647B1B3C1FB109A5850919EFB75774F64ECBEBD7808A0D69FBE124906CDD39B7552B03712FDF9B46399766E79B6BFCBBC75267496B257E40BA05CDB4E294C7353A2FCEA7C0983BF5E1599B71B2C975A682D29CDC9CEEEFE8E11676004C6F1217FB7726451A708A7C1F6CADEB00D2D81110771DEB9050B244B20DC787C58FBD07F2B4AE7467422249A4134D63E09BE26ED165EB1393F313B1877399C803B61E1860F5D8223054DD1F07835492C3AEFA48FDF71F123D7DE430DD1EA69D82B91B85176BF46306FC3D68CC43497A9D625584213BEF754FB0E8D64201FEA74DAE37E3D3BF6D58D1A7CA9502173C192E038DD7CEB5A16A7982044538021AE7B96A94B9941AF87AE879032F783108729EB366114DFCF7A68B440F26D752BEAE6433E90F857C13EAB1CCE1EC42D9897893488E4182";

    // Encrypt Options
    public static final String FALSE = "FALSE";
    public static final String TRUE = "TRUE";
    public static final String STRICT = "STRICT";

    // Start: Connection Properties parsed in AbstractTest class for creating DataSource.
    public static final String INTEGRATED_SECURITY = "INTEGRATEDSECURITY";
    public static final String USER = "USER";
    public static final String USER_NAME = "USERNAME";
    public static final String PORT = "PORT";
    public static final String PORT_NUMBER = "PORTNUMBER";
    public static final String PASSWORD = "PASSWORD";
    public static final String DOMAIN = "DOMAIN";
    public static final String DOMAIN_NAME = "DOMAINNAME";
    public static final String DATABASE = "DATABASE";
    public static final String DATABASE_NAME = "DATABASENAME";
    public static final String SERVER_NAME = "SERVERNAME";

    public static final String COLUMN_ENCRYPTION_SETTING = "COLUMNENCRYPTIONSETTING";
    public static final String DISABLE_STATEMENT_POOLING = "DISABLESTATEMENTPOOLING";
    public static final String STATEMENT_POOLING_CACHE_SIZE = "STATEMENTPOOLINGCACHESIZE";
    public static final String AUTHENTICATION = "AUTHENTICATION";
    public static final String AUTHENTICATION_SCHEME = "AUTHENTICATIONSCHEME";
    public static final String CANCEL_QUERY_TIMEOUT = "CANCELQUERYTIMEOUT";
    public static final String ENCRYPT = "ENCRYPT";
    public static final String HOST_NAME_IN_CERTIFICATE = "HOSTNAMEINCERTIFICATE";
    public static final String MAX_RESULT_BUFFER = "MAXRESULTBUFFER";
    // End: Connection Properties parsed in AbstractTest class for creating DataSource.

    // Other Connection Properties set in FipsTest
    public static final String FIPS = "FIPS";
    public static final String TRUST_STORE_TYPE = "TRUSTSTORETYPE";
    public static final String TRUST_SERVER_CERTIFICATE = "TRUSTSERVERCERTIFICATE";
    public static final String TRUST_STORE_SECRET_PROPERTY = "TRUSTSTOREPASSWORD";
    public static final String TRUST_STORE = "TRUSTSTORE";

    public static final String ENCLAVE_ATTESTATIONURL = "enclaveAttestationUrl";
    public static final String ENCLAVE_ATTESTATIONPROTOCOL = "enclaveAttestationProtocol";

    public static final String MSICLIENTID = "MSICLIENTID";
    public static final String KEYVAULTPROVIDER_CLIENTID = "KEYVAULTPROVIDERCLIENTID";
    public static final String KEYVAULTPROVIDER_CLIENTKEY = "KEYVAULTPROVIDERCLIENTKEY";
    public static final String KEYSTORE_AUTHENTICATION = "KEYSTOREAUTHENTICATION";
    public static final String KEYSTORE_PRINCIPALID = "KEYSTOREPRINCIPALID";
    public static final String KEYSTORE_SECRET = "KEYSTORESECRET";
    public static final String KEYSTORE_LOCATION = "KEYSTORELOCATION";
    public static final String CLIENT_CERTIFICATE = "CLIENTCERTIFICATE";
    public static final String CLIENT_KEY = "CLIENTKEY";
    public static final String AAD_SECURE_PRINCIPAL_ID = "AADSECUREPRINCIPALID";
    public static final String AAD_SECURE_PRINCIPAL_SECRET = "AADSECUREPRINCIPALSECRET";

    public static final String CONNECT_RETRY_COUNT = "CONNECTRETRYCOUNT";
    public static final String CONNECT_RETRY_INTERVAL = "CONNECTRETRYINTERVAL";

    public static final String CLIENT_KEY_PASSWORD = "CLIENTKEYPASSWORD";
    public static final String SEND_TEMPORAL_DATATYPES_AS_STRING_FOR_BULK_COPY = "SENDTEMPORALDATATYPESASSTRINGFORBULKCOPY";
    public static final String PREPARE_METHOD = "PREPAREMETHOD";
    public static final String CONFIG_PROPERTIES_FILE = "config.properties";
    public static final String UTF8 = "UTF-8";
    public static final double MAX_VALUE_MONEY = 922337203685477.5807;
    public static final double MIN_VALUE_MONEY = -922337203685477.5808;
    public static final double MAX_VALUE_SMALLMONEY = 214748.3647;
    public static final double MIN_VALUE_SMALLMONEY = -214748.3648;

    public enum LOB {
        CLOB,
        NCLOB,
        BLOB
    };
}
