package com.microsoft.sqlserver.testframework;

import java.sql.Date;
import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import com.microsoft.sqlserver.jdbc.RandomUtil;


public final class Constants {
    private Constants() {}

    /**
     * Use below tags for tests to exclude them from test group:
     * 
     * <pre>
     * xJDBC42 - - - - - - For tests not compatible with JDBC 42 Specifications
     * xGradle - - - - - - For tests not compatible with Gradle Script (e.g. Manifest File)
     * xSQLv12 - - - - - - For tests not compatible with SQL Server 2008 R2 - 2014
     * xSQLv14 - - - - - - For tests not compatible with SQL Server 2016 - 2017
     * xSQLv15 - - - - - - For tests not compatible with SQL Server 2019
     * xAzureSQLDB - - - - For tests not compatible with Azure SQL Database
     * xAzureSQLDW - - - - For tests not compatible with Azure Data Warehouse
     * xAzureSQLMI - - - - For tests not compatible with Azure SQL Managed Instance
     * NTLM  - - - - - - - For NTLM tests
     * reqExternalSetup  - For tests requiring external setup
     * </pre>
     */
    public static final String xJDBC42 = "xJDBC42";
    public static final String xGradle = "xGradle";
    public static final String xSQLv12 = "xSQLv12";
    public static final String xSQLv14 = "xSQLv14";
    public static final String xSQLv15 = "xSQLv15";
    public static final String xAzureSQLDB = "xAzureSQLDB";
    public static final String xAzureSQLDW = "xAzureSQLDW";
    public static final String xAzureSQLMI = "xAzureSQLMI";
    public static final String NTLM = "NTLM";
    public static final String reqExternalSetup = "reqExternalSetup";

    public static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();
    public static final Logger LOGGER = Logger.getLogger("AbstractTest");
    public static final String JKS_SECRET_STRING = "changeit";
    public static final String JDBC_PREFIX = "jdbc:sqlserver://";
    public static final String DEFAULT_DRIVER_LOG = "Driver.log";
    public static final String MSSQL_JDBC_PACKAGE = "com.microsoft.sqlserver.jdbc";

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
    public static final String JAVA_KEY_STORE_FILENAME = "JavaKeyStore.txt";
    public static final String JAVA_KEY_STORE_NAME = "MSSQL_JAVA_KEYSTORE";
    public static final String JAVA_KEY_STORE_SECRET = "JavaKeyStorePassword";
    public static final String JKS = "JKS";
    public static final String JKS_NAME = "clientcert.jks";
    public static final String JKS_SECRET = "password";
    public static final String PKCS12 = "PKCS12";

    public static final String UID = UUID.randomUUID().toString();
    public static final Long RANDOM_LONG = Long.valueOf((long) (RANDOM.nextDouble() * Math.pow(10, 10)));
    public static final String CMK_NAME = "JDBC_CMK_" + RANDOM_LONG;
    public static final String CEK_NAME = "JDBC_CEK_" + RANDOM_LONG;
    public static final String CEK_ALGORITHM = "RSA_OAEP";
    public static final String CEK_STRING = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    public static final String CHAR_TABLE_AE = RandomUtil.getIdentifier("JDBCEncryptedChar");
    public static final String BINARY_TABLE_AE = RandomUtil.getIdentifier("JDBCEncryptedBinary");
    public static final String DATE_TABLE_AE = RandomUtil.getIdentifier("JDBCEncryptedDate");
    public static final String NUMERIC_TABLE_AE = RandomUtil.getIdentifier("JDBCEncryptedNumeric");
    public static final String SCALE_DATE_TABLE_AE = RandomUtil.getIdentifier("JDBCEncryptedScaleDate");

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
    public static final String COLUMN_ENCRYPTION_SETTING = "COLUMNENCRYPTIONSETTING";
    public static final String DISABLE_STATEMENT_POOLING = "DISABLESTATEMENTPOOLING";
    public static final String STATEMENT_POOLING_CACHE_SIZE = "STATEMENTPOOLINGCACHESIZE";
    public static final String AUTHENTICATION = "AUTHENTICATION";
    public static final String AUTHENTICATION_SCHEME = "AUTHENTICATIONSCHEME";
    public static final String CANCEL_QUERY_TIMEOUT = "CANCELQUERYTIMEOUT";
    public static final String ENCRYPT = "ENCRYPT";
    public static final String HOST_NAME_IN_CERTIFICATE = "HOSTNAMEINCERTIFICATE";
    // End: Connection Properties parsed in AbstractTest class for creating DataSource.

    // Other Connection Properties set in FipsTest
    public static final String FIPS = "FIPS";
    public static final String TRUST_STORE_TYPE = "TRUSTSTORETYPE";
    public static final String TRUST_SERVER_CERTIFICATE = "TRUSTSERVERCERTIFICATE";
    public static final String TRUST_STORE_SECRET_PROPERTY = "TRUSTSTOREPASSWORD";
    public static final String TRUST_STORE = "TRUSTSTORE";

    public enum LOB {
        CLOB,
        NCLOB,
        BLOB
    };
}
