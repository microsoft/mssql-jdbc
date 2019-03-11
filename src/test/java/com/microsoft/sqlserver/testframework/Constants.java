package com.microsoft.sqlserver.testframework;

import java.sql.Date;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import com.microsoft.sqlserver.jdbc.RandomUtil;


public class Constants {
    protected static final ThreadLocalRandom RANDOM = ThreadLocalRandom.current();
    protected static final Logger LOGGER = Logger.getLogger("AbstractTest");
    protected static final String JKS_SECRET_STRING = "changeit";
    protected static final String JDBC_PREFIX = "jdbc:sqlserver://";
    protected static final String DEFAULT_DRIVER_LOG = "Driver.log";
    protected static final String MSSQL_JDBC_PACKAGE = "com.microsoft.sqlserver.jdbc";

    protected static final String DEFAULT_WRAP_IDENTIFIER = "\'";
    protected static final String CREATE_TABLE = "CREATE TABLE";
    protected static final String SPACE_CHAR = " ";
    protected static final String OPEN_BRACKET = "(";
    protected static final String CLOSE_BRACKET = ")";
    protected static final String NOT = "NOT";
    protected static final String NULL = "NULL";
    protected static final String PRIMARY_KEY = "PRIMARY KEY";
    protected static final String DEFAULT = "DEFAULT";
    protected static final String COMMA = ",";
    protected static final String QUESTION_MARK = "?";
    protected static final String SINGLE_QUOTE = "'";
    protected static final String SEMI_COLON = ";";
    protected static final String COLON = ":";
    protected static final String EQUAL_TO = "=";
    protected static final String BACK_SLASH = "\\";

    protected static final String ENABLED = "Enabled";

    // Environment properties
    protected static final String FIPS_ENV = "FIPS_ENV";
    protected static final String MSSQL_JDBC_TEST_CONNECTION_PROPERTIES = "mssql_jdbc_test_connection_properties";
    protected static final String MSSQL_JDBC_LOGGING = "mssql_jdbc_logging";
    protected static final String MSSQL_JDBC_LOGGING_HANDLER = "mssql_jdbc_logging_handler";

    protected static final String LOGGING_HANDLER_FILE = "file";
    protected static final String LOGGING_HANDLER_CONSOLE = "console";

    protected final static int ENGINE_EDITION_FOR_SQL_AZURE = 5;
    protected final static int ENGINE_EDITION_FOR_SQL_AZURE_DW = 6;

    protected static final Date DATE = new Date(new java.util.Date().getTime());

    // AE Constants
    protected static final String JAVA_KEY_STORE_FILENAME = "JavaKeyStore.txt";
    protected static final String JAVA_KEY_STORE_NAME = "MSSQL_JAVA_KEYSTORE";
    protected static final String JAVA_KEY_STORE_PASSWORD = "JavaKeyStorePassword";
    protected static final String JKS = "JKS";
    protected static final String JKS_NAME = "clientcert.jks";
    protected static final String JKS_SECRET = "password";
    protected static final String PKCS12 = "PKCS12";

    protected static final String UID = UUID.randomUUID().toString();
    protected static final Long RANDOM_LONG = Long.valueOf((long) (RANDOM.nextDouble() * Math.pow(10, 10)));
    protected static final String CMK_NAME = "JDBC_CMK_" + RANDOM_LONG;
    protected static final String CEK_NAME = "JDBC_CEK_" + RANDOM_LONG;
    protected static final String CEK_ALGORITHM = "RSA_OAEP";
    protected static final String CEK_STRING = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    protected static final String CHAR_TABLE_AE = RandomUtil.getIdentifier("JDBCEncryptedChar");
    protected static final String BINARY_TABLE_AE = RandomUtil.getIdentifier("JDBCEncryptedBinary");
    protected static final String DATE_TABLE_AE = RandomUtil.getIdentifier("JDBCEncryptedDate");
    protected static final String NUMERIC_TABLE_AE = RandomUtil.getIdentifier("JDBCEncryptedNumeric");
    protected static final String SCALE_DATE_TABLE_AE = RandomUtil.getIdentifier("JDBCEncryptedScaleDate");

    // Start: Connection Properties parsed in AbstractTest class for creating DataSource.
    protected static final String INTEGRATED_SECURITY = "INTEGRATEDSECURITY";
    protected static final String USER = "USER";
    protected static final String USER_NAME = "USERNAME";
    protected static final String PORT = "PORT";
    protected static final String PORT_NUMBER = "PORTNUMBER";
    protected static final String PASSWORD = "PASSWORD";
    protected static final String DATABASE = "DATABASE";
    protected static final String DATABASE_NAME = "DATABASENAME";
    protected static final String COLUMN_ENCRYPTION_SETTING = "COLUMNENCRYPTIONSETTING";
    protected static final String DISABLE_STATEMENT_POOLING = "DISABLESTATEMENTPOOLING";
    protected static final String STATEMENT_POOLING_CACHE_SIZE = "STATEMENTPOOLINGCACHESIZE";
    protected static final String AUTHENTICATION = "AUTHENTICATION";
    protected static final String AUTHENTICATION_SCHEME = "AUTHENTICATIONSCHEME";
    protected static final String CANCEL_QUERY_TIMEOUT = "CANCELQUERYTIMEOUT";
    protected static final String ENCRYPT = "ENCRYPT";
    protected static final String HOST_NAME_IN_CERTIFICATE = "HOSTNAMEINCERTIFICATE";
    // End: Connection Properties parsed in AbstractTest class for creating DataSource.

    // Other Connection Properties set in FipsTest
    protected static final String FIPS = "FIPS";
    protected static final String TRUST_STORE_TYPE = "TRUSTSTORETYPE";
    protected static final String TRUST_SERVER_CERTIFICATE = "TrustServerCertificate";

}
