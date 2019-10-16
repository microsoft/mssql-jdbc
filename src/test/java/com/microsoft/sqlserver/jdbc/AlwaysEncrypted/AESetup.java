/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more AEInformation.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.RandomData;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionAzureKeyVaultProvider;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionJavaKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatementColumnEncryptionSetting;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

import microsoft.sql.DateTimeOffset;


/**
 * Setup for Always Encrypted test This test will work on Appveyor and Travis-ci as java key store gets created from the
 * .yml scripts. Users on their local machine should create the keystore manually and save the alias name in
 * JavaKeyStore.txt file. For local test purposes, put this in the target/test-classes directory
 *
 */
@RunWith(JUnitPlatform.class)
public class AESetup extends AbstractTest {

    static String filePath = null;
    static String thumbprint = null;
    static String javaKeyPath = null;
    static String javaKeyAliases = null;
    static String windowsKeyPath = null;
    static String applicationClientID = null;
    static String applicationKey = null;
    static String[] keyIDs = null;
    static String cmkJks = null;
    static String cmkWin = null;
    static String cmkAkv = null;
    static String cekJks = null;
    static String cekWin = null;
    static String cekAkv = null;

    static boolean kspRegistered = false;
    static SQLServerColumnEncryptionKeyStoreProvider jksProvider = null;
    static SQLServerColumnEncryptionAzureKeyVaultProvider akvProvider = null;
    static SQLServerStatementColumnEncryptionSetting stmtColEncSetting = null;
    static String AETestConnectionString;
    static Properties AEInfo;
    static boolean isAEv2Supported = false;

    public static final String tableName = RandomUtil.getIdentifier("AETest_");

    public static final String CHAR_TABLE_AE = RandomUtil.getIdentifier("JDBCEncryptedChar");
    public static final String BINARY_TABLE_AE = RandomUtil.getIdentifier("JDBCEncryptedBinary");
    public static final String DATE_TABLE_AE = RandomUtil.getIdentifier("JDBCEncryptedDate");
    public static final String NUMERIC_TABLE_AE = RandomUtil.getIdentifier("JDBCEncryptedNumeric");
    public static final String SCALE_DATE_TABLE_AE = RandomUtil.getIdentifier("JDBCEncryptedScaleDate");

    enum ColumnType {
        PLAIN,
        RANDOMIZED,
        DETERMINISTIC
    }

    /*
     * tables used in the tests {columnName, columnType}
     */
    static String binaryTable[][] = {{"Binary", "binary(20)"}, {"Varbinary", "varbinary(50)"},
            {"VarbinaryMax", "varbinary(max)"}, {"Binary512", "binary(512)"}, {"Binary8000", "varbinary(8000)"},};

    static String charTable[][] = {{"Char", "char(20) COLLATE Latin1_General_BIN2", "CHAR"},
            {"Varchar", "varchar(50) COLLATE Latin1_General_BIN2", "CHAR"},
            {"VarcharMax", "varchar(max) COLLATE Latin1_General_BIN2", "LONGVARCHAR"},
            {"Nchar", "nchar(30) COLLATE Latin1_General_BIN2", "NCHAR"},
            {"Nvarchar", "nvarchar(60) COLLATE Latin1_General_BIN2", "NCHAR"},
            {"NvarcharMax", "nvarchar(max) COLLATE Latin1_General_BIN2", "LONGNVARCHAR"},
            {"Uniqueidentifier", "uniqueidentifier", "GUID"},
            {"Varchar8000", "varchar(8000) COLLATE Latin1_General_BIN2", "CHAR"},
            {"Nvarchar4000", "nvarchar(4000) COLLATE Latin1_General_BIN2", "NCHAR"},};

    static String dateTable[][] = {{"Date", "date"}, {"Datetime2Default", "datetime2"},
            {"DatetimeoffsetDefault", "datetimeoffset"}, {"TimeDefault", "time"}, {"Datetime", "datetime"},
            {"Smalldatetime", "smalldatetime"},};

    static String dateScaleTable[][] = {{"Datetime2", "datetime2(2)"}, {"Time", "time(2)"},
            {"Datetimeoffset", "datetimeoffset(2)"},};

    static String numericTable[][] = {{"Bit", "bit"}, {"Tinyint", "tinyint"}, {"Smallint", "smallint"}, {"Int", "int"},
            {"Bigint", "bigint"}, {"FloatDefault", "float"}, {"Float", "float(30)"}, {"Real", "real"},
            {"DecimalDefault", "decimal"}, {"Decimal", "decimal(10,5)"}, {"NumericDefault", "numeric"},
            {"Numeric", "numeric(8,2)"}, {"SmallMoney", "smallmoney"}, {"Money", "money"},
            {"Decimal2", "decimal(28,4)"}, {"Numeric2", "numeric(28,4)"},};

    // CREATE TABLE tableName (columns) NULL"
    static String createSql = "CREATE TABLE %s (%s)";

    // ENCRYPTED WITH (ENCRYPTION_TYPE = encryptionType, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256',
    // COLUMN_ENCRYPTION_KEY = cekName
    static String encryptSql = " ENCRYPTED WITH (ENCRYPTION_TYPE = %s, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = %s";

    /**
     * Create connection, statement and generate path of resource file
     * 
     * @throws Exception
     * @throws TestAbortedException
     */
    @BeforeAll
    public static void setUpConnection() throws TestAbortedException, Exception {
        AETestConnectionString = connectionString + ";sendTimeAsDateTime=false";
        String tmpConnectionString = AETestConnectionString;
        String enclaveAttestationUrl = TestUtils.getConfiguredProperty("enclaveAttestationUrl");
        if (null != enclaveAttestationUrl) {
            tmpConnectionString = TestUtils.addOrOverrideProperty(tmpConnectionString, "enclaveAttestationUrl",
                    enclaveAttestationUrl);
        }
        String enclaveAttestationProtocol = TestUtils.getConfiguredProperty("enclaveAttestationProtocol");
        if (null != enclaveAttestationProtocol) {
            tmpConnectionString = TestUtils.addOrOverrideProperty(tmpConnectionString, "enclaveAttestationProtocol",
                    enclaveAttestationProtocol);
        }

        // add enclave properties if AEv2 supported
        try (Connection con = PrepUtil.getConnection(tmpConnectionString)) {
            if (TestUtils.isAEv2(con)) {
                isAEv2Supported = true;
                AETestConnectionString = tmpConnectionString;
            }
        } catch (SQLServerException e) {
            if (!e.getMessage().matches(TestUtils.formatErrorMsg("R_enclaveNotSupported"))) {
                // ignore AEv2 not supported errors
                fail(e.getMessage());
            }
        }

        cmkJks = Constants.CMK_NAME + "_JKS";
        cekJks = Constants.CEK_NAME + "_JKS";

        readFromFile(Constants.JAVA_KEY_STORE_FILENAME, "Alias name");

        javaKeyPath = TestUtils.getCurrentClassPath() + Constants.JKS_NAME;
        applicationClientID = TestUtils.getConfiguredProperty("applicationClientID");
        applicationKey = TestUtils.getConfiguredProperty("applicationKey");
        String keyID = TestUtils.getConfiguredProperty("keyID");
        if (null != keyID) {
            keyIDs = keyID.split(";");
        }

        Map<String, SQLServerColumnEncryptionKeyStoreProvider> map = new HashMap<String, SQLServerColumnEncryptionKeyStoreProvider>();
        if (null == jksProvider) {
            jksProvider = new SQLServerColumnEncryptionJavaKeyStoreProvider(javaKeyPath,
                    Constants.JKS_SECRET.toCharArray());
            map.put("My_KEYSTORE", jksProvider);
        }

        if (null == akvProvider && null != applicationClientID && null != applicationKey) {
            akvProvider = new SQLServerColumnEncryptionAzureKeyVaultProvider(applicationClientID, applicationKey);
            map.put(Constants.AZURE_KEY_VAULT_NAME, akvProvider);
        }

        if (!kspRegistered && (null != jksProvider || null != akvProvider)) {
            SQLServerConnection.registerColumnEncryptionKeyStoreProviders(map);
            kspRegistered = true;
        }

        dropAll();

        // always test JKS
        createCMK(cmkJks, Constants.JAVA_KEY_STORE_NAME, javaKeyAliases, Constants.CMK_SIGNATURE);
        createCEK(cmkJks, cekJks, jksProvider);

        if (null != akvProvider) {
            cmkAkv = Constants.CMK_NAME + "_AKV";
            cekAkv = Constants.CEK_NAME + "_AKV";

            createCMK(cmkAkv, Constants.AZURE_KEY_VAULT_NAME, keyIDs[0], Constants.CMK_SIGNATURE_AKV);
            createCEK(cmkAkv, cekAkv, akvProvider);
        }

        windowsKeyPath = TestUtils.getConfiguredProperty("windowsKeyPath");
        if (null != windowsKeyPath) {
            AETestConnectionString = TestUtils.addOrOverrideProperty(AETestConnectionString, "windowsKeyPath",
                    windowsKeyPath);
            cmkWin = Constants.CMK_NAME + "_WIN";
            cekWin = Constants.CEK_NAME + "_WIN";
            createCMK(cmkWin, Constants.WINDOWS_KEY_STORE_NAME, windowsKeyPath, Constants.CMK_SIGNATURE);
            createCEK(cmkWin, cekWin, null);
        }

        stmtColEncSetting = SQLServerStatementColumnEncryptionSetting.Enabled;

        AEInfo = new Properties();
        AEInfo.setProperty("ColumnEncryptionSetting", Constants.ENABLED);
        AEInfo.setProperty("keyStoreAuthentication", Constants.JAVA_KEY_STORE_SECRET);
        AEInfo.setProperty("keyStoreLocation", javaKeyPath);
        AEInfo.setProperty("keyStoreSecret", Constants.JKS_SECRET);
    }

    /**
     * Dropping all CMKs and CEKs and any open resources. Technically, dropAll depends on the state of the class so it
     * shouldn't be static, but the AfterAll annotation requires it to be static.
     * 
     * @throws SQLException
     */
    @AfterAll
    public static void dropAll() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            dropTables(stmt);

            dropCEK(cekJks, stmt);
            dropCMK(cmkJks, stmt);

            if (null != cekWin) {
                dropCEK(cekWin, stmt);
                dropCMK(cmkWin, stmt);
            }

            if (null != cekAkv) {
                dropCEK(cekAkv, stmt);
                dropCMK(cmkAkv, stmt);
            }
        }
    }

    /**
     * Read the alias from file which is created during creating jks If the jks and alias name in JavaKeyStore.txt does
     * not exists, will not run!
     * 
     * @param inputFile
     * @param lookupValue
     * @throws IOException
     */
    private static void readFromFile(String inputFile, String lookupValue) throws IOException {
        filePath = TestUtils.getCurrentClassPath();
        try {
            File f = new File(filePath + inputFile);
            try (BufferedReader buffer = new BufferedReader(new FileReader(f))) {
                String readLine = "";
                String[] linecontents;

                while ((readLine = buffer.readLine()) != null) {
                    if (readLine.trim().contains(lookupValue)) {
                        linecontents = readLine.split(" ");
                        javaKeyAliases = linecontents[2];
                        break;
                    }
                }
            }
        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    /**
     * Create AE test tables
     * 
     * @param tableName
     *        name of test table
     * @param table
     *        2d array containing table column definitions
     * @throws SQLException
     */
    protected static void createTable(String tableName, String cekName, String table[][]) throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String sql = "";
            for (int i = 0; i < table.length; i++) {
                sql += ColumnType.PLAIN.name() + table[i][0] + " " + table[i][1] + " NULL,";
                sql += ColumnType.DETERMINISTIC.name() + table[i][0] + " " + table[i][1]
                        + String.format(encryptSql, ColumnType.DETERMINISTIC.name(), cekName) + ") NULL,";
                sql += ColumnType.RANDOMIZED.name() + table[i][0] + " " + table[i][1]
                        + String.format(encryptSql, ColumnType.RANDOMIZED.name(), cekName) + ") NULL,";
            }
            sql = String.format(createSql, AbstractSQLGenerator.escapeIdentifier(tableName), sql);
            stmt.execute(sql);
            stmt.execute("DBCC FREEPROCCACHE");
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    protected static void createPrecisionTable(String tableName, String table[][], String cekName, int floatPrecision,
            int precision, int scale) throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String sql = "";
            for (int i = 0; i < table.length; i++) {
                if ("float" == table[i][1]) {
                    sql += ColumnType.PLAIN.name() + table[i][0] + "Precision " + table[i][1] + "(" + floatPrecision
                            + ") NULL,";
                    sql += ColumnType.RANDOMIZED.name() + table[i][0] + "Precision " + table[i][1] + "("
                            + floatPrecision + ") " + String.format(encryptSql, ColumnType.RANDOMIZED.name(), cekName)
                            + ") NULL,";
                    sql += ColumnType.DETERMINISTIC.name() + table[i][0] + "Precision " + table[i][1] + "("
                            + floatPrecision + ") "
                            + String.format(encryptSql, ColumnType.DETERMINISTIC.name(), cekName) + ") NULL,";
                } else {
                    sql += ColumnType.PLAIN.name() + table[i][0] + "Precision " + table[i][1] + "(" + precision + ","
                            + scale + ") NULL,";
                    sql += ColumnType.RANDOMIZED.name() + table[i][0] + "Precision " + table[i][1] + "(" + precision
                            + "," + scale + ") " + String.format(encryptSql, ColumnType.RANDOMIZED.name(), cekName)
                            + ") NULL,";
                    sql += ColumnType.DETERMINISTIC.name() + table[i][0] + "Precision " + table[i][1] + "(" + precision
                            + "," + scale + ") " + String.format(encryptSql, ColumnType.DETERMINISTIC.name(), cekName)
                            + ") NULL,";
                }
            }
            sql = String.format(createSql, AbstractSQLGenerator.escapeIdentifier(tableName), sql);
            stmt.execute(sql);
            stmt.execute("DBCC FREEPROCCACHE");
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    protected static void createScaleTable(String tableName, String table[][], String cekName,
            int scale) throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String sql = "";
            for (int i = 0; i < table.length; i++) {

                sql += ColumnType.PLAIN.name() + table[i][0] + "Scale " + table[i][1] + "(" + scale + ") NULL,";
                sql += ColumnType.RANDOMIZED.name() + table[i][0] + "Scale " + table[i][1] + "(" + scale + ") "
                        + String.format(encryptSql, ColumnType.RANDOMIZED.name(), cekName) + ") NULL,";
                sql += ColumnType.DETERMINISTIC.name() + table[i][0] + "Scale " + table[i][1] + "(" + scale + ") "
                        + String.format(encryptSql, ColumnType.DETERMINISTIC.name(), cekName) + ") NULL,";

                sql += ColumnType.PLAIN.name() + table[i][0] + " " + table[i][1] + " NULL,";
                sql += ColumnType.RANDOMIZED.name() + table[i][0] + " " + table[i][1]
                        + String.format(encryptSql, ColumnType.RANDOMIZED.name(), cekName) + ") NULL,";
                sql += ColumnType.DETERMINISTIC.name() + table[i][0] + " " + table[i][1]
                        + String.format(encryptSql, ColumnType.DETERMINISTIC.name(), cekName) + ") NULL,";
            }

            sql = String.format(createSql, AbstractSQLGenerator.escapeIdentifier(tableName), sql);
            stmt.execute(sql);
            stmt.execute("DBCC FREEPROCCACHE");
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    /**
     * Create a list of binary values
     * 
     * @param nullable
     */
    protected static LinkedList<byte[]> createbinaryValues(boolean nullable) {

        boolean encrypted = true;
        RandomData.returnNull = nullable;

        byte[] binary20 = RandomData.generateBinaryTypes("20", nullable, encrypted);
        byte[] varbinary50 = RandomData.generateBinaryTypes("50", nullable, encrypted);
        byte[] varbinarymax = RandomData.generateBinaryTypes("max", nullable, encrypted);
        byte[] binary512 = RandomData.generateBinaryTypes("512", nullable, encrypted);
        byte[] varbinary8000 = RandomData.generateBinaryTypes("8000", nullable, encrypted);

        LinkedList<byte[]> list = new LinkedList<>();
        list.add(binary20);
        list.add(varbinary50);
        list.add(varbinarymax);
        list.add(binary512);
        list.add(varbinary8000);

        return list;
    }

    /**
     * Create a list of char values
     * 
     * @param nullable
     */
    protected static String[] createCharValues(boolean nullable) {

        boolean encrypted = true;
        String char20 = RandomData.generateCharTypes("20", nullable, encrypted);
        String varchar50 = RandomData.generateCharTypes("50", nullable, encrypted);
        String varcharmax = RandomData.generateCharTypes("max", nullable, encrypted);
        String nchar30 = RandomData.generateNCharTypes("30", nullable, encrypted);
        String nvarchar60 = RandomData.generateNCharTypes("60", nullable, encrypted);
        String nvarcharmax = RandomData.generateNCharTypes("max", nullable, encrypted);
        String varchar8000 = RandomData.generateCharTypes("8000", nullable, encrypted);
        String nvarchar4000 = RandomData.generateNCharTypes("4000", nullable, encrypted);

        String[] values = {char20.trim(), varchar50, varcharmax, nchar30, nvarchar60, nvarcharmax, Constants.UID,
                varchar8000, nvarchar4000};

        return values;
    }

    /**
     * Create a list of numeric values
     * 
     * @param nullable
     */
    protected static String[] createNumericValues(boolean nullable) {

        Boolean boolValue = RandomData.generateBoolean(nullable);
        Short tinyIntValue = RandomData.generateTinyint(nullable);
        Short smallIntValue = RandomData.generateSmallint(nullable);
        Integer intValue = RandomData.generateInt(nullable);
        Long bigintValue = RandomData.generateLong(nullable);
        Double floatValue = RandomData.generateFloat(24, nullable);
        Double floatValuewithPrecision = RandomData.generateFloat(53, nullable);
        Float realValue = RandomData.generateReal(nullable);
        BigDecimal decimal = RandomData.generateDecimalNumeric(18, 0, nullable);
        BigDecimal decimalPrecisionScale = RandomData.generateDecimalNumeric(10, 5, nullable);
        BigDecimal numeric = RandomData.generateDecimalNumeric(18, 0, nullable);
        BigDecimal numericPrecisionScale = RandomData.generateDecimalNumeric(8, 2, nullable);
        BigDecimal smallMoney = RandomData.generateSmallMoney(nullable);
        BigDecimal money = RandomData.generateMoney(nullable);
        BigDecimal decimalPrecisionScale2 = RandomData.generateDecimalNumeric(28, 4, nullable);
        BigDecimal numericPrecisionScale2 = RandomData.generateDecimalNumeric(28, 4, nullable);

        String[] numericValues = {"" + boolValue, "" + tinyIntValue, "" + smallIntValue, "" + intValue,
                "" + bigintValue, "" + floatValue, "" + floatValuewithPrecision, "" + realValue, "" + decimal,
                "" + decimalPrecisionScale, "" + numeric, "" + numericPrecisionScale, "" + smallMoney, "" + money,
                "" + decimalPrecisionScale2, "" + numericPrecisionScale2};

        return numericValues;
    }

    /**
     * Create a list of temporal values
     * 
     * @param nullable
     */
    protected static LinkedList<Object> createTemporalTypes(boolean nullable) {

        Date date = RandomData.generateDate(nullable);
        Timestamp datetime2 = RandomData.generateDatetime2(7, nullable);
        DateTimeOffset datetimeoffset = RandomData.generateDatetimeoffset(7, nullable);
        Time time = RandomData.generateTime(7, nullable);
        Timestamp datetime = RandomData.generateDatetime(nullable);
        Timestamp smalldatetime = RandomData.generateSmalldatetime(nullable);

        LinkedList<Object> list = new LinkedList<>();
        list.add(date);
        list.add(datetime2);
        list.add(datetimeoffset);
        list.add(time);
        list.add(datetime);
        list.add(smalldatetime);

        return list;
    }

    /**
     * Create column master key
     * 
     * @param keyStoreName
     * @param keyPath
     * @throws SQLException
     */
    private static void createCMK(String cmkName, String keyStoreName, String keyPath,
            String signature) throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) PrepUtil
                .getConnection(AETestConnectionString + ";sendTimeAsDateTime=false", AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String sql = " if not exists (SELECT name from sys.column_master_keys where name='" + cmkName + "')"
                    + " begin" + " CREATE COLUMN MASTER KEY " + cmkName + " WITH (KEY_STORE_PROVIDER_NAME = '"
                    + keyStoreName + "', KEY_PATH = '" + keyPath + "'"
                    + (TestUtils.isAEv2(con) ? ",ENCLAVE_COMPUTATIONS (SIGNATURE = " + signature + ")) end" : ") end");
            stmt.execute(sql);
        }
    }

    /**
     * Create column encryption key
     * 
     * @param storeProvider
     * @param certStore
     * @throws SQLException
     */
    private static void createCEK(String cmkName, String cekName,
            SQLServerColumnEncryptionKeyStoreProvider storeProvider) throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) PrepUtil
                .getConnection(AETestConnectionString + ";sendTimeAsDateTime=false", AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            byte[] valuesDefault = Constants.CEK_STRING.getBytes();
            String encryptedValue;

            if (storeProvider instanceof SQLServerColumnEncryptionJavaKeyStoreProvider) {
                byte[] key = storeProvider.encryptColumnEncryptionKey(javaKeyAliases, Constants.CEK_ALGORITHM,
                        valuesDefault);
                encryptedValue = "0x" + TestUtils.bytesToHexString(key, key.length);
            } else if (storeProvider instanceof SQLServerColumnEncryptionAzureKeyVaultProvider) {
                byte[] key = storeProvider.encryptColumnEncryptionKey(keyIDs[0], Constants.CEK_ALGORITHM,
                        valuesDefault);
                encryptedValue = "0x" + TestUtils.bytesToHexString(key, key.length);
            } else {
                encryptedValue = Constants.CEK_ENCRYPTED_VALUE;
            }

            String cekSql = "CREATE COLUMN ENCRYPTION KEY " + cekName + " WITH VALUES " + "(COLUMN_MASTER_KEY = "
                    + cmkName + ", ALGORITHM = '" + Constants.CEK_ALGORITHM + "', ENCRYPTED_VALUE = " + encryptedValue
                    + ");";
            stmt.execute(cekSql);
        }
    }

    /**
     * Drop all tables that are in use by AE
     * 
     * @throws SQLException
     */
    protected static void dropTables(Statement stmt) throws SQLException {
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(NUMERIC_TABLE_AE), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(CHAR_TABLE_AE), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(BINARY_TABLE_AE), stmt);
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(DATE_TABLE_AE), stmt);
    }

    /**
     * Populate binary data.
     * 
     * @param byteValues
     * @throws SQLException
     */
    protected static void populateBinaryNormalCase(LinkedList<byte[]> byteValues) throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(BINARY_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil
                .getConnection(connectionString + ";sendTimeAsDateTime=false", AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // binary20
            for (int i = 1; i <= 3; i++) {
                if (null == byteValues) {
                    pstmt.setBytes(i, null);
                } else {
                    pstmt.setBytes(i, byteValues.get(0));
                }
            }

            // varbinary50
            for (int i = 4; i <= 6; i++) {
                if (null == byteValues) {
                    pstmt.setBytes(i, null);
                } else {
                    pstmt.setBytes(i, byteValues.get(1));
                }
            }

            // varbinary(max)
            for (int i = 7; i <= 9; i++) {
                if (null == byteValues) {
                    pstmt.setBytes(i, null);
                } else {
                    pstmt.setBytes(i, byteValues.get(2));
                }
            }

            // binary(512)
            for (int i = 10; i <= 12; i++) {
                if (null == byteValues) {
                    pstmt.setBytes(i, null);
                } else {
                    pstmt.setBytes(i, byteValues.get(3));
                }
            }

            // varbinary(8000)
            for (int i = 13; i <= 15; i++) {
                if (null == byteValues) {
                    pstmt.setBytes(i, null);
                } else {
                    pstmt.setBytes(i, byteValues.get(4));
                }
            }

            pstmt.execute();
        }
    }

    /**
     * Populate binary data using set object.
     * 
     * @param byteValues
     * @throws SQLException
     */
    protected static void populateBinarySetObject(LinkedList<byte[]> byteValues) throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(BINARY_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";
        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // binary(20)
            for (int i = 1; i <= 3; i++) {
                if (null == byteValues) {
                    pstmt.setObject(i, null, java.sql.Types.BINARY);
                } else {
                    pstmt.setObject(i, byteValues.get(0));
                }
            }

            // varbinary(50)
            for (int i = 4; i <= 6; i++) {
                if (null == byteValues) {
                    pstmt.setObject(i, null, java.sql.Types.BINARY);
                } else {
                    pstmt.setObject(i, byteValues.get(1));
                }
            }

            // varbinary(max)
            for (int i = 7; i <= 9; i++) {
                if (null == byteValues) {
                    pstmt.setObject(i, null, java.sql.Types.BINARY);
                } else {
                    pstmt.setObject(i, byteValues.get(2));
                }
            }

            // binary(512)
            for (int i = 10; i <= 12; i++) {
                if (null == byteValues) {
                    pstmt.setObject(i, null, java.sql.Types.BINARY);
                } else {
                    pstmt.setObject(i, byteValues.get(3));
                }
            }

            // varbinary(8000)
            for (int i = 13; i <= 15; i++) {
                if (null == byteValues) {
                    pstmt.setObject(i, null, java.sql.Types.BINARY);
                } else {
                    pstmt.setObject(i, byteValues.get(4));
                }
            }

            pstmt.execute();
        }
    }

    /**
     * Populate binary data using set object with JDBC type.
     * 
     * @param byteValues
     * @throws SQLException
     */
    protected static void populateBinarySetObjectWithJDBCType(LinkedList<byte[]> byteValues) throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(BINARY_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // binary(20)
            for (int i = 1; i <= 3; i++) {
                if (null == byteValues) {
                    pstmt.setObject(i, null, JDBCType.BINARY);
                } else {
                    pstmt.setObject(i, byteValues.get(0), JDBCType.BINARY);
                }
            }

            // varbinary(50)
            for (int i = 4; i <= 6; i++) {
                if (null == byteValues) {
                    pstmt.setObject(i, null, JDBCType.VARBINARY);
                } else {
                    pstmt.setObject(i, byteValues.get(1), JDBCType.VARBINARY);
                }
            }

            // varbinary(max)
            for (int i = 7; i <= 9; i++) {
                if (null == byteValues) {
                    pstmt.setObject(i, null, JDBCType.VARBINARY);
                } else {
                    pstmt.setObject(i, byteValues.get(2), JDBCType.VARBINARY);
                }
            }

            // binary(512)
            for (int i = 10; i <= 12; i++) {
                if (null == byteValues) {
                    pstmt.setObject(i, null, JDBCType.BINARY);
                } else {
                    pstmt.setObject(i, byteValues.get(3), JDBCType.BINARY);
                }
            }

            // varbinary(8000)
            for (int i = 13; i <= 15; i++) {
                if (null == byteValues) {
                    pstmt.setObject(i, null, JDBCType.VARBINARY);
                } else {
                    pstmt.setObject(i, byteValues.get(4), JDBCType.VARBINARY);
                }
            }

            pstmt.execute();
        }
    }

    /**
     * Populate binary data using set null.
     * 
     * @throws SQLException
     */
    protected static void populateBinaryNullCase() throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(BINARY_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // binary
            for (int i = 1; i <= 3; i++) {
                pstmt.setNull(i, java.sql.Types.BINARY);
            }

            // varbinary, varbinary(max)
            for (int i = 4; i <= 9; i++) {
                pstmt.setNull(i, java.sql.Types.VARBINARY);
            }

            // binary512
            for (int i = 10; i <= 12; i++) {
                pstmt.setNull(i, java.sql.Types.BINARY);
            }

            // varbinary(8000)
            for (int i = 13; i <= 15; i++) {
                pstmt.setNull(i, java.sql.Types.VARBINARY);
            }

            pstmt.execute();
        }
    }

    /**
     * Populate char data.
     * 
     * @param charValues
     * @throws SQLException
     */
    protected static void populateCharNormalCase(String[] charValues) throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(CHAR_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // char
            for (int i = 1; i <= 3; i++) {
                pstmt.setString(i, charValues[0]);
            }

            // varchar
            for (int i = 4; i <= 6; i++) {
                pstmt.setString(i, charValues[1]);
            }

            // varchar(max)
            for (int i = 7; i <= 9; i++) {
                pstmt.setString(i, charValues[2]);
            }

            // nchar
            for (int i = 10; i <= 12; i++) {
                pstmt.setNString(i, charValues[3]);
            }

            // nvarchar
            for (int i = 13; i <= 15; i++) {
                pstmt.setNString(i, charValues[4]);
            }

            // varchar(max)
            for (int i = 16; i <= 18; i++) {
                pstmt.setNString(i, charValues[5]);
            }

            // uniqueidentifier
            for (int i = 19; i <= 21; i++) {
                if (null == charValues[6]) {
                    pstmt.setUniqueIdentifier(i, null);
                } else {
                    pstmt.setUniqueIdentifier(i, Constants.UID);
                }
            }

            // varchar8000
            for (int i = 22; i <= 24; i++) {
                pstmt.setString(i, charValues[7]);
            }

            // nvarchar4000
            for (int i = 25; i <= 27; i++) {
                pstmt.setNString(i, charValues[8]);
            }

            pstmt.execute();
        }
    }

    /**
     * Populate char data using set object.
     * 
     * @param charValues
     * @throws SQLException
     */
    protected static void populateCharSetObject(String[] charValues) throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(CHAR_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // char
            for (int i = 1; i <= 3; i++) {
                pstmt.setObject(i, charValues[0]);
            }

            // varchar
            for (int i = 4; i <= 6; i++) {
                pstmt.setObject(i, charValues[1]);
            }

            // varchar(max)
            for (int i = 7; i <= 9; i++) {
                pstmt.setObject(i, charValues[2], java.sql.Types.LONGVARCHAR);
            }

            // nchar
            for (int i = 10; i <= 12; i++) {
                pstmt.setObject(i, charValues[3], java.sql.Types.NCHAR);
            }

            // nvarchar
            for (int i = 13; i <= 15; i++) {
                pstmt.setObject(i, charValues[4], java.sql.Types.NCHAR);
            }

            // nvarchar(max)
            for (int i = 16; i <= 18; i++) {
                pstmt.setObject(i, charValues[5], java.sql.Types.LONGNVARCHAR);
            }

            // uniqueidentifier
            for (int i = 19; i <= 21; i++) {
                pstmt.setObject(i, charValues[6], microsoft.sql.Types.GUID);
            }

            // varchar8000
            for (int i = 22; i <= 24; i++) {
                pstmt.setObject(i, charValues[7]);
            }

            // nvarchar4000
            for (int i = 25; i <= 27; i++) {
                pstmt.setObject(i, charValues[8], java.sql.Types.NCHAR);
            }

            pstmt.execute();
        }
    }

    /**
     * Populate char data using set object with JDBC types.
     * 
     * @param charValues
     * @throws SQLException
     */
    protected static void populateCharSetObjectWithJDBCTypes(String[] charValues) throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(CHAR_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // char
            for (int i = 1; i <= 3; i++) {
                pstmt.setObject(i, charValues[0], JDBCType.CHAR);
            }

            // varchar
            for (int i = 4; i <= 6; i++) {
                pstmt.setObject(i, charValues[1], JDBCType.VARCHAR);
            }

            // varchar(max)
            for (int i = 7; i <= 9; i++) {
                pstmt.setObject(i, charValues[2], JDBCType.LONGVARCHAR);
            }

            // nchar
            for (int i = 10; i <= 12; i++) {
                pstmt.setObject(i, charValues[3], JDBCType.NCHAR);
            }

            // nvarchar
            for (int i = 13; i <= 15; i++) {
                pstmt.setObject(i, charValues[4], JDBCType.NVARCHAR);
            }

            // nvarchar(max)
            for (int i = 16; i <= 18; i++) {
                pstmt.setObject(i, charValues[5], JDBCType.LONGNVARCHAR);
            }

            // uniqueidentifier
            for (int i = 19; i <= 21; i++) {
                pstmt.setObject(i, charValues[6], microsoft.sql.Types.GUID);
            }

            // varchar8000
            for (int i = 22; i <= 24; i++) {
                pstmt.setObject(i, charValues[7], JDBCType.VARCHAR);
            }

            // vnarchar4000
            for (int i = 25; i <= 27; i++) {
                pstmt.setObject(i, charValues[8], JDBCType.NVARCHAR);
            }

            pstmt.execute();
        }
    }

    /**
     * Populate char data with set null.
     * 
     * @throws SQLException
     */
    protected static void populateCharNullCase() throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(CHAR_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // char
            for (int i = 1; i <= 3; i++) {
                pstmt.setNull(i, java.sql.Types.CHAR);
            }

            // varchar, varchar(max)
            for (int i = 4; i <= 9; i++) {
                pstmt.setNull(i, java.sql.Types.VARCHAR);
            }

            // nchar
            for (int i = 10; i <= 12; i++) {
                pstmt.setNull(i, java.sql.Types.NCHAR);
            }

            // nvarchar, varchar(max)
            for (int i = 13; i <= 18; i++) {
                pstmt.setNull(i, java.sql.Types.NVARCHAR);
            }

            // uniqueidentifier
            for (int i = 19; i <= 21; i++) {
                pstmt.setNull(i, microsoft.sql.Types.GUID);

            }

            // varchar8000
            for (int i = 22; i <= 24; i++) {
                pstmt.setNull(i, java.sql.Types.VARCHAR);
            }

            // nvarchar4000
            for (int i = 25; i <= 27; i++) {
                pstmt.setNull(i, java.sql.Types.NVARCHAR);
            }

            pstmt.execute();
        }
    }

    /**
     * Populate date data.
     * 
     * @param dateValues
     * @throws SQLException
     */
    protected static void populateDateNormalCase(LinkedList<Object> dateValues) throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(DATE_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {
            // date
            for (int i = 1; i <= 3; i++) {
                pstmt.setDate(i, (Date) dateValues.get(0));
            }

            // datetime2 default
            for (int i = 4; i <= 6; i++) {
                pstmt.setTimestamp(i, (Timestamp) dateValues.get(1));
            }

            // datetimeoffset default
            for (int i = 7; i <= 9; i++) {
                pstmt.setDateTimeOffset(i, (DateTimeOffset) dateValues.get(2));
            }

            // time default
            for (int i = 10; i <= 12; i++) {
                pstmt.setTime(i, (Time) dateValues.get(3));
            }

            // datetime
            for (int i = 13; i <= 15; i++) {
                pstmt.setDateTime(i, (Timestamp) dateValues.get(4));
            }

            // smalldatetime
            for (int i = 16; i <= 18; i++) {
                pstmt.setSmallDateTime(i, (Timestamp) dateValues.get(5));
            }

            pstmt.execute();
        }
    }

    /**
     * Populate date data with scale.
     * 
     * @param dateValues
     * @throws SQLException
     */
    protected static void populateDateScaleNormalCase(LinkedList<Object> dateValues) throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(SCALE_DATE_TABLE_AE) + " values( "
                + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // datetime2(2)
            for (int i = 1; i <= 3; i++) {
                pstmt.setTimestamp(i, (Timestamp) dateValues.get(4), 2);
            }

            // time(2)
            for (int i = 4; i <= 6; i++) {
                pstmt.setTime(i, (Time) dateValues.get(5), 2);
            }

            // datetimeoffset(2)
            for (int i = 7; i <= 9; i++) {
                pstmt.setDateTimeOffset(i, (DateTimeOffset) dateValues.get(6), 2);
            }

            pstmt.execute();
        }
    }

    /**
     * Populate date data using set object.
     * 
     * @param dateValues
     * @param setter
     * @throws SQLException
     */
    protected static void populateDateSetObject(LinkedList<Object> dateValues, String setter) throws SQLException {

        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(DATE_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // date
            for (int i = 1; i <= 3; i++) {
                if (setter.equalsIgnoreCase("setwithJavaType"))
                    pstmt.setObject(i, (Date) dateValues.get(0), java.sql.Types.DATE);
                else if (setter.equalsIgnoreCase("setwithJDBCType"))
                    pstmt.setObject(i, (Date) dateValues.get(0), JDBCType.DATE);
                else
                    pstmt.setObject(i, (Date) dateValues.get(0));
            }

            // datetime2 default
            for (int i = 4; i <= 6; i++) {
                if (setter.equalsIgnoreCase("setwithJavaType"))
                    pstmt.setObject(i, (Timestamp) dateValues.get(1), java.sql.Types.TIMESTAMP);
                else if (setter.equalsIgnoreCase("setwithJDBCType"))
                    pstmt.setObject(i, (Timestamp) dateValues.get(1), JDBCType.TIMESTAMP);
                else
                    pstmt.setObject(i, (Timestamp) dateValues.get(1));
            }

            // datetimeoffset default
            for (int i = 7; i <= 9; i++) {
                if (setter.equalsIgnoreCase("setwithJavaType"))
                    pstmt.setObject(i, (DateTimeOffset) dateValues.get(2), microsoft.sql.Types.DATETIMEOFFSET);
                else if (setter.equalsIgnoreCase("setwithJDBCType"))
                    pstmt.setObject(i, (DateTimeOffset) dateValues.get(2), microsoft.sql.Types.DATETIMEOFFSET);
                else
                    pstmt.setObject(i, (DateTimeOffset) dateValues.get(2));
            }

            // time default
            for (int i = 10; i <= 12; i++) {
                if (setter.equalsIgnoreCase("setwithJavaType"))
                    pstmt.setObject(i, (Time) dateValues.get(3), java.sql.Types.TIME);
                else if (setter.equalsIgnoreCase("setwithJDBCType"))
                    pstmt.setObject(i, (Time) dateValues.get(3), JDBCType.TIME);
                else
                    pstmt.setObject(i, (Time) dateValues.get(3));
            }

            // datetime
            for (int i = 13; i <= 15; i++) {
                pstmt.setObject(i, (Timestamp) dateValues.get(4), microsoft.sql.Types.DATETIME);
            }

            // smalldatetime
            for (int i = 16; i <= 18; i++) {
                pstmt.setObject(i, (Timestamp) dateValues.get(5), microsoft.sql.Types.SMALLDATETIME);
            }

            pstmt.execute();
        }
    }

    /**
     * Populate date data with null data.
     * 
     * @throws SQLException
     */
    protected void populateDateSetObjectNull() throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(DATE_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // date
            for (int i = 1; i <= 3; i++) {
                pstmt.setObject(i, null, java.sql.Types.DATE);
            }

            // datetime2 default
            for (int i = 4; i <= 6; i++) {
                pstmt.setObject(i, null, java.sql.Types.TIMESTAMP);
            }

            // datetimeoffset default
            for (int i = 7; i <= 9; i++) {
                pstmt.setObject(i, null, microsoft.sql.Types.DATETIMEOFFSET);
            }

            // time default
            for (int i = 10; i <= 12; i++) {
                pstmt.setObject(i, null, java.sql.Types.TIME);
            }

            // datetime
            for (int i = 13; i <= 15; i++) {
                pstmt.setObject(i, null, microsoft.sql.Types.DATETIME);
            }

            // smalldatetime
            for (int i = 16; i <= 18; i++) {
                pstmt.setObject(i, null, microsoft.sql.Types.SMALLDATETIME);
            }

            pstmt.execute();
        }
    }

    /**
     * Populate date data with set null.
     * 
     * @throws SQLException
     */
    protected static void populateDateNullCase() throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(DATE_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // date
            for (int i = 1; i <= 3; i++) {
                pstmt.setNull(i, java.sql.Types.DATE);
            }

            // datetime2 default
            for (int i = 4; i <= 6; i++) {
                pstmt.setNull(i, java.sql.Types.TIMESTAMP);
            }

            // datetimeoffset default
            for (int i = 7; i <= 9; i++) {
                pstmt.setNull(i, microsoft.sql.Types.DATETIMEOFFSET);
            }

            // time default
            for (int i = 10; i <= 12; i++) {
                pstmt.setNull(i, java.sql.Types.TIME);
            }

            // datetime
            for (int i = 13; i <= 15; i++) {
                pstmt.setNull(i, microsoft.sql.Types.DATETIME);
            }

            // smalldatetime
            for (int i = 16; i <= 18; i++) {
                pstmt.setNull(i, microsoft.sql.Types.SMALLDATETIME);
            }

            pstmt.execute();
        }
    }

    /**
     * Populating the table
     * 
     * @param values
     * @throws SQLException
     */
    protected static void populateNumeric(String[] values) throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(NUMERIC_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // bit
            for (int i = 1; i <= 3; i++) {
                if (values[0].equalsIgnoreCase(Boolean.TRUE.toString())) {
                    pstmt.setBoolean(i, true);
                } else {
                    pstmt.setBoolean(i, false);
                }
            }

            // tinyint
            for (int i = 4; i <= 6; i++) {
                pstmt.setShort(i, Short.valueOf(values[1]));
            }

            // smallint
            for (int i = 7; i <= 9; i++) {
                pstmt.setShort(i, Short.valueOf(values[2]));
            }

            // int
            for (int i = 10; i <= 12; i++) {
                pstmt.setInt(i, Integer.valueOf(values[3]));
            }

            // bigint
            for (int i = 13; i <= 15; i++) {
                pstmt.setLong(i, Long.valueOf(values[4]));
            }

            // float default
            for (int i = 16; i <= 18; i++) {
                pstmt.setDouble(i, Double.valueOf(values[5]));
            }

            // float(30)
            for (int i = 19; i <= 21; i++) {
                pstmt.setDouble(i, Double.valueOf(values[6]));
            }

            // real
            for (int i = 22; i <= 24; i++) {
                pstmt.setFloat(i, Float.valueOf(values[7]));
            }

            // decimal default
            for (int i = 25; i <= 27; i++) {
                if (values[8].equalsIgnoreCase("0"))
                    pstmt.setBigDecimal(i, new BigDecimal(values[8]), 18, 0);
                else
                    pstmt.setBigDecimal(i, new BigDecimal(values[8]));
            }

            // decimal(10,5)
            for (int i = 28; i <= 30; i++) {
                pstmt.setBigDecimal(i, new BigDecimal(values[9]), 10, 5);
            }

            // numeric
            for (int i = 31; i <= 33; i++) {
                if (values[10].equalsIgnoreCase("0"))
                    pstmt.setBigDecimal(i, new BigDecimal(values[10]), 18, 0);
                else
                    pstmt.setBigDecimal(i, new BigDecimal(values[10]));
            }

            // numeric(8,2)
            for (int i = 34; i <= 36; i++) {
                pstmt.setBigDecimal(i, new BigDecimal(values[11]), 8, 2);
            }

            // small money
            for (int i = 37; i <= 39; i++) {
                pstmt.setSmallMoney(i, new BigDecimal(values[12]));
            }

            // money
            for (int i = 40; i <= 42; i++) {
                pstmt.setMoney(i, new BigDecimal(values[13]));
            }

            // decimal(28,4)
            for (int i = 43; i <= 45; i++) {
                pstmt.setBigDecimal(i, new BigDecimal(values[14]), 28, 4);
            }

            // numeric(28,4)
            for (int i = 46; i <= 48; i++) {
                pstmt.setBigDecimal(i, new BigDecimal(values[15]), 28, 4);
            }

            pstmt.execute();
        }
    }

    /**
     * Populate numeric data with set object.
     * 
     * @param values
     * @throws SQLException
     */
    protected static void populateNumericSetObject(String[] values) throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(NUMERIC_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // bit
            for (int i = 1; i <= 3; i++) {
                if (values[0].equalsIgnoreCase(Boolean.TRUE.toString())) {
                    pstmt.setObject(i, true);
                } else {
                    pstmt.setObject(i, false);
                }
            }

            // tinyint
            for (int i = 4; i <= 6; i++) {
                pstmt.setObject(i, Short.valueOf(values[1]));
            }

            // smallint
            for (int i = 7; i <= 9; i++) {
                pstmt.setObject(i, Short.valueOf(values[2]));
            }

            // int
            for (int i = 10; i <= 12; i++) {
                pstmt.setObject(i, Integer.valueOf(values[3]));
            }

            // bigint
            for (int i = 13; i <= 15; i++) {
                pstmt.setObject(i, Long.valueOf(values[4]));
            }

            // float default
            for (int i = 16; i <= 18; i++) {
                pstmt.setObject(i, Double.valueOf(values[5]));
            }

            // float(30)
            for (int i = 19; i <= 21; i++) {
                pstmt.setObject(i, Double.valueOf(values[6]));
            }

            // real
            for (int i = 22; i <= 24; i++) {
                pstmt.setObject(i, Float.valueOf(values[7]));
            }

            // decimal default
            for (int i = 25; i <= 27; i++) {
                if (RandomData.returnZero)
                    pstmt.setObject(i, new BigDecimal(values[8]), java.sql.Types.DECIMAL, 18, 0);
                else
                    pstmt.setObject(i, new BigDecimal(values[8]));
            }

            // decimal(10,5)
            for (int i = 28; i <= 30; i++) {
                pstmt.setObject(i, new BigDecimal(values[9]), java.sql.Types.DECIMAL, 10, 5);
            }

            // numeric
            for (int i = 31; i <= 33; i++) {
                if (RandomData.returnZero)
                    pstmt.setObject(i, new BigDecimal(values[10]), java.sql.Types.NUMERIC, 18, 0);
                else
                    pstmt.setObject(i, new BigDecimal(values[10]));
            }

            // numeric(8,2)
            for (int i = 34; i <= 36; i++) {
                pstmt.setObject(i, new BigDecimal(values[11]), java.sql.Types.NUMERIC, 8, 2);
            }

            // small money
            for (int i = 37; i <= 39; i++) {
                pstmt.setObject(i, new BigDecimal(values[12]), microsoft.sql.Types.SMALLMONEY);
            }

            // money
            for (int i = 40; i <= 42; i++) {
                pstmt.setObject(i, new BigDecimal(values[13]), microsoft.sql.Types.MONEY);
            }

            // decimal(28,4)
            for (int i = 43; i <= 45; i++) {
                pstmt.setObject(i, new BigDecimal(values[14]), java.sql.Types.DECIMAL, 28, 4);
            }

            // numeric
            for (int i = 46; i <= 48; i++) {
                pstmt.setObject(i, new BigDecimal(values[15]), java.sql.Types.NUMERIC, 28, 4);
            }

            pstmt.execute();
        }
    }

    /**
     * Populate numeric data with set object with JDBC type.
     * 
     * @param values
     * @throws SQLException
     */
    protected static void populateNumericSetObjectWithJDBCTypes(String[] values) throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(NUMERIC_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // bit
            for (int i = 1; i <= 3; i++) {
                if (values[0].equalsIgnoreCase(Boolean.TRUE.toString())) {
                    pstmt.setObject(i, true);
                } else {
                    pstmt.setObject(i, false);
                }
            }

            // tinyint
            for (int i = 4; i <= 6; i++) {
                pstmt.setObject(i, Short.valueOf(values[1]), JDBCType.TINYINT);
            }

            // smallint
            for (int i = 7; i <= 9; i++) {
                pstmt.setObject(i, Short.valueOf(values[2]), JDBCType.SMALLINT);
            }

            // int
            for (int i = 10; i <= 12; i++) {
                pstmt.setObject(i, Integer.valueOf(values[3]), JDBCType.INTEGER);
            }

            // bigint
            for (int i = 13; i <= 15; i++) {
                pstmt.setObject(i, Long.valueOf(values[4]), JDBCType.BIGINT);
            }

            // float default
            for (int i = 16; i <= 18; i++) {
                pstmt.setObject(i, Double.valueOf(values[5]), JDBCType.DOUBLE);
            }

            // float(30)
            for (int i = 19; i <= 21; i++) {
                pstmt.setObject(i, Double.valueOf(values[6]), JDBCType.DOUBLE);
            }

            // real
            for (int i = 22; i <= 24; i++) {
                pstmt.setObject(i, Float.valueOf(values[7]), JDBCType.REAL);
            }

            // decimal default
            for (int i = 25; i <= 27; i++) {
                if (RandomData.returnZero)
                    pstmt.setObject(i, new BigDecimal(values[8]), java.sql.Types.DECIMAL, 18, 0);
                else
                    pstmt.setObject(i, new BigDecimal(values[8]));
            }

            // decimal(10,5)
            for (int i = 28; i <= 30; i++) {
                pstmt.setObject(i, new BigDecimal(values[9]), java.sql.Types.DECIMAL, 10, 5);
            }

            // numeric
            for (int i = 31; i <= 33; i++) {
                if (RandomData.returnZero)
                    pstmt.setObject(i, new BigDecimal(values[10]), java.sql.Types.NUMERIC, 18, 0);
                else
                    pstmt.setObject(i, new BigDecimal(values[10]));
            }

            // numeric(8,2)
            for (int i = 34; i <= 36; i++) {
                pstmt.setObject(i, new BigDecimal(values[11]), java.sql.Types.NUMERIC, 8, 2);
            }

            // small money
            for (int i = 37; i <= 39; i++) {
                pstmt.setObject(i, new BigDecimal(values[12]), microsoft.sql.Types.SMALLMONEY);
            }

            // money
            for (int i = 40; i <= 42; i++) {
                pstmt.setObject(i, new BigDecimal(values[13]), microsoft.sql.Types.MONEY);
            }

            // decimal(28,4)
            for (int i = 43; i <= 45; i++) {
                pstmt.setObject(i, new BigDecimal(values[14]), java.sql.Types.DECIMAL, 28, 4);
            }

            // numeric
            for (int i = 46; i <= 48; i++) {
                pstmt.setObject(i, new BigDecimal(values[15]), java.sql.Types.NUMERIC, 28, 4);
            }

            pstmt.execute();
        }
    }

    /**
     * Populate numeric data with set object using null data.
     * 
     * @throws SQLException
     */
    protected static void populateNumericSetObjectNull() throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(NUMERIC_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // bit
            for (int i = 1; i <= 3; i++) {
                pstmt.setObject(i, null, java.sql.Types.BIT);
            }

            // tinyint
            for (int i = 4; i <= 6; i++) {
                pstmt.setObject(i, null, java.sql.Types.TINYINT);
            }

            // smallint
            for (int i = 7; i <= 9; i++) {
                pstmt.setObject(i, null, java.sql.Types.SMALLINT);
            }

            // int
            for (int i = 10; i <= 12; i++) {
                pstmt.setObject(i, null, java.sql.Types.INTEGER);
            }

            // bigint
            for (int i = 13; i <= 15; i++) {
                pstmt.setObject(i, null, java.sql.Types.BIGINT);
            }

            // float default
            for (int i = 16; i <= 18; i++) {
                pstmt.setObject(i, null, java.sql.Types.DOUBLE);
            }

            // float(30)
            for (int i = 19; i <= 21; i++) {
                pstmt.setObject(i, null, java.sql.Types.DOUBLE);
            }

            // real
            for (int i = 22; i <= 24; i++) {
                pstmt.setObject(i, null, java.sql.Types.REAL);
            }

            // decimal default
            for (int i = 25; i <= 27; i++) {
                pstmt.setObject(i, null, java.sql.Types.DECIMAL);
            }

            // decimal(10,5)
            for (int i = 28; i <= 30; i++) {
                pstmt.setObject(i, null, java.sql.Types.DECIMAL, 10, 5);
            }

            // numeric
            for (int i = 31; i <= 33; i++) {
                pstmt.setObject(i, null, java.sql.Types.NUMERIC);
            }

            // numeric(8,2)
            for (int i = 34; i <= 36; i++) {
                pstmt.setObject(i, null, java.sql.Types.NUMERIC, 8, 2);
            }

            // small money
            for (int i = 37; i <= 39; i++) {
                pstmt.setObject(i, null, microsoft.sql.Types.SMALLMONEY);
            }

            // money
            for (int i = 40; i <= 42; i++) {
                pstmt.setObject(i, null, microsoft.sql.Types.MONEY);
            }

            // decimal(28,4)
            for (int i = 43; i <= 45; i++) {
                pstmt.setObject(i, null, java.sql.Types.DECIMAL, 28, 4);
            }

            // numeric
            for (int i = 46; i <= 48; i++) {
                pstmt.setObject(i, null, java.sql.Types.NUMERIC, 28, 4);
            }

            pstmt.execute();
        }
    }

    /**
     * Populate numeric data with set null.
     * 
     * @param values
     * @throws SQLException
     */
    protected static void populateNumericNullCase(String[] values) throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(NUMERIC_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?"

                + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // bit
            for (int i = 1; i <= 3; i++) {
                pstmt.setNull(i, java.sql.Types.BIT);
            }

            // tinyint
            for (int i = 4; i <= 6; i++) {
                pstmt.setNull(i, java.sql.Types.TINYINT);
            }

            // smallint
            for (int i = 7; i <= 9; i++) {
                pstmt.setNull(i, java.sql.Types.SMALLINT);
            }

            // int
            for (int i = 10; i <= 12; i++) {
                pstmt.setNull(i, java.sql.Types.INTEGER);
            }

            // bigint
            for (int i = 13; i <= 15; i++) {
                pstmt.setNull(i, java.sql.Types.BIGINT);
            }

            // float default
            for (int i = 16; i <= 18; i++) {
                pstmt.setNull(i, java.sql.Types.DOUBLE);
            }

            // float(30)
            for (int i = 19; i <= 21; i++) {
                pstmt.setNull(i, java.sql.Types.DOUBLE);
            }

            // real
            for (int i = 22; i <= 24; i++) {
                pstmt.setNull(i, java.sql.Types.REAL);
            }

            // decimal default
            for (int i = 25; i <= 27; i++) {
                pstmt.setBigDecimal(i, null);
            }

            // decimal(10,5)
            for (int i = 28; i <= 30; i++) {
                pstmt.setBigDecimal(i, null, 10, 5);
            }

            // numeric
            for (int i = 31; i <= 33; i++) {
                pstmt.setBigDecimal(i, null);
            }

            // numeric(8,2)
            for (int i = 34; i <= 36; i++) {
                pstmt.setBigDecimal(i, null, 8, 2);
            }

            // small money
            for (int i = 37; i <= 39; i++) {
                pstmt.setSmallMoney(i, null);
            }

            // money
            for (int i = 40; i <= 42; i++) {
                pstmt.setMoney(i, null);
            }

            // decimal(28,4)
            for (int i = 43; i <= 45; i++) {
                pstmt.setBigDecimal(i, null, 28, 4);
            }

            // decimal(28,4)
            for (int i = 46; i <= 48; i++) {
                pstmt.setBigDecimal(i, null, 28, 4);
            }
            pstmt.execute();
        }
    }

    /**
     * Populate numeric data.
     * 
     * @param numericValues
     * @throws SQLException
     */
    protected static void populateNumericNormalCase(String[] numericValues) throws SQLException {
        String sql = "insert into " + AbstractSQLGenerator.escapeIdentifier(NUMERIC_TABLE_AE) + " values( " + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
                + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?"

                + ")";

        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) TestUtils.getPreparedStmt(con, sql,
                        stmtColEncSetting)) {

            // bit
            for (int i = 1; i <= 3; i++) {
                if (numericValues[0].equalsIgnoreCase(Boolean.TRUE.toString())) {
                    pstmt.setBoolean(i, true);
                } else {
                    pstmt.setBoolean(i, false);
                }
            }

            // tinyint
            for (int i = 4; i <= 6; i++) {
                if (1 == Integer.valueOf(numericValues[1])) {
                    pstmt.setBoolean(i, true);
                } else {
                    pstmt.setBoolean(i, false);
                }
            }

            // smallint
            for (int i = 7; i <= 9; i++) {
                if (numericValues[2].equalsIgnoreCase("255")) {
                    pstmt.setByte(i, (byte) 255);
                } else {
                    pstmt.setByte(i, Byte.valueOf(numericValues[2]));
                }
            }

            // int
            for (int i = 10; i <= 12; i++) {
                pstmt.setShort(i, Short.valueOf(numericValues[3]));
            }

            // bigint
            for (int i = 13; i <= 15; i++) {
                pstmt.setInt(i, Integer.valueOf(numericValues[4]));
            }

            // float default
            for (int i = 16; i <= 18; i++) {
                pstmt.setDouble(i, Double.valueOf(numericValues[5]));
            }

            // float(30)
            for (int i = 19; i <= 21; i++) {
                pstmt.setDouble(i, Double.valueOf(numericValues[6]));
            }

            // real
            for (int i = 22; i <= 24; i++) {
                pstmt.setFloat(i, Float.valueOf(numericValues[7]));
            }

            // decimal default
            for (int i = 25; i <= 27; i++) {
                pstmt.setBigDecimal(i, new BigDecimal(numericValues[8]));
            }

            // decimal(10,5)
            for (int i = 28; i <= 30; i++) {
                pstmt.setBigDecimal(i, new BigDecimal(numericValues[9]), 10, 5);
            }

            // numeric
            for (int i = 31; i <= 33; i++) {
                pstmt.setBigDecimal(i, new BigDecimal(numericValues[10]));
            }

            // numeric(8,2)
            for (int i = 34; i <= 36; i++) {
                pstmt.setBigDecimal(i, new BigDecimal(numericValues[11]), 8, 2);
            }

            // small money
            for (int i = 37; i <= 39; i++) {
                pstmt.setSmallMoney(i, new BigDecimal(numericValues[12]));
            }

            // money
            for (int i = 40; i <= 42; i++) {
                pstmt.setSmallMoney(i, new BigDecimal(numericValues[13]));
            }

            // decimal(28,4)
            for (int i = 43; i <= 45; i++) {
                pstmt.setBigDecimal(i, new BigDecimal(numericValues[14]), 28, 4);
            }

            // numeric
            for (int i = 46; i <= 48; i++) {
                pstmt.setBigDecimal(i, new BigDecimal(numericValues[15]), 28, 4);
            }

            pstmt.execute();
        }
    }

    /**
     * Dropping column encryption key
     * 
     * @throws SQLException
     */
    private static void dropCEK(String cekName, Statement stmt) throws SQLException {
        String cekSql = " if exists (SELECT name from sys.column_encryption_keys where name='" + cekName + "')"
                + " begin" + " drop column encryption key " + cekName + " end";
        stmt.execute(cekSql);
    }

    /**
     * Dropping column master key
     * 
     * @throws SQLException
     */
    private static void dropCMK(String cmkName, Statement stmt) throws SQLException {
        String cekSql = " if exists (SELECT name from sys.column_master_keys where name='" + cmkName + "')" + " begin"
                + " drop column master key " + cmkName + " end";
        stmt.execute(cekSql);
    }
}
