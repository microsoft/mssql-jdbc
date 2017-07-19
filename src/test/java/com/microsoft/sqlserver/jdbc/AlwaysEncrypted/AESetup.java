/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.xml.bind.DatatypeConverter;

import org.junit.jupiter.api.BeforeAll;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionJavaKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatementColumnEncryptionSetting;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.Utils;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

/**
 * Setup for Always Encrypted test
 * This test will work on Appveyor and Travis-ci as java key store gets created from the .yml scripts. Users on their local machine should create the 
 * keystore manually and save the alias name in JavaKeyStore.txt file. For local test purposes, put this in the target/test-classes directory
 *
 */
@RunWith(JUnitPlatform.class)
public class AESetup extends AbstractTest {

    static final String javaKeyStoreInputFile = "JavaKeyStore.txt";
    static final String keyStoreName = "MSSQL_JAVA_KEYSTORE";
    static final String jksName = "clientcert.jks";
    static final String cmkName = "JDBC_CMK";
    static final String cekName = "JDBC_CEK";
    static final String secretstrJks = "password";
	static final String numericTable = "JDBCEncrpytedNumericTable";
	static final String charTable = "JDBCEncrpytedCharTable";
	static final String binaryTable = "JDBCEncrpytedBinaryTable";
	static final String dateTable = "JDBCEncrpytedDateTable";
	static final String uid = "171fbe25-4331-4765-a838-b2e3eea3e7ea";
	static final String uid2 = "171fbe25-4331-4765-a838-b2e3eea3e7eb";
	
    static String filePath = null;
    static String thumbprint = null;
    static SQLServerConnection con = null;
    static SQLServerStatement stmt = null;
    static String keyPath = null;
    static String javaKeyAliases = null;
    static String OS = System.getProperty("os.name").toLowerCase();
    static SQLServerColumnEncryptionKeyStoreProvider storeProvider = null;
	static SQLServerStatementColumnEncryptionSetting stmtColEncSetting = null;
	
    /**
     * Create connection, statement and generate path of resource file
     * @throws Exception 
     * @throws TestAbortedException 
     */
    @BeforeAll
    static void setUpConnection() throws TestAbortedException, Exception {
        assumeTrue(13 <= new DBConnection(connectionString).getServerVersion(),
                "Aborting test case as SQL Server version is not compatible with Always encrypted ");

        readFromFile(javaKeyStoreInputFile, "Alias name");
        con = (SQLServerConnection) DriverManager.getConnection(connectionString);
        stmt = (SQLServerStatement) con.createStatement();
        Utils.dropTableIfExists(numericTable, stmt);
        dropCEK();
        dropCMK();
        con.close();
       
        keyPath = Utils.getCurrentClassPath() + jksName;
        storeProvider = new SQLServerColumnEncryptionJavaKeyStoreProvider(keyPath, secretstrJks.toCharArray());
        stmtColEncSetting = SQLServerStatementColumnEncryptionSetting.Enabled;
        Properties info = new Properties();
        info.setProperty("ColumnEncryptionSetting", "Enabled");
        info.setProperty("keyStoreAuthentication", "JavaKeyStorePassword");
        info.setProperty("keyStoreLocation", keyPath);
        info.setProperty("keyStoreSecret", secretstrJks);
        con = (SQLServerConnection) DriverManager.getConnection(connectionString, info);
        stmt = (SQLServerStatement) con.createStatement();
        createCMK(keyStoreName, javaKeyAliases);
        createCEK(storeProvider);
    }

    /**
     * Read the alias from file which is created during creating jks
     * If the jks and alias name in JavaKeyStore.txt does not exists, will not run!
     * @param inputFile
     * @param lookupValue
     * @throws IOException
     */
    private static void readFromFile(String inputFile,
            String lookupValue) throws IOException {
        BufferedReader buffer = null;
        filePath = Utils.getCurrentClassPath();
        try {
            File f = new File(filePath + inputFile);
            assumeTrue(f.exists(), "Aborting test case since no java key store and alias name exists!");
            buffer = new BufferedReader(new FileReader(f));
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
        catch (IOException e) {
            fail(e.toString());
        }
        finally{
            if (null != buffer){
                buffer.close();
            }
        }

    }
    
    /**
     * Create encrypted table for Binary
     * @throws SQLException
     */
    static void createBinaryTable() throws SQLException {
		String sql = "create table " + binaryTable + " (" + "PlainBinary binary(20) null,"
				+ "RandomizedBinary binary(20) ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicBinary binary(20) ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainVarbinary varbinary(50) null,"
				+ "RandomizedVarbinary varbinary(50) ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicVarbinary varbinary(50) ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainVarbinaryMax varbinary(max) null,"
				+ "RandomizedVarbinaryMax varbinary(max) ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicVarbinaryMax varbinary(max) ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainBinary512 binary(512) null,"
				+ "RandomizedBinary512 binary(512) ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicBinary512 binary(512) ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainBinary8000 varbinary(8000) null,"
				+ "RandomizedBinary8000 varbinary(8000) ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicBinary8000 varbinary(8000) ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ ");";

		stmt.execute(sql);
	}

    /**
     * Create encrypted table for Char
     * @throws SQLException
     */
	static void createCharTable() throws SQLException {
		String sql = "create table " + charTable + " (" + "PlainChar char(20) null,"
				+ "RandomizedChar char(20) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicChar char(20) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainVarchar varchar(50) null,"
				+ "RandomizedVarchar varchar(50) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicVarchar varchar(50) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainVarcharMax varchar(max) null,"
				+ "RandomizedVarcharMax varchar(max) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicVarcharMax varchar(max) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainNchar nchar(30) null,"
				+ "RandomizedNchar nchar(30) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicNchar nchar(30) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainNvarchar nvarchar(60) null,"
				+ "RandomizedNvarchar nvarchar(60) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicNvarchar nvarchar(60) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainNvarcharMax nvarchar(max) null,"
				+ "RandomizedNvarcharMax nvarchar(max) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicNvarcharMax nvarchar(max) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainUniqueidentifier uniqueidentifier null,"
				+ "RandomizedUniqueidentifier uniqueidentifier ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicUniqueidentifier uniqueidentifier ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainVarchar8000 varchar(8000) null,"
				+ "RandomizedVarchar8000 varchar(8000) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicVarchar8000 varchar(8000) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainNvarchar4000 nvarchar(4000) null,"
				+ "RandomizedNvarchar4000 nvarchar(4000) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicNvarchar4000 nvarchar(4000) COLLATE Latin1_General_BIN2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ ");";

		stmt.execute(sql);
	}
	
    /**
     * Create encrypted table for Date
     * @throws SQLException
     */
	static void createDateTable() throws SQLException {
		String sql = "create table " + dateTable + " (" + "PlainDate date null,"
				+ "RandomizedDate date ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicDate date ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainDatetime2Default datetime2 null,"
				+ "RandomizedDatetime2Default datetime2 ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicDatetime2Default datetime2 ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainDatetimeoffsetDefault datetimeoffset null,"
				+ "RandomizedDatetimeoffsetDefault datetimeoffset ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicDatetimeoffsetDefault datetimeoffset ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainTimeDefault time null,"
				+ "RandomizedTimeDefault time ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicTimeDefault time ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainDatetime datetime null,"
				+ "RandomizedDatetime datetime ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicDatetime datetime ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainSmalldatetime smalldatetime null,"
				+ "RandomizedSmalldatetime smalldatetime ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicSmalldatetime smalldatetime ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ ");";

		stmt.execute(sql);
	}

    /**
     * Create encrypted table for Numeric
     * @throws SQLException
     */
	static void createNumericTable() throws SQLException {
		String sql = "create table " + numericTable + " (" + "PlainBit bit null,"
				+ "RandomizedBit bit ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicBit bit ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainTinyint tinyint null,"
				+ "RandomizedTinyint tinyint ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicTinyint tinyint ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainSmallint smallint null,"
				+ "RandomizedSmallint smallint ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicSmallint smallint ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainInt int null,"
				+ "RandomizedInt int ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicInt int ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainBigint bigint null,"
				+ "RandomizedBigint bigint ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicBigint bigint ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainFloatDefault float null,"
				+ "RandomizedFloatDefault float ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicFloatDefault float ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainFloat float(30) null,"
				+ "RandomizedFloat float(30) ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicFloat float(30) ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainReal real null,"
				+ "RandomizedReal real ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicReal real ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainDecimalDefault decimal null,"
				+ "RandomizedDecimalDefault decimal ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicDecimalDefault decimal ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainDecimal decimal(10,5) null,"
				+ "RandomizedDecimal decimal(10,5) ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicDecimal decimal(10,5) ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainNumericDefault numeric null,"
				+ "RandomizedNumericDefault numeric ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicNumericDefault numeric ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainNumeric numeric(8,2) null,"
				+ "RandomizedNumeric numeric(8,2) ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicNumeric numeric(8,2) ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainSmallMoney smallmoney null,"
				+ "RandomizedSmallMoney smallmoney ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicSmallMoney smallmoney ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainMoney money null,"
				+ "RandomizedMoney money ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicMoney money ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainDecimal2 decimal(28,4) null,"
				+ "RandomizedDecimal2 decimal(28,4) ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicDecimal2 decimal(28,4) ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ "PlainNumeric2 numeric(28,4) null,"
				+ "RandomizedNumeric2 numeric(28,4) ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"
				+ "DeterministicNumeric2 numeric(28,4) ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
				+ cekName + ") NULL,"

				+ ");";

		try {
			stmt.execute(sql);
			stmt.execute("DBCC FREEPROCCACHE");
		} catch (SQLException e) {
			fail(e.toString());
		}
	}

    /**
     * Create column master key
     * @param keyStoreName
     * @param keyPath
     * @throws SQLException
     */
    private static void createCMK(String keyStoreName,
            String keyPath) throws SQLException {
        String sql = " if not exists (SELECT name from sys.column_master_keys where name='" + cmkName + "')" + " begin" + " CREATE COLUMN MASTER KEY "
                + cmkName + " WITH (KEY_STORE_PROVIDER_NAME = '" + keyStoreName + "', KEY_PATH = '" + keyPath + "')" + " end";
        stmt.execute(sql);
    }

    /**
     * Create column encryption key
     * @param storeProvider
     * @param certStore
     * @throws SQLException
     */
    static void createCEK(SQLServerColumnEncryptionKeyStoreProvider storeProvider) throws SQLException {
        String letters = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        byte[] valuesDefault = letters.getBytes();
        String cekSql = null;
        byte[] key = storeProvider.encryptColumnEncryptionKey(javaKeyAliases, "RSA_OAEP", valuesDefault);
        cekSql = "CREATE COLUMN ENCRYPTION KEY " + cekName + " WITH VALUES " + "(COLUMN_MASTER_KEY = " + cmkName
                    + ", ALGORITHM = 'RSA_OAEP', ENCRYPTED_VALUE = 0x" + DatatypeConverter.printHexBinary(key) + ")" + ";";        
        stmt.execute(cekSql);
    }
    
    /**
     * Drop all tables that are in use by AE
     * @throws SQLException
     */
	static void dropTables() throws SQLException {
		stmt.executeUpdate("if object_id('" + numericTable + "','U') is not null" + " drop table " + numericTable);
		stmt.executeUpdate("if object_id('" + charTable + "','U') is not null" + " drop table " + charTable);
		stmt.executeUpdate("if object_id('" + binaryTable + "','U') is not null" + " drop table " + binaryTable);
		stmt.executeUpdate("if object_id('" + dateTable + "','U') is not null" + " drop table " + dateTable);
	}
    
    /**
     * Dropping column encryption key
     * @throws SQLServerException
     * @throws SQLException
     */
    static void dropCEK() throws SQLServerException, SQLException {
        String cekSql = " if exists (SELECT name from sys.column_encryption_keys where name='" + cekName + "')" + " begin"
                + " drop column encryption key " + cekName + " end";
        stmt.execute(cekSql);
    }

    /**
     * Dropping column master key
     * @throws SQLServerException
     * @throws SQLException
     */
    static void dropCMK() throws SQLServerException, SQLException {
        String cekSql = " if exists (SELECT name from sys.column_master_keys where name='" + cmkName + "')" + " begin" + " drop column master key "
                + cmkName + " end";
        stmt.execute(cekSql);
    }
}
