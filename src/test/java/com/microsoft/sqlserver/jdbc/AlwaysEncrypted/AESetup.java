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
import java.sql.Statement;
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

    static String javaKeyStoreInputFile = "JavaKeyStore.txt";
    static String keyStoreName = "MSSQL_JAVA_KEYSTORE";
    static String jksName = "clientcert.jks";
    static String filePath = null;
    static String thumbprint = null;
    static SQLServerConnection con = null;
    static Statement stmt = null;
    static String cmkName = "JDBC_CMK";
    static String cekName = "JDBC_CEK";
    static String keyPath = null;
    static String javaKeyAliases = null;
    static String OS = System.getProperty("os.name").toLowerCase();
    static SQLServerColumnEncryptionKeyStoreProvider storeProvider = null;
    static String secretstrJks = "password";
    static String numericTable = "numericTable";

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
        stmt = con.createStatement();
        Utils.dropTableIfExists(numericTable, stmt);
        dropCEK();
        dropCMK();
        con.close();
       

        keyPath = Utils.getCurrentClassPath() + jksName;
        storeProvider = new SQLServerColumnEncryptionJavaKeyStoreProvider(keyPath, secretstrJks.toCharArray());
        Properties info = new Properties();
        info.setProperty("ColumnEncryptionSetting", "Enabled");
        info.setProperty("keyStoreAuthentication", "JavaKeyStorePassword");
        info.setProperty("keyStoreLocation", keyPath);
        info.setProperty("keyStoreSecret", secretstrJks);
        con = (SQLServerConnection) DriverManager.getConnection(connectionString, info);
        stmt = con.createStatement();
        createCMK(keyStoreName, javaKeyAliases);
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
            fail(e.toString());;
        }
        finally{
            if (null != buffer){
                buffer.close();
            }
        }

    }

    /**
     * Creating numeric table 
     */
    static void createNumericTable() {
        String sql = "create table " + numericTable + " (" + "PlainSmallint smallint null,"
                + "RandomizedSmallint smallint ENCRYPTED WITH (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL,"
                + "DeterministicSmallint smallint ENCRYPTED WITH (ENCRYPTION_TYPE = DETERMINISTIC, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256', COLUMN_ENCRYPTION_KEY = "
                + cekName + ") NULL" + ");";

        try {
            stmt.execute(sql);
        }
        catch (SQLException e) {
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
