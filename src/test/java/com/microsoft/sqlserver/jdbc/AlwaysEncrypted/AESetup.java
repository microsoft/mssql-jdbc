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
    static String certStore = null;
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
        certStore = keyStoreName;        
    }

    private static void readFromFile(String inputFile,
            String lookupValue) throws IOException {
        BufferedReader buffer = null;
        filePath = Utils.getCurrentClassPath();
        try {
            File f = new File(filePath + inputFile);
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
    static void createCEK(SQLServerColumnEncryptionKeyStoreProvider storeProvider,
            String certStore) throws SQLException {
        String letters = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        byte[] valuesDefault = letters.getBytes();
        String cekSql = null;
        if (certStore.equalsIgnoreCase("MSSQL_JAVA_KEYSTORE")) {
            byte[] key = storeProvider.encryptColumnEncryptionKey(javaKeyAliases, "RSA_OAEP", valuesDefault);
            cekSql = "CREATE COLUMN ENCRYPTION KEY " + cekName + " WITH VALUES " + "(COLUMN_MASTER_KEY = " + cmkName
                    + ", ALGORITHM = 'RSA_OAEP', ENCRYPTED_VALUE = 0x" + DatatypeConverter.printHexBinary(key) + ")" + ";";
        }
        else if (certStore.equalsIgnoreCase("MSSQL_CERTIFICATE_STORE")) {
            String encryptedValue = "0x016E000001630075007200720065006E00740075007300650072002F006D0079002F0066006200640066003900360031003600360031003100390066006600390039006200380032003800300064003200390064003100360030006600610065006300370030006300640031003100620034002DC298A90D6C4FB6EE59BD1F4E58E3CE334B33E4786608B0A29B8B6FDD376F9C42716E00077D91FE80659EB427F1D5509971D24B3B7CB761E79CBD894CBE8EE0009DE4DB9ABECCC398F80AD8B95E3A89692E91BCF6B0518552CFD224816F67E0C37D48B538E38A91A9BA73D6CF84F315560BCB69423D0F4682FDE1DD12412823362641E6B7F19843390D2BE9E26BDA0FCAB01F987EF7AA882468EE86FAB6FE29C771FB22BEF355377B158DA06D9998171110A21AEEDA875851CE8BC64A49D00925AD844F47150F27B6147DAACE1E4B93C9E2B9B91BF5B26BD6FE10EF0C2EDC9395A9E5D2B007E6F16229ABC27068C07F7A77EC32F24FCFE04D53CF260A58440009F8B70E4A9091426159189C021A25D52E7FEA9B341DAC5361C41F3E32800D31A10EF193E4F58DE161302C1E0607B1FA56288FA4592F3F269173D4177BB77EEFCA6B99052EE9A8725B121A731981133C25414634DAB47040A7AED2EAFBA459FF1CA6A19C500A305C2154D9E64B4DD79D8B7394703756A4BCE39782BC5C3E6C9FAC088149554F5AED125FBFC081CFEE8FA83153135BE10718167AF4114F37CA10925A690D94BF53C69AF4BE6F8CAE74450BCDE312E2074D9F5788E57C515A507B86E64B54AC3624F3F8A29C9007C798518304766F6862A0824108143B2E532B82442816A9D89A9585E343CEE6480F7AC881584CA14F5A929A7FF3562D57B40305";
            cekSql = "CREATE COLUMN ENCRYPTION KEY " + cekName + " WITH VALUES " + "(COLUMN_MASTER_KEY = " + cmkName
                    + ", ALGORITHM = 'RSA_OAEP', ENCRYPTED_VALUE = " + encryptedValue + ")" + ";";

        }
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
