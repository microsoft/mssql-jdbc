package com.microsoft.sqlserver.jdbc;

import com.microsoft.sqlserver.jdbc.AlwaysEncrypted.AESetup;import com.microsoft.sqlserver.testframework.Constants;import com.microsoft.sqlserver.testframework.PrepUtil;import org.junit.jupiter.api.BeforeAll;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;import java.sql.Statement;import java.util.Properties;import java.util.logging.LogManager;
import static org.junit.jupiter.api.Assertions.assertTrue;import static org.junit.jupiter.api.Assertions.fail;

public class TestApp {
    
    public static String connectionString = "jdbc:sqlserver://drivers-ae-vbs-none.database.windows.net;" 
        + "userName=employee;password=Moonshine4me;databaseName=ContosoHR;";
    public static String AETestConnectionString = "";
    public static Properties AEInfo;
    
    public static void main(String[] args) throws Exception  {
        setAEConnectionString("drivers-ae-vbs-none.database.windows.net", " ", "NONE");
        String cekJks = Constants.CEK_NAME + "_JKS";
        String varcharTableSimple[][] = {{"Varchar", "varchar(20) COLLATE LATIN1_GENERAL_BIN2", "VARCHAR"}};
        
        try (SQLServerConnection c = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                Statement s = c.createStatement()) {
            createTable("CHAR_TABLE_AE", "CEK1", varcharTableSimple);
            PreparedStatement pstmt = c.prepareStatement("INSERT INTO " + "CHAR_TABLE_AE" + " VALUES (?,?,?)");
            pstmt.setString(1, "a");
            pstmt.setString(2, "b");
            pstmt.setString(3, "test");
            pstmt.execute();
            pstmt = c.prepareStatement("SELECT * FROM " + "CHAR_TABLE_AE" + " WHERE RANDOMIZEDVarchar LIKE ?");
            pstmt.setString(1, "t%");
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    assertTrue(rs.getString(1).equalsIgnoreCase("a"), "rs.getString(1)=" + rs.getString(1));
                    assertTrue(rs.getString(2).equalsIgnoreCase("b"), "rs.getString(2)=" + rs.getString(2));
                    assertTrue(rs.getString(3).equalsIgnoreCase("test"), "rs.getString(3)=" + rs.getString(3));
                }
            }
        }
    }
    
//    @BeforeAll
//    public static void setupAETest() throws Exception {
//        
//        String javaKeyPath = TestUtils.getCurrentClassPath() + Constants.JKS_NAME;
//        SQLServerColumnEncryptionKeyStoreProvider jksProvider = new SQLServerColumnEncryptionJavaKeyStoreProvider(javaKeyPath,
//            Constants.JKS_SECRET.toCharArray());
//
//        AEInfo = new Properties();
//        AEInfo.setProperty("ColumnEncryptionSetting", Constants.ENABLED);
//        AEInfo.setProperty("keyStoreAuthentication", Constants.JAVA_KEY_STORE_SECRET);
//        AEInfo.setProperty("keyStoreLocation", javaKeyPath);
//        AEInfo.setProperty("keyStoreSecret", Constants.JKS_SECRET);
//
//        createCMK(AETestConnectionString, "CMK1", Constants.JAVA_KEY_STORE_NAME, "lp-37941554-7af9-4e74-a646-8772dd264012",
//                Constants.CMK_SIGNATURE);
//        createCEK(AETestConnectionString, "CMK1", "CEK1", jksProvider);
//    }
    
    protected static void createTable(String tableName, String cekName, String table[][]) throws SQLException {
        String encryptSql = " ENCRYPTED WITH (ENCRYPTION_TYPE = %s, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256'," 
            + " COLUMN_ENCRYPTION_KEY = %s";
        String createSql = "CREATE TABLE %s (%s)";
        try (SQLServerConnection con = (SQLServerConnection) PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            StringBuilder sql = new StringBuilder(16);
            for(String[] strings: table){
                sql.append("PLAIN").append(strings[0]).append(" ").append(strings[1]).append(" NULL,");
                sql.append("DETERMINISTIC").append(strings[0]).append(" ").append(strings[1])
                    .append(String.format(encryptSql, "DETERMINISTIC", cekName)).append(") NULL,");
                sql.append("RANDOMIZED").append(strings[0]).append(" ").append(strings[1])
                    .append(String.format(encryptSql, "RANDOMIZED", cekName)).append(") NULL,");
            }
            TestUtils.dropTableIfExists(tableName, stmt);
            sql=new StringBuilder(String.format(createSql, tableName, sql.toString()));
            stmt.execute(sql.toString());
            stmt.execute("DBCC FREEPROCCACHE");
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }
    
    static void setAEConnectionString(String serverName, String url, String protocol) {
        String enclaveProperties = "serverName=" + serverName + ";" + Constants.ENCLAVE_ATTESTATIONURL + "=" + url + ";"
                + Constants.ENCLAVE_ATTESTATIONPROTOCOL + "=" + protocol;
        AETestConnectionString = connectionString + ";sendTimeAsDateTime=false" + ";columnEncryptionSetting=enabled"
                + ";" + enclaveProperties;
    }
    
//    protected static void createCMK(String connectionString, String cmkName, String keyStoreName, String keyPath,
//            String signature) throws SQLException {
//        try (SQLServerConnection con = (SQLServerConnection) PrepUtil
//                .getConnection(connectionString + ";sendTimeAsDateTime=false", AEInfo);
//                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
//            String sql = " if not exists (SELECT name from sys.column_master_keys where name='" + cmkName + "')"
//                    + " begin" + " CREATE COLUMN MASTER KEY " + cmkName + " WITH (KEY_STORE_PROVIDER_NAME = '"
//                    + keyStoreName + "', KEY_PATH = '" + keyPath + "'"
//                    + (TestUtils.isAEv2(con) ? ",ENCLAVE_COMPUTATIONS (SIGNATURE = " + signature + ")) end" : ") end");
//            stmt.execute(sql);
//        }
//    }
//    
//    protected static void createCEK(String connectionString, String cmkName, String cekName,
//            SQLServerColumnEncryptionKeyStoreProvider storeProvider) throws SQLException {
//        try (SQLServerConnection con = (SQLServerConnection) PrepUtil
//                .getConnection(connectionString + ";sendTimeAsDateTime=false", AEInfo);
//                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
//            byte[] valuesDefault = Constants.CEK_STRING.getBytes();
//            String encryptedValue;
//
//            if (storeProvider instanceof SQLServerColumnEncryptionJavaKeyStoreProvider) {
//                byte[] key = storeProvider.encryptColumnEncryptionKey("lp-37941554-7af9-4e74-a646-8772dd264012", Constants.CEK_ALGORITHM,
//                        valuesDefault);
//                encryptedValue = "0x" + TestUtils.bytesToHexString(key, key.length);
//            } else if (storeProvider instanceof SQLServerColumnEncryptionAzureKeyVaultProvider) {
//                byte[] key = storeProvider.encryptColumnEncryptionKey("https://susanakv.vault.azure.net/keys/Always-Encrypted-Auto1/86412ce2ec8746f4ab1ba159f6d585e6", Constants.CEK_ALGORITHM,
//                        valuesDefault);
//                encryptedValue = "0x" + TestUtils.bytesToHexString(key, key.length);
//            } else {
//                encryptedValue = Constants.CEK_ENCRYPTED_VALUE;
//            }
//
//            String sql = "if not exists (SELECT name from sys.column_encryption_keys where name='" + cekName + "')"
//                    + " begin" + " CREATE COLUMN ENCRYPTION KEY " + cekName + " WITH VALUES " + "(COLUMN_MASTER_KEY = "
//                    + cmkName + ", ALGORITHM = '" + Constants.CEK_ALGORITHM + "', ENCRYPTED_VALUE = " + encryptedValue
//                    + ") end;";
//            stmt.execute(sql);
//        }
//    }
}
