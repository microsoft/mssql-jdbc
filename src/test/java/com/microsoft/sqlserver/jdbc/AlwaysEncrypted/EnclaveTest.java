/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.microsoft.sqlserver.jdbc.EnclavePackageTest;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Tests Enclave decryption and encryption of values
 *
 */
@RunWith(Parameterized.class)
@Tag(Constants.xSQLv11)
@Tag(Constants.xSQLv12)
@Tag(Constants.xSQLv14)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.xAzureSQLDB)
@Tag(Constants.reqExternalSetup)
public class EnclaveTest extends AESetup {
    /**
     * Tests basic connection.
     * 
     * @throws Exception
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testBasicConnection(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        EnclavePackageTest.testBasicConnection(AETestConnectionString, protocol);
    }

    /**
     * Tests invalid connection property combinations.
     * 
     * @throws Exception
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testInvalidProperties(String serverName, String url, String protocol) throws Exception {
        EnclavePackageTest.testInvalidProperties(serverName, url, protocol);
    }

    /*
     * Test calling verifyColumnMasterKeyMetadata for non enclave computation
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testVerifyCMKNoEnclave(String serverName, String url, String protocol) throws Exception {
        EnclavePackageTest.testVerifyCMKNoEnclave();
    }

    /*
     * Test calling verifyColumnMasterKeyMetadata with untrusted key path
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testVerifyCMKUntrusted(String serverName, String url, String protocol) throws Exception {
        EnclavePackageTest.testVerifyCMKUntrusted();
    }

    /*
     * Test getEnclavePackage with null enclaveSession
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testGetEnclavePackage(String serverName, String url, String protocol) throws Exception {
        EnclavePackageTest.testGetEnclavePackage();
    }

    /*
     * Test invalidEnclaveSession
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testInvalidEnclaveSession(String serverName, String url, String protocol) throws Exception {
        EnclavePackageTest.testInvalidEnclaveSession(serverName, url, protocol);
    }

    /*
     * Test VSM createSessionSecret with bad server response
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testNullSessionSecret(String serverName, String url, String protocol) throws Exception {
        EnclavePackageTest.testNullSessionSecret();
    }

    /*
     * Test bad session secret
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testBadSessionSecret(String serverName, String url, String protocol) throws Exception {
        EnclavePackageTest.testBadSessionSecret();
    }

    /*
     * Test null Attestation response
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testNullAttestationResponse(String serverName, String url, String protocol) throws Exception {
        EnclavePackageTest.testNullAttestationResponse();
    }

    /*
     * Test bad Attestation response
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testBadAttestationResponse(String serverName, String url, String protocol) throws Exception {
        EnclavePackageTest.testBadAttestationResponse();
    }

    /*
     * Test bad certificate signature
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testBadCertSignature(String serverName, String url, String protocol) throws Exception {
        EnclavePackageTest.testBadCertSignature();
    }

    /*
     * Negative Test - AEv2 not supported
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testAEv2NotSupported(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        boolean isAEv2 = false;
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo)) {
            isAEv2 = TestUtils.isAEv2(con);
        } catch (SQLException e) {
            isAEv2 = false;
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.getMessage());
        }
        org.junit.Assume.assumeFalse(isAEv2);
        EnclavePackageTest.testAEv2NotSupported(serverName, url, protocol);
    }

    /*
     * Negative Test = AEv2 not enabled
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testAEv2Disabled(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        // connection string w/o AEv2
        String testConnectionString = TestUtils.removeProperty(AETestConnectionString,
                Constants.ENCLAVE_ATTESTATIONURL);
        testConnectionString = TestUtils.removeProperty(testConnectionString, Constants.ENCLAVE_ATTESTATIONPROTOCOL);

        try (SQLServerConnection con = PrepUtil.getConnection(testConnectionString);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            String[] values = createCharValues(false);
            TestUtils.dropTableIfExists(CHAR_TABLE_AE, stmt);
            createTable(CHAR_TABLE_AE, cekJks, charTable);
            populateCharNormalCase(values);
            testAlterColumnEncryption(stmt, CHAR_TABLE_AE, charTable, cekJks);
            fail(TestResource.getResource("R_expectedExceptionNotThrown"));
        } catch (Throwable e) {
            // testChars called fail()
            assertTrue(e.getMessage().contains(TestResource.getResource("R_AlterAEv2Error")), e.getMessage());
        }
    }

    /*
     * Negative Test - bad JKS signature
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testVerifyBadJksSignature(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        // create CMK with a bad signature
        String badCmk = Constants.CMK_NAME + "_badCMK";
        String badCek = Constants.CEK_NAME + "_badCek";
        String badTable = TestUtils.escapeSingleQuotes(
                AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("testVerifyBadJksSignature")));

        try (SQLServerConnection c = PrepUtil.getConnection(AETestConnectionString + ";sendStringParametersAsUnicode=false;", AEInfo);
                Statement s = c.createStatement()) {
            createCMK(AETestConnectionString, badCmk, Constants.JAVA_KEY_STORE_NAME, javaKeyAliases, "0x666");
            createCEK(AETestConnectionString, badCmk, badCek, jksProvider);

            createTable(badTable, badCek, varcharTableSimple);

            PreparedStatement pstmt = c.prepareStatement("INSERT INTO " + badTable + " VALUES (?,?,?)");
            pstmt.setString(1, "a");
            pstmt.setString(2, "b");
            pstmt.setString(3, "test");
            pstmt.execute();

            pstmt = c.prepareStatement("SELECT * FROM " + badTable + " WHERE RANDOMIZEDVarchar LIKE ?");
            pstmt.setString(1, "t%");
            try (ResultSet rs = pstmt.executeQuery()) {
                fail(TestResource.getResource("R_expectedFailPassed"));
            }
        } catch (Exception e) {
            assertTrue(
                    e.getMessage().matches(TestUtils.formatErrorMsg("R_SignatureNotMatch"))
                            || e.getMessage().matches(TestUtils.formatErrorMsg("R_VerifySignatureFailed")),
                    e.getMessage());
        } finally {
            try (Statement s = connection.createStatement()) {
                TestUtils.dropTableIfExists(badTable, s);
                dropCEK(badCek, s);
                dropCMK(badCmk, s);
            }
        }
    }

    /*
     * Negative Test - bad AKS signature
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testVerifyBadAkvSignature(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        // create CMK with a bad signature
        String badCmk = Constants.CMK_NAME + "_badCMK";
        String badCek = Constants.CEK_NAME + "_badCek";
        String badTable = TestUtils.escapeSingleQuotes(
                AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("testVerifyBadAkvSignature")));

        try (SQLServerConnection c = PrepUtil.getConnection(AETestConnectionString + ";sendStringParametersAsUnicode=false;", AEInfo);
                Statement s = c.createStatement()) {
            createCMK(AETestConnectionString, badCmk, Constants.AZURE_KEY_VAULT_NAME, keyIDs[0], "0x666");
            createCEK(AETestConnectionString, badCmk, badCek, akvProvider);

            createTable(badTable, badCek, varcharTableSimple);

            PreparedStatement pstmt = c.prepareStatement("INSERT INTO " + badTable + " VALUES (?,?,?)");
            pstmt.setString(1, "a");
            pstmt.setString(2, "b");
            pstmt.setString(3, "test");
            pstmt.execute();

            pstmt = c.prepareStatement("SELECT * FROM " + badTable + " WHERE RANDOMIZEDVarchar LIKE ?");
            pstmt.setString(1, "t%");
            try (ResultSet rs = pstmt.executeQuery()) {
                fail(TestResource.getResource("R_expectedFailPassed"));
            }
        } catch (Exception e) {
            assertTrue(
                    e.getMessage().matches(TestUtils.formatErrorMsg("R_SignatureNotMatch"))
                            || e.getMessage().matches(TestUtils.formatErrorMsg("R_VerifySignatureFailed")),
                    e.getMessage());
        } finally {
            try (Statement s = connection.createStatement()) {
                TestUtils.dropTableIfExists(badTable, s);
                dropCEK(badCek, s);
                dropCMK(badCmk, s);
            }
        }
    }

    /*
     * Negative Test - bad AKS signature
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testVerifyBadWinSignature(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);

        String badCmk = Constants.CMK_NAME + "_badCMK";
        String badCek = Constants.CEK_NAME + "_badCek";
        String badTable = TestUtils.escapeSingleQuotes(
                AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("testVerifyBadWinSignature")));

        try (SQLServerConnection c = PrepUtil.getConnection(AETestConnectionString + ";sendStringParametersAsUnicode=false;", AEInfo);
                Statement s = c.createStatement()) {
            // create CMK with a bad signature
            createCMK(AETestConnectionString, badCmk, Constants.WINDOWS_KEY_STORE_NAME, windowsKeyPath, "0x666");
            createCEK(AETestConnectionString, badCmk, badCek, null);

            createTable(badTable, badCek, varcharTableSimple);

            PreparedStatement pstmt = c.prepareStatement("INSERT INTO " + badTable + " VALUES (?,?,?)");
            pstmt.setString(1, "a");
            pstmt.setString(2, "b");
            pstmt.setString(3, "test");
            pstmt.execute();

            pstmt = c.prepareStatement("SELECT * FROM " + badTable + " WHERE RANDOMIZEDVarchar LIKE ?");
            pstmt.setString(1, "t%");
            try (ResultSet rs = pstmt.executeQuery()) {
                fail(TestResource.getResource("R_expectedFailPassed"));
            }
        } catch (Exception e) {
            // windows error message is different
            assertTrue(
                    e.getMessage().contains("signature does not match")
                            || e.getMessage().matches(TestUtils.formatErrorMsg("R_VerifySignatureFailed")),
                    e.getMessage());
        } finally {
            try (Statement s = connection.createStatement()) {
                TestUtils.dropTableIfExists(badTable, s);
                dropCEK(badCek, s);
                dropCMK(badCmk, s);
            }
        }
    }

    /*
     * Tests alter column encryption on char tables
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testChar(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            TestUtils.dropTableIfExists(CHAR_TABLE_AE, stmt);
            createTable(CHAR_TABLE_AE, cekJks, charTable);
            populateCharNormalCase(createCharValues(false));
            testAlterColumnEncryption(stmt, CHAR_TABLE_AE, charTable, cekJks);
        }
    }

    /*
     * Tests alter column encryption on char tables with AKV
     */

    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testCharAkv(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection con = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {
            TestUtils.dropTableIfExists(CHAR_TABLE_AE, stmt);
            createTable(CHAR_TABLE_AE, cekAkv, charTable);
            populateCharNormalCase(createCharValues(false));
            testAlterColumnEncryption(stmt, CHAR_TABLE_AE, charTable, cekAkv);
        }
    }

    /**
     * Test FMTOnly with Always Encrypted
     * 
     * @throws SQLException
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testAEFMTOnly(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection c = PrepUtil.getConnection(AETestConnectionString + ";useFmtOnly=true", AEInfo);
                Statement s = c.createStatement()) {
            createTable(NUMERIC_TABLE_AE, cekJks, numericTable);
            String sql = "insert into " + NUMERIC_TABLE_AE + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
                    + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
                    + "?,?,?," + "?,?,?," + "?,?,?" + ")";
            try (PreparedStatement p = c.prepareStatement(sql)) {
                ParameterMetaData pmd = p.getParameterMetaData();
                assertTrue(48 == pmd.getParameterCount(), "parameter count: " + pmd.getParameterCount());
            }
        }
    }

    /**
     * Test alter column
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testAlter(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection c = PrepUtil.getConnection(AETestConnectionString + ";sendStringParametersAsUnicode=false;", AEInfo);
                Statement s = c.createStatement()) {
            createTable(CHAR_TABLE_AE, cekJks, varcharTableSimple);
            PreparedStatement pstmt = c.prepareStatement("INSERT INTO " + CHAR_TABLE_AE + " VALUES (?,?,?)");
            pstmt.setString(1, "a");
            pstmt.setString(2, "b");
            pstmt.setString(3, "test");
            pstmt.execute();
            pstmt = c.prepareStatement("ALTER TABLE " + CHAR_TABLE_AE
                    + " ALTER COLUMN RandomizedVarchar VARCHAR(20) NULL WITH (ONLINE = ON)");
            pstmt.execute();
        }
    }

    /**
     * Rich Query with number compare
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testNumericRichQuery(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection c = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                Statement s = c.createStatement()) {
            createTable(NUMERIC_TABLE_AE, cekJks, numericTableSimple);
            PreparedStatement pstmt = c.prepareStatement("INSERT INTO " + NUMERIC_TABLE_AE + " VALUES (?,?,?)");
            pstmt.setInt(1, 1);
            pstmt.setInt(2, 2);
            pstmt.setInt(3, 3);
            pstmt.execute();
            pstmt = c.prepareStatement("SELECT * FROM " + NUMERIC_TABLE_AE + " WHERE RANDOMIZEDInt = ?");
            pstmt.setInt(1, 3);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    assertTrue(1 == rs.getInt(1), "rs.getInt(1)=" + rs.getInt(1));
                    assertTrue(2 == rs.getInt(2), "rs.getInt(2)=" + rs.getInt(2));
                    assertTrue(3 == rs.getInt(3), "rs.getInt(3)=" + rs.getInt(3));
                }
            }
        }
    }

    /**
     * Rich Query with string compare
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testStringRichQuery(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection c = PrepUtil.getConnection(AETestConnectionString + ";sendStringParametersAsUnicode=false;", AEInfo);
                Statement s = c.createStatement()) {
            createTable(CHAR_TABLE_AE, cekJks, varcharTableSimple);

            PreparedStatement pstmt = c.prepareStatement("INSERT INTO " + CHAR_TABLE_AE + " VALUES (?,?,?)");
            pstmt.setString(1, "a");
            pstmt.setString(2, "b");
            pstmt.setString(3, "test");
            pstmt.execute();
            pstmt = c.prepareStatement("SELECT * FROM " + CHAR_TABLE_AE + " WHERE RANDOMIZEDVarchar LIKE ?");
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

    /**
     * Test alter column with a non AEv2 connection
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testAlterNoEncrypt(String serverName, String url, String protocol) throws Exception {
        setAEConnectionString(serverName, url, protocol);
        try (SQLServerConnection c = PrepUtil.getConnection(AETestConnectionString + ";sendStringParametersAsUnicode=false;", AEInfo);
                Statement s = c.createStatement()) {
            createTable(CHAR_TABLE_AE, cekJks, varcharTableSimple);
            PreparedStatement pstmt = c.prepareStatement("INSERT INTO " + CHAR_TABLE_AE + " VALUES (?,?,?)");
            pstmt.setString(1, "a");
            pstmt.setString(2, "b");
            pstmt.setString(3, "test");
            pstmt.execute();
        }
        String testConnectionString = TestUtils.removeProperty(AETestConnectionString,
                Constants.ENCLAVE_ATTESTATIONURL);
        testConnectionString = TestUtils.removeProperty(testConnectionString, Constants.ENCLAVE_ATTESTATIONPROTOCOL);
        try (Connection c = DriverManager.getConnection(testConnectionString)) {
            PreparedStatement pstmt = c.prepareStatement("ALTER TABLE " + CHAR_TABLE_AE
                    + " ALTER COLUMN RandomizedVarchar VARCHAR(20) NULL WITH (ONLINE = ON)");
            pstmt.execute();
        } catch (SQLException e) {
            assertTrue(e.getMessage().contains(TestResource.getResource("R_enclaveNotEnabled")), e.getMessage());
        }
    }

    @AfterAll
    public static void dropAll() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(CHAR_TABLE_AE, stmt);
            TestUtils.dropTableIfExists(NUMERIC_TABLE_AE, stmt);
            TestUtils.dropTableIfExists(BINARY_TABLE_AE, stmt);
            TestUtils.dropTableIfExists(DATE_TABLE_AE, stmt);
            TestUtils.dropTableIfExists(SCALE_DATE_TABLE_AE, stmt);
        }
    }
}
