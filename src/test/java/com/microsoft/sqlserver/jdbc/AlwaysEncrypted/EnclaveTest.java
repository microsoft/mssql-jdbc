/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.microsoft.sqlserver.jdbc.EnclavePackageTest;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Tests Enclave decryption and encryption of values
 *
 */
@RunWith(Parameterized.class)
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
        EnclavePackageTest.testBasicConnection(serverName, url, protocol);
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
        checkAESetup(serverName, url, protocol);

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
            assertTrue(e.getMessage().contains(TestResource.getResource("R_AlterAEv2Error")));
        }
    }

    /*
     * Test char
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
     * Test char
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
        checkAESetup(serverName, url, protocol);
        try (SQLServerConnection c = PrepUtil.getConnection(AETestConnectionString + ";useFmtOnly=true", AEInfo);
                Statement s = c.createStatement()) {
            createTable(NUMERIC_TABLE_AE, cekJks, numericTable);
            String sql = "insert into " + NUMERIC_TABLE_AE + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
                    + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
                    + "?,?,?," + "?,?,?," + "?,?,?" + ")";
            try (PreparedStatement p = c.prepareStatement(sql)) {
                ParameterMetaData pmd = p.getParameterMetaData();
                assertTrue(pmd.getParameterCount() == 48);
            }
        }
    }
    
    /**
     * Test alter column
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testAlter(String serverName, String url, String protocol) throws Exception {
        checkAESetup(serverName, url, protocol);
        try (SQLServerConnection c = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                Statement s = c.createStatement()) {
            createTable(NUMERIC_TABLE_AE, cekJks, numericTableSimple);
            s.execute("INSERT INTO " + NUMERIC_TABLE_AE + " VALUES (1,2,3)");
            PreparedStatement pstmt = c.prepareStatement("ALTER TABLE " + NUMERIC_TABLE_AE + "ALTER COLUMN RANDOMIZEDInt INT NULL WITH (ONLINE = ON)");
            pstmt.execute();
        }
    }

    /**
     * Rich Query with number compare
     */
    @ParameterizedTest
    @MethodSource("enclaveParams")
    public void testNumericRichQuery(String serverName, String url, String protocol) throws Exception {
        checkAESetup(serverName, url, protocol);
        try (SQLServerConnection c = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                Statement s = c.createStatement()) {
            createTable(NUMERIC_TABLE_AE, cekJks, numericTableSimple);
            s.execute("INSERT INTO " + NUMERIC_TABLE_AE + " VALUES (1,2,3)");
            PreparedStatement pstmt = c.prepareStatement("SELECT * FROM " + NUMERIC_TABLE_AE + " WHERE RANDOMIZEDInt LIKE ?");
            pstmt.setInt(1, 3);
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    assertTrue(rs.getInt(1) == 1);
                    assertTrue(rs.getInt(2) == 2);
                    assertTrue(rs.getInt(3) == 3);
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
        checkAESetup(serverName, url, protocol);
        try (SQLServerConnection c = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                Statement s = c.createStatement()) {
            createTable(CHAR_TABLE_AE, cekJks, charTableSimple);
            s.execute("INSERT INTO " + CHAR_TABLE_AE + " VALUES ('a','b','test')");
            PreparedStatement pstmt = c.prepareStatement("SELECT * FROM " + CHAR_TABLE_AE + " WHERE RANDOMIZEDChar LIKE ?");
            pstmt.setString(1, "t%");
            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    assertTrue(rs.getString(1).equalsIgnoreCase("a"));
                    assertTrue(rs.getString(2).equalsIgnoreCase("b"));
                    assertTrue(rs.getString(3).equalsIgnoreCase("test"));
                }
            }
        }
    }
}
