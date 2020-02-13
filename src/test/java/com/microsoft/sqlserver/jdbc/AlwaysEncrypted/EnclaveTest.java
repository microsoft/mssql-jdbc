/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.microsoft.sqlserver.jdbc.EnclavePackageTest;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
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
}
