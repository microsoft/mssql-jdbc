/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.clientcertauth;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests client certificate authentication feature
 * The feature is only supported against SQL Server Linux CU2 or higher.
 * 
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xSQLv12)
@Tag(Constants.xSQLv14)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.xAzureSQLDB)
@Tag(Constants.clientCertAuth)
public class ClientCertificateAuthenticationTest extends AbstractTest {

    static final String PEM_SUFFIX = ".pem;";
    static final String CER_SUFFIX = ".cer;";
    static final String PVK_SUFFIX = ".pvk;";

    static final String PKCS1_KEY_SUFFIX = "-pkcs1.key;";
    static final String ENCRYPTED_PKCS1_KEY_SUFFIX = "-encrypted-pkcs1.key;";
    static final String PKCS8_KEY_SUFFIX = "-pkcs8.key;";
    static final String ENCRYPTED_PKCS8_KEY_SUFFIX = "-encrypted-pkcs8.key;";
    static final String PFX_KEY_SUFFIX = ".pfx;";
    static final String ENCRYPTED_PFX_KEY_SUFFIX = "-encrypted.pfx;";

    @BeforeAll
    public static void setupTests() throws Exception {
        //Turn off default encrypt true
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"encrypt", "false");
        setConnection();
    }

    /**
     * Tests client certificate authentication feature with PKCS1 private key.
     * 
     * @throws Exception
     */
    @Test
    public void pkcs1Test() throws Exception {
        String conStr = connectionString + ";clientCertificate=" + clientCertificate + PEM_SUFFIX + "clientKey="
                + clientKey + PKCS1_KEY_SUFFIX;
        try (Connection conn = DriverManager.getConnection(conStr)) {
            assertTrue(conn.isValid(1));
        }
    }

    /**
     * Tests client certificate authentication feature with PKCS1 private key that has been encrypted with a password.
     * 
     * @throws Exception
     */
    @Test
    public void pkcs1EncryptedTest() throws Exception {
        String conStr = connectionString + ";clientCertificate=" + clientCertificate + PEM_SUFFIX + "clientKey="
                + clientKey + ENCRYPTED_PKCS1_KEY_SUFFIX + "clientKeyPassword=" + clientKeyPassword + ";";
        try (Connection conn = DriverManager.getConnection(conStr)) {
            assertTrue(conn.isValid(1));
        }
    }

    /**
     * Tests client certificate authentication feature with PKCS8 private key.
     * 
     * @throws Exception
     */
    @Test
    public void pkcs8Test() throws Exception {
        String conStr = connectionString + ";clientCertificate=" + clientCertificate + PEM_SUFFIX + "clientKey="
                + clientKey + PKCS8_KEY_SUFFIX;
        try (Connection conn = DriverManager.getConnection(conStr)) {
            assertTrue(conn.isValid(1));
        }
    }

    /**
     * Tests client certificate authentication feature with PKCS8 private key that has been encrypted with a password.
     * 
     * @throws Exception
     */
    @Test
    public void pkcs8EncryptedTest() throws Exception {
        String conStr = connectionString + ";clientCertificate=" + clientCertificate + PEM_SUFFIX + "clientKey="
                + clientKey + ENCRYPTED_PKCS8_KEY_SUFFIX + "clientKeyPassword=" + clientKeyPassword + ";";
        try (Connection conn = DriverManager.getConnection(conStr)) {
            assertTrue(conn.isValid(1));
        }
    }

    /**
     * Tests client certificate authentication feature with PFX private key.
     * 
     * @throws Exception
     */
    @Test
    public void pfxTest() throws Exception {
        String conStr = connectionString + ";clientCertificate=" + clientCertificate + PFX_KEY_SUFFIX;
        try (Connection conn = DriverManager.getConnection(conStr)) {
            assertTrue(conn.isValid(1));
        }
    }

    /**
     * Tests client certificate authentication feature with PFX private key that has been encrypted with a password.
     * 
     * @throws Exception
     */
    @Test
    public void pfxEncrytedTest() throws Exception {
        String conStr = connectionString + ";clientCertificate=" + clientCertificate + ENCRYPTED_PFX_KEY_SUFFIX
                + "clientKeyPassword=" + clientKeyPassword + ";";
        try (Connection conn = DriverManager.getConnection(conStr)) {
            assertTrue(conn.isValid(1));
        }
    }

    /**
     * Tests client certificate authentication feature with PVK private key.
     * 
     * @throws Exception
     */
    @Test
    public void pvkTest() throws Exception {
        String conStr = connectionString + ";clientCertificate=" + clientCertificate + CER_SUFFIX + "clientKey="
                + clientKey + PVK_SUFFIX + "clientKeyPassword=" + clientKeyPassword + ";";
        try (Connection conn = DriverManager.getConnection(conStr)) {
            assertTrue(conn.isValid(1));
        }
    }

    /**
     * Tests client certificate authentication feature with invalid certificate provided.
     * 
     * @throws Exception
     */
    @Test
    public void invalidCert() throws Exception {
        String conStr = connectionString + ";clientCertificate=invalid_path;" + "clientKeyPassword=" + clientKeyPassword
                + ";";
        try (Connection conn = DriverManager.getConnection(conStr)) {} catch (SQLServerException e) {
            assertTrue(e.getCause().getMessage().matches(TestUtils.formatErrorMsg("R_clientCertError")));
        }
    }

    /**
     * Tests client certificate authentication feature with invalid certificate password provided.
     * 
     * @throws Exception
     */
    @Test
    public void invalidCertPassword() throws Exception {
        String conStr = connectionString + ";clientCertificate=" + clientCertificate + PFX_KEY_SUFFIX
                + "clientKeyPassword=invalid_password;";
        try (Connection conn = DriverManager.getConnection(conStr)) {} catch (SQLServerException e) {
            assertTrue(e.getMessage().contains(TestResource.getResource("R_keystorePassword")));
        }
    }

    /**
     * Tests client certificate authentication feature using a data source.
     * 
     * @throws Exception
     */
    @Test
    public void testDataSource() throws Exception {
        SQLServerDataSource dsLocal = new SQLServerDataSource();
        AbstractTest.updateDataSource(connectionString, dsLocal);
        dsLocal.setClientCertificate(clientCertificate + PEM_SUFFIX.substring(0, PEM_SUFFIX.length() - 1));
        dsLocal.setClientKey(
                clientKey + ENCRYPTED_PKCS1_KEY_SUFFIX.substring(0, ENCRYPTED_PKCS1_KEY_SUFFIX.length() - 1));
        dsLocal.setClientKeyPassword(clientKeyPassword);

        try (Connection conn = dsLocal.getConnection()) {
            assertTrue(conn.isValid(1));
        }
    }

    /**
     * Tests client certificate authentication feature with encryption turned on.
     * 
     * @throws Exception
     */
    @Test
    public void testEncryptTrusted() throws Exception {
        String conStr = connectionString + ";clientCertificate=" + clientCertificate + PEM_SUFFIX + "clientKey="
                + clientKey + PKCS8_KEY_SUFFIX + "encrypt=true;trustServerCertificate=true;";
        try (Connection conn = DriverManager.getConnection(conStr); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt
                    .executeQuery("SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID");
            rs.next();
            assertTrue(rs.getBoolean(1));
        }
    }

    /**
     * Tests client certificate authentication feature with encryption turned on, untrusted.
     * 
     * @throws Exception
     */
    @Test
    public void testEncryptUntrusted() throws Exception {
        String conStr = connectionString + ";clientCertificate=" + clientCertificate + PEM_SUFFIX + "clientKey="
                + clientKey + PKCS8_KEY_SUFFIX + "encrypt=true;trustServerCertificate=false;trustStore="
                + trustStorePath;
        try (Connection conn = DriverManager.getConnection(conStr); Statement stmt = conn.createStatement()) {
            ResultSet rs = stmt
                    .executeQuery("SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id = @@SPID");
            rs.next();
            assertTrue(rs.getBoolean(1));
        }
    }
}
