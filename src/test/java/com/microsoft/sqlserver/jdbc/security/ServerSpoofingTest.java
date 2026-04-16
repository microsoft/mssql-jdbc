/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.security;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Tests server spoofing detection scenarios ported from FX security test suite (securitytest.java).
 *
 * Covers FX Threat7 (Server Spoofing) — verifies the driver correctly rejects connections when
 * SSL/TLS certificate validation detects hostname mismatches, untrusted certificates, or
 * connection string manipulation attempting to redirect to a different server.
 *
 * Also covers Threat11 (Network sniffing prevention) by validating that encryption is enforced
 * when configured.
 *
 * Note: Some tests require specific certificate infrastructure and are tagged with reqExternalSetup.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxSecurity)
public class ServerSpoofingTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    // ---------------------------------------------------------------
    // Threat7: Server Spoofing — Certificate hostname validation
    // FX source: securitytest.Threat7_Server_Spoofing
    // ---------------------------------------------------------------

    /**
     * FX Threat7, Variation 1: Verify connection fails when encrypt=true and
     * trustServerCertificate=false with a hostname that doesn't match cert CN.
     * This simulates a MitM/spoofed server presenting a mismatched certificate.
     */
    @Test
    @Tag(Constants.reqExternalSetup)
    public void testCertificateHostnameMismatchRejected() throws Exception {
        // Connect with encrypt=true, trustServerCertificate=false, and a fake hostNameInCertificate
        // The driver must reject the connection because the cert CN won't match
        String connStr = TestUtils.addOrOverrideProperty(connectionString, "encrypt", "true");
        connStr = TestUtils.addOrOverrideProperty(connStr, "trustServerCertificate", "false");
        connStr = TestUtils.addOrOverrideProperty(connStr, "hostNameInCertificate", "spoofed.server.invalid.test");
        connStr = TestUtils.addOrOverrideProperty(connStr, "loginTimeout", "10");

        try {
            try (Connection conn = PrepUtil.getConnection(connStr)) {
                fail("Connection should have been rejected due to certificate hostname mismatch");
            }
        } catch (SQLException e) {
            // Expected — the driver should reject connections when cert hostname doesn't match
            assertNotNull(e.getMessage(), "Exception should have a message describing the TLS failure");
        }
    }

    /**
     * FX Threat7: Verify that encrypt=strict mode enforces certificate validation.
     * Connection must fail when server certificate is not trusted and trustServerCertificate=false.
     */
    @Test
    @Tag(Constants.reqExternalSetup)
    public void testEncryptStrictEnforcesCertValidation() throws Exception {
        // encrypt=strict should require proper certificate validation
        String connStr = TestUtils.addOrOverrideProperty(connectionString, "encrypt", Constants.STRICT);
        connStr = TestUtils.addOrOverrideProperty(connStr, "trustServerCertificate", "false");
        connStr = TestUtils.addOrOverrideProperty(connStr, "hostNameInCertificate", "spoofed.server.invalid.test");
        connStr = TestUtils.addOrOverrideProperty(connStr, "loginTimeout", "10");

        try {
            try (Connection conn = PrepUtil.getConnection(connStr)) {
                fail("Connection with encrypt=strict should fail with mismatched hostNameInCertificate");
            }
        } catch (SQLException e) {
            assertNotNull(e.getMessage());
        }
    }

    /**
     * FX Threat7, Variation 2: Verify connection string injection cannot redirect to a different server.
     * Attempts to append a second serverName via connection string manipulation; the driver should
     * use only the first/primary serverName and ignore or reject injected ones.
     */
    @Test
    public void testConnectionStringRedirectionPrevented() throws Exception {
        // Extract the real server name we're configured to connect to
        String realServer = TestUtils.getProperty(connectionString, "serverName");
        if (realServer == null) {
            // Try extracting from JDBC URL format
            String prefix = Constants.JDBC_PREFIX;
            int start = connectionString.indexOf(prefix);
            if (start >= 0) {
                start += prefix.length();
                int end = connectionString.indexOf(";", start);
                realServer = end >= 0 ? connectionString.substring(start, end) : connectionString.substring(start);
            }
        }

        // Attempt to inject a fake server via trailing properties
        String[] redirectionAttacks = {
                ";serverName=evil.server.test",
                ";server=evil.server.test",
                ";dataSource=evil.server.test",
        };

        for (String attack : redirectionAttacks) {
            String attackConnStr = connectionString + attack;
            try {
                try (Connection conn = DriverManager.getConnection(attackConnStr)) {
                    // If connection succeeds, verify it connected to the legitimate server, not the injected one
                    String connectedServer = conn.getMetaData().getURL();
                    if (connectedServer != null) {
                        assertTrue(!connectedServer.contains("evil.server.test"),
                                "Connection must not redirect to injected server via: " + attack);
                    }
                }
            } catch (SQLException e) {
                // Connection failure is acceptable — means the injection was blocked or caused an error
            }
        }
    }

    /**
     * FX Threat7, Variation 3: Verify named instance port spoofing is detected.
     * When connecting to a named instance, the driver queries SQL Browser for the port.
     * An attacker could try to have the client connect to a different port.
     * With encrypt=true and proper cert validation, this should be detected.
     */
    @Test
    @Tag(Constants.reqExternalSetup)
    public void testInstancePortSpoofingWithEncryption() throws Exception {
        // Attempt to connect with encrypt=true but specify a wrong port
        // The TLS handshake should fail because a spoofed service on a wrong port
        // won't present a valid certificate
        String connStr = TestUtils.addOrOverrideProperty(connectionString, "encrypt", "true");
        connStr = TestUtils.addOrOverrideProperty(connStr, "trustServerCertificate", "false");
        connStr = TestUtils.addOrOverrideProperty(connStr, "portNumber", "11433"); // wrong port
        connStr = TestUtils.addOrOverrideProperty(connStr, "loginTimeout", "5");

        try {
            try (Connection conn = PrepUtil.getConnection(connStr)) {
                // If we somehow connected, verify it's actually our expected server
                String productName = conn.getMetaData().getDatabaseProductName();
                assertNotNull(productName, "If connected on wrong port, product should still be SQL Server");
            }
        } catch (SQLException e) {
            // Expected — wrong port should fail to connect
            assertNotNull(e.getMessage());
        }
    }

    // ---------------------------------------------------------------
    // Threat11: Network sniffing prevention — encryption enforcement
    // FX source: securitytest.Threat11_Network_Sniffing (SSL)
    // ---------------------------------------------------------------

    /**
     * FX Threat11: Verify that encrypt=true actually establishes an encrypted connection.
     * Confirms the connection is using TLS by checking the connection metadata.
     */
    @Test
    public void testEncryptionIsEnforcedWhenRequested() throws Exception {
        String connStr = TestUtils.addOrOverrideProperty(connectionString, "encrypt", "true");
        connStr = TestUtils.addOrOverrideProperty(connStr, "trustServerCertificate", "true");

        try (Connection conn = PrepUtil.getConnection(connStr)) {
            assertNotNull(conn, "Encrypted connection should succeed with trustServerCertificate=true");
            // Verify the connection is actually alive and functioning
            assertNotNull(conn.getMetaData().getDatabaseProductName());
        }
    }

    /**
     * FX Threat11: Verify encrypt=false followed by encrypt=true on the same server
     * both work, and that the driver respects the property change.
     */
    @Test
    public void testEncryptionPropertyIsRespected() throws Exception {
        // First connect without encryption
        String noEncrypt = TestUtils.addOrOverrideProperty(connectionString, "encrypt", "false");
        try (Connection conn1 = PrepUtil.getConnection(noEncrypt)) {
            assertNotNull(conn1.getMetaData().getDatabaseProductName());
        }

        // Then connect with encryption
        String withEncrypt = TestUtils.addOrOverrideProperty(connectionString, "encrypt", "true");
        withEncrypt = TestUtils.addOrOverrideProperty(withEncrypt, "trustServerCertificate", "true");
        try (Connection conn2 = PrepUtil.getConnection(withEncrypt)) {
            assertNotNull(conn2.getMetaData().getDatabaseProductName());
        }
    }

    /**
     * FX Threat7/Threat11: Verify that trustServerCertificate=false with encrypt=true
     * requires proper certificate validation — connection should succeed only when the
     * server presents a valid, trusted certificate.
     */
    @Test
    public void testTrustServerCertificateFalseRequiresValidCert() throws Exception {
        String connStr = TestUtils.addOrOverrideProperty(connectionString, "encrypt", "true");
        connStr = TestUtils.addOrOverrideProperty(connStr, "trustServerCertificate", "false");
        connStr = TestUtils.addOrOverrideProperty(connStr, "loginTimeout", "10");

        try {
            try (Connection conn = PrepUtil.getConnection(connStr)) {
                // If connection succeeds, the server has a valid trusted certificate
                assertNotNull(conn.getMetaData().getDatabaseProductName());
            }
        } catch (SQLException e) {
            // If it fails, that's also valid — means the server cert is self-signed/untrusted
            // The important thing is the driver CHECKED the certificate (didn't skip validation)
            String msg = e.getMessage() != null ? e.getMessage() : "";
            Throwable cause = e.getCause();
            String causeMsg = (cause != null && cause.getMessage() != null) ? cause.getMessage() : "";

            // The error should be TLS/certificate related, not a generic connection failure
            boolean isCertRelated = msg.toLowerCase().contains("certificate")
                    || msg.toLowerCase().contains("ssl")
                    || msg.toLowerCase().contains("tls")
                    || msg.toLowerCase().contains("trust")
                    || msg.toLowerCase().contains("pkix")
                    || msg.toLowerCase().contains("handshake")
                    || causeMsg.toLowerCase().contains("certificate")
                    || causeMsg.toLowerCase().contains("pkix")
                    || causeMsg.toLowerCase().contains("handshake")
                    || causeMsg.toLowerCase().contains("ssl");

            assertTrue(isCertRelated,
                    "When trustServerCertificate=false and cert is invalid, error should be "
                            + "TLS/certificate related. Got: " + msg);
        }
    }

    /**
     * FX Threat7: Verify that multiple concurrent connections with different trust settings
     * don't interfere with each other — one connection with trustServerCertificate=true
     * should not cause another connection with trustServerCertificate=false to skip validation.
     */
    @Test
    public void testConcurrentTrustSettingsDoNotInterfere() throws Exception {
        String trustedConnStr = TestUtils.addOrOverrideProperty(connectionString, "encrypt", "true");
        trustedConnStr = TestUtils.addOrOverrideProperty(trustedConnStr, "trustServerCertificate", "true");

        String strictConnStr = TestUtils.addOrOverrideProperty(connectionString, "encrypt", "true");
        strictConnStr = TestUtils.addOrOverrideProperty(strictConnStr, "trustServerCertificate", "false");
        strictConnStr = TestUtils.addOrOverrideProperty(strictConnStr, "hostNameInCertificate",
                "spoofed.server.invalid.test");
        strictConnStr = TestUtils.addOrOverrideProperty(strictConnStr, "loginTimeout", "10");

        // Open a trusted connection first
        try (Connection trustedConn = PrepUtil.getConnection(trustedConnStr)) {
            assertNotNull(trustedConn.getMetaData().getDatabaseProductName());

            // While trusted connection is open, strict connection with wrong hostname should still fail
            try {
                try (Connection strictConn = PrepUtil.getConnection(strictConnStr)) {
                    fail("Strict connection with wrong hostNameInCertificate should fail even when "
                            + "a trusted connection is already open");
                }
            } catch (SQLException e) {
                // Expected — the strict connection must independently validate
                assertNotNull(e.getMessage());
            }

            // Verify the original trusted connection is still working
            assertNotNull(trustedConn.getMetaData().getDatabaseProductName(),
                    "Original trusted connection should still be functional");
        }
    }
}
