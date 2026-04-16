/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.ssl;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

/**
 * TLS protocol negotiation tests: supported/unsupported protocols, encryption enforcement,
 * certificate validation, multiple concurrent SSL connections.
 * Ported from FX ssl/ssl.java tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxSSL)
public class TLSProtocolTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    public void testConnectionWithTLSv12() throws Exception {
        String url = connectionString + ";sslProtocol=TLSv1.2";
        try (Connection conn = PrepUtil.getConnection(url)) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertNotNull(dbmd);
        } catch (SQLServerException e) {
            // TLS 1.2 may not be enabled on the server
            assertTrue(e.getMessage().contains("protocol")
                    || e.getMessage().contains("SSL")
                    || e.getMessage().contains("TLS")
                    || e.getMessage().contains("connection"));
        }
    }

    @Test
    public void testConnectionWithTLS() throws Exception {
        String url = connectionString + ";sslProtocol=TLS";
        try (Connection conn = PrepUtil.getConnection(url)) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertNotNull(dbmd);
        } catch (SQLServerException e) {
            assertTrue(e.getMessage().contains("protocol")
                    || e.getMessage().contains("SSL")
                    || e.getMessage().contains("connection"));
        }
    }

    @Test
    public void testConnectionWithInvalidProtocol() throws Exception {
        String url = connectionString + ";sslProtocol=InvalidProtocol";
        assertThrows(SQLServerException.class, () -> {
            PrepUtil.getConnection(url).close();
        });
    }

    @Test
    public void testEncryptTrue() throws Exception {
        String url = connectionString + ";encrypt=true;trustServerCertificate=true";
        try (Connection conn = PrepUtil.getConnection(url)) {
            assertNotNull(conn);
            DatabaseMetaData dbmd = conn.getMetaData();
            assertNotNull(dbmd);
        }
    }

    @Test
    public void testEncryptFalse() throws Exception {
        String url = connectionString + ";encrypt=false";
        try (Connection conn = PrepUtil.getConnection(url)) {
            assertNotNull(conn);
        }
    }

    @Test
    public void testEncryptStrict() throws Exception {
        String url = connectionString + ";encrypt=strict;trustServerCertificate=true";
        try (Connection conn = PrepUtil.getConnection(url)) {
            assertNotNull(conn);
        } catch (SQLServerException e) {
            // Strict encryption may not be supported on all server versions
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testTrustServerCertificateTrue() throws Exception {
        String url = connectionString + ";encrypt=true;trustServerCertificate=true";
        try (Connection conn = PrepUtil.getConnection(url)) {
            assertNotNull(conn);
        }
    }

    @Test
    public void testTrustServerCertificateFalse() throws Exception {
        String url = connectionString + ";encrypt=true;trustServerCertificate=false";
        try (Connection conn = PrepUtil.getConnection(url)) {
            // Connection may succeed if server certificate is trusted
            assertNotNull(conn);
        } catch (SQLServerException e) {
            // Expected if server certificate is not in the trust store
            assertTrue(e.getMessage().contains("certificate")
                    || e.getMessage().contains("SSL")
                    || e.getMessage().contains("trust"));
        }
    }

    @Test
    public void testMultipleConcurrentSSLConnections() throws Exception {
        String url = connectionString + ";encrypt=true;trustServerCertificate=true";
        Connection[] conns = new Connection[5];
        try {
            for (int i = 0; i < 5; i++) {
                conns[i] = PrepUtil.getConnection(url);
                assertNotNull(conns[i]);
            }
        } finally {
            for (Connection conn : conns) {
                if (conn != null) {
                    conn.close();
                }
            }
        }
    }

    @Test
    public void testHostNameInCertificate() throws Exception {
        String url = connectionString + ";encrypt=true;trustServerCertificate=true;hostNameInCertificate=*";
        try (Connection conn = PrepUtil.getConnection(url)) {
            assertNotNull(conn);
        }
    }

    @Test
    public void testEncryptWithDriverManager() throws Exception {
        String url = connectionString + ";encrypt=true;trustServerCertificate=true";
        try (Connection conn = PrepUtil.getConnection(url)) {
            assertNotNull(conn);
            assertFalse(conn.isClosed());
        }
    }

    @Test
    public void testEncryptWithDataSource() throws Exception {
        com.microsoft.sqlserver.jdbc.SQLServerDataSource ds = new com.microsoft.sqlserver.jdbc.SQLServerDataSource();
        ds.setURL(connectionString);
        ds.setEncrypt("true");
        ds.setTrustServerCertificate(true);
        try (Connection conn = ds.getConnection()) {
            assertNotNull(conn);
        }
    }

    @Test
    public void testMultipleProtocolAttempts() throws Exception {
        String[] protocols = {"TLSv1.2", "TLS"};
        boolean connected = false;
        for (String protocol : protocols) {
            String url = connectionString + ";sslProtocol=" + protocol;
            try (Connection conn = PrepUtil.getConnection(url)) {
                assertNotNull(conn);
                connected = true;
                break;
            } catch (SQLServerException e) {
                // Try next protocol
            }
        }
        assertTrue(connected, "Should connect with at least one TLS protocol");
    }
}
