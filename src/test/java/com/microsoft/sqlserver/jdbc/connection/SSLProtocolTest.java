/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.text.MessageFormat;

import com.microsoft.sqlserver.jdbc.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.StringUtils;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Tests new connection property sslProtocol
 */
@RunWith(JUnitPlatform.class)
public class SSLProtocolTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    /**
     * Connect with supported protocol
     * 
     * @param sslProtocol
     * @throws Exception
     */
    public void testWithSupportedProtocols(String sslProtocol) throws Exception {
        String url = connectionString + ";sslProtocol=" + sslProtocol;
        try (Connection con = PrepUtil.getConnection(url)) {
            DatabaseMetaData dbmd = con.getMetaData();
            assertNotNull(dbmd);
            assertTrue(!StringUtils.isEmpty(dbmd.getDatabaseProductName()));
        } catch (SQLServerException e) {
            // Some older versions of SQLServer might not have all the TLS protocol versions enabled.
            // Example, if the highest TLS version enabled in the server is TLSv1.1,
            // the connection will fail if we enable only TLSv1.2
            String errorMsg = e.getMessage();
            Throwable cause = e.getCause();
            assertTrue(
                    errorMsg.contains(TestResource.getResource("R_protocolNotSupported"))
                            || errorMsg.contains(TestResource.getResource("R_protocolInappropriate"))
                            || null != cause && null != cause.getMessage()
                                    && cause.getMessage().contains(TestResource.getResource("R_connectionClosed"))
                            || null != cause.getCause() && null != cause.getCause().getMessage() && cause.getCause()
                                    .getMessage().contains(TestResource.getResource("R_connectionClosed")),
                    e.getMessage());
        }
    }

    /**
     * Connect with unsupported protocol
     * 
     * @param sslProtocol
     * @throws Exception
     */
    public void testWithUnSupportedProtocols(String sslProtocol) throws Exception {
        String url = connectionString + ";sslProtocol=" + sslProtocol;
        try (Connection con = PrepUtil.getConnection(url)) {
            assertFalse(true, TestResource.getResource("R_protocolVersion"));
        } catch (SQLServerException e) {
            assertTrue(true, TestResource.getResource("R_shouldThrowException"));
            MessageFormat form = new MessageFormat(TestResource.getResource("R_invalidProtocolLabel"));
            Object[] msgArgs = {sslProtocol};
            String errMsg = form.format(msgArgs);
            assertTrue(errMsg.equals(e.getMessage()),
                    TestResource.getResource("R_SQLServerResourceMessage") + e.getMessage());
        }
    }

    /**
     * Test with unsupported protocols.
     * 
     * @throws Exception
     */
    @Test
    public void testConnectWithWrongProtocols() throws Exception {
        String[] wrongProtocols = {"SSLv1111", "SSLv2222", "SSLv3111", "SSLv2Hello1111", "TLSv1.11", "TLSv2.4",
                "random"};
        for (String wrongProtocol : wrongProtocols) {
            testWithUnSupportedProtocols(wrongProtocol);
        }
    }

    /**
     * Test with supported protocols.
     * 
     * @throws Exception
     */
    @Test
    public void testConnectWithSupportedProtocols() throws Exception {
        String[] supportedProtocols = {"TLS", "TLSv1", "TLSv1.1", "TLSv1.2"};
        for (String supportedProtocol : supportedProtocols) {
            testWithSupportedProtocols(supportedProtocol);
        }
    }
}
