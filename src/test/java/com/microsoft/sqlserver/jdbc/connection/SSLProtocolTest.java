/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.StringUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;

/**
 * Tests new connection property sslProtocol
 */
@RunWith(JUnitPlatform.class)
public class SSLProtocolTest extends AbstractTest {

    Connection con = null;
    Statement stmt = null;

    /**
     * Connect with supported protocol
     * 
     * @param sslProtocol
     * @throws Exception
     */
    public void testWithSupportedProtocols(String sslProtocol) throws Exception {
        String url = connectionString + ";sslProtocol=" + sslProtocol;
        try {
            con = DriverManager.getConnection(url);
            DatabaseMetaData dbmd = con.getMetaData();
            assertNotNull(dbmd);
            assertTrue(!StringUtils.isEmpty(dbmd.getDatabaseProductName()));
        }
        catch (SQLServerException e) {
            // Some older versions of SQLServer might not have all the TLS protocol versions enabled.
            // Example, if the highest TLS version enabled in the server is TLSv1.1,
            // the connection will fail if we enable only TLSv1.2
            assertTrue(e.getMessage().contains("protocol version is not enabled or not supported by the client."));
        }
    }


    /**
     * Connect with unsupported protocol
     * 
     * @param sslProtocol
     * @throws Exception
     */
    public void testWithUnSupportedProtocols(String sslProtocol) throws Exception {
        try {
            String url = connectionString + ";sslProtocol=" + sslProtocol;
            con = DriverManager.getConnection(url);
            assertFalse(true, "Any protocol other than TLSv1, TLSv1.1, and TLSv1.2 should throw Exception");
        }
        catch (SQLServerException e) {
            assertTrue(true, "Should throw exception");
            String errMsg = "SSL Protocol " + sslProtocol + " label is not valid. Only TLS, TLSv1, TLSv1.1, and TLSv1.2 are supported.";
            assertTrue(errMsg.equals(e.getMessage()), "Message should be from SQL Server resources : " + e.getMessage());
        }
    }

    /**
     * Test with unsupported protocols.
     * 
     * @throws Exception
     */
    @Test
    public void testConnectWithWrongProtocols() throws Exception {
        String[] wrongProtocols = {"SSLv1111", "SSLv2222", "SSLv3111", "SSLv2Hello1111", "TLSv1.11", "TLSv2.4", "random"};
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

