/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.Statement;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.StringUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;

/**
 * This unit test case targeted to test new functionality of configuring SSLProtocol for connecting to SQL Server.  
 */
@RunWith(JUnitPlatform.class)
public class TestSSLProtocol extends AbstractTest {

    Connection con = null;
    Statement stmt = null;

    /**
     * 
     * @param sslProtocol
     * @throws Exception
     */
    @DisplayName("TestForSupportedProtocols") 
    @ParameterizedTest
    @ValueSource(strings={"TLS","TLSv1.1","TLSv1.2"}) 
    public void testSuportedProtocols(String sslProtocol) throws Exception {
        String url = connectionString + ";sslProtocol=" + sslProtocol;
        con = DriverManager.getConnection(url);
        DatabaseMetaData dbmd = con.getMetaData();
        assertNotNull(dbmd);
        assertTrue(!StringUtils.isEmpty(dbmd.getDatabaseProductName()));
    }

    /**
     * Connect with valid but not supported protocols
     * @param sslProtocol
     * @throws Exception
     */
    @DisplayName("TestForNotSupportedProtocols") 
    @ParameterizedTest
    @ValueSource(strings={"SSLv1","SSLv2","SSLv3", "SSLv2Hello","SSL"}) 
    public void testWithUnSupportedProtocols(String sslProtocol) throws Exception {
        try {
        String url = connectionString + ";sslProtocol=" + sslProtocol;
        con = DriverManager.getConnection(url);
        assertFalse(true, "Any protocol other than TLS, TLSv1.1 & TLSv1.2 should throw Exception");
        }catch(SQLServerException e) {
            assertTrue(true,"Should throw exception");
            String errMsg = "SSL Protocol "+ sslProtocol +" is not valid. Supporting only TLS, TLSv1.1 & TLSv1.2.";
            
            assertTrue(errMsg.equals(e.getMessage()),"Message should be from SQL Server resources : " + e.getMessage());
        }
    }

    /**
     * Test with wrong values.
     * @param sslProtocol
     * @throws Exception
     */
    @DisplayName("TestForWrongValues") 
    @ParameterizedTest
    @ValueSource(strings={"SSLv1111","SSLv2222","SSLv3111", "SSLv2Hello1111","TLSv1.11","TLSv2.4","HTTPS"}) 
    public void testConnectWithWrongProtocol(String sslProtocol) throws Exception {
        testWithUnSupportedProtocols(sslProtocol); 
    }
}
