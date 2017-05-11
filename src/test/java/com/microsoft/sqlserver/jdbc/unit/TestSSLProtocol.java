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

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.StringUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;

/**
 * 
 */
@RunWith(JUnitPlatform.class)
public class TestSSLProtocol extends AbstractTest {

    Connection con = null;
    Statement stmt = null;

    /**
     * TODO: add @ParameterizedTest() with @ValueSource(strings={"TLS", "TLSv1.1"})
     * 
     * @throws Exception
     */
    @Test
    public void testConnectWithTLS1_2() throws Exception {
        String url = connectionString + ";sslProtocol=TLSv1.2";
        con = DriverManager.getConnection(url);
        DatabaseMetaData dbmd = con.getMetaData();
        assertNotNull(dbmd);
        assertTrue(!StringUtils.isEmpty(dbmd.getDatabaseProductName()));
    }

    @Test
    public void testConnectWithTLS1_1() throws Exception {
        String url = connectionString + ";sslProtocol=TLSv1.1";
        con = DriverManager.getConnection(url);
        DatabaseMetaData dbmd = con.getMetaData();
        assertNotNull(dbmd);
        assertTrue(!StringUtils.isEmpty(dbmd.getDatabaseProductName()));
    }

    @Test
    public void testConnectWithSSL() throws Exception {
        try {
        String url = connectionString + ";sslProtocol=SSLv3";
        con = DriverManager.getConnection(url);
        assertFalse(true, "With Garbage Protocol you should not see this");
        }catch(Exception e) {
            assertTrue(true,"Should throw exception");
        }
    }

    /**
     * Wrong version number used.
     * @throws Exception
     */
    @Test
    public void testConnectWithWrongProtocol() throws Exception {
        try {
            String url = connectionString + ";sslProtocol=TLSv1.11";
            con = DriverManager.getConnection(url);
            assertFalse(true, "With Garbage Protocol you should not see this");
        }
        catch (SQLServerException e) {
            assertTrue("SSL Protocol TLSv1.11 is not valid. Supporting only TLS, TLSv1.1 & TLSv1.2.".equals(e.getMessage()),"Message should be from SQL Server resources");
        }
    }

}
