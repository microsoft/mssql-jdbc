/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;
import com.microsoft.sqlserver.testframework.AbstractTest;

@RunWith(JUnitPlatform.class)
public class NativeMSSQLDataSourceTest extends AbstractTest {

    @Test
    public void testNativeMSSQLDataSource() throws SQLException {
        SQLServerXADataSource ds = new SQLServerXADataSource();
        ds.setLastUpdateCount(true);
        assertTrue(ds.getLastUpdateCount());
    }

    @Test
    public void testSerialization() throws IOException {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        	 ObjectOutput objectOutput = new ObjectOutputStream(outputStream)) {
	        SQLServerDataSource ds = new SQLServerDataSource();
	        ds.setLogWriter(new PrintWriter(new ByteArrayOutputStream()));
	
	        objectOutput.writeObject(ds);
	        objectOutput.flush();
        }
    }

    @Test
    public void testDSNormal() throws ClassNotFoundException, IOException, SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        try (Connection conn = ds.getConnection()) {}
        ds = testSerial(ds);
        try (Connection conn = ds.getConnection()) {}
    }

    @Test
    public void testDSTSPassword() throws ClassNotFoundException, IOException, SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        System.setProperty("java.net.preferIPv6Addresses", "true");
        ds.setURL(connectionString);
        ds.setTrustStorePassword("wrong_password");
        try (Connection conn = ds.getConnection()) {}
        ds = testSerial(ds);
        try (Connection conn = ds.getConnection()) {}
        catch (SQLException e) {
            assertEquals("The DataSource trustStore password needs to be set.", e.getMessage());
        }
    }

    @Test
    public void testInterfaceWrapping() throws ClassNotFoundException, SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        assertEquals(true, ds.isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerDataSource")));
        assertEquals(true, ds.isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDataSource")));
        assertEquals(true, ds.isWrapperFor(Class.forName("javax.sql.CommonDataSource")));
        ISQLServerDataSource ids = (ISQLServerDataSource) (ds.unwrap(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerDataSource")));
        ids.setApplicationName("AppName");

        SQLServerConnectionPoolDataSource poolDS = new SQLServerConnectionPoolDataSource();
        assertEquals(true, poolDS.isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerDataSource")));
        assertEquals(true, poolDS.isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDataSource")));
        assertEquals(true, poolDS.isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource")));
        assertEquals(true, poolDS.isWrapperFor(Class.forName("javax.sql.CommonDataSource")));
        ISQLServerDataSource ids2 = (ISQLServerDataSource) (poolDS.unwrap(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerDataSource")));
        ids2.setApplicationName("AppName");

        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        assertEquals(true, xaDS.isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerDataSource")));
        assertEquals(true, xaDS.isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDataSource")));
        assertEquals(true, xaDS.isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource")));
        assertEquals(true, xaDS.isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.SQLServerXADataSource")));
        assertEquals(true, xaDS.isWrapperFor(Class.forName("javax.sql.CommonDataSource")));
        ISQLServerDataSource ids3 = (ISQLServerDataSource) (xaDS.unwrap(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerDataSource")));
        ids3.setApplicationName("AppName");
    }

    private SQLServerDataSource testSerial(SQLServerDataSource ds) throws IOException, ClassNotFoundException {
        try (java.io.ByteArrayOutputStream outputStream = new java.io.ByteArrayOutputStream();
        	 java.io.ObjectOutput objectOutput = new java.io.ObjectOutputStream(outputStream)) {
	        objectOutput.writeObject(ds);
	        objectOutput.flush();
	        
	        try (ObjectInputStream in = new ObjectInputStream(new ByteArrayInputStream(outputStream.toByteArray()))) {
		        SQLServerDataSource dtn;
		        dtn = (SQLServerDataSource) in.readObject();
		        return dtn;
	        }
        }
    }
}
