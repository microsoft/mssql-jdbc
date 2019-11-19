/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
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

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


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
            PrintWriter out = new PrintWriter(new ByteArrayOutputStream());
            ds.setLogWriter(out);
            objectOutput.writeObject(ds);
            objectOutput.flush();

            assertTrue(out == ds.getLogWriter());
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
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.xAzureSQLDB)
    public void testDSTSPassword() throws ClassNotFoundException, IOException, SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        System.setProperty("java.net.preferIPv6Addresses", Boolean.TRUE.toString());
        ds.setURL(connectionString);
        ds.setTrustStorePassword("wrong_password");
        try (Connection conn = ds.getConnection()) {}
        ds = testSerial(ds);
        try (Connection conn = ds.getConnection()) {} catch (SQLException e) {
            assertEquals(TestResource.getResource("R_trustStorePasswordNotSet"), e.getMessage());
        }
    }

    @Test
    public void testInterfaceWrapping() throws ClassNotFoundException, SQLException {
        SQLServerDataSource ds = new SQLServerDataSource();
        assertEquals(true, ds.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".ISQLServerDataSource")));
        assertEquals(true, ds.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".SQLServerDataSource")));
        assertEquals(true, ds.isWrapperFor(Class.forName("javax.sql.CommonDataSource")));
        ISQLServerDataSource ids = (ISQLServerDataSource) (ds
                .unwrap(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".ISQLServerDataSource")));
        ids.setApplicationName("AppName");

        SQLServerConnectionPoolDataSource poolDS = new SQLServerConnectionPoolDataSource();
        assertEquals(true, poolDS.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".ISQLServerDataSource")));
        assertEquals(true, poolDS.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".SQLServerDataSource")));
        assertEquals(true, poolDS
                .isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".SQLServerConnectionPoolDataSource")));
        assertEquals(true, poolDS.isWrapperFor(Class.forName("javax.sql.CommonDataSource")));
        ISQLServerDataSource ids2 = (ISQLServerDataSource) (poolDS
                .unwrap(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".ISQLServerDataSource")));
        ids2.setApplicationName("AppName");

        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        assertEquals(true, xaDS.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".ISQLServerDataSource")));
        assertEquals(true, xaDS.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".SQLServerDataSource")));
        assertEquals(true,
                xaDS.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".SQLServerConnectionPoolDataSource")));
        assertEquals(true, xaDS.isWrapperFor(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".SQLServerXADataSource")));
        assertEquals(true, xaDS.isWrapperFor(Class.forName("javax.sql.CommonDataSource")));
        ISQLServerDataSource ids3 = (ISQLServerDataSource) (xaDS
                .unwrap(Class.forName(Constants.MSSQL_JDBC_PACKAGE + ".ISQLServerDataSource")));
        ids3.setApplicationName("AppName");
    }

    @Test
    public void testDSReference() {
        SQLServerDataSource ds = new SQLServerDataSource();
        assertTrue(ds.getReference().getClassName()
                .equals("com.microsoft.sqlserver.jdbc.SQLServerDataSource"));
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
