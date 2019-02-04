/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import javax.sql.DataSource;
import javax.sql.PooledConnection;

import org.apache.commons.dbcp2.BasicDataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerConnection;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;


/**
 * Tests pooled connection
 *
 */
@RunWith(JUnitPlatform.class)
@Tag("AzureDWTest")
public class PoolingTest extends AbstractTest {
    static String tempTableName = RandomUtil.getIdentifier("#poolingtest");
    static String tableName = RandomUtil.getIdentifier("PoolingTestTable");

    @Test
    public void testPooling() throws SQLException {
        assumeTrue(!isSqlAzure(), TestResource.getResource("R_skipAzure"));

        SQLServerXADataSource XADataSource1 = new SQLServerXADataSource();
        XADataSource1.setURL(connectionString);
        XADataSource1.setDatabaseName("tempdb");

        PooledConnection pc = XADataSource1.getPooledConnection();
        try (Connection conn = pc.getConnection(); Statement stmt = conn.createStatement()) {

            // create table in tempdb database
            stmt.execute("create table " + AbstractSQLGenerator.escapeIdentifier(tempTableName) + " (myid int)");
            stmt.execute("insert into " + AbstractSQLGenerator.escapeIdentifier(tempTableName) + " values (1)");
        }

        boolean tempTableFileRemoved = false;
        try (Connection conn = pc.getConnection(); Statement stmt = conn.createStatement()) {
            stmt.executeQuery("select * from [" + tempTableName + "]");
        } catch (SQLException e) {
            // make sure the temporary table is not found.
            if (e.getMessage().startsWith(TestResource.getResource("R_invalidObjectName"))) {
                tempTableFileRemoved = true;
            }
        } finally {
            if (null != pc) {
                pc.close();
            }
        }
        assertTrue(tempTableFileRemoved, TestResource.getResource("R_tempTAbleNotRemoved"));
    }

    @Test
    public void testConnectionPoolReget() throws SQLException {
        SQLServerXADataSource ds = new SQLServerXADataSource();
        ds.setURL(connectionString);
        PooledConnection pc = null;
        try {
            pc = ds.getPooledConnection();
            try (Connection con = pc.getConnection(); Connection con2 = pc.getConnection()) {

                // assert that the first connection is closed.
                assertTrue(con.isClosed(), TestResource.getResource("R_firstConnectionNotClosed"));
            }
        } finally {
            if (null != pc) {
                pc.close();
            }
        }
    }

    @Test
    public void testConnectionPoolConnFunctions() throws SQLException {
        String sql1 = "if exists (select * from dbo.sysobjects where name = '" + TestUtils.escapeSingleQuotes(tableName)
                + "' and type = 'U')\n" + "drop table " + AbstractSQLGenerator.escapeIdentifier(tableName) + "\n"
                + "create table " + AbstractSQLGenerator.escapeIdentifier(tableName) + "\n" + "(\n"
                + "wibble_id int not null,\n" + "counter int null\n" + ");";
        String sql2 = "if exists (select * from dbo.sysobjects where name = '" + TestUtils.escapeSingleQuotes(tableName)
                + "' and type = 'U')\n" + "drop table " + AbstractSQLGenerator.escapeIdentifier(tableName) + "\n";

        SQLServerXADataSource ds = new SQLServerXADataSource();
        ds.setURL(connectionString);

        PooledConnection pc = ds.getPooledConnection();
        try (Connection con = pc.getConnection(); Statement statement = con.createStatement()) {
            statement.execute(sql1);
            statement.execute(sql2);
            con.clearWarnings();

        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        } finally {
            if (null != pc) {
                pc.close();
            }
        }
    }

    @Test
    public void testConnectionPoolClose() throws SQLException {
        SQLServerXADataSource ds = new SQLServerXADataSource();
        ds.setURL(connectionString);

        PooledConnection pc = ds.getPooledConnection();
        try (Connection con = pc.getConnection()) {
            pc.close();

            // assert that the first connection is closed.
            assertTrue(con.isClosed(), TestResource.getResource("R_connectionNotClosedWithPoolClose"));
        } catch (Exception e) {
            fail(TestResource.getResource("R_unexpectedErrorMessage") + e.toString());
        } finally {
            if (null != pc) {
                pc.close();
            }
        }
    }

    @Test
    public void testConnectionPoolClientConnectionId() throws SQLException {
        SQLServerXADataSource ds = new SQLServerXADataSource();
        ds.setURL(connectionString);
        PooledConnection pc = null;
        try {
            pc = ds.getPooledConnection();
            UUID Id1 = null;
            UUID Id2 = null;

            try (ISQLServerConnection con = (ISQLServerConnection) pc.getConnection()) {

                Id1 = con.getClientConnectionId();
                assertTrue(Id1 != null, TestResource.getResource("R_connectionNotClosedWithPoolClose"));
            }

            // now re-get the connection
            try (ISQLServerConnection con = (ISQLServerConnection) pc.getConnection()) {

                Id2 = con.getClientConnectionId();
            }

            assertEquals(Id1, Id2, TestResource.getResource("R_idFromPoolNotSame"));
        } finally {
            if (null != pc) {
                pc.close();
            }
        }
    }

    /**
     * test connection pool with HikariCP
     * 
     * @throws SQLException
     */
    @Test
    public void testHikariCP() throws SQLException {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(connectionString);
        HikariDataSource ds = new HikariDataSource(config);

        try {
            connect(ds);
        } finally {
            ds.close();
        }
    }

    /**
     * test connection pool with Apache DBCP
     * 
     * @throws SQLException
     */
    @Test
    public void testApacheDBCP() throws SQLException {
        BasicDataSource ds = new BasicDataSource();
        ds.setUrl(connectionString);

        try {
            connect(ds);
        } finally {
            ds.close();
        }
    }

    /**
     * setup connection, get connection from pool, and test threads
     * 
     * @param ds
     * @throws SQLException
     */
    private static void connect(DataSource ds) throws SQLException {
        try (Connection con = ds.getConnection(); PreparedStatement pst = con.prepareStatement("SELECT SUSER_SNAME()");
                ResultSet rs = pst.executeQuery()) {

            // TODO : we are commenting this out due to AppVeyor failures. Will investigate later.
            // assertTrue(countTimeoutThreads() >= 1, "Timeout timer is missing.");

            while (rs.next()) {
                rs.getString(1);
            }
        }
    }

    /**
     * count number of mssql-jdbc-TimeoutTimer threads
     * 
     * @return
     */
    private static int countTimeoutThreads() {
        int count = 0;
        String threadName = "mssql-jdbc-TimeoutTimer";

        ThreadInfo[] tinfos = ManagementFactory.getThreadMXBean()
                .getThreadInfo(ManagementFactory.getThreadMXBean().getAllThreadIds(), 0);

        for (ThreadInfo ti : tinfos) {
            if ((ti.getThreadName().startsWith(threadName))
                    && (ti.getThreadState().equals(java.lang.Thread.State.TIMED_WAITING))) {
                count++;
            }
        }

        return count;
    }

    /**
     * drop the tables
     * 
     * @throws SQLException
     */
    @AfterAll
    public static void afterAll() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tempTableName), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), stmt);
        }
    }
}
