/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.fedauth;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import javax.sql.PooledConnection;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.Fedauth)
public class PooledConnectionTest extends FedauthCommon {

    static String charTable = TestUtils.escapeSingleQuotes(
            AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("JDBC_PooledConnection")));

    @Test
    public void testPooledConnectionAccessTokenExpiredThenReconnect() throws SQLException {
        testPooledConnectionAccessTokenExpiredThenReconnect((long) 5 * 60); // suspend 5 mins
        getFedauthInfo();

        // suspend until access token expires
        testPooledConnectionAccessTokenExpiredThenReconnect(secondsBeforeExpiration);
    }

    private void testPooledConnectionAccessTokenExpiredThenReconnect(long testingTimeInSeconds) throws SQLException {
        try {
            SQLServerConnectionPoolDataSource cpds = new SQLServerConnectionPoolDataSource();

            if (enableADIntegrated) {
                cpds.setServerName(azureServer);
                cpds.setDatabaseName(azureDatabase);
                cpds.setAuthentication("ActiveDirectoryIntegrated");
                cpds.setHostNameInCertificate(hostNameInCertificate);
            } else {
                cpds.setServerName(azureServer);
                cpds.setDatabaseName(azureDatabase);
                cpds.setUser(azureUserName);
                cpds.setPassword(azurePassword);
                cpds.setAuthentication("ActiveDirectoryPassword");
                cpds.setHostNameInCertificate(hostNameInCertificate);
            }

            // create pooled connection
            PooledConnection pc = cpds.getPooledConnection();

            // get first connection from pool
            try (Connection connection1 = pc.getConnection()) {
                try (Statement stmt = connection1.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT SUSER_SNAME()")) {
                    rs.next();

                    String retrievedUserName = rs.getString(1);
                    assertTrue(retrievedUserName.equals(azureUserName));

                    if (!enableADIntegrated) {
                        try {
                            TestUtils.dropTableIfExists(charTable, stmt);
                            createTable(stmt, charTable);
                            populateCharTable(connection1, charTable);
                            testChar(stmt, charTable);
                        } finally {
                            TestUtils.dropTableIfExists(charTable, stmt);
                        }
                    }
                }
            }
            Thread.sleep(TimeUnit.SECONDS.toMillis(testingTimeInSeconds));
            Thread.sleep(TimeUnit.SECONDS.toMillis(2)); // give 2 mins more to make sure the access token is expired.

            // get second connection from pool
            try (Connection connection2 = pc.getConnection()) {
                try (Statement stmt = connection2.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT SUSER_SNAME()")) {
                    rs.next();

                    String retrievedUserName = rs.getString(1);
                    assertTrue(retrievedUserName.equals(azureUserName));

                    if (!enableADIntegrated) {
                        try {
                            TestUtils.dropTableIfExists(charTable, stmt);
                            createTable(stmt, charTable);
                            populateCharTable(connection2, charTable);
                            testChar(stmt, charTable);
                        } finally {
                            TestUtils.dropTableIfExists(charTable, stmt);
                        }
                    }
                }
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testPooledConnectionMultiThread() throws SQLException {
        getFedauthInfo();
        testPooledConnectionMultiThread(secondsBeforeExpiration);
    }

    private void testPooledConnectionMultiThread(long testingTimeInSeconds) throws SQLException {
        try {
            SQLServerConnectionPoolDataSource cpds = new SQLServerConnectionPoolDataSource();

            if (enableADIntegrated) {
                cpds.setServerName(azureServer);
                cpds.setDatabaseName(azureDatabase);
                cpds.setAuthentication("ActiveDirectoryIntegrated");
                cpds.setHostNameInCertificate(hostNameInCertificate);
            } else {
                cpds.setServerName(azureServer);
                cpds.setDatabaseName(azureDatabase);
                cpds.setUser(azureUserName);
                cpds.setPassword(azurePassword);
                cpds.setAuthentication("ActiveDirectoryPassword");
                cpds.setHostNameInCertificate(hostNameInCertificate);
            }

            // create pooled connection
            final PooledConnection pc = cpds.getPooledConnection();

            // get first connection from pool
            try (Connection connection1 = pc.getConnection()) {
                try (Statement stmt = connection1.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT SUSER_SNAME()")) {
                    rs.next();

                    String retrievedUserName = rs.getString(1);
                    assertTrue(retrievedUserName.equals(azureUserName));
                }
            }
            Thread.sleep(TimeUnit.SECONDS.toMillis(testingTimeInSeconds));
            Thread.sleep(TimeUnit.SECONDS.toMillis(2)); // give 2 mins more to make sure the access token is expired.

            Random rand = new Random();
            int numberOfThreadsForEachType = rand.nextInt(15) + 1; // 1 to 15

            for (int i = 0; i < numberOfThreadsForEachType; i++) {
                new Thread() {
                    public void run() {
                        try {
                            try (Connection connection2 = pc.getConnection();
                                    Statement st = connection2.createStatement();
                                    ResultSet rs = st.executeQuery("SELECT SUSER_SNAME()")) {
                                if (rs.next()) {
                                    String retrievedUserName = rs.getString(1);
                                    assertTrue(retrievedUserName.equals(azureUserName));
                                }
                            }
                        } catch (SQLException e) {
                            assertTrue(TestResource.getResource("R_invalidExceptionMessage") + ": " + e.getMessage(),
                                    e.getMessage().contains(TestResource.getResource("R_connectionClosed"))
                                            || e.getMessage().contains(TestResource.getResource("R_connectionIsClosed"))
                                            || e.getMessage().contains(TestResource.getResource("R_hasClosed"))
                                            || e.getMessage().contains(TestResource.getResource("R_hasBeenClosed")));
                        }
                    }
                }.start();

                new Thread() {
                    public void run() {
                        try {
                            try (Connection connection2 = pc.getConnection();
                                    Statement st = connection2.createStatement();
                                    ResultSet rs = st.executeQuery("SELECT SUSER_SNAME()")) {
                                if (rs.next()) {
                                    String retrievedUserName = rs.getString(1);
                                    assertTrue(retrievedUserName.equals(azureUserName));
                                }
                            }
                        } catch (SQLException e) {
                            assertTrue(TestResource.getResource("R_invalidExceptionMessage") + ": " + e.getMessage(),
                                    e.getMessage().contains(TestResource.getResource("R_connectionClosed"))
                                            || e.getMessage().contains(TestResource.getResource("R_connectionIsClosed"))
                                            || e.getMessage().contains(TestResource.getResource("R_hasClosed"))
                                            || e.getMessage().contains(TestResource.getResource("R_hasBeenClosed")));
                        }
                    }
                }.start();

                new Thread() {
                    public void run() {
                        try {
                            try (Connection connection2 = pc.getConnection();
                                    Statement st = connection2.createStatement();
                                    ResultSet rs = st.executeQuery("SELECT SUSER_SNAME()")) {
                                if (rs.next()) {
                                    String retrievedUserName = rs.getString(1);
                                    assertTrue(retrievedUserName.equals(azureUserName));
                                }
                            }
                        } catch (SQLException e) {
                            assertTrue(TestResource.getResource("R_invalidExceptionMessage") + ": " + e.getMessage(),
                                    e.getMessage().contains(TestResource.getResource("R_connectionClosed"))
                                            || e.getMessage().contains(TestResource.getResource("R_connectionIsClosed"))
                                            || e.getMessage().contains(TestResource.getResource("R_hasClosed"))
                                            || e.getMessage().contains(TestResource.getResource("R_hasBeenClosed")));
                        }
                    }
                }.start();
            }

            // sleep in order to catch exception from other threads if tests fail.
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(60));

            } catch (InterruptedException e) {
                fail(e.getMessage());
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    // suspend until access token expires
    @Test
    public void testPooledConnectionWithAccessToken() throws SQLException {
        getFedauthInfo();

        try {

            SQLServerConnectionPoolDataSource cpds = new SQLServerConnectionPoolDataSource();

            cpds.setServerName(azureServer);
            cpds.setDatabaseName(azureDatabase);
            cpds.setAccessToken(accessToken);
            cpds.setHostNameInCertificate(hostNameInCertificate);

            // create pooled connection
            final PooledConnection pc = cpds.getPooledConnection();

            // get first connection from pool
            try (Connection connection1 = pc.getConnection()) {
                try (Statement stmt = connection1.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT SUSER_SNAME()")) {
                    rs.next();

                    String retrievedUserName = rs.getString(1);
                    assertTrue(retrievedUserName.equals(azureUserName));
                }
            }

            Random rand = new Random();
            int numberOfThreadsForEachType = rand.nextInt(15) + 1; // 1 to 15
            for (int i = 0; i < numberOfThreadsForEachType; i++) {
                new Thread() {
                    public void run() {
                        try {
                            try (Connection connection2 = pc.getConnection();
                                    Statement st = connection2.createStatement();
                                    ResultSet rs = st.executeQuery("SELECT SUSER_SNAME()")) {
                                if (rs.next()) {
                                    String retrievedUserName = rs.getString(1);
                                    assertTrue(retrievedUserName.equals(azureUserName));
                                }
                            }
                        } catch (SQLException e) {
                            fail(e.getMessage());
                        }
                    }
                }.start();

                new Thread() {
                    public void run() {
                        try {
                            try (Connection connection2 = pc.getConnection();
                                    Statement st = connection2.createStatement();
                                    ResultSet rs = st.executeQuery("SELECT SUSER_SNAME()")) {
                                if (rs.next()) {
                                    String retrievedUserName = rs.getString(1);
                                    assertTrue(retrievedUserName.equals(azureUserName));
                                }
                            }
                        } catch (SQLException e) {
                            fail(e.getMessage());
                        }
                    }
                }.start();

                new Thread() {
                    public void run() {
                        try {
                            try (Connection connection2 = pc.getConnection();
                                    Statement st = connection2.createStatement();
                                    ResultSet rs = st.executeQuery("SELECT SUSER_SNAME()")) {
                                if (rs.next()) {
                                    String retrievedUserName = rs.getString(1);
                                    assertTrue(retrievedUserName.equals(azureUserName));
                                }
                            }
                        } catch (SQLException e) {
                            fail(e.getMessage());
                        }
                    }
                }.start();
            }

            // sleep in order to catch exception from other threads if tests fail.
            try {
                Thread.sleep(TimeUnit.SECONDS.toMillis(60));
            } catch (InterruptedException e) {
                fail(e.getMessage());
            }
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    private void createTable(Statement stmt, String charTable) throws SQLException {
        String createTableSql = "create table " + charTable + " (" + "PlainChar char(20) null,"
                + "PlainVarchar varchar(50) null," + "PlainVarcharMax varchar(max) null," + "PlainNchar nchar(30) null,"
                + "PlainNvarchar nvarchar(60) null," + "PlainNvarcharMax nvarchar(max) null" + ");";

        stmt.execute(createTableSql);
    }

    private void populateCharTable(Connection connection, String charTable) throws SQLException {
        String sql = "insert into " + charTable + " values( " + "?,?,?,?,?,?" + ")";

        try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
            for (int i = 1; i <= 6; i++) {
                pstmt.setString(i, "hello world!!!");
            }
            pstmt.execute();
        }
    }

    private void testChar(Statement stmt, String charTable) throws SQLException {

        try (ResultSet rs = stmt.executeQuery("select * from " + charTable)) {
            int numberOfColumns = rs.getMetaData().getColumnCount();
            rs.next();
            for (int i = 1; i <= numberOfColumns; i++) {
                try {} catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        }
    }

    @AfterAll
    public static void terminate() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(charTable, stmt);
        }
    }
}
