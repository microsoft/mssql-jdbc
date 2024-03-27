/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.resiliency;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

import javax.sql.PooledConnection;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerPooledConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@Tag(Constants.xSQLv11)
@Tag(Constants.xAzureSQLDW)
public class BasicConnectionTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    public void testBasicReconnectDefault() throws SQLException {
        basicReconnect(connectionString);
    }

    @Test
    @Tag(Constants.fedAuth)
    public void testBasicConnectionAAD() throws Exception {
        // retry since this could fail due to server throttling
        int retry = 1;
        while (retry <= THROTTLE_RETRY_COUNT) {
            try {
                String azureServer = getConfiguredProperty("azureServer");
                String azureDatabase = getConfiguredProperty("azureDatabase");
                String azureUserName = getConfiguredProperty("azureUserName");
                String azurePassword = getConfiguredProperty("azurePassword");
                org.junit.Assume.assumeTrue(azureServer != null && !azureServer.isEmpty());

                basicReconnect("jdbc:sqlserver://" + azureServer + ";database=" + azureDatabase + ";user="
                        + azureUserName + ";password=" + azurePassword
                        + ";loginTimeout=90;Authentication=ActiveDirectoryPassword");
                retry = THROTTLE_RETRY_COUNT + 1;
            } catch (Exception e) {
                if (e.getMessage().matches(TestUtils.formatErrorMsg("R_crClientAllRecoveryAttemptsFailed"))) {
                    System.out.println(e.getMessage() + ". Recovery failed, retry #" + retry + " in "
                            + THROTTLE_RETRY_INTERVAL + " ms");

                    Thread.sleep(THROTTLE_RETRY_INTERVAL);
                    retry++;
                } else {
                    e.printStackTrace();

                    fail(e.getMessage());
                }
            }
        }
    }

    @Test
    public void testBasicEncryptedConnection() throws SQLException {
        basicReconnect(connectionString);
    }

    @Test
    public void testGracefulClose() throws SQLException {
        try (Connection c = ResiliencyUtils.getConnection(connectionString)) {
            try (Statement s = c.createStatement()) {
                ResiliencyUtils.killConnection(c, connectionString, 0);
                c.close();
                s.executeQuery("SELECT 1");
                fail("Query execution did not throw an exception on a closed execution");
            } catch (SQLException e) {
                String message = e.getMessage();
                assertEquals(TestResource.getResource("R_connectionIsClosed"), message);
            }
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDB) // Switching databases is not supported against Azure, skip/
    public void testCatalog() throws SQLException {
        String expectedDatabaseName = null;
        String actualDatabaseName = null;
        try (Connection c = ResiliencyUtils.getConnection(connectionString); Statement s = c.createStatement()) {
            expectedDatabaseName = RandomUtil.getIdentifier("resDB");
            TestUtils.dropDatabaseIfExists(expectedDatabaseName, connectionString);
            s.execute("CREATE DATABASE [" + expectedDatabaseName + "]");
            try {
                c.setCatalog(expectedDatabaseName);
            } catch (SQLException e) {
                return;
            }
            ResiliencyUtils.killConnection(c, connectionString, 0);
            try (ResultSet rs = s.executeQuery("SELECT db_name();")) {
                while (rs.next()) {
                    actualDatabaseName = rs.getString(1);
                }
            }
            // Check if the driver reconnected to the expected database.
            assertEquals(expectedDatabaseName, actualDatabaseName);
        } finally {
            TestUtils.dropDatabaseIfExists(expectedDatabaseName, connectionString);
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDB) // Switching databases is not supported against Azure, skip/
    public void testUseDb() throws SQLException {
        String expectedDatabaseName = null;
        String actualDatabaseName = null;
        try (Connection c = ResiliencyUtils.getConnection(connectionString); Statement s = c.createStatement()) {
            expectedDatabaseName = RandomUtil.getIdentifier("resDB");
            TestUtils.dropDatabaseIfExists(expectedDatabaseName, connectionString);
            s.execute("CREATE DATABASE [" + expectedDatabaseName + "]");
            try {
                s.execute("USE [" + expectedDatabaseName + "]");
            } catch (SQLException e) {
                return;
            }
            ResiliencyUtils.killConnection(c, connectionString, 0);
            try (ResultSet rs = s.executeQuery("SELECT db_name();")) {
                while (rs.next()) {
                    actualDatabaseName = rs.getString(1);
                }
            }
            // Check if the driver reconnected to the expected database.
            assertEquals(expectedDatabaseName, actualDatabaseName);
        } finally {
            TestUtils.dropDatabaseIfExists(expectedDatabaseName, connectionString);
        }
    }

    @Test
    public void testSetLanguage() throws SQLException {
        String expectedLanguage = "Italiano";
        String actualLanguage = "";
        try (Connection c = ResiliencyUtils.getConnection(connectionString); Statement s = c.createStatement()) {
            s.execute("SET LANGUAGE " + expectedLanguage);
            ResiliencyUtils.killConnection(c, connectionString, 0);
            try (ResultSet rs = s.executeQuery("SELECT @@LANGUAGE")) {
                while (rs.next()) {
                    actualLanguage = rs.getString(1);
                }
            }
            // Check if the driver reconnected to the expected database.
            assertEquals(expectedLanguage, actualLanguage);
        }
    }

    @Test
    public void testOpenTransaction() throws SQLException {
        String tableName = RandomUtil.getIdentifier("resTable");
        try (Connection c = ResiliencyUtils.getConnection(connectionString); Statement s = c.createStatement()) {
            TestUtils.dropTableIfExists(tableName, s);
            s.execute("CREATE TABLE [" + tableName + "] (col1 varchar(1))");
            c.setAutoCommit(false);
            s.execute("INSERT INTO [" + tableName + "] values ('x')");
            ResiliencyUtils.killConnection(c, connectionString, 0);
            try (ResultSet rs = s.executeQuery("SELECT db_name();")) {
                fail("Connection resiliency should not have reconnected with an open transaction!");
            } catch (SQLException ex) {
                assertTrue(ex.getMessage().matches(TestUtils.formatErrorMsg("R_crServerSessionStateNotRecoverable")));
            }
        }
        try (Connection c = DriverManager.getConnection(connectionString); Statement s = c.createStatement()) {
            TestUtils.dropTableIfExists(tableName, s);
        }
    }

    @Test
    public void testOpenResultSetShouldBeCleanedUp() throws SQLException, InterruptedException {
        try (Connection c = ResiliencyUtils.getConnection(connectionString); Statement s = c.createStatement()) {
            int sessionId = ResiliencyUtils.getSessionId(c);
            try (ResultSet rs = s.executeQuery("select 2")) {
                rs.next();
                ResiliencyUtils.killConnection(sessionId, connectionString, c, 0);
            } catch (SQLException ex) {
                ex.printStackTrace();
                fail("Connection failed to clean up open resultset.");
            }
        }
    }

    @Test
    public void testPooledConnection() throws SQLException {
        SQLServerConnectionPoolDataSource mds = new SQLServerConnectionPoolDataSource();
        mds.setURL(connectionString);
        PooledConnection pooledConnection = mds.getPooledConnection();
        try (Connection c = pooledConnection.getConnection(); Statement s = c.createStatement()) {
            ResiliencyUtils.minimizeIdleNetworkTracker(c);
            c.close();
            Connection c1 = pooledConnection.getConnection();
            Statement s1 = c1.createStatement();
            ResiliencyUtils.killConnection(c1, connectionString, 0);
            ResiliencyUtils.minimizeIdleNetworkTracker(c1);
            s1.executeQuery("SELECT 1");
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    @Test
    @Tag(Constants.xAzureSQLDB) // Switching databases is not supported against Azure, skip/
    public void testPooledConnectionDB() throws SQLException {
        SQLServerConnectionPoolDataSource mds = new SQLServerConnectionPoolDataSource();
        mds.setURL(connectionString);
        PooledConnection pooledConnection = mds.getPooledConnection();
        String newDBName = null;
        String resultDBName = null;
        String originalDBName = null;
        try (Connection c = pooledConnection.getConnection(); Statement s = c.createStatement()) {
            ResiliencyUtils.minimizeIdleNetworkTracker(c);
            ResultSet rs = s.executeQuery("SELECT DB_NAME();");
            rs.next();
            originalDBName = rs.getString(1);
            newDBName = RandomUtil.getIdentifier("resDB");
            TestUtils.dropDatabaseIfExists(newDBName, connectionString);
            s.execute("CREATE DATABASE [" + newDBName + "]");
            try {
                s.execute("USE [" + newDBName + "]");
            } catch (SQLException e) {
                return;
            }
            c.close();
            Connection c1 = pooledConnection.getConnection();
            Statement s1 = c1.createStatement();
            ResiliencyUtils.killConnection(c1, connectionString, 0);
            ResiliencyUtils.minimizeIdleNetworkTracker(c1);
            rs = s1.executeQuery("SELECT db_name();");
            while (rs.next()) {
                resultDBName = rs.getString(1);
            }
            // Check if the driver reconnected to the expected database.
            assertEquals(originalDBName, resultDBName);
        } finally {
            TestUtils.dropDatabaseIfExists(newDBName, connectionString);
        }
    }

    @Test
    public void testPooledConnectionLang() throws SQLException {
        SQLServerConnectionPoolDataSource mds = new SQLServerConnectionPoolDataSource();
        mds.setURL(connectionString);
        PooledConnection pooledConnection = mds.getPooledConnection();
        String lang0 = null, lang1 = null;

        try (Connection c = pooledConnection.getConnection(); Statement s = c.createStatement()) {
            ResiliencyUtils.minimizeIdleNetworkTracker(c);
            ResultSet rs = s.executeQuery("SELECT @@LANGUAGE;");
            while (rs.next())
                lang0 = rs.getString(1);
            s.execute("SET LANGUAGE FRENCH;");
            c.close();
            try (Connection c1 = pooledConnection.getConnection(); Statement s1 = c1.createStatement()) {
                ResiliencyUtils.killConnection(c1, connectionString, 0);
                ResiliencyUtils.minimizeIdleNetworkTracker(c1);
                rs = s1.executeQuery("SELECT @@LANGUAGE;");
                while (rs.next())
                    lang1 = rs.getString(1);
                assertEquals(lang0, lang1);
            } finally {
                rs.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            fail(e.toString());
        }
    }

    @Tag(Constants.xSQLv11)
    @Tag(Constants.xSQLv12)
    @Tag(Constants.xSQLv14)
    @Tag(Constants.xSQLv15)
    @Tag(Constants.xSQLv16)
    @Tag(Constants.xAzureSQLDW)
    @Tag(Constants.reqExternalSetup)
    @Test
    public void testDSPooledConnectionAccessTokenCallbackIdleConnectionResiliency() throws Exception {
        SQLServerConnectionPoolDataSource ds = new SQLServerConnectionPoolDataSource();

        // User/password is not required for access token callback
        String cs = TestUtils.addOrOverrideProperty(connectionString, "user", "");
        cs = TestUtils.addOrOverrideProperty(cs, "password", "");
        AbstractTest.updateDataSource(cs, ds);
        ds.setAccessTokenCallback(TestUtils.accessTokenCallback);

        // change token expiry
        SQLServerPooledConnection pc = (SQLServerPooledConnection) ds.getPooledConnection();
        Field physicalConnectionField = SQLServerPooledConnection.class.getDeclaredField("physicalConnection");
        physicalConnectionField.setAccessible(true);
        Object c = physicalConnectionField.get(pc);
        String accessToken = ds.getAccessToken();
        TestUtils.setAccessTokenExpiry(c, accessToken);

        // Idle Connection Resiliency should reconnect after connection kill, second query should run successfully
        TestUtils.expireTokenToggle = false;
        pc = (SQLServerPooledConnection) ds.getPooledConnection();

        try (Connection conn = pc.getConnection(); Statement s = conn.createStatement()) {
            ResiliencyUtils.minimizeIdleNetworkTracker(conn);
            s.executeQuery("SELECT 1");
            ResiliencyUtils.killConnection(conn, connectionString, 3);
            ResiliencyUtils.minimizeIdleNetworkTracker(conn);
            s.executeQuery("SELECT 1");
        } catch (SQLException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testPreparedStatementCacheShouldBeCleared() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) ResiliencyUtils.getConnection(connectionString)) {
            int cacheSize = 2;
            String query = String.format("/*testPreparedStatementCacheShouldBeCleared_%s*/SELECT 1; -- ",
                    UUID.randomUUID().toString());
            int discardedStatementCount = 1;

            // enable caching
            con.setDisableStatementPooling(false);
            con.setStatementPoolingCacheSize(cacheSize);
            con.setServerPreparedStatementDiscardThreshold(discardedStatementCount);

            // add new statements to fill cache
            for (int i = 0; i < cacheSize; ++i) {
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(query + i)) {
                    pstmt.execute();
                    pstmt.execute();
                }
            }

            // nothing should be discarded yet
            assertEquals(0, con.getDiscardedServerPreparedStatementCount());

            ResiliencyUtils.killConnection(con, connectionString, 1);

            // add 1 more - if cache was not cleared this would cause it to be discarded
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(query)) {
                pstmt.execute();
                pstmt.execute();
            }
            assertEquals(0, con.getDiscardedServerPreparedStatementCount());
        }
    }

    @Test
    public void testPreparedStatementHandleOfStatementShouldBeCleared() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) ResiliencyUtils.getConnection(connectionString)) {
            int cacheSize = 2;
            String query = String.format("/*testPreparedStatementHandleOfStatementShouldBeCleared%s*/SELECT 1; -- ",
                  UUID.randomUUID().toString());

            // enable caching
            con.setDisableStatementPooling(false);
            con.setStatementPoolingCacheSize(cacheSize);
            con.setServerPreparedStatementDiscardThreshold(cacheSize);

            List<SQLServerPreparedStatement> statements = new LinkedList<>();

            // add statements to fill cache
            for (int i = 0; i < cacheSize + 1; ++i) {
                SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(query + i);
                pstmt.execute();
                pstmt.execute();
                pstmt.execute();
                pstmt.getMoreResults();
                statements.add(pstmt);
            }

            // handle of the prepared statement should be set
            assertNotEquals(0, statements.get(1).getPreparedStatementHandle());

            ResiliencyUtils.killConnection(con, connectionString, 1);

            // call first statement to trigger reconnect
            statements.get(0).execute();

            // handle of the other statements should be cleared after reconnect
            assertEquals(0, statements.get(1).getPreparedStatementHandle());
        }
    }

    @Test
    public void testPreparedStatementShouldNotUseWrongHandleAfterReconnect() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) ResiliencyUtils.getConnection(connectionString)) {
            int cacheSize = 3;
            String queryOne = "select * from sys.sysusers where name=?;";
            String queryTwo = "select * from sys.sysusers where name=? and uid=?;";
            String queryThree = "select * from sys.sysusers where name=? and uid=? and islogin=?";

            String parameterOne = "name";
            int parameterUid = 0;
            int parameterIsLogin = 0;

            // enable caching
            con.setDisableStatementPooling(false);
            con.setStatementPoolingCacheSize(cacheSize);
            con.setServerPreparedStatementDiscardThreshold(cacheSize);

            List<PreparedStatement> statements = new LinkedList<>();

            PreparedStatement ps = con.prepareStatement(queryOne);
            ps.setString(1, parameterOne);
            statements.add(ps);

            ps = con.prepareStatement(queryTwo);
            ps.setString(1, parameterOne);
            ps.setInt(2, parameterUid);
            statements.add(ps);

            ps = con.prepareStatement(queryThree);
            ps.setString(1, parameterOne);
            ps.setInt(2, parameterUid);
            ps.setInt(3, parameterIsLogin);
            statements.add(ps);

            // add new statements to fill cache
            for (PreparedStatement preparedStatement : statements) {
                preparedStatement.execute();
                preparedStatement.execute();
                preparedStatement.execute();
                preparedStatement.getMoreResults();
            }

            ResiliencyUtils.killConnection(con, connectionString, 1);

            // call statements in reversed order, in order to force the statement to use the wrong handle
            // first execute triggers a reconnect
            Collections.reverse(statements);
            for (PreparedStatement preparedStatement : statements) {
                preparedStatement.execute();
                preparedStatement.execute();
                preparedStatement.execute();
                preparedStatement.getMoreResults();
            }
        }
    }


    @Test
    public void testUnprocessedResponseCountSuccessfulIdleConnectionRecovery() throws SQLException {
        try (SQLServerConnection con = (SQLServerConnection) ResiliencyUtils.getConnection(connectionString)) {
            int queriesToSend = 5;
            String query = String.format(
                    "/*testUnprocessedResponseCountSuccessfulIdleConnectionRecovery_%s*/SELECT 1; -- ",
                    UUID.randomUUID());

            for (int i = 0; i < queriesToSend; ++i) {
                try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(query + i)) {
                    pstmt.executeQuery();
                    pstmt.executeQuery();
                }
            }

            // Kill the connection. If the unprocessedResponseCount is negative, test will fail.
            ResiliencyUtils.killConnection(con, connectionString, 1);

            // Should successfully recover.
            try (SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) con.prepareStatement(query)) {
                pstmt.executeQuery();
                pstmt.executeQuery();
            }
        }
    }

    private void basicReconnect(String connectionString) throws SQLException {
        // Ensure reconnects can happen multiple times over the same connection and subsequent connections
        for (int i1 = 0; i1 < 2; i1++) {
            try (Connection c = ResiliencyUtils.getConnection(connectionString)) {
                for (int i2 = 0; i2 < 3; i2++) {
                    try (Statement s = c.createStatement()) {
                        ResiliencyUtils.killConnection(c, connectionString, 0);
                        s.executeQuery("SELECT 1");
                    }
                }
            }
        }
    }
}
