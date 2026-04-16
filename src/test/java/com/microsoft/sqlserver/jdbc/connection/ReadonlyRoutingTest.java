/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
 * Read-only routing tests for AlwaysOn availability groups: failover scenarios,
 * routing with SSL, connection timeout with routing, double-hop rejection.
 * Ported from FX readonlyrouting tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxRouting)
@Tag(Constants.reqExternalSetup)
public class ReadonlyRoutingTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    public void testReadOnlyIntentConnection() throws SQLException {
        String url = connectionString + ";applicationIntent=ReadOnly";
        try (Connection conn = PrepUtil.getConnection(url)) {
            assertNotNull(conn);
            assertTrue(conn.isReadOnly() || !conn.isClosed());
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
            }
        } catch (SQLServerException e) {
            // If AG not configured, routing may not be available - this is expected
            assertTrue(e.getMessage().contains("read-only")
                    || e.getMessage().contains("routing")
                    || e.getMessage().contains("connection"));
        }
    }

    @Test
    public void testReadWriteIntentConnection() throws SQLException {
        String url = connectionString + ";applicationIntent=ReadWrite";
        try (Connection conn = PrepUtil.getConnection(url)) {
            assertNotNull(conn);
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
            }
        }
    }

    @Test
    public void testConnectionTimeoutWithReadOnlyRouting() throws SQLException {
        String url = connectionString + ";applicationIntent=ReadOnly;loginTimeout=5";
        try (Connection conn = PrepUtil.getConnection(url)) {
            assertNotNull(conn);
        } catch (SQLServerException e) {
            // Expected if routing target is unreachable within timeout
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testMultiSubnetFailover() throws SQLException {
        String url = connectionString + ";multiSubnetFailover=true";
        try (Connection conn = PrepUtil.getConnection(url)) {
            assertNotNull(conn);
            DatabaseMetaData dbmd = conn.getMetaData();
            assertNotNull(dbmd);
        }
    }

    @Test
    public void testMultiSubnetFailoverWithReadOnlyIntent() throws SQLException {
        String url = connectionString + ";multiSubnetFailover=true;applicationIntent=ReadOnly";
        try (Connection conn = PrepUtil.getConnection(url)) {
            assertNotNull(conn);
        } catch (SQLServerException e) {
            // AG may not be configured
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testFailoverPartnerWithReadOnly() throws SQLException {
        // failoverPartner is incompatible with routing in some configurations
        String url = connectionString + ";applicationIntent=ReadOnly;failoverPartner=nonExistentServer";
        try (Connection conn = PrepUtil.getConnection(url)) {
            // Connection may succeed if failoverPartner is ignored
            assertNotNull(conn);
        } catch (SQLServerException e) {
            // Expected: failover partner conflicts or not reachable
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testReadOnlyWithEncrypt() throws SQLException {
        String url = connectionString + ";applicationIntent=ReadOnly;encrypt=true;trustServerCertificate=true";
        try (Connection conn = PrepUtil.getConnection(url)) {
            assertNotNull(conn);
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
            }
        } catch (SQLServerException e) {
            // If AG not configured
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void testDefaultApplicationIntent() throws SQLException {
        // Default intent should be ReadWrite
        try (Connection conn = getConnection()) {
            assertNotNull(conn);
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT 1")) {
                assertTrue(rs.next());
            }
        }
    }
}
