/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.cts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

/**
 * CTS compliance tests for Connection: lifecycle, transaction isolation,
 * metadata access, catalog, read-only mode, nativeSQL.
 * Ported from FX connectionClient.java CTS tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxCTS)
public class ConnectionCTSTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /**
     * Tests that Connection.close properly closes the connection.
     * Ported from FX CTS connectionClient close variations.
     */
    @Test
    public void testClose() throws SQLException {
        Connection conn = getConnection();
        assertFalse(conn.isClosed());
        conn.close();
        assertTrue(conn.isClosed());
    }

    /**
     * Tests isClosed returns false on an open connection.
     * Ported from FX CTS connectionClient isClosed variations.
     */
    @Test
    public void testIsClosed() throws SQLException {
        try (Connection conn = getConnection()) {
            assertFalse(conn.isClosed());
        }
    }

    /**
     * Tests isClosed returns true after the connection is closed.
     * Ported from FX CTS connectionClient isClosed variations.
     */
    @Test
    public void testIsClosedAfterClose() throws SQLException {
        Connection conn = getConnection();
        conn.close();
        assertTrue(conn.isClosed());
    }

    /**
     * Tests createStatement returns a non-null Statement object.
     * Ported from FX CTS connectionClient createStatement variations.
     */
    @Test
    public void testCreateStatement() throws SQLException {
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
            assertNotNull(stmt);
        }
    }

    /**
     * Tests getCatalog returns a non-empty catalog name.
     * Ported from FX CTS connectionClient getCatalog variations.
     */
    @Test
    public void testGetCatalog() throws SQLException {
        try (Connection conn = getConnection()) {
            String catalog = conn.getCatalog();
            assertNotNull(catalog);
            assertTrue(catalog.length() > 0);
        }
    }

    /**
     * Tests setCatalog with the current catalog name preserves the catalog.
     * Ported from FX CTS connectionClient setCatalog variations.
     */
    @Test
    public void testSetCatalog() throws SQLException {
        try (Connection conn = getConnection()) {
            String originalCatalog = conn.getCatalog();
            conn.setCatalog(originalCatalog);
            assertEquals(originalCatalog, conn.getCatalog());
        }
    }

    /**
     * Tests getMetaData returns a non-null DatabaseMetaData with a valid product name.
     * Ported from FX CTS connectionClient getMetaData variations.
     */
    @Test
    public void testGetMetaData() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertNotNull(dbmd);
            assertNotNull(dbmd.getDatabaseProductName());
        }
    }

    /**
     * Tests getTransactionIsolation returns a valid isolation level constant.
     * Ported from FX CTS connectionClient getTransactionIsolation variations.
     */
    @Test
    public void testGetTransactionIsolation() throws SQLException {
        try (Connection conn = getConnection()) {
            int level = conn.getTransactionIsolation();
            assertTrue(level == Connection.TRANSACTION_READ_UNCOMMITTED
                    || level == Connection.TRANSACTION_READ_COMMITTED
                    || level == Connection.TRANSACTION_REPEATABLE_READ
                    || level == Connection.TRANSACTION_SERIALIZABLE);
        }
    }

    /**
     * Tests setTransactionIsolation to READ_COMMITTED and verifies the level.
     * Ported from FX CTS connectionClient setTransactionIsolation variations.
     */
    @Test
    public void testSetTransactionIsolationReadCommitted() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            assertEquals(Connection.TRANSACTION_READ_COMMITTED, conn.getTransactionIsolation());
        }
    }

    /**
     * Tests setTransactionIsolation to READ_UNCOMMITTED and verifies the level.
     * Ported from FX CTS connectionClient setTransactionIsolation variations.
     */
    @Test
    public void testSetTransactionIsolationReadUncommitted() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, conn.getTransactionIsolation());
        }
    }

    /**
     * Tests setTransactionIsolation to REPEATABLE_READ and verifies the level.
     * Ported from FX CTS connectionClient setTransactionIsolation variations.
     */
    @Test
    public void testSetTransactionIsolationRepeatableRead() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            assertEquals(Connection.TRANSACTION_REPEATABLE_READ, conn.getTransactionIsolation());
        }
    }

    /**
     * Tests setTransactionIsolation to SERIALIZABLE and verifies the level.
     * Ported from FX CTS connectionClient setTransactionIsolation variations.
     */
    @Test
    public void testSetTransactionIsolationSerializable() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            assertEquals(Connection.TRANSACTION_SERIALIZABLE, conn.getTransactionIsolation());
        }
    }

    /**
     * Tests isReadOnly returns false by default.
     * Ported from FX CTS connectionClient isReadOnly variations.
     */
    @Test
    public void testIsReadOnly() throws SQLException {
        try (Connection conn = getConnection()) {
            // Default should be false
            assertFalse(conn.isReadOnly());
        }
    }

    /**
     * Tests setReadOnly toggles read-only mode on and off.
     * Ported from FX CTS connectionClient setReadOnly variations.
     * Note: SQL Server JDBC driver treats setReadOnly as a no-op and isReadOnly always returns false.
     */
    @Test
    public void testSetReadOnly() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setReadOnly(true);
            assertFalse(conn.isReadOnly()); // driver no-op: isReadOnly always returns false
            conn.setReadOnly(false);
            assertFalse(conn.isReadOnly());
        }
    }

    /**
     * Tests nativeSQL returns a non-empty translation for a SQL string.
     * Ported from FX CTS connectionClient nativeSQL variations.
     */
    @Test
    public void testNativeSQL() throws SQLException {
        try (Connection conn = getConnection()) {
            String sql = "SELECT * FROM sys.objects";
            String nativeSql = conn.nativeSQL(sql);
            assertNotNull(nativeSql);
            assertTrue(nativeSql.length() > 0);
        }
    }

    /**
     * Tests nativeSQL processes a JDBC escape sequence without error.
     * Ported from FX CTS connectionClient nativeSQL escape sequence variations.
     */
    @Test
    public void testNativeSQLWithEscapeSequence() throws SQLException {
        try (Connection conn = getConnection()) {
            String sql = "{fn CONVERT('2024-01-01', SQL_DATE)}";
            String nativeSql = conn.nativeSQL(sql);
            assertNotNull(nativeSql);
        }
    }

    /**
     * Tests getAutoCommit default is true and setAutoCommit toggles correctly.
     * Ported from FX CTS connectionClient autoCommit variations.
     */
    @Test
    public void testAutoCommit() throws SQLException {
        try (Connection conn = getConnection()) {
            assertTrue(conn.getAutoCommit()); // default is true
            conn.setAutoCommit(false);
            assertFalse(conn.getAutoCommit());
            conn.setAutoCommit(true);
            assertTrue(conn.getAutoCommit());
        }
    }

    /**
     * Tests manual commit and rollback with autoCommit disabled.
     * Ported from FX CTS connectionClient commit and rollback variations.
     */
    @Test
    public void testCommitAndRollback() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SELECT 1"); // benign operation
            }
            conn.commit();
            conn.rollback();
            conn.setAutoCommit(true);
        }
    }

    /**
     * Tests getWarnings call completes without error.
     * Ported from FX CTS connectionClient getWarnings variations.
     */
    @Test
    public void testGetWarnings() throws SQLException {
        try (Connection conn = getConnection()) {
            // May or may not have warnings, should not throw
            conn.getWarnings();
        }
    }

    /**
     * Tests clearWarnings call completes without error.
     * Ported from FX CTS connectionClient clearWarnings variations.
     */
    @Test
    public void testClearWarnings() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.clearWarnings();
            assertNotNull(conn); // Should not throw
        }
    }

    /**
     * Tests getSchema returns a non-null schema name.
     * Ported from FX CTS connectionClient getSchema variations.
     */
    @Test
    public void testGetSchema() throws SQLException {
        try (Connection conn = getConnection()) {
            String schema = conn.getSchema();
            assertNotNull(schema);
        }
    }

    /**
     * Tests isValid returns true on an active connection with a 5-second timeout.
     * Ported from FX CTS connectionClient isValid variations.
     */
    @Test
    public void testIsValid() throws SQLException {
        try (Connection conn = getConnection()) {
            assertTrue(conn.isValid(5)); // 5 second timeout
        }
    }

    /**
     * Tests isValid returns false after the connection is closed.
     * Ported from FX CTS connectionClient isValid variations.
     */
    @Test
    public void testIsValidAfterClose() throws SQLException {
        Connection conn = getConnection();
        conn.close();
        assertFalse(conn.isValid(5));
    }

    /**
     * Tests getClientInfo call completes without error.
     * Ported from FX CTS connectionClient getClientInfo variations.
     */
    @Test
    public void testGetClientInfo() throws SQLException {
        try (Connection conn = getConnection()) {
            // Should not throw
            conn.getClientInfo();
        }
    }

    /**
     * Tests getNetworkTimeout returns a non-negative value.
     * Ported from FX CTS connectionClient getNetworkTimeout variations.
     */
    @Test
    public void testNetworkTimeout() throws SQLException {
        try (Connection conn = getConnection()) {
            int timeout = conn.getNetworkTimeout();
            assertTrue(timeout >= 0);
        }
    }
}
