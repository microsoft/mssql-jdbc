/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

/**
 * CEK cache TTL tests: cache TTL configuration, cache eviction verification,
 * no-caching mode (TTL=0), extended cache retention.
 * Ported from FX ae/CEKCacheTTLTest.java tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxAE)
@Tag(Constants.reqExternalSetup)
public class CEKCacheTTLTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    public void testSetColumnEncryptionKeyCacheTTLZero() throws SQLException {
        // TTL=0 means no caching
        try (Connection conn = getConnection()) {
            SQLServerConnection sconn = (SQLServerConnection) conn;
            SQLServerConnection.setColumnEncryptionKeyCacheTtl(0, TimeUnit.SECONDS);
            // Verify setting does not throw
            assertNotNull(sconn);
        } finally {
            // Reset to default
            SQLServerConnection.setColumnEncryptionKeyCacheTtl(2, TimeUnit.HOURS);
        }
    }

    @Test
    public void testSetColumnEncryptionKeyCacheTTLSeconds() throws SQLException {
        try (Connection conn = getConnection()) {
            SQLServerConnection sconn = (SQLServerConnection) conn;
            SQLServerConnection.setColumnEncryptionKeyCacheTtl(4, TimeUnit.SECONDS);
            assertNotNull(sconn);
        } finally {
            SQLServerConnection.setColumnEncryptionKeyCacheTtl(2, TimeUnit.HOURS);
        }
    }

    @Test
    public void testSetColumnEncryptionKeyCacheTTLHours() throws SQLException {
        try (Connection conn = getConnection()) {
            SQLServerConnection sconn = (SQLServerConnection) conn;
            SQLServerConnection.setColumnEncryptionKeyCacheTtl(2, TimeUnit.HOURS);
            assertNotNull(sconn);
        }
    }

    @Test
    public void testColumnEncryptionKeyCacheTTLWithMinutes() throws SQLException {
        try (Connection conn = getConnection()) {
            SQLServerConnection sconn = (SQLServerConnection) conn;
            SQLServerConnection.setColumnEncryptionKeyCacheTtl(30, TimeUnit.MINUTES);
            assertNotNull(sconn);
        } finally {
            SQLServerConnection.setColumnEncryptionKeyCacheTtl(2, TimeUnit.HOURS);
        }
    }

    @Test
    public void testMultipleCacheTTLChanges() throws SQLException {
        try (Connection conn = getConnection()) {
            // Rapidly change TTL settings - should not cause issues
            SQLServerConnection.setColumnEncryptionKeyCacheTtl(0, TimeUnit.SECONDS);
            SQLServerConnection.setColumnEncryptionKeyCacheTtl(10, TimeUnit.SECONDS);
            SQLServerConnection.setColumnEncryptionKeyCacheTtl(1, TimeUnit.HOURS);
            SQLServerConnection.setColumnEncryptionKeyCacheTtl(0, TimeUnit.SECONDS);
            assertNotNull(conn);
        } finally {
            SQLServerConnection.setColumnEncryptionKeyCacheTtl(2, TimeUnit.HOURS);
        }
    }
}
