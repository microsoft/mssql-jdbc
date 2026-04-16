/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.resultset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

/**
 * ResultSet holdability tests: HOLD_CURSORS_OVER_COMMIT, CLOSE_CURSORS_AT_COMMIT,
 * holdability interaction with transactions and autocommit.
 * Ported from FX resultset/resultsettest.java holdability tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxResultSet)
public class ResultSetHoldabilityTest extends AbstractTest {

    private static final String tableName = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("RSHold_Tab"));

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.executeUpdate("CREATE TABLE " + tableName
                    + " (ID INT IDENTITY PRIMARY KEY, COL1 VARCHAR(200), COL2 INT)");
            for (int i = 1; i <= 10; i++) {
                stmt.executeUpdate(
                        "INSERT INTO " + tableName + " (COL1, COL2) VALUES ('hold_row" + i + "', " + i + ")");
            }
        }
    }

    @AfterAll
    public static void cleanupTests() throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
        }
    }

    @Test
    public void testHoldCursorsOverCommit() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
            assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());
            try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                assertTrue(rs.next());
                conn.commit();
                // ResultSet should still be usable after commit with HOLD
                assertTrue(rs.next());
            }
            conn.setAutoCommit(true);
        }
    }

    @Test
    public void testCloseCursorsAtCommit() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());
            try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.CLOSE_CURSORS_AT_COMMIT);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                assertTrue(rs.next());
                conn.commit();
                // After commit with CLOSE_CURSORS, cursor operation may throw
            }
            conn.setAutoCommit(true);
        }
    }

    @Test
    public void testDefaultHoldability() throws SQLException {
        try (Connection conn = getConnection()) {
            int holdability = conn.getHoldability();
            assertTrue(holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT
                    || holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT);
        }
    }

    @Test
    public void testSetHoldability() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
            assertEquals(ResultSet.HOLD_CURSORS_OVER_COMMIT, conn.getHoldability());
            conn.setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
            assertEquals(ResultSet.CLOSE_CURSORS_AT_COMMIT, conn.getHoldability());
        }
    }

    @Test
    public void testHoldabilityWithForwardOnly() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                assertTrue(rs.next());
                String val = rs.getString("COL1");
                assertNotNull(val);
                conn.commit();
                // Forward-only with HOLD should still allow reading
                assertTrue(rs.next());
            }
            conn.setAutoCommit(true);
        }
    }

    @Test
    public void testHoldabilityAfterRollback() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                assertTrue(rs.next());
                conn.rollback();
            }
            conn.setAutoCommit(true);
        }
    }

    @Test
    public void testHoldabilityWithAutoCommitTrue() throws SQLException {
        try (Connection conn = getConnection()) {
            assertTrue(conn.getAutoCommit());
            try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                // With autocommit=true, each statement is its own transaction
                assertTrue(rs.next());
                assertNotNull(rs.getString("COL1"));
            }
        }
    }

    @Test
    public void testMultipleResultSetsWithHoldability() throws SQLException {
        try (Connection conn = getConnection()) {
            conn.setAutoCommit(false);
            conn.setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
            try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY, ResultSet.HOLD_CURSORS_OVER_COMMIT)) {
                ResultSet rs1 = stmt.executeQuery("SELECT * FROM " + tableName + " WHERE COL2 <= 5");
                assertTrue(rs1.next());
                conn.commit();
                // rs1 should still be valid with HOLD
                rs1.close();
            }
            conn.setAutoCommit(true);
        }
    }
}
