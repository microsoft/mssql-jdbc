/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.transaction;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Action;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Engine;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Result;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Transaction State Machine Tests - Based on FX Framework fxConnection model
 * actions.
 * 
 * This class implements Model-Based Testing (MBT) for JDBC Transaction
 * operations using
 * the simple StateMachineTest framework. It mirrors the transaction scenarios
 * tested by
 * the FX/KoKoMo framework but with simpler, more debuggable code.
 * 
 * Test scenarios covered (from FX fxConnection.java):
 * - setAutoCommit(true/false) - Toggle auto-commit mode
 * - commit() - Commit transaction (requires autoCommit=false)
 * - rollback() - Rollback transaction (requires autoCommit=false)
 * 
 * All actions are implemented as plain classes (NO LAMBDAS) for easier
 * debugging.
 */
@Tag("statemachine")
class TransactionStateTest extends AbstractTest {

    private static final String TABLE_NAME = AbstractSQLGenerator.escapeIdentifier("SM_Transaction_Test");

    @BeforeAll
    static void setupTests() throws Exception {
        setConnection();
    }

    @AfterAll
    static void cleanupTests() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            TestUtils.dropTableIfExists(TABLE_NAME, connection.createStatement());
        }
    }

    /**
     * Creates a test table with sample data for transaction tests.
     */
    private static void createTestTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, value INT)");
            stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 100)");
        }
    }

    // ==================== REAL DATABASE TESTS ====================

    @Test
    @DisplayName("FX Model: Real Database - Transaction Commit/Rollback")
    void testRealDatabaseTransaction() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            createTestTable(conn);

            StateMachineTest sm = new StateMachineTest("RealTransaction");
            sm.set("conn", conn);
            sm.set("autoCommit", true);
            sm.set("closed", false);

            sm.addAction(new RealSetAutoCommitFalseAction(sm));
            sm.addAction(new RealSetAutoCommitTrueAction(sm));
            sm.addAction(new RealCommitAction(sm));
            sm.addAction(new RealRollbackAction(sm));
            sm.addAction(new RealExecuteUpdateAction(sm));
            sm.addAction(new RealSelectAction(sm));

            Result result = Engine.run(sm).withMaxActions(50).withSeed(54321).execute();

            // Cleanup - ensure autocommit is restored
            conn.setAutoCommit(true);

            System.out.println("\nReal DB transaction test: " + result.actionCount + " actions");
            assertTrue(result.isSuccess());
        }
    }

    // ==================== REAL DATABASE ACTION CLASSES ====================

    static class RealSetAutoCommitFalseAction extends Action {
        private StateMachineTest sm;

        RealSetAutoCommitFalseAction(StateMachineTest sm) {
            super("setAutoCommit(false)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.is("autoCommit");
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) sm.get("conn");
            conn.setAutoCommit(false);
            sm.set("autoCommit", false);
            System.out.println("  setAutoCommit(false)");
        }
    }

    static class RealSetAutoCommitTrueAction extends Action {
        private StateMachineTest sm;

        RealSetAutoCommitTrueAction(StateMachineTest sm) {
            super("setAutoCommit(true)", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && !sm.is("autoCommit");
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) sm.get("conn");
            conn.setAutoCommit(true);
            sm.set("autoCommit", true);
            System.out.println("  setAutoCommit(true) - implicit commit");
        }
    }

    static class RealCommitAction extends Action {
        private StateMachineTest sm;

        RealCommitAction(StateMachineTest sm) {
            super("commit", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && !sm.is("autoCommit");
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) sm.get("conn");
            conn.commit();
            System.out.println("  commit()");
        }
    }

    static class RealRollbackAction extends Action {
        private StateMachineTest sm;

        RealRollbackAction(StateMachineTest sm) {
            super("rollback", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && !sm.is("autoCommit");
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) sm.get("conn");
            conn.rollback();
            System.out.println("  rollback()");
        }
    }

    static class RealExecuteUpdateAction extends Action {
        private StateMachineTest sm;

        RealExecuteUpdateAction(StateMachineTest sm) {
            super("executeUpdate", 15);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) sm.get("conn");
            try (Statement stmt = conn.createStatement()) {
                int newValue = sm.getRandom().nextInt(1000);
                int rows = stmt.executeUpdate("UPDATE " + TABLE_NAME + " SET value = " + newValue + " WHERE id = 1");
                System.out.println("  executeUpdate -> " + rows + " rows, value=" + newValue);
            }
        }
    }

    static class RealSelectAction extends Action {
        private StateMachineTest sm;

        RealSelectAction(StateMachineTest sm) {
            super("executeQuery", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) sm.get("conn");
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT value FROM " + TABLE_NAME + " WHERE id = 1")) {
                if (rs.next()) {
                    System.out.println("  SELECT -> value=" + rs.getInt("value"));
                }
            }
        }
    }
}
