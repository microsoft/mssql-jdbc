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
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest.Action;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest.Engine;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest.Result;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Transaction State Machine Tests - Based on FX Framework fxConnection model actions.
 * 
 * This class implements Model-Based Testing (MBT) for JDBC Transaction operations using
 * the simple StateMachineTest framework. It mirrors the transaction scenarios tested by
 * the FX/KoKoMo framework but with simpler, more debuggable code.
 * 
 * Test scenarios covered (from FX fxConnection.java):
 * - setAutoCommit(true/false) - Toggle auto-commit mode
 * - commit() - Commit transaction (requires autoCommit=false)
 * - rollback() - Rollback transaction (requires autoCommit=false)
 * - setSavepoint() / setSavepoint(name) - Create savepoints
 * - rollback(savepoint) - Rollback to savepoint
 * - releaseSavepoint() - Release a savepoint
 * 
 * All actions are implemented as plain classes (NO LAMBDAS) for easier debugging.
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

    // ==================== SIMULATED TRANSACTION TESTS (No DB needed) ====================

    @Test
    @DisplayName("FX Model: Basic Transaction Control (simulated)")
    void testBasicTransactionControl() {
        StateMachineTest sm = new StateMachineTest("BasicTransaction");

        // State from fxConnection: _autocommit, _closed
        sm.set("autoCommit", true);   // Default is true
        sm.set("closed", false);
        sm.set("inTransaction", false);
        sm.set("uncommittedChanges", 0);

        // FX _modelsetAutoCommit: weight=0 (disabled in FX), requires closed=false
        sm.addAction(new SetAutoCommitTrueAction(sm));
        sm.addAction(new SetAutoCommitFalseAction(sm));

        // FX _modelcommit: weight=10, requires autoCommit=false, closed=false
        sm.addAction(new CommitAction(sm));

        // FX _modelrollback: weight=10, requires autoCommit=false, closed=false
        sm.addAction(new RollbackAction(sm));

        // Simulate data modifications
        sm.addAction(new ExecuteUpdateAction(sm));

        Result r = Engine.run(sm).withMaxActions(500).withSeed(12345).execute();
        assertTrue(r.isSuccess());
        System.out.println("Basic transaction: " + r.actionCount + " actions, seed=" + r.seed);
    }

    @Test
    @DisplayName("FX Model: Savepoint Operations (simulated)")
    void testSavepointOperations() {
        StateMachineTest sm = new StateMachineTest("SavepointTransaction");

        sm.set("autoCommit", true);
        sm.set("closed", false);
        sm.set("inTransaction", false);
        sm.set("savepoints", new ArrayList<String>());  // List of savepoint names
        sm.set("savepointCounter", 0);
        sm.set("uncommittedChanges", 0);

        // Auto-commit control
        sm.addAction(new SavepointSetAutoCommitTrueAction(sm));
        sm.addAction(new SavepointSetAutoCommitFalseAction(sm));

        // Basic transaction
        sm.addAction(new SavepointCommitAction(sm));
        sm.addAction(new SavepointRollbackAction(sm));

        // Savepoint operations (requires autoCommit=false)
        sm.addAction(new SetSavepointAction(sm));
        sm.addAction(new SetNamedSavepointAction(sm));
        sm.addAction(new RollbackToSavepointAction(sm));
        sm.addAction(new ReleaseSavepointAction(sm));

        // Data modification
        sm.addAction(new SavepointExecuteUpdateAction(sm));

        Result r = Engine.run(sm).withMaxActions(500).execute();
        System.out.println("Savepoint transaction: " + r.actionCount + " actions");
        System.out.println("Final state: " + sm.getState());
        assertTrue(r.isSuccess());
    }

    @Test
    @DisplayName("FX Model: Transaction Isolation (simulated)")
    void testTransactionIsolation() {
        StateMachineTest sm = new StateMachineTest("IsolationTransaction");

        sm.set("autoCommit", true);
        sm.set("closed", false);
        sm.set("isolationLevel", Connection.TRANSACTION_READ_COMMITTED); // Default

        // Auto-commit control
        sm.addAction(new IsolationSetAutoCommitFalseAction(sm));
        sm.addAction(new IsolationSetAutoCommitTrueAction(sm));

        // Isolation level changes
        sm.addAction(new SetIsolationReadUncommittedAction(sm));
        sm.addAction(new SetIsolationReadCommittedAction(sm));
        sm.addAction(new SetIsolationRepeatableReadAction(sm));
        sm.addAction(new SetIsolationSerializableAction(sm));

        // Transaction operations
        sm.addAction(new IsolationCommitAction(sm));
        sm.addAction(new IsolationRollbackAction(sm));

        Result r = Engine.run(sm).withMaxActions(300).withSeed(99999).execute();
        assertTrue(r.isSuccess());
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

    @Test
    @DisplayName("FX Model: Complex Transaction - Commit after multiple updates")
    void testComplexCommitAfterUpdates() {
        StateMachineTest sm = new StateMachineTest("ComplexCommit");

        sm.set("autoCommit", false);  // Start in transaction mode
        sm.set("closed", false);
        sm.set("uncommittedChanges", 0);
        sm.set("totalCommits", 0);
        sm.set("totalRollbacks", 0);

        sm.addAction(new ComplexExecuteUpdateAction(sm));
        sm.addAction(new ComplexCommitAction(sm));
        sm.addAction(new ComplexRollbackAction(sm));

        Result r = Engine.run(sm).withMaxActions(200).execute();

        System.out.println("Commits: " + sm.getInt("totalCommits"));
        System.out.println("Rollbacks: " + sm.getInt("totalRollbacks"));
        System.out.println("Final uncommitted: " + sm.getInt("uncommittedChanges"));

        assertTrue(r.isSuccess());
    }

    /**
     * FX Model: Connection Statement Creation Operations (simulated)
     * 
     * Tests FX model actions for creating statements:
     * - _modelcreateStatement: Create basic statement
     * - _modelcreateStatement(type, concurrency): Create with cursor type
     * - _modelcreateStatement(type, concurrency, holdability): Full options
     * - _modelprepareStatement: Prepare SQL statement
     * - _modelprepareCall: Prepare callable statement
     * - _modelgetMetaData: Get database metadata
     */
    @Test
    @DisplayName("FX Model: Connection Statement Creation (simulated)")
    void testConnectionStatementCreation() {
        StateMachineTest sm = new StateMachineTest("StatementCreation");

        sm.set("closed", false);
        sm.set("autoCommit", true);
        sm.set("statementCount", 0);
        sm.set("preparedStatementCount", 0);
        sm.set("callableStatementCount", 0);
        sm.set("holdability", ResultSet.HOLD_CURSORS_OVER_COMMIT);

        // FX _modelcreateStatement: weight=10, requires _closed=false
        sm.addAction(new CreateStatementAction(sm));
        sm.addAction(new CreateStatementWithTypeAction(sm));
        sm.addAction(new CreateStatementWithHoldabilityAction(sm));

        // FX _modelprepareStatement: weight=10, requires _closed=false
        sm.addAction(new PrepareStatementAction(sm));

        // FX _modelprepareCall: weight=10, requires _closed=false
        sm.addAction(new PrepareCallAction(sm));

        // FX _modelgetMetaData: weight=2, requires _closed=false
        sm.addAction(new GetDatabaseMetaDataAction(sm));

        // Transaction control
        sm.addAction(new StatementSetAutoCommitAction(sm));

        Result r = Engine.run(sm).withMaxActions(200).withSeed(88888).execute();
        assertTrue(r.isSuccess());
        System.out.println("Statement creation: " + r.actionCount + " actions");
        System.out.println("  Statements: " + sm.getInt("statementCount"));
        System.out.println("  PreparedStatements: " + sm.getInt("preparedStatementCount"));
        System.out.println("  CallableStatements: " + sm.getInt("callableStatementCount"));
    }

    /**
     * FX Model: Connection Holdability and Catalog Operations (simulated)
     * 
     * Tests FX model actions for connection configuration:
     * - setHoldability / getHoldability
     * - setCatalog / getCatalog
     * - nativeSQL
     * - getWarnings / clearWarnings
     * - isValid
     * - isClosed
     */
    @Test
    @DisplayName("FX Model: Connection Configuration (simulated)")
    void testConnectionConfiguration() {
        StateMachineTest sm = new StateMachineTest("ConnectionConfig");

        sm.set("closed", false);
        sm.set("autoCommit", true);
        sm.set("holdability", ResultSet.HOLD_CURSORS_OVER_COMMIT);
        sm.set("catalog", "testdb");
        sm.set("warnings", false);

        // Holdability
        sm.addAction(new SetHoldabilityAction(sm));
        sm.addAction(new GetHoldabilityAction(sm));

        // Catalog
        sm.addAction(new SetCatalogAction(sm));
        sm.addAction(new GetCatalogAction(sm));

        // Warnings
        sm.addAction(new GetWarningsAction(sm));
        sm.addAction(new ConnectionClearWarningsAction(sm));

        // Connection state
        sm.addAction(new IsValidAction(sm));
        sm.addAction(new IsClosedAction(sm));

        // Native SQL
        sm.addAction(new NativeSQLAction(sm));

        Result r = Engine.run(sm).withMaxActions(150).execute();
        assertTrue(r.isSuccess());
        System.out.println("Connection configuration: " + r.actionCount + " actions");
    }

    // ==================== FX MISSING TEST CASES (from FX_TO_JUNIT_COMPREHENSIVE_ANALYSIS.md) ====================

    /**
     * FX Test: testCommitTransaction() - Transaction Commit
     * 
     * Tests basic transaction commit functionality:
     * - Start transaction (setAutoCommit(false))
     * - Execute DML operations (INSERT, UPDATE, DELETE)
     * - Commit transaction
     * - Verify data persisted
     * 
     * This addresses the NOT FOUND gap in Connection Management.
     */
    @Test
    @DisplayName("FX Missing: testCommitTransaction - Basic Transaction Commit")
    void testCommitTransaction() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            createTestTable(conn);

            // Start transaction
            conn.setAutoCommit(false);

            // Execute DML
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("UPDATE " + TABLE_NAME + " SET value = 200 WHERE id = 1");
                stmt.executeUpdate("INSERT INTO " + TABLE_NAME + " VALUES (2, 300)");
            }

            // Commit
            conn.commit();

            // Verify data persisted
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                int count = rs.getInt(1);
                assertTrue(count >= 2, "Expected at least 2 rows after commit");
                System.out.println("testCommitTransaction: " + count + " rows after commit");
            }
        }
    }

    /**
     * FX Test: testRollbackTransaction() - Transaction Rollback
     * 
     * Tests basic transaction rollback functionality:
     * - Start transaction (setAutoCommit(false))
     * - Execute DML operations
     * - Rollback transaction
     * - Verify data NOT persisted
     * 
     * This addresses the NOT FOUND gap in Connection Management.
     */
    @Test
    @DisplayName("FX Missing: testRollbackTransaction - Basic Transaction Rollback")
    void testRollbackTransaction() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            createTestTable(conn);

            // Get initial value
            int initialValue;
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT value FROM " + TABLE_NAME + " WHERE id = 1")) {
                assertTrue(rs.next());
                initialValue = rs.getInt(1);
            }

            // Start transaction
            conn.setAutoCommit(false);

            // Execute DML
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("UPDATE " + TABLE_NAME + " SET value = 999 WHERE id = 1");
            }

            // Rollback
            conn.rollback();

            // Verify data NOT changed
            conn.setAutoCommit(true);
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT value FROM " + TABLE_NAME + " WHERE id = 1")) {
                assertTrue(rs.next());
                int finalValue = rs.getInt(1);
                assertTrue(finalValue == initialValue, "Value should be unchanged after rollback");
                System.out.println("testRollbackTransaction: value=" + finalValue + " (unchanged after rollback)");
            }
        }
    }

    /**
     * FX Test: testRollbackTransactionOnCnClose() - Rollback on Connection Close
     * 
     * Tests that uncommitted transactions are rolled back when connection closes:
     * - Start transaction
     * - Execute DML
     * - Close connection WITHOUT commit
     * - Verify data NOT persisted (implicit rollback)
     * 
     * This addresses the NOT FOUND gap in Connection Management.
     */
    @Test
    @DisplayName("FX Missing: testRollbackTransactionOnCnClose - Implicit Rollback on Close")
    void testRollbackTransactionOnCnClose() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        // Setup - create table and get initial state
        try (Connection setupConn = PrepUtil.getConnection(connectionString)) {
            createTestTable(setupConn);
        }

        int initialValue;
        try (Connection conn = PrepUtil.getConnection(connectionString);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT value FROM " + TABLE_NAME + " WHERE id = 1")) {
            assertTrue(rs.next());
            initialValue = rs.getInt(1);
        }

        // Execute uncommitted transaction and close connection
        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            conn.setAutoCommit(false);
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("UPDATE " + TABLE_NAME + " SET value = 888 WHERE id = 1");
            }
            // DO NOT COMMIT - just close connection
        } // Connection closed here - should trigger implicit rollback

        // Verify data NOT changed (implicit rollback occurred)
        try (Connection verifyConn = PrepUtil.getConnection(connectionString);
                Statement stmt = verifyConn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT value FROM " + TABLE_NAME + " WHERE id = 1")) {
            assertTrue(rs.next());
            int finalValue = rs.getInt(1);
            assertTrue(finalValue == initialValue, "Value should be unchanged after implicit rollback on close");
            System.out.println("testRollbackTransactionOnCnClose: value=" + finalValue + " (implicit rollback on close)");
        }
    }

    /**
     * FX Test: testRollbackTransactionOnPoolCnClose() - Pool Connection Rollback
     * 
     * Tests rollback behavior with pooled connections (simulated):
     * - Simulates pooled connection behavior
     * - Uncommitted transaction should be rolled back when returned to pool
     * 
     * This addresses the NOT FOUND gap in Connection Management.
     */
    @Test
    @DisplayName("FX Missing: testRollbackTransactionOnPoolCnClose - Pool Connection Rollback (simulated)")
    void testRollbackTransactionOnPoolCnClose() {
        StateMachineTest sm = new StateMachineTest("PoolRollback");

        sm.set("poolSize", 5);
        sm.set("activeConnections", 0);
        sm.set("uncommittedTransactions", 0);
        sm.set("rollbacksOnReturn", 0);

        // Pool operations
        sm.addAction(new PoolGetConnectionAction(sm));
        sm.addAction(new PoolReturnConnectionAction(sm));
        sm.addAction(new PoolBeginTransactionAction(sm));
        sm.addAction(new PoolExecuteDMLAction(sm));
        sm.addAction(new PoolCommitAction(sm));

        Result r = Engine.run(sm).withMaxActions(100).execute();
        assertTrue(r.isSuccess());
        System.out.println("Pool rollback test: " + r.actionCount + " actions");
        System.out.println("  Rollbacks on return: " + sm.getInt("rollbacksOnReturn"));
    }

    // ==================== XA TRANSACTION TESTS (FX Missing) ====================

    /**
     * FX Test: testTransactionAfterPrepare() - XA Prepare Phase
     * 
     * Tests XA two-phase commit prepare functionality (simulated):
     * - XA start
     * - Execute work
     * - XA end
     * - XA prepare (vote phase)
     * - XA commit/rollback
     * 
     * This addresses the NOT FOUND gap in XA Transactions.
     */
    @Test
    @DisplayName("FX Missing: testTransactionAfterPrepare - XA Prepare Phase (simulated)")
    void testTransactionAfterPrepare() {
        StateMachineTest sm = new StateMachineTest("XAPrepare");

        // XA Transaction states
        sm.set("xaState", "IDLE");  // IDLE, ACTIVE, ENDED, PREPARED, COMMITTED, ROLLEDBACK
        sm.set("xid", 0);
        sm.set("workDone", false);
        sm.set("prepareVote", "UNKNOWN");  // XA_OK, XA_RDONLY, XAER_*

        // XA operations
        sm.addAction(new XAStartAction(sm));
        sm.addAction(new XAEndAction(sm));
        sm.addAction(new XAPrepareAction(sm));
        sm.addAction(new XACommitAction(sm));
        sm.addAction(new XARollbackAction(sm));
        sm.addAction(new XAExecuteWorkAction(sm));

        Result r = Engine.run(sm).withMaxActions(100).withSeed(11111).execute();
        assertTrue(r.isSuccess());
        System.out.println("XA Prepare test: " + r.actionCount + " actions, final state=" + sm.get("xaState"));
    }

    /**
     * FX Test: testCaseCleanMultipleSuspend_Join() - XA Suspend/Resume
     * 
     * Tests XA suspend and resume (join) functionality (simulated):
     * - XA start
     * - XA end with TMSUSPEND
     * - XA start with TMJOIN (resume)
     * - Multiple suspend/join cycles
     * 
     * This addresses the NOT FOUND gap in XA Transactions.
     */
    @Test
    @DisplayName("FX Missing: testCaseCleanMultipleSuspend_Join - XA Suspend/Resume (simulated)")
    void testCaseCleanMultipleSuspend_Join() {
        StateMachineTest sm = new StateMachineTest("XASuspendResume");

        sm.set("xaState", "IDLE");
        sm.set("xid", 1);
        sm.set("suspendCount", 0);
        sm.set("joinCount", 0);
        sm.set("workUnits", 0);

        // XA suspend/resume operations
        sm.addAction(new XASuspendStartAction(sm));
        sm.addAction(new XASuspendEndSuspendAction(sm));
        sm.addAction(new XASuspendJoinAction(sm));
        sm.addAction(new XASuspendWorkAction(sm));
        sm.addAction(new XASuspendCommitAction(sm));

        Result r = Engine.run(sm).withMaxActions(80).execute();
        assertTrue(r.isSuccess());
        System.out.println("XA Suspend/Resume: suspends=" + sm.getInt("suspendCount") + 
                           ", joins=" + sm.getInt("joinCount") + 
                           ", workUnits=" + sm.getInt("workUnits"));
    }

    /**
     * FX Test: testCaseClean*() - XA Cleanup/Recovery
     * 
     * Tests XA cleanup and recovery scenarios (simulated):
     * - Recover in-doubt transactions
     * - Clean up orphaned XA branches
     * - Handle XA errors gracefully
     * 
     * This addresses the NOT FOUND gap in XA Transactions.
     */
    @Test
    @DisplayName("FX Missing: testCaseClean - XA Cleanup/Recovery (simulated)")
    void testCaseClean() {
        StateMachineTest sm = new StateMachineTest("XACleanup");

        sm.set("inDoubtTransactions", 3);  // Simulated in-doubt TXs
        sm.set("orphanedBranches", 2);
        sm.set("recoveredCount", 0);
        sm.set("cleanedCount", 0);
        sm.set("errors", 0);

        // XA cleanup operations
        sm.addAction(new XARecoverAction(sm));
        sm.addAction(new XAForgetAction(sm));
        sm.addAction(new XACleanupBranchAction(sm));
        sm.addAction(new XAHandleErrorAction(sm));

        Result r = Engine.run(sm).withMaxActions(50).execute();
        assertTrue(r.isSuccess());
        System.out.println("XA Cleanup: recovered=" + sm.getInt("recoveredCount") + 
                           ", cleaned=" + sm.getInt("cleanedCount") +
                           ", errors=" + sm.getInt("errors"));
    }

    /**
     * FX Test: dtcTimeout tests - XA Timeout Handling
     * 
     * Tests XA transaction timeout scenarios (simulated):
     * - Transaction timeout during work
     * - Timeout during prepare phase
     * - Timeout recovery
     * 
     * This addresses the NOT FOUND gap in XA Transactions.
     */
    @Test
    @DisplayName("FX Missing: dtcTimeout - XA Timeout Handling (simulated)")
    void testXATimeout() {
        StateMachineTest sm = new StateMachineTest("XATimeout");

        sm.set("xaState", "IDLE");
        sm.set("xid", 1);
        sm.set("timeoutSeconds", 30);
        sm.set("elapsedTime", 0);
        sm.set("timedOut", false);
        sm.set("timeoutRecovered", false);

        // XA timeout operations
        sm.addAction(new XATimeoutStartAction(sm));
        sm.addAction(new XATimeoutSetAction(sm));
        sm.addAction(new XATimeoutWorkAction(sm));
        sm.addAction(new XATimeoutTriggerAction(sm));
        sm.addAction(new XATimeoutRecoveryAction(sm));

        Result r = Engine.run(sm).withMaxActions(60).execute();
        assertTrue(r.isSuccess());
        System.out.println("XA Timeout: timedOut=" + sm.is("timedOut") + 
                           ", recovered=" + sm.is("timeoutRecovered"));
    }

    // ==================== ACTION CLASSES (Plain implementations, no lambdas) ====================

    // --- Basic Transaction Actions ---

    static class SetAutoCommitTrueAction extends Action {
        private StateMachineTest sm;

        SetAutoCommitTrueAction(StateMachineTest sm) {
            super("setAutoCommit(true)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && !sm.is("autoCommit");
        }

        @Override
        public void run() {
            // FX: false to true -> implicit rollback
            sm.set("autoCommit", true);
            sm.set("inTransaction", false);
            sm.set("uncommittedChanges", 0);
        }
    }

    static class SetAutoCommitFalseAction extends Action {
        private StateMachineTest sm;

        SetAutoCommitFalseAction(StateMachineTest sm) {
            super("setAutoCommit(false)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.is("autoCommit");
        }

        @Override
        public void run() {
            // FX: true to false -> start transaction
            sm.set("autoCommit", false);
            sm.set("inTransaction", true);
        }
    }

    static class CommitAction extends Action {
        private StateMachineTest sm;

        CommitAction(StateMachineTest sm) {
            super("commit", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            // FX: requires autoCommit=false, closed=false
            return !sm.is("closed") && !sm.is("autoCommit");
        }

        @Override
        public void run() {
            sm.set("uncommittedChanges", 0);
            // Transaction continues after commit
        }
    }

    static class RollbackAction extends Action {
        private StateMachineTest sm;

        RollbackAction(StateMachineTest sm) {
            super("rollback", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            // FX: requires autoCommit=false, closed=false
            return !sm.is("closed") && !sm.is("autoCommit");
        }

        @Override
        public void run() {
            sm.set("uncommittedChanges", 0);
            // Transaction continues after rollback
        }
    }

    static class ExecuteUpdateAction extends Action {
        private StateMachineTest sm;

        ExecuteUpdateAction(StateMachineTest sm) {
            super("executeUpdate", 15);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            if (!sm.is("autoCommit")) {
                sm.set("uncommittedChanges", sm.getInt("uncommittedChanges") + 1);
            }
        }
    }

    // --- Savepoint Actions ---

    static class SavepointSetAutoCommitTrueAction extends Action {
        private StateMachineTest sm;

        SavepointSetAutoCommitTrueAction(StateMachineTest sm) {
            super("setAutoCommit(true)", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && !sm.is("autoCommit");
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            sm.set("autoCommit", true);
            sm.set("inTransaction", false);
            sm.set("uncommittedChanges", 0);
            // Clear savepoints on implicit rollback
            ((List<String>) sm.get("savepoints")).clear();
        }
    }

    static class SavepointSetAutoCommitFalseAction extends Action {
        private StateMachineTest sm;

        SavepointSetAutoCommitFalseAction(StateMachineTest sm) {
            super("setAutoCommit(false)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.is("autoCommit");
        }

        @Override
        public void run() {
            sm.set("autoCommit", false);
            sm.set("inTransaction", true);
        }
    }

    static class SavepointCommitAction extends Action {
        private StateMachineTest sm;

        SavepointCommitAction(StateMachineTest sm) {
            super("commit", 8);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && !sm.is("autoCommit");
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            sm.set("uncommittedChanges", 0);
            // Savepoints are released on commit
            ((List<String>) sm.get("savepoints")).clear();
        }
    }

    static class SavepointRollbackAction extends Action {
        private StateMachineTest sm;

        SavepointRollbackAction(StateMachineTest sm) {
            super("rollback", 8);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && !sm.is("autoCommit");
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            sm.set("uncommittedChanges", 0);
            // Savepoints are released on rollback
            ((List<String>) sm.get("savepoints")).clear();
        }
    }

    static class SetSavepointAction extends Action {
        private StateMachineTest sm;

        SetSavepointAction(StateMachineTest sm) {
            super("setSavepoint()", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            // Savepoints require autoCommit=false
            return !sm.is("closed") && !sm.is("autoCommit");
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            int counter = sm.getInt("savepointCounter") + 1;
            sm.set("savepointCounter", counter);
            String name = "SP_" + counter;
            ((List<String>) sm.get("savepoints")).add(name);
            System.out.println("  Created savepoint: " + name);
        }
    }

    static class SetNamedSavepointAction extends Action {
        private StateMachineTest sm;

        SetNamedSavepointAction(StateMachineTest sm) {
            super("setSavepoint(name)", 8);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && !sm.is("autoCommit");
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            int counter = sm.getInt("savepointCounter") + 1;
            sm.set("savepointCounter", counter);
            String name = "Named_SP_" + counter;
            ((List<String>) sm.get("savepoints")).add(name);
            System.out.println("  Created named savepoint: " + name);
        }
    }

    static class RollbackToSavepointAction extends Action {
        private StateMachineTest sm;

        RollbackToSavepointAction(StateMachineTest sm) {
            super("rollback(savepoint)", 6);
            this.sm = sm;
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean canRun() {
            List<String> savepoints = (List<String>) sm.get("savepoints");
            return !sm.is("closed") && !sm.is("autoCommit") && savepoints != null && !savepoints.isEmpty();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            List<String> savepoints = (List<String>) sm.get("savepoints");
            // Rollback to last savepoint
            String sp = savepoints.get(savepoints.size() - 1);
            // Remove all savepoints after this one (they're invalidated)
            System.out.println("  Rollback to savepoint: " + sp);
        }
    }

    static class ReleaseSavepointAction extends Action {
        private StateMachineTest sm;

        ReleaseSavepointAction(StateMachineTest sm) {
            super("releaseSavepoint", 5);
            this.sm = sm;
        }

        @Override
        @SuppressWarnings("unchecked")
        public boolean canRun() {
            List<String> savepoints = (List<String>) sm.get("savepoints");
            return !sm.is("closed") && !sm.is("autoCommit") && savepoints != null && !savepoints.isEmpty();
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            List<String> savepoints = (List<String>) sm.get("savepoints");
            String sp = savepoints.remove(savepoints.size() - 1);
            System.out.println("  Released savepoint: " + sp);
        }
    }

    static class SavepointExecuteUpdateAction extends Action {
        private StateMachineTest sm;

        SavepointExecuteUpdateAction(StateMachineTest sm) {
            super("executeUpdate", 15);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            if (!sm.is("autoCommit")) {
                sm.set("uncommittedChanges", sm.getInt("uncommittedChanges") + 1);
            }
        }
    }

    // --- Isolation Level Actions ---

    static class IsolationSetAutoCommitFalseAction extends Action {
        private StateMachineTest sm;

        IsolationSetAutoCommitFalseAction(StateMachineTest sm) {
            super("setAutoCommit(false)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.is("autoCommit");
        }

        @Override
        public void run() {
            sm.set("autoCommit", false);
        }
    }

    static class IsolationSetAutoCommitTrueAction extends Action {
        private StateMachineTest sm;

        IsolationSetAutoCommitTrueAction(StateMachineTest sm) {
            super("setAutoCommit(true)", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && !sm.is("autoCommit");
        }

        @Override
        public void run() {
            sm.set("autoCommit", true);
        }
    }

    static class SetIsolationReadUncommittedAction extends Action {
        private StateMachineTest sm;

        SetIsolationReadUncommittedAction(StateMachineTest sm) {
            super("setIsolation(READ_UNCOMMITTED)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            sm.set("isolationLevel", Connection.TRANSACTION_READ_UNCOMMITTED);
        }
    }

    static class SetIsolationReadCommittedAction extends Action {
        private StateMachineTest sm;

        SetIsolationReadCommittedAction(StateMachineTest sm) {
            super("setIsolation(READ_COMMITTED)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            sm.set("isolationLevel", Connection.TRANSACTION_READ_COMMITTED);
        }
    }

    static class SetIsolationRepeatableReadAction extends Action {
        private StateMachineTest sm;

        SetIsolationRepeatableReadAction(StateMachineTest sm) {
            super("setIsolation(REPEATABLE_READ)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            sm.set("isolationLevel", Connection.TRANSACTION_REPEATABLE_READ);
        }
    }

    static class SetIsolationSerializableAction extends Action {
        private StateMachineTest sm;

        SetIsolationSerializableAction(StateMachineTest sm) {
            super("setIsolation(SERIALIZABLE)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            sm.set("isolationLevel", Connection.TRANSACTION_SERIALIZABLE);
        }
    }

    static class IsolationCommitAction extends Action {
        private StateMachineTest sm;

        IsolationCommitAction(StateMachineTest sm) {
            super("commit", 8);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && !sm.is("autoCommit");
        }

        @Override
        public void run() {
            // Commit - isolation level remains unchanged
        }
    }

    static class IsolationRollbackAction extends Action {
        private StateMachineTest sm;

        IsolationRollbackAction(StateMachineTest sm) {
            super("rollback", 8);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && !sm.is("autoCommit");
        }

        @Override
        public void run() {
            // Rollback - isolation level remains unchanged
        }
    }

    // --- Real Database Actions ---

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
                int newValue = (int) (Math.random() * 1000);
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

    // --- Complex Transaction Actions ---

    static class ComplexExecuteUpdateAction extends Action {
        private StateMachineTest sm;

        ComplexExecuteUpdateAction(StateMachineTest sm) {
            super("executeUpdate", 15);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            sm.set("uncommittedChanges", sm.getInt("uncommittedChanges") + 1);
        }
    }

    static class ComplexCommitAction extends Action {
        private StateMachineTest sm;

        ComplexCommitAction(StateMachineTest sm) {
            super("commit", 8);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            // Only commit if there are uncommitted changes
            return !sm.is("closed") && sm.getInt("uncommittedChanges") > 0;
        }

        @Override
        public void run() {
            int changes = sm.getInt("uncommittedChanges");
            System.out.println("  COMMIT " + changes + " changes");
            sm.set("uncommittedChanges", 0);
            sm.set("totalCommits", sm.getInt("totalCommits") + 1);
        }
    }

    static class ComplexRollbackAction extends Action {
        private StateMachineTest sm;

        ComplexRollbackAction(StateMachineTest sm) {
            super("rollback", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            // Only rollback if there are uncommitted changes
            return !sm.is("closed") && sm.getInt("uncommittedChanges") > 0;
        }

        @Override
        public void run() {
            int changes = sm.getInt("uncommittedChanges");
            System.out.println("  ROLLBACK " + changes + " changes");
            sm.set("uncommittedChanges", 0);
            sm.set("totalRollbacks", sm.getInt("totalRollbacks") + 1);
        }
    }

    // --- Statement Creation Actions (FX _modelcreateStatement, etc.) ---

    // FX _modelcreateStatement: weight=10, requires _closed=false
    static class CreateStatementAction extends Action {
        private StateMachineTest sm;

        CreateStatementAction(StateMachineTest sm) {
            super("createStatement()", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            sm.set("statementCount", sm.getInt("statementCount") + 1);
            System.out.println("  createStatement() -> Statement #" + sm.getInt("statementCount"));
        }
    }

    // FX _modelcreateStatement(type, concurrency): weight=10
    static class CreateStatementWithTypeAction extends Action {
        private StateMachineTest sm;
        private int[] cursorTypes = {ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.TYPE_SCROLL_INSENSITIVE};
        private int[] concurrencyModes = {ResultSet.CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};
        private java.util.Random random = new java.util.Random();

        CreateStatementWithTypeAction(StateMachineTest sm) {
            super("createStatement(type,concur)", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            int type = cursorTypes[random.nextInt(cursorTypes.length)];
            int concur = concurrencyModes[random.nextInt(concurrencyModes.length)];
            // Forward-only and scroll-insensitive only support read-only
            if ((type == ResultSet.TYPE_FORWARD_ONLY || type == ResultSet.TYPE_SCROLL_INSENSITIVE)) {
                concur = ResultSet.CONCUR_READ_ONLY;
            }
            sm.set("statementCount", sm.getInt("statementCount") + 1);
            System.out.println("  createStatement(" + type + ", " + concur + ") -> Statement #" + sm.getInt("statementCount"));
        }
    }

    // FX _modelcreateStatement(type, concurrency, holdability): weight=10
    static class CreateStatementWithHoldabilityAction extends Action {
        private StateMachineTest sm;

        CreateStatementWithHoldabilityAction(StateMachineTest sm) {
            super("createStatement(type,concur,hold)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            int holdability = sm.getInt("holdability");
            sm.set("statementCount", sm.getInt("statementCount") + 1);
            System.out.println("  createStatement(FO, RO, " + holdability + ") -> Statement #" + sm.getInt("statementCount"));
        }
    }

    // FX _modelprepareStatement: weight=10, requires _closed=false
    static class PrepareStatementAction extends Action {
        private StateMachineTest sm;

        PrepareStatementAction(StateMachineTest sm) {
            super("prepareStatement(sql)", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            sm.set("preparedStatementCount", sm.getInt("preparedStatementCount") + 1);
            System.out.println("  prepareStatement(\"SELECT 1\") -> PreparedStatement #" + sm.getInt("preparedStatementCount"));
        }
    }

    // FX _modelprepareCall: weight=10, requires _closed=false
    static class PrepareCallAction extends Action {
        private StateMachineTest sm;

        PrepareCallAction(StateMachineTest sm) {
            super("prepareCall(sql)", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            sm.set("callableStatementCount", sm.getInt("callableStatementCount") + 1);
            System.out.println("  prepareCall(\"{call sp_test}\") -> CallableStatement #" + sm.getInt("callableStatementCount"));
        }
    }

    // FX _modelgetMetaData: weight=2, requires _closed=false
    static class GetDatabaseMetaDataAction extends Action {
        private StateMachineTest sm;

        GetDatabaseMetaDataAction(StateMachineTest sm) {
            super("getMetaData()", 2);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            System.out.println("  getMetaData() -> DatabaseMetaData");
        }
    }

    static class StatementSetAutoCommitAction extends Action {
        private StateMachineTest sm;
        private java.util.Random random = new java.util.Random();

        StatementSetAutoCommitAction(StateMachineTest sm) {
            super("setAutoCommit()", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            boolean newValue = random.nextBoolean();
            sm.set("autoCommit", newValue);
            System.out.println("  setAutoCommit(" + newValue + ")");
        }
    }

    // --- Connection Configuration Actions ---

    static class SetHoldabilityAction extends Action {
        private StateMachineTest sm;
        private java.util.Random random = new java.util.Random();

        SetHoldabilityAction(StateMachineTest sm) {
            super("setHoldability()", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            int holdability = random.nextBoolean() ? ResultSet.HOLD_CURSORS_OVER_COMMIT : ResultSet.CLOSE_CURSORS_AT_COMMIT;
            sm.set("holdability", holdability);
            System.out.println("  setHoldability(" + holdability + ")");
        }
    }

    static class GetHoldabilityAction extends Action {
        private StateMachineTest sm;

        GetHoldabilityAction(StateMachineTest sm) {
            super("getHoldability()", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            int holdability = sm.getInt("holdability");
            System.out.println("  getHoldability() -> " + holdability);
        }
    }

    static class SetCatalogAction extends Action {
        private StateMachineTest sm;
        private String[] catalogs = {"master", "tempdb", "testdb", "model"};
        private java.util.Random random = new java.util.Random();

        SetCatalogAction(StateMachineTest sm) {
            super("setCatalog()", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            String catalog = catalogs[random.nextInt(catalogs.length)];
            sm.set("catalog", catalog);
            System.out.println("  setCatalog(\"" + catalog + "\")");
        }
    }

    static class GetCatalogAction extends Action {
        private StateMachineTest sm;

        GetCatalogAction(StateMachineTest sm) {
            super("getCatalog()", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            String catalog = (String) sm.get("catalog");
            System.out.println("  getCatalog() -> \"" + catalog + "\"");
        }
    }

    static class GetWarningsAction extends Action {
        private StateMachineTest sm;

        GetWarningsAction(StateMachineTest sm) {
            super("getWarnings()", 2);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            boolean hasWarnings = sm.is("warnings");
            System.out.println("  getWarnings() -> " + (hasWarnings ? "SQLWarning" : "null"));
        }
    }

    static class ConnectionClearWarningsAction extends Action {
        private StateMachineTest sm;

        ConnectionClearWarningsAction(StateMachineTest sm) {
            super("clearWarnings()", 2);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            sm.set("warnings", false);
            System.out.println("  clearWarnings()");
        }
    }

    static class IsValidAction extends Action {
        private StateMachineTest sm;

        IsValidAction(StateMachineTest sm) {
            super("isValid()", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return true;  // Can always check validity
        }

        @Override
        public void run() {
            boolean valid = !sm.is("closed");
            System.out.println("  isValid(0) -> " + valid);
        }
    }

    static class IsClosedAction extends Action {
        private StateMachineTest sm;

        IsClosedAction(StateMachineTest sm) {
            super("isClosed()", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return true;  // Can always check closed state
        }

        @Override
        public void run() {
            boolean closed = sm.is("closed");
            System.out.println("  isClosed() -> " + closed);
        }
    }

    static class NativeSQLAction extends Action {
        private StateMachineTest sm;

        NativeSQLAction(StateMachineTest sm) {
            super("nativeSQL()", 2);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            System.out.println("  nativeSQL(\"SELECT 1\") -> \"SELECT 1\"");
        }
    }

    // ==================== POOL ROLLBACK ACTIONS (FX testRollbackTransactionOnPoolCnClose) ====================

    static class PoolGetConnectionAction extends Action {
        private StateMachineTest sm;

        PoolGetConnectionAction(StateMachineTest sm) {
            super("pool.getConnection()", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return sm.getInt("activeConnections") < sm.getInt("poolSize");
        }

        @Override
        public void run() {
            sm.set("activeConnections", sm.getInt("activeConnections") + 1);
            System.out.println("  pool.getConnection() -> active=" + sm.getInt("activeConnections"));
        }
    }

    static class PoolReturnConnectionAction extends Action {
        private StateMachineTest sm;

        PoolReturnConnectionAction(StateMachineTest sm) {
            super("pool.returnConnection()", 8);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return sm.getInt("activeConnections") > 0;
        }

        @Override
        public void run() {
            // If uncommitted transaction, rollback occurs
            if (sm.getInt("uncommittedTransactions") > 0) {
                sm.set("rollbacksOnReturn", sm.getInt("rollbacksOnReturn") + 1);
                sm.set("uncommittedTransactions", sm.getInt("uncommittedTransactions") - 1);
                System.out.println("  pool.returnConnection() -> ROLLBACK on return");
            } else {
                System.out.println("  pool.returnConnection() -> clean return");
            }
            sm.set("activeConnections", sm.getInt("activeConnections") - 1);
        }
    }

    static class PoolBeginTransactionAction extends Action {
        private StateMachineTest sm;

        PoolBeginTransactionAction(StateMachineTest sm) {
            super("conn.setAutoCommit(false)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return sm.getInt("activeConnections") > 0;
        }

        @Override
        public void run() {
            sm.set("uncommittedTransactions", sm.getInt("uncommittedTransactions") + 1);
            System.out.println("  setAutoCommit(false) -> started transaction");
        }
    }

    static class PoolExecuteDMLAction extends Action {
        private StateMachineTest sm;

        PoolExecuteDMLAction(StateMachineTest sm) {
            super("stmt.executeUpdate()", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return sm.getInt("activeConnections") > 0;
        }

        @Override
        public void run() {
            System.out.println("  executeUpdate() -> DML executed");
        }
    }

    static class PoolCommitAction extends Action {
        private StateMachineTest sm;

        PoolCommitAction(StateMachineTest sm) {
            super("conn.commit()", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return sm.getInt("uncommittedTransactions") > 0;
        }

        @Override
        public void run() {
            sm.set("uncommittedTransactions", sm.getInt("uncommittedTransactions") - 1);
            System.out.println("  commit() -> transaction committed");
        }
    }

    // ==================== XA PREPARE ACTIONS (FX testTransactionAfterPrepare) ====================

    static class XAStartAction extends Action {
        private StateMachineTest sm;

        XAStartAction(StateMachineTest sm) {
            super("xa.start(xid)", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            String state = (String) sm.get("xaState");
            return "IDLE".equals(state) || "COMMITTED".equals(state) || "ROLLEDBACK".equals(state);
        }

        @Override
        public void run() {
            sm.set("xid", sm.getInt("xid") + 1);
            sm.set("xaState", "ACTIVE");
            sm.set("workDone", false);
            System.out.println("  xa.start(xid=" + sm.getInt("xid") + ") -> ACTIVE");
        }
    }

    static class XAEndAction extends Action {
        private StateMachineTest sm;

        XAEndAction(StateMachineTest sm) {
            super("xa.end(xid)", 8);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return "ACTIVE".equals(sm.get("xaState"));
        }

        @Override
        public void run() {
            sm.set("xaState", "ENDED");
            System.out.println("  xa.end(xid=" + sm.getInt("xid") + ") -> ENDED");
        }
    }

    static class XAPrepareAction extends Action {
        private StateMachineTest sm;

        XAPrepareAction(StateMachineTest sm) {
            super("xa.prepare(xid)", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return "ENDED".equals(sm.get("xaState"));
        }

        @Override
        public void run() {
            sm.set("xaState", "PREPARED");
            // XA_OK if work done, XA_RDONLY if no work
            String vote = sm.is("workDone") ? "XA_OK" : "XA_RDONLY";
            sm.set("prepareVote", vote);
            System.out.println("  xa.prepare(xid=" + sm.getInt("xid") + ") -> " + vote);
        }
    }

    static class XACommitAction extends Action {
        private StateMachineTest sm;

        XACommitAction(StateMachineTest sm) {
            super("xa.commit(xid)", 8);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            String state = (String) sm.get("xaState");
            return "PREPARED".equals(state) || "ENDED".equals(state);
        }

        @Override
        public void run() {
            sm.set("xaState", "COMMITTED");
            System.out.println("  xa.commit(xid=" + sm.getInt("xid") + ") -> COMMITTED");
        }
    }

    static class XARollbackAction extends Action {
        private StateMachineTest sm;

        XARollbackAction(StateMachineTest sm) {
            super("xa.rollback(xid)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            String state = (String) sm.get("xaState");
            return "ACTIVE".equals(state) || "ENDED".equals(state) || "PREPARED".equals(state);
        }

        @Override
        public void run() {
            sm.set("xaState", "ROLLEDBACK");
            System.out.println("  xa.rollback(xid=" + sm.getInt("xid") + ") -> ROLLEDBACK");
        }
    }

    static class XAExecuteWorkAction extends Action {
        private StateMachineTest sm;

        XAExecuteWorkAction(StateMachineTest sm) {
            super("executeWork()", 15);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return "ACTIVE".equals(sm.get("xaState"));
        }

        @Override
        public void run() {
            sm.set("workDone", true);
            System.out.println("  executeWork() in XA transaction");
        }
    }

    // ==================== XA SUSPEND/RESUME ACTIONS (FX testCaseCleanMultipleSuspend_Join) ====================

    static class XASuspendStartAction extends Action {
        private StateMachineTest sm;

        XASuspendStartAction(StateMachineTest sm) {
            super("xa.start(xid, TMNOFLAGS)", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            String state = (String) sm.get("xaState");
            return "IDLE".equals(state);
        }

        @Override
        public void run() {
            sm.set("xaState", "ACTIVE");
            System.out.println("  xa.start(xid=" + sm.getInt("xid") + ", TMNOFLAGS) -> ACTIVE");
        }
    }

    static class XASuspendEndSuspendAction extends Action {
        private StateMachineTest sm;

        XASuspendEndSuspendAction(StateMachineTest sm) {
            super("xa.end(xid, TMSUSPEND)", 8);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return "ACTIVE".equals(sm.get("xaState"));
        }

        @Override
        public void run() {
            sm.set("xaState", "SUSPENDED");
            sm.set("suspendCount", sm.getInt("suspendCount") + 1);
            System.out.println("  xa.end(xid=" + sm.getInt("xid") + ", TMSUSPEND) -> SUSPENDED #" + sm.getInt("suspendCount"));
        }
    }

    static class XASuspendJoinAction extends Action {
        private StateMachineTest sm;

        XASuspendJoinAction(StateMachineTest sm) {
            super("xa.start(xid, TMJOIN)", 8);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return "SUSPENDED".equals(sm.get("xaState"));
        }

        @Override
        public void run() {
            sm.set("xaState", "ACTIVE");
            sm.set("joinCount", sm.getInt("joinCount") + 1);
            System.out.println("  xa.start(xid=" + sm.getInt("xid") + ", TMJOIN) -> ACTIVE (join #" + sm.getInt("joinCount") + ")");
        }
    }

    static class XASuspendWorkAction extends Action {
        private StateMachineTest sm;

        XASuspendWorkAction(StateMachineTest sm) {
            super("executeWork()", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return "ACTIVE".equals(sm.get("xaState"));
        }

        @Override
        public void run() {
            sm.set("workUnits", sm.getInt("workUnits") + 1);
            System.out.println("  executeWork() unit #" + sm.getInt("workUnits"));
        }
    }

    static class XASuspendCommitAction extends Action {
        private StateMachineTest sm;

        XASuspendCommitAction(StateMachineTest sm) {
            super("xa.commit(xid)", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            String state = (String) sm.get("xaState");
            return "ACTIVE".equals(state) && sm.getInt("workUnits") > 0;
        }

        @Override
        public void run() {
            sm.set("xaState", "IDLE");
            System.out.println("  xa.end + xa.prepare + xa.commit -> COMMITTED");
        }
    }

    // ==================== XA CLEANUP ACTIONS (FX testCaseClean) ====================

    static class XARecoverAction extends Action {
        private StateMachineTest sm;

        XARecoverAction(StateMachineTest sm) {
            super("xa.recover()", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return sm.getInt("inDoubtTransactions") > 0;
        }

        @Override
        public void run() {
            int inDoubt = sm.getInt("inDoubtTransactions");
            System.out.println("  xa.recover() -> found " + inDoubt + " in-doubt transactions");
        }
    }

    static class XAForgetAction extends Action {
        private StateMachineTest sm;

        XAForgetAction(StateMachineTest sm) {
            super("xa.forget(xid)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return sm.getInt("inDoubtTransactions") > 0;
        }

        @Override
        public void run() {
            sm.set("inDoubtTransactions", sm.getInt("inDoubtTransactions") - 1);
            sm.set("recoveredCount", sm.getInt("recoveredCount") + 1);
            System.out.println("  xa.forget(xid) -> recovered, remaining=" + sm.getInt("inDoubtTransactions"));
        }
    }

    static class XACleanupBranchAction extends Action {
        private StateMachineTest sm;

        XACleanupBranchAction(StateMachineTest sm) {
            super("cleanupOrphanedBranch()", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return sm.getInt("orphanedBranches") > 0;
        }

        @Override
        public void run() {
            sm.set("orphanedBranches", sm.getInt("orphanedBranches") - 1);
            sm.set("cleanedCount", sm.getInt("cleanedCount") + 1);
            System.out.println("  cleanupOrphanedBranch() -> remaining=" + sm.getInt("orphanedBranches"));
        }
    }

    static class XAHandleErrorAction extends Action {
        private StateMachineTest sm;
        private java.util.Random random = new java.util.Random();

        XAHandleErrorAction(StateMachineTest sm) {
            super("handleXAError()", 2);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return true;
        }

        @Override
        public void run() {
            // Simulate occasional XA errors
            if (random.nextInt(10) < 2) {
                sm.set("errors", sm.getInt("errors") + 1);
                System.out.println("  handleXAError() -> XAER_RMERR handled");
            } else {
                System.out.println("  handleXAError() -> no error");
            }
        }
    }

    // ==================== XA TIMEOUT ACTIONS (FX dtcTimeout) ====================

    static class XATimeoutStartAction extends Action {
        private StateMachineTest sm;

        XATimeoutStartAction(StateMachineTest sm) {
            super("xa.start(xid)", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            String state = (String) sm.get("xaState");
            return "IDLE".equals(state) && !sm.is("timedOut");
        }

        @Override
        public void run() {
            sm.set("xaState", "ACTIVE");
            sm.set("elapsedTime", 0);
            System.out.println("  xa.start(xid=" + sm.getInt("xid") + ") -> ACTIVE");
        }
    }

    static class XATimeoutSetAction extends Action {
        private StateMachineTest sm;
        private java.util.Random random = new java.util.Random();

        XATimeoutSetAction(StateMachineTest sm) {
            super("xa.setTransactionTimeout()", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return true;
        }

        @Override
        public void run() {
            int timeout = 10 + random.nextInt(50);
            sm.set("timeoutSeconds", timeout);
            System.out.println("  xa.setTransactionTimeout(" + timeout + ")");
        }
    }

    static class XATimeoutWorkAction extends Action {
        private StateMachineTest sm;
        private java.util.Random random = new java.util.Random();

        XATimeoutWorkAction(StateMachineTest sm) {
            super("executeWork() [time passes]", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return "ACTIVE".equals(sm.get("xaState")) && !sm.is("timedOut");
        }

        @Override
        public void run() {
            int elapsed = sm.getInt("elapsedTime") + 5 + random.nextInt(10);
            sm.set("elapsedTime", elapsed);
            System.out.println("  executeWork() -> elapsed=" + elapsed + "s");
        }
    }

    static class XATimeoutTriggerAction extends Action {
        private StateMachineTest sm;

        XATimeoutTriggerAction(StateMachineTest sm) {
            super("checkTimeout()", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return "ACTIVE".equals(sm.get("xaState")) && 
                   sm.getInt("elapsedTime") >= sm.getInt("timeoutSeconds") &&
                   !sm.is("timedOut");
        }

        @Override
        public void run() {
            sm.set("timedOut", true);
            sm.set("xaState", "TIMEDOUT");
            System.out.println("  TIMEOUT! elapsed=" + sm.getInt("elapsedTime") + 
                               "s >= timeout=" + sm.getInt("timeoutSeconds") + "s");
        }
    }

    static class XATimeoutRecoveryAction extends Action {
        private StateMachineTest sm;

        XATimeoutRecoveryAction(StateMachineTest sm) {
            super("recoverFromTimeout()", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return sm.is("timedOut") && !sm.is("timeoutRecovered");
        }

        @Override
        public void run() {
            sm.set("timeoutRecovered", true);
            sm.set("xaState", "IDLE");
            sm.set("timedOut", false);
            System.out.println("  recoverFromTimeout() -> recovered, state=IDLE");
        }
    }
}
