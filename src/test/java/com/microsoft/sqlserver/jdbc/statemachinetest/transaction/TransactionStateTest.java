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

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Action;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.DataCache;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Engine;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Result;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateKey;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Self-Contained Transaction State Machine Tests
 * 
 * This test demonstrates a SELF-CONTAINED design where:
 * - States are defined inline (not in separate enum)
 * - Actions are inner classes within this test
 * - All behavior is visible in one file for easier understanding
 * 
 * Migrated from FX Framework:
 * - Based on fxConnection model actions
 * - Implements Model-Based Testing (MBT) for JDBC transaction operations
 * 
 * Test scenarios covered:
 * - setAutoCommit(true/false) - Toggle auto-commit mode
 * - commit() - Commit transaction (requires autoCommit=false)
 * - rollback() - Rollback transaction (requires autoCommit=false)
 */
@Tag(Constants.legacyFX)
public class TransactionStateTest extends AbstractTest {

    private static final String TABLE_NAME_CONST = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SM_Transaction_Test"));

    // ========================================================================
    // STATE DEFINITIONS - Self-contained, specific to this test
    // ========================================================================

    /** State key for Connection object */
    private static final StateKey CONN = () -> "conn";

    /** State key for autoCommit mode (Boolean) */
    private static final StateKey AUTO_COMMIT = () -> "autoCommit";

    /** State key for connection closed status (Boolean) */
    private static final StateKey CLOSED = () -> "closed";

    /** State key for whether initial data has been inserted (Boolean) */
    private static final StateKey TABLE_READY = () -> "tableReady";

    /**
     * Reference to the state machine's DataCache for post-test assertions.
     * The DataCache is owned by StateMachineTest; this reference is obtained
     * via {@code sm.getDataCache()} after SM construction.
     *
     * Row 0 columns:
     * - State: "conn", "autoCommit", "closed", "tableReady"
     * - Data (set by InsertAction): "value", "pendingValue", "commitCount",
     * "rollbackCount"
     */
    private static DataCache cache;

    // ========================================================================
    // TEST SETUP & TEARDOWN
    // ========================================================================

    @BeforeAll
    static void setupTests() throws Exception {
        setConnection();
    }

    @AfterAll
    static void cleanupTests() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            TestUtils.dropTableIfExists(TABLE_NAME_CONST, connection.createStatement());
        }
    }

    /**
     * Creates the test table schema (no data - InsertAction handles that).
     */
    private static void createTestTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME_CONST, stmt);
            stmt.execute("CREATE TABLE " + TABLE_NAME_CONST + " (id INT PRIMARY KEY, value INT)");
        }
    }

    // ========================================================================
    // ACTION DEFINITIONS - Self-contained inner classes for this test
    // ========================================================================

    /**
     * Action: Insert initial data row into the test table.
     * 
     * This is the first action that must run — all other data actions
     * (UPDATE, SELECT, COMMIT, ROLLBACK) require TABLE_READY=true,
     * which this action sets after inserting.
     * 
     * After run():
     * - DB has: id=1, value=100
     * - DataCache row 0: {value=100, pendingValue=null, commitCount=0,
     * rollbackCount=0}
     * - TABLE_READY=true (unlocks other actions)
     */
    private static class InsertAction extends Action {
        private static final int INITIAL_VALUE = 100;

        InsertAction() {
            this(10);
        }

        InsertAction(int weight) {
            super("insert", weight);
        }

        @Override
        public boolean canRun() {
            // Can only run once — before data is initialized
            return !isState(CLOSED) && !isState(TABLE_READY);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) getState(CONN);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME_CONST + " VALUES (1, " + INITIAL_VALUE + ")");
            }

            // Initialize data columns in DataCache row 0
            dataCache.updateValue(0, "value", INITIAL_VALUE);
            dataCache.updateValue(0, "pendingValue", null);
            dataCache.updateValue(0, "commitCount", 0);
            dataCache.updateValue(0, "rollbackCount", 0);

            setState(TABLE_READY, true);
            System.out.println("  + INSERT id=1, value=" + INITIAL_VALUE + " — table data initialized");
        }
    }

    /**
     * Action: Disable auto-commit to enable transaction mode
     */
    private static class SetAutoCommitFalseAction extends Action {

        SetAutoCommitFalseAction() {
            this(5);
        }

        SetAutoCommitFalseAction(int weight) {
            super("setAutoCommit(false)", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && isState(AUTO_COMMIT);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) getState(CONN);
            conn.setAutoCommit(false);
            setState(AUTO_COMMIT, false);
            System.out.println("  → setAutoCommit(false)");
        }
    }

    /**
     * Action: Enable auto-commit (implicitly commits pending changes)
     */
    private static class SetAutoCommitTrueAction extends Action {

        SetAutoCommitTrueAction() {
            this(5);
        }

        SetAutoCommitTrueAction(int weight) {
            super("setAutoCommit(true)", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && !isState(AUTO_COMMIT);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) getState(CONN);
            Integer pending = (Integer) dataCache.getValue(0, "pendingValue");

            conn.setAutoCommit(true);
            setState(AUTO_COMMIT, true);

            // setAutoCommit(true) implicitly commits pending transaction
            if (pending != null) {
                dataCache.updateValue(0, "value", pending);
                dataCache.updateValue(0, "pendingValue", null);
            }

            Object expected = dataCache.getValue(0, "value");
            System.out.println("  → setAutoCommit(true) - implicit commit" +
                    (expected != null ? ", expected=" + expected : ""));
        }
    }

    /**
     * Action: Commit transaction and validate data persistence
     * 
     * What this commits:
     * - Any previous UPDATE statement executed by ExecuteUpdateAction
     * - Makes pendingValue permanent → updates committed value in DataCache
     * 
     * Example flow:
     * 1. ExecuteUpdateAction: UPDATE table SET value=542 → pendingValue=542
     * 2. CommitAction: commit() → value=542, pendingValue=null in DataCache
     * 3. Validation: SELECT value FROM table → confirms DB has 542
     */
    private static class CommitAction extends Action {

        CommitAction() {
            this(10);
        }

        CommitAction(int weight) {
            super("commit", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && !isState(AUTO_COMMIT) && isState(TABLE_READY);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) getState(CONN);
            Integer pending = (Integer) dataCache.getValue(0, "pendingValue");
            Object oldExpected = dataCache.getValue(0, "value");

            // This commits any UPDATE executed earlier (tracked in pendingValue)
            conn.commit();

            // Promote pending → committed in DataCache
            if (pending != null) {
                System.out.println(String.format("  ✓ commit() - committing UPDATE: value %s → %s",
                        oldExpected, pending));
                dataCache.updateValue(0, "value", pending);
                dataCache.updateValue(0, "pendingValue", null);
            } else {
                System.out.println("  ✓ commit() - no pending changes");
            }

            // Track commit count in DataCache
            int commitCount = (Integer) dataCache.getValue(0, "commitCount");
            dataCache.updateValue(0, "commitCount", commitCount + 1);
        }

        @Override
        public void validate() throws SQLException {
            // Validate: Database value matches expected (committed) value in DataCache
            Integer expected = (Integer) dataCache.getValue(0, "value");
            if (expected != null) {
                Connection conn = (Connection) getState(CONN);
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT value FROM " + TABLE_NAME_CONST + " WHERE id = 1")) {
                    if (rs.next()) {
                        int actual = rs.getInt("value");
                        assertExpected(actual, expected.intValue(),
                                "After COMMIT: DB value should match committed value");
                    }
                }
            }
        }
    }

    /**
     * Action: Rollback transaction and validate data reverts
     * 
     * What this rolls back:
     * - Any previous UPDATE statement executed by ExecuteUpdateAction
     * - Discards pendingValue, DB reverts to committed value in DataCache
     * 
     * Example flow:
     * 1. Initial state: DataCache row 0: {value=100}, DB has 100
     * 2. ExecuteUpdateAction: UPDATE table SET value=542 → pendingValue=542
     * 3. RollbackAction: rollback() → pendingValue cleared, DB still has 100
     * 4. Validation: SELECT value FROM table → confirms DB reverted to 100
     */
    private static class RollbackAction extends Action {

        RollbackAction() {
            this(10);
        }

        RollbackAction(int weight) {
            super("rollback", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && !isState(AUTO_COMMIT) && isState(TABLE_READY);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) getState(CONN);
            Integer pending = (Integer) dataCache.getValue(0, "pendingValue");
            Object expected = dataCache.getValue(0, "value");

            // This rolls back any UPDATE executed earlier (tracked in pendingValue)
            conn.rollback();

            if (pending != null) {
                System.out.println(String.format("  ↺ rollback() - discarding UPDATE: value %s (pending) reverts to %s",
                        pending, expected));
            } else {
                System.out.println("  ↺ rollback() - no pending changes to discard");
            }

            dataCache.updateValue(0, "pendingValue", null); // Discard pending

            // Track rollback count in DataCache
            int rollbackCount = (Integer) dataCache.getValue(0, "rollbackCount");
            dataCache.updateValue(0, "rollbackCount", rollbackCount + 1);
        }

        @Override
        public void validate() throws SQLException {
            // Validate: Database value matches expected (reverted) value in DataCache
            Integer expected = (Integer) dataCache.getValue(0, "value");
            if (expected != null) {
                Connection conn = (Connection) getState(CONN);
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT value FROM " + TABLE_NAME_CONST + " WHERE id = 1")) {
                    if (rs.next()) {
                        int actual = rs.getInt("value");
                        assertExpected(actual, expected.intValue(),
                                "After ROLLBACK: DB value should revert to last committed value");
                    }
                }
            }
        }
    }

    /**
     * Action: Execute UPDATE to generate data for commit/rollback testing
     * 
     * What this does:
     * - Executes: UPDATE table SET value=<random> WHERE id=1
     * - Sets pendingValue=<random> in DataCache (uncommitted until commit/rollback)
     * - This UPDATE is what CommitAction commits or RollbackAction discards
     * 
     * Example flow:
     * 1. Initial: DataCache row 0: {value=100}, DB has 100
     * 2. ExecuteUpdateAction: UPDATE table SET value=542
     * → pendingValue=542, DB sees 542 but not committed
     * 3. Next action decides fate:
     * - CommitAction → DB permanently has 542
     * - RollbackAction → DB reverts to 100
     * 
     * In autoCommit mode:
     * - UPDATE is immediately committed (no pending state)
     * - DataCache updated immediately
     */
    private static class ExecuteUpdateAction extends Action {

        ExecuteUpdateAction() {
            this(15);
        }

        ExecuteUpdateAction(int weight) {
            super("executeUpdate", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && isState(TABLE_READY);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) getState(CONN);
            Object oldValue = dataCache.getValue(0, "value");
            int newValue = getRandom().nextInt(1000);

            try (Statement stmt = conn.createStatement()) {
                // THIS IS THE STATEMENT that commit() will commit or rollback() will discard
                int rows = stmt.executeUpdate(
                        "UPDATE " + TABLE_NAME_CONST + " SET value = " + newValue + " WHERE id = 1");

                // Track as pending (uncommitted) in DataCache
                dataCache.updateValue(0, "pendingValue", newValue);

                // In autoCommit mode, changes are immediately committed
                if (isState(AUTO_COMMIT)) {
                    dataCache.updateValue(0, "value", newValue);
                    dataCache.updateValue(0, "pendingValue", null);
                    System.out.println(String.format("  ✎ UPDATE value %s → %s (auto-committed)",
                            oldValue, newValue));
                } else {
                    System.out.println(String.format("  ✎ UPDATE value %s → %s (PENDING - awaiting commit/rollback)",
                            oldValue, newValue));
                }
            }
        }
    }

    /**
     * Action: Execute SELECT to verify current database state
     */
    private static class SelectAction extends Action {
        private int lastValue;
        private boolean hasValue;

        SelectAction() {
            this(5);
        }

        SelectAction(int weight) {
            super("select", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && isState(TABLE_READY);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) getState(CONN);

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT value FROM " + TABLE_NAME_CONST + " WHERE id = 1")) {
                if (rs.next()) {
                    lastValue = rs.getInt("value");
                    hasValue = true;
                    System.out.println("  ⚬ SELECT → value=" + lastValue);
                }
            }
        }

        @Override
        public void validate() throws SQLException {
            if (!hasValue)
                return;

            Integer expected = (Integer) dataCache.getValue(0, "value");
            Integer pending = (Integer) dataCache.getValue(0, "pendingValue");

            // In a transaction, SELECT sees uncommitted changes
            Integer valueToCheck = (pending != null) ? pending : expected;

            if (valueToCheck != null) {
                assertExpected(lastValue, valueToCheck.intValue(),
                        "SELECT should see " + (pending != null ? "pending" : "committed") + " value");
            }
        }
    }

    // ========================================================================
    // TEST CASES
    // ========================================================================

    @Test
    @DisplayName("FX Model: Real Database - Transaction Commit/Rollback")
    void testRealDatabaseTransaction() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            // ===== DATA SETUP: Initialize test table =====
            // Creates table with initial data: id=1, value=100
            createTestTable(conn);

            // SM owns DataCache internally (row 0 = empty state row)
            StateMachineTest sm = new StateMachineTest("RealTransaction");
            cache = sm.getDataCache();
            cache.updateValue(0, CONN.key(), conn);
            cache.updateValue(0, AUTO_COMMIT.key(), true);
            cache.updateValue(0, CLOSED.key(), false);
            cache.updateValue(0, TABLE_READY.key(), false);

            // InsertAction runs first (TABLE_READY=false) and initializes DataCache, then
            // unlocks the rest
            sm.addAction(new InsertAction());
            sm.addAction(new SetAutoCommitFalseAction());
            sm.addAction(new SetAutoCommitTrueAction());
            sm.addAction(new CommitAction());
            sm.addAction(new RollbackAction());
            sm.addAction(new ExecuteUpdateAction());
            sm.addAction(new SelectAction());

            // ===== EXECUTION =====
            Result result = Engine.run(sm).withMaxActions(50).execute();

            // Cleanup - ensure autocommit is restored
            conn.setAutoCommit(true);

            System.out.println("\n=== Test Complete ===");
            System.out.println("Real DB transaction test: " + result.actionCount + " actions");
            assertTrue(result.isSuccess());
        }
    }

    @Test
    @DisplayName("Transaction Validation Test")
    void testTransactionWithValidation() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            // ===== DATA SETUP: Initialize test table =====
            // Creates table with: id=1, value=100
            createTestTable(conn);

            // SM owns DataCache internally (row 0 = empty state row)
            StateMachineTest sm = new StateMachineTest("TransactionValidation");
            cache = sm.getDataCache();
            cache.updateValue(0, CONN.key(), conn);
            cache.updateValue(0, AUTO_COMMIT.key(), true);
            cache.updateValue(0, CLOSED.key(), false);
            cache.updateValue(0, TABLE_READY.key(), false);

            // InsertAction initializes DataCache (including counters), then other actions
            // unlock
            sm.addAction(new InsertAction());
            sm.addAction(new SetAutoCommitFalseAction());
            sm.addAction(new SetAutoCommitTrueAction());
            sm.addAction(new CommitAction());
            sm.addAction(new RollbackAction());
            sm.addAction(new ExecuteUpdateAction());
            sm.addAction(new SelectAction());

            // Vlaidate -> Action within, At last also .... Divang
            // ===== EXECUTION =====
            Result result = Engine.run(sm).withMaxActions(50).withSeed(12345).execute();

            conn.setAutoCommit(true);

            System.out.println("\n=== Test Complete ===");
            System.out.println("Transaction validation test: " + result.actionCount + " actions");
            assertTrue(result.isSuccess());
        }
    }

    /**
     * FX Framework Migration: TCModelFocusCommit
     * 
     * This test migrates the commit-focused Model-Based Testing pattern
     * from FX's DistributedTransactions.TCModelFocusCommit.
     * 
     * Pattern:
     * - Uses WEIGHTED actions to focus heavily on commit operations
     * - Tracks successful commit count (similar to FX's ModelRequirement)
     * - Runs for many iterations to stress-test commit behavior
     * - Validates data persistence after commits
     * 
     * Original FX Pattern:
     * - engine.Options.MaxActions = 10,000
     * - Weighted actions: commit=100, end=100, start=100
     * - ModelRequirement: stop after N successful commits
     */
    @Test
    @DisplayName("FX Migration: TCModelFocusCommit - Commit-Focused Testing")
    void testCommitFocused() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            createTestTable(conn);

            // SM owns DataCache internally (row 0 = empty state row)
            StateMachineTest sm = new StateMachineTest("CommitFocused");
            cache = sm.getDataCache();
            cache.updateValue(0, CONN.key(), conn);
            cache.updateValue(0, AUTO_COMMIT.key(), true);
            cache.updateValue(0, CLOSED.key(), false);
            cache.updateValue(0, TABLE_READY.key(), false);

            // InsertAction initializes DataCache (including counters), then commit-focused
            // weighted actions take over
            sm.addAction(new InsertAction());
            sm.addAction(new SetAutoCommitFalseAction(10));
            sm.addAction(new SetAutoCommitTrueAction(5));
            sm.addAction(new CommitAction(50)); // HIGH: focus on commits
            sm.addAction(new RollbackAction(2)); // Very low
            sm.addAction(new ExecuteUpdateAction(30)); // Moderate - generate data
            sm.addAction(new SelectAction(3));

            // Run with high iteration count (FX used 10,000, using 500 for practicality)
            // Use seed for reproducibility
            Result result = Engine.run(sm).withMaxActions(500).withSeed(67890).execute();

            conn.setAutoCommit(true);

            // Validate results from DataCache
            Integer finalCommitCount = (Integer) cache.getValue(0, "commitCount");
            Integer finalRollbackCount = (Integer) cache.getValue(0, "rollbackCount");

            System.out.println("\n=== Commit-Focused Test Results ===");
            System.out.println("Total actions executed: " + result.actionCount);
            System.out.println("Successful commits: " + (finalCommitCount != null ? finalCommitCount : 0));
            System.out.println("Rollbacks: " + (finalRollbackCount != null ? finalRollbackCount : 0));
            System.out.println("Commit focus ratio: "
                    + String.format("%.1f%%",
                            (finalCommitCount != null ? finalCommitCount : 0) * 100.0
                                    / (result.actionCount > 0 ? result.actionCount : 1)));

            assertTrue(result.isSuccess(), "State machine test should complete successfully");

            // Assert commit-focused behavior: should have executed multiple commits
            // FX TCModelFocusCommit tracked "total committed transactions" via
            // ModelRequirement
            assertTrue(finalCommitCount != null && finalCommitCount >= 5,
                    String.format("Commit-focused test should execute at least 5 commits, got %d",
                            finalCommitCount != null ? finalCommitCount : 0));

            // Commit count should dominate rollback count (weighted heavily toward commits)
            assertTrue(finalCommitCount > finalRollbackCount,
                    String.format("Commits (%d) should exceed rollbacks (%d) in commit-focused test",
                            finalCommitCount, finalRollbackCount));
        }
    }
}
