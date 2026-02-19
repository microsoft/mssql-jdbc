/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.transaction;

import static com.microsoft.sqlserver.jdbc.statemachinetest.transaction.TransactionState.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.statemachinetest.core.Action;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest;


/**
 * Transaction-related actions for state machine testing with validation.
 * 
 * These actions model JDBC transaction operations with data validation:
 * - setAutoCommit(true/false) - Transaction mode control
 * - commit() - Commits pending changes and verifies they persist
 * - rollback() - Rolls back pending changes and verifies they're undone
 * - executeUpdate() - Updates data and tracks pending value
 * - executeQuery() - Selects data and verifies against expected value
 * 
 * Validation approach:
 * - Track EXPECTED_VALUE (last committed value)
 * - Track PENDING_VALUE (value after UPDATE but not yet committed)
 * - After COMMIT: EXPECTED_VALUE = PENDING_VALUE
 * - After ROLLBACK: PENDING_VALUE is discarded
 * - SELECT always verifies actual value matches EXPECTED_VALUE
 * 
 * @see TransactionState for state keys
 */
public final class TransactionActions {

    private TransactionActions() {
        // Utility class - prevent instantiation
    }

    // ==================== Pure Driver Actions ====================
    // These actions represent pure JDBC driver behavior without validation

    /**
     * SetAutoCommitFalseAction - Pure driver behavior.
     * Extends Action directly since no validation is needed.
     */
    public static class SetAutoCommitFalseAction extends Action {
        private final StateMachineTest sm;

        public SetAutoCommitFalseAction(StateMachineTest sm) {
            super("setAutoCommit(false)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && sm.isState(AUTO_COMMIT);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) sm.getStateValue(CONN);
            conn.setAutoCommit(false); // Pure driver behavior
            sm.setState(AUTO_COMMIT, false);
            System.out.println("  setAutoCommit(false)");
        }
    }

    /**
     * SetAutoCommitTrueAction - Pure driver behavior.
     * Extends Action directly since no validation is needed.
     */
    public static class SetAutoCommitTrueAction extends Action {
        private final StateMachineTest sm;

        public SetAutoCommitTrueAction(StateMachineTest sm) {
            super("setAutoCommit(true)", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && !sm.isState(AUTO_COMMIT);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) sm.getStateValue(CONN);
            conn.setAutoCommit(true); // Pure driver behavior
            sm.setState(AUTO_COMMIT, true);
            System.out.println("  setAutoCommit(true) - implicit commit");
        }
    }

    // ==================== Validated Actions ====================
    // These actions validate results against expected data

    /**
     * CommitAction - Validated action.
     * Verifies committed data persists.
     */
    public static class CommitAction extends Action {
        private final StateMachineTest sm;
        private String tableName;

        public CommitAction(StateMachineTest sm) {
            super("commit", 10);
            this.sm = sm;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && !sm.isState(AUTO_COMMIT);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) sm.getStateValue(CONN);
            Integer pending = (Integer) sm.getStateValue(PENDING_VALUE);
            conn.commit();

            // After commit, pending changes become expected (committed)
            if (pending != null) {
                sm.setState(EXPECTED_VALUE, pending);
                sm.setState(PENDING_VALUE, null); // Clear pending
            }

            Integer expectedVal = (Integer) sm.getStateValue(EXPECTED_VALUE);
            System.out.println("  commit() - changes committed" +
                    (expectedVal != null ? ", expected=" + expectedVal : ""));
        }

        @Override
        public void validate() throws SQLException {
            // Framework calls this after run()
            Integer expectedVal = (Integer) sm.getStateValue(EXPECTED_VALUE);
            if (expectedVal != null && tableName != null) {
                Connection conn = (Connection) sm.getStateValue(CONN);
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT value FROM " + tableName + " WHERE id = 1")) {
                    if (rs.next()) {
                        int actualValue = rs.getInt("value");
                        assertExpected(actualValue, expectedVal.intValue(),
                                String.format("After COMMIT: value should be %d", expectedVal));
                    }
                }
            }
        }
    }

    public static class RollbackAction extends Action {
        private final StateMachineTest sm;
        private String tableName;

        public RollbackAction(StateMachineTest sm) {
            super("rollback", 10);
            this.sm = sm;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && !sm.isState(AUTO_COMMIT);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) sm.getStateValue(CONN);
            Integer pending = (Integer) sm.getStateValue(PENDING_VALUE);
            conn.rollback();

            // After rollback, discard pending changes
            sm.setState(PENDING_VALUE, null);

            Integer expectedVal = (Integer) sm.getStateValue(EXPECTED_VALUE);
            System.out.println("  rollback() - changes discarded" +
                    (pending != null ? ", reverted to " + expectedVal : ""));
        }

        @Override
        public void validate() throws SQLException {
            // Framework calls this after run()
            Integer expectedVal = (Integer) sm.getStateValue(EXPECTED_VALUE);
            if (expectedVal != null && tableName != null) {
                Connection conn = (Connection) sm.getStateValue(CONN);
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT value FROM " + tableName + " WHERE id = 1")) {
                    if (rs.next()) {
                        int actualValue = rs.getInt("value");
                        assertExpected(actualValue, expectedVal.intValue(),
                                String.format("After ROLLBACK: value should revert to %d", expectedVal));
                    }
                }
            }
        }
    }

    public static class ExecuteUpdateAction extends Action {
        private final StateMachineTest sm;
        private String tableName;

        public ExecuteUpdateAction(StateMachineTest sm) {
            super("executeUpdate", 15);
            this.sm = sm;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) sm.getStateValue(CONN);
            String table = (tableName != null) ? tableName : "test_table";
            int newValue = sm.getRandom().nextInt(1000);

            try (Statement stmt = conn.createStatement()) {
                int rows = stmt.executeUpdate("UPDATE " + table + " SET value = " + newValue + " WHERE id = 1");

                // FX Pattern: Track pending value (not yet committed)
                sm.setState(PENDING_VALUE, newValue);

                // In autoCommit mode, changes are immediately committed
                if (sm.isState(AUTO_COMMIT)) {
                    sm.setState(EXPECTED_VALUE, newValue);
                    sm.setState(PENDING_VALUE, null);
                }

                System.out.println("  executeUpdate -> " + rows + " rows, value=" + newValue +
                        (sm.isState(AUTO_COMMIT) ? " (committed)" : " (pending)"));
            }
        }
    }

    public static class SelectAction extends Action {
        private final StateMachineTest sm;
        private String tableName;

        public SelectAction(StateMachineTest sm) {
            super("executeQuery", 10);
            this.sm = sm;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) sm.getStateValue(CONN);
            String table = (tableName != null) ? tableName : "test_table";

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT value FROM " + table + " WHERE id = 1")) {
                if (rs.next()) {
                    int actualValue = rs.getInt("value");
                    System.out.println("  SELECT -> value=" + actualValue + " (verified)");
                }
            }
        }

        @Override
        public void validate() throws SQLException {
            // Framework calls this after run()
            Integer expectedVal = (Integer) sm.getStateValue(EXPECTED_VALUE);
            Integer pendingVal = (Integer) sm.getStateValue(PENDING_VALUE);

            // In a transaction, SELECT sees pending (uncommitted) changes
            // So we validate against PENDING if it exists, otherwise EXPECTED
            Integer valueToCheck = (pendingVal != null) ? pendingVal : expectedVal;

            if (valueToCheck != null && tableName != null) {
                Connection conn = (Connection) sm.getStateValue(CONN);
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT value FROM " + tableName + " WHERE id = 1")) {
                    if (rs.next()) {
                        int actualValue = rs.getInt("value");
                        assertExpected(actualValue, valueToCheck.intValue(),
                                String.format("SELECT: expected %svalue %d",
                                        (pendingVal != null ? "pending " : "committed "), valueToCheck));
                    }
                }
            }
        }
    }
}
