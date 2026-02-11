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
 * Transaction-related actions for state machine testing.
 * 
 * These actions model JDBC transaction operations:
 * - setAutoCommit(true/false)
 * - commit()
 * - rollback()
 * - executeUpdate()
 * - executeQuery()
 * 
 * @see TransactionState for state keys
 */
public final class TransactionActions {

    private TransactionActions() {
        // Utility class - prevent instantiation
    }

    // ==================== Action Classes ====================

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
            conn.setAutoCommit(false);
            sm.setState(AUTO_COMMIT, false);
            System.out.println("  setAutoCommit(false)");
        }
    }

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
            conn.setAutoCommit(true);
            sm.setState(AUTO_COMMIT, true);
            System.out.println("  setAutoCommit(true) - implicit commit");
        }
    }

    public static class CommitAction extends Action {
        private final StateMachineTest sm;

        public CommitAction(StateMachineTest sm) {
            super("commit", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && !sm.isState(AUTO_COMMIT);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) sm.getStateValue(CONN);
            conn.commit();
            System.out.println("  commit()");
        }
    }

    public static class RollbackAction extends Action {
        private final StateMachineTest sm;

        public RollbackAction(StateMachineTest sm) {
            super("rollback", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && !sm.isState(AUTO_COMMIT);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) sm.getStateValue(CONN);
            conn.rollback();
            System.out.println("  rollback()");
        }
    }

    public static class ExecuteUpdateAction extends Action {
        private final StateMachineTest sm;

        public ExecuteUpdateAction(StateMachineTest sm) {
            super("executeUpdate", 15);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) sm.getStateValue(CONN);
            String tableName = (String) sm.getStateValue(TABLE_NAME);
            try (Statement stmt = conn.createStatement()) {
                int newValue = sm.getRandom().nextInt(1000);
                int rows = stmt.executeUpdate("UPDATE " + tableName + " SET value = " + newValue + " WHERE id = 1");
                System.out.println("  executeUpdate -> " + rows + " rows, value=" + newValue);
            }
        }
    }

    public static class SelectAction extends Action {
        private final StateMachineTest sm;

        public SelectAction(StateMachineTest sm) {
            super("executeQuery", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) sm.getStateValue(CONN);
            String tableName = (String) sm.getStateValue(TABLE_NAME);
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT value FROM " + tableName + " WHERE id = 1")) {
                if (rs.next()) {
                    System.out.println("  SELECT -> value=" + rs.getInt("value"));
                }
            }
        }
    }
}
