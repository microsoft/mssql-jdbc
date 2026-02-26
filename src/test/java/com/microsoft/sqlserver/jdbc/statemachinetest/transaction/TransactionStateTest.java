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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
 * MBT transaction tests: commit, rollback, autoCommit with
 * INSERT/UPDATE/DELETE.
 */
@Tag(Constants.legacyFX)
public class TransactionStateTest extends AbstractTest {

    private static final String TABLE_NAME_CONST = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SM_Transaction_Test"));

    // STATE DEFINITIONS
    private static final StateKey CONN = () -> "conn";
    private static final StateKey AUTO_COMMIT = () -> "autoCommit";
    private static final StateKey CLOSED = () -> "closed";

    /**
     * Row 0: State (conn, autoCommit, closed, commitCount, rollbackCount, nextId)
     * Rows 1-N: Per-row data (id, value, pendingValue, rowState:
     * committed|pending_insert|pending_delete|removed)
     */
    private static DataCache cache;

    @BeforeAll
    static void setupTests() throws Exception {
        setConnection();
        createTestTable(connection);
    }

    @AfterAll
    static void cleanupTests() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            TestUtils.dropTableIfExists(TABLE_NAME_CONST, connection.createStatement());
        }
    }

    // TEST CASE

    /**
     * E-commerce transaction simulation: orders are inserted, prices updated,
     * cancelled orders deleted — all within transactions that commit or rollback
     * randomly. Weights reflect real-world frequency: inserts and updates are
     * frequent, deletes and rollbacks are rare, commits dominate.
     */
    @Test
    @DisplayName("E-Commerce Transaction Simulation")
    void testECommerceTransactions() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            StateMachineTest sm = new StateMachineTest("ECommerceTransactions");
            cache = sm.getDataCache();
            cache.updateValue(0, CONN.key(), conn);
            cache.updateValue(0, AUTO_COMMIT.key(), true);
            cache.updateValue(0, CLOSED.key(), false);
            cache.updateValue(0, "commitCount", 0);
            cache.updateValue(0, "rollbackCount", 0);
            cache.updateValue(0, "nextId", 1);

            sm.addAction(new InsertAction(20)); // new orders arrive frequently
            sm.addAction(new SetAutoCommitFalseAction(8)); // begin transaction
            sm.addAction(new SetAutoCommitTrueAction(4)); // implicit commit (less common)
            sm.addAction(new CommitAction(30)); // most transactions succeed
            sm.addAction(new RollbackAction(5)); // occasional cancellations
            sm.addAction(new ExecuteUpdateAction(25)); // price/quantity updates
            sm.addAction(new SelectAction(5)); // order lookups
            sm.addAction(new DeleteAction(3)); // order removals are rare

            Result result = Engine.run(sm).withMaxActions(200).execute();
            conn.setAutoCommit(true);

            Integer commitCount = (Integer) cache.getValue(0, "commitCount");
            Integer rollbackCount = (Integer) cache.getValue(0, "rollbackCount");

            System.out.println(String.format("Result: actions=%d, commits=%d, rollbacks=%d",
                    result.actionCount, commitCount != null ? commitCount : 0,
                    rollbackCount != null ? rollbackCount : 0));

            assertTrue(result.isSuccess(), "State machine test should complete successfully");
            assertTrue(commitCount != null && commitCount >= 3,
                    String.format("Expected at least 3 commits, got %d",
                            commitCount != null ? commitCount : 0));
        }
    }

    // ACTION DEFINITIONS

    /** Insert a new row (repeatable). */
    private static class InsertAction extends Action {
        private int insertedId;
        private int insertedValue;

        InsertAction(int weight) {
            super("insert", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) getState(CONN);
            int nextId = (Integer) dataCache.getValue(0, "nextId");
            insertedId = nextId;
            insertedValue = getRandom().nextInt(1000);

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME_CONST
                        + " VALUES (" + insertedId + ", " + insertedValue + ")");
            }

            String rowState = isState(AUTO_COMMIT) ? "committed" : "pending_insert";
            Map<String, Object> row = new HashMap<>();
            row.put("id", insertedId);
            row.put("value", insertedValue);
            row.put("pendingValue", null);
            row.put("rowState", rowState);
            dataCache.addRow(row);
            dataCache.updateValue(0, "nextId", nextId + 1);

            System.out.println(String.format("INSERT id=%d val=%d %s",
                    insertedId, insertedValue, isState(AUTO_COMMIT) ? "committed" : "pending"));
        }

        @Override
        public void validate() throws SQLException {
            Connection conn = (Connection) getState(CONN);
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT value FROM " + TABLE_NAME_CONST + " WHERE id = " + insertedId)) {
                assertTrue(rs.next(), "Inserted row id=" + insertedId + " should be visible");
                assertExpected(rs.getInt("value"), insertedValue,
                        "Inserted row id=" + insertedId + " value mismatch");
            }
        }
    }

    /** Disable auto-commit to enable transaction mode. */
    private static class SetAutoCommitFalseAction extends Action {

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
            System.out.println("setAutoCommit(false)");
        }
    }

    /** Enable auto-commit (implicitly commits all pending changes). */
    private static class SetAutoCommitTrueAction extends Action {

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

            conn.setAutoCommit(true);
            setState(AUTO_COMMIT, true);

            promoteAllPending(dataCache);
            System.out.println("setAutoCommit(true) - implicit commit");
        }

        @Override
        public void validate() throws SQLException {
            Connection conn = (Connection) getState(CONN);
            validateAllRows(this, conn);
        }
    }

    /** Commit transaction: promotes all pending changes to committed. */
    private static class CommitAction extends Action {

        CommitAction(int weight) {
            super("commit", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && !isState(AUTO_COMMIT);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) getState(CONN);

            conn.commit();
            promoteAllPending(dataCache);
            int commitCount = (Integer) dataCache.getValue(0, "commitCount");
            dataCache.updateValue(0, "commitCount", commitCount + 1);

            System.out.println("commit #" + (commitCount + 1));
        }

        @Override
        public void validate() throws SQLException {
            Connection conn = (Connection) getState(CONN);
            validateAllRows(this, conn);
        }
    }

    /** Rollback transaction: discards all pending changes. */
    private static class RollbackAction extends Action {

        RollbackAction(int weight) {
            super("rollback", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && !isState(AUTO_COMMIT);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) getState(CONN);

            conn.rollback();
            discardAllPending(dataCache);
            int rollbackCount = (Integer) dataCache.getValue(0, "rollbackCount");
            dataCache.updateValue(0, "rollbackCount", rollbackCount + 1);

            System.out.println("rollback #" + (rollbackCount + 1));
        }

        @Override
        public void validate() throws SQLException {
            Connection conn = (Connection) getState(CONN);
            validateAllRows(this, conn);
        }
    }

    /**
     * UPDATE a random visible row. Auto-committed immediately or pending until
     * commit/rollback.
     */
    private static class ExecuteUpdateAction extends Action {
        private int updatedId;
        private int updatedValue;

        ExecuteUpdateAction(int weight) {
            super("executeUpdate", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && hasVisibleRows(dataCache);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) getState(CONN);
            List<Integer> visible = getVisibleRowIndices(dataCache);
            int targetIdx = visible.get(getRandom().nextInt(visible.size()));
            updatedId = (Integer) dataCache.getValue(targetIdx, "id");
            Object oldValue = dataCache.getValue(targetIdx, "value");
            updatedValue = getRandom().nextInt(1000);

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate(
                        "UPDATE " + TABLE_NAME_CONST + " SET value = " + updatedValue + " WHERE id = " + updatedId);
                dataCache.updateValue(targetIdx, "pendingValue", updatedValue);
                if (isState(AUTO_COMMIT)) {
                    dataCache.updateValue(targetIdx, "value", updatedValue);
                    dataCache.updateValue(targetIdx, "pendingValue", null);
                    System.out.println(String.format("UPDATE id=%d %s->%s committed",
                            updatedId, oldValue, updatedValue));
                } else {
                    System.out.println(String.format("UPDATE id=%d %s->%s pending",
                            updatedId, oldValue, updatedValue));
                }
            }
        }

        @Override
        public void validate() throws SQLException {
            Connection conn = (Connection) getState(CONN);
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT value FROM " + TABLE_NAME_CONST + " WHERE id = " + updatedId)) {
                assertTrue(rs.next(), "Updated row id=" + updatedId + " should be visible");
                assertExpected(rs.getInt("value"), updatedValue,
                        "Updated row id=" + updatedId + " value mismatch");
            }
        }
    }

    /** SELECT a random visible row and verify value against DataCache. */
    private static class SelectAction extends Action {
        private int lastValue;
        private int lastRowIndex;
        private boolean hasValue;

        SelectAction(int weight) {
            super("select", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && hasVisibleRows(dataCache);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) getState(CONN);
            List<Integer> visible = getVisibleRowIndices(dataCache);
            lastRowIndex = visible.get(getRandom().nextInt(visible.size()));
            int targetId = (Integer) dataCache.getValue(lastRowIndex, "id");
            hasValue = false;

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT value FROM " + TABLE_NAME_CONST + " WHERE id = " + targetId)) {
                if (rs.next()) {
                    lastValue = rs.getInt("value");
                    hasValue = true;
                    System.out.println("SELECT id=" + targetId + " val=" + lastValue);
                }
            }
        }

        @Override
        public void validate() throws SQLException {
            if (!hasValue)
                return;

            Integer expected = (Integer) dataCache.getValue(lastRowIndex, "value");
            Integer pending = (Integer) dataCache.getValue(lastRowIndex, "pendingValue");

            Integer valueToCheck = (pending != null) ? pending : expected;

            if (valueToCheck != null) {
                int rowId = (Integer) dataCache.getValue(lastRowIndex, "id");
                assertExpected(lastValue, valueToCheck.intValue(),
                        "SELECT id=" + rowId + " should see "
                                + (pending != null ? "pending" : "committed") + " value");
            }
        }
    }

    /**
     * DELETE a random visible row. Committed rows become pending_delete;
     * pending_insert rows are removed.
     */
    private static class DeleteAction extends Action {
        private int deletedId;

        DeleteAction(int weight) {
            super("delete", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && hasVisibleRows(dataCache);
        }

        @Override
        public void run() throws SQLException {
            Connection conn = (Connection) getState(CONN);
            List<Integer> visible = getVisibleRowIndices(dataCache);
            int targetIdx = visible.get(getRandom().nextInt(visible.size()));
            deletedId = (Integer) dataCache.getValue(targetIdx, "id");
            String currentState = (String) dataCache.getValue(targetIdx, "rowState");

            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DELETE FROM " + TABLE_NAME_CONST + " WHERE id = " + deletedId);
            }

            if (isState(AUTO_COMMIT) || "pending_insert".equals(currentState)) {
                dataCache.updateValue(targetIdx, "rowState", "removed");
                dataCache.updateValue(targetIdx, "pendingValue", null);
                System.out.println(String.format("DELETE id=%d %s",
                        deletedId, isState(AUTO_COMMIT) ? "committed" : "cancelled pending"));
            } else {
                dataCache.updateValue(targetIdx, "rowState", "pending_delete");
                dataCache.updateValue(targetIdx, "pendingValue", null);
                System.out.println(String.format("DELETE id=%d pending", deletedId));
            }
        }

        @Override
        public void validate() throws SQLException {
            Connection conn = (Connection) getState(CONN);
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT COUNT(*) FROM " + TABLE_NAME_CONST + " WHERE id = " + deletedId)) {
                rs.next();
                assertExpected(rs.getInt(1), 0,
                        "Deleted row id=" + deletedId + " should not be visible");
            }
        }
    }

    // UTILITY METHODS

    private static void createTestTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME_CONST, stmt);
            stmt.execute("CREATE TABLE " + TABLE_NAME_CONST + " (id INT PRIMARY KEY, value INT)");
        }
    }

    /**
     * Returns DataCache row indices where rowState is "committed" or
     * "pending_insert".
     */
    private static List<Integer> getVisibleRowIndices(DataCache dc) {
        List<Integer> visible = new ArrayList<>();
        for (int i = 1; i < dc.getRowCount(); i++) {
            String state = (String) dc.getValue(i, "rowState");
            if ("committed".equals(state) || "pending_insert".equals(state)) {
                visible.add(i);
            }
        }
        return visible;
    }

    private static boolean hasVisibleRows(DataCache dc) {
        return !getVisibleRowIndices(dc).isEmpty();
    }

    /**
     * Commit semantics: pending_insert→committed, pending_delete→removed,
     * pendingValue→value.
     */
    private static void promoteAllPending(DataCache dc) {
        for (int i = 1; i < dc.getRowCount(); i++) {
            String rowState = (String) dc.getValue(i, "rowState");
            if ("pending_insert".equals(rowState)) {
                dc.updateValue(i, "rowState", "committed");
            } else if ("pending_delete".equals(rowState)) {
                dc.updateValue(i, "rowState", "removed");
            }
            Integer pending = (Integer) dc.getValue(i, "pendingValue");
            if (pending != null) {
                dc.updateValue(i, "value", pending);
                dc.updateValue(i, "pendingValue", null);
            }
        }
    }

    /**
     * Rollback semantics: pending_insert→removed, pending_delete→committed,
     * pendingValue discarded.
     */
    private static void discardAllPending(DataCache dc) {
        for (int i = 1; i < dc.getRowCount(); i++) {
            String rowState = (String) dc.getValue(i, "rowState");
            if ("pending_insert".equals(rowState)) {
                dc.updateValue(i, "rowState", "removed");
            } else if ("pending_delete".equals(rowState)) {
                dc.updateValue(i, "rowState", "committed");
            }
            dc.updateValue(i, "pendingValue", null);
        }
    }

    /** Validates committed rows in DataCache match DB via Map-based comparison. */
    private static void validateAllRows(Action action, Connection conn) throws SQLException {
        DataCache dc = action.getDataCache();

        Map<Integer, Integer> expected = new HashMap<>();
        for (int i = 1; i < dc.getRowCount(); i++) {
            String rowState = (String) dc.getValue(i, "rowState");
            if ("committed".equals(rowState)) {
                expected.put((Integer) dc.getValue(i, "id"), (Integer) dc.getValue(i, "value"));
            }
        }

        Map<Integer, Integer> actual = new HashMap<>();
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT id, value FROM " + TABLE_NAME_CONST + " ORDER BY id")) {
            while (rs.next()) {
                actual.put(rs.getInt("id"), rs.getInt("value"));
            }
        }

        action.assertExpected(actual.size(), expected.size(),
                String.format("Row count mismatch: DB has %d rows, expected %d", actual.size(), expected.size()));
        for (Map.Entry<Integer, Integer> entry : expected.entrySet()) {
            Integer actualValue = actual.get(entry.getKey());
            action.assertExpected(actualValue != null ? actualValue : -1, entry.getValue(),
                    String.format("Row id=%d: expected %d", entry.getKey(), entry.getValue()));
        }
    }
}
