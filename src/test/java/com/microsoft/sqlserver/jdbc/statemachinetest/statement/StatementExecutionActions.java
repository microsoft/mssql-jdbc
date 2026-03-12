/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.statement;

import static com.microsoft.sqlserver.jdbc.statemachinetest.statement.StatementExecutionState.*;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Action;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest;


/**
 * Statement execution actions for state machine testing. Models the FX fxStatement
 * operations for non-bug/non-VSTS statement execution scenarios.
 *
 * @see StatementExecutionState for state keys
 */
public final class StatementExecutionActions {

    private StatementExecutionActions() {}

    // ==================== Execute Actions ====================

    /** FX: fxStatement.execute(SELECT) — testSRR / testAllSRR. */
    public static class ExecuteSelectAction extends Action {
        private final StateMachineTest sm;

        public ExecuteSelectAction(StateMachineTest sm) {
            super("execute(SELECT)", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            String tableName = (String) sm.getStateValue(TABLE_NAME);
            String sql = "SELECT * FROM " + tableName;
            boolean hasResultSet = stmt.execute(sql);
            sm.setState(EXECUTED, true);
            sm.setState(HAS_RESULT_SET, hasResultSet);
            sm.setState(LAST_EXECUTE_WAS_BATCH, false);
            sm.setState(LAST_EXECUTE_WAS_UPDATE, false);
            sm.setState(LAST_EXECUTE_GENERATED_KEYS, false);
            sm.setState(QUERY_INDEX, 0);
            if (!hasResultSet) {
                throw new AssertionError("execute(SELECT) should return true, got false");
            }
            System.out.println("  execute(SELECT) -> " + hasResultSet);
        }
    }

    /** FX: fxStatement.execute(DML) — testDML / testAllDML. */
    public static class ExecuteDMLAction extends Action {
        private final StateMachineTest sm;

        public ExecuteDMLAction(StateMachineTest sm) {
            super("execute(UPDATE)", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            String tableName = (String) sm.getStateValue(TABLE_NAME);
            int newValue = sm.getRandom().nextInt(10000);
            int rowId = sm.getRandom().nextInt(sm.getStateInt(ROW_COUNT)) + 1;
            String sql = "UPDATE " + tableName + " SET value = " + newValue + " WHERE id = " + rowId;
            boolean hasResultSet = stmt.execute(sql);
            sm.setState(EXECUTED, true);
            sm.setState(HAS_RESULT_SET, hasResultSet);
            sm.setState(LAST_EXECUTE_WAS_BATCH, false);
            sm.setState(LAST_EXECUTE_WAS_UPDATE, false);
            sm.setState(LAST_EXECUTE_GENERATED_KEYS, false);
            sm.setState(QUERY_INDEX, 0);
            if (hasResultSet) {
                throw new AssertionError("execute(UPDATE) should return false, got true");
            }
            System.out.println("  execute(UPDATE id=" + rowId + ") -> " + hasResultSet);
        }
    }

    /** FX: fxStatement.executeQuery(SELECT) — testSRR with executeQuery path. */
    public static class ExecuteQueryAction extends Action {
        private final StateMachineTest sm;

        public ExecuteQueryAction(StateMachineTest sm) {
            super("executeQuery", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            String tableName = (String) sm.getStateValue(TABLE_NAME);
            String sql = "SELECT * FROM " + tableName;
            try (ResultSet rs = stmt.executeQuery(sql)) {
                // Results consumed inline — do not set EXECUTED since getMoreResults/getResultSet
                // are not valid after executeQuery.
                sm.setState(LAST_EXECUTE_WAS_BATCH, false);
                sm.setState(LAST_EXECUTE_WAS_UPDATE, false);
                sm.setState(LAST_EXECUTE_GENERATED_KEYS, false);
                sm.setState(QUERY_INDEX, 0);
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                System.out.println("  executeQuery(SELECT) -> " + count + " rows");
            }
        }
    }

    /** FX: fxStatement.executeUpdate(DML) — testDML with executeUpdate path. */
    public static class ExecuteUpdateAction extends Action {
        private final StateMachineTest sm;

        public ExecuteUpdateAction(StateMachineTest sm) {
            super("executeUpdate", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            String tableName = (String) sm.getStateValue(TABLE_NAME);
            int newValue = sm.getRandom().nextInt(10000);
            int rowId = sm.getRandom().nextInt(sm.getStateInt(ROW_COUNT)) + 1;
            String sql = "UPDATE " + tableName + " SET value = " + newValue + " WHERE id = " + rowId;
            int updateCount = stmt.executeUpdate(sql);
            sm.setState(EXECUTED, true);
            sm.setState(HAS_RESULT_SET, false);
            sm.setState(LAST_EXECUTE_WAS_BATCH, false);
            sm.setState(LAST_EXECUTE_WAS_UPDATE, true);
            sm.setState(LAST_EXECUTE_GENERATED_KEYS, false);
            sm.setState(QUERY_INDEX, 0);
            // FX: fxLog.Compare(updatecount, query.updateCount())
            if (updateCount != 1) {
                throw new AssertionError("executeUpdate should affect 1 row, got " + updateCount);
            }
            System.out.println("  executeUpdate(id=" + rowId + ") -> " + updateCount + " rows");
        }
    }

    // ==================== Execute with Generated Keys ====================

    /** FX: fxStatement.execute(sql, autokeys). */
    public static class ExecuteWithGeneratedKeysAction extends Action {
        private final StateMachineTest sm;

        public ExecuteWithGeneratedKeysAction(StateMachineTest sm) {
            super("execute(genKeys)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            String tableName = (String) sm.getStateValue(TABLE_NAME);
            int newValue = sm.getRandom().nextInt(10000);
            int rowId = sm.getRandom().nextInt(sm.getStateInt(ROW_COUNT)) + 1;
            String sql = "UPDATE " + tableName + " SET value = " + newValue + " WHERE id = " + rowId;

            int autoKeyFlag = sm.getRandom().nextBoolean()
                    ? Statement.RETURN_GENERATED_KEYS
                    : Statement.NO_GENERATED_KEYS;

            boolean hasResultSet = stmt.execute(sql, autoKeyFlag);
            sm.setState(EXECUTED, true);
            sm.setState(HAS_RESULT_SET, hasResultSet);
            sm.setState(LAST_EXECUTE_WAS_BATCH, false);
            sm.setState(LAST_EXECUTE_WAS_UPDATE, false);
            sm.setState(LAST_EXECUTE_GENERATED_KEYS, false);
            sm.setState(QUERY_INDEX, 0);
            System.out.println("  execute(UPDATE, " +
                    (autoKeyFlag == Statement.RETURN_GENERATED_KEYS ? "RETURN_KEYS" : "NO_KEYS")
                    + ") -> " + hasResultSet);
        }
    }

    /** FX: fxStatement.executeUpdate(sql, autokeys). */
    public static class ExecuteUpdateWithGeneratedKeysAction extends Action {
        private final StateMachineTest sm;

        public ExecuteUpdateWithGeneratedKeysAction(StateMachineTest sm) {
            super("executeUpdate(genKeys)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            String tableName = (String) sm.getStateValue(TABLE_NAME);
            int newValue = sm.getRandom().nextInt(10000);
            int rowId = sm.getRandom().nextInt(sm.getStateInt(ROW_COUNT)) + 1;
            String sql = "UPDATE " + tableName + " SET value = " + newValue + " WHERE id = " + rowId;

            int autoKeyFlag = sm.getRandom().nextBoolean()
                    ? Statement.RETURN_GENERATED_KEYS
                    : Statement.NO_GENERATED_KEYS;

            int updateCount = stmt.executeUpdate(sql, autoKeyFlag);
            sm.setState(EXECUTED, true);
            sm.setState(HAS_RESULT_SET, false);
            sm.setState(LAST_EXECUTE_WAS_BATCH, false);
            sm.setState(LAST_EXECUTE_WAS_UPDATE, true);
            sm.setState(LAST_EXECUTE_GENERATED_KEYS, false);
            sm.setState(QUERY_INDEX, 0);
            System.out.println("  executeUpdate(UPDATE, " +
                    (autoKeyFlag == Statement.RETURN_GENERATED_KEYS ? "RETURN_KEYS" : "NO_KEYS")
                    + ") -> " + updateCount);
        }
    }

    /** FX: fxStatement.execute(sql, int[] indexes). */
    public static class ExecuteWithColumnIndexesAction extends Action {
        private final StateMachineTest sm;

        public ExecuteWithColumnIndexesAction(StateMachineTest sm) {
            super("execute(colIndexes)", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            String tableName = (String) sm.getStateValue(TABLE_NAME);
            int newValue = sm.getRandom().nextInt(10000);
            int rowId = sm.getRandom().nextInt(sm.getStateInt(ROW_COUNT)) + 1;
            String sql = "UPDATE " + tableName + " SET value = " + newValue + " WHERE id = " + rowId;

            boolean hasResultSet = stmt.execute(sql, new int[]{1});
            sm.setState(EXECUTED, true);
            sm.setState(HAS_RESULT_SET, hasResultSet);
            sm.setState(LAST_EXECUTE_WAS_BATCH, false);
            sm.setState(LAST_EXECUTE_WAS_UPDATE, false);
            sm.setState(LAST_EXECUTE_GENERATED_KEYS, false);
            sm.setState(QUERY_INDEX, 0);
            System.out.println("  execute(UPDATE, int[]{1}) -> " + hasResultSet);
        }
    }

    /** FX: fxStatement.execute(sql, String[] columnnames). */
    public static class ExecuteWithColumnNamesAction extends Action {
        private final StateMachineTest sm;

        public ExecuteWithColumnNamesAction(StateMachineTest sm) {
            super("execute(colNames)", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            String tableName = (String) sm.getStateValue(TABLE_NAME);
            int newValue = sm.getRandom().nextInt(10000);
            int rowId = sm.getRandom().nextInt(sm.getStateInt(ROW_COUNT)) + 1;
            String sql = "UPDATE " + tableName + " SET value = " + newValue + " WHERE id = " + rowId;

            boolean hasResultSet = stmt.execute(sql, new String[]{"id"});
            sm.setState(EXECUTED, true);
            sm.setState(HAS_RESULT_SET, hasResultSet);
            sm.setState(LAST_EXECUTE_WAS_BATCH, false);
            sm.setState(LAST_EXECUTE_WAS_UPDATE, false);
            sm.setState(LAST_EXECUTE_GENERATED_KEYS, false);
            sm.setState(QUERY_INDEX, 0);
            System.out.println("  execute(UPDATE, String[]{\"id\"}) -> " + hasResultSet);
        }
    }

    /** FX: fxStatement.executeUpdate(sql, int[] indexes). */
    public static class ExecuteUpdateWithColumnIndexesAction extends Action {
        private final StateMachineTest sm;

        public ExecuteUpdateWithColumnIndexesAction(StateMachineTest sm) {
            super("executeUpdate(colIdx)", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            String tableName = (String) sm.getStateValue(TABLE_NAME);
            int newValue = sm.getRandom().nextInt(10000);
            int rowId = sm.getRandom().nextInt(sm.getStateInt(ROW_COUNT)) + 1;
            String sql = "UPDATE " + tableName + " SET value = " + newValue + " WHERE id = " + rowId;

            int updateCount = stmt.executeUpdate(sql, new int[]{1});
            sm.setState(EXECUTED, true);
            sm.setState(HAS_RESULT_SET, false);
            sm.setState(LAST_EXECUTE_WAS_BATCH, false);
            sm.setState(LAST_EXECUTE_WAS_UPDATE, true);
            sm.setState(LAST_EXECUTE_GENERATED_KEYS, false);
            sm.setState(QUERY_INDEX, 0);
            System.out.println("  executeUpdate(UPDATE, int[]{1}) -> " + updateCount);
        }
    }

    /** FX: fxStatement.executeUpdate(sql, String[] columnnames). */
    public static class ExecuteUpdateWithColumnNamesAction extends Action {
        private final StateMachineTest sm;

        public ExecuteUpdateWithColumnNamesAction(StateMachineTest sm) {
            super("executeUpdate(colNames)", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            String tableName = (String) sm.getStateValue(TABLE_NAME);
            int newValue = sm.getRandom().nextInt(10000);
            int rowId = sm.getRandom().nextInt(sm.getStateInt(ROW_COUNT)) + 1;
            String sql = "UPDATE " + tableName + " SET value = " + newValue + " WHERE id = " + rowId;

            int updateCount = stmt.executeUpdate(sql, new String[]{"id"});
            sm.setState(EXECUTED, true);
            sm.setState(HAS_RESULT_SET, false);
            sm.setState(LAST_EXECUTE_WAS_BATCH, false);
            sm.setState(LAST_EXECUTE_WAS_UPDATE, true);
            sm.setState(LAST_EXECUTE_GENERATED_KEYS, false);
            sm.setState(QUERY_INDEX, 0);
            System.out.println("  executeUpdate(UPDATE, String[]{\"id\"}) -> " + updateCount);
        }
    }

    // ==================== Batch Actions ====================

    /** FX: fxStatement.addBatch(DML) — testBatchDML. */
    public static class AddBatchAction extends Action {
        private final StateMachineTest sm;

        public AddBatchAction(StateMachineTest sm) {
            super("addBatch", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            String tableName = (String) sm.getStateValue(TABLE_NAME);
            int newValue = sm.getRandom().nextInt(10000);
            int rowId = sm.getRandom().nextInt(sm.getStateInt(ROW_COUNT)) + 1;
            String sql = "UPDATE " + tableName + " SET value = " + newValue + " WHERE id = " + rowId;
            stmt.addBatch(sql);
            System.out.println("  addBatch(UPDATE id=" + rowId + ")");
        }
    }

    /** FX: fxStatement.executeBatch() — testBatchDML. */
    public static class ExecuteBatchAction extends Action {
        private final StateMachineTest sm;

        public ExecuteBatchAction(StateMachineTest sm) {
            super("executeBatch", 8);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            int[] results = stmt.executeBatch();
            sm.setState(EXECUTED, true);
            sm.setState(HAS_RESULT_SET, false);
            sm.setState(LAST_EXECUTE_WAS_BATCH, true);
            sm.setState(LAST_EXECUTE_WAS_UPDATE, false);
            sm.setState(LAST_EXECUTE_GENERATED_KEYS, false);
            sm.setState(QUERY_INDEX, 0);
            System.out.println("  executeBatch() -> " + results.length + " results");
        }
    }

    /** FX: fxStatement.clearBatch(). */
    public static class ClearBatchAction extends Action {
        private final StateMachineTest sm;

        public ClearBatchAction(StateMachineTest sm) {
            super("clearBatch", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            stmt.clearBatch();
            System.out.println("  clearBatch()");
        }
    }

    // ==================== Result Navigation Actions ====================

    /** FX: fxStatement.getMoreResults(). */
    public static class GetMoreResultsAction extends Action {
        private final StateMachineTest sm;

        public GetMoreResultsAction(StateMachineTest sm) {
            super("getMoreResults", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && sm.isState(EXECUTED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            boolean hasMore = stmt.getMoreResults();
            sm.setState(HAS_RESULT_SET, hasMore);
            sm.setState(QUERY_INDEX, sm.getStateInt(QUERY_INDEX) + 1);
            // Advancing TDS stream invalidates generated keys
            sm.setState(LAST_EXECUTE_GENERATED_KEYS, false);
            System.out.println("  getMoreResults() -> " + hasMore);
        }
    }

    /** FX: fxStatement.getMoreResults(int) — KEEP_CURRENT_RESULT not supported (VSTS #80597). */
    public static class GetMoreResultsWithFlagAction extends Action {
        private final StateMachineTest sm;

        public GetMoreResultsWithFlagAction(StateMachineTest sm) {
            super("getMoreResults(flag)", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && sm.isState(EXECUTED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            // KEEP_CURRENT_RESULT not supported (VSTS #80597)
            int flag = sm.getRandom().nextBoolean()
                    ? Statement.CLOSE_CURRENT_RESULT
                    : Statement.CLOSE_ALL_RESULTS;
            boolean hasMore = stmt.getMoreResults(flag);
            sm.setState(HAS_RESULT_SET, hasMore);
            sm.setState(QUERY_INDEX, sm.getStateInt(QUERY_INDEX) + 1);
            // Advancing TDS stream invalidates generated keys
            sm.setState(LAST_EXECUTE_GENERATED_KEYS, false);
            System.out.println("  getMoreResults(" +
                    (flag == Statement.CLOSE_CURRENT_RESULT ? "CLOSE_CURRENT" : "CLOSE_ALL") +
                    ") -> " + hasMore);
        }
    }

    /**
     * FX: fxStatement.getResultSet(). Only runs when HAS_RESULT_SET=true to
     * avoid reading a closed ResultSet on repeat calls.
     */
    public static class GetResultSetAction extends Action {
        private final StateMachineTest sm;

        public GetResultSetAction(StateMachineTest sm) {
            super("getResultSet", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && sm.isState(EXECUTED) && sm.isState(HAS_RESULT_SET);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            ResultSet rs = stmt.getResultSet();
            if (rs != null) {
                // Consume the result set fully then close
                int count = 0;
                while (rs.next()) {
                    count++;
                }
                rs.close();
                System.out.println("  getResultSet() -> " + count + " rows");
            } else {
                System.out.println("  getResultSet() -> null");
            }
            sm.setState(HAS_RESULT_SET, false);
        }
    }

    /** FX: fxStatement.getUpdateCount() — skipped after executeBatch (VSTS #79390). */
    public static class GetUpdateCountAction extends Action {
        private final StateMachineTest sm;

        public GetUpdateCountAction(StateMachineTest sm) {
            super("getUpdateCount", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            // VSTS #79390: getUpdateCount after executeBatch returns incorrect value
            return !sm.isState(CLOSED) && sm.isState(EXECUTED) && !sm.isState(LAST_EXECUTE_WAS_BATCH);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            int count = stmt.getUpdateCount();
            System.out.println("  getUpdateCount() -> " + count);
        }
    }

    /**
     * FX: fxStatement.getGeneratedKeys(). Only runs when LAST_EXECUTE_GENERATED_KEYS=true;
     * consumes once per execute (driver internal state is consumed after read).
     */
    public static class GetGeneratedKeysAction extends Action {
        private final StateMachineTest sm;

        public GetGeneratedKeysAction(StateMachineTest sm) {
            super("getGeneratedKeys", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && sm.isState(EXECUTED) && sm.isState(LAST_EXECUTE_GENERATED_KEYS);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            try (ResultSet keys = stmt.getGeneratedKeys()) {
                int count = 0;
                while (keys.next()) {
                    count++;
                }
                System.out.println("  getGeneratedKeys() -> " + count + " keys");
            }
            sm.setState(LAST_EXECUTE_GENERATED_KEYS, false);
        }
    }

    // ==================== Property Actions ====================

    /** FX: fxStatement.getMaxRows() / setMaxRows(x). */
    public static class SetMaxRowsAction extends Action {
        private final StateMachineTest sm;

        public SetMaxRowsAction(StateMachineTest sm) {
            super("setMaxRows", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            int rowCount = sm.getStateInt(ROW_COUNT);
            int maxRows = sm.getRandom().nextInt(rowCount * 2);
            stmt.setMaxRows(maxRows);
            int actual = stmt.getMaxRows();
            if (actual != maxRows) {
                throw new AssertionError("setMaxRows/getMaxRows mismatch: set=" + maxRows + ", got=" + actual);
            }
            sm.setState(MAX_ROWS, maxRows);
            System.out.println("  setMaxRows(" + maxRows + ") -> getMaxRows()=" + actual);
        }
    }

    /** FX: fxStatement.getMaxFieldSize() / setMaxFieldSize(x). */
    public static class SetMaxFieldSizeAction extends Action {
        private final StateMachineTest sm;

        public SetMaxFieldSizeAction(StateMachineTest sm) {
            super("setMaxFieldSize", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            int size = sm.getRandom().nextInt(8000);
            stmt.setMaxFieldSize(size);
            int actual = stmt.getMaxFieldSize();
            if (actual != size) {
                throw new AssertionError("setMaxFieldSize/getMaxFieldSize mismatch: set=" + size + ", got=" + actual);
            }
            sm.setState(MAX_FIELD_SIZE, size);
            System.out.println("  setMaxFieldSize(" + size + ") -> getMaxFieldSize()=" + actual);
        }
    }

    /** FX: fxStatement.getFetchSize() / setFetchSize(x). */
    public static class SetFetchSizeAction extends Action {
        private final StateMachineTest sm;

        public SetFetchSizeAction(StateMachineTest sm) {
            super("setFetchSize", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            int fetchSize = sm.getRandom().nextInt(200) + 1;
            stmt.setFetchSize(fetchSize);
            int actual = stmt.getFetchSize();
            if (actual != fetchSize) {
                throw new AssertionError("setFetchSize/getFetchSize mismatch: set=" + fetchSize + ", got=" + actual);
            }
            sm.setState(FETCH_SIZE, fetchSize);
            System.out.println("  setFetchSize(" + fetchSize + ") -> getFetchSize()=" + actual);
        }
    }

    /**
     * FX: fxStatement.getFetchDirection() / setFetchDirection(x). Only FETCH_FORWARD
     * is valid for FORWARD_ONLY cursors.
     */
    public static class SetFetchDirectionAction extends Action {
        private final StateMachineTest sm;

        public SetFetchDirectionAction(StateMachineTest sm) {
            super("setFetchDirection", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            int cursorType = sm.getStateInt(CURSOR_TYPE);
            int direction;
            if (cursorType == ResultSet.TYPE_FORWARD_ONLY) {
                direction = ResultSet.FETCH_FORWARD;
            } else {
                int[] directions = {ResultSet.FETCH_FORWARD, ResultSet.FETCH_REVERSE, ResultSet.FETCH_UNKNOWN};
                direction = directions[sm.getRandom().nextInt(directions.length)];
            }
            stmt.setFetchDirection(direction);
            int actual = stmt.getFetchDirection();
            System.out.println("  setFetchDirection(" + direction + ") -> getFetchDirection()=" + actual);
        }
    }

    /** FX: fxStatement.getQueryTimeout() / setQueryTimeout(x). */
    public static class SetQueryTimeoutAction extends Action {
        private final StateMachineTest sm;

        public SetQueryTimeoutAction(StateMachineTest sm) {
            super("setQueryTimeout", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            int timeout = sm.getRandom().nextInt(30);
            stmt.setQueryTimeout(timeout);
            int actual = stmt.getQueryTimeout();
            if (actual != timeout) {
                throw new AssertionError("setQueryTimeout/getQueryTimeout mismatch: set=" + timeout + ", got=" + actual);
            }
            // Reset to 0 so we don't slow down other actions
            stmt.setQueryTimeout(0);
            System.out.println("  setQueryTimeout(" + timeout + ") -> getQueryTimeout()=" + actual + " (reset to 0)");
        }
    }

    /** FX: fxStatement.setEscapeProcessing(bool). */
    public static class SetEscapeProcessingAction extends Action {
        private final StateMachineTest sm;

        public SetEscapeProcessingAction(StateMachineTest sm) {
            super("setEscapeProcessing", 2);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            boolean enable = sm.getRandom().nextBoolean();
            stmt.setEscapeProcessing(enable);
            System.out.println("  setEscapeProcessing(" + enable + ")");
        }
    }

    // ==================== Result Set Metadata Actions ====================

    /** FX: fxStatement.getResultSetType(). */
    public static class GetResultSetTypeAction extends Action {
        private final StateMachineTest sm;

        public GetResultSetTypeAction(StateMachineTest sm) {
            super("getResultSetType", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            int type = stmt.getResultSetType();
            int expected = sm.getStateInt(CURSOR_TYPE);
            if (type != expected) {
                throw new AssertionError("getResultSetType mismatch: expected=" + expected + ", got=" + type);
            }
            System.out.println("  getResultSetType() -> " + type);
        }
    }

    /** FX: fxStatement.getResultSetConcurrency(). */
    public static class GetResultSetConcurrencyAction extends Action {
        private final StateMachineTest sm;

        public GetResultSetConcurrencyAction(StateMachineTest sm) {
            super("getResultSetConcurrency", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            int concurrency = stmt.getResultSetConcurrency();
            int expected = sm.getStateInt(CONCURRENCY);
            if (concurrency != expected) {
                throw new AssertionError("getResultSetConcurrency mismatch: expected=" + expected + ", got=" + concurrency);
            }
            System.out.println("  getResultSetConcurrency() -> " + concurrency);
        }
    }

    /** FX: fxStatement.getResultSetHoldability(). */
    public static class GetResultSetHoldabilityAction extends Action {
        private final StateMachineTest sm;

        public GetResultSetHoldabilityAction(StateMachineTest sm) {
            super("getResultSetHoldability", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            int holdability = stmt.getResultSetHoldability();
            System.out.println("  getResultSetHoldability() -> " + holdability);
        }
    }

    // ==================== Connection / Lifecycle Actions ====================

    /** FX: fxStatement.getConnection(). */
    public static class GetConnectionAction extends Action {
        private final StateMachineTest sm;

        public GetConnectionAction(StateMachineTest sm) {
            super("getConnection", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            Connection conn = stmt.getConnection();
            Connection expected = (Connection) sm.getStateValue(CONN);
            if (conn != expected) {
                throw new AssertionError("getConnection() returned different connection instance");
            }
            System.out.println("  getConnection() -> same instance verified");
        }
    }

    /** FX: fxStatement.getWarnings() / clearWarnings(). */
    public static class ClearWarningsAction extends Action {
        private final StateMachineTest sm;

        public ClearWarningsAction(StateMachineTest sm) {
            super("clearWarnings", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            stmt.getWarnings(); // Just exercise the API
            stmt.clearWarnings();
            sm.setState(HAS_WARNINGS, false);
            System.out.println("  getWarnings() + clearWarnings()");
        }
    }

    /** FX: fxStatement.isPoolable() / setPoolable(bool). */
    public static class SetPoolableAction extends Action {
        private final StateMachineTest sm;

        public SetPoolableAction(StateMachineTest sm) {
            super("setPoolable", 2);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            boolean poolable = sm.getRandom().nextBoolean();
            stmt.setPoolable(poolable);
            boolean actual = stmt.isPoolable();
            if (actual != poolable) {
                throw new AssertionError("setPoolable/isPoolable mismatch: set=" + poolable + ", got=" + actual);
            }
            System.out.println("  setPoolable(" + poolable + ") -> isPoolable()=" + actual);
        }
    }

    /** FX: fxStatement.cancel(). */
    public static class CancelAction extends Action {
        private final StateMachineTest sm;

        public CancelAction(StateMachineTest sm) {
            super("cancel", 1);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            stmt.cancel();
            System.out.println("  cancel()");
        }
    }

    /** FX: fxStatement.isClosed(). */
    public static class IsClosedAction extends Action {
        private final StateMachineTest sm;

        public IsClosedAction(StateMachineTest sm) {
            super("isClosed", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return true;
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            boolean closed = stmt.isClosed();
            boolean expected = sm.isState(CLOSED);
            if (closed != expected) {
                throw new AssertionError("isClosed() mismatch: expected=" + expected + ", got=" + closed);
            }
            System.out.println("  isClosed() -> " + closed);
        }
    }

    /** FX: fxStatement.setResponseBuffering(String) / getResponseBuffering(). */
    public static class SetResponseBufferingAction extends Action {
        private final StateMachineTest sm;

        public SetResponseBufferingAction(StateMachineTest sm) {
            super("setResponseBuffering", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            Statement stmt = (Statement) sm.getStateValue(STMT);
            if (stmt instanceof SQLServerStatement) {
                SQLServerStatement ssStmt = (SQLServerStatement) stmt;
                String value = sm.getRandom().nextBoolean() ? "adaptive" : "full";
                ssStmt.setResponseBuffering(value);
                String actual = ssStmt.getResponseBuffering();
                if (!actual.equalsIgnoreCase(value)) {
                    throw new AssertionError("setResponseBuffering mismatch: set=" + value + ", got=" + actual);
                }
                System.out.println("  setResponseBuffering('" + value + "') -> '" + actual + "'");
            } else {
                System.out.println("  setResponseBuffering skipped (not SQLServerStatement)");
            }
        }
    }
}
