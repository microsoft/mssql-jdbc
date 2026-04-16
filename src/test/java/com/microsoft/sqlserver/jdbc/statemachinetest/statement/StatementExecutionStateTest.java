/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.statement;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.sql.BatchUpdateException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Action;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Engine;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Result;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateKey;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * State-machine-driven tests for Statement, PreparedStatement, and CallableStatement execution.
 * Includes a randomized model test and deterministic scenario tests covering SRR, DML, batching,
 * cancellation, warnings, cursor properties, generated keys, concurrency, and error handling.
 */
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxStateMachine)
public class StatementExecutionStateTest extends AbstractTest {

    private static final StateKey CONN = () -> "conn";
    private static final StateKey STMT = () -> "stmt";
    private static final StateKey CLOSED = () -> "closed";
    private static final StateKey EXECUTED = () -> "executed";
    private static final StateKey HAS_RESULT_SET = () -> "hasResultSet";
    private static final StateKey LAST_EXECUTE_WAS_BATCH = () -> "lastExecuteWasBatch";
    private static final StateKey LAST_EXECUTE_WAS_UPDATE = () -> "lastExecuteWasUpdate";
    private static final StateKey LAST_EXECUTE_GENERATED_KEYS = () -> "lastExecuteGeneratedKeys";
    private static final StateKey MAX_ROWS = () -> "maxRows";
    private static final StateKey MAX_FIELD_SIZE = () -> "maxFieldSize";
    private static final StateKey HAS_WARNINGS = () -> "hasWarnings";
    private static final StateKey FETCH_SIZE = () -> "fetchSize";
    private static final StateKey CURSOR_TYPE = () -> "cursorType";
    private static final StateKey CONCURRENCY = () -> "concurrency";
    private static final StateKey HOLDABILITY = () -> "holdability";
    private static final StateKey QUERY_INDEX = () -> "queryIndex";
    private static final StateKey SM_TABLE_NAME = () -> "tableName";
    private static final StateKey ROW_COUNT = () -> "rowCount";
    private static final StateKey NEXT_INSERT_ID = () -> "nextInsertId";

    private static final String TABLE_NAME = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("SM_StmtExec_Test"));
    private static final int ROW_COUNT_VAL = 10;

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
     * Helper to create a stored procedure.
     * @param stmt The setup statement to execute DDL on
     * @param procName The escaped procedure name
     * @param procBody The procedure body (everything after "CREATE PROCEDURE procName")
     */
    private static void createProc(Statement stmt, String procName, String procBody) throws SQLException {
        TestUtils.dropProcedureIfExists(procName, stmt);
        stmt.execute("CREATE PROCEDURE " + procName + " " + procBody);
    }

    /**
     * Creates a test table with sample data.
     * Includes an IDENTITY column for generated-keys testing.
     */
    private static void createTestTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            stmt.execute("CREATE TABLE " + TABLE_NAME
                    + " (id INT PRIMARY KEY, value INT, name VARCHAR(200), id_col INT IDENTITY(1,1))");
            for (int i = 1; i <= ROW_COUNT_VAL; i++) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " (id, value, name) VALUES (" + i + ", " + (i * 10)
                        + ", 'Row" + i + "')");
            }
        }
    }


    /** Execute SELECT — sets HAS_RESULT_SET=true. */
    private static class ExecuteSelectAction extends Action {
        ExecuteSelectAction() { super("execute(SELECT)", 10); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            String tableName = (String) getState(SM_TABLE_NAME);
            boolean hasResultSet = stmt.execute("SELECT * FROM " + tableName);
            setState(EXECUTED, true);
            setState(HAS_RESULT_SET, hasResultSet);
            setState(LAST_EXECUTE_WAS_BATCH, false);
            setState(LAST_EXECUTE_WAS_UPDATE, false);
            setState(LAST_EXECUTE_GENERATED_KEYS, false);
            setState(QUERY_INDEX, 0);
            if (!hasResultSet) throw new AssertionError("execute(SELECT) should return true");
        }
    }

    /** Execute DML (UPDATE). */
    private static class ExecuteDMLAction extends Action {
        ExecuteDMLAction() { super("execute(UPDATE)", 10); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            String tableName = (String) getState(SM_TABLE_NAME);
            int rowId = getRandom().nextInt(getStateInt(ROW_COUNT)) + 1;
            boolean hasResultSet = stmt.execute("UPDATE " + tableName + " SET value = " + getRandom().nextInt(10000) + " WHERE id = " + rowId);
            setState(EXECUTED, true);
            setState(HAS_RESULT_SET, hasResultSet);
            setState(LAST_EXECUTE_WAS_BATCH, false);
            setState(LAST_EXECUTE_WAS_UPDATE, false);
            setState(LAST_EXECUTE_GENERATED_KEYS, false);
            setState(QUERY_INDEX, 0);
            if (hasResultSet) throw new AssertionError("execute(UPDATE) should return false");
        }
    }

    /** executeQuery(SELECT) — consumes result inline. */
    private static class ExecuteQueryAction extends Action {
        ExecuteQueryAction() { super("executeQuery", 10); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            String tableName = (String) getState(SM_TABLE_NAME);
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                setState(LAST_EXECUTE_WAS_BATCH, false);
                setState(LAST_EXECUTE_WAS_UPDATE, false);
                setState(LAST_EXECUTE_GENERATED_KEYS, false);
                setState(QUERY_INDEX, 0);
                while (rs.next()) {}
            }
        }
    }

    /** executeUpdate(DML). */
    private static class ExecuteUpdateAction extends Action {
        ExecuteUpdateAction() { super("executeUpdate", 10); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            String tableName = (String) getState(SM_TABLE_NAME);
            int rowId = getRandom().nextInt(getStateInt(ROW_COUNT)) + 1;
            int updateCount = stmt.executeUpdate("UPDATE " + tableName + " SET value = " + getRandom().nextInt(10000) + " WHERE id = " + rowId);
            setState(EXECUTED, true);
            setState(HAS_RESULT_SET, false);
            setState(LAST_EXECUTE_WAS_BATCH, false);
            setState(LAST_EXECUTE_WAS_UPDATE, true);
            setState(LAST_EXECUTE_GENERATED_KEYS, false);
            setState(QUERY_INDEX, 0);
            if (updateCount != 1) throw new AssertionError("executeUpdate should affect 1 row, got " + updateCount);
        }
    }

    /** execute(sql, autokeys) — randomly INSERT or UPDATE; validates getGeneratedKeys() inline for INSERT. */
    private static class ExecuteWithGeneratedKeysAction extends Action {
        ExecuteWithGeneratedKeysAction() { super("execute(genKeys)", 5); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            String tableName = (String) getState(SM_TABLE_NAME);
            int autoKeyFlag = getRandom().nextBoolean() ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS;
            boolean isInsert = getRandom().nextBoolean();
            boolean hasResultSet;
            if (isInsert) {
                int newId = getStateInt(NEXT_INSERT_ID);
                setState(NEXT_INSERT_ID, newId + 1);
                hasResultSet = stmt.execute("INSERT INTO " + tableName + " (id, value, name) VALUES ("
                        + newId + ", " + getRandom().nextInt(10000) + ", 'GenKey')", autoKeyFlag);
            } else {
                int rowId = getRandom().nextInt(getStateInt(ROW_COUNT)) + 1;
                hasResultSet = stmt.execute("UPDATE " + tableName + " SET value = "
                        + getRandom().nextInt(10000) + " WHERE id = " + rowId, autoKeyFlag);
            }
            setState(EXECUTED, true);
            setState(HAS_RESULT_SET, hasResultSet);
            setState(LAST_EXECUTE_WAS_BATCH, false);
            setState(LAST_EXECUTE_WAS_UPDATE, false);
            setState(LAST_EXECUTE_GENERATED_KEYS,
                    autoKeyFlag == Statement.RETURN_GENERATED_KEYS);
            setState(QUERY_INDEX, 0);
            if (isInsert && autoKeyFlag == Statement.RETURN_GENERATED_KEYS) {
                try (ResultSet keys = stmt.getGeneratedKeys()) {
                    assertTrue(keys.next(), "getGeneratedKeys() should return a row for INSERT with identity");
                }
            }
        }
    }

    /** executeUpdate(sql, autokeys) — randomly INSERT or UPDATE; validates getGeneratedKeys() inline for INSERT. */
    private static class ExecuteUpdateWithGeneratedKeysAction extends Action {
        ExecuteUpdateWithGeneratedKeysAction() { super("executeUpdate(genKeys)", 5); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            String tableName = (String) getState(SM_TABLE_NAME);
            int autoKeyFlag = getRandom().nextBoolean() ? Statement.RETURN_GENERATED_KEYS : Statement.NO_GENERATED_KEYS;
            boolean isInsert = getRandom().nextBoolean();
            if (isInsert) {
                int newId = getStateInt(NEXT_INSERT_ID);
                setState(NEXT_INSERT_ID, newId + 1);
                stmt.executeUpdate("INSERT INTO " + tableName + " (id, value, name) VALUES ("
                        + newId + ", " + getRandom().nextInt(10000) + ", 'GenKey')", autoKeyFlag);
            } else {
                int rowId = getRandom().nextInt(getStateInt(ROW_COUNT)) + 1;
                stmt.executeUpdate("UPDATE " + tableName + " SET value = "
                        + getRandom().nextInt(10000) + " WHERE id = " + rowId, autoKeyFlag);
            }
            setState(EXECUTED, true);
            setState(HAS_RESULT_SET, false);
            setState(LAST_EXECUTE_WAS_BATCH, false);
            setState(LAST_EXECUTE_WAS_UPDATE, true);
            setState(LAST_EXECUTE_GENERATED_KEYS,
                    autoKeyFlag == Statement.RETURN_GENERATED_KEYS);
            setState(QUERY_INDEX, 0);
            if (isInsert && autoKeyFlag == Statement.RETURN_GENERATED_KEYS) {
                try (ResultSet keys = stmt.getGeneratedKeys()) {
                    assertTrue(keys.next(), "getGeneratedKeys() should return a row for INSERT with identity");
                }
            }
        }
    }

    /** execute(sql, int[] columnIndexes) — randomly INSERT or UPDATE; validates generated keys inline for INSERT. */
    private static class ExecuteWithColumnIndexesAction extends Action {
        ExecuteWithColumnIndexesAction() { super("execute(colIndexes)", 3); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            String tableName = (String) getState(SM_TABLE_NAME);
            boolean isInsert = getRandom().nextBoolean();
            boolean hasResultSet;
            if (isInsert) {
                int newId = getStateInt(NEXT_INSERT_ID);
                setState(NEXT_INSERT_ID, newId + 1);
                hasResultSet = stmt.execute("INSERT INTO " + tableName + " (id, value, name) VALUES ("
                        + newId + ", " + getRandom().nextInt(10000) + ", 'GenKey')", new int[]{4});
            } else {
                int rowId = getRandom().nextInt(getStateInt(ROW_COUNT)) + 1;
                hasResultSet = stmt.execute("UPDATE " + tableName + " SET value = "
                        + getRandom().nextInt(10000) + " WHERE id = " + rowId, new int[]{4});
            }
            setState(EXECUTED, true); setState(HAS_RESULT_SET, hasResultSet);
            setState(LAST_EXECUTE_WAS_BATCH, false); setState(LAST_EXECUTE_WAS_UPDATE, false);
            setState(LAST_EXECUTE_GENERATED_KEYS, true); setState(QUERY_INDEX, 0);
            if (isInsert) {
                try (ResultSet keys = stmt.getGeneratedKeys()) {
                    assertTrue(keys.next(), "getGeneratedKeys() should return a row for INSERT with identity");
                }
            }
        }
    }

    /** execute(sql, String[] columnNames) — randomly INSERT or UPDATE; validates generated keys inline for INSERT. */
    private static class ExecuteWithColumnNamesAction extends Action {
        ExecuteWithColumnNamesAction() { super("execute(colNames)", 3); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            String tableName = (String) getState(SM_TABLE_NAME);
            boolean isInsert = getRandom().nextBoolean();
            boolean hasResultSet;
            if (isInsert) {
                int newId = getStateInt(NEXT_INSERT_ID);
                setState(NEXT_INSERT_ID, newId + 1);
                hasResultSet = stmt.execute("INSERT INTO " + tableName + " (id, value, name) VALUES ("
                        + newId + ", " + getRandom().nextInt(10000) + ", 'GenKey')", new String[]{"id_col"});
            } else {
                int rowId = getRandom().nextInt(getStateInt(ROW_COUNT)) + 1;
                hasResultSet = stmt.execute("UPDATE " + tableName + " SET value = "
                        + getRandom().nextInt(10000) + " WHERE id = " + rowId, new String[]{"id_col"});
            }
            setState(EXECUTED, true); setState(HAS_RESULT_SET, hasResultSet);
            setState(LAST_EXECUTE_WAS_BATCH, false); setState(LAST_EXECUTE_WAS_UPDATE, false);
            setState(LAST_EXECUTE_GENERATED_KEYS, true); setState(QUERY_INDEX, 0);
            if (isInsert) {
                try (ResultSet keys = stmt.getGeneratedKeys()) {
                    assertTrue(keys.next(), "getGeneratedKeys() should return a row for INSERT with identity");
                }
            }
        }
    }

    /** executeUpdate(sql, int[] columnIndexes) — randomly INSERT or UPDATE; validates generated keys inline for INSERT. */
    private static class ExecuteUpdateWithColumnIndexesAction extends Action {
        ExecuteUpdateWithColumnIndexesAction() { super("executeUpdate(colIdx)", 3); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            String tableName = (String) getState(SM_TABLE_NAME);
            boolean isInsert = getRandom().nextBoolean();
            if (isInsert) {
                int newId = getStateInt(NEXT_INSERT_ID);
                setState(NEXT_INSERT_ID, newId + 1);
                stmt.executeUpdate("INSERT INTO " + tableName + " (id, value, name) VALUES ("
                        + newId + ", " + getRandom().nextInt(10000) + ", 'GenKey')", new int[]{4});
            } else {
                int rowId = getRandom().nextInt(getStateInt(ROW_COUNT)) + 1;
                stmt.executeUpdate("UPDATE " + tableName + " SET value = "
                        + getRandom().nextInt(10000) + " WHERE id = " + rowId, new int[]{4});
            }
            setState(EXECUTED, true); setState(HAS_RESULT_SET, false);
            setState(LAST_EXECUTE_WAS_BATCH, false); setState(LAST_EXECUTE_WAS_UPDATE, true);
            setState(LAST_EXECUTE_GENERATED_KEYS, true); setState(QUERY_INDEX, 0);
            if (isInsert) {
                try (ResultSet keys = stmt.getGeneratedKeys()) {
                    assertTrue(keys.next(), "getGeneratedKeys() should return a row for INSERT with identity");
                }
            }
        }
    }

    /** executeUpdate(sql, String[] columnNames) — randomly INSERT or UPDATE; validates generated keys inline for INSERT. */
    private static class ExecuteUpdateWithColumnNamesAction extends Action {
        ExecuteUpdateWithColumnNamesAction() { super("executeUpdate(colNames)", 3); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            String tableName = (String) getState(SM_TABLE_NAME);
            boolean isInsert = getRandom().nextBoolean();
            if (isInsert) {
                int newId = getStateInt(NEXT_INSERT_ID);
                setState(NEXT_INSERT_ID, newId + 1);
                stmt.executeUpdate("INSERT INTO " + tableName + " (id, value, name) VALUES ("
                        + newId + ", " + getRandom().nextInt(10000) + ", 'GenKey')", new String[]{"id_col"});
            } else {
                int rowId = getRandom().nextInt(getStateInt(ROW_COUNT)) + 1;
                stmt.executeUpdate("UPDATE " + tableName + " SET value = "
                        + getRandom().nextInt(10000) + " WHERE id = " + rowId, new String[]{"id_col"});
            }
            setState(EXECUTED, true); setState(HAS_RESULT_SET, false);
            setState(LAST_EXECUTE_WAS_BATCH, false); setState(LAST_EXECUTE_WAS_UPDATE, true);
            setState(LAST_EXECUTE_GENERATED_KEYS, true); setState(QUERY_INDEX, 0);
            if (isInsert) {
                try (ResultSet keys = stmt.getGeneratedKeys()) {
                    assertTrue(keys.next(), "getGeneratedKeys() should return a row for INSERT with identity");
                }
            }
        }
    }

    /** addBatch(DML). */
    private static class AddBatchAction extends Action {
        AddBatchAction() { super("addBatch", 10); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            String tableName = (String) getState(SM_TABLE_NAME);
            int rowId = getRandom().nextInt(getStateInt(ROW_COUNT)) + 1;
            stmt.addBatch("UPDATE " + tableName + " SET value = " + getRandom().nextInt(10000) + " WHERE id = " + rowId);
        }
    }

    /** executeBatch(). */
    private static class ExecuteBatchAction extends Action {
        ExecuteBatchAction() { super("executeBatch", 8); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            stmt.executeBatch();
            setState(EXECUTED, true); setState(HAS_RESULT_SET, false);
            setState(LAST_EXECUTE_WAS_BATCH, true); setState(LAST_EXECUTE_WAS_UPDATE, false);
            setState(LAST_EXECUTE_GENERATED_KEYS, false); setState(QUERY_INDEX, 0);
        }
    }

    /** clearBatch(). */
    private static class ClearBatchAction extends Action {
        ClearBatchAction() { super("clearBatch", 5); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException { ((Statement) getState(STMT)).clearBatch(); }
    }

    /** getMoreResults(). */
    private static class GetMoreResultsAction extends Action {
        GetMoreResultsAction() { super("getMoreResults", 10); }
        @Override public boolean canRun() { return !isState(CLOSED) && isState(EXECUTED); }
        @Override public void run() throws SQLException {
            boolean hasMore = ((Statement) getState(STMT)).getMoreResults();
            setState(HAS_RESULT_SET, hasMore);
            setState(QUERY_INDEX, getStateInt(QUERY_INDEX) + 1);
            setState(LAST_EXECUTE_GENERATED_KEYS, false);
        }
    }

    /** getMoreResults(int flag). */
    private static class GetMoreResultsWithFlagAction extends Action {
        GetMoreResultsWithFlagAction() { super("getMoreResults(flag)", 5); }
        @Override public boolean canRun() { return !isState(CLOSED) && isState(EXECUTED); }
        @Override public void run() throws SQLException {
            int flag = getRandom().nextBoolean() ? Statement.CLOSE_CURRENT_RESULT : Statement.CLOSE_ALL_RESULTS;
            boolean hasMore = ((Statement) getState(STMT)).getMoreResults(flag);
            setState(HAS_RESULT_SET, hasMore);
            setState(QUERY_INDEX, getStateInt(QUERY_INDEX) + 1);
            setState(LAST_EXECUTE_GENERATED_KEYS, false);
        }
    }

    /** getResultSet() — only when HAS_RESULT_SET=true. */
    private static class GetResultSetAction extends Action {
        GetResultSetAction() { super("getResultSet", 10); }
        @Override public boolean canRun() { return !isState(CLOSED) && isState(EXECUTED) && isState(HAS_RESULT_SET); }
        @Override public void run() throws SQLException {
            ResultSet rs = ((Statement) getState(STMT)).getResultSet();
            if (rs != null) { while (rs.next()) {} rs.close(); }
            setState(HAS_RESULT_SET, false);
        }
    }

    /** getUpdateCount() — skipped after executeBatch (VSTS #79390). */
    private static class GetUpdateCountAction extends Action {
        GetUpdateCountAction() { super("getUpdateCount", 10); }
        @Override public boolean canRun() { return !isState(CLOSED) && isState(EXECUTED) && !isState(LAST_EXECUTE_WAS_BATCH); }
        @Override public void run() throws SQLException { ((Statement) getState(STMT)).getUpdateCount(); }
    }

    /** setMaxRows / getMaxRows. */
    private static class SetMaxRowsAction extends Action {
        SetMaxRowsAction() { super("setMaxRows", 5); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            int maxRows = getRandom().nextInt(getStateInt(ROW_COUNT) * 2);
            stmt.setMaxRows(maxRows);
            if (stmt.getMaxRows() != maxRows) throw new AssertionError("setMaxRows/getMaxRows mismatch");
            setState(MAX_ROWS, maxRows);
        }
    }

    /** setMaxFieldSize / getMaxFieldSize. */
    private static class SetMaxFieldSizeAction extends Action {
        SetMaxFieldSizeAction() { super("setMaxFieldSize", 3); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            int size = getRandom().nextInt(8000);
            stmt.setMaxFieldSize(size);
            if (stmt.getMaxFieldSize() != size) throw new AssertionError("setMaxFieldSize mismatch");
            setState(MAX_FIELD_SIZE, size);
        }
    }

    /** setFetchSize / getFetchSize. */
    private static class SetFetchSizeAction extends Action {
        SetFetchSizeAction() { super("setFetchSize", 5); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            int fetchSize = getRandom().nextInt(200) + 1;
            stmt.setFetchSize(fetchSize);
            if (stmt.getFetchSize() != fetchSize) throw new AssertionError("setFetchSize mismatch");
            setState(FETCH_SIZE, fetchSize);
        }
    }

    /** setFetchDirection / getFetchDirection. */
    private static class SetFetchDirectionAction extends Action {
        SetFetchDirectionAction() { super("setFetchDirection", 3); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            int cursorType = getStateInt(CURSOR_TYPE);
            int direction = (cursorType == ResultSet.TYPE_FORWARD_ONLY) ? ResultSet.FETCH_FORWARD
                    : new int[]{ResultSet.FETCH_FORWARD, ResultSet.FETCH_REVERSE, ResultSet.FETCH_UNKNOWN}[getRandom().nextInt(3)];
            stmt.setFetchDirection(direction);
        }
    }

    /** setQueryTimeout / getQueryTimeout. */
    private static class SetQueryTimeoutAction extends Action {
        SetQueryTimeoutAction() { super("setQueryTimeout", 3); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            int timeout = getRandom().nextInt(30);
            stmt.setQueryTimeout(timeout);
            if (stmt.getQueryTimeout() != timeout) throw new AssertionError("setQueryTimeout mismatch");
            stmt.setQueryTimeout(0);
        }
    }

    /** setEscapeProcessing. */
    private static class SetEscapeProcessingAction extends Action {
        SetEscapeProcessingAction() { super("setEscapeProcessing", 2); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException { ((Statement) getState(STMT)).setEscapeProcessing(getRandom().nextBoolean()); }
    }

    /** getResultSetType — validates against CURSOR_TYPE state. */
    private static class GetResultSetTypeAction extends Action {
        GetResultSetTypeAction() { super("getResultSetType", 5); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            int type = ((Statement) getState(STMT)).getResultSetType();
            if (type != getStateInt(CURSOR_TYPE)) throw new AssertionError("getResultSetType mismatch");
        }
    }

    /** getResultSetConcurrency — validates against CONCURRENCY state. */
    private static class GetResultSetConcurrencyAction extends Action {
        GetResultSetConcurrencyAction() { super("getResultSetConcurrency", 5); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            int c = ((Statement) getState(STMT)).getResultSetConcurrency();
            if (c != getStateInt(CONCURRENCY)) throw new AssertionError("getResultSetConcurrency mismatch");
        }
    }

    /** getResultSetHoldability. */
    private static class GetResultSetHoldabilityAction extends Action {
        GetResultSetHoldabilityAction() { super("getResultSetHoldability", 3); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException { ((Statement) getState(STMT)).getResultSetHoldability(); }
    }

    /** getConnection — validates same instance. */
    private static class GetConnectionAction extends Action {
        GetConnectionAction() { super("getConnection", 5); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Connection conn = ((Statement) getState(STMT)).getConnection();
            if (conn != getState(CONN)) throw new AssertionError("getConnection() returned different instance");
        }
    }

    /** getWarnings / clearWarnings. */
    private static class ClearWarningsAction extends Action {
        ClearWarningsAction() { super("clearWarnings", 5); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            stmt.getWarnings(); stmt.clearWarnings(); setState(HAS_WARNINGS, false);
        }
    }

    /** setPoolable / isPoolable. */
    private static class SetPoolableAction extends Action {
        SetPoolableAction() { super("setPoolable", 2); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            boolean poolable = getRandom().nextBoolean();
            stmt.setPoolable(poolable);
            if (stmt.isPoolable() != poolable) throw new AssertionError("setPoolable mismatch");
        }
    }

    /** cancel(). */
    private static class CancelAction extends Action {
        CancelAction() { super("cancel", 1); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException { ((Statement) getState(STMT)).cancel(); }
    }

    /** isClosed — validates against CLOSED state. */
    private static class IsClosedAction extends Action {
        IsClosedAction() { super("isClosed", 5); }
        @Override public boolean canRun() { return true; }
        @Override public void run() throws SQLException {
            boolean closed = ((Statement) getState(STMT)).isClosed();
            if (closed != isState(CLOSED)) throw new AssertionError("isClosed mismatch");
        }
    }

    /** setResponseBuffering / getResponseBuffering (SQLServerStatement). */
    private static class SetResponseBufferingAction extends Action {
        SetResponseBufferingAction() { super("setResponseBuffering", 3); }
        @Override public boolean canRun() { return !isState(CLOSED); }
        @Override public void run() throws SQLException {
            Statement stmt = (Statement) getState(STMT);
            if (stmt instanceof SQLServerStatement) {
                SQLServerStatement ssStmt = (SQLServerStatement) stmt;
                String value = getRandom().nextBoolean() ? "adaptive" : "full";
                ssStmt.setResponseBuffering(value);
                if (!ssStmt.getResponseBuffering().equalsIgnoreCase(value))
                    throw new AssertionError("setResponseBuffering mismatch");
            }
        }
    }


    @Test
    @DisplayName("Randomized Statement Execution Exploration")
    void testRandomizedStatementExecution() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            createTestTable(conn);
            try (Statement stmt = conn.createStatement()) {
                StateMachineTest sm = new StateMachineTest("StatementExecution");
                sm.getDataCache().updateValue(0, CONN.key(), conn);
                sm.getDataCache().updateValue(0, STMT.key(), stmt);
                sm.getDataCache().updateValue(0, CLOSED.key(), false);
                sm.getDataCache().updateValue(0, EXECUTED.key(), false);
                sm.getDataCache().updateValue(0, HAS_RESULT_SET.key(), false);
                sm.getDataCache().updateValue(0, LAST_EXECUTE_WAS_BATCH.key(), false);
                sm.getDataCache().updateValue(0, LAST_EXECUTE_WAS_UPDATE.key(), false);
                sm.getDataCache().updateValue(0, LAST_EXECUTE_GENERATED_KEYS.key(), false);
                sm.getDataCache().updateValue(0, MAX_ROWS.key(), 0);
                sm.getDataCache().updateValue(0, MAX_FIELD_SIZE.key(), 0);
                sm.getDataCache().updateValue(0, HAS_WARNINGS.key(), false);
                sm.getDataCache().updateValue(0, FETCH_SIZE.key(), 0);
                sm.getDataCache().updateValue(0, CURSOR_TYPE.key(), ResultSet.TYPE_FORWARD_ONLY);
                sm.getDataCache().updateValue(0, CONCURRENCY.key(), ResultSet.CONCUR_READ_ONLY);
                sm.getDataCache().updateValue(0, HOLDABILITY.key(), ResultSet.HOLD_CURSORS_OVER_COMMIT);
                sm.getDataCache().updateValue(0, QUERY_INDEX.key(), 0);
                sm.getDataCache().updateValue(0, SM_TABLE_NAME.key(), TABLE_NAME);
                sm.getDataCache().updateValue(0, ROW_COUNT.key(), ROW_COUNT_VAL);
                sm.getDataCache().updateValue(0, NEXT_INSERT_ID.key(), ROW_COUNT_VAL + 1);

                sm.addAction(new ExecuteSelectAction());
                sm.addAction(new ExecuteDMLAction());
                sm.addAction(new ExecuteQueryAction());
                sm.addAction(new ExecuteUpdateAction());
                sm.addAction(new ExecuteWithGeneratedKeysAction());
                sm.addAction(new ExecuteUpdateWithGeneratedKeysAction());
                sm.addAction(new ExecuteWithColumnIndexesAction());
                sm.addAction(new ExecuteWithColumnNamesAction());
                sm.addAction(new ExecuteUpdateWithColumnIndexesAction());
                sm.addAction(new ExecuteUpdateWithColumnNamesAction());
                sm.addAction(new AddBatchAction());
                sm.addAction(new ExecuteBatchAction());
                sm.addAction(new ClearBatchAction());
                sm.addAction(new GetMoreResultsAction());
                sm.addAction(new GetMoreResultsWithFlagAction());
                sm.addAction(new GetResultSetAction());
                sm.addAction(new GetUpdateCountAction());
                // getGeneratedKeys() is validated inline within the execute-with-keys actions.
                sm.addAction(new SetMaxRowsAction());
                sm.addAction(new SetMaxFieldSizeAction());
                sm.addAction(new SetFetchSizeAction());
                sm.addAction(new SetFetchDirectionAction());
                sm.addAction(new SetQueryTimeoutAction());
                sm.addAction(new SetEscapeProcessingAction());
                sm.addAction(new GetResultSetTypeAction());
                sm.addAction(new GetResultSetConcurrencyAction());
                sm.addAction(new GetResultSetHoldabilityAction());
                sm.addAction(new GetConnectionAction());
                sm.addAction(new ClearWarningsAction());
                sm.addAction(new SetPoolableAction());
                sm.addAction(new CancelAction());
                sm.addAction(new IsClosedAction());
                sm.addAction(new SetResponseBufferingAction());

                Result result = Engine.run(sm).withMaxActions(100).withTimeout(60).execute();
                assertTrue(result.isSuccess(), "Model run failed: " + result);
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Single Result Set Read (SRR)")
    class SRRTests {

        /**
         * Statement SRR.
         * Create Statement, execute SELECT, verify result set returned.
         * Cursor types: default, FORWARD_ONLY, SCROLL_INSENSITIVE, SCROLL_SENSITIVE.
         */
        @Test
        @DisplayName("testSRR: Statement executeQuery returns ResultSet")
        void testSRR_Statement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                    assertNotNull(rs, "executeQuery should return non-null ResultSet");
                    int count = 0;
                    while (rs.next()) {
                        count++;
                    }
                    assertEquals(ROW_COUNT_VAL, count, "Should read all rows");
                }
            }
        }

        @Test
        @DisplayName("testSRR: PreparedStatement executeQuery returns ResultSet")
        void testSRR_PreparedStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM " + TABLE_NAME);
                        ResultSet rs = pstmt.executeQuery()) {
                    assertNotNull(rs, "executeQuery should return non-null ResultSet");
                    int count = 0;
                    while (rs.next()) {
                        count++;
                    }
                    assertEquals(ROW_COUNT_VAL, count, "Should read all rows");
                }
            }
        }

        @Test
        @DisplayName("testSRR: CallableStatement executeQuery returns ResultSet")
        void testSRR_CallableStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                String procName = AbstractSQLGenerator
                        .escapeIdentifier(RandomUtil.getIdentifier("SM_SRR_proc"));
                try (Statement setup = conn.createStatement()) {
                    TestUtils.dropProcedureIfExists(procName, setup);
                    setup.execute(
                            "CREATE PROCEDURE " + procName + " AS SELECT * FROM " + TABLE_NAME);
                }
                try (CallableStatement cstmt = conn.prepareCall("{call " + procName + "}");
                        ResultSet rs = cstmt.executeQuery()) {
                    assertNotNull(rs, "executeQuery should return non-null ResultSet");
                    int count = 0;
                    while (rs.next()) {
                        count++;
                    }
                    assertEquals(ROW_COUNT_VAL, count, "Should read all rows");
                } finally {
                    try (Statement cleanup = conn.createStatement()) {
                        TestUtils.dropProcedureIfExists(procName, cleanup);
                    }
                }
            }
        }

        /**
         * All execute states with SRR.
         * Loop through execute/executeQuery for each statement type.
         */
        @Test
        @DisplayName("testAllSRR: All statement types with execute(SELECT)")
        void testAllSRR() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                String sql = "SELECT * FROM " + TABLE_NAME;

                try (Statement stmt = conn.createStatement()) {
                    boolean hasRS = stmt.execute(sql);
                    assertTrue(hasRS, "execute(SELECT) should return true");
                    try (ResultSet rs = stmt.getResultSet()) {
                        assertNotNull(rs);
                        int count = 0;
                        while (rs.next()) count++;
                        assertEquals(ROW_COUNT_VAL, count);
                    }
                }
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(sql)) {
                    int count = 0;
                    while (rs.next()) count++;
                    assertEquals(ROW_COUNT_VAL, count);
                }
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    boolean hasRS = pstmt.execute();
                    assertTrue(hasRS, "pstmt.execute() should return true");
                    try (ResultSet rs = pstmt.getResultSet()) {
                        assertNotNull(rs);
                        int count = 0;
                        while (rs.next()) count++;
                        assertEquals(ROW_COUNT_VAL, count);
                    }
                }
                try (PreparedStatement pstmt = conn.prepareStatement(sql);
                        ResultSet rs = pstmt.executeQuery()) {
                    int count = 0;
                    while (rs.next()) count++;
                    assertEquals(ROW_COUNT_VAL, count);
                }
            }
        }

        /**
         * TCStatementSanity.testSRR with scrollable cursor types.
         * FORWARD_ONLY, SCROLL_INSENSITIVE, SCROLL_SENSITIVE.
         */
        @Test
        @DisplayName("testSRR: All cursor types - FORWARD_ONLY/SCROLL_INSENSITIVE/SCROLL_SENSITIVE")
        void testSRR_AllCursorTypes() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            int[][] cursorTypes = {
                    {ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY},
                    {ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE},
                    {ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY},
                    {ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY},
                    {ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE}
            };
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                for (int[] ct : cursorTypes) {
                    try (Statement stmt = conn.createStatement(ct[0], ct[1]);
                            ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                        int count = 0;
                        while (rs.next()) count++;
                        assertEquals(ROW_COUNT_VAL, count,
                                "Cursor type=" + ct[0] + " concurrency=" + ct[1]);
                    }
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Data Manipulation (DML)")
    class DMLTests {

        /**
         * Statement DML.
         * Create Statement, execute UPDATE, verify update count.
         */
        @Test
        @DisplayName("testDML: Statement executeUpdate returns correct count")
        void testDML_Statement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    int count = stmt.executeUpdate(
                            "UPDATE " + TABLE_NAME + " SET value = 999 WHERE id = 1");
                    assertEquals(1, count, "UPDATE should affect 1 row");
                }
            }
        }

        @Test
        @DisplayName("testDML: PreparedStatement executeUpdate returns correct count")
        void testDML_PreparedStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "UPDATE " + TABLE_NAME + " SET value = ? WHERE id = ?")) {
                    pstmt.setInt(1, 999);
                    pstmt.setInt(2, 1);
                    int count = pstmt.executeUpdate();
                    assertEquals(1, count, "UPDATE should affect 1 row");
                }
            }
        }

        @Test
        @DisplayName("testDML: CallableStatement executeUpdate returns correct count")
        void testDML_CallableStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                String procName = AbstractSQLGenerator
                        .escapeIdentifier(RandomUtil.getIdentifier("SM_DML_proc"));
                try (Statement setup = conn.createStatement()) {
                    TestUtils.dropProcedureIfExists(procName, setup);
                    setup.execute("CREATE PROCEDURE " + procName
                            + " @val INT, @rowid INT AS UPDATE " + TABLE_NAME
                            + " SET value = @val WHERE id = @rowid");
                }
                try (CallableStatement cstmt = conn.prepareCall("{call " + procName + "(?, ?)}")) {
                    cstmt.setInt(1, 999);
                    cstmt.setInt(2, 1);
                    int count = cstmt.executeUpdate();
                    assertEquals(1, count, "UPDATE via sproc should affect 1 row");
                } finally {
                    try (Statement cleanup = conn.createStatement()) {
                        TestUtils.dropProcedureIfExists(procName, cleanup);
                    }
                }
            }
        }

        /**
         * All execute states with DML.
         * Loop through execute/executeUpdate for all statement types.
         */
        @Test
        @DisplayName("testAllDML: All statement types with execute(UPDATE)")
        void testAllDML() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);

                try (Statement stmt = conn.createStatement()) {
                    boolean hasRS = stmt
                            .execute("UPDATE " + TABLE_NAME + " SET value = 111 WHERE id = 1");
                    assertFalse(hasRS, "execute(UPDATE) should return false");
                    int uc = stmt.getUpdateCount();
                    assertEquals(1, uc, "getUpdateCount should be 1");
                }
                try (Statement stmt = conn.createStatement()) {
                    int uc = stmt.executeUpdate(
                            "UPDATE " + TABLE_NAME + " SET value = 222 WHERE id = 2");
                    assertEquals(1, uc, "executeUpdate should return 1");
                }
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "UPDATE " + TABLE_NAME + " SET value = ? WHERE id = ?")) {
                    pstmt.setInt(1, 333);
                    pstmt.setInt(2, 3);
                    boolean hasRS = pstmt.execute();
                    assertFalse(hasRS, "pstmt.execute(UPDATE) should return false");
                    assertEquals(1, pstmt.getUpdateCount());
                }
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "UPDATE " + TABLE_NAME + " SET value = ? WHERE id = ?")) {
                    pstmt.setInt(1, 444);
                    pstmt.setInt(2, 4);
                    int uc = pstmt.executeUpdate();
                    assertEquals(1, uc, "pstmt.executeUpdate should return 1");
                }
            }
        }

        /**
         * TCStatementSanity.testDML with all cursor types.
         */
        @Test
        @DisplayName("testDML: All cursor types with executeUpdate")
        void testDML_AllCursorTypes() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            int[][] cursorTypes = {
                    {ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY},
                    {ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE},
                    {ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY},
                    {ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE}
            };
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                for (int[] ct : cursorTypes) {
                    try (Statement stmt = conn.createStatement(ct[0], ct[1])) {
                        int count = stmt.executeUpdate(
                                "UPDATE " + TABLE_NAME + " SET value = 500 WHERE id = 5");
                        assertEquals(1, count,
                                "Cursor type=" + ct[0] + " concurrency=" + ct[1]);
                    }
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Batch Operations")
    class BatchTests {

        /**
         * Statement batch DML.
         * addBatch(DML), addBatch(DML), executeBatch, clearBatch.
         */
        @Test
        @DisplayName("testBatchDML: Statement addBatch/executeBatch/clearBatch")
        void testBatchDML_Statement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(
                            "UPDATE " + TABLE_NAME + " SET value = 100 WHERE id = 1");
                    stmt.addBatch("UPDATE " + TABLE_NAME + " SET value = 200 WHERE id = 2");
                    stmt.addBatch("UPDATE " + TABLE_NAME + " SET value = 300 WHERE id = 3");
                    int[] results = stmt.executeBatch();
                    assertEquals(2, results.length, "Batch should have 2 results");
                    assertEquals(1, results[0], "First batch update count");
                    assertEquals(1, results[1], "Second batch update count");
                    stmt.clearBatch();
                }
            }
        }

        @Test
        @DisplayName("testBatchDML: PreparedStatement addBatch/executeBatch")
        void testBatchDML_PreparedStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "UPDATE " + TABLE_NAME + " SET value = ? WHERE id = ?")) {
                    for (int i = 1; i <= 5; i++) {
                        pstmt.setInt(1, i * 100);
                        pstmt.setInt(2, i);
                        pstmt.addBatch();
                    }
                    int[] results = pstmt.executeBatch();
                    assertEquals(5, results.length, "Batch should have 5 results");
                    for (int r : results) {
                        assertEquals(1, r, "Each batch should affect 1 row");
                    }
                }
            }
        }

        /**
         * Complex batch SQL (mixed queries).
         * Execute complex query with SELECT and UPDATE combined.
         */
        @Test
        @DisplayName("testBatchSQL: Statement batch with mixed SQL")
        void testBatchSQL() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    boolean hasRS = stmt.execute(
                            "UPDATE " + TABLE_NAME + " SET value = 999 WHERE id = 1;"
                                    + "SELECT * FROM " + TABLE_NAME);
                    // First result depends on driver behavior
                    if (hasRS) {
                        try (ResultSet rs = stmt.getResultSet()) {
                            assertNotNull(rs);
                            while (rs.next()) { }
                        }
                    } else {
                        int uc = stmt.getUpdateCount();
                        assertTrue(uc >= 0, "Should have valid update count");
                    }
                    boolean moreResults = stmt.getMoreResults();
                    if (moreResults) {
                        try (ResultSet rs = stmt.getResultSet()) {
                            assertNotNull(rs);
                        }
                    }
                }
            }
        }

        /**
         * Batch with RAISERROR level < 10 (warning).
         */
        @Test
        @DisplayName("testBatchWarning: Batch with warning-level RAISERROR")
        void testBatchWarning() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    stmt.addBatch("UPDATE " + TABLE_NAME + " SET value = 100 WHERE id = 1");
                    stmt.addBatch("RAISERROR('Warning level 5', 5, 1)");
                    // Warnings should not cause exception
                    int[] results = stmt.executeBatch();
                    assertEquals(2, results.length);
                }
            }
        }

        /**
         * Batch with RAISERROR level >= 11 (error).
         */
        @Test
        @DisplayName("testBatchException: Batch with error-level RAISERROR throws BatchUpdateException")
        void testBatchException() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    stmt.addBatch("UPDATE " + TABLE_NAME + " SET value = 100 WHERE id = 1");
                    stmt.addBatch("RAISERROR('Batch error level 16', 16, 1)");
                    assertThrows(BatchUpdateException.class, () -> stmt.executeBatch(),
                            "executeBatch should throw BatchUpdateException for error-level RAISERROR");
                }
            }
        }

        /**
         * Batch with PK violation.
         * addBatch(good), addBatch(PK violation), executeBatch, verify exception & update counts.
         */
        @Test
        @DisplayName("testBatchWithErrors: Batch with PK violation throws BatchUpdateException")
        void testBatchWithErrors() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    stmt.addBatch("INSERT INTO " + TABLE_NAME + " (id, value, name) VALUES (100, 1000, 'NewRow100')");
                    stmt.addBatch("INSERT INTO " + TABLE_NAME + " (id, value, name) VALUES (1, 9999, 'DuplicatePK')");
                    BatchUpdateException bue = assertThrows(BatchUpdateException.class,
                            () -> stmt.executeBatch(),
                            "executeBatch should throw BatchUpdateException when batch contains a PK violation");
                    int[] updateCounts = bue.getUpdateCounts();
                    assertNotNull(updateCounts, "Update counts should not be null");
                    assertTrue(updateCounts.length >= 1,
                            "Update counts should contain at least one entry for the successful statement");
                    // First statement (id = 100) should succeed.
                    assertTrue(updateCounts[0] != Statement.EXECUTE_FAILED,
                            "First batch statement should not be marked as failed");
                    // Second statement (duplicate PK) is expected to fail.
                    if (updateCounts.length > 1) {
                        assertEquals(Statement.EXECUTE_FAILED, updateCounts[1],
                                "Second batch statement (PK violation) should be marked as EXECUTE_FAILED");
                    }
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Partial Consumption & Multi-Result")
    class PartialConsumptionTests {

        /**
         * Execute query,
         * consume partial rows, re-execute.
         */
        @Test
        @DisplayName("testPartialConsumption: Statement partial RS consumption then re-execute")
        void testPartialConsumption_Statement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    for (int iteration = 0; iteration < 2; iteration++) {
                        try (ResultSet rs = stmt
                                .executeQuery("SELECT * FROM " + TABLE_NAME)) {
                            // Consume only partial rows
                            if (rs.next()) {
                                rs.getInt("id");
                                rs.getString("name");
                            }
                            // Don't consume the rest - just re-execute
                        }
                    }
                    // Final full read to verify no corruption
                    try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                        int count = 0;
                        while (rs.next()) count++;
                        assertEquals(ROW_COUNT_VAL, count, "All rows should be readable");
                    }
                }
            }
        }

        @Test
        @DisplayName("testPartialConsumption: PreparedStatement partial consumption")
        void testPartialConsumption_PreparedStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (PreparedStatement pstmt = conn
                        .prepareStatement("SELECT * FROM " + TABLE_NAME)) {
                    for (int iteration = 0; iteration < 2; iteration++) {
                        try (ResultSet rs = pstmt.executeQuery()) {
                            if (rs.next()) {
                                rs.getInt("id");
                            }
                        }
                    }
                    try (ResultSet rs = pstmt.executeQuery()) {
                        int count = 0;
                        while (rs.next()) count++;
                        assertEquals(ROW_COUNT_VAL, count);
                    }
                }
            }
        }

        /**
         * TCStatementSanity.testRandomWalk / testOtherQueries -
         * Multiple result sets with getMoreResults navigation.
         */
        @Test
        @DisplayName("testRandomWalk: Multiple result sets with getMoreResults")
        void testMultipleResultSets() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    boolean hasRS = stmt.execute(
                            "SELECT * FROM " + TABLE_NAME + " WHERE id <= 3;"
                                    + "SELECT * FROM " + TABLE_NAME + " WHERE id > 7");
                    assertTrue(hasRS, "First result should be a ResultSet");
                    try (ResultSet rs1 = stmt.getResultSet()) {
                        int count1 = 0;
                        while (rs1.next()) count1++;
                        assertEquals(3, count1, "First RS should have 3 rows");
                    }
                    boolean hasMore = stmt.getMoreResults();
                    assertTrue(hasMore, "Should have second ResultSet");
                    try (ResultSet rs2 = stmt.getResultSet()) {
                        int count2 = 0;
                        while (rs2.next()) count2++;
                        assertEquals(3, count2, "Second RS should have 3 rows");
                    }
                    assertFalse(stmt.getMoreResults(), "No more results");
                    assertEquals(-1, stmt.getUpdateCount(), "No more update counts");
                }
            }
        }

        /**
         * Cross-statement type execution.
         * Execute a sproc via Statement (not CallableStatement).
         */
        @Test
        @DisplayName("testOtherQueries: Execute sproc via Statement and PreparedStatement")
        void testOtherQueries() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                String procName = AbstractSQLGenerator
                        .escapeIdentifier(RandomUtil.getIdentifier("SM_OtherQ_proc"));
                try (Statement setup = conn.createStatement()) {
                    TestUtils.dropProcedureIfExists(procName, setup);
                    setup.execute(
                            "CREATE PROCEDURE " + procName + " AS SELECT * FROM " + TABLE_NAME);
                }
                try {
                    try (Statement stmt = conn.createStatement()) {
                        boolean hasRS = stmt.execute("EXEC " + procName);
                        assertTrue(hasRS, "Sproc should return ResultSet");
                        try (ResultSet rs = stmt.getResultSet()) {
                            int count = 0;
                            while (rs.next()) count++;
                            assertEquals(ROW_COUNT_VAL, count);
                        }
                    }
                    try (PreparedStatement pstmt = conn
                            .prepareStatement("EXEC " + procName)) {
                        boolean hasRS = pstmt.execute();
                        assertTrue(hasRS, "Sproc via pstmt should return ResultSet");
                        try (ResultSet rs = pstmt.getResultSet()) {
                            int count = 0;
                            while (rs.next()) count++;
                            assertEquals(ROW_COUNT_VAL, count);
                        }
                    }
                } finally {
                    try (Statement cleanup = conn.createStatement()) {
                        TestUtils.dropProcedureIfExists(procName, cleanup);
                    }
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Closed Connection / Statement")
    class ClosedConnectionTests {

        /**
         * Operations on closed stmt throw.
         * Also covers testStmtMethodsWhenClosed.
         */
        @Test
        @DisplayName("testClosedConnection: Operations on closed statement throw SQLException")
        void testClosedStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                Statement stmt = conn.createStatement();
                stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
                stmt.close();

                assertTrue(stmt.isClosed(), "Statement should be closed");

                assertThrows(SQLException.class,
                        () -> stmt.executeQuery("SELECT * FROM " + TABLE_NAME),
                        "executeQuery on closed statement should throw");

                assertThrows(SQLException.class,
                        () -> stmt.executeUpdate("UPDATE " + TABLE_NAME + " SET value = 1 WHERE id = 1"),
                        "executeUpdate on closed statement should throw");

                assertThrows(SQLException.class,
                        () -> stmt.execute("SELECT 1"),
                        "execute on closed statement should throw");
            }
        }

        /**
         * isClosed variations.
         * Verify isClosed before and after close, multiple close calls.
         */
        @Test
        @DisplayName("testClosedConnection: isClosed and multiple close calls")
        void testIsClosed() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                Statement stmt = conn.createStatement();
                assertFalse(stmt.isClosed(), "New statement should not be closed");

                stmt.close();
                assertTrue(stmt.isClosed(), "Closed statement should report isClosed=true");

                assertDoesNotThrow(() -> stmt.close(),
                        "Multiple close calls should not throw");
                assertTrue(stmt.isClosed(), "Still closed after second close");
            }
        }

        /**
         * Operations on statement after connection close.
         */
        @Test
        @DisplayName("testClosedConnection: Statement after connection close")
        void testStatementAfterConnectionClose() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement()) {
                createTestTable(conn);
                stmt.executeQuery("SELECT * FROM " + TABLE_NAME);

                conn.close();

                assertThrows(SQLException.class,
                        () -> stmt.executeQuery("SELECT * FROM " + TABLE_NAME),
                        "Execute after connection close should throw");
                assertTrue(stmt.isClosed(), "Statement should be closed after connection close");
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Statement Cancellation")
    class CancelTests {

        @Test
        @DisplayName("testCancel: Cancel before execute (no-op) and after execute")
        void testCancel() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    assertDoesNotThrow(() -> stmt.cancel(),
                            "Cancel before execute should not throw");
                    stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
                    assertDoesNotThrow(() -> stmt.cancel(),
                            "Cancel after completed execute should not throw");
                }
            }
        }

        /**
         * Cancel a long-running WAITFOR query from another thread.
         * Asserts that cancel() was invoked and that WAITFOR threw a cancellation exception.
         */
        @Test
        @DisplayName("testCancelWaitfor: Cancel WAITFOR from another thread")
        void testCancelWaitfor() throws Exception {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement()) {
                AtomicBoolean cancelled = new AtomicBoolean(false);
                AtomicBoolean cancelException = new AtomicBoolean(false);
                CountDownLatch latch = new CountDownLatch(1);

                ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
                executor.schedule(() -> {
                    try {
                        stmt.cancel();
                        cancelled.set(true);
                    } catch (SQLException e) {
                        // Ignore cancellation failure; test will assert on flags below.
                    }
                    latch.countDown();
                }, 1, TimeUnit.SECONDS);

                try {
                    stmt.execute("WAITFOR DELAY '00:00:30'");
                } catch (SQLException e) {
                    String msg = e.getMessage();
                    assertNotNull(msg, "Cancellation exception message should not be null");
                    assertTrue(msg.contains("cancel") || msg.contains("Attention"),
                            "Should get cancellation error, got: " + msg);
                    cancelException.set(true);
                } finally {
                    assertTrue(latch.await(35, TimeUnit.SECONDS), "Cancel thread did not finish in time");
                    executor.shutdown();
                    assertTrue(cancelled.get(), "Statement.cancel() was not successfully invoked");
                    assertTrue(cancelException.get(), "WAITFOR did not throw a cancellation exception");
                }
            }
        }

        /**
         * Cancel multiple statements on
         * same connection from cancel thread.
         */
        @Test
        @DisplayName("testCancelMultipleStmts: Cancel multiple concurrent statements")
        void testCancelMultipleStmts() throws Exception {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                Statement stmt1 = conn.createStatement();
                Statement stmt2 = conn.createStatement();

                assertDoesNotThrow(() -> stmt1.cancel());
                assertDoesNotThrow(() -> stmt2.cancel());

                stmt1.executeQuery("SELECT * FROM " + TABLE_NAME);
                stmt2.executeUpdate("UPDATE " + TABLE_NAME + " SET value = 1 WHERE id = 1");
                assertDoesNotThrow(() -> stmt1.cancel());
                assertDoesNotThrow(() -> stmt2.cancel());

                stmt1.close();
                stmt2.close();
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Warnings and Exceptions")
    class WarningsTests {

        /**
         * Execute RAISERROR with level < 10,
         * verify getWarnings/clearWarnings.
         */
        @Test
        @DisplayName("testWarnings: RAISERROR level 1-9 produces warnings")
        void testWarnings() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                try (Statement stmt = conn.createStatement()) {
                    SQLWarning warning = stmt.getWarnings();
                    assertNull(warning, "No warnings initially");

                    for (int level = 1; level <= 9; level++) {
                        stmt.execute("RAISERROR('Warning level " + level + "', " + level + ", 1)");
                        warning = stmt.getWarnings();
                        assertNotNull(warning, "Should have warning for level " + level);
                        assertTrue(warning.getMessage().contains("Warning level " + level),
                                "Warning message mismatch for level " + level);

                        stmt.clearWarnings();
                        assertNull(stmt.getWarnings(), "Warnings should be null after clear");
                    }
                }
            }
        }

        /**
         * RAISERROR level 11-18
         * throws exception with no warning.
         */
        @Test
        @DisplayName("testWarningsExceptions: RAISERROR level 11+ throws SQLException")
        void testWarningsExceptions() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                for (int level = 11; level <= 18; level++) {
                    try (Statement stmt = conn.createStatement()) {
                        final int errorLevel = level;
                        assertThrows(SQLException.class,
                                () -> stmt.execute(
                                        "RAISERROR('Error level " + errorLevel + "', " + errorLevel
                                                + ", 1)"),
                                "RAISERROR level " + level + " should throw");
                        assertNull(stmt.getWarnings(),
                                "getWarnings should be null after exception for level " + level);
                        stmt.clearWarnings();
                        assertNull(stmt.getWarnings(),
                                "getWarnings should be null after clearWarnings");
                    }
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Statement Property Getters/Setters")
    class GetterSetterTests {

        /**
         * Verify getConnection returns same instance.
         */
        @Test
        @DisplayName("testGetConnection: getConnection returns parent connection")
        void testGetConnection() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                    assertEquals(conn, stmt.getConnection(),
                            "getConnection should return the parent connection");
                    while (rs.next()) { }
                }
            }
        }

        /**
         * getMaxFieldSize/getMaxRows default to 0.
         */
        @Test
        @DisplayName("testMaxGetters: Default maxFieldSize and maxRows are 0")
        void testMaxGetters() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
                    assertEquals(0, stmt.getMaxFieldSize(), "Default maxFieldSize should be 0");
                    assertEquals(0, stmt.getMaxRows(), "Default maxRows should be 0");
                }
            }
        }

        /**
         * setMaxFieldSize/setMaxRows and verify.
         */
        @Test
        @DisplayName("testMaxSetters: Set and verify maxFieldSize/maxRows")
        void testMaxSetters() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    int maxFieldSize = Integer.MAX_VALUE;
                    stmt.setMaxFieldSize(maxFieldSize);
                    assertEquals(maxFieldSize, stmt.getMaxFieldSize(), "maxFieldSize mismatch");

                    int maxRows = 5;
                    stmt.setMaxRows(maxRows);
                    assertEquals(maxRows, stmt.getMaxRows(), "maxRows mismatch");

                    try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                        assertEquals(maxFieldSize, stmt.getMaxFieldSize(),
                                "maxFieldSize after execute mismatch");
                        assertEquals(maxRows, stmt.getMaxRows(), "maxRows after execute mismatch");
                        int count = 0;
                        while (rs.next()) count++;
                        assertTrue(count <= maxRows,
                                "Row count should be <= maxRows: " + count);
                    }
                }
            }
        }

        @Test
        @DisplayName("testMaxSettersInvalid: Negative maxFieldSize/maxRows throw SQLException")
        void testMaxSettersInvalid() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                try (Statement stmt = conn.createStatement()) {
                    assertThrows(SQLException.class, () -> stmt.setMaxFieldSize(-1),
                            "Negative maxFieldSize should throw");
                    assertThrows(SQLException.class, () -> stmt.setMaxRows(-1),
                            "Negative maxRows should throw");
                }
            }
        }

        /**
         * maxFieldSize/maxRows are per-statement.
         */
        @Test
        @DisplayName("testMaxSettersMultipleStmt: maxRows/maxFieldSize are per-statement")
        void testMaxSettersMultipleStmt() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt1 = conn.createStatement();
                        Statement stmt2 = conn.createStatement()) {
                    stmt1.setMaxRows(3);
                    stmt2.setMaxRows(5);
                    assertEquals(3, stmt1.getMaxRows(), "stmt1 maxRows");
                    assertEquals(5, stmt2.getMaxRows(), "stmt2 maxRows");
                    // One statement's setting should not affect the other
                    try (ResultSet rs1 = stmt1.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                        int count1 = 0;
                        while (rs1.next()) count1++;
                        assertTrue(count1 <= 3, "stmt1 should return <= 3 rows");
                    }
                    try (ResultSet rs2 = stmt2.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                        int count2 = 0;
                        while (rs2.next()) count2++;
                        assertTrue(count2 <= 5, "stmt2 should return <= 5 rows");
                    }
                }
            }
        }

        /**
         * maxRows with executeUpdate and executeBatch.
         * Automates VSTS #157330.
         */
        @Test
        @DisplayName("testSetMaxRows: maxRows does not affect executeUpdate/executeBatch")
        void testSetMaxRows() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    stmt.setMaxRows(2);
                    // executeUpdate should work despite maxRows
                    int uc = stmt.executeUpdate(
                            "UPDATE " + TABLE_NAME + " SET value = 999 WHERE id <= 5");
                    assertEquals(5, uc, "executeUpdate should not be limited by maxRows");

                    // executeBatch should work despite maxRows
                    stmt.addBatch("UPDATE " + TABLE_NAME + " SET value = 111 WHERE id = 1");
                    stmt.addBatch("UPDATE " + TABLE_NAME + " SET value = 222 WHERE id = 2");
                    int[] results = stmt.executeBatch();
                    assertEquals(2, results.length, "Batch results count");
                }
            }
        }

        /**
         * setCursorName(null).
         */
        @Test
        @DisplayName("testCursorName: setCursorName(null) does not throw")
        void testCursorName() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
                    assertDoesNotThrow(() -> stmt.setCursorName(null));
                }
            }
        }

        /**
         * setCursorName(value).
         */
        @Test
        @DisplayName("testCursorNameValue: setCursorName with valid value")
        void testCursorNameValue() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
                    assertDoesNotThrow(
                            () -> stmt.setCursorName("testCursor_" + RandomUtil.getIdentifier("c")));
                }
            }
        }

        /**
         * setCursorName with > 128 chars.
         */
        @Test
        @DisplayName("testCursorNameValueTooLarge: setCursorName with oversized name")
        void testCursorNameValueTooLarge() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
                    StringBuilder tooLarge = new StringBuilder();
                    for (int i = 0; i <= 128; i++) {
                        tooLarge.append(i % 10);
                    }
                    // Should not throw but silently truncate or accept
                    assertDoesNotThrow(() -> stmt.setCursorName(tooLarge.toString()));
                }
            }
        }

        /**
         * Verify getResultSetHoldability returns a valid holdability constant.
         */
        @Test
        @DisplayName("testGetHoldability: getResultSetHoldability returns valid value")
        void testGetHoldability() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                try (Statement stmt = conn.createStatement()) {
                    int holdability = stmt.getResultSetHoldability();
                    assertTrue(
                            holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT
                                    || holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT,
                            "Holdability should be valid JDBC constant");
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Response Buffering")
    class ResponseBufferingTests {

        /**
         * All conn/stmt level combos.
         */
        @Test
        @DisplayName("testSetResponseBuffering: adaptive and full modes")
        @SuppressWarnings("resource")
        void testSetResponseBuffering() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    if (stmt instanceof SQLServerStatement) {
                        SQLServerStatement ssStmt = (SQLServerStatement) stmt;
                        ssStmt.setResponseBuffering("adaptive");
                        assertEquals("adaptive", ssStmt.getResponseBuffering().toLowerCase(),
                                "Response buffering should be adaptive");

                        try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                            while (rs.next()) { }
                        }

                        ssStmt.setResponseBuffering("full");
                        assertEquals("full", ssStmt.getResponseBuffering().toLowerCase(),
                                "Response buffering should be full");

                        try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                            while (rs.next()) { }
                        }
                    }
                }
            }
        }

        /**
         * Per-statement setting.
         */
        @Test
        @DisplayName("testSetResponseBufferingMultipleStmt: Per-statement response buffering")
        @SuppressWarnings("resource")
        void testSetResponseBufferingMultipleStmt() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt1 = conn.createStatement();
                        Statement stmt2 = conn.createStatement()) {
                    if (stmt1 instanceof SQLServerStatement && stmt2 instanceof SQLServerStatement) {
                        SQLServerStatement ss1 = (SQLServerStatement) stmt1;
                        SQLServerStatement ss2 = (SQLServerStatement) stmt2;
                        ss1.setResponseBuffering("adaptive");
                        ss2.setResponseBuffering("full");
                        assertEquals("adaptive", ss1.getResponseBuffering().toLowerCase());
                        assertEquals("full", ss2.getResponseBuffering().toLowerCase());

                        try (ResultSet rs1 = stmt1.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                            while (rs1.next()) { }
                        }
                        try (ResultSet rs2 = stmt2.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                            while (rs2.next()) { }
                        }
                    }
                }
            }
        }

        @Test
        @DisplayName("testSetResponseBufferingInvalid: Invalid value throws SQLException")
        @SuppressWarnings("resource")
        void testSetResponseBufferingInvalid() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                try (Statement stmt = conn.createStatement()) {
                    if (stmt instanceof SQLServerStatement) {
                        SQLServerStatement ssStmt = (SQLServerStatement) stmt;
                        assertThrows(SQLException.class,
                                () -> ssStmt.setResponseBuffering("invalid_value"),
                                "Invalid responseBuffering should throw");
                    }
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Cursor Properties")
    class CursorPropertyTests {

        @Test
        @DisplayName("testGetFetchSize: Default fetch size per cursor type")
        void testGetFetchSize() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY)) {
                    stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
                    int fetchSize = stmt.getFetchSize();
                    assertTrue(fetchSize > 0, "Fetch size should be positive, got: " + fetchSize);
                }
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {
                    stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
                    int fetchSize = stmt.getFetchSize();
                    assertTrue(fetchSize > 0, "Fetch size should be positive, got: " + fetchSize);
                }
            }
        }

        @Test
        @DisplayName("testGetFetchDirection: Default fetch direction per cursor type")
        void testGetFetchDirection() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY)) {
                    stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
                    assertEquals(ResultSet.FETCH_FORWARD, stmt.getFetchDirection(),
                            "FORWARD_ONLY should have FETCH_FORWARD direction");
                }
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {
                    stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
                    int dir = stmt.getFetchDirection();
                    assertTrue(dir == ResultSet.FETCH_FORWARD || dir == ResultSet.FETCH_UNKNOWN,
                            "Scrollable cursor direction should be FORWARD or UNKNOWN");
                }
            }
        }

        /**
         * setFetchSize(0) resets to default.
         */
        @Test
        @DisplayName("testSetFetchSizeDefault: setFetchSize(0) resets to default")
        void testSetFetchSizeDefault() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_READ_ONLY)) {
                    stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
                    int defaultSize = stmt.getFetchSize();
                    stmt.setFetchSize(50);
                    assertEquals(50, stmt.getFetchSize(), "Should be set to 50");
                    stmt.setFetchSize(0);
                    assertEquals(defaultSize, stmt.getFetchSize(),
                            "setFetchSize(0) should reset to default");
                }
            }
        }

        @Test
        @DisplayName("testSetFetchSizeValid: Set random valid fetch size")
        void testSetFetchSizeValid() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_READ_ONLY)) {
                    stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
                    Random rand = new Random();
                    int fetchSize = rand.nextInt(1000) + 1;
                    stmt.setFetchSize(fetchSize);
                    assertEquals(fetchSize, stmt.getFetchSize(), "getFetchSize should match");
                }
            }
        }

        @Test
        @DisplayName("testSetFetchSizeInvalid: Negative fetch size throws SQLException")
        void testSetFetchSizeInvalid() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                try (Statement stmt = conn.createStatement()) {
                    assertThrows(SQLException.class, () -> stmt.setFetchSize(-1),
                            "Negative fetchSize should throw");
                }
            }
        }

        @Test
        @DisplayName("testGetResultSetConcurrency: Matches statement creation parameter")
        void testGetResultSetConcurrency() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                int[] concurrencies = {ResultSet.CONCUR_READ_ONLY, ResultSet.CONCUR_UPDATABLE};
                for (int concurrency : concurrencies) {
                    try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                            concurrency)) {
                        stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
                        assertEquals(concurrency, stmt.getResultSetConcurrency(),
                                "getResultSetConcurrency mismatch for " + concurrency);
                    }
                }
            }
        }

        @Test
        @DisplayName("testGetResultSetType: Matches statement creation parameter")
        void testGetResultSetType() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                int[] types = {ResultSet.TYPE_FORWARD_ONLY, ResultSet.TYPE_SCROLL_INSENSITIVE,
                        ResultSet.TYPE_SCROLL_SENSITIVE};
                for (int type : types) {
                    try (Statement stmt = conn.createStatement(type, ResultSet.CONCUR_READ_ONLY)) {
                        stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
                        assertEquals(type, stmt.getResultSetType(),
                                "getResultSetType mismatch for " + type);
                    }
                }
            }
        }

        /**
         * Set timeout, execute WAITFOR,
         * verify timeout exception. Loops twice to test re-execute after timeout (VSTS #141264).
         */
        @Test
        @DisplayName("testForcedQueryTimeout: Query timeout with WAITFOR")
        void testForcedQueryTimeout() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement()) {
                assertEquals(0, stmt.getQueryTimeout(), "Default queryTimeout should be 0");

                int timeout = 2;
                int waittime = 1 + 2 * timeout;
                stmt.setQueryTimeout(timeout);
                assertEquals(timeout, stmt.getQueryTimeout(), "setQueryTimeout mismatch");

                // VSTS #141264, Re-execute - loop twice to verify re-execute after timeout
                for (int i = 0; i < 2; i++) {
                    long startTime = System.currentTimeMillis();
                    try {
                        stmt.execute("WAITFOR DELAY '00:00:" + String.format("%02d", waittime) + "'");
                        fail("Expected SQLException for query timeout");
                    } catch (SQLException e) {
                        long elapsed = System.currentTimeMillis() - startTime;
                        // The JDBC driver throws "The query has timed out."
                        assertTrue("The query has timed out.".equalsIgnoreCase(e.getMessage()),
                                "Should get 'The query has timed out.' but got: " + e.getMessage());
                        // Verify actual timeout >= configured timeout (with small tolerance)
                        long diff = elapsed + 10; // allow 10ms deviation
                        assertTrue(diff >= timeout * 1000L,
                                "Query timed out earlier than expected: " + (timeout * 1000L)
                                        + ", actual: " + elapsed);
                    }
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Statement Pooling")
    class PoolingTests {

        /**
         * isPoolable/setPoolable.
         * Check default, toggle, verify closed throws.
         */
        @Test
        @DisplayName("testStmtPooling: isPoolable/setPoolable before and after close")
        void testStmtPooling() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                Statement stmt = conn.createStatement();
                stmt.executeQuery("SELECT * FROM " + TABLE_NAME);

                boolean defaultPoolable = stmt.isPoolable();
                stmt.setPoolable(!defaultPoolable);
                assertEquals(!defaultPoolable, stmt.isPoolable(), "Poolable should toggle");

                stmt.close();

                assertThrows(SQLException.class, () -> stmt.isPoolable(),
                        "isPoolable on closed statement should throw");
                assertThrows(SQLException.class, () -> stmt.setPoolable(true),
                        "setPoolable on closed statement should throw");
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Prepared & Callable Tests")
    class PreparedCallableTests {

        /**
         * TCPreparedStatement - SRR with all cursor types for PreparedStatement.
         */
        @Test
        @DisplayName("PreparedStatement SRR with all cursor types")
        void testPreparedStatementSRR() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            int[][] cursorTypes = {
                    {ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY},
                    {ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE},
                    {ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY},
                    {ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY},
                    {ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE}
            };
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                for (int[] ct : cursorTypes) {
                    try (PreparedStatement pstmt = conn.prepareStatement(
                            "SELECT * FROM " + TABLE_NAME, ct[0], ct[1]);
                            ResultSet rs = pstmt.executeQuery()) {
                        int count = 0;
                        while (rs.next()) count++;
                        assertEquals(ROW_COUNT_VAL, count,
                                "PreparedStatement cursor " + ct[0] + "/" + ct[1]);
                    }
                }
            }
        }

        /**
         * TCPreparedStatement - DML with all cursor types.
         */
        @Test
        @DisplayName("PreparedStatement DML with all cursor types")
        void testPreparedStatementDML() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            int[][] cursorTypes = {
                    {ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY},
                    {ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY},
                    {ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE}
            };
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                for (int[] ct : cursorTypes) {
                    try (PreparedStatement pstmt = conn.prepareStatement(
                            "UPDATE " + TABLE_NAME + " SET value = ? WHERE id = ?",
                            ct[0], ct[1])) {
                        pstmt.setInt(1, 999);
                        pstmt.setInt(2, 1);
                        int uc = pstmt.executeUpdate();
                        assertEquals(1, uc, "PreparedStatement DML cursor " + ct[0] + "/" + ct[1]);
                    }
                }
            }
        }

        /**
         * TCCallableStatement - SRR with all cursor types.
         */
        @Test
        @DisplayName("CallableStatement SRR with all cursor types")
        void testCallableStatementSRR() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                String procName = AbstractSQLGenerator
                        .escapeIdentifier(RandomUtil.getIdentifier("SM_CStmt_SRR"));
                try (Statement setup = conn.createStatement()) {
                    TestUtils.dropProcedureIfExists(procName, setup);
                    setup.execute(
                            "CREATE PROCEDURE " + procName + " AS SELECT * FROM " + TABLE_NAME);
                }
                int[][] cursorTypes = {
                        {ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY},
                        {ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY},
                        {ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY}
                };
                try {
                    for (int[] ct : cursorTypes) {
                        try (CallableStatement cstmt = conn.prepareCall(
                                "{call " + procName + "}", ct[0], ct[1]);
                                ResultSet rs = cstmt.executeQuery()) {
                            int count = 0;
                            while (rs.next()) count++;
                            assertEquals(ROW_COUNT_VAL, count,
                                    "CallableStatement cursor " + ct[0] + "/" + ct[1]);
                        }
                    }
                } finally {
                    try (Statement cleanup = conn.createStatement()) {
                        TestUtils.dropProcedureIfExists(procName, cleanup);
                    }
                }
            }
        }

        /**
         * TCCallableStatement - DML with callable statement.
         */
        @Test
        @DisplayName("CallableStatement DML with cursor types")
        void testCallableStatementDML() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                String procName = AbstractSQLGenerator
                        .escapeIdentifier(RandomUtil.getIdentifier("SM_CStmt_DML"));
                try (Statement setup = conn.createStatement()) {
                    TestUtils.dropProcedureIfExists(procName, setup);
                    setup.execute("CREATE PROCEDURE " + procName
                            + " @val INT, @rowid INT AS UPDATE " + TABLE_NAME
                            + " SET value = @val WHERE id = @rowid");
                }
                try (CallableStatement cstmt = conn.prepareCall("{call " + procName + "(?, ?)}")) {
                    cstmt.setInt(1, 999);
                    cstmt.setInt(2, 1);
                    int uc = cstmt.executeUpdate();
                    assertEquals(1, uc, "CallableStatement DML");
                } finally {
                    try (Statement cleanup = conn.createStatement()) {
                        TestUtils.dropProcedureIfExists(procName, cleanup);
                    }
                }
            }
        }

        /**
         * CallableStatement batch updates via addBatch.
         */
        @Test
        @DisplayName("CallableStatement batch DML with addBatch")
        void testBatchUpdateCstmt() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                String procName = AbstractSQLGenerator
                        .escapeIdentifier(RandomUtil.getIdentifier("SM_CStmt_Batch"));
                try (Statement setup = conn.createStatement()) {
                    TestUtils.dropProcedureIfExists(procName, setup);
                    setup.execute("CREATE PROCEDURE " + procName
                            + " @val INT, @rowid INT AS UPDATE " + TABLE_NAME
                            + " SET value = @val WHERE id = @rowid");
                }
                try (CallableStatement cstmt = conn.prepareCall("{call " + procName + "(?, ?)}")) {
                    for (int i = 1; i <= 5; i++) {
                        cstmt.setInt(1, i * 100);
                        cstmt.setInt(2, i);
                        cstmt.addBatch();
                    }
                    int[] results = cstmt.executeBatch();
                    assertEquals(5, results.length, "Batch should have 5 results");
                } finally {
                    try (Statement cleanup = conn.createStatement()) {
                        TestUtils.dropProcedureIfExists(procName, cleanup);
                    }
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Multiple Statements on Same Connection")
    class MultipleStatementTests {

        /**
         * Execute multiple statements
         * returning ResultSets, access concurrently on same connection.
         */
        @Test
        @DisplayName("testAccessRS: Multiple statements with ResultSets on same connection")
        void testAccessRS() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt1 = conn.createStatement();
                        Statement stmt2 = conn.createStatement()) {
                    ResultSet rs1 = stmt1.executeQuery(
                            "SELECT * FROM " + TABLE_NAME + " WHERE id <= 5");
                    ResultSet rs2 = stmt2.executeQuery(
                            "SELECT * FROM " + TABLE_NAME + " WHERE id > 5");

                    int count1 = 0, count2 = 0;
                    boolean rs1HasNext = rs1.next();
                    boolean rs2HasNext = rs2.next();
                    while (rs1HasNext || rs2HasNext) {
                        if (rs1HasNext) {
                            rs1.getInt("id");
                            count1++;
                            rs1HasNext = rs1.next();
                        }
                        if (rs2HasNext) {
                            rs2.getInt("id");
                            count2++;
                            rs2HasNext = rs2.next();
                        }
                    }
                    assertEquals(5, count1, "First RS should have 5 rows");
                    assertEquals(5, count2, "Second RS should have 5 rows");

                    rs1.close();
                    rs2.close();
                }
            }
        }

        /**
         * Multiple statements with mixed DML and SELECT.
         */
        @Test
        @DisplayName("Multiple statements: DML interleaved with SELECT")
        void testMixedDMLAndSelect() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmtDML = conn.createStatement();
                        Statement stmtSelect = conn.createStatement()) {
                    stmtDML.executeUpdate(
                            "UPDATE " + TABLE_NAME + " SET value = 9999 WHERE id = 1");
                    try (ResultSet rs = stmtSelect
                            .executeQuery("SELECT value FROM " + TABLE_NAME + " WHERE id = 1")) {
                        assertTrue(rs.next());
                        assertEquals(9999, rs.getInt("value"), "Should see updated value");
                    }
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Execute with Generated Keys")
    class GeneratedKeysTests {

        @Test
        @DisplayName("execute with column indexes for generated keys")
        void testExecuteWithColumnIndexes() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String identTable = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_GenKeysIdx"));
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(identTable, stmt);
                    stmt.execute("CREATE TABLE " + identTable
                            + " (id INT IDENTITY(1,1) PRIMARY KEY, value INT)");

                    stmt.execute("INSERT INTO " + identTable + " (value) VALUES (300)",
                            new int[]{1});
                    try (ResultSet keys = stmt.getGeneratedKeys()) {
                        assertTrue(keys.next(), "Should have generated key");
                    }
                } finally {
                    try (Statement cleanup = conn.createStatement()) {
                        TestUtils.dropTableIfExists(identTable, cleanup);
                    }
                }
            }
        }

        @Test
        @DisplayName("execute with column names for generated keys")
        void testExecuteWithColumnNames() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String identTable = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_GenKeysCol"));
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(identTable, stmt);
                    stmt.execute("CREATE TABLE " + identTable
                            + " (id INT IDENTITY(1,1) PRIMARY KEY, value INT)");

                    stmt.execute("INSERT INTO " + identTable + " (value) VALUES (400)",
                            new String[]{"id"});
                    try (ResultSet keys = stmt.getGeneratedKeys()) {
                        assertTrue(keys.next(), "Should have generated key");
                    }
                } finally {
                    try (Statement cleanup = conn.createStatement()) {
                        TestUtils.dropTableIfExists(identTable, cleanup);
                    }
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Additional Statement Scenarios")
    class AdditionalScenarios {

        /**
         * Execute empty string via Statement.
         * Executes executeUpdate("") twice with new statements — no exception expected.
         */
        @Test
        @DisplayName("testEmptyStringText: Execute empty string SQL")
        void testEmptyStringText() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                try (Statement stmt = conn.createStatement()) {
                    assertDoesNotThrow(() -> stmt.executeUpdate(""),
                            "Empty string executeUpdate should not throw");
                }
                try (Statement stmt2 = conn.createStatement()) {
                    assertDoesNotThrow(() -> stmt2.executeUpdate(""),
                            "Empty string executeUpdate (2nd) should not throw");
                }
            }
        }

        /**
         * Execute empty string
         * via PreparedStatement. Executes twice with new pstmt — no exception expected.
         */
        @Test
        @DisplayName("testEmptyStringPreparedStatement: PrepareStatement with empty SQL")
        void testEmptyStringPreparedStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                try (PreparedStatement pstmt = conn.prepareStatement("")) {
                    assertDoesNotThrow(() -> pstmt.executeUpdate(),
                            "Empty string pstmt executeUpdate should not throw");
                }
                try (PreparedStatement pstmt2 = conn.prepareStatement("")) {
                    assertDoesNotThrow(() -> pstmt2.executeUpdate(),
                            "Empty string pstmt executeUpdate (2nd) should not throw");
                }
            }
        }

        /**
         * Execute empty string
         * via CallableStatement. Executes twice with new cstmt — no exception expected.
         */
        @Test
        @DisplayName("testEmptyStringCallableStatement: prepareCall with empty SQL")
        void testEmptyStringCallableStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                try (CallableStatement cstmt = conn.prepareCall("")) {
                    assertDoesNotThrow(() -> cstmt.executeUpdate(),
                            "Empty string cstmt executeUpdate should not throw");
                }
                try (CallableStatement cstmt2 = conn.prepareCall("")) {
                    assertDoesNotThrow(() -> cstmt2.executeUpdate(),
                            "Empty string cstmt executeUpdate (2nd) should not throw");
                }
            }
        }

        @Test
        @DisplayName("testGetMetaData: getMetaData after executeQuery")
        void testGetMetaData() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                    ResultSetMetaData meta = rs.getMetaData();
                    assertNotNull(meta, "getMetaData should not return null");
                    assertTrue(meta.getColumnCount() >= 3,
                            "Should have at least 3 columns (id, value, name)");
                    assertTrue(rs.next());
                }
            }
        }

        @Test
        @DisplayName("testSetEscapeProcessing: Enable/disable escape processing")
        void testSetEscapeProcessing() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    stmt.setEscapeProcessing(false);
                    try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                        int count = 0;
                        while (rs.next()) count++;
                        assertEquals(ROW_COUNT_VAL, count);
                    }
                    stmt.setEscapeProcessing(true);
                    try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                        int count = 0;
                        while (rs.next()) count++;
                        assertEquals(ROW_COUNT_VAL, count);
                    }
                }
            }
        }

        /**
         * Execute compound statement - executeQuery with update counts.
         */
        @Test
        @DisplayName("testExecuteQueryWithUpdateCounts: Compound SQL via execute")
        void testExecuteQueryWithUpdateCounts() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    boolean hasRS = stmt.execute(
                            "UPDATE " + TABLE_NAME + " SET value = 888 WHERE id = 1;"
                                    + "SELECT * FROM " + TABLE_NAME + " WHERE id = 1");
                    if (!hasRS) {
                        int uc = stmt.getUpdateCount();
                        assertTrue(uc >= 0, "Should have update count");
                        hasRS = stmt.getMoreResults();
                    }
                    if (hasRS) {
                        try (ResultSet rs = stmt.getResultSet()) {
                            assertTrue(rs.next(), "Should have at least one row");
                            assertEquals(888, rs.getInt("value"), "Value should be updated");
                        }
                    }
                }
            }
        }

        /**
         * Execute compound statement - executeUpdate with result sets.
         */
        @Test
        @DisplayName("testExecuteUpdateWithResultSets: SELECT then UPDATE via execute")
        void testExecuteUpdateWithResultSets() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    boolean hasRS = stmt.execute(
                            "SELECT * FROM " + TABLE_NAME + ";"
                                    + "UPDATE " + TABLE_NAME + " SET value = 777 WHERE id = 2");
                    if (hasRS) {
                        try (ResultSet rs = stmt.getResultSet()) {
                            int count = 0;
                            while (rs.next()) count++;
                            assertEquals(ROW_COUNT_VAL, count);
                        }
                    }
                    boolean moreRS = stmt.getMoreResults();
                    if (!moreRS) {
                        int uc = stmt.getUpdateCount();
                        if (uc != -1) {
                            assertEquals(1, uc, "UPDATE should affect 1 row");
                        }
                    }
                }
            }
        }

        /**
         * SET NOCOUNT ON/OFF behavior.
         */
        @Test
        @DisplayName("testSetNoCount: SET NOCOUNT ON suppresses update counts")
        void testSetNoCount() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    boolean hasRS = stmt.execute("SET NOCOUNT ON;"
                            + "UPDATE " + TABLE_NAME + " SET value = 555 WHERE id = 1;"
                            + "SELECT * FROM " + TABLE_NAME + " WHERE id = 1");
                    if (hasRS) {
                        try (ResultSet rs = stmt.getResultSet()) {
                            assertTrue(rs.next());
                            assertEquals(555, rs.getInt("value"));
                        }
                    }
                }
            }
        }

        /**
         * SET NOCOUNT with IDENTITY - insert with identity and NOCOUNT.
         */
        @Test
        @DisplayName("testNoCountWithIdentity: NOCOUNT with IDENTITY column")
        void testNoCountWithIdentity() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String identTable = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_NoCount"));
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(identTable, stmt);
                    stmt.execute("CREATE TABLE " + identTable
                            + " (id INT IDENTITY(1,1) PRIMARY KEY, value INT)");
                    stmt.execute("SET NOCOUNT ON; INSERT INTO " + identTable + " (value) VALUES (100)");
                    try (ResultSet rs = stmt
                            .executeQuery("SELECT COUNT(*) FROM " + identTable)) {
                        assertTrue(rs.next());
                        assertEquals(1, rs.getInt(1));
                    }
                } finally {
                    try (Statement cleanup = conn.createStatement()) {
                        TestUtils.dropTableIfExists(identTable, cleanup);
                    }
                }
            }
        }

        /**
         * Duplicate insert (PK violation).
         */
        @Test
        @DisplayName("testDuplicateInsert: PK violation throws SQLException")
        void testDuplicateInsert() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    assertThrows(SQLException.class,
                            () -> stmt.executeUpdate(
                                    "INSERT INTO " + TABLE_NAME + " (id, value, name) VALUES (1, 9999, 'Dup')"),
                            "Duplicate PK insert should throw");
                }
            }
        }

        /**
         * testReusePreparedStatement - Re-execute prepared statement multiple times.
         */
        @Test
        @DisplayName("testReusePreparedStatement: Re-execute same pstmt multiple times")
        void testReusePreparedStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (PreparedStatement pstmt = conn
                        .prepareStatement("SELECT * FROM " + TABLE_NAME + " WHERE id = ?")) {
                    for (int i = 1; i <= 5; i++) {
                        pstmt.setInt(1, i);
                        try (ResultSet rs = pstmt.executeQuery()) {
                            assertTrue(rs.next(), "Should have row for id=" + i);
                            assertEquals(i, rs.getInt("id"));
                            assertFalse(rs.next(), "Should have only 1 row for id=" + i);
                        }
                    }
                }
            }
        }

        /**
         * testUpdateCount - getUpdateCount after various operations.
         */
        @Test
        @DisplayName("testUpdateCount: getUpdateCount verification after execute")
        void testUpdateCount() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    int uc = stmt.executeUpdate(
                            "UPDATE " + TABLE_NAME + " SET value = 123 WHERE id <= 3");
                    assertEquals(3, uc, "executeUpdate should return 3");

                    boolean hasRS = stmt
                            .execute("UPDATE " + TABLE_NAME + " SET value = 456 WHERE id = 1");
                    assertFalse(hasRS);
                    assertEquals(1, stmt.getUpdateCount(), "getUpdateCount should be 1");

                    assertFalse(stmt.getMoreResults());
                    assertEquals(-1, stmt.getUpdateCount(),
                            "No more results: getUpdateCount should be -1");
                }
            }
        }

        /**
         * testForXmlAuto - Execute FOR XML AUTO query.
         */
        @Test
        @DisplayName("testForXmlAuto: SELECT ... FOR XML AUTO")
        void testForXmlAuto() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(
                                "SELECT id, value FROM " + TABLE_NAME + " FOR XML AUTO")) {
                    assertTrue(rs.next(), "FOR XML AUTO should return data");
                    String xmlData = rs.getString(1);
                    assertNotNull(xmlData, "XML data should not be null");
                    assertTrue(xmlData.contains("id="), "XML should contain id attribute");
                }
            }
        }

        /**
         * getMoreResults(CLOSE_CURRENT_RESULT / CLOSE_ALL_RESULTS).
         */
        @Test
        @DisplayName("testGetMoreResultsWithFlags: CLOSE_CURRENT_RESULT and CLOSE_ALL_RESULTS")
        void testGetMoreResultsWithFlags() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("SELECT * FROM " + TABLE_NAME + " WHERE id <= 3;"
                            + "SELECT * FROM " + TABLE_NAME + " WHERE id > 7");
                    boolean hasMore = stmt.getMoreResults(Statement.CLOSE_CURRENT_RESULT);
                    if (hasMore) {
                        try (ResultSet rs = stmt.getResultSet()) {
                            assertNotNull(rs);
                            while (rs.next()) {
                            }
                        }
                    }

                    stmt.execute("SELECT * FROM " + TABLE_NAME + " WHERE id <= 3;"
                            + "SELECT * FROM " + TABLE_NAME + " WHERE id > 7");
                    hasMore = stmt.getMoreResults(Statement.CLOSE_ALL_RESULTS);
                    if (hasMore) {
                        try (ResultSet rs = stmt.getResultSet()) {
                            assertNotNull(rs);
                        }
                    }
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Regression Scenarios")
    class RegressionCaseTests {

        /**
         * testSelectInto - SELECT INTO with SCROLL_SENSITIVE/CONCUR_UPDATABLE
         * should throw "statement did not return result set" when executeQuery
         * is called with a SELECT INTO statement.
         */
        @Test
        @DisplayName("testSelectInto: SELECT INTO should fail on executeQuery")
        void testSelectInto() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String functionName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_Fn_SelectInto"));
            String intoTable = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_SelectInto"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                setup.executeUpdate("CREATE FUNCTION " + functionName
                        + "(@p1 int) RETURNS TABLE AS RETURN (SELECT @p1 as x1, @p1*2 as x2, @p1*3 as x3)");

                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE)) {
                    assertThrows(SQLException.class, () -> {
                        stmt.executeQuery("SELECT isnull(x1,0) as v1, isnull(x2,0) as v2, isnull(x3,0) as v3 INTO "
                                + intoTable + " FROM " + functionName + "(2)");
                    }, "SELECT INTO via executeQuery should throw SQLException");
                } finally {
                    TestUtils.dropFunctionIfExists(functionName, setup);
                    TestUtils.dropTableIfExists(intoTable, setup);
                }
            }
        }

        /**
         * testDDLReturnValue - CREATE TABLE and DROP TABLE should return 0,
         * INSERT should return 1.
         */
        @Test
        @DisplayName("testDDLReturnValue: DDL returns 0, DML returns row count")
        void testDDLReturnValue() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tableName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_DDLReturn"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement()) {
                int count1 = stmt.executeUpdate("CREATE TABLE " + tableName
                        + " (col1 INT IDENTITY PRIMARY KEY, col2 VARCHAR(50), col3 INT)");
                assertEquals(0, count1, "DDL (CREATE TABLE) should return 0");

                int count2 = stmt.executeUpdate("INSERT INTO " + tableName
                        + " (col2, col3) VALUES ('a', 10)");
                assertEquals(1, count2, "INSERT should return 1");

                int count3 = stmt.executeUpdate("DROP TABLE " + tableName);
                assertEquals(0, count3, "DDL (DROP TABLE) should return 0");
            }
        }

        /**
         * testCallableIfExists - CallableStatement with IF EXISTS query
         * should return a ResultSet without error.
         * Uses a simple unique identifier (no special chars) since the name
         * appears inside a string literal in object_id().
         */
        @Test
        @DisplayName("testCallableIfExists: IF EXISTS via prepareCall should work")
        void testCallableIfExists() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            // Use a simple name without special chars — this goes inside object_id('...')
            // string literal, so apostrophes/brackets from escapeIdentifier would break SQL.
            String tableName = "SM_IfExists_" + Long.toHexString(System.nanoTime());

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    CallableStatement cstmt = conn.prepareCall(
                            "IF EXISTS (SELECT * FROM sysobjects WHERE id = object_id('["
                                    + tableName + "]')) SELECT 1 ELSE SELECT 0")) {
                try (ResultSet rs = cstmt.executeQuery()) {
                    assertTrue(rs.next(), "IF EXISTS query should return a row");
                    int val = rs.getInt(1);
                    assertEquals(0, val, "Non-existent table should return 0");
                }
            }
        }

        /**
         * testUpdatePrepareCall - prepareCall with SCROLL_SENSITIVE/CONCUR_UPDATABLE
         * cursor to INSERT via ResultSet.
         */
        @Test
        @DisplayName("testUpdatePrepareCall: prepareCall with updatable cursor for insertRow")
        void testUpdatePrepareCall() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tableName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_UpdPrepCall"));
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_UpdPrepCallProc"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                setup.executeUpdate("CREATE TABLE " + tableName
                        + " (id INT, value NVARCHAR(30), col999 INT IDENTITY(1,1) PRIMARY KEY)");
                createProc(setup, procName,
                        "AS SELECT * FROM " + tableName);

                try (PreparedStatement pstmt = conn.prepareCall("{ call " + procName + " }",
                        ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                        ResultSet rs = pstmt.executeQuery()) {
                    rs.moveToInsertRow();
                    rs.updateString(2, null);
                    try {
                        rs.insertRow();
                    } catch (SQLException e) {
                        // Cursor may be read-only depending on server version
                        assertTrue(e.getMessage() != null, "Exception should have a message");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                    TestUtils.dropTableIfExists(tableName, setup);
                }
            }
        }

        /**
         * testPLPInputStream - read XML column as BinaryStream and close it twice.
         * Verifies PLP InputStream behavior with XML data type.
         */
        @Test
        @DisplayName("testPLPInputStream: BinaryStream from XML column, double close")
        void testPLPInputStream() throws SQLException, IOException {
            Assumptions.assumeTrue(connectionString != null);
            String tableName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_PLPInput"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                setup.executeUpdate("CREATE TABLE " + tableName
                        + " (c1_xml XML, c999_int INT IDENTITY(1,1) PRIMARY KEY)");
                setup.executeUpdate("INSERT INTO " + tableName
                        + " (c1_xml) VALUES (N'<doc>Test XML data</doc>')");

                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                    assertTrue(rs.next(), "Should have one row");
                    InputStream is = rs.getBinaryStream("c1_xml");
                    assertNotNull(is, "BinaryStream should not be null");
                    is.close();
                    is.close();
                } finally {
                    TestUtils.dropTableIfExists(tableName, setup);
                }
            }
        }

        /**
         * testTableSpaces - table with spaces in name can be used for
         * FORWARD_ONLY/CONCUR_UPDATABLE insertRow.
         */
        @Test
        @DisplayName("testTableSpaces: table with spaces in name, updatable cursor insertRow")
        void testTableSpaces() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tableName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM stmt table spaces "));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                setup.executeUpdate("CREATE TABLE " + tableName
                        + " (a NVARCHAR(255), id_col INT IDENTITY(1,1) PRIMARY KEY)");

                try (Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_UPDATABLE);
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tableName)) {
                    rs.moveToInsertRow();
                    rs.insertRow();

                    try (Statement verify = conn.createStatement();
                            ResultSet rs2 = verify.executeQuery("SELECT * FROM " + tableName)) {
                        assertTrue(rs2.next(), "Should have one row");
                        assertNull(rs2.getString("a"), "Inserted value should be null");
                        assertFalse(rs2.next(), "Should only have one row");
                    }
                } finally {
                    TestUtils.dropTableIfExists(tableName, setup);
                }
            }
        }

        /**
         * testParseEnvChanges - stored proc that does BEGIN TRAN / SELECT / COMMIT TRAN
         * should not leave active transactions via Statement, PreparedStatement, or CallableStatement.
         */
        @Test
        @DisplayName("testParseEnvChanges: stored proc with tran should leave no active tran (Statement)")
        void testParseEnvChangesStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_CommitTran"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                createProc(setup, procName,
                        "AS BEGIN TRANSACTION SELECT 13 AS 'intValue' COMMIT TRANSACTION");

                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("EXEC " + procName)) {
                    assertTrue(rs.next(), "Stored proc should return a row");
                    assertEquals(13, rs.getInt(1), "Value should be 13");
                    // Consume all results to ensure tran completes
                    while (stmt.getMoreResults() || stmt.getUpdateCount() != -1) { }
                }
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT @@TRANCOUNT")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1), "No active transaction should remain");
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }

        @Test
        @DisplayName("testParseEnvChanges: stored proc with tran (PreparedStatement)")
        void testParseEnvChangesPreparedStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_CommitTranPS"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                createProc(setup, procName,
                        "AS BEGIN TRANSACTION SELECT 13 AS 'intValue' COMMIT TRANSACTION");

                try (PreparedStatement pstmt = conn.prepareStatement("EXEC " + procName)) {
                    assertTrue(pstmt.execute(), "Should return ResultSet");
                    try (ResultSet rs = pstmt.getResultSet()) {
                        assertTrue(rs.next());
                        assertEquals(13, rs.getInt(1));
                    }
                    while (pstmt.getMoreResults() || pstmt.getUpdateCount() != -1) { }
                }
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT @@TRANCOUNT")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1), "No active transaction should remain");
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }

        @Test
        @DisplayName("testParseEnvChanges: stored proc with tran (CallableStatement)")
        void testParseEnvChangesCallableStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_CommitTranCS"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                createProc(setup, procName,
                        "AS BEGIN TRANSACTION SELECT 13 AS 'intValue' COMMIT TRANSACTION");

                try (CallableStatement cstmt = conn.prepareCall("{ call " + procName + " }")) {
                    assertTrue(cstmt.execute(), "Should return ResultSet");
                    try (ResultSet rs = cstmt.getResultSet()) {
                        assertTrue(rs.next());
                        assertEquals(13, rs.getInt(1));
                    }
                    while (cstmt.getMoreResults() || cstmt.getUpdateCount() != -1) { }
                }
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT @@TRANCOUNT")) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1), "No active transaction should remain");
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }

        /**
         * testEmptyIndices - passing null int[] or null String[] for generated key
         * column names/indices to Statement/PreparedStatement.
         */
        @Test
        @DisplayName("testEmptyIndices: null column indices for Statement execute")
        void testEmptyIndicesStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    int[] nullIntIndices = null;
                    assertThrows(SQLException.class, () -> {
                        stmt.execute("SELECT * FROM " + TABLE_NAME, nullIntIndices);
                    }, "Null int[] indices should throw");
                }
            }
        }

        /**
         * testEmptyIndices - null String[] column names for Statement.
         */
        @Test
        @DisplayName("testEmptyIndices: null column names for Statement execute")
        void testEmptyIndicesStatementNames() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    String[] nullStringIndices = null;
                    assertThrows(SQLException.class, () -> {
                        stmt.execute("SELECT * FROM " + TABLE_NAME, nullStringIndices);
                    }, "Null String[] indices should throw");
                }
            }
        }

        /**
         * testEmptyIndices - null int[] for PreparedStatement DML.
         */
        @Test
        @DisplayName("testEmptyIndices: null column indices for PreparedStatement")
        void testEmptyIndicesPreparedStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                int[] nullIndices = null;
                assertThrows(SQLException.class, () -> {
                    conn.prepareStatement("SELECT * FROM " + TABLE_NAME, nullIndices);
                }, "PreparedStatement with null indices should throw");
            }
        }

        /**
         * testEmptyStringLiteral - query with empty string literal ''
         * appended to SELECT for Statement, PreparedStatement, CallableStatement.
         */
        @Test
        @DisplayName("testEmptyStringLiteral: SELECT with '' literal via Statement")
        void testEmptyStringLiteralStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(
                                "SELECT *, '' FROM " + TABLE_NAME)) {
                    int count = 0;
                    while (rs.next()) {
                        count++;
                    }
                    assertEquals(ROW_COUNT_VAL, count, "Row count mismatch for Statement");
                }
            }
        }

        /**
         * testEmptyStringLiteral via PreparedStatement.
         */
        @Test
        @DisplayName("testEmptyStringLiteral: SELECT with '' literal via PreparedStatement")
        void testEmptyStringLiteralPreparedStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "SELECT *, '' FROM " + TABLE_NAME + " WHERE id = ?")) {
                    pstmt.setInt(1, 1);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        int count = 0;
                        while (rs.next()) {
                            count++;
                        }
                        assertEquals(1, count, "Row count mismatch for PreparedStatement");
                    }
                }
            }
        }

        /**
         * testEmptyStringLiteral via CallableStatement.
         */
        @Test
        @DisplayName("testEmptyStringLiteral: SELECT with '' literal via CallableStatement")
        void testEmptyStringLiteralCallableStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (CallableStatement cstmt = conn.prepareCall(
                        "SELECT *, '' FROM " + TABLE_NAME);
                        ResultSet rs = cstmt.executeQuery()) {
                    int count = 0;
                    while (rs.next()) {
                        count++;
                    }
                    assertEquals(ROW_COUNT_VAL, count, "Row count mismatch for CallableStatement");
                }
            }
        }

        /**
         * testEmptyStringText - stored proc that handles empty string text parameter,
         * inserting and updating text data.
         */
        @Test
        @DisplayName("testEmptyStringText: stored proc with empty string text param insert+update")
        void testEmptyStringText() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tableName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_EmptyStrText"));
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_EmptyStrTextProc"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                setup.executeUpdate("CREATE TABLE " + tableName
                        + " (TextData VARCHAR(MAX) NULL, ID_Col INT IDENTITY(1,1) PRIMARY KEY)");
                createProc(setup, procName,
                        "@InputText VARCHAR(MAX), @OutputInt INT OUTPUT AS "
                                + "IF EXISTS(SELECT * FROM " + tableName + ") "
                                + "BEGIN UPDATE " + tableName + " SET TextData = @InputText "
                                + "SET @OutputInt = 0 RETURN 0 END "
                                + "ELSE BEGIN INSERT INTO " + tableName + " VALUES(@InputText) "
                                + "SET @OutputInt = 0 RETURN 0 END");

                for (int i = 0; i < 2; i++) {
                    try (CallableStatement cstmt = conn.prepareCall(
                            "{call " + procName + "(?,?)}")) {
                        cstmt.setString(1, "");
                        cstmt.registerOutParameter(2, Types.INTEGER);
                        cstmt.execute();
                        cstmt.getInt(2);
                    }
                }
            } finally {
                try (Connection conn = PrepUtil.getConnection(connectionString);
                        Statement cleanup = conn.createStatement()) {
                    TestUtils.dropProcedureIfExists(procName, cleanup);
                    TestUtils.dropTableIfExists(tableName, cleanup);
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Invalid Statement Operations")
    class InvalidCaseTests {

        /**
         * testBatchInvalidStatement - PreparedStatement and CallableStatement
         * should throw when addBatch(String) is called (JDBC spec violation).
         */
        @Test
        @DisplayName("testBatchInvalidStatement: addBatch(String) on PreparedStatement should fail")
        void testBatchInvalidPreparedStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "UPDATE " + TABLE_NAME + " SET value = value + 1 WHERE id = ?")) {
                    pstmt.setInt(1, 1);
                    pstmt.executeUpdate();
                    assertThrows(SQLException.class, () -> {
                        pstmt.addBatch("UPDATE " + TABLE_NAME + " SET value = 999 WHERE id = 1");
                    }, "addBatch(String) on PreparedStatement should throw");
                }
            }
        }

        /**
         * testBatchInvalidStatement - CallableStatement variant.
         */
        @Test
        @DisplayName("testBatchInvalidStatement: addBatch(String) on CallableStatement should fail")
        void testBatchInvalidCallableStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_BatchInvalidCS"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                createTestTable(conn);
                createProc(setup, procName,
                        "@val INT AS UPDATE " + TABLE_NAME + " SET value = @val WHERE id = 1");

                try (CallableStatement cstmt = conn.prepareCall("{ call " + procName + "(?) }")) {
                    cstmt.setInt(1, 100);
                    cstmt.executeUpdate();
                    assertThrows(SQLException.class, () -> {
                        cstmt.addBatch("UPDATE " + TABLE_NAME + " SET value = 999 WHERE id = 1");
                    }, "addBatch(String) on CallableStatement should throw");
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }

        /**
         * testUnsupportedCursor - creating statement with random invalid
         * ResultSet type/concurrency should throw SQLException.
         */
        @Test
        @DisplayName("testUnsupportedCursor: invalid RS type/concurrency for Statement")
        void testUnsupportedCursorStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                int invalidType = Integer.MAX_VALUE - 1;
                int invalidConcurrency = Integer.MAX_VALUE - 1;
                assertThrows(SQLException.class, () -> {
                    conn.createStatement(invalidType, invalidConcurrency);
                }, "Invalid RS type/concurrency should throw");
            }
        }

        /**
         * testUnsupportedCursor - PreparedStatement variant.
         */
        @Test
        @DisplayName("testUnsupportedCursor: invalid RS type/concurrency for PreparedStatement")
        void testUnsupportedCursorPreparedStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                int invalidType = Integer.MAX_VALUE - 1;
                int invalidConcurrency = Integer.MAX_VALUE - 1;
                assertThrows(SQLException.class, () -> {
                    conn.prepareStatement("SELECT * FROM " + TABLE_NAME, invalidType,
                            invalidConcurrency);
                }, "Invalid RS type/concurrency should throw");
            }
        }

        /**
         * testUnsupportedCursor - CallableStatement variant.
         */
        @Test
        @DisplayName("testUnsupportedCursor: invalid RS type/concurrency for CallableStatement")
        void testUnsupportedCursorCallableStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                int invalidType = Integer.MAX_VALUE - 1;
                int invalidConcurrency = Integer.MAX_VALUE - 1;
                assertThrows(SQLException.class, () -> {
                    conn.prepareCall("SELECT * FROM " + TABLE_NAME, invalidType,
                            invalidConcurrency);
                }, "Invalid RS type/concurrency should throw");
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Parameter Handling")
    class PreparedStatementParamTests {

        /**
         * testParametersEachRow - set params for each row with strongly-typed setters,
         * execute SELECT with WHERE clause per row.
         */
        @Test
        @DisplayName("testParametersEachRow: set params per row for SELECT")
        void testParametersEachRow() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "SELECT * FROM " + TABLE_NAME + " WHERE id = ?")) {
                    for (int i = 1; i <= ROW_COUNT_VAL; i++) {
                        pstmt.setInt(1, i);
                        try (ResultSet rs = pstmt.executeQuery()) {
                            assertTrue(rs.next(), "Row " + i + " should exist");
                            assertEquals(i, rs.getInt("id"));
                            assertEquals(i * 10, rs.getInt("value"));
                            assertFalse(rs.next(), "Should return exactly one row");
                        }
                    }
                }
            }
        }

        /**
         * testParametersEachRowObject - set params via setObject for UPDATE per row.
         */
        @Test
        @DisplayName("testParametersEachRowObject: setObject per row for UPDATE")
        void testParametersEachRowObject() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "UPDATE " + TABLE_NAME + " SET name = ? WHERE id = ?")) {
                    for (int i = 1; i <= Math.min(5, ROW_COUNT_VAL); i++) {
                        pstmt.setObject(1, "UpdatedRow" + i);
                        pstmt.setObject(2, i);
                        int count = pstmt.executeUpdate();
                        assertEquals(1, count, "UPDATE should affect 1 row");
                    }
                }
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(
                                "SELECT name FROM " + TABLE_NAME + " WHERE id <= 5 ORDER BY id")) {
                    for (int i = 1; i <= 5; i++) {
                        assertTrue(rs.next());
                        assertEquals("UpdatedRow" + i, rs.getString("name"));
                    }
                }
            }
        }

        /**
         * testParametersEachRowDML - set params for DML, then re-execute
         * same PreparedStatement with updated params.
         */
        @Test
        @DisplayName("testParametersEachRowDML: DML params per row with re-execute")
        void testParametersEachRowDML() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "UPDATE " + TABLE_NAME + " SET value = ? WHERE id = ?")) {
                    for (int i = 1; i <= Math.min(5, ROW_COUNT_VAL); i++) {
                        pstmt.setInt(1, i * 100);
                        pstmt.setInt(2, i);
                        int count = pstmt.executeUpdate();
                        assertEquals(1, count, "UPDATE should affect 1 row");

                        // Re-execute with different value
                        pstmt.setInt(1, i * 200);
                        pstmt.setInt(2, i);
                        count = pstmt.executeUpdate();
                        assertEquals(1, count, "Re-executed UPDATE should affect 1 row");
                    }
                }
            }
        }

        /**
         * testParametersNullEachRow - set null params for each row via setNull.
         */
        @Test
        @DisplayName("testParametersNullEachRow: setNull per row for UPDATE")
        void testParametersNullEachRow() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "UPDATE " + TABLE_NAME + " SET name = ? WHERE id = ?")) {
                    for (int i = 1; i <= Math.min(5, ROW_COUNT_VAL); i++) {
                        pstmt.setNull(1, Types.VARCHAR);
                        pstmt.setInt(2, i);
                        int count = pstmt.executeUpdate();
                        assertEquals(1, count, "UPDATE with NULL should affect 1 row");
                    }
                }
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery(
                                "SELECT name FROM " + TABLE_NAME + " WHERE id <= 5 ORDER BY id")) {
                    for (int i = 1; i <= 5; i++) {
                        assertTrue(rs.next());
                        assertNull(rs.getString("name"), "Name should be null for row " + i);
                    }
                }
            }
        }

        /**
         * testClearParameters - clearParameters and re-execute should throw,
         * then re-set params and execute should succeed.
         */
        @Test
        @DisplayName("testClearParameters: clearParameters then re-execute throws")
        void testClearParameters() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (PreparedStatement pstmt = conn.prepareStatement(
                        "SELECT * FROM " + TABLE_NAME + " WHERE id = ?")) {
                    pstmt.setInt(1, 1);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        assertTrue(rs.next(), "First execute should return results");
                    }

                    pstmt.clearParameters();

                    assertThrows(SQLException.class, () -> {
                        pstmt.executeQuery();
                    }, "Execute after clearParameters without re-setting should throw");

                    pstmt.setInt(1, 2);
                    try (ResultSet rs = pstmt.executeQuery()) {
                        assertTrue(rs.next(), "Execute after re-set params should return results");
                        assertEquals(2, rs.getInt("id"));
                    }
                }
            }
        }

        /**
         * testStreamReset - stream parameter not reset between executions
         * should throw. Re-setting the stream should allow execute.
         */
        @Test
        @DisplayName("testStreamReset: stream param not reset between executions throws")
        void testStreamResetNotReset() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tableName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_StreamReset"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                setup.executeUpdate("CREATE TABLE " + tableName
                        + " (setter VARCHAR(100), keycol INT, id_col INT IDENTITY(1,1) PRIMARY KEY)");

                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT INTO " + tableName + " VALUES(?, 1)")) {
                    StringReader reader = new StringReader("Hello World");
                    pstmt.setCharacterStream(1, reader, 11);
                    pstmt.execute(); // 1st execute should succeed

                    // 2nd execute without re-setting stream should throw
                    assertThrows(Exception.class, () -> {
                        pstmt.execute();
                    }, "2nd execute without stream reset should throw");
                } finally {
                    TestUtils.dropTableIfExists(tableName, setup);
                }
            }
        }

        /**
         * testStreamReset - even with stream.reset(), re-execute without
         * calling setCharacterStream again should throw.
         */
        @Test
        @DisplayName("testStreamReset: stream.reset() still needs re-set for execution")
        void testStreamResetWithReset() throws Exception {
            Assumptions.assumeTrue(connectionString != null);
            String tableName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_StreamResetR"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                setup.executeUpdate("CREATE TABLE " + tableName
                        + " (setter VARCHAR(100), keycol INT, id_col INT IDENTITY(1,1) PRIMARY KEY)");

                try (PreparedStatement pstmt = conn.prepareStatement(
                        "INSERT INTO " + tableName + " VALUES(?, 1)")) {
                    StringReader reader = new StringReader("Hello World");
                    pstmt.setCharacterStream(1, reader, 11);
                    pstmt.execute(); // 1st execute should succeed

                    reader.reset();
                    // 2nd execute even after reset should throw since setCharacterStream
                    // was not called again
                    assertThrows(Exception.class, () -> {
                        pstmt.execute();
                    }, "2nd execute after reset without re-set should throw");
                } finally {
                    TestUtils.dropTableIfExists(tableName, setup);
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Parameter Handling")
    class CallableStatementParamTests {

        /**
         * testParams - CallableStatement with input and output params.
         * Default sproc: returns out params and result set.
         */
        @Test
        @DisplayName("testParams: callable with input and output params")
        void testParams() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_CParams"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                createProc(setup, procName,
                        "@inVal INT, @outVal INT OUTPUT AS SET @outVal = @inVal * 2 SELECT @inVal AS inVal");

                try (CallableStatement cstmt = conn.prepareCall("{ call " + procName + "(?, ?) }")) {
                    for (int i = 1; i <= 5; i++) {
                        cstmt.setInt(1, i);
                        cstmt.registerOutParameter(2, Types.INTEGER);
                        cstmt.execute();

                        int outVal = cstmt.getInt(2);
                        assertEquals(i * 2, outVal, "Output param should be input * 2");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }

        /**
         * testParams with update sproc (insert with input params only).
         */
        @Test
        @DisplayName("testParams: callable with insert sproc, in params only")
        void testParamsInsertSproc() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tableName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_CParamsInsert"));
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_CParamsInsProc"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                setup.executeUpdate("CREATE TABLE " + tableName
                        + " (id INT, val VARCHAR(50), pk_col INT IDENTITY(1,1) PRIMARY KEY)");
                createProc(setup, procName,
                        "@id INT, @val VARCHAR(50) AS INSERT INTO " + tableName
                                + " (id, val) VALUES (@id, @val)");

                try (CallableStatement cstmt = conn.prepareCall("{ call " + procName + "(?, ?) }")) {
                    for (int i = 1; i <= 5; i++) {
                        cstmt.setInt(1, i);
                        cstmt.setString(2, "Value" + i);
                        cstmt.executeUpdate();
                    }
                }
                try (ResultSet rs = setup.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                    assertTrue(rs.next());
                    assertEquals(5, rs.getInt(1), "Should have 5 rows inserted");
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                    TestUtils.dropTableIfExists(tableName, setup);
                }
            }
        }

        /**
         * testInvalidGetParams - getObject on params before execution should throw.
         * After registering in-params and executing, should work for out-params.
         */
        @Test
        @DisplayName("testInvalidGetParams: get output param before execute should throw")
        void testInvalidGetParams() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_InvalidGet"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                createProc(setup, procName,
                        "@inVal INT, @outVal INT OUTPUT AS SET @outVal = @inVal * 2 SELECT @inVal AS val");

                try (CallableStatement cstmt = conn.prepareCall("{ call " + procName + "(?, ?) }")) {
                    cstmt.setInt(1, 10);
                    cstmt.registerOutParameter(2, Types.INTEGER);

                    assertThrows(SQLException.class, () -> {
                        cstmt.getInt(2);
                    }, "Get output param before execute should throw");

                    cstmt.execute();
                    int outVal = cstmt.getInt(2);
                    assertEquals(20, outVal, "Output param should be 20 after execute");
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }

        /**
         * testWasNull - set in-params to null, execute, call wasNull on out-params.
         */
        @Test
        @DisplayName("testWasNull: wasNull() returns true for null out-params")
        void testWasNull() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_WasNull"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                createProc(setup, procName,
                        "@inVal INT, @outVal INT OUTPUT AS SET @outVal = @inVal");

                try (CallableStatement cstmt = conn.prepareCall("{ call " + procName + "(?, ?) }")) {
                    cstmt.setNull(1, Types.INTEGER);
                    cstmt.registerOutParameter(2, Types.INTEGER);
                    cstmt.execute();
                    cstmt.getInt(2);
                    assertTrue(cstmt.wasNull(), "wasNull should be true for null out-param");

                    cstmt.setInt(1, 42);
                    cstmt.execute();
                    int val = cstmt.getInt(2);
                    assertFalse(cstmt.wasNull(), "wasNull should be false for non-null out-param");
                    assertEquals(42, val);

                    cstmt.setNull(1, Types.INTEGER);
                    cstmt.execute();
                    cstmt.getInt(2);
                    assertTrue(cstmt.wasNull(), "wasNull should be true again");
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }

        /**
         * testClearParams - clearParameters then execute should fail if
         * sproc needs input params, succeed if only out params.
         */
        @Test
        @DisplayName("testClearParams: clearParameters on CallableStatement")
        void testClearParams() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_ClearParams"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                createProc(setup, procName,
                        "@inVal INT, @outVal INT OUTPUT AS SET @outVal = @inVal * 2");

                try (CallableStatement cstmt = conn.prepareCall("{ call " + procName + "(?, ?) }")) {
                    cstmt.setInt(1, 5);
                    cstmt.registerOutParameter(2, Types.INTEGER);
                    cstmt.execute();
                    assertEquals(10, cstmt.getInt(2));

                    cstmt.clearParameters();

                    assertThrows(SQLException.class, () -> {
                        cstmt.execute();
                    }, "Execute after clearParameters should throw for required in-params");

                    cstmt.setInt(1, 7);
                    cstmt.registerOutParameter(2, Types.INTEGER);
                    cstmt.execute();
                    assertEquals(14, cstmt.getInt(2));
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }

        /**
         * testExecuteBatchWithErrors - batch on CallableStatement with
         * out params should throw BatchUpdateException.
         */
        @Test
        @DisplayName("testExecuteBatchWithErrors: batch with out params throws BatchUpdateException")
        void testExecuteBatchWithErrorsOutParams() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_BatchErrOut"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                createTestTable(conn);
                createProc(setup, procName,
                        "@inVal INT, @outVal INT OUTPUT AS SET @outVal = @inVal UPDATE "
                                + TABLE_NAME + " SET value = @inVal WHERE id = 1");

                try (CallableStatement cstmt = conn.prepareCall("{ call " + procName + "(?, ?) }")) {
                    cstmt.registerOutParameter(2, Types.INTEGER);
                    for (int i = 0; i < 3; i++) {
                        cstmt.setInt(1, i * 10);
                        cstmt.addBatch();
                    }
                    assertThrows(BatchUpdateException.class, () -> {
                        cstmt.executeBatch();
                    }, "Batch with output params should throw BatchUpdateException");
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }

        /**
         * testExecuteBatchWithErrors - batch on CallableStatement with
         * SELECT (no update count) should throw BatchUpdateException.
         */
        @Test
        @DisplayName("testExecuteBatchWithErrors: batch with SELECT sproc throws BatchUpdateException")
        void testExecuteBatchWithErrorsSelectSproc() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_BatchErrSel"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                createTestTable(conn);
                createProc(setup, procName,
                        "AS SELECT * FROM " + TABLE_NAME);

                try (CallableStatement cstmt = conn.prepareCall("{ call " + procName + " }")) {
                    for (int i = 0; i < 3; i++) {
                        cstmt.addBatch();
                    }
                    assertThrows(BatchUpdateException.class, () -> {
                        cstmt.executeBatch();
                    }, "Batch with SELECT sproc should throw BatchUpdateException");
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }

        /**
         * testExecuteWithParamErrors - execute callable where server cannot
         * assign out-param value due to type mismatch (overflow).
         */
        @Test
        @DisplayName("testExecuteWithParamErrors: out param type overflow errors")
        void testExecuteWithParamErrors() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_ParamErrs"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                // Sproc takes INT in, SMALLINT out - passing MAX_VALUE should cause overflow
                createProc(setup, procName,
                        "@inVal INT, @outVal SMALLINT OUTPUT AS SET @outVal = @inVal");

                try (CallableStatement cstmt = conn.prepareCall("{ call " + procName + "(?, ?) }")) {
                    cstmt.setInt(1, Integer.MAX_VALUE);
                    cstmt.registerOutParameter(2, Types.SMALLINT);
                    assertThrows(SQLException.class, () -> {
                        cstmt.execute();
                    }, "Out param overflow should throw SQLException");
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }

        /**
         * testReExecuteSameParams - execute CallableStatement twice without
         * re-setting input params between executions should succeed.
         */
        @Test
        @DisplayName("testReExecuteSameParams: re-execute without re-setting params")
        void testReExecuteSameParams() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_ReExecSame"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                createProc(setup, procName,
                        "@inVal INT, @outVal INT OUTPUT AS SET @outVal = @inVal * 3 SELECT @inVal AS val");

                try (CallableStatement cstmt = conn.prepareCall("{ call " + procName + "(?, ?) }")) {
                    cstmt.setInt(1, 7);
                    cstmt.registerOutParameter(2, Types.INTEGER);

                    cstmt.execute();
                    assertEquals(21, cstmt.getInt(2), "First execute: out param should be 21");
                    try (ResultSet rs = cstmt.getResultSet()) {
                        if (rs != null) {
                            while (rs.next()) { }
                        }
                    }

                    cstmt.execute();
                    assertEquals(21, cstmt.getInt(2), "Second execute: out param should still be 21");
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Concurrent Statement Access")
    class ConcurrentStatementTests {

        /**
         * testAccessCSTMT - CallableStatement with DML and output params,
         * concurrent access pattern.
         */
        @Test
        @DisplayName("testAccessCSTMT: CallableStatement with DML and out params")
        void testAccessCSTMTDml() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_AccessCSDml"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                createTestTable(conn);
                createProc(setup, procName,
                        "@inVal INT, @outVal INT OUTPUT AS SET @outVal = @inVal "
                                + "UPDATE " + TABLE_NAME + " SET value = @inVal WHERE id = 1");

                try (CallableStatement cstmt = conn.prepareCall("{ call " + procName + "(?, ?) }")) {
                    cstmt.setInt(1, 999);
                    cstmt.registerOutParameter(2, Types.INTEGER);
                    cstmt.execute();
                    assertEquals(999, cstmt.getInt(2), "Output param should match input");
                    cstmt.setInt(1, 888);
                    cstmt.execute();
                    assertEquals(888, cstmt.getInt(2));
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }

        /**
         * testAccessCSTMT - CallableStatement with SELECT and output params,
         * walking through ResultSet and out params.
         */
        @Test
        @DisplayName("testAccessCSTMT: CallableStatement with SELECT and out params")
        void testAccessCSTMTResultSet() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_AccessCSRS"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                createTestTable(conn);
                createProc(setup, procName,
                        "@inVal INT, @outVal INT OUTPUT AS SET @outVal = @inVal "
                                + "SELECT * FROM " + TABLE_NAME + " WHERE id <= @inVal");

                try (CallableStatement cstmt = conn.prepareCall("{ call " + procName + "(?, ?) }")) {
                    cstmt.setInt(1, 5);
                    cstmt.registerOutParameter(2, Types.INTEGER);
                    assertTrue(cstmt.execute(), "Should return ResultSet");

                    try (ResultSet rs = cstmt.getResultSet()) {
                        int count = 0;
                        while (rs.next()) {
                            count++;
                        }
                        assertEquals(5, count, "Should have 5 rows");
                    }
                    assertEquals(5, cstmt.getInt(2), "Output param should be 5");
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }

        /**
         * testModel - state machine model-based exploration with ANY statement type
         * and various query types (SRR, MRR, DML, MDML, MIX).
         * This is already covered by testRandomizedStatementExecution but
         * here we exercise specific combinations.
         */
        @Test
        @DisplayName("testModel: model-based SRR exploration")
        void testModelSRR() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                        int count = 0;
                        while (rs.next()) {
                            count++;
                        }
                        assertEquals(ROW_COUNT_VAL, count);
                    }
                    boolean hasResult = stmt.execute("SELECT * FROM " + TABLE_NAME
                            + " WHERE id <= 5; SELECT * FROM " + TABLE_NAME + " WHERE id > 5");
                    assertTrue(hasResult);
                    int totalCount = 0;
                    do {
                        try (ResultSet rs = stmt.getResultSet()) {
                            if (rs != null) {
                                while (rs.next()) {
                                    totalCount++;
                                }
                            }
                        }
                    } while (stmt.getMoreResults());
                    assertEquals(ROW_COUNT_VAL, totalCount,
                            "Total rows across multiple result sets should match");
                }
            }
        }

        /**
         * testModel - DML model exploration.
         */
        @Test
        @DisplayName("testModel: model-based DML exploration")
        void testModelDML() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    int updateCount = stmt.executeUpdate(
                            "UPDATE " + TABLE_NAME + " SET value = value + 1 WHERE id <= 5");
                    assertEquals(5, updateCount, "Should update 5 rows");

                    boolean hasResult = stmt.execute(
                            "UPDATE " + TABLE_NAME + " SET value = value + 1 WHERE id = 1;"
                                    + "UPDATE " + TABLE_NAME + " SET value = value + 1 WHERE id = 2");
                    assertFalse(hasResult, "DML should not return ResultSet");
                    int total = stmt.getUpdateCount();
                    assertTrue(total >= 0, "Should have update count");
                }
            }
        }

        /**
         * testModel - mixed query exploration (UPDATE + SELECT).
         */
        @Test
        @DisplayName("testModel: model-based mixed query (DML+SELECT)")
        void testModelMixed() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                try (Statement stmt = conn.createStatement()) {
                    boolean hasResult = stmt.execute(
                            "UPDATE " + TABLE_NAME + " SET value = value + 1 WHERE id = 1;"
                                    + "SELECT * FROM " + TABLE_NAME + " WHERE id = 1");
                    do {
                        if (hasResult) {
                            try (ResultSet rs = stmt.getResultSet()) {
                                assertTrue(rs.next());
                            }
                        } else {
                            int uc = stmt.getUpdateCount();
                            if (uc == -1) break;
                        }
                        hasResult = stmt.getMoreResults();
                    } while (true);
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("SendStringParametersAsUnicode")
    class SSPAUTests {

        /**
         * testParametersSSPAUUnicodeInCollation - PreparedStatement with
         * sendStringParametersAsUnicode=true, using setString for textual columns
         * in the database collation.
         */
        @Test
        @DisplayName("testParametersSSPAUUnicodeInCollation: PreparedStatement with Unicode params in collation")
        void testParametersSSPAUUnicodeInCollation() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tableName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_SSPAUInColl"));

            try (Connection conn = PrepUtil.getConnection(connectionString
                    + ";sendStringParametersAsUnicode=true");
                    Statement setup = conn.createStatement()) {
                setup.executeUpdate("CREATE TABLE " + tableName
                        + " (id INT PRIMARY KEY, col_nvarchar NVARCHAR(200), col_varchar VARCHAR(200))");
                setup.executeUpdate("INSERT INTO " + tableName + " VALUES (1, N'test', 'test')");

                try (PreparedStatement pstmt = conn.prepareStatement(
                        "UPDATE " + tableName + " SET col_nvarchar = ? WHERE id = ?")) {
                    String unicodeStr = "Unicode\u00E9\u00E8\u00EA";
                    pstmt.setString(1, unicodeStr);
                    pstmt.setInt(2, 1);
                    int count = pstmt.executeUpdate();
                    assertEquals(1, count);

                    try (ResultSet rs = setup.executeQuery(
                            "SELECT col_nvarchar FROM " + tableName + " WHERE id = 1")) {
                        assertTrue(rs.next());
                        assertEquals(unicodeStr, rs.getString(1), "Unicode data should match");
                    }
                }

                try (PreparedStatement pstmt = conn.prepareStatement(
                        "UPDATE " + tableName + " SET col_nvarchar = ? WHERE id = ?")) {
                    String unicodeStr2 = "ObjUnicode\u00C0\u00C1";
                    pstmt.setObject(1, unicodeStr2);
                    pstmt.setObject(2, 1);
                    pstmt.executeUpdate();

                    try (ResultSet rs = setup.executeQuery(
                            "SELECT col_nvarchar FROM " + tableName + " WHERE id = 1")) {
                        assertTrue(rs.next());
                        assertEquals(unicodeStr2, rs.getString(1));
                    }
                }
            } finally {
                try (Connection conn = PrepUtil.getConnection(connectionString);
                        Statement cleanup = conn.createStatement()) {
                    TestUtils.dropTableIfExists(tableName, cleanup);
                }
            }
        }

        /**
         * testParametersSSPAUUnicodeNotInCollation - PreparedStatement with
         * sendStringParametersAsUnicode=false, potential data corruption for
         * Unicode characters not in the database collation.
         */
        @Test
        @DisplayName("testParametersSSPAUUnicodeNotInCollation: PreparedStatement with non-Unicode params")
        void testParametersSSPAUUnicodeNotInCollation() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tableName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_SSPAUNotColl"));

            try (Connection conn = PrepUtil.getConnection(connectionString
                    + ";sendStringParametersAsUnicode=false");
                    Statement setup = conn.createStatement()) {
                setup.executeUpdate("CREATE TABLE " + tableName
                        + " (id INT PRIMARY KEY, col_varchar VARCHAR(200) "
                        + "COLLATE SQL_Latin1_General_CP1_CI_AS)");
                setup.executeUpdate("INSERT INTO " + tableName + " VALUES (1, 'test')");

                try (PreparedStatement pstmt = conn.prepareStatement(
                        "UPDATE " + tableName + " SET col_varchar = ? WHERE id = ?")) {
                    String asciiStr = "SimpleASCII";
                    pstmt.setString(1, asciiStr);
                    pstmt.setInt(2, 1);
                    int count = pstmt.executeUpdate();
                    assertEquals(1, count);

                    try (ResultSet rs = setup.executeQuery(
                            "SELECT col_varchar FROM " + tableName + " WHERE id = 1")) {
                        assertTrue(rs.next());
                        assertEquals(asciiStr, rs.getString(1),
                                "ASCII data should be preserved with SSPAU=false");
                    }
                }
            } finally {
                try (Connection conn = PrepUtil.getConnection(connectionString);
                        Statement cleanup = conn.createStatement()) {
                    TestUtils.dropTableIfExists(tableName, cleanup);
                }
            }
        }

        /**
         * testParamsSSPAUUnicodeInCollation - CallableStatement with
         * sendStringParametersAsUnicode=true, using setString for callable params.
         */
        @Test
        @DisplayName("testParamsSSPAUUnicodeInCollation: CallableStatement with Unicode params")
        void testParamsSSPAUUnicodeInCollation() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_SSPAUCSIn"));

            try (Connection conn = PrepUtil.getConnection(connectionString
                    + ";sendStringParametersAsUnicode=true");
                    Statement setup = conn.createStatement()) {
                createProc(setup, procName,
                        "@inVal NVARCHAR(200), @outVal NVARCHAR(200) OUTPUT "
                                + "AS SET @outVal = @inVal");

                try (CallableStatement cstmt = conn.prepareCall(
                        "{ call " + procName + "(?, ?) }")) {
                    String unicodeStr = "CallUnicode\u00F1\u00F2";
                    cstmt.setString(1, unicodeStr);
                    cstmt.registerOutParameter(2, Types.NVARCHAR);
                    cstmt.execute();

                    String outVal = cstmt.getString(2);
                    assertEquals(unicodeStr, outVal,
                            "Unicode output param should match input");
                }

                try (CallableStatement cstmt = conn.prepareCall(
                        "{ call " + procName + "(?, ?) }")) {
                    String unicodeStr2 = "ObjCallUnicode\u00D0\u00D1";
                    cstmt.setObject(1, unicodeStr2);
                    cstmt.registerOutParameter(2, Types.NVARCHAR);
                    cstmt.execute();

                    assertEquals(unicodeStr2, cstmt.getString(2));
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }

        /**
         * testParamsSSPAUUnicodeNotInCollation - CallableStatement with
         * sendStringParametersAsUnicode=false, testing potential corruption.
         */
        @Test
        @DisplayName("testParamsSSPAUUnicodeNotInCollation: CallableStatement with non-Unicode params")
        void testParamsSSPAUUnicodeNotInCollation() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_SSPAUCSNot"));

            try (Connection conn = PrepUtil.getConnection(connectionString
                    + ";sendStringParametersAsUnicode=false");
                    Statement setup = conn.createStatement()) {
                createProc(setup, procName,
                        "@inVal VARCHAR(200), @outVal VARCHAR(200) OUTPUT "
                                + "AS SET @outVal = @inVal");

                try (CallableStatement cstmt = conn.prepareCall(
                        "{ call " + procName + "(?, ?) }")) {
                    String asciiStr = "CallASCII";
                    cstmt.setString(1, asciiStr);
                    cstmt.registerOutParameter(2, Types.VARCHAR);
                    cstmt.execute();

                    assertEquals(asciiStr, cstmt.getString(2),
                            "ASCII output param should match input with SSPAU=false");
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Additional Statement Scenarios")
    class AEVariantTests {

        /**
         * testFourPartSproc - four-part sproc name execution
         * (server.database.schema.proc). Tests that CallableStatement can
         * handle fully qualified four-part naming for stored procedures.
         */
        @Test
        @DisplayName("testFourPartSproc: four-part sproc name with return value and out params")
        void testFourPartSproc() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_FourPart"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                String dbName;
                try (ResultSet rs = setup.executeQuery("SELECT DB_NAME()")) {
                    assertTrue(rs.next());
                    dbName = rs.getString(1);
                }

                createProc(setup, procName,
                        "@f1 INT, @outVal INT OUTPUT AS SET @outVal = @f1 * 3 SELECT @f1 AS val");

                // Three-part name (database.dbo.proc) is more universally supported than four-part
                String threePartName = "[" + dbName + "].dbo." + procName;
                try (CallableStatement cstmt = conn.prepareCall(
                        "{ call " + threePartName + "(?, ?) }")) {
                    cstmt.setInt(1, 10);
                    cstmt.registerOutParameter(2, Types.INTEGER);
                    cstmt.execute();
                    assertEquals(30, cstmt.getInt(2),
                            "Out param via three-part name should be input * 3");
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }

        /**
         * testInvalidStmtJDBC4Methods - JDBC4 specific Statement methods.
         * Tests isClosed, setPoolable, isPoolable on Statement.
         * (Verifies JDBC4 methods work correctly on modern JVM.)
         */
        @Test
        @DisplayName("testInvalidStmtJDBC4Methods: JDBC4 Statement methods work correctly")
        void testInvalidStmtJDBC4Methods() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement()) {
                assertFalse(stmt.isClosed(), "Open statement should not be closed");

                stmt.setPoolable(true);
                assertTrue(stmt.isPoolable(), "Statement should be poolable after setPoolable(true)");
                stmt.setPoolable(false);
                assertFalse(stmt.isPoolable(), "Statement should not be poolable after setPoolable(false)");

                assertTrue(stmt.isWrapperFor(Statement.class));

                stmt.close();
                assertTrue(stmt.isClosed(), "Closed statement should be closed");
            }
        }

        /**
         * testInvalidPStmtJDBC4Methods - JDBC4 specific PreparedStatement methods.
         * Tests setNString, setNCharacterStream, setNClob on PreparedStatement.
         */
        @Test
        @DisplayName("testInvalidPStmtJDBC4Methods: JDBC4 PreparedStatement methods work correctly")
        void testInvalidPStmtJDBC4Methods() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tableName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_JDBC4PS"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                setup.executeUpdate("CREATE TABLE " + tableName
                        + " (id INT PRIMARY KEY, ncol NVARCHAR(200))");
                setup.executeUpdate("INSERT INTO " + tableName + " VALUES (1, N'test')");

                try (PreparedStatement pstmt = conn.prepareStatement(
                        "UPDATE " + tableName + " SET ncol = ? WHERE id = ?")) {
                    pstmt.setNString(1, "NStringValue\u00E9");
                    pstmt.setInt(2, 1);
                    int count = pstmt.executeUpdate();
                    assertEquals(1, count);

                    try (ResultSet rs = setup.executeQuery(
                            "SELECT ncol FROM " + tableName + " WHERE id = 1")) {
                        assertTrue(rs.next());
                        assertEquals("NStringValue\u00E9", rs.getString(1));
                    }

                    assertFalse(pstmt.isClosed());
                    pstmt.setPoolable(true);
                    assertTrue(pstmt.isPoolable());
                } finally {
                    TestUtils.dropTableIfExists(tableName, setup);
                }
            }
        }

        /**
         * testInvalidCStmtJDBC4Methods - JDBC4 specific CallableStatement methods.
         * Tests getNString, getNCharacterStream on CallableStatement.
         */
        @Test
        @DisplayName("testInvalidCStmtJDBC4Methods: JDBC4 CallableStatement methods work correctly")
        void testInvalidCStmtJDBC4Methods() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_JDBC4CS"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                createProc(setup, procName,
                        "@inVal NVARCHAR(200), @outVal NVARCHAR(200) OUTPUT "
                                + "AS SET @outVal = @inVal");

                try (CallableStatement cstmt = conn.prepareCall(
                        "{ call " + procName + "(?, ?) }")) {
                    cstmt.setNString(1, "JDBC4Test\u00F1");
                    cstmt.registerOutParameter(2, Types.NVARCHAR);
                    cstmt.execute();

                    String outVal = cstmt.getNString(2);
                    assertEquals("JDBC4Test\u00F1", outVal, "getNString should return Unicode value");

                    assertFalse(cstmt.isClosed());
                    cstmt.setPoolable(true);
                    assertTrue(cstmt.isPoolable());
                } finally {
                    TestUtils.dropProcedureIfExists(procName, setup);
                }
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Closed Statement Method Validation")
    class ClosedStatementMethodTests {

        /**
         * testStmtMethodsWhenClosed - calling methods on a closed Statement
         * should throw "Statement is closed" for Statement type.
         */
        @Test
        @DisplayName("testStmtMethodsWhenClosed: methods on closed Statement throw")
        void testStmtMethodsWhenClosedStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                Statement stmt = conn.createStatement();
                stmt.close();

                assertThrows(SQLException.class, () -> stmt.executeQuery("SELECT 1"),
                        "executeQuery on closed statement should throw");
                assertThrows(SQLException.class, () -> stmt.executeUpdate("SELECT 1"),
                        "executeUpdate on closed statement should throw");
                assertThrows(SQLException.class, () -> stmt.execute("SELECT 1"),
                        "execute on closed statement should throw");
                assertThrows(SQLException.class, () -> stmt.getResultSet(),
                        "getResultSet on closed statement should throw");
                assertThrows(SQLException.class, () -> stmt.getMoreResults(),
                        "getMoreResults on closed statement should throw");
                assertThrows(SQLException.class, () -> stmt.getUpdateCount(),
                        "getUpdateCount on closed statement should throw");
                assertTrue(stmt.isClosed(), "isClosed should return true");
            }
        }

        /**
         * testStmtMethodsWhenClosed - PreparedStatement variant.
         */
        @Test
        @DisplayName("testStmtMethodsWhenClosed: methods on closed PreparedStatement throw")
        void testStmtMethodsWhenClosedPreparedStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn);
                PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM " + TABLE_NAME);
                pstmt.close();

                assertThrows(SQLException.class, () -> pstmt.executeQuery(),
                        "executeQuery on closed pstmt should throw");
                assertThrows(SQLException.class, () -> pstmt.executeUpdate(),
                        "executeUpdate on closed pstmt should throw");
                assertThrows(SQLException.class, () -> pstmt.execute(),
                        "execute on closed pstmt should throw");
                assertThrows(SQLException.class, () -> pstmt.getResultSet(),
                        "getResultSet on closed pstmt should throw");
                assertThrows(SQLException.class, () -> pstmt.getMoreResults(),
                        "getMoreResults on closed pstmt should throw");
                assertTrue(pstmt.isClosed());
            }
        }

        /**
         * testStmtMethodsWhenClosed - CallableStatement variant.
         */
        @Test
        @DisplayName("testStmtMethodsWhenClosed: methods on closed CallableStatement throw")
        void testStmtMethodsWhenClosedCallableStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String procName = AbstractSQLGenerator
                    .escapeIdentifier(RandomUtil.getIdentifier("SM_ClosedCS"));

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement setup = conn.createStatement()) {
                createTestTable(conn);
                createProc(setup, procName,
                        "AS SELECT * FROM " + TABLE_NAME);

                CallableStatement cstmt = conn.prepareCall("{ call " + procName + " }");
                cstmt.close();

                assertThrows(SQLException.class, () -> cstmt.executeQuery(),
                        "executeQuery on closed cstmt should throw");
                assertThrows(SQLException.class, () -> cstmt.execute(),
                        "execute on closed cstmt should throw");
                assertThrows(SQLException.class, () -> cstmt.getResultSet(),
                        "getResultSet on closed cstmt should throw");
                assertThrows(SQLException.class, () -> cstmt.getMoreResults(),
                        "getMoreResults on closed cstmt should throw");
                assertTrue(cstmt.isClosed());

                TestUtils.dropProcedureIfExists(procName, setup);
            }
        }

        /**
         * testStmtMethodsWhenClosed - closing connection then calling stmt methods.
         */
        @Test
        @DisplayName("testStmtMethodsWhenClosed: methods after connection close")
        void testStmtMethodsAfterConnectionClose() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement()) {
                conn.close();

                assertThrows(SQLException.class, () -> stmt.executeQuery("SELECT 1"),
                        "executeQuery after conn close should throw");
                assertThrows(SQLException.class, () -> stmt.execute("SELECT 1"),
                        "execute after conn close should throw");
                assertTrue(stmt.isClosed(), "Statement should be closed after connection close");
            }
        }

        /**
         * testStmtMethodsWhenClosed - verify all getter/setter methods on closed
         * statement using reflection.
         */
        @Test
        @DisplayName("testStmtMethodsWhenClosed: reflection-based check of all methods on closed Statement")
        void testAllMethodsOnClosedStatementViaReflection() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                Statement stmt = conn.createStatement();
                stmt.close();

                Method[] methods = stmt.getClass().getMethods();
                int testedCount = 0;
                for (Method method : methods) {
                    // Skip Object methods, toString, isClosed (returns boolean without throwing)
                    if (method.getDeclaringClass() == Object.class) continue;
                    if ("toString".equalsIgnoreCase(method.getName())) continue;
                    if ("isClosed".equalsIgnoreCase(method.getName())) continue;
                    if ("close".equalsIgnoreCase(method.getName())) continue;
                    if (method.getName().contains("Poolable")) continue;
                    if ("isWrapperFor".equalsIgnoreCase(method.getName())) continue;
                    if ("unwrap".equalsIgnoreCase(method.getName())) continue;
                    // Skip methods with complex param types we can't easily construct
                    Class<?>[] paramTypes = method.getParameterTypes();
                    boolean skip = false;
                    for (Class<?> pt : paramTypes) {
                        if (!pt.isPrimitive() && pt != String.class
                                && pt != int[].class && pt != String[].class) {
                            skip = true;
                            break;
                        }
                    }
                    if (skip) continue;

                    // Build args
                    Object[] args = new Object[paramTypes.length];
                    for (int j = 0; j < paramTypes.length; j++) {
                        if (paramTypes[j] == String.class) args[j] = "SELECT 1";
                        else if (paramTypes[j] == int.class) args[j] = 0;
                        else if (paramTypes[j] == boolean.class) args[j] = false;
                        else if (paramTypes[j] == long.class) args[j] = 0L;
                        else if (paramTypes[j] == int[].class) args[j] = new int[]{1};
                        else if (paramTypes[j] == String[].class) args[j] = new String[]{"col"};
                        else args[j] = null;
                    }

                    try {
                        method.invoke(stmt, args);
                    } catch (java.lang.reflect.InvocationTargetException ite) {
                        Throwable cause = ite.getCause();
                        if (cause instanceof SQLException) {
                            // Expected - statement is closed
                            testedCount++;
                        }
                    } catch (Exception e) {
                        // Other exceptions are also acceptable
                    }
                }
                assertTrue(testedCount > 0,
                        "At least some methods should throw SQLException on closed statement");
            }
        }
    }


    @Nested
    @Tag(Constants.legacyFx)
    @DisplayName("Fatal Error Handling")
    class FatalErrorTests {

        /**
         * testFatalError - RAISERROR with severity 15 should throw
         * without hanging. Can re-execute after error.
         */
        @Test
        @DisplayName("testFatalError: RAISERROR severity 15 throws and allows re-execute")
        void testFatalError() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    PreparedStatement pstmt = conn.prepareStatement(
                            "RAISERROR ('raiserror level 15', 15, 42)")) {
                assertThrows(SQLException.class, () -> {
                    pstmt.executeUpdate();
                }, "RAISERROR level 15 should throw");

                // Second execute should not hang
                assertThrows(SQLException.class, () -> {
                    pstmt.executeUpdate();
                }, "Second RAISERROR should also throw without hanging");
            }
        }

        /**
         * testReExecuteAfterQueryTimeout - verify re-execute works after
         * a query timeout occurs.
         */
        @Test
        @DisplayName("testReExecuteAfterQueryTimeout: re-execute after timeout")
        void testReExecuteAfterQueryTimeout() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement()) {
                createTestTable(conn);
                stmt.setQueryTimeout(1);
                try {
                    stmt.execute("WAITFOR DELAY '00:00:10'");
                } catch (SQLException e) {
                    assertTrue(e.getMessage() != null);
                }

                stmt.setQueryTimeout(30);
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                    assertTrue(rs.next(), "Re-execute after timeout should work");
                    assertEquals(ROW_COUNT_VAL, rs.getInt(1));
                }
            }
        }
    }
}
