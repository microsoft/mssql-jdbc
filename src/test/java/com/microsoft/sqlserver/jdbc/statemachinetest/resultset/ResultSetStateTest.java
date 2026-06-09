/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.resultset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
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
 * MBT for JDBC ResultSet: scrollable cursor navigation and data retrieval
 * validated against DataCache.
 */
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxStateMachine)
public class ResultSetStateTest extends AbstractTest {

    private static final String TABLE_NAME = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SM_ResultSet_Test"));

    private static final StateKey RS = () -> "rs";
    private static final StateKey CLOSED = () -> "closed";
    private static final StateKey ON_VALID_ROW = () -> "onValidRow";
    private static final StateKey CURRENT_ROW = () -> "currentRow";
    private static final StateKey ON_INSERT_ROW = () -> "onInsertRow";
    private static final StateKey IS_UPDATABLE = () -> "isUpdatable";
    private static final StateKey IS_SCROLLABLE = () -> "isScrollable";
    private static final StateKey ROW_DELETED = () -> "rowDeleted";

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

    @Test
    @DisplayName("Randomized ResultSet State Validation")
    void testWithDataValidation() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        StateMachineTest sm = new StateMachineTest("DataValidation");
        DataCache cache = sm.getDataCache();

        cache.updateValue(0, CLOSED.key(), false);
        cache.updateValue(0, ON_VALID_ROW.key(), false);
        cache.updateValue(0, CURRENT_ROW.key(), 0);

        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, name VARCHAR(50), value INT)");

            for (int i = 1; i <= 10; i++) {
                String name = "Row" + i;
                int value = i * 10;
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (" + i + ", '" + name + "', " + value + ")");

                Map<String, Object> row = new HashMap<>();
                row.put("id", i);
                row.put("name", name);
                row.put("value", value);
                cache.addRow(row);
            }
        }

        try (Connection conn = PrepUtil.getConnection(connectionString);
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id")) {

            cache.updateValue(0, RS.key(), rs);

            sm.addAction(new NextAction(10)); // frequent navigation
            sm.addAction(new PreviousAction(8)); // backward scrolling
            sm.addAction(new FirstAction(5)); // jump to start
            sm.addAction(new LastAction(5)); // jump to end
            sm.addAction(new AbsoluteAction(6)); // random position
            sm.addAction(new GetStringAction(10)); // read string column
            sm.addAction(new GetIntAction(10)); // read int column
            sm.addAction(new CloseAction(1)); // close is rare

            Result result = Engine.run(sm).withMaxActions(100).execute();

            System.out.println("ResultSet test: " + result.actionCount + " actions");
            assertTrue(result.isSuccess(), "State machine test should complete successfully");
        }
    }

    /** Verifies current row data against expected DataCache values. */
    private static void verifyCurrentRow(Action action, ResultSet rs) throws SQLException {
        DataCache cache = action.getDataCache();
        if (cache == null || cache.getRowCount() <= 1) {
            return;
        }

        int currentRow = action.getStateInt(CURRENT_ROW);
        if (currentRow < 1 || currentRow >= cache.getRowCount()) {
            return;
        }

        Map<String, Object> expectedRow = cache.getRow(currentRow);
        if (expectedRow == null) {
            return;
        }

        for (Map.Entry<String, Object> entry : expectedRow.entrySet()) {
            String columnName = entry.getKey();
            Object expected = entry.getValue();
            Object actual = rs.getObject(columnName);

            action.assertExpected(actual, expected,
                    String.format("Row %d column '%s' mismatch", currentRow, columnName));
        }
    }

    /** Move cursor to next row. */
    private static class NextAction extends Action {

        NextAction(int weight) {
            super("next", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            boolean valid = rs.next();
            setState(ON_VALID_ROW, valid);

            if (valid) {
                setState(CURRENT_ROW, rs.getRow());
                setState(ROW_DELETED, isState(IS_UPDATABLE) && rs.rowDeleted());
            } else {
                setState(CURRENT_ROW, 0);
                setState(ROW_DELETED, false);
            }

            System.out.println("next() -> " + valid + (valid ? " row=" + getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            if (isState(ON_VALID_ROW) && !isState(ROW_DELETED)) {
                ResultSet rs = (ResultSet) getState(RS);
                verifyCurrentRow(this, rs);
            }
        }
    }

    /** Move cursor to previous row. */
    private static class PreviousAction extends Action {

        PreviousAction(int weight) {
            super("previous", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            boolean valid = rs.previous();
            setState(ON_VALID_ROW, valid);

            if (valid) {
                setState(CURRENT_ROW, rs.getRow());
                setState(ROW_DELETED, isState(IS_UPDATABLE) && rs.rowDeleted());
            } else {
                setState(CURRENT_ROW, 0);
                setState(ROW_DELETED, false);
            }

            System.out.println("previous() -> " + valid + (valid ? " row=" + getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            if (isState(ON_VALID_ROW) && !isState(ROW_DELETED)) {
                ResultSet rs = (ResultSet) getState(RS);
                verifyCurrentRow(this, rs);
            }
        }
    }

    /** Move cursor to first row. */
    private static class FirstAction extends Action {

        FirstAction(int weight) {
            super("first", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            boolean valid = rs.first();
            setState(ON_VALID_ROW, valid);

            if (valid) {
                setState(CURRENT_ROW, rs.getRow());
                setState(ROW_DELETED, isState(IS_UPDATABLE) && rs.rowDeleted());
            } else {
                setState(CURRENT_ROW, 0);
                setState(ROW_DELETED, false);
            }

            System.out.println("first() -> " + valid + (valid ? " row=" + getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            if (isState(ON_VALID_ROW) && !isState(ROW_DELETED)) {
                ResultSet rs = (ResultSet) getState(RS);
                verifyCurrentRow(this, rs);
            }
        }
    }

    /** Move cursor to last row. */
    private static class LastAction extends Action {

        LastAction(int weight) {
            super("last", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            boolean valid = rs.last();
            setState(ON_VALID_ROW, valid);

            if (valid) {
                setState(CURRENT_ROW, rs.getRow());
                setState(ROW_DELETED, isState(IS_UPDATABLE) && rs.rowDeleted());
            } else {
                setState(CURRENT_ROW, 0);
                setState(ROW_DELETED, false);
            }

            System.out.println("last() -> " + valid + (valid ? " row=" + getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            if (isState(ON_VALID_ROW) && !isState(ROW_DELETED)) {
                ResultSet rs = (ResultSet) getState(RS);
                verifyCurrentRow(this, rs);
            }
        }
    }

    /** Move cursor to random absolute row position. */
    private static class AbsoluteAction extends Action {

        AbsoluteAction(int weight) {
            super("absolute", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);

            int dataRows = dataCache.getRowCount() - 1;
            int maxRow = dataRows > 0 ? dataRows : 10;
            int target = getRandom().nextInt(maxRow + 2) - 1;

            boolean valid = rs.absolute(target);
            setState(ON_VALID_ROW, valid);

            if (valid) {
                setState(CURRENT_ROW, rs.getRow());
                setState(ROW_DELETED, isState(IS_UPDATABLE) && rs.rowDeleted());
            } else {
                setState(CURRENT_ROW, 0);
                setState(ROW_DELETED, false);
            }

            System.out.println(
                    "absolute(" + target + ") -> " + valid + (valid ? " row=" + getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            if (isState(ON_VALID_ROW) && !isState(ROW_DELETED)) {
                ResultSet rs = (ResultSet) getState(RS);
                verifyCurrentRow(this, rs);
            }
        }
    }

    /** Get getString('name') and validate against DataCache. */
    private static class GetStringAction extends Action {
        private String lastValue;

        GetStringAction(int weight) {
            super("getString", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && isState(ON_VALID_ROW) && !isState(ROW_DELETED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            lastValue = rs.getString("name");
            System.out.println("getString('name') -> " + lastValue);
        }

        @Override
        public void validate() throws SQLException {
            if (hasDataCache()) {
                int currentRow = getStateInt(CURRENT_ROW);
                if (currentRow >= 1 && currentRow < dataCache.getRowCount()) {
                    Object expectedName = dataCache.getValue(currentRow, "name");
                    assertExpected(lastValue, expectedName,
                            String.format("getString('name') mismatch at row %d", currentRow));
                }
            }
        }
    }

    /** Get getInt('value') and validate against DataCache. */
    private static class GetIntAction extends Action {
        private int lastValue;
        private boolean hasLastValue;

        GetIntAction(int weight) {
            super("getInt", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && isState(ON_VALID_ROW) && !isState(ROW_DELETED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            lastValue = rs.getInt("value");
            hasLastValue = true;
            System.out.println("getInt('value') -> " + lastValue);
        }

        @Override
        public void validate() throws SQLException {
            if (!hasLastValue)
                return;

            if (hasDataCache()) {
                int currentRow = getStateInt(CURRENT_ROW);
                if (currentRow >= 1 && currentRow < dataCache.getRowCount()) {
                    Object expectedValue = dataCache.getValue(currentRow, "value");
                    if (expectedValue != null) {
                        assertExpected(lastValue, ((Number) expectedValue).intValue(),
                                String.format("getInt('value') mismatch at row %d", currentRow));
                    }
                }
            }
        }
    }

    /** Close the ResultSet. */
    private static class CloseAction extends Action {

        CloseAction(int weight) {
            super("close", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            rs.close();
            setState(CLOSED, true);
            System.out.println("close()");
        }
    }

    private static void createTestTable(Connection conn, String tableName, int rows) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(tableName, stmt);
            stmt.execute("CREATE TABLE " + tableName
                    + " (id INT PRIMARY KEY, value INT, name VARCHAR(200))");
            for (int i = 1; i <= rows; i++) {
                stmt.execute("INSERT INTO " + tableName + " VALUES (" + i + ", " + (i * 10) + ", 'Row" + i + "')");
            }
        }
    }

    private static void createTestTable(Connection conn, String tableName) throws SQLException {
        createTestTable(conn, tableName, 10);
    }

    /** Move cursor by relative offset. */
    private static class RelativeAction extends Action {

        RelativeAction(int weight) {
            super("relative", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && isState(ON_VALID_ROW);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            int offset = getRandom().nextInt(5) - 2;
            boolean valid = rs.relative(offset);
            setState(ON_VALID_ROW, valid);

            if (valid) {
                setState(CURRENT_ROW, rs.getRow());
                setState(ROW_DELETED, isState(IS_UPDATABLE) && rs.rowDeleted());
            } else {
                setState(CURRENT_ROW, 0);
                setState(ROW_DELETED, false);
            }

            System.out.println("relative(" + offset + ") -> " + valid
                    + (valid ? " row=" + getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            if (isState(ON_VALID_ROW) && !isState(ROW_DELETED)) {
                ResultSet rs = (ResultSet) getState(RS);
                verifyCurrentRow(this, rs);
            }
        }
    }

    /** Move cursor before first row. */
    private static class BeforeFirstAction extends Action {

        BeforeFirstAction(int weight) {
            super("beforeFirst", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            rs.beforeFirst();
            setState(ON_VALID_ROW, false);
            setState(ON_INSERT_ROW, false);
            setState(CURRENT_ROW, 0);
            setState(ROW_DELETED, false);
            System.out.println("beforeFirst()");
        }
    }

    /** Move cursor after last row. */
    private static class AfterLastAction extends Action {

        AfterLastAction(int weight) {
            super("afterLast", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            rs.afterLast();
            setState(ON_VALID_ROW, false);
            setState(ON_INSERT_ROW, false);
            setState(CURRENT_ROW, 0);
            setState(ROW_DELETED, false);
            System.out.println("afterLast()");
        }
    }

    /** Move to insert row. */
    private static class MoveToInsertRowAction extends Action {

        MoveToInsertRowAction(int weight) {
            super("moveToInsertRow", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && isState(IS_UPDATABLE);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            rs.moveToInsertRow();
            setState(ON_INSERT_ROW, true);
            setState(ON_VALID_ROW, false);
            System.out.println("moveToInsertRow()");
        }
    }

    /** Move back to current row from insert row. */
    private static class MoveToCurrentRowAction extends Action {

        MoveToCurrentRowAction(int weight) {
            super("moveToCurrentRow", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && isState(IS_UPDATABLE);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            rs.moveToCurrentRow();
            setState(ON_INSERT_ROW, false);
            System.out.println("moveToCurrentRow()");
        }
    }

    /** Update column values on current row (without calling updateRow). */
    private static class UpdateValueAction extends Action {

        UpdateValueAction(int weight) {
            super("updateValue", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && isState(IS_UPDATABLE) && isState(ON_VALID_ROW)
                    && !isState(ROW_DELETED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            int newValue = getRandom().nextInt(10000);
            rs.updateInt("value", newValue);
            rs.updateString("name", "Updated_" + newValue);
            System.out.println("updateValue(value=" + newValue + ")");
        }
    }

    /** Call updateRow to persist pending changes. */
    private static class UpdateRowAction extends Action {

        UpdateRowAction(int weight) {
            super("updateRow", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && isState(IS_UPDATABLE) && isState(ON_VALID_ROW)
                    && !isState(ON_INSERT_ROW) && !isState(ROW_DELETED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            int newValue = getRandom().nextInt(10000);
            rs.updateInt("value", newValue);
            rs.updateString("name", "UpdRow_" + newValue);
            rs.updateRow();
            System.out.println("updateRow(value=" + newValue + ")");
        }
    }

    /** Delete the current row. */
    private static class DeleteRowAction extends Action {

        DeleteRowAction(int weight) {
            super("deleteRow", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && isState(IS_UPDATABLE) && isState(ON_VALID_ROW)
                    && !isState(ON_INSERT_ROW) && !isState(ROW_DELETED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            rs.deleteRow();
            setState(ROW_DELETED, true);
            System.out.println("deleteRow()");
        }
    }

    /** Cancel pending row updates. */
    private static class CancelRowUpdatesAction extends Action {

        CancelRowUpdatesAction(int weight) {
            super("cancelRowUpdates", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && isState(ON_VALID_ROW) && !isState(ON_INSERT_ROW)
                    && !isState(ROW_DELETED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            rs.cancelRowUpdates();
            System.out.println("cancelRowUpdates()");
        }
    }

    /** Refresh the current row from the database. */
    private static class RefreshRowAction extends Action {

        RefreshRowAction(int weight) {
            super("refreshRow", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && isState(IS_UPDATABLE) && isState(ON_VALID_ROW)
                    && !isState(ON_INSERT_ROW) && isState(IS_SCROLLABLE) && !isState(ROW_DELETED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            rs.refreshRow();
            System.out.println("refreshRow()");
        }
    }

    /** Get the current row number. */
    private static class GetRowAction extends Action {

        GetRowAction(int weight) {
            super("getRow", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            int row = rs.getRow();
            System.out.println("getRow() -> " + row);
        }
    }

    /** Check if cursor is on the first row. */
    private static class IsFirstAction extends Action {

        IsFirstAction(int weight) {
            super("isFirst", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            boolean result = rs.isFirst();
            System.out.println("isFirst() -> " + result);
        }
    }

    /** Check if cursor is on the last row. */
    private static class IsLastAction extends Action {

        IsLastAction(int weight) {
            super("isLast", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            boolean result = rs.isLast();
            System.out.println("isLast() -> " + result);
        }
    }

    /** Check if cursor is before the first row. */
    private static class IsBeforeFirstAction extends Action {

        IsBeforeFirstAction(int weight) {
            super("isBeforeFirst", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            boolean result = rs.isBeforeFirst();
            System.out.println("isBeforeFirst() -> " + result);
        }
    }

    /** Check if cursor is after the last row. */
    private static class IsAfterLastAction extends Action {

        IsAfterLastAction(int weight) {
            super("isAfterLast", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            boolean result = rs.isAfterLast();
            System.out.println("isAfterLast() -> " + result);
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testModelRun")
    class TestModelRun {

        /** Helper: create a StateMachineTest with common state initialized via DataCache. */
        private StateMachineTest createSM(String name, ResultSet rs,
                boolean updatable, boolean scrollable) {
            StateMachineTest sm = new StateMachineTest(name);
            DataCache cache = sm.getDataCache();
            cache.updateValue(0, RS.key(), rs);
            cache.updateValue(0, CLOSED.key(), false);
            cache.updateValue(0, ON_VALID_ROW.key(), false);
            cache.updateValue(0, CURRENT_ROW.key(), 0);
            cache.updateValue(0, ON_INSERT_ROW.key(), false);
            cache.updateValue(0, IS_UPDATABLE.key(), updatable);
            cache.updateValue(0, IS_SCROLLABLE.key(), scrollable);
            cache.updateValue(0, ROW_DELETED.key(), false);
            return sm;
        }

        @Test
        @DisplayName("FX Model: Real Database - Scrollable Sensitive Cursor")
        void testRealDatabaseScrollableCursor() throws SQLException {
            Assumptions.assumeTrue(connectionString != null, "No database connection configured");
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {

                StateMachineTest sm = createSM("RealScrollableCursor", rs, true, true);

                sm.addAction(new NextAction(10));
                sm.addAction(new PreviousAction(8));
                sm.addAction(new FirstAction(5));
                sm.addAction(new LastAction(5));
                sm.addAction(new AbsoluteAction(6));
                sm.addAction(new GetStringAction(10));

                Result result = Engine.run(sm).withMaxActions(50).execute();
                assertTrue(result.isSuccess(), "Model run failed: " + result);
            }
        }

        @Test
        @DisplayName("Scrollable Sensitive Cursor — randomized model")
        void testScrollableSensitiveModel() throws SQLException {
            Assumptions.assumeTrue(connectionString != null, "No database connection configured");
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {

                StateMachineTest sm = createSM("ScrollSensitiveCursor", rs, true, true);

                sm.addAction(new NextAction(10));
                sm.addAction(new PreviousAction(8));
                sm.addAction(new FirstAction(5));
                sm.addAction(new LastAction(5));
                sm.addAction(new AbsoluteAction(6));
                sm.addAction(new RelativeAction(4));
                sm.addAction(new BeforeFirstAction(3));
                sm.addAction(new AfterLastAction(3));
                sm.addAction(new GetStringAction(10));
                sm.addAction(new MoveToInsertRowAction(3));
                sm.addAction(new MoveToCurrentRowAction(3));
                sm.addAction(new UpdateValueAction(5));
                sm.addAction(new GetRowAction(4));
                sm.addAction(new IsFirstAction(3));
                sm.addAction(new IsLastAction(3));
                sm.addAction(new IsBeforeFirstAction(3));
                sm.addAction(new IsAfterLastAction(3));
                sm.addAction(new CancelRowUpdatesAction(3));
                sm.addAction(new RefreshRowAction(2));

                Result result = Engine.run(sm).withMaxActions(100).execute();
                assertTrue(result.isSuccess(), "Model run failed: " + result);
            }
        }

        @Test
        @DisplayName("Forward-Only Read-Only Cursor — randomized model")
        void testForwardOnlyModel() throws SQLException {
            Assumptions.assumeTrue(connectionString != null, "No database connection configured");
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {

                StateMachineTest sm = createSM("ForwardOnlyCursor", rs, false, false);

                sm.addAction(new NextAction(10));
                sm.addAction(new GetStringAction(10));
                sm.addAction(new GetRowAction(4));

                Result result = Engine.run(sm).withMaxActions(50).execute();
                assertTrue(result.isSuccess(), "Model run failed: " + result);
            }
        }

        @Test
        @DisplayName("Keyset-Driven Updatable Cursor — randomized model")
        void testKeysetDrivenModel() throws SQLException {
            Assumptions.assumeTrue(connectionString != null, "No database connection configured");
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {

                StateMachineTest sm = createSM("KeysetDrivenCursor", rs, true, true);

                sm.addAction(new NextAction(10));
                sm.addAction(new PreviousAction(8));
                sm.addAction(new FirstAction(5));
                sm.addAction(new LastAction(5));
                sm.addAction(new AbsoluteAction(6));
                sm.addAction(new GetStringAction(10));
                sm.addAction(new UpdateRowAction(3));
                sm.addAction(new DeleteRowAction(2));
                sm.addAction(new CancelRowUpdatesAction(3));

                Result result = Engine.run(sm).withMaxActions(50).execute();
                assertTrue(result.isSuccess(), "Model run failed: " + result);
            }
        }

        @Test
        @DisplayName("PreparedStatement — randomized model")
        void testPreparedStatementModel() throws SQLException {
            Assumptions.assumeTrue(connectionString != null, "No database connection configured");
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM " + TABLE_NAME,
                            ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = pstmt.executeQuery()) {

                StateMachineTest sm = createSM("PreparedStmtModel", rs, true, true);

                sm.addAction(new NextAction(10));
                sm.addAction(new PreviousAction(8));
                sm.addAction(new FirstAction(5));
                sm.addAction(new LastAction(5));
                sm.addAction(new GetStringAction(10));
                sm.addAction(new UpdateValueAction(5));
                sm.addAction(new GetRowAction(4));

                Result result = Engine.run(sm).withMaxActions(50).execute();
                assertTrue(result.isSuccess(), "Model run failed: " + result);
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testOthers")
    class TestOthers {

        @Test
        @DisplayName("getType, getConcurrency, getStatement return valid values")
        void testOthersBasic() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {

                assertEquals(ResultSet.TYPE_SCROLL_SENSITIVE, rs.getType());
                assertEquals(ResultSet.CONCUR_UPDATABLE, rs.getConcurrency());
                assertNotNull(rs.getStatement());
                assertSame(stmt, rs.getStatement());
            }
        }

        @Test
        @DisplayName("getCursorName throws not-supported exception")
        void testGetCursorNameThrows() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {

                assertThrows(SQLException.class, () -> rs.getCursorName());
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testScan")
    class TestScan {

        @Test
        @DisplayName("Next-getXXX[int] ascending order")
        void testScanNextOrdinalAsc() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id ASC")) {

                int prevId = 0;
                while (rs.next()) {
                    int id = rs.getInt(1);
                    int value = rs.getInt(2);
                    String name = rs.getString(3);
                    assertTrue(id > prevId, "IDs should be ascending");
                    assertEquals(id * 10, value);
                    assertEquals("Row" + id, name);
                    prevId = id;
                }
                assertEquals(10, prevId);
            }
        }

        @Test
        @DisplayName("Next-getXXX[int] descending order")
        void testScanNextOrdinalDesc() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id DESC")) {

                int prevId = Integer.MAX_VALUE;
                while (rs.next()) {
                    int id = rs.getInt(1);
                    assertTrue(id < prevId, "IDs should be descending");
                    prevId = id;
                }
                assertEquals(1, prevId);
            }
        }

        @Test
        @DisplayName("Next-getXXX[str] ascending order")
        void testScanNextNameAsc() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id ASC")) {

                int count = 0;
                while (rs.next()) {
                    count++;
                    int id = rs.getInt("id");
                    int value = rs.getInt("value");
                    String name = rs.getString("name");
                    assertEquals(count, id);
                    assertEquals(count * 10, value);
                    assertEquals("Row" + count, name);
                }
                assertEquals(10, count);
            }
        }

        @Test
        @DisplayName("Next-getXXX[str] descending order")
        void testScanNextNameDesc() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id DESC")) {

                int count = 0;
                while (rs.next()) {
                    count++;
                    assertNotNull(rs.getString("name"));
                }
                assertEquals(10, count);
            }
        }

        @Test
        @DisplayName("Previous-getXXX[int] ascending order")
        void testScanPreviousOrdinalAsc() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id ASC")) {

                rs.afterLast();
                int prevId = Integer.MAX_VALUE;
                while (rs.previous()) {
                    int id = rs.getInt(1);
                    assertTrue(id < prevId, "IDs should decrease in previous() traversal");
                    prevId = id;
                }
                assertEquals(1, prevId);
            }
        }

        @Test
        @DisplayName("Previous-getXXX[str] descending order")
        void testScanPreviousNameDesc() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id ASC")) {

                rs.afterLast();
                int count = 0;
                while (rs.previous()) {
                    count++;
                    String name = rs.getString("name");
                    assertNotNull(name);
                }
                assertEquals(10, count);
            }
        }

        @Test
        @DisplayName("Next-getXXX[model] — random type coercion via getString/getObject")
        void testScanNextRandomCoercion() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id")) {

                int count = 0;
                while (rs.next()) {
                    count++;
                    assertNotNull(rs.getString(1));
                    assertNotNull(rs.getObject(1));
                    assertEquals(count * 10, rs.getInt(2));
                    assertNotNull(rs.getObject(2));
                    assertNotNull(rs.getString(3));
                    assertNotNull(rs.getObject(3));
                }
                assertEquals(10, count);
            }
        }

        @Test
        @DisplayName("Previous-getXXX[model] — random type coercion via getString/getObject")
        void testScanPreviousRandomCoercion() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id ASC")) {

                rs.afterLast();
                int count = 0;
                while (rs.previous()) {
                    count++;
                    assertNotNull(rs.getString(1));
                    assertNotNull(rs.getObject(1));
                    assertNotNull(rs.getString(2));
                    assertNotNull(rs.getObject(3));
                }
                assertEquals(10, count);
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testUpdate")
    class TestUpdate {

        private final String tbl = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("SM_RS_Upd"));

        @BeforeEach
        void setup() throws Exception {
            try (Connection conn = getConnection()) {
                createTestTable(conn, tbl, 5);
            }
        }

        @AfterEach
        void cleanup() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tbl, stmt);
            }
        }

        @Test
        @DisplayName("Next-updateXXX[int] — update by ordinal")
        void testUpdateByOrdinal() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());
                rs.updateInt(2, 9999);
                rs.updateString(3, "UpdatedOrd");
                rs.updateRow();

                rs.refreshRow();
                assertEquals(9999, rs.getInt(2));
                assertEquals("UpdatedOrd", rs.getString(3));
            }
        }

        @Test
        @DisplayName("Next-updateXXX[str] — update by name")
        void testUpdateByName() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());
                rs.updateInt("value", 8888);
                rs.updateString("name", "UpdatedName");
                rs.updateRow();

                rs.refreshRow();
                assertEquals(8888, rs.getInt("value"));
                assertEquals("UpdatedName", rs.getString("name"));
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testRowers")
    class TestRowers {

        private final String tbl = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("SM_RS_Rower"));

        @BeforeEach
        void setup() throws Exception {
            try (Connection conn = getConnection()) {
                createTestTable(conn, tbl, 5);
            }
        }

        @AfterEach
        void cleanup() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tbl, stmt);
            }
        }

        @Test
        @DisplayName("insertRow — move to insert row and insert")
        void testInsertRow() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());
                rs.moveToInsertRow();
                rs.updateInt("id", 100);
                rs.updateInt("value", 1000);
                rs.updateString("name", "InsertedRow");
                rs.insertRow();
                rs.moveToCurrentRow();
            }

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " WHERE id = 100")) {
                assertTrue(rs.next());
                assertEquals(1000, rs.getInt("value"));
                assertEquals("InsertedRow", rs.getString("name"));
            }
        }

        @Test
        @DisplayName("deleteRow — delete current row")
        void testDeleteRow() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());
                int deletedId = rs.getInt("id");
                rs.deleteRow();

                try (Statement stmt2 = conn.createStatement();
                        ResultSet rs2 = stmt2.executeQuery(
                                "SELECT COUNT(*) FROM " + tbl + " WHERE id = " + deletedId)) {
                    rs2.next();
                    assertEquals(0, rs2.getInt(1), "Deleted row should not exist");
                }
            }
        }

        @Test
        @DisplayName("updateRow — update values then updateRow")
        void testUpdateRow() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());
                rs.updateInt("value", 7777);
                rs.updateString("name", "RowerUpdate");
                rs.updateRow();

                rs.refreshRow();
                assertEquals(7777, rs.getInt("value"));
                assertEquals("RowerUpdate", rs.getString("name"));
            }
        }

        @Test
        @DisplayName("updateRow with nothing to update throws exception")
        void testUpdateRowNothingToUpdate() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());
                assertThrows(SQLException.class, () -> rs.updateRow());
            }
        }

        @Test
        @DisplayName("updateRow on insert row throws exception")
        void testUpdateRowOnInsertRow() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());
                rs.moveToInsertRow();
                assertThrows(SQLException.class, () -> rs.updateRow());
            }
        }

        @Test
        @DisplayName("getXXX on updated-but-uncommitted values before updateRow")
        void testGetAfterUpdateBeforeCommit() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());
                int originalValue = rs.getInt("value");
                rs.updateInt("value", 6666);

                int retrieved = rs.getInt("value");
                assertTrue(retrieved == 6666 || retrieved == originalValue,
                        "Value should be pending update or original");

                rs.updateRow();
                rs.refreshRow();
                assertEquals(6666, rs.getInt("value"));
            }
        }

        @Test
        @DisplayName("cancelRowUpdates — cancel pending updates")
        void testCancelRowUpdates() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());
                String originalName = rs.getString("name");
                int originalValue = rs.getInt("value");

                rs.updateInt("value", 5555);
                rs.updateString("name", "ShouldBeCancelled");
                rs.cancelRowUpdates();

                rs.refreshRow();
                assertEquals(originalName, rs.getString("name"));
                assertEquals(originalValue, rs.getInt("value"));
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testMovers")
    class TestMovers {

        @Test
        @DisplayName("Absolute — move to specific row and update")
        void testAbsoluteUpdateXXX() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tbl = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SM_RS_MovAbs"));
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn, tbl, 5);
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                    assertTrue(rs.absolute(3));
                    assertEquals(3, rs.getInt("id"));
                    rs.updateInt("value", 3333);
                    rs.updateRow();
                    rs.refreshRow();
                    assertEquals(3333, rs.getInt("value"));
                } finally {
                    try (Statement s = conn.createStatement()) {
                        TestUtils.dropTableIfExists(tbl, s);
                    }
                }
            }
        }

        @Test
        @DisplayName("First — move to first row and update")
        void testFirstUpdateXXX() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tbl = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SM_RS_MovFst"));
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn, tbl, 5);
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                    assertTrue(rs.first());
                    assertEquals(1, rs.getInt("id"));
                    rs.updateInt("value", 1111);
                    rs.updateRow();
                    rs.refreshRow();
                    assertEquals(1111, rs.getInt("value"));
                } finally {
                    try (Statement s = conn.createStatement()) {
                        TestUtils.dropTableIfExists(tbl, s);
                    }
                }
            }
        }

        @Test
        @DisplayName("Last — move to last row and update")
        void testLastUpdateXXX() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tbl = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SM_RS_MovLst"));
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn, tbl, 5);
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                    assertTrue(rs.last());
                    assertEquals(5, rs.getInt("id"));
                    rs.updateInt("value", 5555);
                    rs.updateRow();
                    rs.refreshRow();
                    assertEquals(5555, rs.getInt("value"));
                } finally {
                    try (Statement s = conn.createStatement()) {
                        TestUtils.dropTableIfExists(tbl, s);
                    }
                }
            }
        }

        @Test
        @DisplayName("Relative[+] — move forward relative and update")
        void testRelativePosUpdateXXX() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tbl = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SM_RS_MovRelP"));
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn, tbl, 5);
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                    assertTrue(rs.next());
                    assertTrue(rs.relative(2));
                    assertEquals(3, rs.getInt("id"));
                    rs.updateInt("value", 3030);
                    rs.updateRow();
                } finally {
                    try (Statement s = conn.createStatement()) {
                        TestUtils.dropTableIfExists(tbl, s);
                    }
                }
            }
        }

        @Test
        @DisplayName("Relative[-] — move backward relative and update")
        void testRelativeNegUpdateXXX() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tbl = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SM_RS_MovRelN"));
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn, tbl, 5);
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                    rs.last();
                    assertTrue(rs.relative(-2));
                    assertEquals(3, rs.getInt("id"));
                    rs.updateInt("value", 3031);
                    rs.updateRow();
                } finally {
                    try (Statement s = conn.createStatement()) {
                        TestUtils.dropTableIfExists(tbl, s);
                    }
                }
            }
        }

        @Test
        @DisplayName("BeforeFirst — navigate to beforeFirst, model to valid row, update")
        void testBeforeFirstModelUpdateXXX() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tbl = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SM_RS_MovBF"));
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn, tbl, 5);
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                    rs.next();
                    rs.beforeFirst();
                    assertTrue(rs.isBeforeFirst());
                    assertTrue(rs.next());
                    rs.updateInt("value", 1010);
                    rs.updateRow();
                } finally {
                    try (Statement s = conn.createStatement()) {
                        TestUtils.dropTableIfExists(tbl, s);
                    }
                }
            }
        }

        @Test
        @DisplayName("AfterLast — navigate to afterLast, model to valid row, update")
        void testAfterLastModelUpdateXXX() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tbl = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SM_RS_MovAL"));
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn, tbl, 5);
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                    rs.next();
                    rs.afterLast();
                    assertTrue(rs.isAfterLast());
                    assertTrue(rs.previous());
                    rs.updateInt("value", 5050);
                    rs.updateRow();
                } finally {
                    try (Statement s = conn.createStatement()) {
                        TestUtils.dropTableIfExists(tbl, s);
                    }
                }
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testGetters")
    class TestGetters {

        @Test
        @DisplayName("getXXX — valid ordinal and name, invalid index/name errors")
        void testGettersValidAndInvalid() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id")) {

                assertTrue(rs.next());

                assertEquals(1, rs.getInt(1));
                assertEquals(10, rs.getInt(2));
                assertEquals("Row1", rs.getString(3));

                assertEquals(1, rs.getInt("id"));
                assertEquals(10, rs.getInt("value"));
                assertEquals("Row1", rs.getString("name"));
                assertEquals(1, rs.getInt("ID"));
                assertEquals("Row1", rs.getString("NAME"));

                assertThrows(SQLException.class, () -> rs.getInt(0));
                assertThrows(SQLException.class, () -> rs.getInt(-1));
                assertThrows(SQLException.class, () -> rs.getInt(4));

                assertThrows(SQLException.class, () -> rs.getString("nonexistent_col"));
                assertThrows(SQLException.class, () -> rs.getString(""));
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testUpdaterValid")
    class TestUpdaterValid {

        private final String tbl = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("SM_RS_UpdVal"));

        @BeforeEach
        void setup() throws Exception {
            try (Connection conn = getConnection()) {
                createTestTable(conn, tbl, 3);
            }
        }

        @AfterEach
        void cleanup() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tbl, stmt);
            }
        }

        @Test
        @DisplayName("updateXXX with valid coercions by ordinal and name")
        void testUpdaterValidCoercions() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());

                rs.updateInt(2, 4444);
                rs.updateString(3, "ValidOrd");
                rs.updateRow();
                rs.refreshRow();
                assertEquals(4444, rs.getInt(2));
                assertEquals("ValidOrd", rs.getString(3));

                assertTrue(rs.next());
                rs.updateInt("value", 5555);
                rs.updateString("name", "ValidName");
                rs.updateRow();
                rs.refreshRow();
                assertEquals(5555, rs.getInt("value"));
                assertEquals("ValidName", rs.getString("name"));
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testUpdateNullValid")
    class TestUpdateNullValid {

        private final String tbl = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("SM_RS_UpdNull"));

        @BeforeEach
        void setup() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tbl, stmt);
                stmt.execute("CREATE TABLE " + tbl
                        + " (id INT PRIMARY KEY, value INT, name VARCHAR(200))");
                stmt.execute("INSERT INTO " + tbl + " VALUES (1, 100, 'NotNull')");
                stmt.execute("INSERT INTO " + tbl + " VALUES (2, 200, 'AlsoNotNull')");
            }
        }

        @AfterEach
        void cleanup() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tbl, stmt);
            }
        }

        @Test
        @DisplayName("updateNull by ordinal — sets column to NULL")
        void testUpdateNullByOrdinal() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());
                rs.updateNull(2);
                rs.updateRow();
                rs.refreshRow();
                rs.getInt(2);
                assertTrue(rs.wasNull());
            }
        }

        @Test
        @DisplayName("updateNull by name — sets column to NULL")
        void testUpdateNullByName() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());
                assertTrue(rs.next());
                rs.updateNull("name");
                rs.updateRow();
                rs.refreshRow();
                assertNull(rs.getString("name"));
                assertTrue(rs.wasNull());
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testFindColumn")
    class TestFindColumn {

        @Test
        @DisplayName("findColumn — valid and invalid column names")
        void testFindColumnValidAndInvalid() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {

                assertEquals(1, rs.findColumn("id"));
                assertEquals(2, rs.findColumn("value"));
                assertEquals(3, rs.findColumn("name"));
                assertEquals(1, rs.findColumn("ID"));
                assertEquals(3, rs.findColumn("NAME"));

                assertThrows(SQLException.class, () -> rs.findColumn("nonexistent"));
                assertThrows(SQLException.class, () -> rs.findColumn(""));
                assertThrows(SQLException.class, () -> rs.findColumn(" id"));
                assertThrows(SQLException.class, () -> rs.findColumn("id "));
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testAbsolute")
    class TestAbsolute {

        @Test
        @DisplayName("absolute — valid and boundary positions")
        void testAbsolutePositions() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id")) {

                assertTrue(rs.absolute(1));
                assertEquals(1, rs.getInt("id"));

                assertTrue(rs.absolute(10));
                assertEquals(10, rs.getInt("id"));

                assertFalse(rs.absolute(0)); // before first

                assertTrue(rs.absolute(-1)); // last row
                assertEquals(10, rs.getInt("id"));

                assertFalse(rs.absolute(11)); // beyond last
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testRelative")
    class TestRelative {

        @Test
        @DisplayName("relative — forward and backward moves")
        void testRelativePositions() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id")) {

                assertTrue(rs.absolute(3));
                assertEquals(3, rs.getInt("id"));

                assertTrue(rs.relative(2));
                assertEquals(5, rs.getInt("id"));

                assertTrue(rs.relative(-2));
                assertEquals(3, rs.getInt("id"));

                assertTrue(rs.relative(0));
                assertEquals(3, rs.getInt("id"));

                assertTrue(rs.relative(1));
                assertEquals(4, rs.getInt("id"));
            }
        }

        @Test
        @DisplayName("relative — edge cases: -2, tablerows, MIN_VALUE")
        void testRelativeEdgeCases() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id")) {

                // relative(-2) from row 5 → row 3
                assertTrue(rs.absolute(5));
                assertTrue(rs.relative(-2));
                assertEquals(3, rs.getInt("id"));

                // relative(rowCount) from row 1 → beyond last
                assertTrue(rs.absolute(1));
                assertFalse(rs.relative(10));

                // relative(MIN_VALUE) → before first
                assertTrue(rs.absolute(5));
                assertFalse(rs.relative(Integer.MIN_VALUE));
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testNext")
    class TestNext {

        @Test
        @DisplayName("next — iterate and verify termination")
        void testNextTerminates() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id")) {

                int count = 0;
                while (rs.next()) {
                    count++;
                    assertTrue(count <= 10, "next() should terminate");
                }
                assertEquals(10, count);
                assertFalse(rs.next(), "next() after last should return false");
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testMiscMovers")
    class TestMiscMovers {

        @Test
        @DisplayName("beforeFirst — at various cursor positions")
        void testBeforeFirst() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id")) {

                rs.beforeFirst();
                assertTrue(rs.isBeforeFirst());

                rs.absolute(5);
                rs.beforeFirst();
                assertTrue(rs.isBeforeFirst());

                while (rs.next()) {}
                rs.beforeFirst();
                assertTrue(rs.isBeforeFirst());
            }
        }

        @Test
        @DisplayName("afterLast — at various cursor positions")
        void testAfterLast() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id")) {

                rs.afterLast();
                assertTrue(rs.isAfterLast());

                rs.absolute(5);
                rs.afterLast();
                assertTrue(rs.isAfterLast());
            }
        }

        @Test
        @DisplayName("first — at various cursor positions")
        void testFirst() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id")) {

                assertTrue(rs.first());
                assertTrue(rs.isFirst());
                assertEquals(1, rs.getInt("id"));

                rs.absolute(5);
                assertTrue(rs.first());
                assertTrue(rs.isFirst());

                while (rs.next()) {}
                assertTrue(rs.first());
                assertTrue(rs.isFirst());
            }
        }

        @Test
        @DisplayName("last — at various cursor positions")
        void testLast() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id")) {

                assertTrue(rs.last());
                assertTrue(rs.isLast());
                assertEquals(10, rs.getInt("id"));
            }
        }

        @Test
        @DisplayName("isBeforeFirst — verified at various positions")
        void testIsBeforeFirst() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id")) {

                assertTrue(rs.isBeforeFirst());
                rs.next();
                assertFalse(rs.isBeforeFirst());
                rs.beforeFirst();
                assertTrue(rs.isBeforeFirst());
            }
        }

        @Test
        @DisplayName("isAfterLast — verified at various positions")
        void testIsAfterLast() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id")) {

                assertFalse(rs.isAfterLast());
                rs.afterLast();
                assertTrue(rs.isAfterLast());
                rs.previous();
                assertFalse(rs.isAfterLast());
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testDeleteRow")
    class TestDeleteRow {

        private final String tbl = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("SM_RS_DelRow"));

        @BeforeEach
        void setup() throws Exception {
            try (Connection conn = getConnection()) {
                createTestTable(conn, tbl, 5);
            }
        }

        @AfterEach
        void cleanup() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tbl, stmt);
            }
        }

        @Test
        @DisplayName("delete current row, try delete again on deleted row")
        void testDeleteAndDoubleDelete() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());
                rs.deleteRow();

                // Double-delete on same row (SQLBU#413368)
                assertThrows(SQLException.class, () -> rs.deleteRow());
            }
        }

        @Test
        @DisplayName("getXXX/updateXXX on deleted row throws — SQLBU#413368")
        void testGetUpdateOnDeletedRow() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());
                rs.deleteRow();

                assertThrows(SQLException.class, () -> rs.getInt("id"));
                assertThrows(SQLException.class, () -> rs.getString("name"));
                assertThrows(SQLException.class, () -> rs.updateInt("value", 999));
            }
        }

        @Test
        @DisplayName("deleteRow on insert row throws exception")
        void testDeleteOnInsertRow() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());
                rs.moveToInsertRow();
                assertThrows(SQLException.class, () -> rs.deleteRow());
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testGetRow")
    class TestGetRow {

        @Test
        @DisplayName("getRow returns correct row number")
        void testGetRowValid() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id")) {

                assertEquals(0, rs.getRow());
                assertTrue(rs.next());
                assertEquals(1, rs.getRow());
                assertTrue(rs.absolute(5));
                assertEquals(5, rs.getRow());
                assertTrue(rs.last());
                assertEquals(10, rs.getRow());
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testCancelRow")
    class TestCancelRow {

        private final String tbl = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("SM_RS_Cancel"));

        @BeforeEach
        void setup() throws Exception {
            try (Connection conn = getConnection()) {
                createTestTable(conn, tbl, 3);
            }
        }

        @AfterEach
        void cleanup() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tbl, stmt);
            }
        }

        @Test
        @DisplayName("cancelRowUpdates preserves original data")
        void testCancelPreservesData() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());
                String originalName = rs.getString("name");
                rs.updateString("name", "CHANGED");
                rs.cancelRowUpdates();
                rs.refreshRow();
                assertEquals(originalName, rs.getString("name"));
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testMiscRower")
    class TestMiscRower {

        private final String tbl = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("SM_RS_MiscRow"));

        @BeforeEach
        void setup() throws Exception {
            try (Connection conn = getConnection()) {
                createTestTable(conn, tbl, 3);
            }
        }

        @AfterEach
        void cleanup() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tbl, stmt);
            }
        }

        @Test
        @DisplayName("all rower operations on invalid row positions throw appropriately")
        void testRowersOnInvalidPositions() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                // Before first
                assertThrows(SQLException.class, () -> rs.insertRow());
                assertThrows(SQLException.class, () -> rs.deleteRow());
                assertThrows(SQLException.class, () -> rs.updateRow());
                try { rs.cancelRowUpdates(); } catch (SQLException e) { /* no-op acceptable */ }
                assertThrows(SQLException.class, () -> rs.refreshRow());
                assertEquals(0, rs.getRow());

                // After last
                while (rs.next()) {}
                assertThrows(SQLException.class, () -> rs.insertRow());
                assertThrows(SQLException.class, () -> rs.deleteRow());
                assertThrows(SQLException.class, () -> rs.updateRow());
                try { rs.cancelRowUpdates(); } catch (SQLException e) { /* no-op acceptable */ }
                assertThrows(SQLException.class, () -> rs.refreshRow());
                assertEquals(0, rs.getRow());
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testFetchSize")
    class TestFetchSize {

        @Test
        @DisplayName("default fetch size and set/get roundtrip")
        void testFetchSizeStandalone() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {

                int defaultSize = rs.getFetchSize();
                assertTrue(defaultSize > 0);

                rs.setFetchSize(100);
                assertEquals(100, rs.getFetchSize());

                rs.setFetchSize(0);
                assertEquals(defaultSize, rs.getFetchSize());

                assertThrows(SQLException.class, () -> rs.setFetchSize(-1));
            }
        }

        @Test
        @DisplayName("fetch size does not affect result completeness")
        void testFetchSizeWithMovers() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            int[] fetchSizes = {1, 5, 100, 129};
            for (int fetchSize : fetchSizes) {
                try (Connection conn = PrepUtil.getConnection(connectionString);
                        Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id")) {

                    rs.setFetchSize(fetchSize);
                    int count = 0;
                    while (rs.next()) {
                        count++;
                    }
                    assertEquals(10, count, "All rows should be returned with fetchSize=" + fetchSize);
                }
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testUpdaterInvalid")
    class TestUpdaterInvalid {

        @Test
        @DisplayName("updateXXX with invalid ordinals and names throws — all updater types")
        void testUpdaterInvalidIndices() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tbl = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SM_RS_UpdInv"));
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn, tbl, 3);
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                    assertTrue(rs.next());

                    assertThrows(SQLException.class, () -> rs.updateInt(0, 1));
                    assertThrows(SQLException.class, () -> rs.updateInt(-1, 1));
                    assertThrows(SQLException.class, () -> rs.updateInt(4, 1));

                    assertThrows(SQLException.class, () -> rs.updateString(0, "x"));
                    assertThrows(SQLException.class, () -> rs.updateString(-1, "x"));
                    assertThrows(SQLException.class, () -> rs.updateString(4, "x"));

                    assertThrows(SQLException.class, () -> rs.updateDouble(0, 1.0));
                    assertThrows(SQLException.class, () -> rs.updateDouble(4, 1.0));

                    assertThrows(SQLException.class, () -> rs.updateFloat(0, 1.0f));
                    assertThrows(SQLException.class, () -> rs.updateFloat(4, 1.0f));

                    assertThrows(SQLException.class, () -> rs.updateLong(0, 1L));
                    assertThrows(SQLException.class, () -> rs.updateLong(4, 1L));

                    assertThrows(SQLException.class, () -> rs.updateShort(0, (short) 1));
                    assertThrows(SQLException.class, () -> rs.updateShort(4, (short) 1));

                    assertThrows(SQLException.class, () -> rs.updateBoolean(0, true));
                    assertThrows(SQLException.class, () -> rs.updateBoolean(4, true));

                    assertThrows(SQLException.class, () -> rs.updateByte(0, (byte) 1));
                    assertThrows(SQLException.class, () -> rs.updateByte(4, (byte) 1));

                    assertThrows(SQLException.class, () -> rs.updateBigDecimal(0, java.math.BigDecimal.ONE));
                    assertThrows(SQLException.class, () -> rs.updateBigDecimal(4, java.math.BigDecimal.ONE));

                    assertThrows(SQLException.class, () -> rs.updateObject(0, "x"));
                    assertThrows(SQLException.class, () -> rs.updateObject(4, "x"));

                    assertThrows(SQLException.class, () -> rs.updateInt("", 1));
                    assertThrows(SQLException.class, () -> rs.updateInt("nonexistent", 1));
                    assertThrows(SQLException.class, () -> rs.updateInt(" id", 1));
                    assertThrows(SQLException.class, () -> rs.updateInt("id ", 1));
                    assertThrows(SQLException.class, () -> rs.updateString("nonexistent", "x"));
                    assertThrows(SQLException.class, () -> rs.updateDouble("nonexistent", 1.0));
                    assertThrows(SQLException.class, () -> rs.updateObject("nonexistent", "x"));
                } finally {
                    try (Statement s = conn.createStatement()) {
                        TestUtils.dropTableIfExists(tbl, s);
                    }
                }
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testUpdateNullInvalid")
    class TestUpdateNullInvalid {

        @Test
        @DisplayName("updateNull with invalid ordinals and names throws")
        void testUpdateNullInvalidIndices() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            String tbl = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SM_RS_NullInv"));
            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                createTestTable(conn, tbl, 3);
                try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                        ResultSet.CONCUR_UPDATABLE);
                        ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                    assertTrue(rs.next());

                    assertThrows(SQLException.class, () -> rs.updateNull(0));
                    assertThrows(SQLException.class, () -> rs.updateNull(-1));
                    assertThrows(SQLException.class, () -> rs.updateNull(4));

                    assertThrows(SQLException.class, () -> rs.updateNull(""));
                    assertThrows(SQLException.class, () -> rs.updateNull("nonexistent"));
                    assertThrows(SQLException.class, () -> rs.updateNull(" id"));
                    assertThrows(SQLException.class, () -> rs.updateNull("id "));
                } finally {
                    try (Statement s = conn.createStatement()) {
                        TestUtils.dropTableIfExists(tbl, s);
                    }
                }
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testClose")
    class TestClose {

        @Test
        @DisplayName("close ResultSet — operations throw after close")
        void testCloseResultSet() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {

                assertFalse(rs.isClosed());
                rs.close();
                assertTrue(rs.isClosed());

                assertThrows(SQLException.class, () -> rs.next());
                assertThrows(SQLException.class, () -> rs.getInt(1));
                assertThrows(SQLException.class, () -> rs.getString("id"));

                rs.close(); // double close should not throw
                assertTrue(rs.isClosed());
            }
        }

        @Test
        @DisplayName("close ResultSet — all methods throw after close (callAllMethods)")
        void testCloseResultSetCallAllMethods() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {

                assertFalse(rs.isClosed());
                rs.close();
                assertTrue(rs.isClosed());

                // Movers
                assertThrows(SQLException.class, () -> rs.next());
                assertThrows(SQLException.class, () -> rs.previous());
                assertThrows(SQLException.class, () -> rs.first());
                assertThrows(SQLException.class, () -> rs.last());
                assertThrows(SQLException.class, () -> rs.absolute(1));
                assertThrows(SQLException.class, () -> rs.relative(1));
                assertThrows(SQLException.class, () -> rs.beforeFirst());
                assertThrows(SQLException.class, () -> rs.afterLast());

                // Rowers
                assertThrows(SQLException.class, () -> rs.insertRow());
                assertThrows(SQLException.class, () -> rs.deleteRow());
                assertThrows(SQLException.class, () -> rs.updateRow());
                assertThrows(SQLException.class, () -> rs.refreshRow());

                // Getters/indexers
                assertThrows(SQLException.class, () -> rs.getInt(1));
                assertThrows(SQLException.class, () -> rs.getString("name"));
                assertThrows(SQLException.class, () -> rs.findColumn("id"));

                assertTrue(rs.isClosed());
            }
        }

        @Test
        @DisplayName("close Statement — ResultSet becomes closed")
        void testCloseStatement() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString)) {
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
                assertFalse(rs.isClosed());

                stmt.close();
                assertTrue(rs.isClosed());
            }
        }

        @Test
        @DisplayName("close Connection — ResultSet becomes closed")
        void testCloseConnection() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            Connection conn = PrepUtil.getConnection(connectionString);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME);
            assertFalse(rs.isClosed());

            conn.close();
            assertTrue(rs.isClosed());
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testFetchDirection")
    class TestFetchDirection {

        @Test
        @DisplayName("fetch direction get/set for scrollable cursor")
        void testFetchDirectionScrollable() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {

                // Default direction is readable
                assertTrue(rs.getFetchDirection() >= 0);

                rs.setFetchDirection(ResultSet.FETCH_FORWARD);
                assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());

                rs.setFetchDirection(ResultSet.FETCH_REVERSE);
                assertEquals(ResultSet.FETCH_REVERSE, rs.getFetchDirection());

                rs.setFetchDirection(ResultSet.FETCH_UNKNOWN);
                assertEquals(ResultSet.FETCH_UNKNOWN, rs.getFetchDirection());

                // Data still accessible after direction changes
                assertTrue(rs.next());
                assertNotNull(rs.getString("name"));
            }
        }

        @Test
        @DisplayName("fetch direction on forward-only cursor — only FETCH_FORWARD allowed")
        void testFetchDirectionForwardOnly() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                            ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {

                assertEquals(ResultSet.FETCH_FORWARD, rs.getFetchDirection());
                assertThrows(SQLException.class, () -> rs.setFetchDirection(ResultSet.FETCH_REVERSE));
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testDefaultInsertRow")
    class TestDefaultInsertRow {

        private final String tbl = AbstractSQLGenerator
                .escapeIdentifier(RandomUtil.getIdentifier("SM_RS_DefIns"));

        @BeforeEach
        void setup() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tbl, stmt);
                stmt.execute("CREATE TABLE " + tbl
                        + " (id INT PRIMARY KEY, value INT NULL, name VARCHAR(200) NULL)");
                stmt.execute("INSERT INTO " + tbl + " VALUES (1, 10, 'Original')");
            }
        }

        @AfterEach
        void cleanup() throws Exception {
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tbl, stmt);
            }
        }

        @Test
        @DisplayName("insertRow with only required columns (PK), nullable columns get NULL defaults")
        void testInsertWithDefaults() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " ORDER BY id")) {

                assertTrue(rs.next());

                rs.moveToInsertRow();
                rs.updateInt("id", 99);
                rs.insertRow();
                rs.moveToCurrentRow();
            }

            // Verify nullable columns got NULL defaults
            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM " + tbl + " WHERE id = 99")) {
                assertTrue(rs.next());
                rs.getInt("value");
                assertTrue(rs.wasNull());
                assertNull(rs.getString("name"));
            }
        }
    }

    @Tag(Constants.legacyFx)
    @Nested
    @DisplayName("FX testMultipleResultSetsClose")
    class TestMultipleResultSetsClose {

        @Test
        @DisplayName("re-executing statement closes first ResultSet, second remains usable")
        void testMultipleRSClose() throws SQLException {
            Assumptions.assumeTrue(connectionString != null);
            createTestTable(connection, TABLE_NAME);

            try (Connection conn = PrepUtil.getConnection(connectionString);
                    Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_READ_ONLY)) {

                ResultSet firstRS = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id");
                assertTrue(firstRS.next());
                assertNotNull(firstRS.getString("name"));

                // Re-execution implicitly closes the first RS
                ResultSet secondRS = stmt.executeQuery("SELECT * FROM " + TABLE_NAME + " ORDER BY id");

                assertTrue(firstRS.isClosed());
                assertThrows(SQLException.class, () -> firstRS.getInt(1));

                // Second RS is fully usable
                assertTrue(secondRS.next());
                assertNotNull(secondRS.getString("name"));
                int count = 1;
                while (secondRS.next()) count++;
                assertEquals(10, count);

                secondRS.close();
            }
        }
    }
}