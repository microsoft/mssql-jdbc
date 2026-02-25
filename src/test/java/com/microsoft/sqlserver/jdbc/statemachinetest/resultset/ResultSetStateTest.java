/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.resultset;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
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
 * Self-Contained ResultSet State Machine Tests
 * 
 * This test demonstrates a SELF-CONTAINED design where:
 * - States are defined inline (not in separate enum)
 * - Actions are inner classes within this test
 * - All behavior is visible in one file for easier understanding
 * 
 * Migrated from FX Framework:
 * - Based on fxResultSet model actions
 * - Implements Model-Based Testing (MBT) for JDBC ResultSet operations
 * 
 * Test scenarios covered:
 * - Scrollable cursor navigation (next, previous, first, last, absolute)
 * - Data retrieval (getString, getInt) with exact value comparison
 * - Data is compared against DataCache expected values
 */
@Tag(Constants.legacyFX)
public class ResultSetStateTest extends AbstractTest {

    private static final String TABLE_NAME = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SM_ResultSet_Test"));

    // ========================================================================
    // STATE DEFINITIONS - Self-contained, specific to this test
    // ========================================================================

    /** State key for the JDBC ResultSet object */
    private static final StateKey RS = () -> "rs";

    /** State key for ResultSet closed status (Boolean) */
    private static final StateKey CLOSED = () -> "closed";

    /** State key for whether cursor is on a valid row (Boolean) */
    private static final StateKey ON_VALID_ROW = () -> "onValidRow";

    /** State key for current row index (1-based, 0 = before first) */
    private static final StateKey CURRENT_ROW = () -> "currentRow";

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
            TestUtils.dropTableIfExists(TABLE_NAME, connection.createStatement());
        }
    }

    /**
     * Creates a test table with sample data for real database tests.
     */
    private static void createTestTable() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, name VARCHAR(50), value INT)");
            for (int i = 1; i <= 10; i++) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (" + i + ", 'Row" + i + "', " + (i * 10) + ")");
            }
        }
    }

    // ========================================================================
    // HELPER METHODS
    // ========================================================================

    /**
     * Verifies current row data matches expected values in cache.
     * 
     * Core validation pattern:
     * - Get expected data from DataCache for current row (data rows start at index
     * 1)
     * - Compare actual ResultSet values against expected values
     * - Throws AssertionFailedError if any value doesn't match
     * 
     * @param action the action performing validation (provides DataCache and state
     *               access)
     * @param rs     the ResultSet
     * @throws SQLException if database access fails
     */
    private static void verifyCurrentRow(Action action, ResultSet rs) throws SQLException {
        DataCache cache = action.getDataCache();
        if (cache == null || cache.getRowCount() <= 1) {
            return; // No expected data rows beyond state (row 0)
        }

        int currentRow = action.getStateInt(CURRENT_ROW);
        if (currentRow < 1 || currentRow >= cache.getRowCount()) {
            return; // Not on a valid cached data row (data at indices 1..getRowCount()-1)
        }

        // Get expected data (data rows start at index 1, matching 1-based currentRow)
        Map<String, Object> expectedRow = cache.getRow(currentRow);
        if (expectedRow == null) {
            return;
        }

        // Verify each column
        for (Map.Entry<String, Object> entry : expectedRow.entrySet()) {
            String columnName = entry.getKey();
            Object expected = entry.getValue();
            Object actual = rs.getObject(columnName);

            action.assertExpected(actual, expected,
                    String.format("Row %d column '%s' mismatch", currentRow, columnName));
        }
    }

    // ========================================================================
    // ACTION DEFINITIONS - Self-contained inner classes for this test
    // ========================================================================

    /**
     * Action: Move cursor to the next row.
     */
    private static class NextAction extends Action {

        NextAction() {
            super("next", 10);
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
            } else {
                setState(CURRENT_ROW, 0);
            }

            System.out.println("  next() -> " + valid +
                    (valid ? " row=" + getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            if (isState(ON_VALID_ROW)) {
                ResultSet rs = (ResultSet) getState(RS);
                verifyCurrentRow(this, rs);
            }
        }
    }

    /**
     * Action: Move cursor to the previous row.
     */
    private static class PreviousAction extends Action {

        PreviousAction() {
            super("previous", 8);
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
            } else {
                setState(CURRENT_ROW, 0);
            }

            System.out.println("  previous() -> " + valid +
                    (valid ? " row=" + getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            if (isState(ON_VALID_ROW)) {
                ResultSet rs = (ResultSet) getState(RS);
                verifyCurrentRow(this, rs);
            }
        }
    }

    /**
     * Action: Move cursor to the first row.
     */
    private static class FirstAction extends Action {

        FirstAction() {
            super("first", 5);
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
            } else {
                setState(CURRENT_ROW, 0);
            }

            System.out.println("  first() -> " + valid +
                    (valid ? " row=" + getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            if (isState(ON_VALID_ROW)) {
                ResultSet rs = (ResultSet) getState(RS);
                verifyCurrentRow(this, rs);
            }
        }
    }

    /**
     * Action: Move cursor to the last row.
     */
    private static class LastAction extends Action {

        LastAction() {
            super("last", 5);
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
            } else {
                setState(CURRENT_ROW, 0);
            }

            System.out.println("  last() -> " + valid +
                    (valid ? " row=" + getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            if (isState(ON_VALID_ROW)) {
                ResultSet rs = (ResultSet) getState(RS);
                verifyCurrentRow(this, rs);
            }
        }
    }

    /**
     * Action: Move cursor to an absolute row position.
     * Generates a random target row for exploration.
     */
    private static class AbsoluteAction extends Action {

        AbsoluteAction() {
            super("absolute", 6);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);

            // Generate random target row (data rows: getRowCount()-1, or default 10)
            int dataRows = dataCache.getRowCount() - 1; // Exclude state row 0
            int maxRow = dataRows > 0 ? dataRows : 10;
            int target = getRandom().nextInt(maxRow + 2) - 1; // -1 to maxRow

            boolean valid = rs.absolute(target);
            setState(ON_VALID_ROW, valid);

            if (valid) {
                setState(CURRENT_ROW, rs.getRow());
            } else {
                setState(CURRENT_ROW, 0);
            }

            System.out.println("  absolute(" + target + ") -> " + valid +
                    (valid ? " row=" + getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            if (isState(ON_VALID_ROW)) {
                ResultSet rs = (ResultSet) getState(RS);
                verifyCurrentRow(this, rs);
            }
        }
    }

    /**
     * Action: Get a String column value from the current row.
     * Validates against expected "name" column in DataCache.
     */
    private static class GetStringAction extends Action {
        private String lastValue;

        GetStringAction() {
            super("getString", 10);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && isState(ON_VALID_ROW);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            lastValue = rs.getString("name");
            System.out.println("  getString('name') -> " + lastValue);
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

    /**
     * Action: Get an int column value from the current row.
     * Validates against expected "value" column in DataCache.
     */
    private static class GetIntAction extends Action {
        private int lastValue;
        private boolean hasLastValue;

        GetIntAction() {
            super("getInt", 10);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && isState(ON_VALID_ROW);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) getState(RS);
            lastValue = rs.getInt("value");
            hasLastValue = true;
            System.out.println("  getInt('value') -> " + lastValue);
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

    /**
     * Action: Close the ResultSet.
     * Pure driver behavior — no validation needed.
     */
    private static class CloseAction extends Action {

        CloseAction() {
            super("close", 1);
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
            System.out.println("  close()");
        }
    }

    // ========================================================================
    // TEST CASES
    // ========================================================================

    @Test
    @DisplayName("FX Model: Real Database - Scrollable Sensitive Cursor")
    void testRealDatabaseScrollableCursor() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        createTestTable();

        try (Connection conn = PrepUtil.getConnection(connectionString);
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {

            StateMachineTest sm = new StateMachineTest("RealScrollableCursor");
            DataCache cache = sm.getDataCache();
            cache.updateValue(0, RS.key(), rs);
            cache.updateValue(0, CLOSED.key(), false);
            cache.updateValue(0, ON_VALID_ROW.key(), false);

            sm.addAction(new NextAction());
            sm.addAction(new PreviousAction());
            sm.addAction(new FirstAction());
            sm.addAction(new LastAction());
            sm.addAction(new AbsoluteAction());
            sm.addAction(new GetStringAction());

            Result result = Engine.run(sm).withMaxActions(50).execute();

            System.out.println("\nReal DB test: " + result.actionCount + " actions");
            assertTrue(result.isSuccess());
        }
    }

    @Test
    @DisplayName("Data Validation Test")
    void testWithDataValidation() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        // SM owns DataCache internally (row 0 = empty state row)
        StateMachineTest sm = new StateMachineTest("DataValidation");
        DataCache cache = sm.getDataCache();

        // Populate state in row 0 (RS set later after query)
        cache.updateValue(0, CLOSED.key(), false);
        cache.updateValue(0, ON_VALID_ROW.key(), false);
        cache.updateValue(0, CURRENT_ROW.key(), 0);

        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, name VARCHAR(50), value INT)");

            // Rows 1-10: expected data
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

            // Set RS in state row now that query is executed
            cache.updateValue(0, RS.key(), rs);

            // All actions share the same DataCache (auto-linked by addAction)
            sm.addAction(new NextAction());
            sm.addAction(new PreviousAction());
            sm.addAction(new FirstAction());
            sm.addAction(new LastAction());
            sm.addAction(new PreviousAction());
            sm.addAction(new AbsoluteAction());
            sm.addAction(new GetStringAction());
            sm.addAction(new GetIntAction());

            Result result = Engine.run(sm).withMaxActions(50).withSeed(12345).execute();

            System.out.println("\nValidation test: " + result.actionCount + " actions");
            assertTrue(result.isSuccess());
        }
    }
}
