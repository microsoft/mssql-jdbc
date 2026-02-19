package com.microsoft.sqlserver.jdbc.statemachinetest.resultset;

import static com.microsoft.sqlserver.jdbc.statemachinetest.resultset.ResultSetState.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

import com.microsoft.sqlserver.jdbc.statemachinetest.core.Action;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.DataCache;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest;


/**
 * ResultSet-related actions for state machine testing.
 * 
 * These actions model JDBC ResultSet operations with data validation:
 * - Scrollable cursor navigation (next, previous, first, last, absolute)
 * - Data retrieval (getString, getInt) with exact value comparison
 * - Data is compared against DataCache expected values
 * - Uses strict assertions for validation (throws on mismatch)
 * 
 * @see ResultSetState for state keys
 * @see DataCache for expected data storage
 */
public final class ResultSetActions {

    private ResultSetActions() {
        // Utility class - prevent instantiation
    }

    // ==================== Helper Methods ====================

    /**
     * Verifies current row data matches expected values in cache.
     * 
     * Core validation pattern:
     * - Get expected data from DataCache for current row
     * - Compare actual ResultSet values against expected values
     * - Throws ValidationException if any value doesn't match
     * 
     * @param action the action performing validation
     * @param sm     the state machine
     * @param rs     the ResultSet
     * @throws SQLException if database access fails
     */
    private static void verifyCurrentRow(Action action, StateMachineTest sm, ResultSet rs)
            throws SQLException {
        DataCache cache = action.getDataCache(); // Use getter method
        if (cache == null || cache.isEmpty()) {
            return; // No expected data to verify against
        }

        int currentRow = sm.getStateInt(CURRENT_ROW);
        if (currentRow < 1 || currentRow > cache.getRowCount()) {
            return; // Not on a valid cached row
        }

        // Get expected data (cache uses 0-based indexing)
        Map<String, Object> expectedRow = cache.getRow(currentRow - 1);
        if (expectedRow == null) {
            return;
        }

        // Verify each column
        for (String columnName : cache.getColumnNames()) {
            Object expected = expectedRow.get(columnName);
            Object actual;

            // Get actual value based on column type
            String colType = cache.getColumnType(columnName);
            if ("INT".equals(colType)) {
                actual = rs.getInt(columnName);
                if (rs.wasNull()) {
                    actual = null;
                }
            } else {
                actual = rs.getString(columnName);
            }

            // Strict comparison - throws on mismatch
            action.assertExpected(actual, expected,
                    String.format("Row %d column '%s' mismatch", currentRow, columnName));
        }
    }

    // ==================== Validated Action Classes ====================

    public static class NextAction extends Action {
        private final StateMachineTest sm;

        public NextAction(StateMachineTest sm) {
            super("next", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            boolean valid = rs.next();
            sm.setState(ON_VALID_ROW, valid);

            // Update current row position using actual cursor position
            if (valid) {
                sm.setState(CURRENT_ROW, rs.getRow());
            }

            System.out.println("  next() -> " + valid +
                    (valid ? " row=" + sm.getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            // Framework calls this after run()
            if (sm.isState(ON_VALID_ROW)) {
                ResultSet rs = (ResultSet) sm.getStateValue(RS);
                verifyCurrentRow(this, sm, rs);
            }
        }
    }

    public static class PreviousAction extends Action {
        private final StateMachineTest sm;

        public PreviousAction(StateMachineTest sm) {
            super("previous", 8);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            boolean valid = rs.previous();
            sm.setState(ON_VALID_ROW, valid);

            // Update current row position using actual cursor position
            if (valid) {
                sm.setState(CURRENT_ROW, rs.getRow());
            }

            System.out.println("  previous() -> " + valid +
                    (valid ? " row=" + sm.getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            // Framework calls this after run()
            if (sm.isState(ON_VALID_ROW)) {
                ResultSet rs = (ResultSet) sm.getStateValue(RS);
                verifyCurrentRow(this, sm, rs);
            }
        }
    }

    public static class FirstAction extends Action {
        private final StateMachineTest sm;

        public FirstAction(StateMachineTest sm) {
            super("first", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            boolean valid = rs.first();
            sm.setState(ON_VALID_ROW, valid);

            // Update current row position using actual cursor position
            if (valid) {
                sm.setState(CURRENT_ROW, rs.getRow());
            }

            System.out.println("  first() -> " + valid +
                    (valid ? " row=" + sm.getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            // Framework calls this after run()
            if (sm.isState(ON_VALID_ROW)) {
                ResultSet rs = (ResultSet) sm.getStateValue(RS);
                verifyCurrentRow(this, sm, rs);
            }
        }
    }

    public static class LastAction extends Action {
        private final StateMachineTest sm;

        public LastAction(StateMachineTest sm) {
            super("last", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            boolean valid = rs.last();
            sm.setState(ON_VALID_ROW, valid);

            // Update current row position using actual cursor position
            if (valid) {
                sm.setState(CURRENT_ROW, rs.getRow());
            }

            System.out.println("  last() -> " + valid +
                    (valid ? " row=" + sm.getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            // Framework calls this after run()
            if (sm.isState(ON_VALID_ROW)) {
                ResultSet rs = (ResultSet) sm.getStateValue(RS);
                verifyCurrentRow(this, sm, rs);
            }
        }
    }

    public static class AbsoluteAction extends Action {
        private final StateMachineTest sm;

        public AbsoluteAction(StateMachineTest sm) {
            super("absolute", 6);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            DataCache cache = getDataCache();

            // Generate random target row
            int maxRow = (cache != null && !cache.isEmpty()) ? cache.getRowCount() : 10;
            int target = sm.getRandom().nextInt(maxRow + 2) - 1; // -1 to maxRow

            boolean valid = rs.absolute(target);
            sm.setState(ON_VALID_ROW, valid);

            // Update current row position using actual cursor position
            if (valid) {
                sm.setState(CURRENT_ROW, rs.getRow());
            }

            System.out.println("  absolute(" + target + ") -> " + valid +
                    (valid ? " row=" + sm.getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            // Framework calls this after run()
            if (sm.isState(ON_VALID_ROW)) {
                ResultSet rs = (ResultSet) sm.getStateValue(RS);
                verifyCurrentRow(this, sm, rs);
            }
        }
    }

    public static class GetStringAction extends Action {
        private final StateMachineTest sm;

        public GetStringAction(StateMachineTest sm) {
            super("getString", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            // Only allow getString when cursor is on a valid row
            return !sm.isState(CLOSED) && sm.isState(ON_VALID_ROW);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            String actualName = rs.getString("name");
            System.out.println("  getString('name') -> " + actualName);
        }

        @Override
        public void validate() throws SQLException {
            // Framework calls this after run()
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            String actualName = rs.getString("name");

            DataCache cache = getDataCache();
            if (cache != null && !cache.isEmpty()) {
                int currentRow = sm.getStateInt(CURRENT_ROW);
                if (currentRow >= 1 && currentRow <= cache.getRowCount()) {
                    Object expectedName = cache.getValue(currentRow - 1, "name");
                    assertExpected(actualName, expectedName,
                            String.format("getString('name') mismatch at row %d", currentRow));
                }
            }
        }
    }

    public static class GetIntAction extends Action {
        private final StateMachineTest sm;

        public GetIntAction(StateMachineTest sm) {
            super("getInt", 10);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            // Only allow getInt when cursor is on a valid row
            return !sm.isState(CLOSED) && sm.isState(ON_VALID_ROW);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            int actualValue = rs.getInt("value");
            System.out.println("  getInt('value') -> " + actualValue);
        }

        @Override
        public void validate() throws SQLException {
            // Framework calls this after run()
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            int actualValue = rs.getInt("value");

            DataCache cache = getDataCache();
            if (cache != null && !cache.isEmpty()) {
                int currentRow = sm.getStateInt(CURRENT_ROW);
                if (currentRow >= 1 && currentRow <= cache.getRowCount()) {
                    Object expectedValue = cache.getValue(currentRow - 1, "value");
                    if (expectedValue != null) {
                        assertExpected(actualValue, ((Number) expectedValue).intValue(),
                                String.format("getInt('value') mismatch at row %d", currentRow));
                    }
                }
            }
        }
    }

    // ==================== Pure Driver Actions ====================
    // These actions represent pure JDBC driver behavior without validation

    /**
     * CloseAction - Pure driver behavior.
     * Extends Action directly since no validation is needed.
     * Simply closes the ResultSet and updates state.
     */
    public static class CloseAction extends Action {
        private final StateMachineTest sm;

        public CloseAction(StateMachineTest sm) {
            super("close", 1);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            rs.close(); // Pure driver behavior - just close the resource
            sm.setState(CLOSED, true);
            System.out.println("  close()");
        }
        // No validate() needed - pure driver operation
    }
}
