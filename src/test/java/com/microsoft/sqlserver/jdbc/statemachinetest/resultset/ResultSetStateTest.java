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
 * MBT for JDBC ResultSet: scrollable cursor navigation and data retrieval
 * validated against DataCache.
 */
@Tag(Constants.legacyFx)
public class ResultSetStateTest extends AbstractTest {

    private static final String TABLE_NAME = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SM_ResultSet_Test"));

    // State definitions
    private static final StateKey RS = () -> "rs";
    private static final StateKey CLOSED = () -> "closed";
    private static final StateKey ON_VALID_ROW = () -> "onValidRow";
    private static final StateKey CURRENT_ROW = () -> "currentRow";

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
            } else {
                setState(CURRENT_ROW, 0);
            }

            System.out.println("next() -> " + valid + (valid ? " row=" + getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            if (isState(ON_VALID_ROW)) {
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
            } else {
                setState(CURRENT_ROW, 0);
            }

            System.out.println("previous() -> " + valid + (valid ? " row=" + getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            if (isState(ON_VALID_ROW)) {
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
            } else {
                setState(CURRENT_ROW, 0);
            }

            System.out.println("first() -> " + valid + (valid ? " row=" + getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            if (isState(ON_VALID_ROW)) {
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
            } else {
                setState(CURRENT_ROW, 0);
            }

            System.out.println("last() -> " + valid + (valid ? " row=" + getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            if (isState(ON_VALID_ROW)) {
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
            } else {
                setState(CURRENT_ROW, 0);
            }

            System.out.println(
                    "absolute(" + target + ") -> " + valid + (valid ? " row=" + getStateInt(CURRENT_ROW) : ""));
        }

        @Override
        public void validate() throws SQLException {
            if (isState(ON_VALID_ROW)) {
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
            return !isState(CLOSED) && isState(ON_VALID_ROW);
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
            return !isState(CLOSED) && isState(ON_VALID_ROW);
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

}
