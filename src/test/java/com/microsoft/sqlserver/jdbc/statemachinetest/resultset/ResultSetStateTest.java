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
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest.Action;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest.Engine;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest.Result;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * ResultSet State Machine Tests - Based on FX Framework fxResultSet model actions.
 * 
 * This class implements Model-Based Testing (MBT) for JDBC ResultSet operations using
 * the simple StateMachineTest framework. It mirrors the complex scenarios tested by
 * the FX/KoKoMo framework but with simpler, more debuggable code.
 * 
 * Test scenarios covered:
 * - Scrollable cursor navigation (next, previous, first, last, absolute, relative)
 * - Updatable cursor operations (insertRow, updateRow, deleteRow)
 * - Forward-only cursor constraints
 * - Complex sequences like delete-navigate-update
 * 
 * All actions are implemented as plain classes (NO LAMBDAS) for easier debugging.
 */
@Tag("statemachine")
class ResultSetStateTest extends AbstractTest {

    // Use escaped identifier for table name to handle special characters
    private static final String TABLE_NAME = AbstractSQLGenerator.escapeIdentifier("SM_ResultSet_Test");

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
    
    // ==================== SIMULATED RESULTSET (No DB needed) ====================
    
    @Test
    @DisplayName("FX Model: Scrollable Cursor Navigation (simulated)")
    void testScrollableCursorNavigation() {
        StateMachineTest sm = new StateMachineTest("ScrollableCursor");
        
        // State from fxResultSet: _currentrow, _closed, _insertrow, cache size
        sm.set("row", 0);           // 0=beforeFirst, 1-10=data, 11=afterLast
        sm.set("rowCount", 10);
        sm.set("closed", false);
        sm.set("insertRow", false); // On special insert row
        sm.set("scrollable", true);
        sm.set("updatable", true);
        
        // FX _modelnext: weight=10, requires closed()=false
        sm.addAction(new NextAction(sm));
        
        // FX _modelprevious: weight=10, requires isValidRow()=true, isScrollable()=true
        sm.addAction(new PreviousAction(sm));
        
        // FX _modelfirst: requires isScrollable()=true
        sm.addAction(new FirstAction(sm));
        
        // FX _modellast: requires isScrollable()=true
        sm.addAction(new LastAction(sm));
        
        // FX _modelabsolute: requires isValidRow()=true, isScrollable()=true
        sm.addAction(new AbsoluteAction(sm));
        
        // FX _modelrelative: weight=10, requires isValidRow()=true, isScrollable()=true
        sm.addAction(new RelativeAction(sm));
        
        // FX _modelbeforeFirst: requires isScrollable()=true
        sm.addAction(new BeforeFirstAction(sm));
        
        // FX _modelafterLast: requires isScrollable()=true
        sm.addAction(new AfterLastAction(sm));
        
        Result r = Engine.run(sm).withMaxActions(1000).withSeed(12345).execute();
        assertTrue(r.isSuccess());
        System.out.println("Scrollable cursor: " + r.actionCount + " actions, seed=" + r.seed);
    }
    
    @Test
    @DisplayName("FX Model: Updatable Cursor with Insert/Update/Delete (simulated)")
    void testUpdatableCursorOperations() {
        StateMachineTest sm = new StateMachineTest("UpdatableCursor");
        
        sm.set("row", 0);
        sm.set("rowCount", 10);
        sm.set("closed", false);
        sm.set("insertRow", false);
        sm.set("savedRow", 0);      // Row to return to after insert
        sm.set("dirty", false);     // Pending changes (updateXXX called but not updateRow)
        sm.set("holes", new HashSet<Integer>()); // Deleted rows become "holes"
        sm.set("scrollable", true);
        sm.set("updatable", true);
        
        // Navigation actions
        sm.addAction(new UpdatableNextAction(sm));
        sm.addAction(new UpdatablePreviousAction(sm));
        sm.addAction(new UpdatableFirstAction(sm));
        sm.addAction(new UpdatableLastAction(sm));
        
        // Data access/modify actions
        sm.addAction(new GetXXXAction(sm));
        sm.addAction(new UpdateXXXAction(sm));
        sm.addAction(new UpdateRowAction(sm));
        sm.addAction(new CancelRowUpdatesAction(sm));
        sm.addAction(new DeleteRowAction(sm));
        sm.addAction(new MoveToInsertRowAction(sm));
        sm.addAction(new InsertRowAction(sm));
        sm.addAction(new MoveToCurrentRowAction(sm));
        
        Result r = Engine.run(sm).withMaxActions(500).execute();
        System.out.println("Updatable cursor: " + r.actionCount + " actions");
        System.out.println("Final state: " + sm.getState());
        assertTrue(r.isSuccess());
    }
    
    @Test
    @DisplayName("FX Model: Forward-Only Cursor (no scrolling)")
    void testForwardOnlyCursor() {
        StateMachineTest sm = new StateMachineTest("ForwardOnlyCursor");
        
        sm.set("row", 0);
        sm.set("rowCount", 10);
        sm.set("closed", false);
        sm.set("scrollable", false); // Forward-only!
        sm.set("updatable", false);  // Read-only
        
        // Only next() is allowed for forward-only
        sm.addAction(new ForwardOnlyNextAction(sm));
        sm.addAction(new ForwardOnlyGetXXXAction(sm));
        sm.addAction(new ForwardOnlyCloseAction(sm));
        
        Result r = Engine.run(sm).withMaxActions(100).execute();
        assertTrue(r.isSuccess());
    }
    
    // ==================== REAL DATABASE TESTS ====================

    @Test
    @DisplayName("FX Model: Real Database - Scrollable Sensitive Cursor")
    void testRealDatabaseScrollableCursor() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        createTestTable();

        try (Connection conn = PrepUtil.getConnection(connectionString);
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {

            StateMachineTest sm = new StateMachineTest("RealScrollableCursor");
            sm.set("rs", rs);
            sm.set("closed", false);

            sm.addAction(new RealNextAction(sm));
            sm.addAction(new RealPreviousAction(sm));
            sm.addAction(new RealFirstAction(sm));
            sm.addAction(new RealLastAction(sm));
            sm.addAction(new RealAbsoluteAction(sm));
            sm.addAction(new RealGetStringAction(sm));

            // Run fewer actions for real DB test
            Result result = Engine.run(sm).withMaxActions(50).withSeed(99999).execute();

            System.out.println("\nReal DB test: " + result.actionCount + " actions");
            assertTrue(result.isSuccess());
        }
    }
    
    @Test
    @DisplayName("FX Model: Complex Sequence - Delete-Navigate-Update")
    void testComplexDeleteNavigateUpdate() {
        StateMachineTest sm = new StateMachineTest("DeleteNavigateUpdate");
        
        sm.set("row", 0);
        sm.set("rowCount", 10);
        sm.set("closed", false);
        sm.set("dirty", false);
        sm.set("holes", new HashSet<Integer>());
        
        sm.addAction(new ComplexNextAction(sm));
        sm.addAction(new ComplexPreviousAction(sm));
        sm.addAction(new ComplexUpdateXXXAction(sm));
        sm.addAction(new ComplexUpdateRowAction(sm));
        sm.addAction(new ComplexDeleteRowAction(sm));
        
        Result r = Engine.run(sm).withMaxActions(200).execute();
        
        @SuppressWarnings("unchecked")
        Set<Integer> holes = (Set<Integer>) sm.get("holes");
        System.out.println("Deleted rows (holes): " + holes);
        System.out.println("Final row position: " + sm.getInt("row"));
        
        assertTrue(r.isSuccess());
    }
    
    // ==================== ACTION CLASSES (Plain implementations, no lambdas) ====================
    
    // --- Scrollable Cursor Actions ---
    
    static class NextAction extends Action {
        private StateMachineTest sm;
        
        NextAction(StateMachineTest sm) {
            super("next", 10);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && !sm.is("insertRow");
        }
        
        @Override
        public void run() {
            int row = sm.getInt("row");
            int rowCount = sm.getInt("rowCount");
            if (row <= rowCount) {
                sm.set("row", row + 1);
            }
        }
    }
    
    static class PreviousAction extends Action {
        private StateMachineTest sm;
        
        PreviousAction(StateMachineTest sm) {
            super("previous", 10);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") 
                && sm.is("scrollable") 
                && isValidRow(sm) 
                && !sm.is("insertRow");
        }
        
        @Override
        public void run() {
            int row = sm.getInt("row");
            if (row > 0) {
                sm.set("row", row - 1);
            }
        }
    }
    
    static class FirstAction extends Action {
        private StateMachineTest sm;
        
        FirstAction(StateMachineTest sm) {
            super("first", 5);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.is("scrollable") && !sm.is("insertRow");
        }
        
        @Override
        public void run() {
            int rowCount = sm.getInt("rowCount");
            sm.set("row", rowCount > 0 ? 1 : 0);
        }
    }
    
    static class LastAction extends Action {
        private StateMachineTest sm;
        
        LastAction(StateMachineTest sm) {
            super("last", 5);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.is("scrollable") && !sm.is("insertRow");
        }
        
        @Override
        public void run() {
            sm.set("row", sm.getInt("rowCount"));
        }
    }
    
    static class AbsoluteAction extends Action {
        private StateMachineTest sm;
        private Random random = new Random();
        
        AbsoluteAction(StateMachineTest sm) {
            super("absolute", 10);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") 
                && sm.is("scrollable") 
                && isValidRow(sm) 
                && !sm.is("insertRow");
        }
        
        @Override
        public void run() {
            int target = random.nextInt(15) - 2; // -2 to 12
            int rows = sm.getInt("rowCount");
            int newRow;
            if (target == 0) {
                newRow = 0;
            } else if (target > 0) {
                newRow = Math.min(target, rows + 1);
            } else {
                newRow = Math.max(0, rows + 1 + target);
            }
            sm.set("row", newRow);
        }
    }
    
    static class RelativeAction extends Action {
        private StateMachineTest sm;
        private Random random = new Random();
        
        RelativeAction(StateMachineTest sm) {
            super("relative", 10);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") 
                && sm.is("scrollable") 
                && isValidRow(sm) 
                && !sm.is("insertRow");
        }
        
        @Override
        public void run() {
            int offset = random.nextInt(7) - 3; // -3 to 3
            int rows = sm.getInt("rowCount");
            int newRow = Math.max(0, Math.min(sm.getInt("row") + offset, rows + 1));
            sm.set("row", newRow);
        }
    }
    
    static class BeforeFirstAction extends Action {
        private StateMachineTest sm;
        
        BeforeFirstAction(StateMachineTest sm) {
            super("beforeFirst", 3);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.is("scrollable") && !sm.is("insertRow");
        }
        
        @Override
        public void run() {
            sm.set("row", 0);
        }
    }
    
    static class AfterLastAction extends Action {
        private StateMachineTest sm;
        
        AfterLastAction(StateMachineTest sm) {
            super("afterLast", 3);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.is("scrollable") && !sm.is("insertRow");
        }
        
        @Override
        public void run() {
            sm.set("row", sm.getInt("rowCount") + 1);
        }
    }
    
    // --- Updatable Cursor Actions ---
    
    static class UpdatableNextAction extends Action {
        private StateMachineTest sm;
        
        UpdatableNextAction(StateMachineTest sm) {
            super("next", 10);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && !sm.is("insertRow");
        }
        
        @Override
        public void run() {
            int row = sm.getInt("row");
            int rowCount = sm.getInt("rowCount");
            if (row <= rowCount) {
                sm.set("row", row + 1);
            }
            sm.set("dirty", false);
        }
    }
    
    static class UpdatablePreviousAction extends Action {
        private StateMachineTest sm;
        
        UpdatablePreviousAction(StateMachineTest sm) {
            super("previous", 8);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") 
                && sm.is("scrollable") 
                && isValidRow(sm) 
                && !sm.is("insertRow");
        }
        
        @Override
        public void run() {
            sm.set("row", sm.getInt("row") - 1);
            sm.set("dirty", false);
        }
    }
    
    static class UpdatableFirstAction extends Action {
        private StateMachineTest sm;
        
        UpdatableFirstAction(StateMachineTest sm) {
            super("first", 5);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.is("scrollable") && !sm.is("insertRow");
        }
        
        @Override
        public void run() {
            sm.set("row", 1);
            sm.set("dirty", false);
        }
    }
    
    static class UpdatableLastAction extends Action {
        private StateMachineTest sm;
        
        UpdatableLastAction(StateMachineTest sm) {
            super("last", 5);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.is("scrollable") && !sm.is("insertRow");
        }
        
        @Override
        public void run() {
            sm.set("row", sm.getInt("rowCount"));
            sm.set("dirty", false);
        }
    }
    
    static class GetXXXAction extends Action {
        private StateMachineTest sm;
        
        GetXXXAction(StateMachineTest sm) {
            super("getXXX", 10);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && isValidRow(sm) && !sm.is("dirty") && !isHole(sm);
        }
        
        @Override
        public void run() {
            // Read column value - no state change
        }
    }
    
    static class UpdateXXXAction extends Action {
        private StateMachineTest sm;
        
        UpdateXXXAction(StateMachineTest sm) {
            super("updateXXX", 8);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") 
                && (isValidRow(sm) || sm.is("insertRow")) 
                && !isHole(sm);
        }
        
        @Override
        public void run() {
            sm.set("dirty", true);
        }
    }
    
    static class UpdateRowAction extends Action {
        private StateMachineTest sm;
        
        UpdateRowAction(StateMachineTest sm) {
            super("updateRow", 10);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") 
                && sm.is("updatable") 
                && isValidRow(sm) 
                && sm.is("dirty") 
                && !isHole(sm);
        }
        
        @Override
        public void run() {
            sm.set("dirty", false);
        }
    }
    
    static class CancelRowUpdatesAction extends Action {
        private StateMachineTest sm;
        
        CancelRowUpdatesAction(StateMachineTest sm) {
            super("cancelRowUpdates", 3);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") 
                && sm.is("updatable") 
                && sm.is("scrollable") 
                && isValidRow(sm) 
                && !isHole(sm);
        }
        
        @Override
        public void run() {
            sm.set("dirty", false);
        }
    }
    
    static class DeleteRowAction extends Action {
        private StateMachineTest sm;
        
        DeleteRowAction(StateMachineTest sm) {
            super("deleteRow", 2);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") 
                && sm.is("updatable") 
                && isValidRow(sm) 
                && !isHole(sm) 
                && !sm.is("dirty");
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            Set<Integer> holes = (Set<Integer>) sm.get("holes");
            holes.add(sm.getInt("row"));
        }
    }
    
    static class MoveToInsertRowAction extends Action {
        private StateMachineTest sm;
        
        MoveToInsertRowAction(StateMachineTest sm) {
            super("moveToInsertRow", 10);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.is("updatable") && !sm.is("insertRow");
        }
        
        @Override
        public void run() {
            sm.set("savedRow", sm.getInt("row"));
            sm.set("insertRow", true);
            sm.set("dirty", false);
        }
    }
    
    static class InsertRowAction extends Action {
        private StateMachineTest sm;
        
        InsertRowAction(StateMachineTest sm) {
            super("insertRow", 3);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") 
                && sm.is("updatable") 
                && sm.is("insertRow") 
                && sm.is("dirty");
        }
        
        @Override
        public void run() {
            sm.set("rowCount", sm.getInt("rowCount") + 1);
            sm.set("dirty", false);
            // Stay on insert row until moveToCurrentRow
        }
    }
    
    static class MoveToCurrentRowAction extends Action {
        private StateMachineTest sm;
        
        MoveToCurrentRowAction(StateMachineTest sm) {
            super("moveToCurrentRow", 5);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.is("insertRow");
        }
        
        @Override
        public void run() {
            sm.set("row", sm.getInt("savedRow"));
            sm.set("insertRow", false);
            sm.set("dirty", false);
        }
    }
    
    // --- Forward-Only Cursor Actions ---
    
    static class ForwardOnlyNextAction extends Action {
        private StateMachineTest sm;
        
        ForwardOnlyNextAction(StateMachineTest sm) {
            super("next", 10);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.getInt("row") <= sm.getInt("rowCount");
        }
        
        @Override
        public void run() {
            sm.set("row", sm.getInt("row") + 1);
        }
    }
    
    static class ForwardOnlyGetXXXAction extends Action {
        private StateMachineTest sm;
        
        ForwardOnlyGetXXXAction(StateMachineTest sm) {
            super("getXXX", 10);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && isValidRow(sm);
        }
        
        @Override
        public void run() {
            // read
        }
    }
    
    static class ForwardOnlyCloseAction extends Action {
        private StateMachineTest sm;
        
        ForwardOnlyCloseAction(StateMachineTest sm) {
            super("close", 1);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }
        
        @Override
        public void run() {
            sm.set("closed", true);
        }
    }
    
    // --- Real Database Actions ---
    
    static class RealNextAction extends Action {
        private StateMachineTest sm;
        
        RealNextAction(StateMachineTest sm) {
            super("next", 10);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }
        
        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.get("rs");
            boolean valid = rs.next();
            System.out.println("  next() -> " + valid + " row=" + (valid ? rs.getInt("id") : "N/A"));
        }
    }
    
    static class RealPreviousAction extends Action {
        private StateMachineTest sm;
        
        RealPreviousAction(StateMachineTest sm) {
            super("previous", 8);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }
        
        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.get("rs");
            boolean valid = rs.previous();
            System.out.println("  previous() -> " + valid);
        }
    }
    
    static class RealFirstAction extends Action {
        private StateMachineTest sm;
        
        RealFirstAction(StateMachineTest sm) {
            super("first", 5);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }
        
        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.get("rs");
            boolean valid = rs.first();
            System.out.println("  first() -> " + valid + " id=" + (valid ? rs.getInt("id") : "N/A"));
        }
    }
    
    static class RealLastAction extends Action {
        private StateMachineTest sm;
        
        RealLastAction(StateMachineTest sm) {
            super("last", 5);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }
        
        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.get("rs");
            boolean valid = rs.last();
            System.out.println("  last() -> " + valid + " id=" + (valid ? rs.getInt("id") : "N/A"));
        }
    }
    
    static class RealAbsoluteAction extends Action {
        private StateMachineTest sm;
        private Random random = new Random();
        
        RealAbsoluteAction(StateMachineTest sm) {
            super("absolute", 6);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }
        
        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.get("rs");
            int target = random.nextInt(12) - 1;
            boolean valid = rs.absolute(target);
            System.out.println("  absolute(" + target + ") -> " + valid);
        }
    }
    
    static class RealGetStringAction extends Action {
        private StateMachineTest sm;
        
        RealGetStringAction(StateMachineTest sm) {
            super("getString", 10);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }
        
        @Override
        public void run() {
            ResultSet rs = (ResultSet) sm.get("rs");
            try {
                String name = rs.getString("name");
                System.out.println("  getString('name') -> " + name);
            } catch (SQLException e) {
                System.out.println("  getString failed: " + e.getMessage());
            }
        }
    }
    
    // --- Complex Delete-Navigate-Update Actions ---
    
    static class ComplexNextAction extends Action {
        private StateMachineTest sm;
        
        ComplexNextAction(StateMachineTest sm) {
            super("next", 10);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }
        
        @Override
        public void run() {
            int row = sm.getInt("row");
            int rowCount = sm.getInt("rowCount");
            sm.set("row", Math.min(row + 1, rowCount + 1));
            sm.set("dirty", false);
        }
    }
    
    static class ComplexPreviousAction extends Action {
        private StateMachineTest sm;
        
        ComplexPreviousAction(StateMachineTest sm) {
            super("previous", 8);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.getInt("row") > 0;
        }
        
        @Override
        public void run() {
            sm.set("row", sm.getInt("row") - 1);
            sm.set("dirty", false);
        }
    }
    
    static class ComplexUpdateXXXAction extends Action {
        private StateMachineTest sm;
        
        ComplexUpdateXXXAction(StateMachineTest sm) {
            super("updateXXX", 8);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && isValidRow(sm) && !isHole(sm);
        }
        
        @Override
        public void run() {
            sm.set("dirty", true);
        }
    }
    
    static class ComplexUpdateRowAction extends Action {
        private StateMachineTest sm;
        
        ComplexUpdateRowAction(StateMachineTest sm) {
            super("updateRow", 6);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && isValidRow(sm) && sm.is("dirty") && !isHole(sm);
        }
        
        @Override
        public void run() {
            sm.set("dirty", false);
        }
    }
    
    static class ComplexDeleteRowAction extends Action {
        private StateMachineTest sm;
        
        ComplexDeleteRowAction(StateMachineTest sm) {
            super("deleteRow", 2);
            this.sm = sm;
        }
        
        @Override
        public boolean canRun() {
            return !sm.is("closed") && isValidRow(sm) && !isHole(sm) && !sm.is("dirty");
        }
        
        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            Set<Integer> holes = (Set<Integer>) sm.get("holes");
            holes.add(sm.getInt("row"));
            System.out.println("  DELETED row " + sm.getInt("row"));
        }
    }
    
    // ==================== HELPER METHODS (mirrors FX isValidRow, isHole) ====================
    
    private static boolean isValidRow(StateMachineTest sm) {
        int row = sm.getInt("row");
        int count = sm.getInt("rowCount");
        return row > 0 && row <= count && !sm.is("insertRow");
    }
    
    @SuppressWarnings("unchecked")
    private static boolean isHole(StateMachineTest sm) {
        Set<Integer> holes = (Set<Integer>) sm.get("holes");
        return holes != null && holes.contains(sm.getInt("row"));
    }
}
