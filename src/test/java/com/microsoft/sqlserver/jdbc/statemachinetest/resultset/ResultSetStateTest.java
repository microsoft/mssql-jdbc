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

    /**
     * FX Model: ResultSet Metadata Operations (simulated)
     * 
     * Tests FX model actions for metadata retrieval:
     * - _modelgetRow: Get current row number
     * - _modelgetType: Get cursor type
     * - _modelgetConcurrency: Get concurrency mode
     * - _modelgetFetchDirection: Get fetch direction
     * - _modelgetFetchSize: Get fetch size
     * - _modelgetMetaData: Get result set metadata
     * - _modelfindColumn: Find column by name
     * - _modelclearWarnings: Clear warnings
     * - wasNull: Check if last column was NULL
     */
    @Test
    @DisplayName("FX Model: ResultSet Metadata Operations (simulated)")
    void testResultSetMetadataOperations() {
        StateMachineTest sm = new StateMachineTest("MetadataOperations");

        // State
        sm.set("row", 0);
        sm.set("rowCount", 10);
        sm.set("closed", false);
        sm.set("insertRow", false);
        sm.set("scrollable", true);
        sm.set("updatable", true);
        sm.set("dynamic", false); // isDynamic() for getRow
        sm.set("cursorType", ResultSet.TYPE_SCROLL_SENSITIVE);
        sm.set("concurrency", ResultSet.CONCUR_UPDATABLE);
        sm.set("fetchDirection", ResultSet.FETCH_FORWARD);
        sm.set("fetchSize", 0);
        sm.set("columnCount", 3); // id, name, value
        sm.set("wasNull", false);
        sm.set("warnings", false);

        // Navigation (to get into valid positions)
        sm.addAction(new MetadataNextAction(sm));
        sm.addAction(new MetadataFirstAction(sm));
        sm.addAction(new MetadataLastAction(sm));

        // FX _modelgetRow: requires closed()=false, isDynamic()=false
        sm.addAction(new GetRowAction(sm));

        // FX _modelgetType: requires closed()=false
        sm.addAction(new GetTypeAction(sm));

        // FX _modelgetConcurrency: requires closed()=false
        sm.addAction(new GetConcurrencyAction(sm));

        // FX _modelgetFetchDirection: requires closed()=false
        sm.addAction(new GetFetchDirectionAction(sm));

        // FX _modelgetFetchSize: requires closed()=false
        sm.addAction(new GetFetchSizeAction(sm));

        // FX _modelgetMetaData: weight=1, requires closed()=false
        sm.addAction(new GetMetaDataAction(sm));

        // FX _modelfindColumn: requires closed()=false
        sm.addAction(new FindColumnAction(sm));

        // FX _modelclearWarnings: requires closed()=false
        sm.addAction(new ClearWarningsAction(sm));

        // wasNull: Check if last read was NULL
        sm.addAction(new WasNullAction(sm));

        Result r = Engine.run(sm).withMaxActions(300).withSeed(77777).execute();
        assertTrue(r.isSuccess());
        System.out.println("Metadata operations: " + r.actionCount + " actions");
    }

    /**
     * FX Model: ResultSet with RefreshRow and RowDeleted/RowInserted/RowUpdated
     * (simulated)
     * 
     * Tests FX model actions for row state detection:
     * - refreshRow: Refresh current row from database
     * - rowDeleted: Check if row was deleted
     * - rowInserted: Check if row was inserted
     * - rowUpdated: Check if row was updated
     */
    @Test
    @DisplayName("FX Model: Row State Detection Operations (simulated)")
    void testRowStateDetection() {
        StateMachineTest sm = new StateMachineTest("RowStateDetection");

        sm.set("row", 0);
        sm.set("rowCount", 10);
        sm.set("closed", false);
        sm.set("insertRow", false);
        sm.set("scrollable", true);
        sm.set("updatable", true);
        sm.set("dirty", false);
        sm.set("holes", new HashSet<Integer>());
        sm.set("insertedRows", new HashSet<Integer>());
        sm.set("updatedRows", new HashSet<Integer>());

        // Navigation
        sm.addAction(new RowStateNextAction(sm));
        sm.addAction(new RowStatePreviousAction(sm));
        sm.addAction(new RowStateFirstAction(sm));

        // Row state checking
        sm.addAction(new RowDeletedAction(sm));
        sm.addAction(new RowInsertedAction(sm));
        sm.addAction(new RowUpdatedAction(sm));

        // Modification operations (to create states to check)
        sm.addAction(new RowStateDeleteRowAction(sm));
        sm.addAction(new RowStateUpdateXXXAction(sm));
        sm.addAction(new RowStateUpdateRowAction(sm));

        // RefreshRow - refresh from database
        sm.addAction(new RefreshRowAction(sm));

        Result r = Engine.run(sm).withMaxActions(200).execute();
        assertTrue(r.isSuccess());
        System.out.println("Row state detection: " + r.actionCount + " actions");
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

    // --- Metadata Operations Actions (FX _modelgetRow, _modelgetType, etc.) ---

    static class MetadataNextAction extends Action {
        private StateMachineTest sm;

        MetadataNextAction(StateMachineTest sm) {
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

    static class MetadataFirstAction extends Action {
        private StateMachineTest sm;

        MetadataFirstAction(StateMachineTest sm) {
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

    static class MetadataLastAction extends Action {
        private StateMachineTest sm;

        MetadataLastAction(StateMachineTest sm) {
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

    // FX _modelgetRow: requires closed()=false, isDynamic()=false
    static class GetRowAction extends Action {
        private StateMachineTest sm;

        GetRowAction(StateMachineTest sm) {
            super("getRow", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && !sm.is("dynamic");
        }

        @Override
        public void run() {
            int row = sm.getInt("row");
            // getRow returns 0 if not on a valid row
            int result = isValidRow(sm) ? row : 0;
            System.out.println("  getRow() -> " + result);
        }
    }

    // FX _modelgetType: requires closed()=false
    static class GetTypeAction extends Action {
        private StateMachineTest sm;

        GetTypeAction(StateMachineTest sm) {
            super("getType", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            int type = sm.getInt("cursorType");
            System.out.println("  getType() -> " + type);
        }
    }

    // FX _modelgetConcurrency: requires closed()=false
    static class GetConcurrencyAction extends Action {
        private StateMachineTest sm;

        GetConcurrencyAction(StateMachineTest sm) {
            super("getConcurrency", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            int concurrency = sm.getInt("concurrency");
            System.out.println("  getConcurrency() -> " + concurrency);
        }
    }

    // FX _modelgetFetchDirection: requires closed()=false
    static class GetFetchDirectionAction extends Action {
        private StateMachineTest sm;

        GetFetchDirectionAction(StateMachineTest sm) {
            super("getFetchDirection", 2);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            int direction = sm.getInt("fetchDirection");
            System.out.println("  getFetchDirection() -> " + direction);
        }
    }

    // FX _modelgetFetchSize: requires closed()=false
    static class GetFetchSizeAction extends Action {
        private StateMachineTest sm;

        GetFetchSizeAction(StateMachineTest sm) {
            super("getFetchSize", 2);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            int size = sm.getInt("fetchSize");
            System.out.println("  getFetchSize() -> " + size);
        }
    }

    // FX _modelgetMetaData: weight=1, requires closed()=false
    static class GetMetaDataAction extends Action {
        private StateMachineTest sm;

        GetMetaDataAction(StateMachineTest sm) {
            super("getMetaData", 1);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            int columnCount = sm.getInt("columnCount");
            System.out.println("  getMetaData().getColumnCount() -> " + columnCount);
        }
    }

    // FX _modelfindColumn: requires closed()=false
    static class FindColumnAction extends Action {
        private StateMachineTest sm;
        private String[] columnNames = { "id", "name", "value" };
        private Random random = new Random();

        FindColumnAction(StateMachineTest sm) {
            super("findColumn", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            String colName = columnNames[random.nextInt(columnNames.length)];
            int ordinal = java.util.Arrays.asList(columnNames).indexOf(colName) + 1;
            System.out.println("  findColumn(\"" + colName + "\") -> " + ordinal);
        }
    }

    // FX _modelclearWarnings: requires closed()=false
    static class ClearWarningsAction extends Action {
        private StateMachineTest sm;

        ClearWarningsAction(StateMachineTest sm) {
            super("clearWarnings", 2);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            sm.set("warnings", false);
            System.out.println("  clearWarnings()");
        }
    }

    // wasNull: Check if last read was NULL
    static class WasNullAction extends Action {
        private StateMachineTest sm;

        WasNullAction(StateMachineTest sm) {
            super("wasNull", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed");
        }

        @Override
        public void run() {
            boolean wasNull = sm.is("wasNull");
            System.out.println("  wasNull() -> " + wasNull);
        }
    }

    // --- Row State Detection Actions ---

    static class RowStateNextAction extends Action {
        private StateMachineTest sm;

        RowStateNextAction(StateMachineTest sm) {
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

    static class RowStatePreviousAction extends Action {
        private StateMachineTest sm;

        RowStatePreviousAction(StateMachineTest sm) {
            super("previous", 8);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.is("scrollable") && isValidRow(sm) && !sm.is("insertRow");
        }

        @Override
        public void run() {
            sm.set("row", sm.getInt("row") - 1);
            sm.set("dirty", false);
        }
    }

    static class RowStateFirstAction extends Action {
        private StateMachineTest sm;

        RowStateFirstAction(StateMachineTest sm) {
            super("first", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.is("scrollable") && !sm.is("insertRow");
        }

        @Override
        public void run() {
            sm.set("row", sm.getInt("rowCount") > 0 ? 1 : 0);
            sm.set("dirty", false);
        }
    }

    // FX rowDeleted: Check if current row was deleted
    static class RowDeletedAction extends Action {
        private StateMachineTest sm;

        RowDeletedAction(StateMachineTest sm) {
            super("rowDeleted", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && isValidRow(sm);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            Set<Integer> holes = (Set<Integer>) sm.get("holes");
            boolean deleted = holes != null && holes.contains(sm.getInt("row"));
            System.out.println("  rowDeleted() -> " + deleted);
        }
    }

    // FX rowInserted: Check if current row was inserted
    static class RowInsertedAction extends Action {
        private StateMachineTest sm;

        RowInsertedAction(StateMachineTest sm) {
            super("rowInserted", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && isValidRow(sm);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            Set<Integer> insertedRows = (Set<Integer>) sm.get("insertedRows");
            boolean inserted = insertedRows != null && insertedRows.contains(sm.getInt("row"));
            System.out.println("  rowInserted() -> " + inserted);
        }
    }

    // FX rowUpdated: Check if current row was updated
    static class RowUpdatedAction extends Action {
        private StateMachineTest sm;

        RowUpdatedAction(StateMachineTest sm) {
            super("rowUpdated", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && isValidRow(sm);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            Set<Integer> updatedRows = (Set<Integer>) sm.get("updatedRows");
            boolean updated = updatedRows != null && updatedRows.contains(sm.getInt("row"));
            System.out.println("  rowUpdated() -> " + updated);
        }
    }

    static class RowStateDeleteRowAction extends Action {
        private StateMachineTest sm;

        RowStateDeleteRowAction(StateMachineTest sm) {
            super("deleteRow", 2);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.is("updatable") && isValidRow(sm) && !isHole(sm) && !sm.is("dirty");
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            Set<Integer> holes = (Set<Integer>) sm.get("holes");
            holes.add(sm.getInt("row"));
            System.out.println("  deleteRow() - row " + sm.getInt("row"));
        }
    }

    static class RowStateUpdateXXXAction extends Action {
        private StateMachineTest sm;

        RowStateUpdateXXXAction(StateMachineTest sm) {
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

    static class RowStateUpdateRowAction extends Action {
        private StateMachineTest sm;

        RowStateUpdateRowAction(StateMachineTest sm) {
            super("updateRow", 6);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.is("closed") && sm.is("updatable") && isValidRow(sm) && sm.is("dirty") && !isHole(sm);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void run() {
            sm.set("dirty", false);
            Set<Integer> updatedRows = (Set<Integer>) sm.get("updatedRows");
            updatedRows.add(sm.getInt("row"));
            System.out.println("  updateRow() - row " + sm.getInt("row"));
        }
    }

    // FX refreshRow: Refresh current row from database
    static class RefreshRowAction extends Action {
        private StateMachineTest sm;

        RefreshRowAction(StateMachineTest sm) {
            super("refreshRow", 2);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            // refreshRow requires: valid row, not on insert row, updatable cursor
            return !sm.is("closed") && sm.is("updatable") && isValidRow(sm) && !sm.is("insertRow") && !isHole(sm);
        }

        @Override
        public void run() {
            // refreshRow discards pending changes
            sm.set("dirty", false);
            System.out.println("  refreshRow() - row " + sm.getInt("row"));
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
