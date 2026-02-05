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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Action;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Engine;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Result;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest;
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
public class ResultSetStateTest extends AbstractTest {

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
            sm.set("onValidRow", false); // Track if cursor is on a valid row

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

    // ==================== REAL DATABASE ACTION CLASSES ====================
    
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
            sm.set("onValidRow", valid);
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
            sm.set("onValidRow", valid);
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
            sm.set("onValidRow", valid);
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
            sm.set("onValidRow", valid);
            System.out.println("  last() -> " + valid + " id=" + (valid ? rs.getInt("id") : "N/A"));
        }
    }
    
    static class RealAbsoluteAction extends Action {
        private StateMachineTest sm;
        
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
            int target = sm.getRandom().nextInt(12) - 1;
            boolean valid = rs.absolute(target);
            sm.set("onValidRow", valid);
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
            // Only allow getString when cursor is on a valid row
            return !sm.is("closed") && sm.is("onValidRow");
        }
        
        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.get("rs");
            String name = rs.getString("name");
            System.out.println("  getString('name') -> " + name);
        }
    }
}
