/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.resultset;

import static com.microsoft.sqlserver.jdbc.statemachinetest.resultset.ResultSetState.*;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.microsoft.sqlserver.jdbc.statemachinetest.core.Action;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest;


/**
 * ResultSet-related actions for state machine testing.
 * 
 * These actions model JDBC ResultSet operations:
 * - Scrollable cursor navigation (next, previous, first, last, absolute)
 * - Data retrieval (getString)
 * 
 * @see ResultSetState for state keys
 */
public final class ResultSetActions {

    private ResultSetActions() {
        // Utility class - prevent instantiation
    }

    // ==================== Action Classes ====================

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
            System.out.println("  next() -> " + valid + " row=" + (valid ? rs.getInt("id") : "N/A"));
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
            System.out.println("  previous() -> " + valid);
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
            System.out.println("  first() -> " + valid + " id=" + (valid ? rs.getInt("id") : "N/A"));
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
            System.out.println("  last() -> " + valid + " id=" + (valid ? rs.getInt("id") : "N/A"));
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
            int target = sm.getRandom().nextInt(12) - 1;
            boolean valid = rs.absolute(target);
            sm.setState(ON_VALID_ROW, valid);
            System.out.println("  absolute(" + target + ") -> " + valid);
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
            String name = rs.getString("name");
            System.out.println("  getString('name') -> " + name);
        }
    }
}
