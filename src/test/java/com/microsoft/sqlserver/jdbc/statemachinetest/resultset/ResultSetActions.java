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
 * ResultSet actions for state machine testing.
 */
public final class ResultSetActions {

    private ResultSetActions() {
    }

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
            sm.setState(ON_INSERT_ROW, false);
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
            sm.setState(ON_INSERT_ROW, false);
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
            sm.setState(ON_INSERT_ROW, false);
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
            sm.setState(ON_INSERT_ROW, false);
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
            sm.setState(ON_INSERT_ROW, false);
        }
    }

    public static class RelativeAction extends Action {
        private final StateMachineTest sm;

        public RelativeAction(StateMachineTest sm) {
            super("relative", 4);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && sm.isState(ON_VALID_ROW);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            int offset = sm.getRandom().nextInt(5) - 2;
            boolean valid = rs.relative(offset);
            sm.setState(ON_VALID_ROW, valid);
        }
    }

    public static class BeforeFirstAction extends Action {
        private final StateMachineTest sm;

        public BeforeFirstAction(StateMachineTest sm) {
            super("beforeFirst", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            rs.beforeFirst();
            sm.setState(ON_VALID_ROW, false);
            sm.setState(ON_INSERT_ROW, false);
        }
    }

    public static class AfterLastAction extends Action {
        private final StateMachineTest sm;

        public AfterLastAction(StateMachineTest sm) {
            super("afterLast", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            rs.afterLast();
            sm.setState(ON_VALID_ROW, false);
            sm.setState(ON_INSERT_ROW, false);
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
            return !sm.isState(CLOSED) && sm.isState(ON_VALID_ROW);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            rs.getString("name");
        }
    }

    public static class MoveToInsertRowAction extends Action {
        private final StateMachineTest sm;

        public MoveToInsertRowAction(StateMachineTest sm) {
            super("moveToInsertRow", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && sm.isState(IS_UPDATABLE);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            rs.moveToInsertRow();
            sm.setState(ON_INSERT_ROW, true);
            sm.setState(ON_VALID_ROW, false);
        }
    }

    public static class MoveToCurrentRowAction extends Action {
        private final StateMachineTest sm;

        public MoveToCurrentRowAction(StateMachineTest sm) {
            super("moveToCurrentRow", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && sm.isState(IS_UPDATABLE);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            rs.moveToCurrentRow();
            sm.setState(ON_INSERT_ROW, false);
        }
    }

    public static class UpdateValueAction extends Action {
        private final StateMachineTest sm;

        public UpdateValueAction(StateMachineTest sm) {
            super("updateValue", 5);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && sm.isState(IS_UPDATABLE) && sm.isState(ON_VALID_ROW);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            int newValue = sm.getRandom().nextInt(10000);
            rs.updateInt("value", newValue);
            rs.updateString("name", "Updated_" + newValue);
        }
    }

    public static class UpdateRowAction extends Action {
        private final StateMachineTest sm;

        public UpdateRowAction(StateMachineTest sm) {
            super("updateRow", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && sm.isState(IS_UPDATABLE) && sm.isState(ON_VALID_ROW)
                    && !sm.isState(ON_INSERT_ROW);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            int newValue = sm.getRandom().nextInt(10000);
            rs.updateInt("value", newValue);
            rs.updateString("name", "UpdRow_" + newValue);
            rs.updateRow();
        }
    }

    public static class DeleteRowAction extends Action {
        private final StateMachineTest sm;

        public DeleteRowAction(StateMachineTest sm) {
            super("deleteRow", 2);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && sm.isState(IS_UPDATABLE) && sm.isState(ON_VALID_ROW)
                    && !sm.isState(ON_INSERT_ROW);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            rs.deleteRow();
            sm.setState(ROW_DELETED, true);
        }
    }

    public static class CancelRowUpdatesAction extends Action {
        private final StateMachineTest sm;

        public CancelRowUpdatesAction(StateMachineTest sm) {
            super("cancelRowUpdates", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && sm.isState(ON_VALID_ROW) && !sm.isState(ON_INSERT_ROW);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            rs.cancelRowUpdates();
        }
    }

    public static class RefreshRowAction extends Action {
        private final StateMachineTest sm;

        public RefreshRowAction(StateMachineTest sm) {
            super("refreshRow", 2);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED) && sm.isState(IS_UPDATABLE) && sm.isState(ON_VALID_ROW)
                    && !sm.isState(ON_INSERT_ROW) && sm.isState(IS_SCROLLABLE);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            rs.refreshRow();
        }
    }

    public static class GetRowAction extends Action {
        private final StateMachineTest sm;

        public GetRowAction(StateMachineTest sm) {
            super("getRow", 4);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            rs.getRow();
        }
    }

    public static class IsFirstAction extends Action {
        private final StateMachineTest sm;

        public IsFirstAction(StateMachineTest sm) {
            super("isFirst", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            rs.isFirst();
        }
    }

    public static class IsLastAction extends Action {
        private final StateMachineTest sm;

        public IsLastAction(StateMachineTest sm) {
            super("isLast", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            rs.isLast();
        }
    }

    public static class IsBeforeFirstAction extends Action {
        private final StateMachineTest sm;

        public IsBeforeFirstAction(StateMachineTest sm) {
            super("isBeforeFirst", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            rs.isBeforeFirst();
        }
    }

    public static class IsAfterLastAction extends Action {
        private final StateMachineTest sm;

        public IsAfterLastAction(StateMachineTest sm) {
            super("isAfterLast", 3);
            this.sm = sm;
        }

        @Override
        public boolean canRun() {
            return !sm.isState(CLOSED);
        }

        @Override
        public void run() throws SQLException {
            ResultSet rs = (ResultSet) sm.getStateValue(RS);
            rs.isAfterLast();
        }
    }
}
