/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.checkconstraint;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.ISQLServerConnection;
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
 * MBT test demonstrating a silent data-corruption bug in BulkCopy batch insert:
 * CHECK constraints are bypassed when useBulkCopyForBatchInsert=true.
 *
 * Root cause: SQLServerPreparedStatement.executeBatch() creates a
 * SQLServerBulkCopy with SQLServerBulkCopyOptions where checkConstraints
 * defaults to false. The generated INSERT BULK command omits the
 * CHECK_CONSTRAINTS option (see SQLServerBulkCopy.createInsertBulkCommand()),
 * so SQL Server skips constraint validation for bulk-loaded rows.
 *
 * Regular batch inserts use individual INSERT statements which always enforce
 * CHECK constraints server-side.
 *
 * This bug is uniquely suited for MBT discovery: randomized value generation
 * naturally produces constraint-violating values that a developer writing
 * deterministic tests would never include (tests use known-good data).
 * The model's validation action then detects invalid data in the database —
 * data that should have been rejected by the CHECK constraint.
 */
@Tag(Constants.legacyFX)
public class CheckConstraintBypassStateTest extends AbstractTest {

    private static final String TABLE_NAME = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("SM_CheckConstraint_Test"));

    private static final StateKey CONN = () -> "conn";
    private static final StateKey AUTO_COMMIT = () -> "autoCommit";
    private static final StateKey CLOSED = () -> "closed";

    private static final int CHECK_MAX = 100;
    private static final int VALUE_RANGE = 200;

    @BeforeAll
    static void setupTests() throws Exception {
        setConnection();
        createTestTable(connection);
    }

    @AfterAll
    static void cleanupTests() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            TestUtils.dropTableIfExists(TABLE_NAME, connection.createStatement());
        }
    }

    // TEST CASES

    /**
     * BulkCopy path: CHECK constraints are silently bypassed.
     *
     * When useBulkCopyForBatchInsert=true, the driver sends data via
     * INSERT BULK protocol without CHECK_CONSTRAINTS. Random values in
     * [0, 200) include values > 100 that violate the table's CHECK constraint.
     * These values are silently inserted — no exception, no warning.
     *
     * The ValidateCheckConstraintAction queries the DB for rows violating the
     * constraint. When it finds one, the test fails — proving the bug.
     *
     * This test is EXPECTED TO FAIL.
     */
    @Test
    @DisplayName("BulkCopy silently bypasses CHECK constraints (expected failure)")
    void testBulkCopyBypassesCheckConstraints() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            ((ISQLServerConnection) conn).setUseBulkCopyForBatchInsert(true);
            cleanTable(conn);

            StateMachineTest sm = new StateMachineTest("BulkCopyCheckBypass");
            DataCache cache = sm.getDataCache();
            cache.updateValue(0, CONN.key(), conn);
            cache.updateValue(0, AUTO_COMMIT.key(), true);
            cache.updateValue(0, CLOSED.key(), false);
            cache.updateValue(0, "nextId", 1);

            sm.addAction(new InsertRandomValueAction(30));
            sm.addAction(new ValidateCheckConstraintAction(25));
            sm.addAction(new SetAutoCommitFalseAction(10));
            sm.addAction(new SetAutoCommitTrueAction(8));
            sm.addAction(new CommitAction(12));
            sm.addAction(new RollbackAction(10));
            sm.addAction(new SelectCountAction(5));

            Result result = Engine.run(sm).withMaxActions(200).execute();
            conn.setAutoCommit(true);

            assertTrue(result.isSuccess(),
                    "BulkCopy should enforce CHECK constraints but silently bypasses them. "
                            + "INSERT BULK omits CHECK_CONSTRAINTS when copyOptions.checkConstraints=false (default).");
        }
    }

    /**
     * Regular batch path: CHECK constraints ARE enforced.
     *
     * Same random values [0, 200) are generated, but values > 100 cause
     * BatchUpdateException from the server. Invalid rows never reach the DB.
     * The ValidateCheckConstraintAction confirms all stored values are valid.
     *
     * This test SHOULD PASS — proving the model is correct and only the
     * BulkCopy code path has the bug.
     */
    @Test
    @DisplayName("Regular batch enforces CHECK constraints correctly")
    void testRegularBatchEnforcesCheckConstraints() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            ((ISQLServerConnection) conn).setUseBulkCopyForBatchInsert(false);
            cleanTable(conn);

            StateMachineTest sm = new StateMachineTest("RegularBatchCheck");
            DataCache cache = sm.getDataCache();
            cache.updateValue(0, CONN.key(), conn);
            cache.updateValue(0, AUTO_COMMIT.key(), true);
            cache.updateValue(0, CLOSED.key(), false);
            cache.updateValue(0, "nextId", 1);

            sm.addAction(new InsertRandomValueAction(30));
            sm.addAction(new ValidateCheckConstraintAction(25));
            sm.addAction(new SetAutoCommitFalseAction(10));
            sm.addAction(new SetAutoCommitTrueAction(8));
            sm.addAction(new CommitAction(12));
            sm.addAction(new RollbackAction(10));
            sm.addAction(new SelectCountAction(5));

            Result result = Engine.run(sm).withMaxActions(200).execute();
            conn.setAutoCommit(true);

            assertTrue(result.isSuccess(),
                    "Regular batch should enforce CHECK constraints correctly");
        }
    }

    // ACTION DEFINITIONS

    /**
     * Inserts a row with random value in [0, VALUE_RANGE). Values > CHECK_MAX
     * violate the CHECK constraint. BulkCopy inserts them silently; regular
     * batch rejects them with BatchUpdateException.
     */
    private static class InsertRandomValueAction extends Action {
        private int insertedId;
        private int insertedValue;
        private boolean wasInserted;

        InsertRandomValueAction(int weight) {
            super("insertRandomValue", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws Exception {
            Connection conn = (Connection) getState(CONN);
            int nextId = (Integer) dataCache.getValue(0, "nextId");
            insertedValue = getRandom().nextInt(VALUE_RANGE);
            insertedId = nextId;
            wasInserted = false;

            try (PreparedStatement pstmt = conn.prepareStatement(
                    "INSERT INTO " + TABLE_NAME + " (id, value) VALUES (?, ?)")) {
                pstmt.setInt(1, insertedId);
                pstmt.setInt(2, insertedValue);
                pstmt.addBatch();

                try {
                    pstmt.executeBatch();
                    wasInserted = true;

                    Map<String, Object> row = new HashMap<>();
                    row.put("id", insertedId);
                    row.put("value", insertedValue);
                    row.put("rowState", isState(AUTO_COMMIT) ? "committed" : "pending_insert");
                    dataCache.addRow(row);
                } catch (Exception e) {
                    wasInserted = false;
                }
            }
            dataCache.updateValue(0, "nextId", nextId + 1);

            System.out.println(String.format("INSERT id=%d value=%d %s%s",
                    insertedId, insertedValue,
                    wasInserted ? "OK" : "REJECTED",
                    insertedValue > CHECK_MAX ? " (violates CHECK)" : ""));
        }
    }

    /**
     * Validates the CHECK constraint invariant: no row in the DB should have
     * value outside [0, CHECK_MAX]. This is the detection point for the
     * BulkCopy bypass — it queries the DB directly for constraint violations.
     *
     * With BulkCopy: finds rows with value > 100 -> assertion failure.
     * With regular batch: all values in range -> passes.
     */
    private static class ValidateCheckConstraintAction extends Action {

        ValidateCheckConstraintAction(int weight) {
            super("validateCheck", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws Exception {
            // validation only
        }

        @Override
        public void validate() throws Exception {
            Connection conn = (Connection) getState(CONN);

            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT id, value FROM " + TABLE_NAME
                                    + " WHERE value < 0 OR value > " + CHECK_MAX)) {

                if (rs.next()) {
                    int id = rs.getInt("id");
                    int value = rs.getInt("value");
                    int count = 1;
                    while (rs.next())
                        count++;

                    throw new AssertionError(String.format(
                            "CHECK constraint bypassed! Found %d row(s) violating "
                                    + "CHECK (value >= 0 AND value <= %d). "
                                    + "First violation: id=%d, value=%d. "
                                    + "BulkCopy INSERT BULK omits CHECK_CONSTRAINTS by default "
                                    + "(SQLServerBulkCopyOptions.checkConstraints=false).",
                            count, CHECK_MAX, id, value));
                }
            }
            System.out.println("CHECK validated — all values in [0, " + CHECK_MAX + "]");
        }
    }

    /** Reports DB vs model row count for visibility. */
    private static class SelectCountAction extends Action {

        SelectCountAction(int weight) {
            super("selectCount", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED);
        }

        @Override
        public void run() throws Exception {
            Connection conn = (Connection) getState(CONN);
            int dbCount;
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                rs.next();
                dbCount = rs.getInt(1);
            }

            int modelCount = 0;
            for (int i = 1; i < dataCache.getRowCount(); i++) {
                String state = (String) dataCache.getValue(i, "rowState");
                if ("committed".equals(state) || "pending_insert".equals(state)) {
                    modelCount++;
                }
            }

            System.out.println(String.format("ROW COUNT: DB=%d, Model=%d", dbCount, modelCount));
        }
    }

    private static class SetAutoCommitFalseAction extends Action {

        SetAutoCommitFalseAction(int weight) {
            super("setAutoCommit(false)", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && isState(AUTO_COMMIT);
        }

        @Override
        public void run() throws Exception {
            Connection conn = (Connection) getState(CONN);
            conn.setAutoCommit(false);
            setState(AUTO_COMMIT, false);
            System.out.println("setAutoCommit(false)");
        }
    }

    private static class SetAutoCommitTrueAction extends Action {

        SetAutoCommitTrueAction(int weight) {
            super("setAutoCommit(true)", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && !isState(AUTO_COMMIT);
        }

        @Override
        public void run() throws Exception {
            Connection conn = (Connection) getState(CONN);
            conn.setAutoCommit(true);
            setState(AUTO_COMMIT, true);
            promoteAllPending(dataCache);
            System.out.println("setAutoCommit(true) — implicit commit");
        }
    }

    private static class CommitAction extends Action {

        CommitAction(int weight) {
            super("commit", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && !isState(AUTO_COMMIT);
        }

        @Override
        public void run() throws Exception {
            Connection conn = (Connection) getState(CONN);
            conn.commit();
            promoteAllPending(dataCache);
            System.out.println("commit");
        }
    }

    private static class RollbackAction extends Action {

        RollbackAction(int weight) {
            super("rollback", weight);
        }

        @Override
        public boolean canRun() {
            return !isState(CLOSED) && !isState(AUTO_COMMIT);
        }

        @Override
        public void run() throws Exception {
            Connection conn = (Connection) getState(CONN);
            conn.rollback();
            discardAllPending(dataCache);
            System.out.println("rollback");
        }
    }

    // UTILITIES

    private static void createTestTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            stmt.execute("CREATE TABLE " + TABLE_NAME
                    + " (id INT PRIMARY KEY, "
                    + "value INT CHECK (value >= 0 AND value <= " + CHECK_MAX + "))");
        }
    }

    private static void cleanTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM " + TABLE_NAME);
        }
    }

    private static void promoteAllPending(DataCache dc) {
        for (int i = 1; i < dc.getRowCount(); i++) {
            String rowState = (String) dc.getValue(i, "rowState");
            if ("pending_insert".equals(rowState)) {
                dc.updateValue(i, "rowState", "committed");
            }
        }
    }

    private static void discardAllPending(DataCache dc) {
        for (int i = 1; i < dc.getRowCount(); i++) {
            String rowState = (String) dc.getValue(i, "rowState");
            if ("pending_insert".equals(rowState)) {
                dc.updateValue(i, "rowState", "removed");
            }
        }
    }
}
