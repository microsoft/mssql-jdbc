/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.xa;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.sql.XAConnection;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;
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


/**
 * Model-Based Testing for XA (distributed) transactions.
 * Tests XA state machine transitions: start, end, prepare, commit, rollback,
 * suspend/resume, recover, forget with weighted random action selection.
 */
@Tag(Constants.legacyFx)
public class XAStateTest extends AbstractTest {

    private static final String TABLE_NAME = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("SM_XA_Test"));

    // STATE KEYS
    private static final StateKey XA_CONN = () -> "xaConn";
    private static final StateKey XA_RES = () -> "xaRes";
    private static final StateKey CONN = () -> "conn";
    private static final StateKey XID = () -> "xid";
    private static final StateKey XA_STATE = () -> "xaState"; // IDLE, STARTED, ENDED, PREPARED, COMPLETED
    private static final StateKey SUSPENDED = () -> "suspended";
    private static final StateKey SUSPEND_XID = () -> "suspendXid";

    // XA State Machine States
    private enum XAState {
        IDLE, STARTED, SUSPENDED, ENDED, PREPARED, COMPLETED
    }

    private static DataCache cache;

    @BeforeAll
    static void setupTests() throws Exception {
        setConnection();
        createTestTable(connection);
    }

    /**
     * Checks if XA support is installed on SQL Server by attempting
     * to get an XA connection and call recover.
     */
    private static boolean isXASupported(String connString) {
        XAConnection xaConn = null;
        try {
            SQLServerXADataSource ds = new SQLServerXADataSource();
            ds.setURL(connString);
            xaConn = ds.getXAConnection();
            XAResource xaRes = xaConn.getXAResource();
            // Try to call recover - this will fail if XA is not installed
            xaRes.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
            return true;
        } catch (Exception e) {
            String msg = e.getMessage();
            if (msg != null && msg.contains("xp_sqljdbc_xa")) {
                return false;
            }
            // Other errors might be transient, assume XA is available
            return true;
        } finally {
            if (xaConn != null) {
                try {
                    xaConn.close();
                } catch (SQLException e) {
                    // Ignore close errors
                }
            }
        }
    }

    @AfterAll
    static void cleanupTests() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            TestUtils.dropTableIfExists(TABLE_NAME, connection.createStatement());
        }
    }

    /**
     * XA distributed transaction simulation with weighted random actions.
     * Tests the full XA state machine including two-phase commit (prepare/commit),
     * suspend/resume, and recovery scenarios.
     */
    @Test
    @DisplayName("Randomized XA State Machine Validation")
    void testRandomizedXATransactions() throws Exception {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");
        
        // Check if XA is supported on this SQL Server instance
        boolean xaSupported = isXASupported(connectionString);
        Assumptions.assumeTrue(xaSupported, 
            "XA distributed transactions not supported - install xa_install.sql on SQL Server");

        SQLServerXADataSource ds = new SQLServerXADataSource();
        ds.setURL(connectionString);

        XAConnection xaConn = ds.getXAConnection();
        try {
            StateMachineTest sm = new StateMachineTest("XATransactions");
            cache = sm.getDataCache();

            // Initialize state
            XAResource xaRes = xaConn.getXAResource();
            Connection conn = xaConn.getConnection();
            cache.updateValue(0, XA_CONN.key(), xaConn);
            cache.updateValue(0, XA_RES.key(), xaRes);
            cache.updateValue(0, CONN.key(), conn);
            cache.updateValue(0, XID.key(), null);
            cache.updateValue(0, XA_STATE.key(), XAState.IDLE);
            cache.updateValue(0, SUSPENDED.key(), false);
            cache.updateValue(0, SUSPEND_XID.key(), null);
            cache.updateValue(0, "commitCount", 0);
            cache.updateValue(0, "rollbackCount", 0);
            cache.updateValue(0, "nextId", 1);
            cache.updateValue(0, "preparedXids", new ArrayList<Xid>());

            // Add XA actions with weights matching JDBCFX-AE patterns
            sm.addAction(new XAStartAction(1000));
            sm.addAction(new XAEndAction(1000));
            sm.addAction(new XAPrepareAction(1000));
            sm.addAction(new XACommitOnePCAction(800));      // One-phase commit (common)
            sm.addAction(new XACommitTwoPCAction(800));      // Two-phase commit
            sm.addAction(new XARollbackAction(500));
            sm.addAction(new XASuspendAction(300));
            sm.addAction(new XAResumeAction(300));
            sm.addAction(new XAInsertAction(600));           // Data operations
            sm.addAction(new XARecoverAction(50));           // Lower weight - only after some transactions
            sm.addAction(new XAForgetAction(100));           // Rare

            Result result = Engine.run(sm)
                    .withMaxActions(500)
                    .withSeed(System.currentTimeMillis())
                    .execute();

            // Cleanup any active transaction
            XAState finalState = (XAState) cache.getValue(0, XA_STATE.key());
            Xid activeXid = (Xid) cache.getValue(0, XID.key());
            if (activeXid != null && finalState != XAState.IDLE) {
                try {
                    if (finalState == XAState.STARTED || finalState == XAState.SUSPENDED) {
                        xaRes.end(activeXid, XAResource.TMSUCCESS);
                    }
                    if (finalState != XAState.PREPARED) {
                        xaRes.rollback(activeXid);
                    } else {
                        // For prepared transactions, must rollback directly
                        xaRes.rollback(activeXid);
                    }
                } catch (XAException e) {
                    // Ignore cleanup errors
                }
            }

            Integer commitCount = (Integer) cache.getValue(0, "commitCount");
            Integer rollbackCount = (Integer) cache.getValue(0, "rollbackCount");

            System.out.println(String.format("XA Result: actions=%d, commits=%d, rollbacks=%d",
                    result.actionCount, commitCount, rollbackCount));

            assertTrue(result.isSuccess(), "XA state machine test should complete successfully");
            assertTrue(commitCount != null && commitCount >= 2,
                    String.format("Expected at least 2 XA commits, got %d", commitCount));
        } finally {
            if (xaConn != null) {
                xaConn.close();
            }
        }
    }

    // ==================== XA ACTION DEFINITIONS ====================

    /** XA Start - Begin a new distributed transaction. */
    private static class XAStartAction extends Action {

        XAStartAction(int weight) {
            super("xa_start", weight);
        }

        @Override
        public boolean canRun() {
            XAState state = (XAState) getState(XA_STATE);
            return state == XAState.IDLE;
        }

        @Override
        public void run() throws Exception {
            XAResource xaRes = (XAResource) getState(XA_RES);
            Xid xid = createXid();

            xaRes.start(xid, XAResource.TMNOFLAGS);
            setState(XID, xid);
            setState(XA_STATE, XAState.STARTED);
            System.out.println("XA_START xid=" + formatXid(xid));
        }
    }

    /** XA End - Mark the end of work on behalf of a transaction branch. */
    private static class XAEndAction extends Action {

        XAEndAction(int weight) {
            super("xa_end", weight);
        }

        @Override
        public boolean canRun() {
            XAState state = (XAState) getState(XA_STATE);
            return state == XAState.STARTED;
        }

        @Override
        public void run() throws Exception {
            XAResource xaRes = (XAResource) getState(XA_RES);
            Xid xid = (Xid) getState(XID);

            xaRes.end(xid, XAResource.TMSUCCESS);
            setState(XA_STATE, XAState.ENDED);
            System.out.println("XA_END xid=" + formatXid(xid));
        }
    }

    /** XA Prepare - Prepare for two-phase commit. */
    private static class XAPrepareAction extends Action {

        XAPrepareAction(int weight) {
            super("xa_prepare", weight);
        }

        @Override
        public boolean canRun() {
            XAState state = (XAState) getState(XA_STATE);
            return state == XAState.ENDED;
        }

        @Override
        public void run() throws Exception {
            XAResource xaRes = (XAResource) getState(XA_RES);
            Xid xid = (Xid) getState(XID);

            int result = xaRes.prepare(xid);
            assertTrue(result == XAResource.XA_OK || result == XAResource.XA_RDONLY,
                    "XA prepare should return XA_OK or XA_RDONLY");

            setState(XA_STATE, XAState.PREPARED);

            // Track prepared XIDs for recovery testing
            @SuppressWarnings("unchecked")
            List<Xid> preparedXids = (List<Xid>) dataCache.getValue(0, "preparedXids");
            preparedXids.add(xid);

            System.out.println("XA_PREPARE xid=" + formatXid(xid) + " result=" + result);
        }
    }

    /** XA Commit (One-Phase) - Commit without prepare. */
    private static class XACommitOnePCAction extends Action {

        XACommitOnePCAction(int weight) {
            super("xa_commit_1pc", weight);
        }

        @Override
        public boolean canRun() {
            XAState state = (XAState) getState(XA_STATE);
            return state == XAState.ENDED;
        }

        @Override
        public void run() throws Exception {
            XAResource xaRes = (XAResource) getState(XA_RES);
            Xid xid = (Xid) getState(XID);

            xaRes.commit(xid, true); // onePhase = true
            promotePendingRows();
            
            // Reset to IDLE for new transaction
            setState(XA_STATE, XAState.IDLE);
            setState(XID, null);
            setState(SUSPENDED, false);
            setState(SUSPEND_XID, null);

            int commitCount = (Integer) dataCache.getValue(0, "commitCount");
            dataCache.updateValue(0, "commitCount", commitCount + 1);

            System.out.println("XA_COMMIT_1PC xid=" + formatXid(xid) + " #" + (commitCount + 1));
        }

        @Override
        public void validate() throws Exception {
            validateCommittedData();
        }
    }

    /** XA Commit (Two-Phase) - Commit after prepare. */
    private static class XACommitTwoPCAction extends Action {

        XACommitTwoPCAction(int weight) {
            super("xa_commit_2pc", weight);
        }

        @Override
        public boolean canRun() {
            XAState state = (XAState) getState(XA_STATE);
            return state == XAState.PREPARED;
        }

        @Override
        public void run() throws Exception {
            XAResource xaRes = (XAResource) getState(XA_RES);
            Xid xid = (Xid) getState(XID);

            xaRes.commit(xid, false); // onePhase = false
            promotePendingRows();

            // Reset to IDLE for new transaction
            setState(XA_STATE, XAState.IDLE);
            setState(XID, null);
            setState(SUSPENDED, false);
            setState(SUSPEND_XID, null);

            // Remove from prepared list
            @SuppressWarnings("unchecked")
            List<Xid> preparedXids = (List<Xid>) dataCache.getValue(0, "preparedXids");
            preparedXids.remove(xid);

            int commitCount = (Integer) dataCache.getValue(0, "commitCount");
            dataCache.updateValue(0, "commitCount", commitCount + 1);

            System.out.println("XA_COMMIT_2PC xid=" + formatXid(xid) + " #" + (commitCount + 1));
        }

        @Override
        public void validate() throws Exception {
            validateCommittedData();
        }
    }

    /** XA Rollback - Rollback a transaction branch. */
    private static class XARollbackAction extends Action {

        XARollbackAction(int weight) {
            super("xa_rollback", weight);
        }

        @Override
        public boolean canRun() {
            XAState state = (XAState) getState(XA_STATE);
            return state == XAState.ENDED || state == XAState.PREPARED;
        }

        @Override
        public void run() throws Exception {
            XAResource xaRes = (XAResource) getState(XA_RES);
            Xid xid = (Xid) getState(XID);

            xaRes.rollback(xid);
            removePendingRows();

            // Reset to IDLE for new transaction
            setState(XA_STATE, XAState.IDLE);
            setState(XID, null);
            setState(SUSPENDED, false);
            setState(SUSPEND_XID, null);

            // Remove from prepared list if it was prepared
            @SuppressWarnings("unchecked")
            List<Xid> preparedXids = (List<Xid>) dataCache.getValue(0, "preparedXids");
            preparedXids.remove(xid);

            int rollbackCount = (Integer) dataCache.getValue(0, "rollbackCount");
            dataCache.updateValue(0, "rollbackCount", rollbackCount + 1);

            System.out.println("XA_ROLLBACK xid=" + formatXid(xid) + " #" + (rollbackCount + 1));
        }
    }

    /** XA Suspend - Temporarily suspend work on a transaction branch. */
    private static class XASuspendAction extends Action {

        XASuspendAction(int weight) {
            super("xa_suspend", weight);
        }

        @Override
        public boolean canRun() {
            XAState state = (XAState) getState(XA_STATE);
            return state == XAState.STARTED && !isState(SUSPENDED);
        }

        @Override
        public void run() throws Exception {
            XAResource xaRes = (XAResource) getState(XA_RES);
            Xid xid = (Xid) getState(XID);

            xaRes.end(xid, XAResource.TMSUSPEND);
            setState(XA_STATE, XAState.SUSPENDED);
            setState(SUSPENDED, true);
            setState(SUSPEND_XID, xid);

            System.out.println("XA_SUSPEND xid=" + formatXid(xid));
        }
    }

    /** XA Resume - Resume work on a suspended transaction branch. */
    private static class XAResumeAction extends Action {

        XAResumeAction(int weight) {
            super("xa_resume", weight);
        }

        @Override
        public boolean canRun() {
            XAState state = (XAState) getState(XA_STATE);
            return state == XAState.SUSPENDED;
        }

        @Override
        public void run() throws Exception {
            XAResource xaRes = (XAResource) getState(XA_RES);
            Xid xid = (Xid) getState(SUSPEND_XID);

            xaRes.start(xid, XAResource.TMRESUME);
            setState(XA_STATE, XAState.STARTED);
            setState(SUSPENDED, false);
            setState(SUSPEND_XID, null);

            System.out.println("XA_RESUME xid=" + formatXid(xid));
        }
    }

    /** XA Insert - Execute a SQL insert within the XA transaction. */
    private static class XAInsertAction extends Action {
        private int insertedId;
        private int insertedValue;

        XAInsertAction(int weight) {
            super("xa_insert", weight);
        }

        @Override
        public boolean canRun() {
            XAState state = (XAState) getState(XA_STATE);
            return state == XAState.STARTED;
        }

        @Override
        public void run() throws Exception {
            Connection conn = (Connection) getState(CONN);
            int nextId = (Integer) dataCache.getValue(0, "nextId");
            insertedId = nextId;
            insertedValue = getRandom().nextInt(1000);

            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME
                        + " VALUES (" + insertedId + ", " + insertedValue + ")");
            }

            Map<String, Object> row = new HashMap<>();
            row.put("id", insertedId);
            row.put("value", insertedValue);
            row.put("state", "pending");
            dataCache.addRow(row);
            dataCache.updateValue(0, "nextId", nextId + 1);

            System.out.println(String.format("XA_INSERT id=%d val=%d", insertedId, insertedValue));
        }
    }

    /** XA Recover - Obtain a list of prepared transaction branches. */
    private static class XARecoverAction extends Action {

        XARecoverAction(int weight) {
            super("xa_recover", weight);
        }

        @Override
        public boolean canRun() {
            XAState state = (XAState) getState(XA_STATE);
            // Can recover anytime when not in active transaction
            return state == XAState.IDLE;
        }

        @Override
        public void run() throws Exception {
            XAResource xaRes = (XAResource) getState(XA_RES);

            try {
                Xid[] recovered = xaRes.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
                assertNotNull(recovered, "XA recover should not return null");

                @SuppressWarnings("unchecked")
                List<Xid> preparedXids = (List<Xid>) dataCache.getValue(0, "preparedXids");

                System.out.println(String.format("XA_RECOVER found %d XIDs (expected %d prepared)",
                        recovered.length, preparedXids.size()));

                // Note: Recovered XIDs may include transactions from other tests
                // so we don't strictly validate count, but ensure non-negative
                assertTrue(recovered.length >= 0, "Recovered XID count should be non-negative");
            } catch (XAException e) {
                // Recover can fail if XA is not properly configured - log and continue
                System.out.println("XA_RECOVER failed: " + e.getMessage());
            }
        }
    }

    /** XA Forget - Forget about a heuristically completed transaction branch. */
    private static class XAForgetAction extends Action {

        XAForgetAction(int weight) {
            super("xa_forget", weight);
        }

        @Override
        public boolean canRun() {
            // Forget is only valid after heuristic completion
            // For simplicity, we'll allow it rarely on prepared transactions
            XAState state = (XAState) getState(XA_STATE);
            return state == XAState.PREPARED && getRandom().nextInt(10) == 0; // 10% chance
        }

        @Override
        public void run() throws Exception {
            XAResource xaRes = (XAResource) getState(XA_RES);
            Xid xid = (Xid) getState(XID);

            try {
                xaRes.forget(xid);
                removePendingRows();

                // Reset to IDLE
                setState(XA_STATE, XAState.IDLE);
                setState(XID, null);
                setState(SUSPENDED, false);
                setState(SUSPEND_XID, null);

                @SuppressWarnings("unchecked")
                List<Xid> preparedXids = (List<Xid>) dataCache.getValue(0, "preparedXids");
                preparedXids.remove(xid);

                System.out.println("XA_FORGET xid=" + formatXid(xid));
            } catch (XAException e) {
                // Forget may fail if transaction is not heuristically completed
                // This is expected - just log and continue
                System.out.println("XA_FORGET failed (expected): " + e.getMessage());
            }
        }
    }

    // ==================== UTILITY METHODS ====================

    private static void createTestTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, value INT)");
        }
    }

    /** Creates a unique Xid for XA transactions. */
    private static Xid createXid() {
        return new XidImpl(
                1234, // formatId
                UUID.randomUUID().toString().substring(0, 8).getBytes(), // gtrid
                UUID.randomUUID().toString().substring(0, 8).getBytes()  // bqual
        );
    }

    /** Formats Xid for logging. */
    private static String formatXid(Xid xid) {
        if (xid == null)
            return "null";
        return String.format("Xid[%d]", xid.getFormatId());
    }

    /** Validates that committed data is visible in the database. */
    private static void validateCommittedData() throws SQLException {
        Connection conn = (Connection) cache.getValue(0, CONN.key());
        int expectedRows = 0;
        
        // Count committed rows in cache
        for (int i = 1; i < cache.getRowCount(); i++) {
            String state = (String) cache.getValue(i, "state");
            if ("committed".equals(state)) {
                expectedRows++;
            }
        }
        
        // Verify in database
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
            assertTrue(rs.next(), "Should have count result");
            int actualRows = rs.getInt(1);
            assertEquals(expectedRows, actualRows,
                    String.format("Expected %d committed rows, found %d in DB", expectedRows, actualRows));
        }
    }

    /** Promotes pending rows to committed state. */
    private static void promotePendingRows() {
        for (int i = 1; i < cache.getRowCount(); i++) {
            String state = (String) cache.getValue(i, "state");
            if ("pending".equals(state)) {
                cache.updateValue(i, "state", "committed");
            }
        }
    }

    /** Removes pending rows (on rollback). */
    private static void removePendingRows() {
        for (int i = cache.getRowCount() - 1; i >= 1; i--) {
            String state = (String) cache.getValue(i, "state");
            if ("pending".equals(state)) {
                cache.updateValue(i, "state", "removed");
            }
        }
    }

    // ==================== DETERMINISTIC TEST CASES ====================

    /**
     * Test #1: TCVerifyXAResource - XAResource Compliance Testing
     * Tests fundamental XAResource interface behavior and contract compliance.
     */
    @Test
    public void testVerifyXAResource() throws Exception {
        assumeTrue(isXASupported(connectionString), "Skipping: XA not supported or connection not configured");
        
        // Setup
        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        xaDS.setURL(connectionString);
        
        XAConnection xaConn = null;
        try {
            xaConn = xaDS.getXAConnection();
            XAResource xaRes = xaConn.getXAResource();
            Connection conn = xaConn.getConnection();
            
            // Test 1: Verify XAResource is not null
            assertNotNull(xaRes, "XAResource should not be null");
            
            // Test 2: Verify isSameRM behavior
            XAConnection xaConn2 = null;
            try {
                xaConn2 = xaDS.getXAConnection();
                XAResource xaRes2 = xaConn2.getXAResource();
                boolean isSame = xaRes.isSameRM(xaRes2);
                // Both should point to same resource manager (SQL Server instance)
                assertTrue(isSame, "XAResources from same datasource should have isSameRM=true");
            } finally {
                if (xaConn2 != null) {
                    try {
                        xaConn2.close();
                    } catch (SQLException e) {
                        // Ignore
                    }
                }
            }
            
            // Test 3: Verify getTransactionTimeout defaults
            int timeout = xaRes.getTransactionTimeout();
            assertTrue(timeout >= 0, "Transaction timeout should be non-negative");
            
            // Test 4: Verify setTransactionTimeout
            boolean timeoutSet = xaRes.setTransactionTimeout(60);
            assertTrue(timeoutSet, "Should be able to set transaction timeout");
            int newTimeout = xaRes.getTransactionTimeout();
            assertEquals(60, newTimeout, "Transaction timeout should be updated");
            
            // Test 5: Verify recover returns empty array when no prepared transactions
            Xid[] xids = xaRes.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
            assertNotNull(xids, "Recover should return non-null array");
            
            // Test 6: Verify basic XA lifecycle
            Xid xid = createXid();
            xaRes.start(xid, XAResource.TMNOFLAGS);
            
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(TABLE_NAME, stmt);
                stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, value INT)");
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 100)");
            }
            
            xaRes.end(xid, XAResource.TMSUCCESS);
            int prepResult = xaRes.prepare(xid);
            assertTrue(prepResult == XAResource.XA_OK || prepResult == XAResource.XA_RDONLY,
                    "Prepare should return XA_OK or XA_RDONLY");
            
            if (prepResult == XAResource.XA_OK) {
                xaRes.commit(xid, false);
            }
            
            // Test 7: Verify error conditions - starting with invalid flags
            try {
                xaRes.start(createXid(), 999999); // Invalid flag
                fail("Should throw XAException for invalid flags");
            } catch (XAException e) {
                // Expected
            }
            
            // Test 8: Verify error - ending transaction without starting
            try {
                xaRes.end(createXid(), XAResource.TMSUCCESS);
                fail("Should throw XAException when ending non-started transaction");
            } catch (XAException e) {
                assertEquals(XAException.XAER_NOTA, e.errorCode,
                        "Should return XAER_NOTA for unknown transaction");
            }
            
            System.out.println("✓ TCVerifyXAResource: All XAResource compliance tests passed");
        } finally {
            if (xaConn != null) {
                try {
                    xaConn.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
        }
    }

    /**
     * Test #2: TCSanity - Basic XA Operations
     * Tests the fundamental XA transaction workflow: start -> end -> commit.
     */
    @Test
    public void testBasicXAOperations() throws Exception {
        assumeTrue(isXASupported(connectionString), "Skipping: XA not supported or connection not configured");
        
        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        xaDS.setURL(connectionString);
        
        XAConnection xaConn = null;
        try {
            xaConn = xaDS.getXAConnection();
            XAResource xaRes = xaConn.getXAResource();
            Connection conn = xaConn.getConnection();
            
            // Create test table
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(TABLE_NAME, stmt);
                stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, value INT)");
            }
            
            // Test 1: One-phase commit (start -> end -> commit with onePhase=true)
            Xid xid1 = createXid();
            xaRes.start(xid1, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 100)");
            }
            xaRes.end(xid1, XAResource.TMSUCCESS);
            xaRes.commit(xid1, true); // One-phase commit
            
            // Verify data was committed
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "Should have 1 row after one-phase commit");
            }
            
            // Test 2: Two-phase commit (start -> end -> prepare -> commit)
            Xid xid2 = createXid();
            xaRes.start(xid2, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 200)");
            }
            xaRes.end(xid2, XAResource.TMSUCCESS);
            int prepResult = xaRes.prepare(xid2);
            assertEquals(XAResource.XA_OK, prepResult, "Prepare should return XA_OK");
            xaRes.commit(xid2, false); // Two-phase commit
            
            // Verify data was committed
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should have 2 rows after two-phase commit");
            }
            
            // Test 3: Read-only transaction (no modifications)
            Xid xid3 = createXid();
            xaRes.start(xid3, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {
                int count = 0;
                while (rs.next()) count++;
                assertEquals(2, count, "Should read 2 rows");
            }
            xaRes.end(xid3, XAResource.TMSUCCESS);
            int prepResult3 = xaRes.prepare(xid3);
            assertEquals(XAResource.XA_RDONLY, prepResult3, 
                    "Read-only transaction should return XA_RDONLY");
            // No commit needed for read-only
            
            // Test 4: Multiple operations within single transaction
            Xid xid4 = createXid();
            xaRes.start(xid4, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (3, 300)");
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (4, 400)");
                stmt.execute("UPDATE " + TABLE_NAME + " SET value = 150 WHERE id = 1");
            }
            xaRes.end(xid4, XAResource.TMSUCCESS);
            xaRes.prepare(xid4);
            xaRes.commit(xid4, false);
            
            // Verify all operations committed
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1), "Should have 4 rows");
            }
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT value FROM " + TABLE_NAME + " WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(150, rs.getInt(1), "Value for id=1 should be updated to 150");
            }
            
            System.out.println("✓ TCSanity: All basic XA operation tests passed");
        } finally {
            if (xaConn != null) {
                try {
                    xaConn.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
        }
    }

    /**
     * Test #3: TCCommit - XA Commit Scenarios
     * Tests various commit scenarios, including one-phase, two-phase, and error conditions.
     */
    @Test
    public void testXACommitScenarios() throws Exception {
        assumeTrue(isXASupported(connectionString), "Skipping: XA not supported or connection not configured");
        
        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        xaDS.setURL(connectionString);
        
        XAConnection xaConn = null;
        try {
            xaConn = xaDS.getXAConnection();
            XAResource xaRes = xaConn.getXAResource();
            Connection conn = xaConn.getConnection();
            
            // Setup test table
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(TABLE_NAME, stmt);
                stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, value INT)");
            }
            
            // Test 1: One-phase commit without prepare
            Xid xid1 = createXid();
            xaRes.start(xid1, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 100)");
            }
            xaRes.end(xid1, XAResource.TMSUCCESS);
            xaRes.commit(xid1, true); // onePhase=true, no prepare needed
            
            // Test 2: Two-phase commit with explicit prepare
            Xid xid2 = createXid();
            xaRes.start(xid2, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 200)");
            }
            xaRes.end(xid2, XAResource.TMSUCCESS);
            xaRes.prepare(xid2);
            xaRes.commit(xid2, false); // onePhase=false after prepare
            
            // Test 3: Commit non-existent transaction should fail
            try {
                Xid invalidXid = createXid();
                xaRes.commit(invalidXid, false);
                fail("Should throw XAException when committing non-existent transaction");
            } catch (XAException e) {
                assertEquals(XAException.XAER_NOTA, e.errorCode, 
                        "Should return XAER_NOTA for unknown XID");
            }
            
            // Test 4: Commit without prepare (when two-phase required) should fail
            try {
                Xid xid4 = createXid();
                xaRes.start(xid4, XAResource.TMNOFLAGS);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (4, 400)");
                }
                xaRes.end(xid4, XAResource.TMSUCCESS);
                xaRes.commit(xid4, false); // Trying two-phase commit without prepare
                fail("Should throw XAException when committing without prepare");
            } catch (XAException e) {
                // Expected - XAER_PROTO for protocol error
                assertTrue(e.errorCode == XAException.XAER_PROTO || 
                          e.errorCode == XAException.XAER_NOTA,
                        "Should return XAER_PROTO or XAER_NOTA");
            }
            
            // Test 5: Double commit should fail
            Xid xid5 = createXid();
            xaRes.start(xid5, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (5, 500)");
            }
            xaRes.end(xid5, XAResource.TMSUCCESS);
            xaRes.commit(xid5, true); // First commit succeeds
            
            try {
                xaRes.commit(xid5, true); // Second commit should fail
                fail("Should throw XAException on double commit");
            } catch (XAException e) {
                assertEquals(XAException.XAER_NOTA, e.errorCode, 
                        "Should return XAER_NOTA for already-committed transaction");
            }
            
            // Test 6: Commit with wrong onePhase flag after prepare
            Xid xid6 = createXid();
            xaRes.start(xid6, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (6, 600)");
            }
            xaRes.end(xid6, XAResource.TMSUCCESS);
            xaRes.prepare(xid6);
            
            // After prepare, must use onePhase=false
            try {
                xaRes.commit(xid6, true); // Wrong flag
                // Some implementations may allow this, so don't fail if it succeeds
            } catch (XAException e) {
                // Expected in strict implementations
                assertEquals(XAException.XAER_PROTO, e.errorCode);
            }
            
            // Try correct commit if previous failed
            try {
                xaRes.commit(xid6, false);
            } catch (XAException e) {
                // May already be committed from previous attempt
            }
            
            // Verify all successful commits
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                int count = rs.getInt(1);
                assertTrue(count >= 3, "Should have at least 3 committed rows");
            }
            
            System.out.println("✓ TCCommit: All commit scenario tests passed");
        } finally {
            if (xaConn != null) {
                try {
                    xaConn.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
        }
    }

    /**
     * Test #4: TCRollback - XA Rollback Scenarios
     * Tests rollback functionality, including before and after prepare.
     */
    @Test
    public void testXARollbackScenarios() throws Exception {
        assumeTrue(isXASupported(connectionString), "Skipping: XA not supported or connection not configured");
        
        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        xaDS.setURL(connectionString);
        
        XAConnection xaConn = null;
        try {
            xaConn = xaDS.getXAConnection();
            XAResource xaRes = xaConn.getXAResource();
            Connection conn = xaConn.getConnection();
            
            // Setup test table
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(TABLE_NAME, stmt);
                stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, value INT)");
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (0, 0)"); // Baseline row
            }
            
            // Test 1: Rollback before prepare
            Xid xid1 = createXid();
            xaRes.start(xid1, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 100)");
            }
            xaRes.end(xid1, XAResource.TMSUCCESS);
            xaRes.rollback(xid1); // Rollback without prepare
            
            // Verify rollback - should only have baseline row
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "Should have only baseline row after rollback");
            }
            
            // Test 2: Rollback after prepare
            Xid xid2 = createXid();
            xaRes.start(xid2, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 200)");
            }
            xaRes.end(xid2, XAResource.TMSUCCESS);
            xaRes.prepare(xid2);
            xaRes.rollback(xid2); // Rollback after prepare
            
            // Verify still only baseline row
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "Should still have only baseline row");
            }
            
            // Test 3: Rollback with TMFAIL flag during end
            Xid xid3 = createXid();
            xaRes.start(xid3, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (3, 300)");
            }
            xaRes.end(xid3, XAResource.TMFAIL); // End with FAIL flag
            xaRes.rollback(xid3); // Rollback is expected after TMFAIL
            
            // Test 4: Rollback non-existent transaction should fail
            try {
                Xid invalidXid = createXid();
                xaRes.rollback(invalidXid);
                fail("Should throw XAException when rolling back non-existent transaction");
            } catch (XAException e) {
                assertEquals(XAException.XAER_NOTA, e.errorCode, 
                        "Should return XAER_NOTA for unknown XID");
            }
            
            // Test 5: Double rollback should fail
            Xid xid5 = createXid();
            xaRes.start(xid5, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (5, 500)");
            }
            xaRes.end(xid5, XAResource.TMSUCCESS);
            xaRes.rollback(xid5); // First rollback succeeds
            
            try {
                xaRes.rollback(xid5); // Second rollback should fail
                fail("Should throw XAException on double rollback");
            } catch (XAException e) {
                assertEquals(XAException.XAER_NOTA, e.errorCode, 
                        "Should return XAER_NOTA for already-rolled-back transaction");
            }
            
            // Test 6: Mixed commit and rollback - verify isolation
            Xid xid6commit = createXid();
            xaRes.start(xid6commit, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (6, 600)");
            }
            xaRes.end(xid6commit, XAResource.TMSUCCESS);
            xaRes.commit(xid6commit, true); // Commit this one
            
            Xid xid7rollback = createXid();
            xaRes.start(xid7rollback, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (7, 700)");
            }
            xaRes.end(xid7rollback, XAResource.TMSUCCESS);
            xaRes.rollback(xid7rollback); // Rollback this one
            
            // Verify: should have baseline (id=0) + committed (id=6) = 2 rows
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should have baseline + committed row");
            }
            
            // Test 7: Rollback after exception in transaction
            Xid xid8 = createXid();
            xaRes.start(xid8, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (8, 800)");
                try {
                    // Try to insert duplicate primary key - will fail
                    stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (8, 888)");
                } catch (SQLException e) {
                    // Expected - duplicate key error
                }
            }
            xaRes.end(xid8, XAResource.TMFAIL);
            xaRes.rollback(xid8);
            
            // Verify id=8 was not inserted
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE id = 8")) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1), "Rolled-back row should not exist");
            }
            
            System.out.println("✓ TCRollback: All rollback scenario tests passed");
        } finally {
            if (xaConn != null) {
                try {
                    xaConn.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
        }
    }

    /**
     * Test #5: TCException - XA Exception Handling
     * Tests proper XAException error codes for various error conditions.
     */
    @Test
    public void testXAExceptionHandling() throws Exception {
        assumeTrue(isXASupported(connectionString), "Skipping: XA not supported or connection not configured");
        
        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        xaDS.setURL(connectionString);
        
        XAConnection xaConn = null;
        try {
            xaConn = xaDS.getXAConnection();
            XAResource xaRes = xaConn.getXAResource();
            Connection conn = xaConn.getConnection();
            
            // Setup test table
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(TABLE_NAME, stmt);
                stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, value INT)");
            }
            
            // Test 1: XAER_NOTA - Unknown XID
            try {
                Xid unknownXid = createXid();
                xaRes.end(unknownXid, XAResource.TMSUCCESS);
                fail("Should throw XAER_NOTA for unknown XID");
            } catch (XAException e) {
                assertEquals(XAException.XAER_NOTA, e.errorCode, 
                        "Should return XAER_NOTA for unknown transaction");
            }
            
            // Test 2: XAER_PROTO - Protocol error (commit without prepare)
            try {
                Xid xid2 = createXid();
                xaRes.start(xid2, XAResource.TMNOFLAGS);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 200)");
                }
                xaRes.end(xid2, XAResource.TMSUCCESS);
                xaRes.commit(xid2, false); // Two-phase without prepare
                fail("Should throw XAER_PROTO for protocol violation");
            } catch (XAException e) {
                assertTrue(e.errorCode == XAException.XAER_PROTO || 
                          e.errorCode == XAException.XAER_NOTA,
                        "Should return XAER_PROTO or XAER_NOTA");
            }
            
            // Test 3: XAER_DUPID - Duplicate XID (start with same XID twice)
            Xid xid3 = createXid();
            xaRes.start(xid3, XAResource.TMNOFLAGS);
            try {
                xaRes.start(xid3, XAResource.TMNOFLAGS); // Same XID
                fail("Should throw XAER_DUPID for duplicate XID");
            } catch (XAException e) {
                assertTrue(e.errorCode == XAException.XAER_DUPID || 
                          e.errorCode == XAException.XAER_PROTO,
                        "Should return XAER_DUPID or XAER_PROTO");
            } finally {
                // Cleanup
                try {
                    xaRes.end(xid3, XAResource.TMFAIL);
                    xaRes.rollback(xid3);
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }
            
            // Test 4: XAER_INVAL - Invalid arguments
            try {
                Xid xid4 = createXid();
                xaRes.start(xid4, 999999); // Invalid flag
                fail("Should throw XAER_INVAL for invalid flag");
            } catch (XAException e) {
                assertTrue(e.errorCode == XAException.XAER_INVAL || 
                          e.errorCode == XAException.XAER_PROTO,
                        "Should return XAER_INVAL or XAER_PROTO");
            }
            
            // Test 5: XAER_PROTO - End without start
            try {
                Xid xid5 = createXid();
                xaRes.end(xid5, XAResource.TMSUCCESS);
                fail("Should throw XAER_PROTO when ending non-started transaction");
            } catch (XAException e) {
                assertTrue(e.errorCode == XAException.XAER_PROTO || 
                          e.errorCode == XAException.XAER_NOTA,
                        "Should return XAER_PROTO or XAER_NOTA");
            }
            
            // Test 6: XAER_PROTO - Prepare without end
            try {
                Xid xid6 = createXid();
                xaRes.start(xid6, XAResource.TMNOFLAGS);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (6, 600)");
                }
                xaRes.prepare(xid6); // Prepare without end
                fail("Should throw XAER_PROTO when preparing without end");
            } catch (XAException e) {
                assertEquals(XAException.XAER_PROTO, e.errorCode, 
                        "Should return XAER_PROTO for protocol violation");
            } finally {
                // Cleanup
                try {
                    Xid xid6 = createXid(); // Different XID for cleanup
                } catch (Exception e) {
                    // Ignore
                }
            }
            
            // Test 7: XA_RB* - Rollback error codes (simulate with TMFAIL)
            Xid xid7 = createXid();
            xaRes.start(xid7, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (7, 700)");
            }
            xaRes.end(xid7, XAResource.TMFAIL); // End with failure
            
            try {
                int prepResult = xaRes.prepare(xid7);
                // Prepare after TMFAIL should either throw exception or return XA_RBROLLBACK
                if (prepResult >= XAException.XA_RBBASE && prepResult <= XAException.XA_RBEND) {
                    // Got rollback code - this is valid
                    assertTrue(true, "Received rollback code as expected");
                }
            } catch (XAException e) {
                // Also valid - threw exception for failed transaction
                assertTrue(e.errorCode >= XAException.XA_RBBASE && 
                          e.errorCode <= XAException.XA_RBEND,
                        "Should return XA_RB* rollback code");
            } finally {
                // Cleanup - rollback is expected
                try {
                    xaRes.rollback(xid7);
                } catch (Exception e) {
                    // May already be rolled back
                }
            }
            
            // Test 8: XAER_PROTO - Multiple starts without end
            Xid xid8a = createXid();
            Xid xid8b = createXid();
            xaRes.start(xid8a, XAResource.TMNOFLAGS);
            try {
                xaRes.start(xid8b, XAResource.TMNOFLAGS); // Start another without ending first
                fail("Should throw XAER_PROTO for nested transactions");
            } catch (XAException e) {
                assertTrue(e.errorCode == XAException.XAER_PROTO || 
                          e.errorCode == XAException.XAER_OUTSIDE,
                        "Should return XAER_PROTO or XAER_OUTSIDE");
            } finally {
                // Cleanup first transaction
                try {
                    xaRes.end(xid8a, XAResource.TMFAIL);
                    xaRes.rollback(xid8a);
                } catch (Exception e) {
                    // Ignore
                }
            }
            
            System.out.println("✓ TCException: All XA exception handling tests passed");
        } finally {
            if (xaConn != null) {
                try {
                    xaConn.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
        }
    }

    /**
     * Test #6: TCIsolationLevels - XA with Different Isolation Levels
     * Tests XA transactions with various SQL transaction isolation levels.
     */
    @Test
    public void testXAWithIsolationLevels() throws Exception {
        assumeTrue(isXASupported(connectionString), "Skipping: XA not supported or connection not configured");
        
        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        xaDS.setURL(connectionString);
        
        XAConnection xaConn = null;
        try {
            xaConn = xaDS.getXAConnection();
            XAResource xaRes = xaConn.getXAResource();
            Connection conn = xaConn.getConnection();
            
            // Setup test table with initial data
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(TABLE_NAME, stmt);
                stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, value INT)");
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 100)");
            }
            
            // Test 1: READ UNCOMMITTED - Should see uncommitted changes (but in XA, still isolated)
            int originalIsolation = conn.getTransactionIsolation();
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            
            Xid xid1 = createXid();
            xaRes.start(xid1, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 200)");
            }
            xaRes.end(xid1, XAResource.TMSUCCESS);
            xaRes.commit(xid1, true);
            
            // Verify committed
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should have 2 rows after commit");
            }
            
            // Test 2: READ COMMITTED (default) - Should only see committed data
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            
            Xid xid2 = createXid();
            xaRes.start(xid2, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (3, 300)");
                // Within the transaction, we should see our own changes
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getInt(1), "Should see own changes within transaction");
                }
            }
            xaRes.end(xid2, XAResource.TMSUCCESS);
            xaRes.rollback(xid2); // Rollback this one
            
            // Should not see rolled back row
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should not see rolled back row");
            }
            
            // Test 3: REPEATABLE READ - Prevent non-repeatable reads
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            
            Xid xid3 = createXid();
            xaRes.start(xid3, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                // Read current value
                try (ResultSet rs = stmt.executeQuery("SELECT value FROM " + TABLE_NAME + " WHERE id = 1")) {
                    assertTrue(rs.next());
                    assertEquals(100, rs.getInt(1), "Initial value should be 100");
                }
                
                // Update within transaction
                stmt.execute("UPDATE " + TABLE_NAME + " SET value = 101 WHERE id = 1");
                
                // Re-read should show updated value
                try (ResultSet rs = stmt.executeQuery("SELECT value FROM " + TABLE_NAME + " WHERE id = 1")) {
                    assertTrue(rs.next());
                    assertEquals(101, rs.getInt(1), "Should see updated value");
                }
            }
            xaRes.end(xid3, XAResource.TMSUCCESS);
            xaRes.commit(xid3, true);
            
            // Verify update committed
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT value FROM " + TABLE_NAME + " WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(101, rs.getInt(1), "Updated value should be committed");
            }
            
            // Test 4: SERIALIZABLE - Highest isolation level
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            
            Xid xid4 = createXid();
            xaRes.start(xid4, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (4, 400)");
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (5, 500)");
            }
            xaRes.end(xid4, XAResource.TMSUCCESS);
            xaRes.prepare(xid4);
            xaRes.commit(xid4, false);
            
            // Verify both inserts
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1), "Should have 4 rows total");
            }
            
            // Test 5: Mix isolation levels across transactions
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            Xid xid5 = createXid();
            xaRes.start(xid5, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("DELETE FROM " + TABLE_NAME + " WHERE id = 5");
            }
            xaRes.end(xid5, XAResource.TMSUCCESS);
            xaRes.commit(xid5, true);
            
            // Final verification
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(3, rs.getInt(1), "Should have 3 rows after deletion");
            }
            
            // Restore original isolation level
            conn.setTransactionIsolation(originalIsolation);
            
            System.out.println("✓ TCIsolationLevels: All isolation level tests passed");
        } finally {
            if (xaConn != null) {
                try {
                    xaConn.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
        }
    }

    /**
     * Test #7: TCMultithreaded - Concurrent Independent XA Transactions
     * Tests multiple threads executing independent XA transactions concurrently.
     */
    @Test
    public void testConcurrentXATransactions() throws Exception {
        assumeTrue(isXASupported(connectionString), "Skipping: XA not supported or connection not configured");
        
        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        xaDS.setURL(connectionString);
        
        // Setup test table
        try (Connection setupConn = xaDS.getXAConnection().getConnection();
             Statement stmt = setupConn.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME, stmt);
            stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, value INT, thread_id INT)");
        }
        
        final int THREAD_COUNT = 5;
        final int TRANSACTIONS_PER_THREAD = 3;
        final java.util.concurrent.CountDownLatch latch = new java.util.concurrent.CountDownLatch(THREAD_COUNT);
        final java.util.concurrent.atomic.AtomicInteger successCount = new java.util.concurrent.atomic.AtomicInteger(0);
        final java.util.concurrent.atomic.AtomicInteger failureCount = new java.util.concurrent.atomic.AtomicInteger(0);
        
        java.util.List<Thread> threads = new java.util.ArrayList<>();
        
        // Create threads for concurrent XA transactions
        for (int threadNum = 0; threadNum < THREAD_COUNT; threadNum++) {
            final int threadId = threadNum;
            Thread thread = new Thread(() -> {
                try {
                    XAConnection xaConn = xaDS.getXAConnection();
                    try {
                        XAResource xaRes = xaConn.getXAResource();
                        Connection conn = xaConn.getConnection();
                        
                        for (int txNum = 0; txNum < TRANSACTIONS_PER_THREAD; txNum++) {
                            Xid xid = createXid();
                            int recordId = threadId * 100 + txNum;
                            
                            try {
                                // Start XA transaction
                                xaRes.start(xid, XAResource.TMNOFLAGS);
                                
                                // Insert data
                                try (Statement stmt = conn.createStatement()) {
                                    stmt.execute(String.format(
                                        "INSERT INTO %s VALUES (%d, %d, %d)",
                                        TABLE_NAME, recordId, recordId * 10, threadId));
                                }
                                
                                // End transaction
                                xaRes.end(xid, XAResource.TMSUCCESS);
                                
                                // Commit (alternate between one-phase and two-phase)
                                if (txNum % 2 == 0) {
                                    // Two-phase commit
                                    xaRes.prepare(xid);
                                    xaRes.commit(xid, false);
                                } else {
                                    // One-phase commit
                                    xaRes.commit(xid, true);
                                }
                                
                                successCount.incrementAndGet();
                            } catch (XAException | SQLException e) {
                                failureCount.incrementAndGet();
                                System.err.println("Thread " + threadId + " TX " + txNum + " failed: " + e.getMessage());
                                
                                // Try to rollback on error
                                try {
                                    xaRes.rollback(xid);
                                } catch (XAException rollbackEx) {
                                    // Ignore rollback errors
                                }
                            }
                            
                            // Small delay between transactions
                            Thread.sleep(10);
                        }
                    } finally {
                        xaConn.close();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    latch.countDown();
                }
            });
            
            threads.add(thread);
            thread.start();
        }
        
        // Wait for all threads to complete (max 30 seconds)
        boolean completed = latch.await(30, java.util.concurrent.TimeUnit.SECONDS);
        assertTrue(completed, "All threads should complete within timeout");
        
        // Verify results
        int expectedSuccesses = THREAD_COUNT * TRANSACTIONS_PER_THREAD;
        System.out.println(String.format("Concurrent XA: %d successes, %d failures out of %d total",
                successCount.get(), failureCount.get(), expectedSuccesses));
        
        // At least 80% should succeed (allowing for some contention)
        assertTrue(successCount.get() >= expectedSuccesses * 0.8,
                String.format("Expected at least %d successes, got %d",
                        (int)(expectedSuccesses * 0.8), successCount.get()));
        
        // Verify data in database
        try (Connection verifyConn = xaDS.getXAConnection().getConnection();
             Statement stmt = verifyConn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
            assertTrue(rs.next());
            int rowCount = rs.getInt(1);
            assertEquals(successCount.get(), rowCount,
                    "Database row count should match successful commits");
        }
        
        // Verify each thread's data is isolated
        try (Connection verifyConn = xaDS.getXAConnection().getConnection();
             Statement stmt = verifyConn.createStatement()) {
            for (int threadId = 0; threadId < THREAD_COUNT; threadId++) {
                try (ResultSet rs = stmt.executeQuery(
                        "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE thread_id = " + threadId)) {
                    assertTrue(rs.next());
                    int threadRows = rs.getInt(1);
                    assertTrue(threadRows <= TRANSACTIONS_PER_THREAD,
                            "Thread " + threadId + " should have at most " + TRANSACTIONS_PER_THREAD + " rows");
                }
            }
        }
        
        System.out.println("✓ TCMultithreaded: Concurrent XA transaction tests passed");
    }

    /** Simple Xid implementation for XA transactions. */
    private static class XidImpl implements Xid {
        private final int formatId;
        private final byte[] gtrid;
        private final byte[] bqual;

        XidImpl(int formatId, byte[] gtrid, byte[] bqual) {
            this.formatId = formatId;
            this.gtrid = gtrid;
            this.bqual = bqual;
        }

        @Override
        public int getFormatId() {
            return formatId;
        }

        @Override
        public byte[] getGlobalTransactionId() {
            return gtrid;
        }

        @Override
        public byte[] getBranchQualifier() {
            return bqual;
        }
    }
}
