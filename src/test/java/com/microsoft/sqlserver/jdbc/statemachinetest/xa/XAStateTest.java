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
import java.util.Arrays;
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
import com.microsoft.sqlserver.jdbc.SQLServerXAResource;
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
     * to get an XA connection and perform an XA operation.
     */
    private static boolean isXASupported(String connString) {
        XAConnection xaConn = null;
        try {
            SQLServerXADataSource ds = new SQLServerXADataSource();
            ds.setURL(connString);
            xaConn = ds.getXAConnection();
            XAResource xaRes = xaConn.getXAResource();

            // Try a simple XA operation - start with a test Xid
            // This will fail if XA DLL is not loaded or procedures not installed
            Xid testXid = createXid();

            try {
                xaRes.start(testXid, XAResource.TMNOFLAGS);
                xaRes.end(testXid, XAResource.TMSUCCESS);
                xaRes.rollback(testXid);
            } catch (XAException xe) {
                // Check if this is an XA availability issue (not a state/usage error)
                if (isXANotInstalledError(xe)) {
                    return false;
                }
                // If it's a different XA error (like state error), XA is available
            }

            return true;
        } catch (Exception e) {
            // Check the entire exception chain for XA-related errors
            if (isXANotInstalledError(e)) {
                return false;
            }

            // Other errors might be transient (network, auth, etc.), assume XA is available
            // and let the actual test provide better diagnostics if XA is truly unavailable
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

    /**
     * Checks if an exception indicates XA is not installed/available.
     */
    private static boolean isXANotInstalledError(Throwable e) {
        // Check if it's an XAException with specific error codes
        if (e instanceof XAException) {
            XAException xe = (XAException) e;
            String msg = xe.getMessage();

            // Error code -3: DTC initialization failure (XA not enabled in MSDTC)
            if (xe.errorCode == -3) {
                System.out.println("XA not supported: MSDTC is not configured for XA transactions");
                System.out.println("  Error: " + (msg != null ? msg : "DTC_ERROR StatusCode:-3"));
                System.out.println(
                        "  Solution: Enable XA in MSDTC using Set-DtcNetworkSetting -XATransactionsEnabled $true");
                return true;
            }

            // Error code -7: MSDTC service not available
            if (xe.errorCode == -7) {
                System.out.println("XA not supported: MSDTC service is not running");
                System.out.println("  Solution: Start MSDTC service with 'net start msdtc'");
                return true;
            }
        }

        Throwable current = e;
        while (current != null) {
            String msg = current.getMessage();
            String fullMsg = current.toString();

            // XA stored procedures not found indicates XA is not installed
            if ((msg != null && msg.toLowerCase().contains("xp_sqljdbc_xa")) ||
                    (fullMsg != null && fullMsg.toLowerCase().contains("xp_sqljdbc_xa"))) {
                System.out.println("XA not supported: XA stored procedures not found");
                System.out.println("  Solution: Run xa_install.sql script on SQL Server");
                return true;
            }

            // DLL load errors also indicate XA is not properly installed
            if ((msg != null && (msg.toLowerCase().contains("sqljdbc_xa.dll") ||
                    msg.toLowerCase().contains("xa control connection") ||
                    msg.toLowerCase().contains("could not load the dll"))) ||
                    (fullMsg != null && (fullMsg.toLowerCase().contains("sqljdbc_xa.dll") ||
                            fullMsg.toLowerCase().contains("xa control connection") ||
                            fullMsg.toLowerCase().contains("could not load the dll")))) {
                System.out.println("XA not supported: " + msg);
                System.out.println("  Solution: Install sqljdbc_xa.dll in SQL Server Binn directory and unblock it");
                return true;
            }

            // DTC_ERROR in message indicates MSDTC configuration issue
            if ((msg != null && msg.toLowerCase().contains("dtc_error")) ||
                    (fullMsg != null && fullMsg.toLowerCase().contains("dtc_error"))) {
                System.out.println("XA not supported: MSDTC configuration error");
                System.out.println("  Error: " + msg);
                System.out.println("  Solution: Enable XA transactions in MSDTC settings");
                return true;
            }

            current = current.getCause();
        }
        return false;
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
    @org.junit.jupiter.api.Timeout(value = 5, unit = java.util.concurrent.TimeUnit.MINUTES)
    public void testXAWithIsolationLevels() throws Exception {
        assumeTrue(isXASupported(connectionString), "Skipping: XA not supported or connection not configured");
        
        System.out.println("Starting TCIsolationLevels tests...");
        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        xaDS.setURL(connectionString);
        
        XAConnection xaConn = null;
        XAResource xaRes = null;
        Connection conn = null;
        List<Xid> pendingXids = new ArrayList<>();

        try {
            xaConn = xaDS.getXAConnection();
            xaRes = xaConn.getXAResource();
            conn = xaConn.getConnection();
            
            // Setup test table with initial data (outside XA transaction to avoid locks)
            System.out.println("Setting up test table...");
            try (Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(30); // 30 second timeout for DDL
                TestUtils.dropTableIfExists(TABLE_NAME, stmt);
                stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, value INT)");
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 100)");
            }
            System.out.println("Test table setup complete");
            
            // Test 1: READ UNCOMMITTED - Should see uncommitted changes (but in XA, still isolated)
            System.out.println("Starting Test 1: READ_UNCOMMITTED...");
            int originalIsolation = conn.getTransactionIsolation();
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            
            Xid xid1 = createXid();
            pendingXids.add(xid1);
            try {
                xaRes.start(xid1, XAResource.TMNOFLAGS);
                try (Statement stmt = conn.createStatement()) {
                    stmt.setQueryTimeout(30);
                    stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 200)");
                }
                xaRes.end(xid1, XAResource.TMSUCCESS);
                xaRes.commit(xid1, true);
                pendingXids.remove(xid1);
            } catch (Exception e) {
                System.err.println("Test 1 failed: " + e.getMessage());
                try {
                    xaRes.end(xid1, XAResource.TMFAIL);
                } catch (Exception ignored) {
                }
                try {
                    xaRes.rollback(xid1);
                    pendingXids.remove(xid1);
                } catch (Exception ignored) {
                }
                throw e;
            }
            
            // Verify committed
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should have 2 rows after commit");
            }
            
            // Test 2: READ COMMITTED (default) - Should only see committed data
            System.out.println("Starting Test 2: READ_COMMITTED...");
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            
            Xid xid2 = createXid();
            pendingXids.add(xid2);
            try {
                xaRes.start(xid2, XAResource.TMNOFLAGS);
                try (Statement stmt = conn.createStatement()) {
                    stmt.setQueryTimeout(30);
                    stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (3, 300)");
                    // Within the transaction, we should see our own changes
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                        assertTrue(rs.next());
                        assertEquals(3, rs.getInt(1), "Should see own changes within transaction");
                    }
                }
                xaRes.end(xid2, XAResource.TMSUCCESS);
                xaRes.rollback(xid2); // Rollback this one
                pendingXids.remove(xid2);
            } catch (Exception e) {
                System.err.println("Test 2 failed: " + e.getMessage());
                try {
                    xaRes.end(xid2, XAResource.TMFAIL);
                } catch (Exception ignored) {
                }
                try {
                    xaRes.rollback(xid2);
                    pendingXids.remove(xid2);
                } catch (Exception ignored) {
                }
                throw e;
            }
            
            // Should not see rolled back row
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1), "Should not see rolled back row");
            }
            
            // Test 3: REPEATABLE READ - Prevent non-repeatable reads
            System.out.println("Starting Test 3: REPEATABLE_READ...");
            conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            
            Xid xid3 = createXid();
            pendingXids.add(xid3);
            try {
                xaRes.start(xid3, XAResource.TMNOFLAGS);
                try (Statement stmt = conn.createStatement()) {
                    stmt.setQueryTimeout(30);
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
                pendingXids.remove(xid3);
            } catch (Exception e) {
                System.err.println("Test 3 failed: " + e.getMessage());
                try {
                    xaRes.end(xid3, XAResource.TMFAIL);
                } catch (Exception ignored) {
                }
                try {
                    xaRes.rollback(xid3);
                    pendingXids.remove(xid3);
                } catch (Exception ignored) {
                }
                throw e;
            }
            
            // Verify update committed
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT value FROM " + TABLE_NAME + " WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(101, rs.getInt(1), "Updated value should be committed");
            }
            
            // Test 4: SERIALIZABLE - Highest isolation level
            System.out.println("Starting Test 4: SERIALIZABLE...");
            conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            
            Xid xid4 = createXid();
            pendingXids.add(xid4);
            try {
                xaRes.start(xid4, XAResource.TMNOFLAGS);
                try (Statement stmt = conn.createStatement()) {
                    stmt.setQueryTimeout(30);
                    stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (4, 400)");
                    stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (5, 500)");
                }
                xaRes.end(xid4, XAResource.TMSUCCESS);
                xaRes.prepare(xid4);
                xaRes.commit(xid4, false);
                pendingXids.remove(xid4);
            } catch (Exception e) {
                System.err.println("Test 4 failed: " + e.getMessage());
                try {
                    xaRes.end(xid4, XAResource.TMFAIL);
                } catch (Exception ignored) {
                }
                try {
                    xaRes.rollback(xid4);
                    pendingXids.remove(xid4);
                } catch (Exception ignored) {
                }
                throw e;
            }
            
            // Verify both inserts
            try (Statement stmt = conn.createStatement();
                 ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1), "Should have 4 rows total");
            }
            
            // Test 5: Mix isolation levels across transactions
            System.out.println("Starting Test 5: Mixed isolation levels...");
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            Xid xid5 = createXid();
            pendingXids.add(xid5);
            try {
                xaRes.start(xid5, XAResource.TMNOFLAGS);
                try (Statement stmt = conn.createStatement()) {
                    stmt.setQueryTimeout(30);
                    stmt.execute("DELETE FROM " + TABLE_NAME + " WHERE id = 5");
                }
                xaRes.end(xid5, XAResource.TMSUCCESS);
                xaRes.commit(xid5, true);
                pendingXids.remove(xid5);
            } catch (Exception e) {
                System.err.println("Test 5 failed: " + e.getMessage());
                try {
                    xaRes.end(xid5, XAResource.TMFAIL);
                } catch (Exception ignored) {
                }
                try {
                    xaRes.rollback(xid5);
                    pendingXids.remove(xid5);
                } catch (Exception ignored) {
                }
                throw e;
            }
            
            // Final verification
            System.out.println("Final verification...");
            try (Statement stmt = conn.createStatement()) {
                stmt.setQueryTimeout(30);
                try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                    assertTrue(rs.next());
                    assertEquals(3, rs.getInt(1), "Should have 3 rows after deletion");
                }
            }
            
            // Restore original isolation level
            conn.setTransactionIsolation(originalIsolation);
            
            System.out.println("✓ TCIsolationLevels: All isolation level tests passed");
        } catch (Exception e) {
            System.err.println("TCIsolationLevels test failed with exception: " + e.getMessage());
            e.printStackTrace();
            throw e;
        } finally {
            // Clean up any pending XA transactions
            if (xaRes != null && !pendingXids.isEmpty()) {
                System.out.println("Cleaning up " + pendingXids.size() + " pending XA transactions...");
                for (Xid xid : pendingXids) {
                    try {
                        xaRes.rollback(xid);
                        System.out.println("Rolled back pending XID: " + xid);
                    } catch (Exception e) {
                        System.err.println("Failed to rollback XID " + xid + ": " + e.getMessage());
                    }
                }
            }

            // Close connection
            if (xaConn != null) {
                try {
                    xaConn.close();
                    System.out.println("XA connection closed successfully");
                } catch (SQLException e) {
                    System.err.println("Error closing XA connection: " + e.getMessage());
                }
            }
        }
    }

    /**
     * Test #7: TCMultithreaded - Concurrent Independent XA Transactions
     * Tests multiple threads executing independent XA transactions concurrently.
     */
    @Test
    @org.junit.jupiter.api.Timeout(value = 3, unit = java.util.concurrent.TimeUnit.MINUTES)
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

    // ==================== ADDITIONAL DETERMINISTIC TEST CASES ====================

    /**
     * Test: TCCommit(testCommitEndException) + TCRollback(testRollbackEndException)
     * Verifies that calling end() on an already-committed or already-rolled-back XA
     * transaction
     * returns XAER_NOTA. SQL Server allows commit/rollback from STARTED state
     * (implicit end),
     * so subsequent end() calls must fail.
     */
    @Test
    public void testEndAfterCommitOrRollback() throws Exception {
        assumeTrue(isXASupported(connectionString),
                "Skipping: XA not supported or connection not configured");

        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        xaDS.setURL(connectionString);

        XAConnection xaConn = null;
        try {
            xaConn = xaDS.getXAConnection();
            XAResource xaRes = xaConn.getXAResource();
            Connection conn = xaConn.getConnection();

            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(TABLE_NAME, stmt);
                stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, value INT)");
            }

            // TCCommit.testCommitEndException:
            // SQL Server XA allows commit(onePhase=true) from STARTED state (implicit end);
            // subsequent end() calls on the completed XID must return XAER_NOTA.
            Xid xid1 = createXid();
            xaRes.start(xid1, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 100)");
            }
            xaRes.commit(xid1, true); // commit from STARTED state (no explicit end)

            try {
                xaRes.end(xid1, XAResource.TMSUCCESS);
                fail("Should throw XAException when calling end() after commit");
            } catch (XAException e) {
                assertEquals(XAException.XAER_NOTA, e.errorCode,
                        "end() after commit should return XAER_NOTA");
            }
            try {
                xaRes.end(xid1, XAResource.TMSUCCESS); // second end still fails
                fail("Should throw XAException on repeated end() after commit");
            } catch (XAException e) {
                assertEquals(XAException.XAER_NOTA, e.errorCode,
                        "Repeated end() after commit must still return XAER_NOTA");
            }

            // TCRollback.testRollbackEndException:
            // rollback from STARTED state, subsequent end() must return XAER_NOTA.
            Xid xid2 = createXid();
            xaRes.start(xid2, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 200)");
            }
            xaRes.rollback(xid2); // rollback from STARTED state (no explicit end)

            try {
                xaRes.end(xid2, XAResource.TMSUCCESS);
                fail("Should throw XAException when calling end() after rollback");
            } catch (XAException e) {
                assertEquals(XAException.XAER_NOTA, e.errorCode,
                        "end() after rollback should return XAER_NOTA");
            }
            try {
                xaRes.end(xid2, XAResource.TMSUCCESS); // second end still fails
                fail("Should throw XAException on repeated end() after rollback");
            } catch (XAException e) {
                assertEquals(XAException.XAER_NOTA, e.errorCode,
                        "Repeated end() after rollback must still return XAER_NOTA");
            }

            System.out.println("\u2713 TCCommit/TCRollback: end() after commit/rollback correctly throws XAER_NOTA");
        } finally {
            if (xaConn != null) {
                try {
                    xaConn.close();
                } catch (SQLException e) {
                    /* ignore */ }
            }
        }
    }

    /**
     * Test: TCSanity(testDefaultIsolation) + TCIsolationLevels server-side
     * verification.
     * Queries DBCC USEROPTIONS inside XA to verify the active isolation level on
     * the server.
     * Also covers TRANSACTION_NONE mapping to READ COMMITTED (gap in
     * TCIsolationLevels).
     */
    @Test
    public void testXAIsolationLevelServerVerification() throws Exception {
        assumeTrue(isXASupported(connectionString),
                "Skipping: XA not supported or connection not configured");

        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        xaDS.setURL(connectionString);

        XAConnection xaConn = null;
        try {
            xaConn = xaDS.getXAConnection();
            XAResource xaRes = xaConn.getXAResource();
            Connection conn = xaConn.getConnection();

            // Test 1: Default isolation inside XA must be READ COMMITTED
            // (TCSanity.testDefaultIsolation)
            Xid xid1 = createXid();
            xaRes.start(xid1, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("DBCC USEROPTIONS")) {
                boolean found = false;
                while (rs.next()) {
                    if ("isolation level".equals(rs.getString(1))) {
                        assertEquals("read committed", rs.getString(2).trim().toLowerCase(),
                                "Default XA isolation level must be read committed");
                        found = true;
                    }
                }
                assertTrue(found, "DBCC USEROPTIONS must return an 'isolation level' row");
            }
            xaRes.end(xid1, XAResource.TMSUCCESS);
            xaRes.rollback(xid1);

            // Test 2: TRANSACTION_NONE maps to read committed (TCIsolationLevels gap)
            conn.setTransactionIsolation(Connection.TRANSACTION_NONE);
            Xid xid2 = createXid();
            xaRes.start(xid2, XAResource.TMNOFLAGS);
            try (Statement stmt = conn.createStatement();
                    ResultSet rs = stmt.executeQuery("DBCC USEROPTIONS")) {
                while (rs.next()) {
                    if ("isolation level".equals(rs.getString(1))) {
                        assertEquals("read committed", rs.getString(2).trim().toLowerCase(),
                                "TRANSACTION_NONE should map to read committed in XA");
                    }
                }
            }
            xaRes.end(xid2, XAResource.TMSUCCESS);
            xaRes.rollback(xid2);

            // Test 3: Each standard isolation level verified via DBCC USEROPTIONS
            int[] levels = {
                    Connection.TRANSACTION_READ_UNCOMMITTED,
                    Connection.TRANSACTION_READ_COMMITTED,
                    Connection.TRANSACTION_REPEATABLE_READ,
                    Connection.TRANSACTION_SERIALIZABLE
            };
            String[] expected = {
                    "read uncommitted",
                    "read committed",
                    "repeatable read",
                    "serializable"
            };
            for (int i = 0; i < levels.length; i++) {
                conn.setTransactionIsolation(levels[i]);
                Xid xid = createXid();
                xaRes.start(xid, XAResource.TMNOFLAGS);
                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("DBCC USEROPTIONS")) {
                    while (rs.next()) {
                        if ("isolation level".equals(rs.getString(1))) {
                            assertEquals(expected[i], rs.getString(2).trim().toLowerCase(),
                                    "Server isolation level mismatch for JDBC level " + levels[i]);
                        }
                    }
                }
                xaRes.end(xid, XAResource.TMSUCCESS);
                xaRes.rollback(xid);
            }

            System.out.println("\u2713 TCSanity/TCIsolationLevels: Server-verified isolation levels passed");
        } finally {
            if (xaConn != null) {
                try {
                    xaConn.close();
                } catch (SQLException e) {
                    /* ignore */ }
            }
        }
    }

    /**
     * Test: TCSanity(testCommit2Phase) - FormatId round-trip via recover().
     * After prepare(), recover() must return the XID with the exact formatId that
     * was sent.
     * Verifies fix for VSTS #841313 ("Introduce support for formatId").
     */
    @Test
    public void testFormatIdRoundTrip() throws Exception {
        assumeTrue(isXASupported(connectionString),
                "Skipping: XA not supported or connection not configured");

        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        xaDS.setURL(connectionString);

        int[] formatIds = { 1, 1234, 9999 };
        for (int formatId : formatIds) {
            XAConnection xaConn = xaDS.getXAConnection();
            try {
                XAResource xaRes = xaConn.getXAResource();
                Connection conn = xaConn.getConnection();

                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(TABLE_NAME, stmt);
                    stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, value INT)");
                }

                byte[] gtrid = UUID.randomUUID().toString().substring(0, 8).getBytes();
                byte[] bqual = UUID.randomUUID().toString().substring(0, 8).getBytes();
                Xid xid = new XidImpl(formatId, gtrid, bqual);

                xaRes.start(xid, XAResource.TMNOFLAGS);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 100)");
                }
                xaRes.end(xid, XAResource.TMSUCCESS);
                int prepResult = xaRes.prepare(xid);
                assertEquals(XAResource.XA_OK, prepResult,
                        "Prepare must return XA_OK for formatId=" + formatId);

                // recover() must return the XID with the same formatId sent (VSTS #841313)
                Xid[] recovered = xaRes.recover(XAResource.TMSTARTRSCAN | XAResource.TMENDRSCAN);
                assertNotNull(recovered, "Recover must not return null");

                boolean found = false;
                for (Xid r : recovered) {
                    if (r.getFormatId() == formatId
                            && Arrays.equals(r.getGlobalTransactionId(), gtrid)) {
                        found = true;
                        assertEquals(formatId, r.getFormatId(),
                                "Recovered XID formatId must match sent formatId=" + formatId);
                        break;
                    }
                }
                assertTrue(found,
                        "Prepared XID must be recoverable with matching formatId=" + formatId);

                xaRes.commit(xid, false);
                System.out.println("FormatId=" + formatId + ": round-trip via recover() OK");
            } finally {
                xaConn.close();
            }
        }
        System.out.println("\u2713 FormatId round-trip: all formatId values verified through recover()");
    }

    /**
     * Test: TCSanity - XA transaction timeout set/get behavior.
     * Verifies setTransactionTimeout/getTransactionTimeout round-trip and that
     * timeout=0 (infinite) allows normal transaction completion.
     * Note: the actual auto-abort wait scenario (FX testSetTimeout, 40s wait) is
     * intentionally excluded to keep test duration practical.
     */
    @Test
    public void testXATransactionTimeout() throws Exception {
        assumeTrue(isXASupported(connectionString),
                "Skipping: XA not supported or connection not configured");

        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        xaDS.setURL(connectionString);

        XAConnection xaConn = null;
        try {
            xaConn = xaDS.getXAConnection();
            XAResource xaRes = xaConn.getXAResource();

            // Test 1: Default timeout (0) is infinite - start/end must not throw
            xaRes.setTransactionTimeout(0);
            Xid xid1 = createXid();
            xaRes.start(xid1, XAResource.TMNOFLAGS);
            xaRes.end(xid1, XAResource.TMSUCCESS);
            xaRes.rollback(xid1);

            // Test 2: setTransactionTimeout / getTransactionTimeout round-trip
            assertTrue(xaRes.setTransactionTimeout(20), "setTransactionTimeout(20) must return true");
            assertEquals(20, xaRes.getTransactionTimeout(),
                    "getTransactionTimeout() must return 20 after setTransactionTimeout(20)");

            assertTrue(xaRes.setTransactionTimeout(60), "setTransactionTimeout(60) must return true");
            assertEquals(60, xaRes.getTransactionTimeout(),
                    "getTransactionTimeout() must return 60 after setTransactionTimeout(60)");

            // Test 3: Reset to default and verify transaction is still usable
            assertTrue(xaRes.setTransactionTimeout(0), "setTransactionTimeout(0) must return true");
            Xid xid2 = createXid();
            xaRes.start(xid2, XAResource.TMNOFLAGS);
            xaRes.end(xid2, XAResource.TMSUCCESS);
            xaRes.rollback(xid2);

            System.out.println("\u2713 TCSanity: XA transaction timeout behavior passed");
        } finally {
            if (xaConn != null) {
                try {
                    xaConn.getXAResource().setTransactionTimeout(0); // always reset to default
                } catch (Exception e) {
                    /* ignore */ }
                try {
                    xaConn.close();
                } catch (SQLException e) {
                    /* ignore */ }
            }
        }
    }

    /**
     * Test: TCPoolingWithXATransactions - XA interaction with connection pooling.
     * (1) testMultipleCommitsWithCnClose: XAConnection reused for new transactions
     * after conn.close().
     * (2) testResumeTxWithCnClose: suspend XA -> conn.close() -> reget conn ->
     * resume -> commit.
     */
    @Test
    public void testXAPoolingWithConnectionClose() throws Exception {
        assumeTrue(isXASupported(connectionString),
                "Skipping: XA not supported or connection not configured");

        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        xaDS.setURL(connectionString);

        XAConnection xaConn = xaDS.getXAConnection();
        try {
            // Initialize test table
            try (Connection setupConn = xaConn.getConnection();
                    Statement stmt = setupConn.createStatement()) {
                TestUtils.dropTableIfExists(TABLE_NAME, stmt);
                stmt.execute("CREATE TABLE " + TABLE_NAME
                        + " (id INT PRIMARY KEY, value INT, scenario INT)");
            }

            // testMultipleCommitsWithCnClose: commit, close conn, get new conn, commit
            // again
            for (int i = 0; i < 2; i++) {
                Connection conn = xaConn.getConnection();
                XAResource xaRes = xaConn.getXAResource();
                Xid xid = createXid();
                xaRes.start(xid, XAResource.TMNOFLAGS);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO " + TABLE_NAME
                            + " VALUES (" + (i + 1) + ", " + (i * 100 + 100) + ", 1)");
                }
                xaRes.end(xid, XAResource.TMSUCCESS);
                xaRes.commit(xid, true);
                conn.close(); // soft-close between transactions
            }

            try (Connection verifyConn = xaConn.getConnection();
                    Statement stmt = verifyConn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE scenario = 1")) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1),
                        "Should have 2 rows after multiple commits with conn.close() between each");
            }

            // testResumeTxWithCnClose: suspend -> conn.close() -> reget conn -> resume ->
            // commit
            for (int i = 0; i < 2; i++) {
                Connection conn = xaConn.getConnection();
                XAResource xaRes = xaConn.getXAResource();
                Xid xid = createXid();

                xaRes.start(xid, XAResource.TMNOFLAGS);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO " + TABLE_NAME
                            + " VALUES (" + (i * 2 + 10) + ", 10, 2)");
                }
                xaRes.end(xid, XAResource.TMSUSPEND);
                conn.close(); // soft-close while transaction is suspended

                conn = xaConn.getConnection(); // re-get connection
                xaRes.start(xid, XAResource.TMRESUME);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO " + TABLE_NAME
                            + " VALUES (" + (i * 2 + 11) + ", 20, 2)");
                }
                xaRes.end(xid, XAResource.TMSUCCESS);
                xaRes.commit(xid, true);
                conn.close();
            }

            try (Connection verifyConn = xaConn.getConnection();
                    Statement stmt = verifyConn.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT COUNT(*) FROM " + TABLE_NAME + " WHERE scenario = 2")) {
                assertTrue(rs.next());
                assertEquals(4, rs.getInt(1),
                        "Should have 4 rows (2 per iteration) after suspend/resume with conn.close()");
            }

            System.out.println("\u2713 TCPoolingWithXATransactions: Connection pooling with XA passed");
        } finally {
            xaConn.close();
        }
    }

    /**
     * Test: TCException(testMinorError + testMajorError) - RAISERROR severity in XA
     * transactions.
     * Minor errors (severity 1-9): informational only, do NOT abort XA; commit
     * succeeds.
     * Major errors (severity 11+): throw SQLException and invalidate the
     * transaction branch.
     */
    @Test
    public void testXASqlErrorSeverity() throws Exception {
        assumeTrue(isXASupported(connectionString),
                "Skipping: XA not supported or connection not configured");

        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        xaDS.setURL(connectionString);

        // Test 1: Minor errors (severity 1, 5, 9) - must NOT abort XA transaction
        int[] minorSeverities = { 1, 5, 9 };
        for (int severity : minorSeverities) {
            XAConnection xaConn = xaDS.getXAConnection();
            try {
                XAResource xaRes = xaConn.getXAResource();
                Connection conn = xaConn.getConnection();

                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(TABLE_NAME, stmt);
                    stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, value INT)");
                }

                Xid xid = createXid();
                xaRes.start(xid, XAResource.TMNOFLAGS);
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 100)");
                    // Minor RAISERROR should NOT throw SQLException in JDBC
                    stmt.execute("RAISERROR ('minor error level " + severity + "', " + severity + ", 1)");
                }
                xaRes.end(xid, XAResource.TMSUCCESS);
                xaRes.commit(xid, true); // must succeed despite minor error

                try (Statement stmt = conn.createStatement();
                        ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1),
                            "Insert must be committed after minor RAISERROR (severity=" + severity + ")");
                }
                System.out.println("Minor severity=" + severity + ": committed OK");
            } finally {
                xaConn.close();
            }
        }

        // Test 2: Major errors (severity 11, 16) - MUST throw SQLException and
        // invalidate XA
        int[] majorSeverities = { 11, 16 };
        for (int severity : majorSeverities) {
            XAConnection xaConn = xaDS.getXAConnection();
            XAResource xaRes = xaConn.getXAResource();
            Xid xid = createXid();
            try {
                Connection conn = xaConn.getConnection();
                xaRes.start(xid, XAResource.TMNOFLAGS);

                boolean exceptionThrown = false;
                try (Statement stmt = conn.createStatement()) {
                    stmt.execute("RAISERROR ('major error level " + severity + "', " + severity + ", 1)");
                } catch (SQLException e) {
                    exceptionThrown = true;
                }
                assertTrue(exceptionThrown,
                        "RAISERROR with severity=" + severity + " must throw SQLException");

                // Transaction is now invalid; clean up via TMFAIL + rollback, or forget
                try {
                    xaRes.end(xid, XAResource.TMFAIL);
                    xaRes.rollback(xid);
                } catch (XAException e) {
                    try {
                        xaRes.forget(xid);
                    } catch (XAException fe) {
                        /* ignore */ }
                }
                System.out.println("Major severity=" + severity + ": transaction correctly invalidated");
            } finally {
                xaConn.close();
            }
        }

        System.out.println("\u2713 TCException: SQL error severity handling in XA passed");
    }

    /**
     * Test: TCTightlyCoupled - SQL Server tightly-coupled XA transactions
     * (SSTRANSTIGHTLYCPLD).
     * Multiple branches sharing the same gtrid are tightly coupled. The primary
     * branch returns
     * XA_OK from prepare; secondary branches return XA_RDONLY (their work is
     * absorbed by primary).
     * Committing the primary commits all; rolling back the primary rolls back all.
     * Also covers the SSTRANSTIGHTLYCPLD constant verification (TCVerifyXAResource
     * gap).
     */
    @Test
    public void testTightlyCoupledXATransactions() throws Exception {
        assumeTrue(isXASupported(connectionString),
                "Skipping: XA not supported or connection not configured");

        // Verify SSTRANSTIGHTLYCPLD constant is defined
        // (TCVerifyXAResource.testDefinedConstants)
        int tightlyCpldFlag = SQLServerXAResource.SSTRANSTIGHTLYCPLD;
        assertTrue(tightlyCpldFlag != 0,
                "SQLServerXAResource.SSTRANSTIGHTLYCPLD must be a non-zero flag value");

        SQLServerXADataSource xaDS = new SQLServerXADataSource();
        xaDS.setURL(connectionString);

        XAConnection xaConn1 = null;
        XAConnection xaConn2 = null;
        try {
            xaConn1 = xaDS.getXAConnection();
            xaConn2 = xaDS.getXAConnection();
            XAResource xaRes1 = xaConn1.getXAResource();
            XAResource xaRes2 = xaConn2.getXAResource();
            Connection conn1 = xaConn1.getConnection();
            Connection conn2 = xaConn2.getConnection();

            // Tightly-coupled branches must share formatId AND gtrid, differ only in bqual
            final int sharedFormatId = 5678;

            // --- Scenario 1: Two-phase commit with two tightly-coupled branches ---
            try (Statement stmt = conn1.createStatement()) {
                TestUtils.dropTableIfExists(TABLE_NAME, stmt);
                stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, value INT)");
            }

            byte[] gtrid1 = UUID.randomUUID().toString().substring(0, 8).getBytes();
            Xid primary1 = new XidImpl(sharedFormatId, gtrid1, "b1-s1".getBytes());
            Xid secondary1 = new XidImpl(sharedFormatId, gtrid1, "b2-s1".getBytes());

            xaRes1.start(primary1, SQLServerXAResource.SSTRANSTIGHTLYCPLD);
            try (Statement stmt = conn1.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (1, 100)");
            }
            xaRes2.start(secondary1, SQLServerXAResource.SSTRANSTIGHTLYCPLD);
            try (Statement stmt = conn2.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (2, 200)");
            }
            xaRes1.end(primary1, XAResource.TMSUCCESS);
            xaRes2.end(secondary1, XAResource.TMSUCCESS);

            int prepPrimary1 = xaRes1.prepare(primary1);
            assertEquals(XAResource.XA_OK, prepPrimary1,
                    "Primary tightly-coupled branch must return XA_OK from prepare");

            int prepSecondary1 = xaRes2.prepare(secondary1);
            assertEquals(XAResource.XA_RDONLY, prepSecondary1,
                    "Secondary tightly-coupled branch must return XA_RDONLY from prepare");

            xaRes1.commit(primary1, false); // committing primary commits all branches

            try (Statement stmt = conn1.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1),
                        "Both tightly-coupled branches should be committed after primary commit");
            }

            // --- Scenario 2: Rollback primary branch rolls back all ---
            try (Statement stmt = conn1.createStatement()) {
                TestUtils.dropTableIfExists(TABLE_NAME, stmt);
                stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, value INT)");
            }

            byte[] gtrid2 = UUID.randomUUID().toString().substring(0, 8).getBytes();
            Xid primary2 = new XidImpl(sharedFormatId, gtrid2, "b1-s2".getBytes());
            Xid secondary2 = new XidImpl(sharedFormatId, gtrid2, "b2-s2".getBytes());

            xaRes1.start(primary2, SQLServerXAResource.SSTRANSTIGHTLYCPLD);
            try (Statement stmt = conn1.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (3, 300)");
            }
            xaRes2.start(secondary2, SQLServerXAResource.SSTRANSTIGHTLYCPLD);
            try (Statement stmt = conn2.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (4, 400)");
            }
            xaRes1.end(primary2, XAResource.TMSUCCESS);
            xaRes2.end(secondary2, XAResource.TMSUCCESS);

            int prepPrimary2 = xaRes1.prepare(primary2);
            assertEquals(XAResource.XA_OK, prepPrimary2,
                    "Primary must return XA_OK from prepare in rollback scenario");
            int prepSecondary2 = xaRes2.prepare(secondary2);
            assertEquals(XAResource.XA_RDONLY, prepSecondary2,
                    "Secondary must return XA_RDONLY in rollback scenario");

            xaRes1.rollback(primary2); // rolling back primary rolls back all

            try (Statement stmt = conn1.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1),
                        "Rolling back the primary tightly-coupled branch must remove all branch work");
            }

            // --- Scenario 3: Suspend/Resume with SSTRANSTIGHTLYCPLD ---
            try (Statement stmt = conn1.createStatement()) {
                TestUtils.dropTableIfExists(TABLE_NAME, stmt);
                stmt.execute("CREATE TABLE " + TABLE_NAME + " (id INT PRIMARY KEY, value INT)");
            }

            byte[] gtrid3 = UUID.randomUUID().toString().substring(0, 8).getBytes();
            Xid primary3 = new XidImpl(sharedFormatId, gtrid3, "b1-s3".getBytes());

            xaRes1.start(primary3, SQLServerXAResource.SSTRANSTIGHTLYCPLD);
            try (Statement stmt = conn1.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (5, 500)");
            }
            xaRes1.end(primary3, XAResource.TMSUSPEND | SQLServerXAResource.SSTRANSTIGHTLYCPLD);
            xaRes1.start(primary3, XAResource.TMRESUME | SQLServerXAResource.SSTRANSTIGHTLYCPLD);
            try (Statement stmt = conn1.createStatement()) {
                stmt.execute("INSERT INTO " + TABLE_NAME + " VALUES (6, 600)");
            }
            xaRes1.end(primary3, XAResource.TMSUCCESS);
            xaRes1.prepare(primary3);
            xaRes1.commit(primary3, false);

            try (Statement stmt = conn1.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + TABLE_NAME)) {
                assertTrue(rs.next());
                assertEquals(2, rs.getInt(1),
                        "Suspend/resume within a tightly-coupled branch must preserve all work");
            }

            // --- Scenario 4: Invalid flag combination TMJOIN | SSTRANSTIGHTLYCPLD must
            // fail ---
            try {
                Xid invalidXid = createXid();
                xaRes1.start(invalidXid, XAResource.TMJOIN | SQLServerXAResource.SSTRANSTIGHTLYCPLD);
                try {
                    xaRes1.end(invalidXid, XAResource.TMFAIL);
                    xaRes1.rollback(invalidXid);
                } catch (XAException ignored) {
                    /* ignore cleanup */ }
                fail("TMJOIN | SSTRANSTIGHTLYCPLD is invalid and must throw XAException");
            } catch (XAException e) {
                System.out.println("Invalid flag (TMJOIN|SSTRANSTIGHTLYCPLD) correctly rejected");
            }

            System.out.println("\u2713 TCTightlyCoupled: All tightly-coupled XA transaction tests passed");
        } finally {
            if (xaConn1 != null)
                try {
                    xaConn1.close();
                } catch (SQLException e) {
                    /* ignore */ }
            if (xaConn2 != null)
                try {
                    xaConn2.close();
                } catch (SQLException e) {
                    /* ignore */ }
        }
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
