/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.transaction;

import static com.microsoft.sqlserver.jdbc.statemachinetest.transaction.TransactionActions.*;
import static com.microsoft.sqlserver.jdbc.statemachinetest.transaction.TransactionState.*;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Engine;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Result;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Transaction State Machine Tests - Based on FX Framework fxConnection model
 * actions.
 * 
 * This class implements Model-Based Testing (MBT) for JDBC Transaction
 * operations using the simple StateMachineTest framework.
 * 
 * Test scenarios covered:
 * - setAutoCommit(true/false) - Toggle auto-commit mode
 * - commit() - Commit transaction (requires autoCommit=false)
 * - rollback() - Rollback transaction (requires autoCommit=false)
 */
@Tag(Constants.legacyFX)
public class TransactionStateTest extends AbstractTest {

    private static final String TABLE_NAME_CONST = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SM_Transaction_Test"));

    @BeforeAll
    static void setupTests() throws Exception {
        setConnection();
    }

    @AfterAll
    static void cleanupTests() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            TestUtils.dropTableIfExists(TABLE_NAME_CONST, connection.createStatement());
        }
    }

    /**
     * Creates a test table with sample data for transaction tests.
     */
    private static void createTestTable(Connection conn) throws SQLException {
        try (Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(TABLE_NAME_CONST, stmt);
            stmt.execute("CREATE TABLE " + TABLE_NAME_CONST + " (id INT PRIMARY KEY, value INT)");
            stmt.execute("INSERT INTO " + TABLE_NAME_CONST + " VALUES (1, 100)");
        }
    }

    @Test
    @DisplayName("FX Model: Real Database - Transaction Commit/Rollback")
    void testRealDatabaseTransaction() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            createTestTable(conn);

            StateMachineTest sm = new StateMachineTest("RealTransaction");
            sm.setState(CONN, conn);
            sm.setState(AUTO_COMMIT, true);
            sm.setState(CLOSED, false);

            // Setup actions - validation fields set directly (no context object needed)
            sm.addAction(new SetAutoCommitFalseAction(sm));
            sm.addAction(new SetAutoCommitTrueAction(sm));
            sm.addAction(new CommitAction(sm, TABLE_NAME_CONST));
            sm.addAction(new RollbackAction(sm, TABLE_NAME_CONST));
            sm.addAction(new ExecuteUpdateAction(sm, TABLE_NAME_CONST));
            sm.addAction(new SelectAction(sm, TABLE_NAME_CONST));

            Result result = Engine.run(sm).withMaxActions(50).execute();

            // Cleanup - ensure autocommit is restored
            conn.setAutoCommit(true);

            System.out.println("\nReal DB transaction test: " + result.actionCount + " actions");
            assertTrue(result.isSuccess());
        }
    }

    @Test
    @DisplayName("Transaction Validation Test")
    void testTransactionWithValidation() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        try (Connection conn = PrepUtil.getConnection(connectionString)) {
            createTestTable(conn);

            StateMachineTest sm = new StateMachineTest("TransactionValidation");
            sm.setState(CONN, conn);
            sm.setState(AUTO_COMMIT, true);
            sm.setState(CLOSED, false);
            sm.setState(EXPECTED_VALUE, 100); // Initial committed value
            sm.setState(PENDING_VALUE, null); // No pending changes

            // Setup actions with validation data - shared via state machine state
            sm.addAction(new SetAutoCommitFalseAction(sm));
            sm.addAction(new SetAutoCommitTrueAction(sm));
            sm.addAction(new CommitAction(sm, TABLE_NAME_CONST));
            sm.addAction(new RollbackAction(sm, TABLE_NAME_CONST));
            sm.addAction(new ExecuteUpdateAction(sm, TABLE_NAME_CONST));
            sm.addAction(new SelectAction(sm, TABLE_NAME_CONST));

            Result result = Engine.run(sm).withMaxActions(50).withSeed(12345).execute();

            conn.setAutoCommit(true);

            System.out.println("\nTransaction validation test: " + result.actionCount + " actions");
            assertTrue(result.isSuccess());
        }
    }
}
