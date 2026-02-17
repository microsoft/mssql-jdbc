/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved.
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.statemachinetest.resultset;

import static com.microsoft.sqlserver.jdbc.statemachinetest.resultset.ResultSetActions.*;
import static com.microsoft.sqlserver.jdbc.statemachinetest.resultset.ResultSetState.*;
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
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Engine;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.Result;
import com.microsoft.sqlserver.jdbc.statemachinetest.core.StateMachineTest;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * ResultSet State Machine Tests - Based on FX Framework fxResultSet model
 * actions.
 * 
 * This class implements Model-Based Testing (MBT) for JDBC ResultSet operations
 * using
 * the simple StateMachineTest framework.
 * 
 * Test scenarios covered:
 * - Scrollable cursor navigation (next, previous, first, last, absolute)
 * - Data retrieval (getString)
 */
@Tag(Constants.stateMachine)
@Tag(Constants.legacyFX)
public class ResultSetStateTest extends AbstractTest {

    private static final String TABLE_NAME = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SM_ResultSet_Test"));

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

    @Test
    @DisplayName("FX Model: Real Database - Scrollable Sensitive Cursor")
    void testRealDatabaseScrollableCursor() throws SQLException {
        Assumptions.assumeTrue(connectionString != null, "No database connection configured");

        createTestTable();

        try (Connection conn = PrepUtil.getConnection(connectionString);
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                ResultSet rs = stmt.executeQuery("SELECT * FROM " + TABLE_NAME)) {

            StateMachineTest sm = new StateMachineTest("RealScrollableCursor");
            sm.setState(RS, rs);
            sm.setState(CLOSED, false);
            sm.setState(ON_VALID_ROW, false);

            sm.addAction(new NextAction(sm));
            sm.addAction(new PreviousAction(sm));
            sm.addAction(new FirstAction(sm));
            sm.addAction(new LastAction(sm));
            sm.addAction(new AbsoluteAction(sm));
            sm.addAction(new GetStringAction(sm));

            Result result = Engine.run(sm).withMaxActions(50).execute();

            System.out.println("\nReal DB test: " + result.actionCount + " actions");
            assertTrue(result.isSuccess());
        }
    }
}
