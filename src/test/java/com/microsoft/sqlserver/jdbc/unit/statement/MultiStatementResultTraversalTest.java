/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Regression matrix for multi-statement result traversal across {@link Statement} and
 * {@link PreparedStatement}.
 *
 * <p>Pins the {@code execute}, {@code executeUpdate} and {@code executeQuery} contracts on
 * compound SQL (DML chains, trailing/leading SELECT, triggers, generated keys, mid-batch errors,
 * and connection-property variants) using the canonical JDBC §13.1.4 / {@link Statement#getMoreResults()}
 * traversal loop. Every test drives results through {@link #traverseAllResults} and asserts the
 * exact ordered list of update counts and ResultSet row counts — replacing the loose
 * {@code do { ... } while (count != -1)} pattern whose silent-exit behaviour let #2722 and #2940
 * ship.
 *
 * <p>Issues covered: #2554, #2587, #2722, #2737, #2740, #2742, #2817, #2850, #2866, #2940, #2941.
 * Trigger-based tests are tagged {@link Constants#xAzureSQLDW} (Azure SQL DW does not support DML triggers).
 */
@RunWith(JUnitPlatform.class)
public class MultiStatementResultTraversalTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    // =========================================================================================
    //  Infrastructure
    // =========================================================================================

    /** Ordered snapshot of update counts and ResultSet row counts observed by {@link #traverseAllResults}. */
    static final class Traversal {
        /** Update counts in arrival order; the terminal {@code -1} is not included. */
        final List<Integer> updateCounts = new ArrayList<>();
        /** Row count of each {@code ResultSet} visited, in arrival order. */
        final List<Integer> resultSetRowCounts = new ArrayList<>();

        @Override
        public String toString() {
            return "Traversal{updateCounts=" + updateCounts
                    + ", resultSetRowCounts=" + resultSetRowCounts + "}";
        }
    }

    /**
     * Canonical JDBC §13.1.4 traversal — single source of truth for every test so no test can
     * exit early and mask a missing result.
     */
    static Traversal traverseAllResults(Statement stmt, boolean firstIsResultSet) throws SQLException {
        Traversal t = new Traversal();
        boolean isResultSet = firstIsResultSet;
        while (true) {
            if (isResultSet) {
                try (ResultSet rs = stmt.getResultSet()) {
                    assertNotNull(rs, "execute()/getMoreResults() returned true but getResultSet() was null");
                    int rows = 0;
                    while (rs.next()) {
                        rows++;
                    }
                    t.resultSetRowCounts.add(rows);
                }
            } else {
                int count = stmt.getUpdateCount();
                if (count == -1) {
                    break;
                }
                t.updateCounts.add(count);
            }
            isResultSet = stmt.getMoreResults();
        }
        return t;
    }

    /** {@code (c1 INT, c2 SMALLINT)}. */
    static void createPlainTable(Statement stmt, String table) throws SQLException {
        stmt.executeUpdate("CREATE TABLE " + table + " (c1 INT, c2 SMALLINT)");
    }

    /** {@code (id INT IDENTITY(1,1) PRIMARY KEY, name VARCHAR(32))}. */
    static void createIdentityTable(Statement stmt, String table) throws SQLException {
        stmt.executeUpdate("CREATE TABLE " + table
                + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY, name VARCHAR(32))");
    }

    /**
     * AFTER INSERT trigger that inserts one row into {@code auditTable}.
     * When {@code suppressNoCount=true} the trigger body begins with {@code SET NOCOUNT ON},
     * suppressing the trigger's own DONEINPROC row count on the wire.
     */
    static void createInsertTrigger(Statement stmt, String triggerName, String mainTable,
            String auditTable, boolean suppressNoCount) throws SQLException {
        String body = (suppressNoCount ? "SET NOCOUNT ON; " : "")
                + "INSERT INTO " + auditTable + " DEFAULT VALUES";
        stmt.executeUpdate(
                "CREATE TRIGGER " + triggerName + " ON " + mainTable + " FOR INSERT AS " + body);
    }

    // =========================================================================================
    //  Section 1 — Single-result baselines
    // =========================================================================================

    /**
     * Single-result baselines that prove {@link #traverseAllResults} matches the JDBC spec.
     * If anything here fails, no other test in the suite can be trusted.
     */
    @Nested
    public class JdbcSpecBaseline {

        /** {@code Statement.execute("UPDATE")} → returns false; one count; terminal -1. */
        @Test
        public void statementExecuteSingleUpdate() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BaseSU"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2), (3, 3)");

                    boolean isResultSet = stmt.execute("UPDATE " + table + " SET c2 = c2 + 1");

                    assertFalse(isResultSet, "execute(UPDATE) must return false because no ResultSet is produced");
                    Traversal t = traverseAllResults(stmt, isResultSet);
                    assertEquals(Arrays.asList(3), t.updateCounts,
                            "UPDATE on 3 rows must surface exactly one update count of 3");
                    assertEquals(0, t.resultSetRowCounts.size(),
                            "UPDATE must not produce any ResultSet");
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** {@code Statement.execute("SELECT")} → returns true; one RS; terminal -1. */
        @Test
        public void statementExecuteSingleSelect() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BaseSS"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2)");

                    boolean isResultSet = stmt.execute("SELECT * FROM " + table);

                    assertTrue(isResultSet, "execute(SELECT) must return true because a ResultSet is produced");
                    Traversal t = traverseAllResults(stmt, isResultSet);
                    assertEquals(0, t.updateCounts.size(),
                            "SELECT must not surface any update count");
                    assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                            "SELECT must surface exactly one ResultSet containing 2 rows");
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** {@code PreparedStatement.execute("UPDATE … WHERE ?")} — same contract as Statement. */
        @Test
        public void preparedStatementExecuteSingleUpdate() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BasePSU"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2)");

                    try (PreparedStatement ps = conn.prepareStatement(
                            "UPDATE " + table + " SET c1 = ? WHERE c2 > 0")) {
                        ps.setInt(1, 99);
                        boolean isResultSet = ps.execute();

                        assertFalse(isResultSet,
                                "PreparedStatement.execute(UPDATE) must return false (no ResultSet)");
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(2), t.updateCounts,
                                "UPDATE WHERE c2>0 on 2 rows must surface exactly one count of 2");
                        assertEquals(0, t.resultSetRowCounts.size(),
                                "UPDATE must not produce any ResultSet");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** {@code PreparedStatement.execute("SELECT … WHERE ?")} → one RS. */
        @Test
        public void preparedStatementExecuteSingleSelect() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BasePSS"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2), (3, 3)");

                    try (PreparedStatement ps = conn.prepareStatement(
                            "SELECT * FROM " + table + " WHERE c1 >= ?")) {
                        ps.setInt(1, 2);
                        boolean isResultSet = ps.execute();

                        assertTrue(isResultSet,
                                "PreparedStatement.execute(SELECT) must return true (ResultSet produced)");
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(0, t.updateCounts.size(),
                                "SELECT must not surface any update count");
                        assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                                "SELECT c1>=2 on (1,2,3) must surface one ResultSet with 2 rows");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** {@code Statement.executeUpdate(multi-row INSERT)} returns the row count. */
        @Test
        public void statementExecuteUpdateSingleInsert() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BaseSUI"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);

                    int count = stmt.executeUpdate(
                            "INSERT INTO " + table + " VALUES (1, 1), (2, 2), (3, 3)");

                    assertEquals(3, count,
                            "executeUpdate(multi-row INSERT VALUES (...), (...), (...)) must return total inserted rows = 3");
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** {@code Statement.executeQuery("SELECT")} returns the ResultSet directly. */
        @Test
        public void statementExecuteQuerySingleSelect() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BaseSQ"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2)");

                    try (ResultSet rs = stmt.executeQuery("SELECT * FROM " + table + " ORDER BY c1")) {
                        assertNotNull(rs, "executeQuery(SELECT) must return a non-null ResultSet");
                        assertTrue(rs.next(), "ResultSet must have at least one row (first of two)");
                        assertEquals(1, rs.getInt("c1"), "first row's c1 must be 1");
                        assertTrue(rs.next(), "ResultSet must have a second row");
                        assertEquals(2, rs.getInt("c1"), "second row's c1 must be 2");
                        assertFalse(rs.next(), "ResultSet must have exactly 2 rows");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }
    }

    // =========================================================================================
    //  Section 2 — Compound SQL via execute()  (the #2722 / #2940 regression class)
    // =========================================================================================

    /**
     * Compound semicolon-separated SQL via {@code execute()}. Asserts every intermediate update
     * count surfaces and every trailing ResultSet is reachable — the shapes that silently broke in
     * #2722 (fixed in #2737) and broke again in #2940 (fixed by #2941).
     */
    @Nested
    public class CompoundSqlExecute {

        /**
         * #2722 / #2940 literal repro on {@code PreparedStatement.execute}.
         * SQL: {@code DELETE; INSERT(?,?); SELECT}. Before the fix, the trailing SELECT was lost.
         */
        @Test
        public void preparedStatementExecuteDeleteInsertSelect_Issue2940LiteralRepro() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("PSExecDIS"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2)");

                    String sql = "DELETE FROM " + table + " WHERE c1 = 1;"
                            + " INSERT INTO " + table + " (c1, c2) VALUES (?, ?);"
                            + " SELECT * FROM " + table;
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, 3);
                        ps.setInt(2, 30);
                        boolean isResultSet = ps.execute();

                        assertFalse(isResultSet,
                                "execute() must return false because first result is the DELETE update count");
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(1, 1), t.updateCounts,
                                "DELETE (1 row) then INSERT (1 row) must surface as counts [1, 1]");
                        assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                                "trailing SELECT must surface as one ResultSet with 2 rows (1 remaining + 1 inserted)");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * #2737 PR shape: {@code DELETE; INSERT; INSERT; UPDATE; INSERT; SELECT}.
         * Asserts all five DML counts surface in order plus the trailing SELECT.
         */
        @Test
        public void preparedStatementExecuteFiveDmlsPlusSelect_Issue2737Shape() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("PSExecFull"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2), (3, 3)");

                    String sql = "DELETE FROM " + table + ";"
                            + " INSERT INTO " + table + " (c1, c2) VALUES (?, ?);"
                            + " INSERT INTO " + table + " (c1, c2) VALUES (?, ?);"
                            + " UPDATE " + table + " SET c1 = 99;"
                            + " INSERT INTO " + table + " (c1, c2) VALUES (?, ?);"
                            + " SELECT * FROM " + table;
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, 10); ps.setInt(2, 10);
                        ps.setInt(3, 20); ps.setInt(4, 20);
                        ps.setInt(5, 30); ps.setInt(6, 30);
                        boolean isResultSet = ps.execute();

                        assertFalse(isResultSet, "first result is DELETE's update count, not a ResultSet");
                        Traversal t = traverseAllResults(ps, isResultSet);
                        // DELETE=3, INSERT=1, INSERT=1, UPDATE=2 (UPDATE c1=99 on the 2 rows), INSERT=1, SELECT=3 rows
                        assertEquals(Arrays.asList(3, 1, 1, 2, 1), t.updateCounts,
                                "all 5 DML counts must surface in order: DELETE=3, INSERT=1, INSERT=1, UPDATE=2, INSERT=1");
                        assertEquals(Arrays.asList(3), t.resultSetRowCounts,
                                "trailing SELECT must surface as one ResultSet with 3 rows");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Simplest compound on {@link Statement}: {@code INSERT; SELECT}. Statement uses
         * PKT_QUERY → TDS_DONE (not DONEINPROC), so this path was never broken — control test.
         */
        @Test
        public void statementExecuteInsertThenSelect() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("StmtIS"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);

                    boolean isResultSet = stmt.execute(
                            "INSERT INTO " + table + " VALUES (1, 1); SELECT * FROM " + table);

                    assertFalse(isResultSet, "first result is INSERT's update count, not a ResultSet");
                    Traversal t = traverseAllResults(stmt, isResultSet);
                    assertEquals(Arrays.asList(1), t.updateCounts,
                            "INSERT must surface as one update count of 1");
                    assertEquals(Arrays.asList(1), t.resultSetRowCounts,
                            "trailing SELECT must surface as one ResultSet with the inserted row (1 row)");
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Smallest {@link PreparedStatement} case that exercises both halves of #2941
         * (the {@code shouldConsumeInsertDoneToken()} hook and the stale-stmtDoneToken cleanup).
         */
        @Test
        public void preparedStatementExecuteInsertThenSelect() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("PSIS"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + table + " VALUES (?, ?); SELECT * FROM " + table)) {
                        ps.setInt(1, 7);
                        ps.setInt(2, 70);
                        boolean isResultSet = ps.execute();

                        assertFalse(isResultSet,
                                "first result is INSERT's update count; SELECT comes second");
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(1), t.updateCounts,
                                "single INSERT must surface as one update count of 1");
                        assertEquals(Arrays.asList(1), t.resultSetRowCounts,
                                "trailing SELECT must surface as one ResultSet with the just-inserted row");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Interleaved {@code INSERT; SELECT; INSERT; SELECT}. The consume-vs-surface decision and
         * the COLMETADATA stale-token cleanup fire twice — proves both are stateless and idempotent.
         */
        @Test
        public void preparedStatementExecuteInterleavedInsertSelect() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("PSInter"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);

                    String sql = "INSERT INTO " + table + " VALUES (?, ?);"
                            + " SELECT * FROM " + table + ";"
                            + " INSERT INTO " + table + " VALUES (?, ?);"
                            + " SELECT * FROM " + table;
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, 1); ps.setInt(2, 1);
                        ps.setInt(3, 2); ps.setInt(4, 2);
                        boolean isResultSet = ps.execute();

                        assertFalse(isResultSet,
                                "first result is the first INSERT's update count");
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(1, 1), t.updateCounts,
                                "both INSERTs (1 row each) must surface as counts [1, 1] interleaved with the SELECTs");
                        assertEquals(Arrays.asList(1, 2), t.resultSetRowCounts,
                                "first SELECT sees the 1 row from INSERT#1; second SELECT sees both rows after INSERT#2");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** SELECT-first compound: {@code SELECT; INSERT(?,?)}. {@code execute()} must return true. */
        @Test
        public void preparedStatementExecuteSelectThenInsert() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("PSSI"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);

                    try (PreparedStatement ps = conn.prepareStatement(
                            "SELECT * FROM " + table + "; INSERT INTO " + table + " VALUES (?, ?)")) {
                        ps.setInt(1, 5);
                        ps.setInt(2, 50);
                        boolean isResultSet = ps.execute();

                        assertTrue(isResultSet,
                                "execute() must return true because first result is the SELECT");
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(0), t.resultSetRowCounts,
                                "leading SELECT on empty table must surface as one ResultSet with 0 rows");
                        assertEquals(Arrays.asList(1), t.updateCounts,
                                "trailing INSERT must surface as one update count of 1");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** {@code INSERT; INSERT} — both counts surface; no phantom ResultSet at the end. */
        @Test
        public void preparedStatementExecuteTwoInsertsNoTrailingSelect() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("PSII"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);

                    String sql = "INSERT INTO " + table + " VALUES (?, ?);"
                            + " INSERT INTO " + table + " VALUES (?, ?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, 1); ps.setInt(2, 1);
                        ps.setInt(3, 2); ps.setInt(4, 2);
                        boolean isResultSet = ps.execute();

                        assertFalse(isResultSet, "first result is INSERT's update count");
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(1, 1), t.updateCounts,
                                "both INSERTs must surface as counts [1, 1] — no count must be swallowed");
                        assertEquals(0, t.resultSetRowCounts.size(),
                                "no SELECT in payload → no ResultSet must surface (terminal -1 ends loop)");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** Two SELECTs back-to-back surface as two ResultSets, no update counts. */
        @Test
        public void preparedStatementExecuteTwoSelects() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("PSSS"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2), (3, 3)");

                    String sql = "SELECT * FROM " + table + " WHERE c1 < ?;"
                            + " SELECT * FROM " + table + " WHERE c1 >= ?";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, 2);
                        ps.setInt(2, 2);
                        boolean isResultSet = ps.execute();

                        assertTrue(isResultSet, "first result is the first SELECT");
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(0, t.updateCounts.size(),
                                "no DML in payload → no update count must surface");
                        assertEquals(Arrays.asList(1, 2), t.resultSetRowCounts,
                                "SELECT c1<2 returns 1 row, SELECT c1>=2 returns 2 rows");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** Pure DML chain {@code UPDATE; DELETE} — both counts surface. */
        @Test
        public void preparedStatementExecuteUpdateDeleteChain() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("PSUD"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2), (3, 3)");

                    try (PreparedStatement ps = conn.prepareStatement(
                            "UPDATE " + table + " SET c2 = ?; DELETE FROM " + table)) {
                        ps.setInt(1, 99);
                        boolean isResultSet = ps.execute();

                        assertFalse(isResultSet, "first result is UPDATE's count, not a ResultSet");
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(3, 3), t.updateCounts,
                                "UPDATE touches 3 rows, then DELETE removes 3 rows → counts [3, 3]");
                        assertEquals(0, t.resultSetRowCounts.size(),
                                "no SELECT → no ResultSet must surface");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Statement parity for the #2737 five-DML+SELECT shape: {@link Statement} and
         * {@link PreparedStatement} must produce identical traversals for the same compound SQL.
         */
        @Test
        public void statementExecuteFiveDmlsPlusSelectParity() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("StmtFull"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2), (3, 3)");

                    String sql = "DELETE FROM " + table + ";"
                            + " INSERT INTO " + table + " VALUES (10, 10);"
                            + " INSERT INTO " + table + " VALUES (20, 20);"
                            + " UPDATE " + table + " SET c1 = 99;"
                            + " INSERT INTO " + table + " VALUES (30, 30);"
                            + " SELECT * FROM " + table;

                    boolean isResultSet = stmt.execute(sql);

                    assertFalse(isResultSet, "first result is DELETE's update count");
                    Traversal t = traverseAllResults(stmt, isResultSet);
                    assertEquals(Arrays.asList(3, 1, 1, 2, 1), t.updateCounts,
                            "Statement.execute must surface the same 5 counts as PreparedStatement.execute "
                                    + "(DELETE=3, INSERT=1, INSERT=1, UPDATE=2, INSERT=1)");
                    assertEquals(Arrays.asList(3), t.resultSetRowCounts,
                            "trailing SELECT must surface as one ResultSet with 3 rows");
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }
    }

    // =========================================================================================
    //  Section 3 — executeUpdate() contract
    // =========================================================================================

    /**
     * On compound DML, {@code PreparedStatement.executeUpdate} returns the <b>LAST</b> count
     * when {@code lastUpdateCount=true} (default) and the <b>FIRST</b> count when
     * {@code lastUpdateCount=false}. Per JDBC 4.3 spec, {@code executeUpdate} must throw if the
     * SQL produces a ResultSet — that throw is also the inversion proof that a trailing SELECT
     * is reachable.
     */
    @Nested
    public class ExecuteUpdateContracts {

        /** {@code lastUpdateCount=true} (default): {@code executeUpdate("INSERT;UPDATE")} returns LAST count. */
        @Test
        public void preparedStatementExecuteUpdateCompoundReturnsLastCount() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EULast"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2)");

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + table + " VALUES (?, ?); UPDATE " + table + " SET c2 = 99")) {
                        ps.setInt(1, 3);
                        ps.setInt(2, 3);
                        int count = ps.executeUpdate();
                        // 2 pre-existing rows + 1 new INSERT = 3 rows for the trailing UPDATE
                        assertEquals(3, count,
                                "default lastUpdateCount=true: executeUpdate returns LAST count");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** {@code lastUpdateCount=false}: {@code executeUpdate("INSERT;UPDATE")} returns FIRST count. */
        @Test
        public void preparedStatementExecuteUpdateCompoundReturnsFirstCountWhenLastUpdateCountFalse()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EUFirst"));
            try (Connection conn = PrepUtil.getConnection(connectionString + ";lastUpdateCount=false");
                    Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2)");

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + table + " VALUES (?, ?); UPDATE " + table + " SET c2 = 99")) {
                        ps.setInt(1, 3);
                        ps.setInt(2, 3);
                        int count = ps.executeUpdate();
                        assertEquals(1, count,
                                "lastUpdateCount=false: executeUpdate returns FIRST count");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Per JDBC 4.3 spec ({@link Statement#executeUpdate(String)} javadoc), {@code executeUpdate}
         * MUST throw if the SQL produces a ResultSet. Payload uses SELECT-first to guarantee
         * {@code resultSet} is non-null at the line-1106 check; the {@code INSERT; SELECT} shape
         * has a separate pre-existing deviation on {@link Statement} (see
         * {@link #preparedStatementExecuteUpdateThrowsOnTrailingSelect} for the symmetric, working
         * PreparedStatement case).
         */
        @Test
        public void statementExecuteUpdateThrowsOnTrailingSelect() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EUStmtRS"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1)");
                    SQLException ex = assertThrows(SQLException.class, () -> stmt.executeUpdate(
                            "SELECT * FROM " + table + "; INSERT INTO " + table + " VALUES (2, 2)"),
                            "leading SELECT produces a ResultSet → executeUpdate must throw per JDBC spec");
                    assertNotNull(ex.getMessage(),
                            "thrown SQLException must carry a diagnostic message");
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** Symmetric for {@link PreparedStatement}: {@code executeUpdate("INSERT;SELECT")} must throw. */
        @Test
        public void preparedStatementExecuteUpdateThrowsOnTrailingSelect() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EUPSRS"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + table + " VALUES (?, ?); SELECT * FROM " + table)) {
                        ps.setInt(1, 1);
                        ps.setInt(2, 1);
                        assertThrows(SQLException.class, ps::executeUpdate,
                                "trailing SELECT produces a ResultSet → executeUpdate must throw");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** {@code Statement.executeUpdate("SELECT")} — pure SELECT must throw. */
        @Test
        public void statementExecuteUpdateThrowsOnSelectOnly() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EUSelOnly"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    assertThrows(SQLException.class,
                            () -> stmt.executeUpdate("SELECT * FROM " + table),
                            "executeUpdate on a pure SELECT must throw per JDBC spec");
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }
    }

    // =========================================================================================
    //  Section 4 — executeQuery() contract
    // =========================================================================================

    /**
     * mssql-jdbc's {@code executeQuery} consumes leading DML update counts and returns the first
     * ResultSet. Throws {@code R_noResultset} if the SQL produces no ResultSet at all.
     */
    @Nested
    public class ExecuteQueryContracts {

        /** {@code Statement.executeQuery("INSERT;SELECT")} silently consumes the INSERT and returns the SELECT. */
        @Test
        public void statementExecuteQueryConsumesLeadingInsert() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EQLeadStmt"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    try (ResultSet rs = stmt.executeQuery(
                            "INSERT INTO " + table + " VALUES (1, 1); SELECT * FROM " + table)) {
                        assertNotNull(rs, "executeQuery must return a non-null ResultSet (the trailing SELECT)");
                        assertTrue(rs.next(), "ResultSet must have the just-inserted row");
                        assertEquals(1, rs.getInt("c1"), "row's c1 must be 1");
                        assertEquals(1, rs.getInt("c2"), "row's c2 must be 1");
                        assertFalse(rs.next(), "ResultSet must have exactly 1 row");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** {@link PreparedStatement} equivalent of {@code executeQuery("INSERT;SELECT")}. */
        @Test
        public void preparedStatementExecuteQueryConsumesLeadingInsert() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EQLeadPS"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + table + " VALUES (?, ?); SELECT * FROM " + table)) {
                        ps.setInt(1, 42);
                        ps.setInt(2, 4);
                        try (ResultSet rs = ps.executeQuery()) {
                            assertNotNull(rs, "executeQuery must return the trailing SELECT's ResultSet");
                            assertTrue(rs.next(), "ResultSet must have the just-inserted row");
                            assertEquals(42, rs.getInt("c1"), "row's c1 must match the parameter value");
                            assertEquals(4, rs.getInt("c2"), "row's c2 must match the parameter value");
                            assertFalse(rs.next(), "ResultSet must have exactly 1 row");
                        }
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** {@code Statement.executeQuery("UPDATE")} — no ResultSet → must throw {@code R_noResultset}. */
        @Test
        public void statementExecuteQueryThrowsWhenNoResultSetProduced() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EQNoRS"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1)");
                    assertThrows(SQLException.class,
                            () -> stmt.executeQuery("UPDATE " + table + " SET c2 = 9"),
                            "executeQuery on a pure UPDATE must throw because no ResultSet is produced");
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }
    }

    // =========================================================================================
    //  Section 5 — RETURN_GENERATED_KEYS  (#2554, #2587, #2740, #2742)
    // =========================================================================================

    /**
     * After a successful INSERT on an identity table, {@link Statement#getGeneratedKeys()} must
     * return a ResultSet with the generated identity. The driver injects {@code SELECT SCOPE_IDENTITY()}
     * after the INSERT to fetch it. Covers #2554, #2587 (SET NOCOUNT ON), #2740 (trigger), #2742.
     */
    @Nested
    @Tag(Constants.xAzureSQLDW)
    public class GeneratedKeysMatrix {

        /** #2554: {@code Statement.execute(INSERT, RETURN_GENERATED_KEYS)} on plain identity table. */
        @Test
        public void statementExecuteInsertReturnsIdentityKey_Issue2554() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKStmtE"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createIdentityTable(stmt, table);

                    boolean isResultSet = stmt.execute(
                            "INSERT INTO " + table + " (name) VALUES ('row1')",
                            Statement.RETURN_GENERATED_KEYS);
                    assertFalse(isResultSet,
                            "INSERT execute() must return false even with RETURN_GENERATED_KEYS");
                    assertEquals(1, stmt.getUpdateCount(),
                            "INSERT must surface its update count of 1 (gen-keys must not consume it)");

                    try (ResultSet keys = stmt.getGeneratedKeys()) {
                        assertNotNull(keys, "getGeneratedKeys must return a non-null ResultSet");
                        assertTrue(keys.next(),
                                "generated keys ResultSet must have a row for the single INSERT");
                        assertEquals(1, keys.getInt(1),
                                "first identity value must be 1 (IDENTITY(1,1) starts at 1)");
                        assertFalse(keys.next(),
                                "generated keys ResultSet must contain exactly 1 row for one INSERT");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** {@code Statement.executeUpdate(INSERT, RETURN_GENERATED_KEYS)} on plain identity table. */
        @Test
        public void statementExecuteUpdateInsertReturnsIdentityKey() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKStmtU"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createIdentityTable(stmt, table);

                    int count = stmt.executeUpdate(
                            "INSERT INTO " + table + " (name) VALUES ('row1')",
                            Statement.RETURN_GENERATED_KEYS);
                    assertEquals(1, count, "executeUpdate must return INSERT row count of 1");

                    try (ResultSet keys = stmt.getGeneratedKeys()) {
                        assertTrue(keys.next(),
                                "generated keys ResultSet must have a row for the single INSERT");
                        assertEquals(1, keys.getInt(1), "first identity value must be 1");
                        assertFalse(keys.next(), "exactly 1 generated key for 1 row");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** {@code PreparedStatement.execute(INSERT(?), RETURN_GENERATED_KEYS)}. */
        @Test
        public void preparedStatementExecuteInsertReturnsIdentityKey() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKPSE"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createIdentityTable(stmt, table);

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + table + " (name) VALUES (?)",
                            Statement.RETURN_GENERATED_KEYS)) {
                        ps.setString(1, "row1");
                        boolean isResultSet = ps.execute();
                        assertFalse(isResultSet,
                                "PreparedStatement.execute(INSERT) must return false even with RETURN_GENERATED_KEYS");
                        assertEquals(1, ps.getUpdateCount(),
                                "INSERT update count must be 1");
                        try (ResultSet keys = ps.getGeneratedKeys()) {
                            assertTrue(keys.next(),
                                    "generated keys ResultSet must have a row");
                            assertEquals(1, keys.getInt(1),
                                    "first identity value must be 1");
                            assertFalse(keys.next(),
                                    "exactly 1 generated key for 1 row");
                        }
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** {@code PreparedStatement.executeUpdate(INSERT(?), RETURN_GENERATED_KEYS)}. */
        @Test
        public void preparedStatementExecuteUpdateInsertReturnsIdentityKey() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKPSU"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createIdentityTable(stmt, table);

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + table + " (name) VALUES (?)",
                            Statement.RETURN_GENERATED_KEYS)) {
                        ps.setString(1, "row1");
                        int count = ps.executeUpdate();
                        assertEquals(1, count, "executeUpdate must return INSERT row count of 1");
                        try (ResultSet keys = ps.getGeneratedKeys()) {
                            assertTrue(keys.next(),
                                    "generated keys ResultSet must have a row");
                            assertEquals(1, keys.getInt(1),
                                    "first identity value must be 1");
                            assertFalse(keys.next(),
                                    "exactly 1 generated key for 1 row");
                        }
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * #2587 literal repro: {@code SET NOCOUNT ON; INSERT … VALUES(?)} via
         * {@code executeUpdate(RETURN_GENERATED_KEYS)} must not throw {@code R_resultsetGeneratedForUpdate}.
         */
        @Test
        public void preparedStatementExecuteUpdateWithSetNoCountOn_Issue2587LiteralRepro()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKNoCnt"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createIdentityTable(stmt, table);

                    String sql = "SET NOCOUNT ON; INSERT INTO " + table + " (name) VALUES (?);";
                    try (PreparedStatement ps = conn.prepareStatement(sql,
                            Statement.RETURN_GENERATED_KEYS)) {
                        ps.setString(1, "row1");
                        // Must not throw "A result set was generated for update"
                        ps.executeUpdate();
                        try (ResultSet keys = ps.getGeneratedKeys()) {
                            assertTrue(keys.next(),
                                    "generated keys must return a row even with leading SET NOCOUNT ON");
                            assertEquals(1, keys.getInt(1),
                                    "first identity value must be 1");
                            assertFalse(keys.next(),
                                    "exactly 1 generated key for 1 row");
                        }
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * #2740 literal repro: {@code PreparedStatement.execute(INSERT, columnNames={"id"})} into
         * a trigger table must not throw "The statement must be executed before any results can
         * be obtained" on {@code getGeneratedKeys()}.
         */
        @Test
        public void preparedStatementExecuteInsertWithTriggerReturnsGeneratedKey_Issue2740LiteralRepro()
                throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKTrigA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKTrigB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKTrig"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    // Trigger without NOCOUNT — exact #2740 reporter setup
                    createInsertTrigger(stmt, trigger, tableA, tableB, false);

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + tableA + " (name) VALUES (?)",
                            new String[] {"id"})) {
                        ps.setString(1, "test");
                        ps.execute();
                        assertEquals(1, ps.getUpdateCount(),
                                "outer INSERT count must be preserved even though trigger DONE is consumed");
                        try (ResultSet keys = ps.getGeneratedKeys()) {
                            assertTrue(keys.next(),
                                    "generated keys must return a row from the injected SCOPE_IDENTITY() (must not throw 'statement must be executed')");
                            assertEquals(1, keys.getInt(1),
                                    "first identity value must be 1 from the outer INSERT (not the trigger's INSERT)");
                            assertFalse(keys.next(),
                                    "exactly 1 generated key for 1 row");
                        }
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Triple combination — {@code SET NOCOUNT ON} + trigger + {@code RETURN_GENERATED_KEYS}.
         * Exercises #2554, #2587, and #2740 in one shape; any future change must satisfy all three.
         */
        @Test
        public void preparedStatementSetNoCountOnTriggerAndGeneratedKeys() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKTriA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKTriB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKTri"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, true);

                    try (PreparedStatement ps = conn.prepareStatement(
                            "SET NOCOUNT ON; INSERT INTO " + tableA + " (name) VALUES (?);",
                            new String[] {"id"})) {
                        ps.setString(1, "test");
                        ps.execute();
                        try (ResultSet keys = ps.getGeneratedKeys()) {
                            assertTrue(keys.next(),
                                    "generated keys must work with NOCOUNT + trigger + RETURN_GENERATED_KEYS combined");
                            assertEquals(1, keys.getInt(1),
                                    "first identity value must be 1");
                            assertFalse(keys.next(),
                                    "exactly 1 generated key for 1 row");
                        }
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }
    }

    // =========================================================================================
    //  Section 6 — Trigger interactions
    // =========================================================================================

    /**
     * Trigger-noise filtering trade-offs pinned by API. Without {@code SET NOCOUNT ON} in the
     * trigger body, the trigger's own DONEINPROC row count is wire-level indistinguishable from
     * the outer INSERT's DONEINPROC:
     *
     * <p>{@code PreparedStatement.execute()} — post-#2941 — surfaces both (trigger first, outer
     * second). {@code PreparedStatement.executeUpdate()} (default {@code lastUpdateCount=true})
     * skips intermediate DONEINPROCs and returns only the outer count. {@code Statement.execute()}
     * filters the trigger DONE via the base hook and surfaces only the outer count.
     *
     * <p>Microsoft's documented best practice for triggers is to begin the body with
     * {@code SET NOCOUNT ON}, which suppresses the trigger's DONEINPROC row count and makes all
     * APIs consistently surface only the outer INSERT count.
     */
    @Nested
    @Tag(Constants.xAzureSQLDW)
    public class TriggerInteractions {

        /**
         * {@code PreparedStatement.execute}, single-row INSERT into a trigger table, no NOCOUNT.
         * Wire: trigger DONEINPROC (1), outer DONEINPROC (1). Both surface.
         */
        @Test
        public void preparedStatementExecuteSingleRowInsertWithoutNoCountTrigger() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrigSRA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrigSRB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrigSR"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, false);

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + tableA + " (name) VALUES (?)")) {
                        ps.setString(1, "x");
                        boolean isResultSet = ps.execute();

                        assertFalse(isResultSet, "INSERT execute() must return false (no ResultSet)");
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(1, 1), t.updateCounts,
                                "without NOCOUNT: trigger count=1 surfaces first, then outer INSERT count=1");
                        assertEquals(0, t.resultSetRowCounts.size(),
                                "no SELECT in payload → no ResultSet must surface");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * {@code PreparedStatement.execute}, 2-row INSERT, no NOCOUNT.
         * Trigger fires once per statement → counts=[trigger=1, outer=2].
         */
        @Test
        public void preparedStatementExecuteMultiRowInsertWithoutNoCountTrigger() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrigMRA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrigMRB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrigMR"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, false);

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + tableA + " (name) VALUES (?), (?)")) {
                        ps.setString(1, "x");
                        ps.setString(2, "y");
                        boolean isResultSet = ps.execute();

                        assertFalse(isResultSet, "INSERT execute() must return false (no ResultSet)");
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(1, 2), t.updateCounts,
                                "without NOCOUNT: trigger fires once (count=1), then outer INSERT count=2");
                        assertEquals(0, t.resultSetRowCounts.size(),
                                "no SELECT in payload → no ResultSet must surface");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * {@code PreparedStatement.execute}, 2-row INSERT, <b>WITH</b> NOCOUNT.
         * Trigger's DONEINPROC carries no count and is skipped → only the outer count surfaces.
         */
        @Test
        public void preparedStatementExecuteMultiRowInsertWithNoCountTrigger() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrigNCA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrigNCB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrigNC"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, true);

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + tableA + " (name) VALUES (?), (?)")) {
                        ps.setString(1, "x");
                        ps.setString(2, "y");
                        boolean isResultSet = ps.execute();

                        assertFalse(isResultSet, "INSERT execute() must return false (no ResultSet)");
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(2), t.updateCounts,
                                "with SET NOCOUNT ON in trigger body: trigger DONE carries no count → only outer INSERT count=2 surfaces");
                        assertEquals(0, t.resultSetRowCounts.size(),
                                "no SELECT in payload → no ResultSet must surface");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * {@code Statement.execute} on a trigger table without {@code SET NOCOUNT ON} surfaces
         * only the outer INSERT count — the trigger's nested {@code DONEINPROC} is filtered as
         * noise by the base {@code shouldConsumeInsertDoneToken()} hook. Matches Microsoft's
         * <a href="https://learn.microsoft.com/en-us/sql/t-sql/statements/create-trigger-transact-sql">
         * trigger best-practices</a> (use {@code SET NOCOUNT ON} in trigger bodies). The
         * PreparedStatement variant ({@link #preparedStatementExecuteMultiRowInsertWithoutNoCountTrigger})
         * surfaces both counts — its override returns {@code false} to preserve compound-SQL
         * fidelity for #2722 / #2940.
         */
        @Test
        public void statementExecuteMultiRowInsertWithoutNoCountTrigger() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrigStmA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrigStmB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrigStm"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, false);

                    boolean isResultSet = stmt.execute(
                            "INSERT INTO " + tableA + " (name) VALUES ('a'), ('b'), ('c')");

                    assertFalse(isResultSet, "INSERT execute() must return false (no ResultSet)");
                    Traversal t = traverseAllResults(stmt, isResultSet);
                    assertEquals(Arrays.asList(3), t.updateCounts,
                            "Statement.execute must filter the trigger's nested DONEINPROC and surface "
                                    + "only the outer INSERT count=3 (per Microsoft trigger best-practices)");
                    assertEquals(0, t.resultSetRowCounts.size(),
                            "no SELECT in payload → no ResultSet must surface");
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * {@code PreparedStatement.executeUpdate} returns the outer INSERT count regardless of
         * trigger noise — default {@code lastUpdateCount=true} skips intermediate DONEINPROCs.
         */
        @Test
        public void preparedStatementExecuteUpdateMultiRowInsertWithoutNoCountTrigger()
                throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrigEUA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrigEUB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrigEU"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, false);

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + tableA + " (name) VALUES (?), (?), (?)")) {
                        ps.setString(1, "a");
                        ps.setString(2, "b");
                        ps.setString(3, "c");
                        int count = ps.executeUpdate();
                        assertEquals(3, count,
                                "executeUpdate must return outer INSERT count=3; intermediate trigger DONEINPROC must be skipped (default lastUpdateCount=true)");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }
    }

    // =========================================================================================
    //  Section 7 — Mid-batch error recovery  (#2850 / #2866)
    // =========================================================================================

    /**
     * After a mid-batch failure the canonical recovery is to catch the {@link SQLException} and
     * call {@link Statement#getMoreResults()}. The next valid update count must NOT be swallowed
     * (the bug #2850 / fix #2866).
     */
    @Nested
    public class MidBatchErrorRecovery {

        /**
         * #2850 literal repro on {@code Statement.execute}.
         * {@code INSERT(1); INSERT(1) [PK violation]; INSERT(2); SELECT} — after catching the
         * violation, the third INSERT's count and the trailing SELECT must both surface.
         */
        @Test
        public void statementExecuteRecoversValidCountAfterMidBatchError_Issue2850LiteralRepro()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BRStmt"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    stmt.executeUpdate(
                            "CREATE TABLE " + table + " (id INT PRIMARY KEY, name VARCHAR(16))");

                    String sql = "INSERT INTO " + table + " VALUES (1, 'a');"
                            + " INSERT INTO " + table + " VALUES (1, 'dup');"
                            + " INSERT INTO " + table + " VALUES (2, 'b');"
                            + " SELECT * FROM " + table + " ORDER BY id";
                    boolean isResultSet = stmt.execute(sql);

                    Traversal t = traverseWithRecovery(stmt, isResultSet);
                    assertEquals(Arrays.asList(1, 1), t.updateCounts,
                            "first INSERT + third INSERT (third must NOT be swallowed after PK-violation error)");
                    assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                            "trailing SELECT must surface as one ResultSet with 2 rows (id=1 + id=2)");
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** {@link PreparedStatement} variant of the same recovery contract. */
        @Test
        public void preparedStatementExecuteRecoversValidCountAfterMidBatchError() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("BRPS"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    stmt.executeUpdate(
                            "CREATE TABLE " + table + " (id INT PRIMARY KEY, name VARCHAR(16))");

                    String sql = "INSERT INTO " + table + " VALUES (?, ?);"
                            + " INSERT INTO " + table + " VALUES (?, ?);"
                            + " INSERT INTO " + table + " VALUES (?, ?);"
                            + " SELECT * FROM " + table + " ORDER BY id";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, 1); ps.setString(2, "a");
                        ps.setInt(3, 1); ps.setString(4, "dup");
                        ps.setInt(5, 2); ps.setString(6, "b");
                        boolean isResultSet = ps.execute();

                        Traversal t = traverseWithRecovery(ps, isResultSet);
                        assertEquals(Arrays.asList(1, 1), t.updateCounts,
                                "third INSERT count must NOT be swallowed after PK violation (PreparedStatement path)");
                        assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                                "trailing SELECT must surface as one ResultSet with 2 rows after error recovery");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Recovery-aware traversal — same as {@link #traverseAllResults} but catches mid-stream
         * {@code SQLException}s and continues with {@code getMoreResults()}.
         */
        private Traversal traverseWithRecovery(Statement stmt, boolean firstIsResultSet)
                throws SQLException {
            Traversal t = new Traversal();
            boolean isResultSet = firstIsResultSet;
            while (true) {
                try {
                    if (isResultSet) {
                        try (ResultSet rs = stmt.getResultSet()) {
                            int rows = 0;
                            while (rs.next()) {
                                rows++;
                            }
                            t.resultSetRowCounts.add(rows);
                        }
                    } else {
                        int count = stmt.getUpdateCount();
                        if (count == -1) {
                            break;
                        }
                        t.updateCounts.add(count);
                    }
                    isResultSet = stmt.getMoreResults();
                } catch (SQLException e) {
                    // Spec-style recovery: advance past the error.
                    isResultSet = stmt.getMoreResults();
                }
            }
            return t;
        }
    }

    // =========================================================================================
    //  Section 8 — Connection-property variants
    // =========================================================================================

    /**
     * Pins the interaction between each connection knob and the consume-vs-surface decision.
     * Any future pivot must consciously choose what to do under each property.
     */
    @Nested
    public class ConnectionPropertyVariants {

        /**
         * Default {@code lastUpdateCount=true} must not suppress {@code execute()} counts —
         * the property applies only to {@code executeUpdate}.
         */
        @Test
        public void preparedStatementExecuteCompoundUnderDefaultLastUpdateCount() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("PropDef"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + table + " VALUES (?, ?);"
                                    + " INSERT INTO " + table + " VALUES (?, ?);"
                                    + " SELECT * FROM " + table)) {
                        ps.setInt(1, 1); ps.setInt(2, 1);
                        ps.setInt(3, 2); ps.setInt(4, 2);
                        boolean isResultSet = ps.execute();

                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(1, 1), t.updateCounts,
                                "default lastUpdateCount=true must not suppress execute() counts; both INSERT counts must surface");
                        assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                                "trailing SELECT must surface as one ResultSet with 2 rows");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * {@code lastUpdateCount=false} must produce the same {@code execute()} traversal as the
         * default — the property has no observable effect on the {@code execute()} path.
         */
        @Test
        public void preparedStatementExecuteCompoundUnderLastUpdateCountFalseIdenticalToDefault()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("PropF"));
            try (Connection conn = PrepUtil.getConnection(connectionString + ";lastUpdateCount=false");
                    Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + table + " VALUES (?, ?);"
                                    + " INSERT INTO " + table + " VALUES (?, ?);"
                                    + " SELECT * FROM " + table)) {
                        ps.setInt(1, 1); ps.setInt(2, 1);
                        ps.setInt(3, 2); ps.setInt(4, 2);
                        boolean isResultSet = ps.execute();

                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(1, 1), t.updateCounts,
                                "lastUpdateCount=false must produce SAME execute() traversal as default (property is executeUpdate-only)");
                        assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                                "trailing SELECT must still surface as one ResultSet with 2 rows");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * {@code prepareMethod=none} routes {@link PreparedStatement} through PKT_QUERY (TDS_DONE,
         * not DONEINPROC) — the override hook does not fire; compound {@code execute()} works
         * through the base fast path.
         */
        @Test
        public void preparedStatementExecuteCompoundWithPrepareMethodNone() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("PropPM"));
            try (Connection conn = PrepUtil.getConnection(connectionString + ";prepareMethod=none");
                    Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + table + " VALUES (?, ?); SELECT * FROM " + table)) {
                        ps.setInt(1, 1);
                        ps.setInt(2, 1);
                        boolean isResultSet = ps.execute();

                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(1), t.updateCounts,
                                "with prepareMethod=none (PKT_QUERY), single INSERT must surface as count=1 via the base TDS_DONE path");
                        assertEquals(Arrays.asList(1), t.resultSetRowCounts,
                                "with prepareMethod=none, trailing SELECT must still surface as one ResultSet with 1 row");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Leading {@code SET NOCOUNT ON} suppresses all subsequent DML row counts at the wire
         * level — only the trailing SELECT surfaces.
         */
        @Test
        public void preparedStatementExecuteCompoundWithLeadingSetNoCountOn() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("PropNoC"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);

                    String sql = "SET NOCOUNT ON;"
                            + " INSERT INTO " + table + " VALUES (?, ?);"
                            + " INSERT INTO " + table + " VALUES (?, ?);"
                            + " SELECT * FROM " + table;
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, 1); ps.setInt(2, 1);
                        ps.setInt(3, 2); ps.setInt(4, 2);
                        boolean isResultSet = ps.execute();

                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(0, t.updateCounts.size(),
                                "leading SET NOCOUNT ON must suppress every subsequent INSERT update count");
                        assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                                "trailing SELECT must still surface as one ResultSet with 2 rows (NOCOUNT does not affect SELECTs)");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }
    }
}
