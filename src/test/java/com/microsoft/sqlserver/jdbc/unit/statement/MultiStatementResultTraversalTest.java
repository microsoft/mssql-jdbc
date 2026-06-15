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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
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
 * Regression matrix for multi-statement result traversal across {@link Statement},
 * {@link PreparedStatement}, and {@link java.sql.CallableStatement}. Pins {@code execute},
 * {@code executeUpdate}, {@code executeQuery}, and {@code executeBatch} contracts on single and
 * compound SQL using the canonical JDBC §13.1.4 / {@link Statement#getMoreResults()} traversal
 * loop. Every test asserts exact ordered update counts and ResultSet row counts to prevent the
 * silent-exit regressions that let #2722 and #2940 ship. Covers issues #2554, #2587, #2722,
 * #2737, #2740, #2742, #2817, #2850, #2866, #2940, #2941. Trigger tests are tagged
 * {@link Constants#xAzureSQLDW}.
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

        /**
         * Validates PreparedStatement.executeUpdate on a complex compound DML chain
         * (DELETE;INSERT;UPDATE;INSERT;UPDATE) returns the LAST count (UPDATE = 2 rows)
         * under default lastUpdateCount=true.
         */
        @Test
        public void preparedStatementExecuteUpdateComplexCompoundDmlChainReturnsLastCount()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EUCpxLast"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2), (3, 3)");

                    String sql = "DELETE FROM " + table + ";"
                            + " INSERT INTO " + table + " VALUES (?, ?);"
                            + " UPDATE " + table + " SET c2 = 50;"
                            + " INSERT INTO " + table + " VALUES (?, ?);"
                            + " UPDATE " + table + " SET c2 = 99";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, 10); ps.setInt(2, 10);
                        ps.setInt(3, 20); ps.setInt(4, 20);
                        int count = ps.executeUpdate();
                        // DELETE=3, INSERT=1, UPDATE=1, INSERT=1, UPDATE=2 → LAST=2
                        assertEquals(2, count,
                                "executeUpdate on DELETE;INSERT;UPDATE;INSERT;UPDATE must return LAST count "
                                        + "(final UPDATE touched 2 rows); default lastUpdateCount=true returns LAST");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates PreparedStatement.executeUpdate on multi-INSERT compound (INSERT;INSERT;INSERT)
         * returns LAST INSERT's count = 1; all 3 rows must persist.
         */
        @Test
        public void preparedStatementExecuteUpdateMultipleInsertsCompoundReturnsLastCount()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EUMultiIns"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);

                    String sql = "INSERT INTO " + table + " VALUES (?, ?);"
                            + " INSERT INTO " + table + " VALUES (?, ?);"
                            + " INSERT INTO " + table + " VALUES (?, ?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, 1); ps.setInt(2, 1);
                        ps.setInt(3, 2); ps.setInt(4, 2);
                        ps.setInt(5, 3); ps.setInt(6, 3);
                        int count = ps.executeUpdate();
                        assertEquals(1, count,
                                "executeUpdate on INSERT;INSERT;INSERT must return LAST count of 1 "
                                        + "(each single-row INSERT reports 1)");
                    }

                    // Verify all 3 INSERTs actually persisted
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                        assertTrue(rs.next());
                        assertEquals(3, rs.getInt(1),
                                "all 3 INSERTs in the compound payload must have persisted");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates Statement.executeUpdate on compound DML returns FIRST count, not LAST —
         * the lastUpdateCount property is PreparedStatement-only and does NOT apply to the
         * Statement code path. PS would return LAST=2 under default lastUpdateCount=true.
         */
        @Test
        public void statementExecuteUpdateComplexCompoundDmlChainReturnsFirstCount()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EUStmtCpx"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2), (3, 3)");

                    String sql = "DELETE FROM " + table + ";"
                            + " INSERT INTO " + table + " VALUES (10, 10);"
                            + " UPDATE " + table + " SET c2 = 50;"
                            + " INSERT INTO " + table + " VALUES (20, 20);"
                            + " UPDATE " + table + " SET c2 = 99";
                    int count = stmt.executeUpdate(sql);
                    // Statement.executeUpdate returns FIRST count = DELETE = 3 (3 pre-existing
                    // rows removed); lastUpdateCount property does NOT apply to Statement path.
                    assertEquals(3, count,
                            "Statement.executeUpdate returns FIRST count = 3 (DELETE's count); "
                                    + "lastUpdateCount property is PreparedStatement-only and does NOT "
                                    + "apply to the Statement code path");
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates executeUpdate with leading SET NOCOUNT ON on compound DML returns -1
         * (no DONE-with-count tokens on wire), not 0. DML still executes — all 3 INSERTs
         * persist — but no count surfaces. Spec would allow 0; mssql-jdbc returns -1.
         */
        @Test
        public void preparedStatementExecuteUpdateCompoundWithLeadingSetNoCountOnReturnsMinusOne()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EUNoCnt"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);

                    String sql = "SET NOCOUNT ON;"
                            + " INSERT INTO " + table + " VALUES (?, ?);"
                            + " INSERT INTO " + table + " VALUES (?, ?);"
                            + " UPDATE " + table + " SET c2 = 99";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, 1); ps.setInt(2, 1);
                        ps.setInt(3, 2); ps.setInt(4, 2);
                        int count = ps.executeUpdate();
                        assertEquals(-1, count,
                                "leading SET NOCOUNT ON suppresses all DML counts → no DONE-with-count "
                                        + "tokens on wire → executeUpdate returns -1 (terminal sentinel); "
                                        + "mssql-jdbc-specific (spec would allow 0)");
                    }

                    // Verify the DML actually ran despite no counts surfacing
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table
                            + " WHERE c2 = 99")) {
                        assertTrue(rs.next());
                        assertEquals(2, rs.getInt(1),
                                "DML must still execute even though NOCOUNT suppresses counts — "
                                        + "both INSERTs persist and UPDATE applies c2=99 to both");
                    }
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

        /**
         * Validates #2940 compound (DELETE;INSERT) + RGK via execute(). Caller must advance
         * past intermediate counts via getMoreResults() before getGeneratedKeys() — unlike
         * executeUpdate which consumes intermediates internally. Calling getGeneratedKeys()
         * directly throws "statement must be executed".
         */
        @Test
        public void preparedStatementExecuteCompoundDeleteInsertWithGeneratedKeys()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKCmpDIS"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createIdentityTable(stmt, table);
                    // Pre-populate with id=1, id=2 so DELETE has something to remove
                    stmt.executeUpdate("INSERT INTO " + table + " (name) VALUES ('a'), ('b')");

                    String sql = "DELETE FROM " + table + " WHERE name = 'a';"
                            + " INSERT INTO " + table + " (name) VALUES (?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql,
                            Statement.RETURN_GENERATED_KEYS)) {
                        ps.setString(1, "new");
                        boolean isResultSet = ps.execute();
                        assertFalse(isResultSet, "first result must be DELETE's update count");
                        assertEquals(1, ps.getUpdateCount(), "DELETE count must be 1");

                        // Advance past the DELETE count to reach the INSERT count.
                        // (Required before getGeneratedKeys works on compound + RGK shape.)
                        assertFalse(ps.getMoreResults(), "next result is INSERT count (not a ResultSet)");
                        assertEquals(1, ps.getUpdateCount(), "INSERT count must be 1");

                        try (ResultSet keys = ps.getGeneratedKeys()) {
                            assertTrue(keys.next(),
                                    "compound payload + RETURN_GENERATED_KEYS must produce gen-keys for the INSERT");
                            assertEquals(3, keys.getInt(1),
                                    "INSERT in compound payload must produce identity=3 (after pre-populated 1, 2)");
                            assertFalse(keys.next(),
                                    "exactly 1 generated key for the single INSERT in the compound payload");
                        }
                    }

                    // Verify expected rows persisted (DELETE removed 'a', INSERT added 'new')
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                        assertTrue(rs.next());
                        assertEquals(2, rs.getInt(1),
                                "after DELETE+INSERT: 2 rows must remain ('b' + 'new')");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates compound (DELETE;INSERT) + RGK via executeUpdate() returns LAST count = 1
         * (INSERT) and getGeneratedKeys() returns the identity from the INSERT.
         */
        @Test
        public void preparedStatementExecuteUpdateCompoundDeleteInsertWithGeneratedKeys()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKEUCmp"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createIdentityTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " (name) VALUES ('a'), ('b')");

                    String sql = "DELETE FROM " + table + " WHERE name = 'a';"
                            + " INSERT INTO " + table + " (name) VALUES (?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql,
                            Statement.RETURN_GENERATED_KEYS)) {
                        ps.setString(1, "new");
                        int count = ps.executeUpdate();
                        assertEquals(1, count,
                                "executeUpdate on DELETE;INSERT must return LAST count=1 (INSERT count)");

                        try (ResultSet keys = ps.getGeneratedKeys()) {
                            assertTrue(keys.next(),
                                    "executeUpdate + compound + RETURN_GENERATED_KEYS must produce gen-keys");
                            assertEquals(3, keys.getInt(1),
                                    "identity must be 3 (after pre-populated 1, 2)");
                            assertFalse(keys.next(),
                                    "exactly 1 generated key for the single INSERT");
                        }
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates multi-INSERT compound + RGK via execute(). Only ONE update count surfaces
         * (RGK collapses intermediate INSERT counts) — caller advances past it via
         * getMoreResults() then getGeneratedKeys(). Gap G1: SCOPE_IDENTITY() returns only LAST
         * identity, not all three. All 3 rows persist.
         */
        @Test
        public void preparedStatementExecuteCompoundMultipleInsertsReturnsLastIdentityViaScopeIdentity()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKMultiIns"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createIdentityTable(stmt, table);

                    String sql = "INSERT INTO " + table + " (name) VALUES (?);"
                            + " INSERT INTO " + table + " (name) VALUES (?);"
                            + " INSERT INTO " + table + " (name) VALUES (?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql,
                            Statement.RETURN_GENERATED_KEYS)) {
                        ps.setString(1, "r1");
                        ps.setString(2, "r2");
                        ps.setString(3, "r3");
                        ps.execute();

                        // On execute() + multi-INSERT compound + RGK, only ONE count surfaces
                        // (the RGK code path collapses intermediate INSERT counts). Advance once
                        // past that single count to position for getGeneratedKeys().
                        assertEquals(1, ps.getUpdateCount(),
                                "only ONE count surfaces with RGK on multi-INSERT compound; "
                                        + "intermediate INSERT counts are collapsed by the RGK code path "
                                        + "(mssql-jdbc-specific)");
                        ps.getMoreResults();
                        assertEquals(-1, ps.getUpdateCount(),
                                "after advancing past the single surfaced count, no more counts "
                                        + "are visible to the user (SCOPE_IDENTITY RS is reserved for getGeneratedKeys)");

                        try (ResultSet keys = ps.getGeneratedKeys()) {
                            assertTrue(keys.next(),
                                    "getGeneratedKeys must return at least one row");
                            assertEquals(3, keys.getInt(1),
                                    "mssql-jdbc returns LAST identity via SCOPE_IDENTITY() (current behaviour, "
                                            + "known gap G1 for true per-row keys)");
                            assertFalse(keys.next(),
                                    "exactly 1 generated key returned (SCOPE_IDENTITY returns only the last)");
                        }
                    }

                    // Verify all 3 rows actually persisted with identities 1, 2, 3
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                        assertTrue(rs.next());
                        assertEquals(3, rs.getInt(1),
                                "all 3 rows must persist (gap G1 is in gen-keys retrieval, "
                                        + "not INSERT execution)");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates SET NOCOUNT ON + INSERT + RGK via execute() — NOCOUNT suppresses INSERT
         * count, getGeneratedKeys() still retrieves the identity (covers #2587 with compound
         * shape). SQL omits trailing user SELECT — the driver does NOT support user SELECT
         * + RGK (user SELECT and driver SCOPE_IDENTITY RS clash).
         */
        @Test
        public void preparedStatementExecuteCompoundWithNoCountAndInsertAndGeneratedKeys()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKNCSelect"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createIdentityTable(stmt, table);

                    String sql = "SET NOCOUNT ON;"
                            + " INSERT INTO " + table + " (name) VALUES (?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql,
                            Statement.RETURN_GENERATED_KEYS)) {
                        ps.setString(1, "x");
                        ps.execute();

                        try (ResultSet keys = ps.getGeneratedKeys()) {
                            assertTrue(keys.next(),
                                    "NOCOUNT must NOT break SCOPE_IDENTITY() injection for gen-keys");
                            assertEquals(1, keys.getInt(1),
                                    "first identity value must be 1");
                            assertFalse(keys.next(),
                                    "exactly 1 generated key for the single INSERT");
                        }
                    }

                    // Verify the INSERT actually persisted
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                        assertTrue(rs.next());
                        assertEquals(1, rs.getInt(1), "INSERT must have persisted the row");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates SET NOCOUNT ON + (DELETE;INSERT) + RGK via executeUpdate() — NOCOUNT
         * suppresses all counts so executeUpdate returns -1 (mssql-jdbc-specific; spec allows
         * 0), but getGeneratedKeys() still returns the identity (covers #2587 with compound).
         */
        @Test
        public void preparedStatementExecuteUpdateCompoundWithNoCountAndGeneratedKeys()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKEUNoCnt"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createIdentityTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " (name) VALUES ('seed')");

                    String sql = "SET NOCOUNT ON;"
                            + " DELETE FROM " + table + " WHERE name = 'seed';"
                            + " INSERT INTO " + table + " (name) VALUES (?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql,
                            Statement.RETURN_GENERATED_KEYS)) {
                        ps.setString(1, "new");
                        int count = ps.executeUpdate();
                        assertEquals(-1, count,
                                "NOCOUNT suppresses all DML counts → no DONE-with-count on wire → "
                                        + "executeUpdate returns -1 (mssql-jdbc-specific; spec would allow 0)");

                        try (ResultSet keys = ps.getGeneratedKeys()) {
                            assertTrue(keys.next(),
                                    "NOCOUNT on compound must NOT break gen-keys retrieval (#2587 with compound)");
                            assertEquals(2, keys.getInt(1),
                                    "identity must be 2 (seed was 1, new INSERT gets 2 — IDENTITY doesn't reset on DELETE)");
                            assertFalse(keys.next(),
                                    "exactly 1 generated key for the single INSERT");
                        }
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates single INSERT into trigger table + RGK (covers #2740 trigger + gen-keys).
         * Trigger has NOCOUNT. SQL is single-statement — driver does NOT support user SELECT
         * + RGK on compound payloads.
         */
        @Test
        public void preparedStatementExecuteInsertOnTriggerTableWithGeneratedKeys()
                throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKTrSelA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKTrSelB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKTrSel"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, true); // NOCOUNT trigger

                    String sql = "INSERT INTO " + tableA + " (name) VALUES (?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql,
                            new String[] {"id"})) {
                        ps.setString(1, "x");
                        ps.execute();

                        try (ResultSet keys = ps.getGeneratedKeys()) {
                            assertTrue(keys.next(),
                                    "trigger + RETURN_GENERATED_KEYS must produce gen-keys for the outer INSERT");
                            assertEquals(1, keys.getInt(1),
                                    "identity must be 1 from the outer INSERT into tableA (not the trigger's INSERT into tableB)");
                            assertFalse(keys.next(),
                                    "exactly 1 generated key for the single outer INSERT");
                        }
                    }

                    // Verify INSERT into tableA persisted and trigger fired (1 row in tableB)
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableA)) {
                        assertTrue(rs.next());
                        assertEquals(1, rs.getInt(1), "outer INSERT must have persisted in tableA");
                    }
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableB)) {
                        assertTrue(rs.next());
                        assertEquals(1, rs.getInt(1), "trigger must have inserted 1 row into tableB");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates compound (DELETE;INSERT) into trigger table + RGK via executeUpdate()
         * (NOCOUNT trigger) returns LAST count = 1 (outer INSERT) and getGeneratedKeys()
         * returns the identity.
         */
        @Test
        public void preparedStatementExecuteUpdateCompoundOnTriggerTableWithGeneratedKeys()
                throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKTrEUA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKTrEUB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKTrEU"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " (name) VALUES ('seed1'), ('seed2')");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, true); // NOCOUNT trigger

                    String sql = "DELETE FROM " + tableA + " WHERE name = 'seed1';"
                            + " INSERT INTO " + tableA + " (name) VALUES (?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql,
                            new String[] {"id"})) {
                        ps.setString(1, "new");
                        int count = ps.executeUpdate();
                        assertEquals(1, count,
                                "executeUpdate on DELETE;INSERT (with NOCOUNT trigger) must return "
                                        + "LAST count=1 (the outer INSERT's count)");

                        try (ResultSet keys = ps.getGeneratedKeys()) {
                            assertTrue(keys.next(),
                                    "executeUpdate + compound + trigger + RETURN_GENERATED_KEYS must produce gen-keys");
                            assertEquals(3, keys.getInt(1),
                                    "identity must be 3 (seeds were 1, 2; new INSERT gets 3 — IDENTITY doesn't reset)");
                            assertFalse(keys.next(),
                                    "exactly 1 generated key for the single new INSERT");
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
         * Quadruple combination — SET NOCOUNT ON + trigger + compound (DELETE;INSERT) + RGK.
         * All four dimensions interact through the same onDone() / gen-keys injection path;
         * any regression in one will fail this test. SQL omits trailing user SELECT (unsupported
         * with RGK).
         */
        @Test
        public void preparedStatementExecuteComplexCompoundNoCountTriggerAndGeneratedKeys()
                throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKQuadA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKQuadB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("GKQuad"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " (name) VALUES ('old')");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, true); // NOCOUNT trigger

                    String sql = "SET NOCOUNT ON;"
                            + " DELETE FROM " + tableA + " WHERE name = 'old';"
                            + " INSERT INTO " + tableA + " (name) VALUES (?)";
                    try (PreparedStatement ps = conn.prepareStatement(sql,
                            new String[] {"id"})) {
                        ps.setString(1, "new");
                        ps.execute();

                        try (ResultSet keys = ps.getGeneratedKeys()) {
                            assertTrue(keys.next(),
                                    "NOCOUNT + trigger + compound + RETURN_GENERATED_KEYS combo must still produce gen-keys");
                            assertEquals(2, keys.getInt(1),
                                    "identity must be 2 (seed 'old' got 1; new INSERT gets 2 — IDENTITY doesn't reset)");
                            assertFalse(keys.next(),
                                    "exactly 1 generated key for the single outer INSERT in the compound payload");
                        }
                    }

                    // Verify state after execution: tableA has 1 row ('new'), tableB has 1 row (trigger)
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableA)) {
                        assertTrue(rs.next());
                        assertEquals(1, rs.getInt(1),
                                "after DELETE+INSERT: 1 row in tableA (the new row only)");
                    }
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableB)) {
                        assertTrue(rs.next());
                        assertEquals(1, rs.getInt(1), "trigger must have inserted 1 row into tableB");
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
     * Trigger-noise filtering trade-offs by API. Without {@code SET NOCOUNT ON}, the trigger's
     * DONEINPROC is wire-indistinguishable from the outer DONEINPROC:
     * {@code PreparedStatement.execute()} (post-#2941) surfaces both; {@code executeUpdate} and
     * {@code Statement.execute()} surface only the outer count. With {@code SET NOCOUNT ON}
     * (Microsoft best practice), all APIs surface only the outer count.
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
         * Validates Statement.execute on a trigger table without NOCOUNT surfaces only the outer
         * INSERT count — the trigger's nested DONEINPROC is filtered as noise by the base
         * {@code shouldConsumeInsertDoneToken()} hook (per Microsoft trigger best-practices).
         * Contrast with the PreparedStatement variant which surfaces both counts.
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

        // -----------------------------------------------------------------------------------------
        //  Section 6.A — Compound SQL on trigger tables (Statement × execute/executeUpdate/executeQuery × NOCOUNT)
        // -----------------------------------------------------------------------------------------

        /**
         * Validates Statement.execute on compound (DELETE;INSERT;SELECT) over a trigger table
         * (no NOCOUNT) — base hook filters trigger's nested CMD_INSERT DONE as noise, so only
         * outer DELETE and INSERT counts surface plus the trailing SELECT.
         */
        @Test
        public void statementExecuteCompoundOnTriggerTableWithoutNoCount() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSCpA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSCpB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSCp"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " (name) VALUES ('old1'), ('old2')");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, false);

                    String sql = "DELETE FROM " + tableA + " WHERE name = 'old1';"
                            + " INSERT INTO " + tableA + " (name) VALUES ('new');"
                            + " SELECT id, name FROM " + tableA;
                    boolean isResultSet = stmt.execute(sql);

                    assertFalse(isResultSet, "first result must be DELETE's count, not a ResultSet");
                    Traversal t = traverseAllResults(stmt, isResultSet);
                    assertEquals(Arrays.asList(1, 1), t.updateCounts,
                            "Statement.execute filters trigger noise → only outer DELETE count=1 and "
                                    + "outer INSERT count=1 surface (no trigger DONE)");
                    assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                            "trailing SELECT must surface as one ResultSet with 2 rows (old2 + new)");
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates Statement.execute on same compound shape with trigger WITH NOCOUNT — NOCOUNT
         * produces a count-less DONE that's skipped regardless; outcome identical to
         * WITHOUT-NOCOUNT variant on the Statement path.
         */
        @Test
        public void statementExecuteCompoundOnTriggerTableWithNoCount() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSCpNA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSCpNB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSCpN"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " (name) VALUES ('old1'), ('old2')");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, true); // WITH NOCOUNT

                    String sql = "DELETE FROM " + tableA + " WHERE name = 'old1';"
                            + " INSERT INTO " + tableA + " (name) VALUES ('new');"
                            + " SELECT id, name FROM " + tableA;
                    boolean isResultSet = stmt.execute(sql);

                    assertFalse(isResultSet, "first result must be DELETE's count, not a ResultSet");
                    Traversal t = traverseAllResults(stmt, isResultSet);
                    assertEquals(Arrays.asList(1, 1), t.updateCounts,
                            "WITH NOCOUNT in trigger: outer DELETE count=1 and outer INSERT count=1 "
                                    + "surface (identical to WITHOUT-NOCOUNT outcome on Statement path)");
                    assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                            "trailing SELECT must surface as one ResultSet with 2 rows");
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates Statement.executeUpdate on compound (DELETE;INSERT;UPDATE) over a trigger
         * table (no NOCOUNT) returns FIRST count = DELETE = 1 — lastUpdateCount property is
         * PS-only and does NOT apply to Statement path.
         */
        @Test
        public void statementExecuteUpdateCompoundOnTriggerTableWithoutNoCount() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSEUA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSEUB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSEU"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " (name) VALUES ('a'), ('b')");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, false);

                    String sql = "DELETE FROM " + tableA + " WHERE name = 'a';"
                            + " INSERT INTO " + tableA + " (name) VALUES ('c');"
                            + " UPDATE " + tableA + " SET name = 'updated'";
                    int count = stmt.executeUpdate(sql);

                    // Statement.executeUpdate returns FIRST count = DELETE's count = 1;
                    // lastUpdateCount property doesn't apply to Statement code path.
                    assertEquals(1, count,
                            "Statement.executeUpdate returns FIRST count = 1 (DELETE's count); "
                                    + "lastUpdateCount property is PS-only — Statement path always FIRST");
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates Statement.executeUpdate on same shape with trigger WITH NOCOUNT — NOCOUNT
         * suppresses trigger DONE but Statement path returns FIRST = DELETE = 1 regardless
         * (NOCOUNT has no observable effect on this code path).
         */
        @Test
        public void statementExecuteUpdateCompoundOnTriggerTableWithNoCount() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSEUNA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSEUNB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSEUN"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " (name) VALUES ('a'), ('b')");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, true); // WITH NOCOUNT

                    String sql = "DELETE FROM " + tableA + " WHERE name = 'a';"
                            + " INSERT INTO " + tableA + " (name) VALUES ('c');"
                            + " UPDATE " + tableA + " SET name = 'updated'";
                    int count = stmt.executeUpdate(sql);

                    // Same FIRST count = 1 as WITHOUT-NOCOUNT variant — trigger NOCOUNT has no
                    // effect on Statement.executeUpdate (already returns FIRST regardless).
                    assertEquals(1, count,
                            "Statement.executeUpdate returns FIRST count = 1 (DELETE's count); "
                                    + "trigger NOCOUNT has no effect on Statement path");
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates Statement.executeQuery on compound (INSERT;SELECT) over a trigger table
         * (no NOCOUNT) — driver consumes leading INSERT (filtered trigger DONE) and returns
         * the trailing SELECT's RS. WITH-NOCOUNT variant is omitted: executeQuery consumes
         * leading DML regardless of NOCOUNT through indistinguishable code paths.
         */
        @Test
        public void statementExecuteQueryCompoundOnTriggerTableWithoutNoCount() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSEQA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSEQB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSEQ"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, false);

                    String sql = "INSERT INTO " + tableA + " (name) VALUES ('new');"
                            + " SELECT id, name FROM " + tableA;
                    try (ResultSet rs = stmt.executeQuery(sql)) {
                        assertNotNull(rs, "executeQuery must return the trailing SELECT's ResultSet");
                        assertTrue(rs.next(), "ResultSet must have the inserted row");
                        assertEquals(1, rs.getInt("id"), "row's id must be 1 (first INSERT into IDENTITY table)");
                        assertEquals("new", rs.getString("name"), "row's name must be 'new'");
                        assertFalse(rs.next(), "ResultSet must have exactly 1 row");
                    }

                    // Trigger must have fired and inserted into tableB
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableB)) {
                        assertTrue(rs.next());
                        assertEquals(1, rs.getInt(1),
                                "trigger must have fired once and inserted 1 row into tableB");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates {@code Statement.executeQuery} on compound payload with trigger WITH NOCOUNT.
         * NOCOUNT in the trigger has no observable effect on {@code executeQuery} — the driver
         * still consumes leading DML and returns the trailing SELECT's RS.
         */
        @Test
        public void statementExecuteQueryCompoundOnTriggerTableWithNoCount() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSEQNA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSEQNB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrSEQN"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, true); // WITH NOCOUNT

                    String sql = "INSERT INTO " + tableA + " (name) VALUES ('new');"
                            + " SELECT id, name FROM " + tableA;
                    try (ResultSet rs = stmt.executeQuery(sql)) {
                        assertNotNull(rs, "executeQuery must return the trailing SELECT's ResultSet");
                        assertTrue(rs.next(), "ResultSet must have the inserted row");
                        assertEquals("new", rs.getString("name"), "row's name must be 'new'");
                        assertFalse(rs.next(), "ResultSet must have exactly 1 row");
                    }

                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableB)) {
                        assertTrue(rs.next());
                        assertEquals(1, rs.getInt(1),
                                "trigger must have fired once and inserted 1 row into tableB (NOCOUNT doesn't affect execution)");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        // -----------------------------------------------------------------------------------------
        //  Section 6.B — Compound SQL on trigger tables (PreparedStatement × execute/executeUpdate/executeQuery × NOCOUNT)
        // -----------------------------------------------------------------------------------------

        /**
         * Validates PreparedStatement.execute on compound (DELETE;INSERT;SELECT) over a trigger
         * table (no NOCOUNT) — post-#2941, the PS override returns false so ALL DONEs surface
         * including the trigger's nested INSERT count [1, 1, 1] plus the trailing SELECT.
         */
        @Test
        public void preparedStatementExecuteCompoundOnTriggerTableWithoutNoCount() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPCpA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPCpB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPCp"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " (name) VALUES ('old1'), ('old2')");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, false);

                    String sql = "DELETE FROM " + tableA + " WHERE name = 'old1';"
                            + " INSERT INTO " + tableA + " (name) VALUES (?);"
                            + " SELECT id, name FROM " + tableA;
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, "new");
                        boolean isResultSet = ps.execute();
                        assertFalse(isResultSet, "first result must be DELETE's count, not a ResultSet");
                        Traversal t = traverseAllResults(ps, isResultSet);
                        // PS execute() per #2941: override returns false → ALL DONEs surface
                        // including the trigger's nested INSERT DONE between DELETE and outer INSERT
                        assertEquals(Arrays.asList(1, 1, 1), t.updateCounts,
                                "PreparedStatement.execute (post-#2941) surfaces ALL counts: "
                                        + "DELETE=1, trigger nested INSERT=1, outer INSERT=1");
                        assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                                "trailing SELECT must surface as one ResultSet with 2 rows (old2 + new)");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates PreparedStatement.execute on same compound shape with trigger WITH NOCOUNT
         * — NOCOUNT suppresses trigger's DONE row count, only outer DELETE and INSERT counts
         * surface [1, 1] plus the trailing SELECT.
         */
        @Test
        public void preparedStatementExecuteCompoundOnTriggerTableWithNoCount() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPCpNA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPCpNB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPCpN"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " (name) VALUES ('old1'), ('old2')");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, true); // WITH NOCOUNT

                    String sql = "DELETE FROM " + tableA + " WHERE name = 'old1';"
                            + " INSERT INTO " + tableA + " (name) VALUES (?);"
                            + " SELECT id, name FROM " + tableA;
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, "new");
                        boolean isResultSet = ps.execute();
                        assertFalse(isResultSet, "first result must be DELETE's count, not a ResultSet");
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(1, 1), t.updateCounts,
                                "WITH NOCOUNT in trigger: trigger DONE carries no count → only outer "
                                        + "DELETE=1 and outer INSERT=1 surface");
                        assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                                "trailing SELECT must surface as one ResultSet with 2 rows");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates PreparedStatement.executeUpdate on compound (DELETE;INSERT;UPDATE) over a
         * trigger table (no NOCOUNT) — default lastUpdateCount=true returns LAST count =
         * UPDATE = 2; trigger noise skipped.
         */
        @Test
        public void preparedStatementExecuteUpdateCompoundOnTriggerTableWithoutNoCount()
                throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPEUA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPEUB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPEU"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " (name) VALUES ('a'), ('b')");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, false);

                    String sql = "DELETE FROM " + tableA + " WHERE name = 'a';"
                            + " INSERT INTO " + tableA + " (name) VALUES (?);"
                            + " UPDATE " + tableA + " SET name = 'updated'";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, "c");
                        int count = ps.executeUpdate();
                        assertEquals(2, count,
                                "PreparedStatement.executeUpdate (lastUpdateCount=true) returns LAST "
                                        + "count = 2 (final UPDATE touched 2 surviving rows); trigger noise skipped");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates PreparedStatement.executeUpdate on compound + trigger WITH NOCOUNT under
         * lastUpdateCount=false — returns FIRST count = DELETE = 1. Pins the executeUpdate ×
         * property × trigger interaction.
         */
        @Test
        public void preparedStatementExecuteUpdateCompoundOnTriggerTableWithNoCountAndLastUpdateCountFalse()
                throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPEUNA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPEUNB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPEUN"));
            try (Connection conn = PrepUtil.getConnection(connectionString + ";lastUpdateCount=false");
                    Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " (name) VALUES ('a'), ('b')");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, true); // WITH NOCOUNT

                    String sql = "DELETE FROM " + tableA + " WHERE name = 'a';"
                            + " INSERT INTO " + tableA + " (name) VALUES (?);"
                            + " UPDATE " + tableA + " SET name = 'updated'";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, "c");
                        int count = ps.executeUpdate();
                        assertEquals(1, count,
                                "lastUpdateCount=false: executeUpdate returns FIRST count = 1 "
                                        + "(DELETE's count); WITH NOCOUNT in trigger doesn't change semantics");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates PreparedStatement.executeQuery on compound (INSERT;SELECT) over a trigger
         * table (no NOCOUNT) — driver consumes leading INSERT (filtered trigger DONE) and
         * returns the trailing SELECT's RS. WITH-NOCOUNT variant is omitted: executeQuery
         * consumes leading DML regardless of NOCOUNT through indistinguishable code paths.
         */
        @Test
        public void preparedStatementExecuteQueryCompoundOnTriggerTableWithoutNoCount() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPEQA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPEQB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPEQ"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, false);

                    String sql = "INSERT INTO " + tableA + " (name) VALUES (?);"
                            + " SELECT id, name FROM " + tableA;
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, "new");
                        try (ResultSet rs = ps.executeQuery()) {
                            assertNotNull(rs, "executeQuery must return the trailing SELECT's ResultSet");
                            assertTrue(rs.next(), "ResultSet must have the inserted row");
                            assertEquals(1, rs.getInt("id"), "row's id must be 1");
                            assertEquals("new", rs.getString("name"), "row's name must be 'new'");
                            assertFalse(rs.next(), "ResultSet must have exactly 1 row");
                        }
                    }

                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableB)) {
                        assertTrue(rs.next());
                        assertEquals(1, rs.getInt(1),
                                "trigger must have fired once and inserted 1 row into tableB");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates {@code PreparedStatement.executeQuery} on compound payload with trigger
         * WITH NOCOUNT — identical outcome to WITHOUT-NOCOUNT for executeQuery (the API consumes
         * leading DML and trigger noise regardless of NOCOUNT setting).
         */
        @Test
        public void preparedStatementExecuteQueryCompoundOnTriggerTableWithNoCount() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPEQNA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPEQNB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TrPEQN"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, true); // WITH NOCOUNT

                    String sql = "INSERT INTO " + tableA + " (name) VALUES (?);"
                            + " SELECT id, name FROM " + tableA;
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setString(1, "new");
                        try (ResultSet rs = ps.executeQuery()) {
                            assertNotNull(rs, "executeQuery must return the trailing SELECT's ResultSet");
                            assertTrue(rs.next(), "ResultSet must have the inserted row");
                            assertEquals("new", rs.getString("name"), "row's name must be 'new'");
                            assertFalse(rs.next(), "ResultSet must have exactly 1 row");
                        }
                    }

                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableB)) {
                        assertTrue(rs.next());
                        assertEquals(1, rs.getInt(1),
                                "trigger must have fired once and inserted 1 row into tableB (NOCOUNT doesn't affect execution)");
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

    // =========================================================================================
    //  Section 9 — Stored procedure & CallableStatement
    // =========================================================================================

    /**
     * Stored procedures take the {@code procedureName != null} branch in {@code onDone()} — a
     * code path not exercised by the inline-SQL sections. Also verifies
     * {@link java.sql.CallableStatement} inherits PreparedStatement's #2941 fix for compound SQL.
     */
    @Nested
    public class StoredProcedureAndCallable {

        /**
         * Validates proc body (DELETE;INSERT;INSERT;SELECT) — all DML counts and the trailing
         * SELECT must surface through the procedureName != null branch of onDone().
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void callableStatementExecuteProcWithMultipleDmlAndSelect() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPDml"));
            String proc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPDmlProc"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2), (3, 3)");
                    stmt.executeUpdate("CREATE PROCEDURE " + proc + " AS BEGIN "
                            + "DELETE FROM " + table + "; "
                            + "INSERT INTO " + table + " VALUES (10, 10); "
                            + "INSERT INTO " + table + " VALUES (20, 20); "
                            + "SELECT * FROM " + table + "; "
                            + "END");

                    try (CallableStatement cs = conn.prepareCall("{call " + proc + "}")) {
                        boolean isResultSet = cs.execute();
                        Traversal t = traverseAllResults(cs, isResultSet);
                        assertEquals(Arrays.asList(3, 1, 1), t.updateCounts,
                                "proc body DML counts (DELETE=3, INSERT=1, INSERT=1) must all surface");
                        assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                                "trailing SELECT inside proc must surface as one ResultSet with 2 rows");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates proc body with SET NOCOUNT ON + INSERT + SELECT — NOCOUNT suppresses the
         * INSERT count, trailing SELECT still surfaces (covers #2587 with stored-proc path).
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void callableStatementExecuteProcWithSetNoCountOnAndSelect() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPNoCnt"));
            String proc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPNoCntProc"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("CREATE PROCEDURE " + proc + " AS BEGIN "
                            + "SET NOCOUNT ON; "
                            + "INSERT INTO " + table + " VALUES (1, 1); "
                            + "SELECT * FROM " + table + "; "
                            + "END");

                    try (CallableStatement cs = conn.prepareCall("{call " + proc + "}")) {
                        boolean isResultSet = cs.execute();
                        Traversal t = traverseAllResults(cs, isResultSet);
                        assertEquals(0, t.updateCounts.size(),
                                "SET NOCOUNT ON in proc body must suppress the INSERT update count");
                        assertEquals(Arrays.asList(1), t.resultSetRowCounts,
                                "trailing SELECT in proc must still surface (NOCOUNT does not suppress SELECTs)");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates proc returning only a ResultSet — trailing RETSTATUS token must be consumed
         * via procedureRetStatToken without producing a phantom result.
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void callableStatementExecuteProcReturningResultSetOnly() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPRsOnly"));
            String proc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPRsOnlyProc"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2)");
                    stmt.executeUpdate("CREATE PROCEDURE " + proc + " AS BEGIN "
                            + "SET NOCOUNT ON; SELECT * FROM " + table + "; END");

                    try (CallableStatement cs = conn.prepareCall("{call " + proc + "}")) {
                        boolean isResultSet = cs.execute();
                        assertTrue(isResultSet, "execute() must return true because proc's first result is a ResultSet");
                        Traversal t = traverseAllResults(cs, isResultSet);
                        assertEquals(0, t.updateCounts.size(),
                                "no DML in proc body → no update count must surface");
                        assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                                "single SELECT from proc must surface as exactly one ResultSet with 2 rows");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates proc returning ResultSet + OUT param — OUT delivered as RETVALUE token
         * after the RS; consuming the RS must not interfere with subsequent getInt() on OUT.
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void callableStatementExecuteProcWithResultSetAndOutParam() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPRsOut"));
            String proc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPRsOutProc"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2), (3, 3)");
                    stmt.executeUpdate("CREATE PROCEDURE " + proc + " @rowCount INT OUTPUT AS BEGIN "
                            + "SET NOCOUNT ON; "
                            + "SELECT * FROM " + table + "; "
                            + "SELECT @rowCount = COUNT(*) FROM " + table + "; "
                            + "END");

                    try (CallableStatement cs = conn.prepareCall("{call " + proc + "(?)}")) {
                        cs.registerOutParameter(1, Types.INTEGER);
                        boolean isResultSet = cs.execute();
                        Traversal t = traverseAllResults(cs, isResultSet);
                        assertEquals(Arrays.asList(3), t.resultSetRowCounts,
                                "proc's SELECT must surface as one ResultSet with 3 rows");
                        assertEquals(3, cs.getInt(1),
                                "OUT parameter must be 3 (COUNT(*)) after consuming all results");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates CallableStatement parity for #2940 literal repro (DELETE;INSERT;SELECT) —
         * the #2941 fix in SQLServerPreparedStatement must be inherited by
         * SQLServerCallableStatement.
         */
        @Test
        public void callableStatementExecuteCompoundDeleteInsertSelectParityWithPreparedStatement()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("CSCompound"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2)");

                    String sql = "DELETE FROM " + table + " WHERE c1 = 1;"
                            + " INSERT INTO " + table + " (c1, c2) VALUES (?, ?);"
                            + " SELECT * FROM " + table;
                    try (CallableStatement cs = conn.prepareCall(sql)) {
                        cs.setInt(1, 3);
                        cs.setInt(2, 30);
                        boolean isResultSet = cs.execute();
                        Traversal t = traverseAllResults(cs, isResultSet);
                        assertEquals(Arrays.asList(1, 1), t.updateCounts,
                                "CallableStatement must inherit PreparedStatement behaviour for compound SQL");
                        assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                                "trailing SELECT must surface on CallableStatement path");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates stored proc emitting two back-to-back ResultSets — both must surface as
         * distinct ResultSets in order through the proc-path multi-RS interleaving.
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void callableStatementExecuteProcReturningMultipleResultSets() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPMultiRs"));
            String proc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPMultiRsProc"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2), (3, 3)");
                    stmt.executeUpdate("CREATE PROCEDURE " + proc + " AS BEGIN "
                            + "SET NOCOUNT ON; "
                            + "SELECT * FROM " + table + " WHERE c1 < 2; "
                            + "SELECT * FROM " + table + " WHERE c1 >= 2; "
                            + "END");

                    try (CallableStatement cs = conn.prepareCall("{call " + proc + "}")) {
                        boolean isResultSet = cs.execute();
                        assertTrue(isResultSet, "first result from proc must be the first SELECT's ResultSet");
                        Traversal t = traverseAllResults(cs, isResultSet);
                        assertEquals(0, t.updateCounts.size(),
                                "no DML in proc body (NOCOUNT on) → no update count must surface");
                        assertEquals(Arrays.asList(1, 2), t.resultSetRowCounts,
                                "both SELECTs must surface in order: c1<2 → 1 row, c1>=2 → 2 rows");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates {@code {? = call sp(?)}} return-value parameter — proc's RETURN value flows
         * via the RETSTATUS token to the registered OUT parameter at index 1.
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void callableStatementExecuteProcWithReturnValueParameter() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPRet"));
            String proc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPRetProc"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("CREATE PROCEDURE " + proc + " AS BEGIN "
                            + "SET NOCOUNT ON; "
                            + "INSERT INTO " + table + " VALUES (1, 1); "
                            + "RETURN 42; "
                            + "END");

                    try (CallableStatement cs = conn.prepareCall("{? = call " + proc + "}")) {
                        cs.registerOutParameter(1, Types.INTEGER);
                        cs.execute();
                        assertEquals(42, cs.getInt(1),
                                "proc return-value (RETSTATUS token) must be retrievable as the registered OUT parameter");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates proc with INTERLEAVED multi-DML and multi-RS (INSERT;SELECT;UPDATE;SELECT;
         * DELETE;SELECT) — all 3 DML counts AND all 3 ResultSets surface in exact order
         * through the proc-path interleaving in onDone() / onColMetaData().
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void callableStatementExecuteProcWithInterleavedDmlAndResultSets() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPInter"));
            String proc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPInterProc"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2)");
                    stmt.executeUpdate("CREATE PROCEDURE " + proc + " AS BEGIN "
                            + "INSERT INTO " + table + " VALUES (3, 3); "          // count=1
                            + "SELECT c1 FROM " + table + " ORDER BY c1; "         // RS with 3 rows
                            + "UPDATE " + table + " SET c2 = 99; "                 // count=3
                            + "SELECT c2 FROM " + table + " WHERE c2 = 99; "       // RS with 3 rows
                            + "DELETE FROM " + table + " WHERE c1 = 1; "           // count=1
                            + "SELECT COUNT(*) AS remaining FROM " + table + "; "  // RS with 1 row (value=2)
                            + "END");

                    try (CallableStatement cs = conn.prepareCall("{call " + proc + "}")) {
                        boolean isResultSet = cs.execute();
                        assertFalse(isResultSet,
                                "proc's first result is INSERT's count (not a ResultSet)");
                        Traversal t = traverseAllResults(cs, isResultSet);
                        assertEquals(Arrays.asList(1, 3, 1), t.updateCounts,
                                "all 3 DML counts must surface in proc body order: "
                                        + "INSERT=1, UPDATE=3, DELETE=1");
                        assertEquals(Arrays.asList(3, 3, 1), t.resultSetRowCounts,
                                "all 3 SELECTs must surface as distinct ResultSets in order: "
                                        + "[3 rows after INSERT, 3 rows after UPDATE, 1 row after DELETE]");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        // -----------------------------------------------------------------------------------------
        //  Section 9.A — Stored proc × compound × triggers × NOCOUNT × lastUpdateCount matrix
        // -----------------------------------------------------------------------------------------

        /**
         * Validates proc body (DELETE;INSERT;UPDATE;SELECT) over a trigger table (no NOCOUNT)
         * — proc-path surfaces ALL counts including trigger's nested INSERT DONE in arrival
         * order, plus trailing SELECT.
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void callableStatementExecuteProcWithCompoundDmlOnTriggerTableWithoutNoCount()
                throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPCpTrA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPCpTrB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPCpTr"));
            String proc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPCpTrProc"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " (name) VALUES ('old1'), ('old2')");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, false); // WITHOUT NOCOUNT
                    stmt.executeUpdate("CREATE PROCEDURE " + proc + " AS BEGIN "
                            + "DELETE FROM " + tableA + " WHERE name = 'old1'; "
                            + "INSERT INTO " + tableA + " (name) VALUES ('new'); "
                            + "UPDATE " + tableA + " SET name = 'updated'; "
                            + "SELECT id, name FROM " + tableA + "; "
                            + "END");

                    try (CallableStatement cs = conn.prepareCall("{call " + proc + "}")) {
                        boolean isResultSet = cs.execute();
                        assertFalse(isResultSet, "first result is DELETE count, not a ResultSet");
                        Traversal t = traverseAllResults(cs, isResultSet);
                        // DELETE=1, trigger_INSERT=1 (no NOCOUNT), outer_INSERT=1, UPDATE=2
                        assertEquals(Arrays.asList(1, 1, 1, 2), t.updateCounts,
                                "proc body without NOCOUNT must surface ALL counts in order: "
                                        + "DELETE=1, trigger INSERT=1, outer INSERT=1, UPDATE=2");
                        assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                                "trailing SELECT must surface as one ResultSet with 2 rows (old2 + updated)");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates same proc/compound shape with trigger WITH NOCOUNT — trigger's DONE is
         * suppressed, only proc's own DML counts surface (DELETE=1, INSERT=1, UPDATE=2).
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void callableStatementExecuteProcWithCompoundDmlOnTriggerTableWithNoCount()
                throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPCpTNA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPCpTNB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPCpTN"));
            String proc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPCpTNProc"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " (name) VALUES ('old1'), ('old2')");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, true); // WITH NOCOUNT
                    stmt.executeUpdate("CREATE PROCEDURE " + proc + " AS BEGIN "
                            + "DELETE FROM " + tableA + " WHERE name = 'old1'; "
                            + "INSERT INTO " + tableA + " (name) VALUES ('new'); "
                            + "UPDATE " + tableA + " SET name = 'updated'; "
                            + "SELECT id, name FROM " + tableA + "; "
                            + "END");

                    try (CallableStatement cs = conn.prepareCall("{call " + proc + "}")) {
                        boolean isResultSet = cs.execute();
                        assertFalse(isResultSet, "first result is DELETE count, not a ResultSet");
                        Traversal t = traverseAllResults(cs, isResultSet);
                        // DELETE=1, trigger DONE suppressed by NOCOUNT, outer_INSERT=1, UPDATE=2
                        assertEquals(Arrays.asList(1, 1, 2), t.updateCounts,
                                "trigger WITH NOCOUNT suppresses its DONE → only proc's own counts surface: "
                                        + "DELETE=1, outer INSERT=1, UPDATE=2");
                        assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                                "trailing SELECT must surface as one ResultSet with 2 rows");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Proc body begins with {@code SET NOCOUNT ON} → suppresses every DML count regardless of
         * trigger. Only the trailing SELECT surfaces.
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void callableStatementExecuteProcWithSetNoCountOnInBodyAndCompoundDmlAndTrigger()
                throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPNCBdyA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPNCBdyB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPNCBdy"));
            String proc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPNCBdyProc"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " (name) VALUES ('seed')");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    // Trigger WITHOUT NOCOUNT — proc's SET NOCOUNT ON should still suppress everything
                    createInsertTrigger(stmt, trigger, tableA, tableB, false);
                    stmt.executeUpdate("CREATE PROCEDURE " + proc + " AS BEGIN "
                            + "SET NOCOUNT ON; "
                            + "DELETE FROM " + tableA + " WHERE name = 'seed'; "
                            + "INSERT INTO " + tableA + " (name) VALUES ('a'); "
                            + "INSERT INTO " + tableA + " (name) VALUES ('b'); "
                            + "UPDATE " + tableA + " SET name = 'updated'; "
                            + "SELECT id, name FROM " + tableA + "; "
                            + "END");

                    try (CallableStatement cs = conn.prepareCall("{call " + proc + "}")) {
                        boolean isResultSet = cs.execute();
                        assertTrue(isResultSet,
                                "proc-level SET NOCOUNT ON suppresses all DML counts → first result is the SELECT's ResultSet");
                        Traversal t = traverseAllResults(cs, isResultSet);
                        assertEquals(0, t.updateCounts.size(),
                                "SET NOCOUNT ON in proc body must suppress every DML count "
                                        + "(DELETE, trigger INSERTs, outer INSERTs, UPDATE — all silenced)");
                        assertEquals(Arrays.asList(2), t.resultSetRowCounts,
                                "trailing SELECT must surface as one ResultSet with 2 rows (a, b after UPDATE)");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates CallableStatement.executeUpdate on stored proc with compound DML always
         * returns FIRST count regardless of lastUpdateCount property — the proc-path branch
         * in onDone() (procedureName != null) returns counts in arrival order without applying
         * the LAST-vs-FIRST selection. Paired with the LastUpdateCountFalse test to catch any
         * future change that adds proc-path lastUpdateCount support.
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void callableStatementExecuteUpdateProcWithCompoundDmlAndLastUpdateCountTrue()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPEULast"));
            String proc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPEULastProc"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2), (3, 3)");
                    stmt.executeUpdate("CREATE PROCEDURE " + proc + " AS BEGIN "
                            + "DELETE FROM " + table + "; "                          // count=3 (FIRST)
                            + "INSERT INTO " + table + " VALUES (10, 10); "          // count=1
                            + "INSERT INTO " + table + " VALUES (20, 20); "          // count=1
                            + "UPDATE " + table + " SET c2 = 99; "                   // count=2
                            + "END");

                    try (CallableStatement cs = conn.prepareCall("{call " + proc + "}")) {
                        int count = cs.executeUpdate();
                        // Default lastUpdateCount=true is IGNORED on the stored-proc path;
                        // executeUpdate returns FIRST count from the proc body (DELETE=3),
                        // not the LAST count (UPDATE=2) it would return for inline compound SQL.
                        assertEquals(3, count,
                                "stored proc executeUpdate returns FIRST count from proc body (DELETE=3); "
                                        + "lastUpdateCount=true does NOT apply to stored proc path "
                                        + "(unlike inline compound SQL where it returns LAST count)");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates same proc body under lastUpdateCount=false returns SAME FIRST count
         * (DELETE=3) — proving lastUpdateCount has no effect on stored-proc executeUpdate path.
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void callableStatementExecuteUpdateProcWithCompoundDmlAndLastUpdateCountFalse()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPEUFst"));
            String proc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPEUFstProc"));
            try (Connection conn = PrepUtil.getConnection(connectionString + ";lastUpdateCount=false");
                    Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2), (3, 3)");
                    stmt.executeUpdate("CREATE PROCEDURE " + proc + " AS BEGIN "
                            + "DELETE FROM " + table + "; "                          // count=3 (FIRST)
                            + "INSERT INTO " + table + " VALUES (10, 10); "          // count=1
                            + "INSERT INTO " + table + " VALUES (20, 20); "          // count=1
                            + "UPDATE " + table + " SET c2 = 99; "                   // count=2
                            + "END");

                    try (CallableStatement cs = conn.prepareCall("{call " + proc + "}")) {
                        int count = cs.executeUpdate();
                        // Same FIRST count = 3 as the paired LAST test — proves lastUpdateCount
                        // property has no effect on the stored-proc executeUpdate path.
                        assertEquals(3, count,
                                "lastUpdateCount=false returns same FIRST count = 3 as default "
                                        + "lastUpdateCount=true — property has no effect on stored proc path");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates proc.executeUpdate on compound DML over trigger table (no NOCOUNT) under
         * default lastUpdateCount=true returns FIRST count = DELETE = 1 — lastUpdateCount does
         * NOT apply to stored-proc executeUpdate path.
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void callableStatementExecuteUpdateProcWithCompoundDmlOnTriggerTableLastUpdateCountTrue()
                throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPEUTrA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPEUTrB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPEUTr"));
            String proc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPEUTrProc"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " (name) VALUES ('old1'), ('old2')");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, false); // WITHOUT NOCOUNT
                    stmt.executeUpdate("CREATE PROCEDURE " + proc + " AS BEGIN "
                            + "DELETE FROM " + tableA + " WHERE name = 'old1'; "    // count=1 (FIRST)
                            + "INSERT INTO " + tableA + " (name) VALUES ('new'); "  // count=1 + trigger=1
                            + "UPDATE " + tableA + " SET name = 'updated'; "        // count=2
                            + "END");

                    try (CallableStatement cs = conn.prepareCall("{call " + proc + "}")) {
                        int count = cs.executeUpdate();
                        // Default lastUpdateCount=true is IGNORED on the stored-proc path;
                        // returns FIRST count = DELETE's count = 1, same as the FALSE variant.
                        assertEquals(1, count,
                                "stored proc executeUpdate returns FIRST count = 1 (DELETE's count); "
                                        + "lastUpdateCount=true does NOT apply to stored proc path");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates same proc/trigger setup under lastUpdateCount=false returns SAME FIRST
         * count = 1 — property has no effect on stored-proc executeUpdate even with triggers.
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void callableStatementExecuteUpdateProcWithCompoundDmlOnTriggerTableLastUpdateCountFalse()
                throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPEUTFA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPEUTFB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPEUTF"));
            String proc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPEUTFProc"));
            try (Connection conn = PrepUtil.getConnection(connectionString + ";lastUpdateCount=false");
                    Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " (name) VALUES ('old1'), ('old2')");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    createInsertTrigger(stmt, trigger, tableA, tableB, false); // WITHOUT NOCOUNT
                    stmt.executeUpdate("CREATE PROCEDURE " + proc + " AS BEGIN "
                            + "DELETE FROM " + tableA + " WHERE name = 'old1'; "    // count=1 (FIRST)
                            + "INSERT INTO " + tableA + " (name) VALUES ('new'); "
                            + "UPDATE " + tableA + " SET name = 'updated'; "
                            + "END");

                    try (CallableStatement cs = conn.prepareCall("{call " + proc + "}")) {
                        int count = cs.executeUpdate();
                        assertEquals(1, count,
                                "lastUpdateCount=false returns same FIRST count = 1 (DELETE's count) "
                                        + "as default lastUpdateCount=true — property has no effect on "
                                        + "stored proc path even with trigger in play");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates quintuple combo: proc body + compound DML + trigger (no NOCOUNT) +
         * proc-level SET NOCOUNT ON + multi-SELECT. Proc-level NOCOUNT wins — all DML counts
         * suppressed, both SELECTs surface as distinct ResultSets.
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void callableStatementExecuteProcWithSetNoCountOnAndCompoundDmlAndTriggerAndMultipleSelects()
                throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPQuintA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPQuintB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPQuint"));
            String proc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPQuintProc"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " (name) VALUES ('old')");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    // Trigger WITHOUT NOCOUNT — proc-level NOCOUNT should still silence everything
                    createInsertTrigger(stmt, trigger, tableA, tableB, false);
                    stmt.executeUpdate("CREATE PROCEDURE " + proc + " AS BEGIN "
                            + "SET NOCOUNT ON; "
                            + "DELETE FROM " + tableA + " WHERE name = 'old'; "
                            + "INSERT INTO " + tableA + " (name) VALUES ('a'); "
                            + "SELECT id, name FROM " + tableA + "; "         // RS#1: 1 row (a)
                            + "INSERT INTO " + tableA + " (name) VALUES ('b'); "
                            + "UPDATE " + tableA + " SET name = name + '!'; "
                            + "SELECT id, name FROM " + tableA + " ORDER BY id; " // RS#2: 2 rows (a!, b!)
                            + "END");

                    try (CallableStatement cs = conn.prepareCall("{call " + proc + "}")) {
                        boolean isResultSet = cs.execute();
                        assertTrue(isResultSet,
                                "proc-level SET NOCOUNT ON → first result is the first SELECT's ResultSet");
                        Traversal t = traverseAllResults(cs, isResultSet);
                        assertEquals(0, t.updateCounts.size(),
                                "proc-level SET NOCOUNT ON must suppress every DML count "
                                        + "(DELETE, trigger INSERTs, outer INSERTs, UPDATE — all silenced)");
                        assertEquals(Arrays.asList(1, 2), t.resultSetRowCounts,
                                "both SELECTs must surface in order: [1 row after first INSERT, "
                                        + "2 rows after second INSERT+UPDATE]");
                    }
                } finally {
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }
    }

    // =========================================================================================
    //  Section 10 — executeBatch() result handling
    // =========================================================================================

    /**
     * Validates {@code executeBatch()} per-batch-item update count semantics for both
     * {@link Statement} and {@link PreparedStatement}. Uses {@code EXECUTE_BATCH} code path
     * with its own DONE-token handling in {@code onDone()}.
     */
    @Nested
    public class ExecuteBatchHandling {

        /** {@code Statement.addBatch + executeBatch} on mixed DML — one count per batch item. */
        @Test
        public void statementExecuteBatchReturnsOneCountPerItem() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EBStmt"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);

                    stmt.addBatch("INSERT INTO " + table + " VALUES (1, 1)");
                    stmt.addBatch("INSERT INTO " + table + " VALUES (2, 2), (3, 3)");
                    stmt.addBatch("UPDATE " + table + " SET c2 = 99");
                    int[] counts = stmt.executeBatch();

                    assertEquals(3, counts.length,
                            "executeBatch must return one count per addBatch item");
                    assertEquals(1, counts[0],
                            "batch item 0 (INSERT 1 row) must report 1");
                    assertEquals(2, counts[1],
                            "batch item 1 (INSERT 2 rows) must report 2");
                    assertEquals(3, counts[2],
                            "batch item 2 (UPDATE 3 rows) must report 3");
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /** {@code PreparedStatement.addBatch + executeBatch} — one count per parameter set. */
        @Test
        public void preparedStatementExecuteBatchReturnsOneCountPerParameterSet() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EBPS"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + table + " VALUES (?, ?)")) {
                        ps.setInt(1, 1); ps.setInt(2, 1); ps.addBatch();
                        ps.setInt(1, 2); ps.setInt(2, 2); ps.addBatch();
                        ps.setInt(1, 3); ps.setInt(2, 3); ps.addBatch();
                        int[] counts = ps.executeBatch();

                        assertEquals(3, counts.length,
                                "executeBatch must return one count per parameter-set addBatch");
                        for (int i = 0; i < counts.length; i++) {
                            assertEquals(1, counts[i],
                                    "each single-row INSERT batch item must report 1 (index " + i + ")");
                        }
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Mid-batch failure: {@code addBatch + executeBatch} where the second item is a PK
         * violation. JDBC requires a {@link java.sql.BatchUpdateException} with one element per
         * item including the failure. The failed item is reported as {@link Statement#EXECUTE_FAILED}.
         */
        @Test
        public void statementExecuteBatchReportsExecuteFailedOnMidBatchPkViolation() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EBErr"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    stmt.executeUpdate("CREATE TABLE " + table
                            + " (id INT PRIMARY KEY, name VARCHAR(16))");

                    stmt.addBatch("INSERT INTO " + table + " VALUES (1, 'a')");
                    stmt.addBatch("INSERT INTO " + table + " VALUES (1, 'dup')");
                    stmt.addBatch("INSERT INTO " + table + " VALUES (2, 'b')");

                    int[] counts;
                    try {
                        stmt.executeBatch();
                        counts = new int[0];
                    } catch (java.sql.BatchUpdateException bue) {
                        counts = bue.getUpdateCounts();
                    }

                    assertEquals(3, counts.length,
                            "BatchUpdateException must carry one count per addBatch item");
                    assertEquals(1, counts[0],
                            "first INSERT (success) must report 1");
                    assertEquals(Statement.EXECUTE_FAILED, counts[1],
                            "second INSERT (PK violation) must report EXECUTE_FAILED");
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }
    }

    // =========================================================================================
    //  Section 11 — Cross-product property combinations
    // =========================================================================================

    /**
     * Cross-products of dimensions that already have single-dimension coverage but combine in
     * ways the driver code paths can interact unexpectedly. Examples: trigger + lastUpdateCount,
     * trigger + prepareMethod=none, gen-keys + lastUpdateCount=false, gen-keys +
     * prepareMethod=none.
     */
    @Nested
    public class PropertyCrossProducts {

        /**
         * Trigger + {@code lastUpdateCount=false}: connection property only affects
         * {@code executeUpdate}, so {@code PreparedStatement.execute} still surfaces both trigger
         * and outer counts identically to default.
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void preparedStatementExecuteOnTriggerTableWithLastUpdateCountFalse() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("XTrigLucA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("XTrigLucB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("XTrigLuc"));
            try (Connection conn = PrepUtil.getConnection(connectionString + ";lastUpdateCount=false");
                    Statement stmt = conn.createStatement()) {
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
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(1, 2), t.updateCounts,
                                "lastUpdateCount=false must NOT change execute() trigger behaviour — same [trigger=1, outer=2]");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates that {@code prepareMethod=none} (prepare-strategy knob) does NOT change
         * compound-SQL traversal: runtime class is still PreparedStatement, override still fires,
         * both trigger and outer counts surface. Setting it must not re-introduce #2940.
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void preparedStatementExecuteOnTriggerTableWithPrepareMethodNone() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("XTrigPMA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("XTrigPMB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("XTrigPM"));
            try (Connection conn = PrepUtil.getConnection(connectionString + ";prepareMethod=none");
                    Statement stmt = conn.createStatement()) {
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
                        boolean isResultSet = ps.execute();
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(1, 3), t.updateCounts,
                                "prepareMethod=none must NOT change PreparedStatement.execute trigger-noise "
                                        + "behaviour — both trigger count=1 and outer INSERT count=3 surface, "
                                        + "identical to default prepareMethod");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Generated keys + {@code lastUpdateCount=false}: the connection property must not break
         * the {@code SCOPE_IDENTITY()} injection path. INSERT count must still be 1 and
         * {@code getGeneratedKeys()} must return the identity.
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void preparedStatementGeneratedKeysWithLastUpdateCountFalse() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("XGKLuc"));
            try (Connection conn = PrepUtil.getConnection(connectionString + ";lastUpdateCount=false");
                    Statement stmt = conn.createStatement()) {
                try {
                    createIdentityTable(stmt, table);

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + table + " (name) VALUES (?)",
                            Statement.RETURN_GENERATED_KEYS)) {
                        ps.setString(1, "row1");
                        int count = ps.executeUpdate();
                        assertEquals(1, count,
                                "INSERT count must be 1 regardless of lastUpdateCount=false");
                        try (ResultSet keys = ps.getGeneratedKeys()) {
                            assertTrue(keys.next(),
                                    "lastUpdateCount=false must not break generated-keys retrieval");
                            assertEquals(1, keys.getInt(1),
                                    "first identity value must be 1");
                            assertFalse(keys.next(),
                                    "exactly 1 generated key");
                        }
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

    }

    // =========================================================================================
    //  Section 12 — DDL, MERGE, and SELECT INTO in compound SQL
    // =========================================================================================

    /**
     * Validates DDL/MERGE/SELECT INTO traversal. Per JDBC spec, DDL surfaces as count=0
     * ("0 for SQL statements that return nothing") — a valid result distinct from -1.
     */
    @Nested
    public class DdlAndMergeAndSelectInto {

        /**
         * {@code CREATE TABLE; INSERT; SELECT} via {@code Statement.execute}. CREATE TABLE
         * surfaces as count=0 per JDBC spec; INSERT surfaces as count=1; SELECT surfaces as
         * a ResultSet. All three results are visible to the canonical traversal loop.
         */
        @Test
        public void statementExecuteCreateTableInsertSelect() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("DdlCIS"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    String sql = "CREATE TABLE " + table + " (c1 INT, c2 SMALLINT);"
                            + " INSERT INTO " + table + " VALUES (1, 1);"
                            + " SELECT * FROM " + table;

                    boolean isResultSet = stmt.execute(sql);
                    Traversal t = traverseAllResults(stmt, isResultSet);
                    assertEquals(Arrays.asList(0, 1), t.updateCounts,
                            "CREATE TABLE must surface as count=0 (per JDBC spec: 0 for SQL statements that return nothing); "
                                    + "INSERT must surface as count=1");
                    assertEquals(Arrays.asList(1), t.resultSetRowCounts,
                            "trailing SELECT must surface as one ResultSet with the inserted row");
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * {@code MERGE} statement with no trailing semicolon-statement. MERGE returns a single
         * row count for the total rows affected (inserts + updates + deletes combined).
         */
        @Test
        public void statementExecuteMergeReturnsTotalAffectedRowCount() throws SQLException {
            String src = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("MrgSrc"));
            String tgt = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("MrgTgt"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, src);
                    createPlainTable(stmt, tgt);
                    stmt.executeUpdate("INSERT INTO " + src + " VALUES (1, 10), (2, 20), (3, 30)");
                    stmt.executeUpdate("INSERT INTO " + tgt + " VALUES (1, 99), (4, 40)");

                    String mergeSql = "MERGE INTO " + tgt + " AS t"
                            + " USING " + src + " AS s ON t.c1 = s.c1"
                            + " WHEN MATCHED THEN UPDATE SET t.c2 = s.c2"
                            + " WHEN NOT MATCHED BY TARGET THEN INSERT (c1, c2) VALUES (s.c1, s.c2)"
                            + ";"; // MERGE statement must terminate with semicolon
                    boolean isResultSet = stmt.execute(mergeSql);

                    assertFalse(isResultSet, "MERGE must return false (no ResultSet)");
                    Traversal t = traverseAllResults(stmt, isResultSet);
                    // MERGE affects: 1 row updated (c1=1), 2 rows inserted (c1=2,3) = 3 total
                    assertEquals(Arrays.asList(3), t.updateCounts,
                            "MERGE must surface a single total affected count (1 UPDATE + 2 INSERT = 3)");
                    assertEquals(0, t.resultSetRowCounts.size(),
                            "MERGE without OUTPUT clause must not produce a ResultSet");
                } finally {
                    TestUtils.dropTableIfExists(tgt, stmt);
                    TestUtils.dropTableIfExists(src, stmt);
                }
            }
        }

        /**
         * {@code SELECT INTO new_table} — hybrid DDL+DML. Creates the table AND inserts rows in
         * one statement, returning the inserted row count.
         */
        @Test
        public void statementExecuteSelectIntoReturnsRowCount() throws SQLException {
            String src = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SelIntoSrc"));
            String dst = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SelIntoDst"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, src);
                    stmt.executeUpdate("INSERT INTO " + src + " VALUES (1, 1), (2, 2), (3, 3)");

                    boolean isResultSet = stmt.execute(
                            "SELECT * INTO " + dst + " FROM " + src);

                    assertFalse(isResultSet, "SELECT INTO must return false (no ResultSet)");
                    Traversal t = traverseAllResults(stmt, isResultSet);
                    assertEquals(Arrays.asList(3), t.updateCounts,
                            "SELECT INTO must surface one count equal to rows copied (3)");
                    assertEquals(0, t.resultSetRowCounts.size(),
                            "SELECT INTO must not produce a ResultSet to the caller");
                } finally {
                    TestUtils.dropTableIfExists(dst, stmt);
                    TestUtils.dropTableIfExists(src, stmt);
                }
            }
        }
    }

    // =========================================================================================
    //  Section 13 — Trigger varieties (UPDATE / DELETE / INSTEAD OF / nested-DML body)
    // =========================================================================================

    /**
     * Trigger varieties beyond AFTER INSERT (which is covered in Section 6). Each variety
     * touches a different {@code CMD_*} code path in {@code StreamDone.getCurCmd()} and
     * different conditions in {@code shouldConsumeInsertDoneToken()}.
     */
    @Nested
    @Tag(Constants.xAzureSQLDW)
    public class TriggerVarieties {

        /**
         * Validates AFTER UPDATE trigger that does an INSERT — PreparedStatement.execute()
         * surfaces both the trigger's nested CMD_INSERT count and the outer CMD_UPDATE count
         * (per #2941 design: override returns {@code false} to preserve compound-SQL fidelity).
         */
        @Test
        public void preparedStatementExecuteOnUpdateTriggerTable() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TUpdA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TUpdB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TUpd"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createPlainTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " VALUES (1, 1), (2, 2), (3, 3)");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    stmt.executeUpdate("CREATE TRIGGER " + trigger + " ON " + tableA
                            + " FOR UPDATE AS INSERT INTO " + tableB + " DEFAULT VALUES");

                    try (PreparedStatement ps = conn.prepareStatement(
                            "UPDATE " + tableA + " SET c2 = ?")) {
                        ps.setInt(1, 99);
                        boolean isResultSet = ps.execute();
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(1, 3), t.updateCounts,
                                "PreparedStatement.execute surfaces both trigger CMD_INSERT count=1 and "
                                        + "outer CMD_UPDATE count=3 (override returns false on execute() path)");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates AFTER DELETE trigger that does an INSERT — PreparedStatement.execute()
         * surfaces both the trigger's CMD_INSERT count and the outer CMD_DELETE count
         * (symmetric to the AFTER UPDATE variant).
         */
        @Test
        public void preparedStatementExecuteOnDeleteTriggerTable() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TDelA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TDelB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TDel"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createPlainTable(stmt, tableA);
                    stmt.executeUpdate("INSERT INTO " + tableA + " VALUES (1, 1), (2, 2)");
                    stmt.executeUpdate("CREATE TABLE " + tableB
                            + " (id INT NOT NULL IDENTITY(1,1) PRIMARY KEY)");
                    stmt.executeUpdate("CREATE TRIGGER " + trigger + " ON " + tableA
                            + " FOR DELETE AS INSERT INTO " + tableB + " DEFAULT VALUES");

                    try (PreparedStatement ps = conn.prepareStatement(
                            "DELETE FROM " + tableA)) {
                        boolean isResultSet = ps.execute();
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(1, 2), t.updateCounts,
                                "PreparedStatement.execute surfaces both trigger CMD_INSERT count=1 and "
                                        + "outer CMD_DELETE count=2 (override returns false on execute() path)");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * INSTEAD OF INSERT trigger — the outer INSERT statement is NOT executed; the trigger's
         * body runs in its place. The reported count is whatever the trigger body produces.
         */
        @Test
        public void preparedStatementExecuteOnInsteadOfInsertTriggerTable() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TIofA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TIofB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TIof"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createPlainTable(stmt, tableA);
                    createPlainTable(stmt, tableB);
                    // INSTEAD OF redirects the INSERT entirely into tableB
                    stmt.executeUpdate("CREATE TRIGGER " + trigger + " ON " + tableA
                            + " INSTEAD OF INSERT AS INSERT INTO " + tableB
                            + " SELECT * FROM inserted");

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + tableA + " VALUES (?, ?)")) {
                        ps.setInt(1, 1);
                        ps.setInt(2, 1);
                        ps.execute();
                        // Outer INSERT contributes no rows (suppressed by INSTEAD OF);
                        // trigger body's INSERT contributes 1 row to tableB.
                        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableA)) {
                            assertTrue(rs.next());
                            assertEquals(0, rs.getInt(1),
                                    "INSTEAD OF INSERT must suppress the outer INSERT — tableA must be empty");
                        }
                        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableB)) {
                            assertTrue(rs.next());
                            assertEquals(1, rs.getInt(1),
                                    "INSTEAD OF trigger body must have inserted 1 row into tableB");
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
         * Validates the scope of the trigger-noise filter: it filters {@code CMD_INSERT} DONEs
         * only. A trigger body with nested INSERT+UPDATE — the INSERT is filtered as noise but
         * the UPDATE surfaces. Expected counts=[trigger UPDATE=2, outer INSERT=2]; trigger's
         * nested INSERT count=1 is filtered.
         */
        @Test
        public void statementExecuteOnTriggerWithCompoundBody() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TBodyA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TBodyB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TBody"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    createPlainTable(stmt, tableB);
                    stmt.executeUpdate("INSERT INTO " + tableB + " VALUES (1, 1)");
                    stmt.executeUpdate("CREATE TRIGGER " + trigger + " ON " + tableA
                            + " FOR INSERT AS BEGIN "
                            + "INSERT INTO " + tableB + " VALUES (99, 99); "
                            + "UPDATE " + tableB + " SET c2 = c2 + 1; "
                            + "END");

                    boolean isResultSet = stmt.execute(
                            "INSERT INTO " + tableA + " (name) VALUES ('a'), ('b')");
                    Traversal t = traverseAllResults(stmt, isResultSet);
                    assertEquals(Arrays.asList(2, 2), t.updateCounts,
                            "trigger-noise filter scope is CMD_INSERT only; trigger body's UPDATE count=2 "
                                    + "surfaces (NOT filtered), and outer INSERT count=2 also surfaces");
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * INSTEAD OF UPDATE trigger — the outer UPDATE is suppressed; the trigger body runs in
         * its place. Pins symmetric behaviour to {@link #preparedStatementExecuteOnInsteadOfInsertTriggerTable}.
         */
        @Test
        public void preparedStatementExecuteOnInsteadOfUpdateTriggerTable() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TIofUA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TIofUB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TIofU"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createPlainTable(stmt, tableA);
                    createPlainTable(stmt, tableB);
                    stmt.executeUpdate("INSERT INTO " + tableA + " VALUES (1, 1), (2, 2)");
                    // INSTEAD OF redirects the UPDATE entirely into tableB instead of tableA
                    stmt.executeUpdate("CREATE TRIGGER " + trigger + " ON " + tableA
                            + " INSTEAD OF UPDATE AS INSERT INTO " + tableB
                            + " SELECT c1, c2 FROM inserted");

                    try (PreparedStatement ps = conn.prepareStatement(
                            "UPDATE " + tableA + " SET c2 = ?")) {
                        ps.setInt(1, 99);
                        ps.execute();
                        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableA
                                + " WHERE c2 = 99")) {
                            assertTrue(rs.next());
                            assertEquals(0, rs.getInt(1),
                                    "INSTEAD OF UPDATE must suppress the outer UPDATE — no row in tableA must have c2=99");
                        }
                        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableB)) {
                            assertTrue(rs.next());
                            assertEquals(2, rs.getInt(1),
                                    "INSTEAD OF trigger body must have inserted 2 rows into tableB (one per inserted-pseudotable row)");
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
         * INSTEAD OF DELETE trigger — the outer DELETE is suppressed; the trigger body runs in
         * its place. Pins symmetric behaviour to the INSERT/UPDATE INSTEAD OF variants.
         */
        @Test
        public void preparedStatementExecuteOnInsteadOfDeleteTriggerTable() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TIofDA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TIofDB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TIofD"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createPlainTable(stmt, tableA);
                    createPlainTable(stmt, tableB);
                    stmt.executeUpdate("INSERT INTO " + tableA + " VALUES (1, 1), (2, 2), (3, 3)");
                    // INSTEAD OF DELETE preserves tableA and only logs the deleted rows to tableB
                    stmt.executeUpdate("CREATE TRIGGER " + trigger + " ON " + tableA
                            + " INSTEAD OF DELETE AS INSERT INTO " + tableB
                            + " SELECT c1, c2 FROM deleted");

                    try (PreparedStatement ps = conn.prepareStatement(
                            "DELETE FROM " + tableA)) {
                        ps.execute();
                        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableA)) {
                            assertTrue(rs.next());
                            assertEquals(3, rs.getInt(1),
                                    "INSTEAD OF DELETE must suppress the outer DELETE — tableA must still have all 3 rows");
                        }
                        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableB)) {
                            assertTrue(rs.next());
                            assertEquals(3, rs.getInt(1),
                                    "INSTEAD OF trigger body must have logged 3 rows into tableB (one per deleted-pseudotable row)");
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
         * Validates compound-SQL recovery model when a trigger body raises an error:
         * {@code execute()} returns normally, the trigger's nested INSERT count surfaces first,
         * then the {@code RAISERROR} surfaces as a {@link SQLException} on the next traversal
         * step (same pattern as Section 7 mid-batch error recovery).
         */
        @Test
        public void preparedStatementExecuteOnTriggerWithRaiserror() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TRaA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TRaB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TRa"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    createPlainTable(stmt, tableB);
                    stmt.executeUpdate("CREATE TRIGGER " + trigger + " ON " + tableA
                            + " FOR INSERT AS BEGIN "
                            + "INSERT INTO " + tableB + " VALUES (99, 99); "
                            + "RAISERROR ('trigger-side error', 16, 1); "
                            + "END");

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + tableA + " (name) VALUES (?)")) {
                        ps.setString(1, "test");
                        // execute() must NOT throw eagerly (recovery model)
                        boolean isResultSet = ps.execute();
                        assertFalse(isResultSet,
                                "execute() must return false (first result is the trigger's INSERT count, not a ResultSet)");

                        // Traverse with recovery — the trigger's nested INSERT count surfaces
                        // first, then the RAISERROR surfaces as SQLException on the next step.
                        SQLException raised = null;
                        List<Integer> countsBeforeError = new ArrayList<>();
                        try {
                            int c = ps.getUpdateCount();
                            assertTrue(c >= 0,
                                    "first result must be the trigger's nested INSERT count, not -1");
                            countsBeforeError.add(c);
                            ps.getMoreResults();
                        } catch (SQLException e) {
                            raised = e;
                        }

                        assertNotNull(raised,
                                "RAISERROR(16) in trigger body must surface as SQLException during traversal");
                        assertTrue(raised.getMessage() != null
                                && raised.getMessage().contains("trigger-side error"),
                                "SQLException message must include the RAISERROR text; got: " + raised.getMessage());
                        assertEquals(Arrays.asList(1), countsBeforeError,
                                "trigger's nested INSERT count=1 must surface before the RAISERROR");
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates self-recursive AFTER INSERT trigger: SQL Server's default
         * {@code RECURSIVE_TRIGGERS=OFF} prevents infinite recursion, so the trigger fires once
         * and inserts its sentinel row. Final table has 2 rows: user's row + trigger's row.
         */
        @Test
        public void preparedStatementExecuteOnRecursiveTriggerTable() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TRecur"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TRecurTrig"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(table, stmt);
                    createPlainTable(stmt, table);
                    // The trigger inserts a 'marker' row when c1 != 99 (preventing infinite recursion
                    // even if RECURSIVE_TRIGGERS were on)
                    stmt.executeUpdate("CREATE TRIGGER " + trigger + " ON " + table
                            + " FOR INSERT AS BEGIN "
                            + "IF NOT EXISTS (SELECT 1 FROM inserted WHERE c1 = 99) "
                            + "INSERT INTO " + table + " VALUES (99, 99); "
                            + "END");

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + table + " VALUES (?, ?)")) {
                        ps.setInt(1, 1);
                        ps.setInt(2, 1);
                        ps.execute();
                        try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                            assertTrue(rs.next());
                            assertEquals(2, rs.getInt(1),
                                    "recursive trigger must produce 2 rows total: user's (1,1) + trigger's (99,99)");
                        }
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Validates trigger body that calls a stored procedure: the proc's NOCOUNT-suppressed
         * DONEs are filtered, only the outer INSERT count surfaces.
         */
        @Test
        @Tag(Constants.xAzureSQLDW)
        public void statementExecuteOnTriggerThatCallsStoredProcedure() throws SQLException {
            String tableA = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TProcA"));
            String tableB = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TProcB"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TProc"));
            String proc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TProcSP"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                    createIdentityTable(stmt, tableA);
                    createPlainTable(stmt, tableB);
                    stmt.executeUpdate("CREATE PROCEDURE " + proc + " AS BEGIN "
                            + "SET NOCOUNT ON; "
                            + "INSERT INTO " + tableB + " VALUES (99, 99); "
                            + "END");
                    stmt.executeUpdate("CREATE TRIGGER " + trigger + " ON " + tableA
                            + " FOR INSERT AS EXEC " + proc);

                    boolean isResultSet = stmt.execute(
                            "INSERT INTO " + tableA + " (name) VALUES ('a'), ('b')");
                    Traversal t = traverseAllResults(stmt, isResultSet);
                    assertEquals(Arrays.asList(2), t.updateCounts,
                            "trigger-body proc with NOCOUNT must contribute no noise; only outer INSERT count=2 surfaces");
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropProcedureIfExists(proc, stmt);
                    TestUtils.dropTableIfExists(tableB, stmt);
                    TestUtils.dropTableIfExists(tableA, stmt);
                }
            }
        }

        /**
         * Validates trigger emitting a SELECT — on modern SQL Server
         * ({@code disallow_results_from_triggers=ON} default) the INSERT throws and the table
         * stays empty; on older servers the SELECT surfaces and the INSERT succeeds. Test
         * accepts either outcome — the contract is that the driver must not crash.
         */
        @Test
        public void preparedStatementExecuteOnTriggerThatEmitsResultSet() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TRsT"));
            String trigger = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TRs"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(table, stmt);
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("CREATE TRIGGER " + trigger + " ON " + table
                            + " FOR INSERT AS BEGIN "
                            + "SET NOCOUNT ON; "
                            + "SELECT 'noise' AS trigger_result; "
                            + "END");

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + table + " VALUES (?, ?)")) {
                        ps.setInt(1, 1);
                        ps.setInt(2, 1);
                        // On modern SQL Server (disallow_results_from_triggers=ON by default),
                        // the trigger's SELECT causes the INSERT to fail.
                        // If the server allows it (older default), the SELECT's ResultSet would
                        // surface — accept either outcome but ensure the driver doesn't crash.
                        try {
                            ps.execute();
                            // If we get here, the server allowed the trigger-side SELECT —
                            // the row should still have been inserted.
                            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                                assertTrue(rs.next());
                                assertEquals(1, rs.getInt(1),
                                        "if trigger SELECT is allowed, the INSERT must still succeed");
                            }
                        } catch (SQLException expected) {
                            // disallow_results_from_triggers=ON: the INSERT must have failed
                            // and the row must NOT have been inserted.
                            try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                                assertTrue(rs.next());
                                assertEquals(0, rs.getInt(1),
                                        "with disallow_results_from_triggers=ON, the INSERT must fail and leave the table empty");
                            }
                        }
                    }
                } finally {
                    TestUtils.dropTriggerIfExists(trigger, stmt);
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }
    }

    // =========================================================================================
    //  Section 14 — Edge cases
    // =========================================================================================

    /**
     * Edge cases that test specific niche behaviours not covered by the main matrix.
     */
    @Nested
    public class EdgeCases {

        /**
         * Empty ResultSets in compound SQL: {@code SELECT WHERE 1=0; INSERT; SELECT WHERE 1=0}.
         * A 0-row ResultSet is still a ResultSet — it must surface as an empty RS with row
         * count 0, not be silently dropped.
         */
        @Test
        public void preparedStatementExecuteCompoundWithEmptyResultSets() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EEmptyRs"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);

                    String sql = "SELECT * FROM " + table + " WHERE 1=0;"
                            + " INSERT INTO " + table + " VALUES (?, ?);"
                            + " SELECT * FROM " + table + " WHERE 1=0";
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, 1);
                        ps.setInt(2, 1);
                        boolean isResultSet = ps.execute();
                        assertTrue(isResultSet, "first result must be the empty SELECT");
                        Traversal t = traverseAllResults(ps, isResultSet);
                        assertEquals(Arrays.asList(1), t.updateCounts,
                                "middle INSERT count=1 must surface");
                        assertEquals(Arrays.asList(0, 0), t.resultSetRowCounts,
                                "both empty SELECTs must surface as 0-row ResultSets — empty RS is still an RS");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * {@code getMoreResults(CLOSE_ALL_RESULTS)} mode: explicit verification that the mode flag
         * is honoured. The default mode ({@code CLOSE_CURRENT_RESULT}) is covered by every test
         * via {@code traverseAllResults}; this test pins the alternative mode.
         */
        @Test
        public void preparedStatementGetMoreResultsCloseAllResultsMode() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EGMRMode"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);
                    stmt.executeUpdate("INSERT INTO " + table + " VALUES (1, 1), (2, 2)");

                    try (PreparedStatement ps = conn.prepareStatement(
                            "SELECT * FROM " + table + "; SELECT * FROM " + table)) {
                        assertTrue(ps.execute(),
                                "first result must be the first SELECT");
                        ResultSet rs1 = ps.getResultSet();
                        assertNotNull(rs1, "first ResultSet must be retrievable");

                        boolean isResultSet = ps.getMoreResults(Statement.CLOSE_ALL_RESULTS);
                        assertTrue(isResultSet, "second result must also be a ResultSet");
                        assertTrue(rs1.isClosed(),
                                "first ResultSet must be closed by CLOSE_ALL_RESULTS mode");
                        try (ResultSet rs2 = ps.getResultSet()) {
                            assertNotNull(rs2, "second ResultSet must be retrievable");
                            int rows = 0;
                            while (rs2.next()) {
                                rows++;
                            }
                            assertEquals(2, rows, "second SELECT must return both rows");
                        }
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Update count after error then continue then SELECT — on {@link PreparedStatement}.
         * Verifies that the recovery semantics (#2850 / #2866 fix) also work on the PS path
         * when the failing statement is in the middle of a longer compound payload.
         */
        @Test
        public void preparedStatementExecuteWithMidBatchErrorFollowedByCountAndSelect()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("EErrCS"));
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                try {
                    stmt.executeUpdate("CREATE TABLE " + table
                            + " (id INT PRIMARY KEY, name VARCHAR(16))");

                    String sql = "INSERT INTO " + table + " VALUES (?, ?);"
                            + " INSERT INTO " + table + " VALUES (?, ?);"   // PK violation
                            + " UPDATE " + table + " SET name = 'x';"
                            + " SELECT * FROM " + table;
                    try (PreparedStatement ps = conn.prepareStatement(sql)) {
                        ps.setInt(1, 1); ps.setString(2, "a");
                        ps.setInt(3, 1); ps.setString(4, "dup");
                        boolean isResultSet = ps.execute();

                        // Use the same recovery pattern as Section 7.
                        List<Integer> counts = new ArrayList<>();
                        List<Integer> rsRows = new ArrayList<>();
                        boolean errorSeen = false;
                        while (true) {
                            try {
                                if (isResultSet) {
                                    try (ResultSet rs = ps.getResultSet()) {
                                        int rows = 0;
                                        while (rs.next()) {
                                            rows++;
                                        }
                                        rsRows.add(rows);
                                    }
                                } else {
                                    int c = ps.getUpdateCount();
                                    if (c == -1) {
                                        break;
                                    }
                                    counts.add(c);
                                }
                                isResultSet = ps.getMoreResults();
                            } catch (SQLException e) {
                                errorSeen = true;
                                isResultSet = ps.getMoreResults();
                            }
                        }

                        assertTrue(errorSeen, "PK violation must surface as SQLException");
                        assertEquals(Arrays.asList(1, 1), counts,
                                "first INSERT count + post-error UPDATE count must both surface");
                        assertEquals(Arrays.asList(1), rsRows,
                                "trailing SELECT must surface with the single surviving row");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }
    }

    // =========================================================================================
    //  Section 15 — Specialized property dimensions (smoke tests; dedicated suites exist)
    // =========================================================================================

    /**
     * Narrow smoke checks — one per dimension — verifying #2941's consume-vs-surface contract
     * is not broken by alternative code paths. NOT comprehensive coverage; each dimension has
     * its own dedicated test suite elsewhere in the project.
     */
    @Nested
    public class SpecializedPropertyDimensions {

        /**
         * Smoke-validates {@code useBulkCopyForBatchInsert=true}: PreparedStatement batches go
         * through SQLServerBulkCopy (not the override hook); per-batch-item counts must still
         * surface and all rows must persist. Comprehensive coverage in {@code BatchExecutionTest}.
         */
        @Test
        public void preparedStatementBatchWithUseBulkCopyForBatchInsertSmoke() throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPBulk"));
            try (Connection conn = PrepUtil.getConnection(
                    connectionString + ";useBulkCopyForBatchInsert=true");
                    Statement stmt = conn.createStatement()) {
                try {
                    createPlainTable(stmt, table);

                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO " + table + " VALUES (?, ?)")) {
                        ps.setInt(1, 1); ps.setInt(2, 1); ps.addBatch();
                        ps.setInt(1, 2); ps.setInt(2, 2); ps.addBatch();
                        ps.setInt(1, 3); ps.setInt(2, 3); ps.addBatch();
                        int[] counts = ps.executeBatch();
                        assertEquals(3, counts.length,
                                "useBulkCopyForBatchInsert must still return one count per batch item");
                        // Bulk-copy reports SUCCESS_NO_INFO (-2) for individual items because the
                        // per-row count is not available; accept either SUCCESS_NO_INFO or 1.
                        for (int i = 0; i < counts.length; i++) {
                            assertTrue(counts[i] == 1 || counts[i] == Statement.SUCCESS_NO_INFO,
                                    "batch item " + i + " must report 1 (per-row) or SUCCESS_NO_INFO "
                                            + "(bulk-copy aggregate); got " + counts[i]);
                        }
                    }

                    // Verify all 3 rows were actually inserted regardless of how the count was reported
                    try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table)) {
                        assertTrue(rs.next());
                        assertEquals(3, rs.getInt(1),
                                "all 3 rows must be persisted via bulk-copy path");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }

        /**
         * Smoke-validates {@code prepareMethod=scopeTempTablesToConnection}: temp-table-aware
         * direct-SQL routing must not break compound-SQL traversal.
         * Comprehensive coverage in {@code PrepStmtDirectSQLExecutionTests}.
         */
        @Test
        public void preparedStatementWithScopeTempTablesToConnectionSmoke() throws SQLException {
            try (Connection conn = PrepUtil.getConnection(
                    connectionString + ";prepareMethod=scopeTempTablesToConnection");
                    Statement stmt = conn.createStatement()) {
                String tempTable = "#tmpScope_" + System.nanoTime();
                // Create the temp table first via direct SQL so the prepared statement that
                // references it is the one routed through the direct-SQL path.
                stmt.executeUpdate("CREATE TABLE " + tempTable + " (c1 INT, c2 SMALLINT)");

                try (PreparedStatement ps = conn.prepareStatement(
                        "INSERT INTO " + tempTable + " VALUES (?, ?); SELECT * FROM " + tempTable)) {
                    ps.setInt(1, 1);
                    ps.setInt(2, 1);
                    boolean isResultSet = ps.execute();
                    Traversal t = traverseAllResults(ps, isResultSet);
                    assertEquals(Arrays.asList(1), t.updateCounts,
                            "scopeTempTablesToConnection must not break compound-SQL count surfacing");
                    assertEquals(Arrays.asList(1), t.resultSetRowCounts,
                            "scopeTempTablesToConnection must not break trailing-SELECT surfacing");
                }
            }
        }

        /**
         * Smoke-validates {@code columnEncryptionSetting=Enabled} on a non-encrypted table:
         * AlwaysEncrypted code path must be a no-op for non-encrypted statements.
         * Comprehensive AE coverage in the {@code AlwaysEncrypted} package suites.
         */
        @Test
        public void preparedStatementExecuteWithColumnEncryptionEnabledOnPlainTableSmoke()
                throws SQLException {
            String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("SPAE"));
            try (Connection conn = PrepUtil.getConnection(
                    connectionString + ";columnEncryptionSetting=Enabled");
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
                                "columnEncryptionSetting=Enabled on a non-encrypted table must NOT change compound-SQL count surfacing");
                        assertEquals(Arrays.asList(1), t.resultSetRowCounts,
                                "columnEncryptionSetting=Enabled on a non-encrypted table must NOT change trailing-SELECT surfacing");
                    }
                } finally {
                    TestUtils.dropTableIfExists(table, stmt);
                }
            }
        }
    }
}
