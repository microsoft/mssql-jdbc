/*
 * Microsoft JDBC Driver for SQL Server
 *
 * Copyright(c) Microsoft Corporation All rights reserved.
 *
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerConnection;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;

/**
 * Tests for the {@code useColumnTypeSizing} connection property, which sizes unsized variable-length string/binary
 * parameters to the target column's declared length (discovered via sp_describe_undeclared_parameters) instead of the
 * fixed varchar(8000)/nvarchar(4000)/varbinary(8000) defaults. Related to issue #2913 and PR #2960.
 *
 * The property is opt-in (default false). With it OFF, behavior is identical to today. With it ON, the declared type
 * keyword stays the runtime arm's native type (so the TDS value encoding is unchanged) and only the declared length is
 * tightened to the column; if a bound value is longer than the column the declaration snaps to the (max) variant so the
 * operand is never truncated and the plan cache stays bounded.
 *
 * Assertions read the {@code Parameter.typeDefinition} actually produced at execution time (the string sent to the
 * server) via reflection, so value-dependent behavior such as the snap-to-max clamp is observed accurately.
 */
@RunWith(JUnitPlatform.class)
public class ColumnTypeSizingTest extends AbstractTest {

    static String varcharTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("ctsVarchar"));
    static String charTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("ctsChar"));
    static String ncharTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("ctsNchar"));
    static String nvarcharTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("ctsNvarchar"));
    static String varbinaryTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("ctsVarbinary"));
    static String lobTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("ctsLob"));
    static String insertTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("ctsInsert"));
    static String partTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("ctsPart"));
    static String partFunc = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("ctsPF"));
    static String partScheme = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("ctsPS"));
    static boolean partitioningAvailable = false;

    private static final String SIZING_VARCHAR = ";useColumnTypeSizing=true;sendStringParametersAsUnicode=false";
    private static final String SIZING_DEFAULTUNICODE = ";useColumnTypeSizing=true";

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        try (Statement stmt = connection.createStatement()) {
            dropAll(stmt);
            stmt.execute("CREATE TABLE " + varcharTable + " (id int IDENTITY PRIMARY KEY, c varchar(20) NOT NULL)");
            stmt.execute("CREATE TABLE " + charTable + " (id int IDENTITY PRIMARY KEY, c char(10) NOT NULL)");
            stmt.execute("CREATE TABLE " + ncharTable + " (id int IDENTITY PRIMARY KEY, c nchar(10) NOT NULL)");
            stmt.execute("CREATE TABLE " + nvarcharTable + " (id int IDENTITY PRIMARY KEY, c nvarchar(20) NOT NULL)");
            stmt.execute(
                    "CREATE TABLE " + varbinaryTable + " (id int IDENTITY PRIMARY KEY, c varbinary(16) NOT NULL)");
            stmt.execute("CREATE TABLE " + lobTable + " (id int IDENTITY PRIMARY KEY, c varchar(max) NOT NULL)");
            stmt.execute("CREATE TABLE " + insertTable + " (id int IDENTITY PRIMARY KEY, c varchar(20) NOT NULL)");
            stmt.execute("INSERT INTO " + varcharTable + "(c) VALUES ('alpha'),('beta')");
            stmt.execute("INSERT INTO " + charTable + "(c) VALUES ('0000000001')");
            stmt.execute("INSERT INTO " + ncharTable + "(c) VALUES (N'0000000001')");
            stmt.execute("INSERT INTO " + nvarcharTable + "(c) VALUES (N'alpha')");
            stmt.execute("INSERT INTO " + varbinaryTable + "(c) VALUES (0x0102030405)");
            stmt.execute("INSERT INTO " + lobTable + "(c) VALUES ('alpha')");

            // Table partitioned ON a varchar(8) column, used to prove that partition elimination needs the
            // column-sized length (a wider varchar(8000) parameter seeks but still probes every partition).
            // Partitioning may be unavailable on some targets; if so, the related test self-skips.
            try {
                stmt.execute("CREATE PARTITION FUNCTION " + partFunc
                        + " (varchar(8)) AS RANGE RIGHT FOR VALUES ('D','H','M','R','V')");
                stmt.execute("CREATE PARTITION SCHEME " + partScheme + " AS PARTITION " + partFunc
                        + " ALL TO ([PRIMARY])");
                stmt.execute("CREATE TABLE " + partTable
                        + " (id int IDENTITY(1,1) NOT NULL, region_code varchar(8) NOT NULL, amount int NOT NULL)");
                stmt.execute("CREATE CLUSTERED INDEX cix_" + System.identityHashCode(partTable) + " ON " + partTable
                        + " (region_code, id) ON " + partScheme + "(region_code)");
                stmt.execute("INSERT INTO " + partTable + "(region_code, amount) "
                        + "SELECT CHAR(65 + (v.number % 26)), v.number FROM master.dbo.spt_values v "
                        + "WHERE v.type = 'P' AND v.number BETWEEN 1 AND 8000");
                stmt.execute("UPDATE STATISTICS " + partTable + " WITH FULLSCAN");
                partitioningAvailable = true;
            } catch (java.sql.SQLException e) {
                partitioningAvailable = false;
            }
        }
    }

    @AfterAll
    public static void cleanup() throws Exception {
        try (Statement stmt = connection.createStatement()) {
            dropAll(stmt);
        }
    }

    private static void dropAll(Statement stmt) throws Exception {
        for (String t : new String[] {varcharTable, charTable, ncharTable, nvarcharTable, varbinaryTable, lobTable,
                insertTable, partTable}) {
            TestUtils.dropTableIfExists(t, stmt);
        }
        // Partition scheme/function must be dropped after the table that uses them; ignore if absent.
        try {
            stmt.execute("IF EXISTS (SELECT 1 FROM sys.partition_schemes WHERE name = '"
                    + partScheme.replace("[", "").replace("]", "") + "') DROP PARTITION SCHEME " + partScheme);
            stmt.execute("IF EXISTS (SELECT 1 FROM sys.partition_functions WHERE name = '"
                    + partFunc.replace("[", "").replace("]", "") + "') DROP PARTITION FUNCTION " + partFunc);
        } catch (java.sql.SQLException e) {
            // best-effort cleanup
        }
    }

    // ---- no-regression -------------------------------------------------------

    @Test
    public void testDefaultOffNoRegressionUnicode() throws Exception {
        // Default connection (property off, SSPAU default true) must declare nvarchar(4000) exactly as before.
        assertEquals("nvarchar(4000)", declaredTypeAfterQuery(connectionString,
                "SELECT COUNT(*) FROM " + varcharTable + " WHERE c = ?", "alpha"));
    }

    @Test
    public void testDefaultOffNoRegressionNonUnicode() throws Exception {
        // Property off, SSPAU=false must still declare varchar(8000) (legacy).
        assertEquals("varchar(8000)", declaredTypeAfterQuery(connectionString + ";sendStringParametersAsUnicode=false",
                "SELECT COUNT(*) FROM " + varcharTable + " WHERE c = ?", "alpha"));
    }

    // ---- VARCHAR / CHAR (both Symptom A and B) -------------------------------

    @Test
    public void testVarcharColumnSizedToColumn() throws Exception {
        // SSPAU=false + sizing -> varchar(20): fixes both memory grant and SARGability.
        assertEquals("varchar(20)", declaredTypeAfterQuery(connectionString + SIZING_VARCHAR,
                "SELECT COUNT(*) FROM " + varcharTable + " WHERE c = ?", "alpha"));
    }

    @Test
    public void testCharColumnSizedToColumn() throws Exception {
        // CHAR(10) suffers the same problem as VARCHAR; sizing declares varchar(10) (SARGable against char).
        assertEquals("varchar(10)", declaredTypeAfterQuery(connectionString + SIZING_VARCHAR,
                "SELECT COUNT(*) FROM " + charTable + " WHERE c = ?", "0000000001"));
    }

    @Test
    public void testVarcharColumnSSPAUTrueLengthOnly() throws Exception {
        // With the default SSPAU=true, the type stays nvarchar (no encoding change) but the length is tightened.
        assertEquals("nvarchar(20)", declaredTypeAfterQuery(connectionString + SIZING_DEFAULTUNICODE,
                "SELECT COUNT(*) FROM " + varcharTable + " WHERE c = ?", "alpha"));
    }

    // ---- NCHAR / NVARCHAR (Symptom A only - seek already works) --------------

    @Test
    public void testNvarcharColumnSizedToColumn() throws Exception {
        // nvarchar column with default SSPAU=true -> nvarchar(20): fixes the memory grant; seek already worked.
        assertEquals("nvarchar(20)", declaredTypeAfterQuery(connectionString + SIZING_DEFAULTUNICODE,
                "SELECT COUNT(*) FROM " + nvarcharTable + " WHERE c = ?", "alpha"));
    }

    @Test
    public void testNcharColumnSizedToColumn() throws Exception {
        // nchar column with default SSPAU=true -> nvarchar(10): tightens memory grant.
        assertEquals("nvarchar(10)", declaredTypeAfterQuery(connectionString + SIZING_DEFAULTUNICODE,
                "SELECT COUNT(*) FROM " + ncharTable + " WHERE c = ?", "0000000001"));
    }

    // ---- VARBINARY ----------------------------------------------------------

    @Test
    public void testVarbinaryColumnSizedToColumn() throws Exception {
        String connStr = connectionString + ";useColumnTypeSizing=true";
        String sql = "SELECT COUNT(*) FROM " + varbinaryTable + " WHERE c = ?";
        try (Connection con = PrepUtil.getConnection(connStr);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setBytes(1, new byte[] {1, 2, 3, 4, 5});
            try (ResultSet rs = pstmt.executeQuery()) {
                rs.next();
            }
            assertEquals("varbinary(16)", actualTypeDefinition(pstmt, 1));
        }
    }

    // ---- clamp / snap-to-max (correctness: never truncate) ------------------

    @Test
    public void testValueLongerThanColumnSnapsToMax() throws Exception {
        // A 30-char value against a varchar(20) column must NOT be declared varchar(20) (would truncate the operand
        // and risk a false match); it snaps to varchar(max). The lookup must therefore find no row.
        String connStr = connectionString + SIZING_VARCHAR;
        String sql = "SELECT COUNT(*) FROM " + varcharTable + " WHERE c = ?";
        String longValue = "abcdefghijklmnopqrstuvwxyz0123"; // 30 chars > column 20
        try (Connection con = PrepUtil.getConnection(connStr);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setString(1, longValue);
            int count;
            try (ResultSet rs = pstmt.executeQuery()) {
                rs.next();
                count = rs.getInt(1);
            }
            assertEquals("varchar(max)", actualTypeDefinition(pstmt, 1));
            assertEquals(0, count, "An over-length value must not match any row (no operand truncation)");
        }
    }

    @Test
    public void testValueExactlyColumnLengthSized() throws Exception {
        // A value exactly the column width is still sized to the column (not max).
        assertEquals("varchar(20)", declaredTypeAfterQuery(connectionString + SIZING_VARCHAR,
                "SELECT COUNT(*) FROM " + varcharTable + " WHERE c = ?", "12345678901234567890"));
    }

    // ---- range predicate (length must not change comparison results) ---------

    @Test
    public void testRangePredicateNoTruncation() throws Exception {
        // For c < ? the server returns the wide varchar(8000) (it does not bind the column for ranges), so the operand
        // is never truncated. Verify both the (wide) declaration and that results are unaffected.
        String connStr = connectionString + SIZING_VARCHAR;
        String sql = "SELECT COUNT(*) FROM " + varcharTable + " WHERE c < ?";
        try (Connection con = PrepUtil.getConnection(connStr)) {
            try (PreparedStatement pstmt = con.prepareStatement(sql)) {
                pstmt.setString(1, "b"); // 'alpha' < 'b' but 'beta' is not -> exactly one row
                int count;
                try (ResultSet rs = pstmt.executeQuery()) {
                    rs.next();
                    count = rs.getInt(1);
                }
                assertEquals(1, count);
            }
            try (PreparedStatement pstmt = con.prepareStatement(sql)) {
                pstmt.setString(1, "abcdefghijklmnopqrstuvwxyz0123"); // 30 chars, must not be truncated
                try (ResultSet rs = pstmt.executeQuery()) {
                    rs.next();
                }
                String d = actualTypeDefinition(pstmt, 1);
                assertTrue("varchar(8000)".equals(d) || "varchar(max)".equals(d),
                        "Range operand must stay wide enough to never truncate; was " + d);
            }
        }
    }

    // ---- fallback (parameter not bound to a string/binary column) ------------

    @Test
    public void testNonStringColumnFallsBackToDefault() throws Exception {
        // Binding a string to an int column: the inferred column type is not char/binary -> not eligible -> legacy.
        assertEquals("varchar(8000)", declaredTypeAfterQuery(connectionString + SIZING_VARCHAR,
                "SELECT COUNT(*) FROM " + varcharTable + " WHERE id = ?", "1"));
    }

    // ---- LOB column ---------------------------------------------------------

    @Test
    public void testLobColumnDeclaredAsMax() throws Exception {
        assertEquals("varchar(max)", declaredTypeAfterQuery(connectionString + SIZING_VARCHAR,
                "SELECT COUNT(*) FROM " + lobTable + " WHERE c = ?", "alpha"));
    }

    // ---- INSERT (explicit column list) --------------------------------------

    @Test
    public void testInsertExplicitColumnSized() throws Exception {
        String connStr = connectionString + SIZING_VARCHAR;
        String sql = "INSERT INTO " + insertTable + " (c) VALUES (?)";
        try (Connection con = PrepUtil.getConnection(connStr);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setString(1, "inserted");
            pstmt.executeUpdate();
            assertEquals("varchar(20)", actualTypeDefinition(pstmt, 1));
        }
    }

    @Test
    public void testBatchInsertSizedAndPersisted() throws Exception {
        String connStr = connectionString + SIZING_VARCHAR;
        String sql = "INSERT INTO " + insertTable + " (c) VALUES (?)";
        try (Connection con = PrepUtil.getConnection(connStr);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            for (String v : new String[] {"b1", "b2", "b3"}) {
                pstmt.setString(1, v);
                pstmt.addBatch();
            }
            int[] counts = pstmt.executeBatch();
            assertEquals(3, counts.length);
            // The batch path sizes via a separate parameter array (batchParamValues), so assert on the type
            // definition string actually sent for the batch - not inOutParam, which the batch flow does not size.
            String batchTypeDefs = preparedTypeDefinitions(pstmt);
            assertTrue(batchTypeDefs != null && batchTypeDefs.contains("varchar(20)"),
                    "batch INSERT should size the parameter to varchar(20); sent: " + batchTypeDefs);
            assertTrue(batchTypeDefs != null && !batchTypeDefs.contains("(8000)"),
                    "batch INSERT must not fall back to the legacy varchar(8000); sent: " + batchTypeDefs);
        }
        // verify all three rows landed (no truncation/misalignment)
        try (Connection con = PrepUtil.getConnection(connStr);
                PreparedStatement pstmt = con
                        .prepareStatement("SELECT COUNT(*) FROM " + insertTable + " WHERE c IN ('b1','b2','b3')");
                ResultSet rs = pstmt.executeQuery()) {
            rs.next();
            assertEquals(3, rs.getInt(1));
        }
    }

    // ---- partition elimination (the length, not just the type, matters) ------

    @Test
    public void testPartitionEliminationRequiresColumnSizedLength() throws Exception {
        org.junit.jupiter.api.Assumptions.assumeTrue(partitioningAvailable,
                "partitioning not available on this target");
        String sql = "SELECT COUNT(amount) FROM " + partTable + " WHERE region_code = ?";
        // Baseline (default: nvarchar(4000)) cannot eliminate -> probes more than one partition.
        int basePartitions = partitionsAccessed(connectionString, sql, "M");
        // Column-sized varchar(8) matches the partition function type -> eliminates to a single partition.
        int fixPartitions = partitionsAccessed(connectionString + SIZING_VARCHAR, sql, "M");
        assertTrue(basePartitions > 1,
                "baseline should probe multiple partitions (no elimination); was " + basePartitions);
        assertEquals(1, fixPartitions,
                "column-sized varchar(8) parameter should eliminate to a single partition; was " + fixPartitions);
        // Confirm the declared type is the column-matched varchar(8) under the fix.
        assertEquals("varchar(8)", declaredTypeAfterQuery(connectionString + SIZING_VARCHAR, sql, "M"));
    }

    /**
     * Executes the query with one string parameter under SET STATISTICS XML ON and returns the number of partitions
     * the actual plan accessed (RunTimePartitionSummary/@PartitionCount), or -1 if no PartitionCount is present in the
     * plan. Returning -1 (rather than a plausible 1) on a parse miss ensures the elimination assertion cannot pass by
     * accident when the plan shape is not what the test expects.
     */
    private static int partitionsAccessed(String connStr, String sql, String value) throws Exception {
        try (Connection con = PrepUtil.getConnection(connStr)) {
            try (Statement s = con.createStatement()) {
                s.execute("SET STATISTICS XML ON");
            }
            String planXml = null;
            try (PreparedStatement ps = con.prepareStatement(sql)) {
                ps.setString(1, value);
                boolean hasRs = ps.execute();
                while (true) {
                    if (hasRs) {
                        try (ResultSet rs = ps.getResultSet()) {
                            while (rs.next()) {
                                String v = rs.getString(1);
                                if (v != null && v.contains("ShowPlanXML")) {
                                    planXml = v;
                                }
                            }
                        }
                    }
                    hasRs = ps.getMoreResults();
                    if (!hasRs && ps.getUpdateCount() == -1) {
                        break;
                    }
                }
            }
            assertTrue(planXml != null, "expected a Showplan XML result");
            java.util.regex.Matcher m = java.util.regex.Pattern.compile("PartitionCount=\"([0-9]+)\"")
                    .matcher(planXml);
            return m.find() ? Integer.parseInt(m.group(1)) : -1;
        }
    }

    // ---- UPDATE / DELETE (same describe flow as SELECT/INSERT) ---------------

    @Test
    public void testUpdateSetAndWhereParametersSized() throws Exception {
        // UPDATE t SET c = ? WHERE c = ?  -> both the assignment param and the predicate param size to varchar(20).
        String connStr = connectionString + SIZING_VARCHAR;
        try (Connection con = PrepUtil.getConnection(connStr)) {
            try (PreparedStatement ins = con.prepareStatement("INSERT INTO " + insertTable + " (c) VALUES (?)")) {
                ins.setString(1, "upd-seed");
                ins.executeUpdate();
            }
            try (PreparedStatement up = con
                    .prepareStatement("UPDATE " + insertTable + " SET c = ? WHERE c = ?")) {
                up.setString(1, "upd-done");
                up.setString(2, "upd-seed");
                int n = up.executeUpdate();
                assertEquals("varchar(20)", actualTypeDefinition(up, 1)); // SET assignment param
                assertEquals("varchar(20)", actualTypeDefinition(up, 2)); // WHERE predicate param
                assertEquals(1, n);
            }
            // confirm the row was actually updated (no truncation/misalignment)
            try (PreparedStatement chk = con
                    .prepareStatement("SELECT COUNT(*) FROM " + insertTable + " WHERE c = 'upd-done'");
                    ResultSet rs = chk.executeQuery()) {
                rs.next();
                assertEquals(1, rs.getInt(1));
            }
        }
    }

    @Test
    public void testDeleteWhereParameterSized() throws Exception {
        // DELETE FROM t WHERE c = ?  -> the predicate param sizes to varchar(20) and the row is removed.
        String connStr = connectionString + SIZING_VARCHAR;
        try (Connection con = PrepUtil.getConnection(connStr)) {
            try (PreparedStatement ins = con.prepareStatement("INSERT INTO " + insertTable + " (c) VALUES (?)")) {
                ins.setString(1, "del-seed");
                ins.executeUpdate();
            }
            try (PreparedStatement del = con.prepareStatement("DELETE FROM " + insertTable + " WHERE c = ?")) {
                del.setString(1, "del-seed");
                int n = del.executeUpdate();
                assertEquals("varchar(20)", actualTypeDefinition(del, 1));
                assertEquals(1, n);
            }
        }
    }

    @Test
    public void testDeleteOverLengthValueSnapsToMaxNoFalseDelete() throws Exception {
        // An over-length value in a DELETE predicate must snap to (max) so it cannot truncate and delete a wrong row.
        String connStr = connectionString + SIZING_VARCHAR;
        try (Connection con = PrepUtil.getConnection(connStr)) {
            try (PreparedStatement ins = con.prepareStatement("INSERT INTO " + insertTable + " (c) VALUES (?)")) {
                ins.setString(1, "12345678901234567890"); // exactly 20 chars, exists
                ins.executeUpdate();
            }
            try (PreparedStatement del = con.prepareStatement("DELETE FROM " + insertTable + " WHERE c = ?")) {
                del.setString(1, "12345678901234567890EXTRA"); // 25 chars > column 20
                int n = del.executeUpdate();
                assertEquals("varchar(max)", actualTypeDefinition(del, 1));
                assertEquals(0, n, "Over-length value must not truncate and delete the 20-char row");
            }
            // cleanup the seeded row
            try (PreparedStatement c2 = con
                    .prepareStatement("DELETE FROM " + insertTable + " WHERE c = '12345678901234567890'")) {
                c2.executeUpdate();
            }
        }
    }

    // ---- plan-cache stability ----------------------------------------------

    @Test
    public void testDeclarationStableAcrossValues() throws Exception {
        // Two different short values must yield the identical declaration -> a single plan-cache entry.
        String connStr = connectionString + SIZING_VARCHAR;
        String sql = "SELECT COUNT(*) FROM " + varcharTable + " WHERE c = ?";
        try (Connection con = PrepUtil.getConnection(connStr)) {
            String d1;
            String d2;
            try (PreparedStatement pstmt = con.prepareStatement(sql)) {
                pstmt.setString(1, "alpha");
                try (ResultSet rs = pstmt.executeQuery()) {
                    rs.next();
                }
                d1 = actualTypeDefinition(pstmt, 1);
            }
            try (PreparedStatement pstmt = con.prepareStatement(sql)) {
                pstmt.setString(1, "x");
                try (ResultSet rs = pstmt.executeQuery()) {
                    rs.next();
                }
                d2 = actualTypeDefinition(pstmt, 1);
            }
            assertEquals("varchar(20)", d1);
            assertEquals(d1, d2);
        }
    }

    @Test
    public void testNullValueDoesNotTruncateOrFail() throws Exception {
        String connStr = connectionString + SIZING_VARCHAR;
        String sql = "SELECT COUNT(*) FROM " + varcharTable + " WHERE c = ?";
        try (Connection con = PrepUtil.getConnection(connStr);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setString(1, null);
            try (ResultSet rs = pstmt.executeQuery()) {
                rs.next();
            }
            // NULL has length 0, so it fits the column: the declaration is the column-sized varchar(20) (not the
            // snap-to-(max) form) and never an error.
            assertEquals("varchar(20)", actualTypeDefinition(pstmt, 1));
        }
    }

    @Test
    public void testReexecuteLongerValueSnapsToMax() throws Exception {
        // Same PreparedStatement, two executions: a value that fits the varchar(20) column declares varchar(20);
        // a value longer than the column snaps to varchar(max) so the operand is never truncated. Proves the
        // per-execution clamp and that the declaration stays one of only two bounded forms across re-execution.
        String connStr = connectionString + SIZING_VARCHAR;
        String sql = "SELECT COUNT(*) FROM " + varcharTable + " WHERE c = ?";
        try (Connection con = PrepUtil.getConnection(connStr);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setString(1, "fits");
            try (ResultSet rs = pstmt.executeQuery()) {
                rs.next();
            }
            assertEquals("varchar(20)", actualTypeDefinition(pstmt, 1));

            pstmt.setString(1, "this-value-is-far-longer-than-twenty-characters");
            try (ResultSet rs = pstmt.executeQuery()) {
                rs.next();
            }
            assertEquals("varchar(max)", actualTypeDefinition(pstmt, 1));
        }
    }

    @Test
    public void testMultipleParametersAlignToTheirOwnColumns() throws Exception {
        // Two parameters compared to columns of different widths must each size to their own column, in order -
        // catches any ordinal off-by-one between the describe metadata and the parameter array.
        String connStr = connectionString + SIZING_VARCHAR;
        String sql = "SELECT (SELECT COUNT(*) FROM " + varcharTable + " WHERE c = ?)" + " + (SELECT COUNT(*) FROM "
                + charTable + " WHERE c = ?)";
        try (Connection con = PrepUtil.getConnection(connStr);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setString(1, "alpha");
            pstmt.setString(2, "0000000001");
            try (ResultSet rs = pstmt.executeQuery()) {
                rs.next();
            }
            assertEquals("varchar(20)", actualTypeDefinition(pstmt, 1)); // varcharTable.c is varchar(20)
            assertEquals("varchar(10)", actualTypeDefinition(pstmt, 2)); // charTable.c is char(10) -> varchar(10)
        }
    }

    // ---- property accessors (Connection + DataSource parity) ----------------

    @Test
    public void testConnectionAndDataSourceAccessors() throws Exception {
        try (Connection con = PrepUtil.getConnection(connectionString + ";useColumnTypeSizing=true")) {
            assertTrue(((ISQLServerConnection) con).getUseColumnTypeSizing());
        }
        try (Connection con = PrepUtil.getConnection(connectionString)) {
            assertEquals(false, ((ISQLServerConnection) con).getUseColumnTypeSizing());
        }
        SQLServerDataSource ds = new SQLServerDataSource();
        assertEquals(false, ds.getUseColumnTypeSizing());
        ds.setUseColumnTypeSizing(true);
        assertTrue(ds.getUseColumnTypeSizing());
    }

    // ---- helpers ------------------------------------------------------------

    /**
     * Executes the query with one string parameter on a connection built from connStr, then returns the TDS type
     * definition the driver actually produced for the first parameter at execution time.
     */
    private static String declaredTypeAfterQuery(String connStr, String sql, String value) throws Exception {
        try (Connection con = PrepUtil.getConnection(connStr);
                PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setString(1, value);
            try (ResultSet rs = pstmt.executeQuery()) {
                rs.next();
            }
            return actualTypeDefinition(pstmt, 1);
        }
    }

    /**
     * Reads the {@code Parameter.typeDefinition} string that the driver computed and sent for the given parameter
     * during the most recent execution (via reflection). Reading the stored field - rather than recomputing - captures
     * the value-dependent snap-to-max clamp accurately.
     */
    private static String actualTypeDefinition(PreparedStatement pstmt, int paramIndex) throws Exception {
        Field inOutParamField = SQLServerStatement.class.getDeclaredField("inOutParam");
        inOutParamField.setAccessible(true);
        Object[] inOutParam = (Object[]) inOutParamField.get(pstmt);
        Object param = inOutParam[paramIndex - 1];

        Field typeDefField = param.getClass().getDeclaredField("typeDefinition");
        typeDefField.setAccessible(true);
        return (String) typeDefField.get(param);
    }

    /**
     * Reads the {@code preparedTypeDefinitions} string the driver last built and sent for this statement (via
     * reflection). Unlike {@link #actualTypeDefinition}, which reads inOutParam, this reflects the type-definition
     * string produced by the batch path, whose parameter array (batchParamValues) is released after execution.
     */
    private static String preparedTypeDefinitions(PreparedStatement pstmt) throws Exception {
        Field f = SQLServerPreparedStatement.class.getDeclaredField("preparedTypeDefinitions");
        f.setAccessible(true);
        return (String) f.get(pstmt);
    }
}
