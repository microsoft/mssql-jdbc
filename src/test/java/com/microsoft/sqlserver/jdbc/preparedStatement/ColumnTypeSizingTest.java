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
                insertTable}) {
            TestUtils.dropTableIfExists(t, stmt);
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
            // NULL fits any size; declaration is a sound varchar form, never an error.
            String d = actualTypeDefinition(pstmt, 1);
            assertTrue(d != null && d.startsWith("varchar"), "Unexpected declaration for NULL: " + d);
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
}
