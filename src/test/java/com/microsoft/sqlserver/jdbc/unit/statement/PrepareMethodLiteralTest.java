/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDataTable;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;

import microsoft.sql.DateTimeOffset;


/**
 * SQL Server integration coverage for parameter literals, comparing direct execution with typed parameter binding.
 * Exercises expression and procedure arguments, string conversion predicates, escaping, NULLs, and statement reuse.
 */
@RunWith(JUnitPlatform.class)
public class PrepareMethodLiteralTest extends AbstractTest {
    private static final String[] MODES = {"default", "prepexec", "prepare", "none", "scopeTempTablesToConnection"};
    private static final String DATE_TEXT = "2024-02-13";
    private static final String TIMESTAMP_TEXT = "2024-02-13 12:23:45.047";
    private static final String GUID_TEXT = "01234567-89AB-CDEF-0123-456789ABCDEF";
    private static final byte[] BINARY_VALUE = {1, 2, 3, 4};
    private static final String UNICODE_TEXT = "caf\u00e9 \u540d\u524d";

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @ParameterizedTest(name = "{0}, mode={1}, unicode={2}, callable={3}")
    @MethodSource("predicateCases")
    public void testPredicateConversion(PredicateCase testCase, String mode, boolean unicode,
            boolean callable) throws SQLException {
        try (Connection conn = openConnection(mode, unicode); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE #literal_values (value " + testCase.sqlType + ")");
            stmt.execute("INSERT INTO #literal_values VALUES (" + testCase.seed + ")");
            String query = "SELECT COUNT(*) FROM #literal_values WHERE value " + testCase.operator + " ?";
            if (callable) {
                stmt.execute("CREATE PROCEDURE #literal_filter @value " + testCase.sqlType
                        + " AS SELECT COUNT(*) FROM #literal_values WHERE value " + testCase.operator + " @value");
                query = "{call #literal_filter(?)}";
            }
            try (PreparedStatement ps = callable ? conn.prepareCall(query) : conn.prepareStatement(query)) {
                for (int execution = 0; execution < 2; execution++) {
                    testCase.binder.bind(ps);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(1, rs.getInt(1), "Expected the seeded row on execution " + execution);
                        assertFalse(rs.next());
                    }
                }
            }
        }
    }

    @ParameterizedTest(name = "{0}, mode={1}, callable={2}")
    @MethodSource("roundTripCases")
    public void testValueRoundTrip(ParameterCase testCase, String mode, boolean callable) throws SQLException {
        try (Connection reference = openConnection("prepexec", true); Connection actual = openConnection(mode, true);
                Statement stmt = actual.createStatement()) {
            // The reference uses typed binding and the default date format; direct execution must not depend on it.
            stmt.execute("SET DATEFORMAT dmy");
            Object expected = readValue(reference, "SELECT CAST(? AS " + testCase.sqlType + ")", testCase.binder,
                    false);
            String query = "SELECT CAST(? AS " + testCase.sqlType + ")";
            if (callable) {
                stmt.execute("CREATE PROCEDURE #literal_echo @value " + testCase.sqlType + " AS SELECT @value");
                query = "{call #literal_echo(?)}";
            }
            Object value = readValue(actual, query, testCase.binder, callable);
            if (expected instanceof byte[]) {
                assertArrayEquals((byte[]) expected, (byte[]) value);
            } else {
                assertEquals(expected, value);
            }
        }
    }

    @ParameterizedTest(name = "mode={0}, unicode={1}")
    @MethodSource("configurations")
    public void testStringAndBinaryLiteralTypes(String mode, boolean unicode) throws SQLException {
        try (Connection conn = openConnection(mode, unicode);
                PreparedStatement ps = conn.prepareStatement("SELECT ?, ?")) {
            ps.setString(1, DATE_TEXT);
            ps.setBytes(2, BINARY_VALUE);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(unicode ? Types.NVARCHAR : Types.VARCHAR, rs.getMetaData().getColumnType(1));
                assertEquals(DATE_TEXT, rs.getString(1));
                assertEquals(Types.VARBINARY, rs.getMetaData().getColumnType(2));
                assertArrayEquals(BINARY_VALUE, rs.getBytes(2));
                assertFalse(rs.next());
            }
        }
    }

    @ParameterizedTest(name = "mode={0}, unicode={1}")
    @MethodSource("configurations")
    public void testAdjacentKeywordAfterStringParameter(String mode, boolean unicode) throws SQLException {
        try (Connection conn = openConnection(mode, unicode); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE #literal_values (value varchar(30))");
            stmt.execute("INSERT INTO #literal_values VALUES ('O''Brien')");
            try (PreparedStatement ps = conn
                    .prepareStatement("SELECT COUNT(*) FROM #literal_values WHERE value = ?and 1=1")) {
                ps.setString(1, "O'Brien");
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(1, rs.getInt(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @ParameterizedTest(name = "mode={0}")
    @MethodSource("prepareModes")
    public void testMultipleProcedureInputs(String mode) throws SQLException {
        Date expectedDate = Date.valueOf(DATE_TEXT);
        String expectedText = "sample'value";
        try (Connection conn = openConnection(mode, true); Statement stmt = conn.createStatement()) {
            stmt.execute("SET DATEFORMAT dmy");
            stmt.execute("CREATE PROCEDURE #literal_inputs @date date, @text nvarchar(30) AS SELECT @date, @text");
            try (PreparedStatement ps = conn.prepareCall("{call #literal_inputs(?, ?)}")) {
                ps.setDate(1, expectedDate);
                ps.setString(2, expectedText);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(expectedDate, rs.getDate(1));
                    assertEquals(expectedText, rs.getString(2));
                    assertFalse(rs.next());
                }
            }
        }
    }

    @ParameterizedTest(name = "mode={0}")
    @ValueSource(strings = {"none", "scopeTempTablesToConnection"})
    public void testSelectIntoPreservesCharacterAndBinaryTypes(String mode) throws SQLException {
        try (Connection conn = openConnection(mode, false);
                PreparedStatement ps = conn
                        .prepareStatement("SELECT ? AS string_value, ? AS object_value, ? AS binary_value, "
                                + "CONVERT(datetime2(3), ?, 121) AS converted_value INTO #literal_bound");
                Statement stmt = conn.createStatement()) {
            bindLiteralValues(ps, "Black'Rock");
            assertEquals(1, ps.executeUpdate());
            // Reading from another statement also verifies that creation used the connection's scope.
            try (ResultSet rs = stmt.executeQuery("SELECT * FROM #literal_bound")) {
                assertTrue(rs.next());
                assertEquals(Types.VARCHAR, rs.getMetaData().getColumnType(1));
                assertEquals(Types.VARCHAR, rs.getMetaData().getColumnType(2));
                assertEquals(Types.VARBINARY, rs.getMetaData().getColumnType(3));
                assertLiteralValues(rs, "Black'Rock");
                assertFalse(rs.next());
            }
        }
    }

    @ParameterizedTest(name = "mode={0}, createInPreparedSql={1}, batch={2}")
    @MethodSource("literalExecutionCases")
    public void testLiteralBindingsThroughExecution(String mode, boolean createInPreparedSql,
            boolean batch) throws SQLException {
        String insert = "INSERT INTO #literal_results VALUES (?, ?, ?, CONVERT(datetime2(3), ?, 121))";
        String sql = createInPreparedSql ? "CREATE TABLE #literal_marker (id int); " + insert : insert;
        try (Connection conn = openConnection(mode, false); Statement stmt = conn.createStatement()) {
            // sql_variant preserves the input types instead of coercing binary text into a VARCHAR column.
            stmt.execute("CREATE TABLE #literal_results (string_value sql_variant, object_value sql_variant, "
                    + "binary_value sql_variant, converted_value datetime2(3))");
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (String text : new String[] {"Black'Rock", "second'value"}) {
                    bindLiteralValues(ps, text);
                    if (batch) {
                        // One creation per SQL batch avoids duplicate CREATE TABLE declarations.
                        // Rebinding after addBatch verifies that execution uses the captured values.
                        ps.addBatch();
                        bindLiteralValues(ps, "not the batched value");
                        assertEquals(1, ps.executeBatch().length);
                        ps.clearBatch();
                    } else {
                        ps.executeUpdate();
                    }
                    try (ResultSet rs = stmt
                            .executeQuery("SELECT string_value, object_value, binary_value, converted_value, "
                                    + "SQL_VARIANT_PROPERTY(string_value, 'BaseType'), "
                                    + "SQL_VARIANT_PROPERTY(object_value, 'BaseType'), "
                                    + "SQL_VARIANT_PROPERTY(binary_value, 'BaseType') FROM #literal_results")) {
                        assertTrue(rs.next());
                        assertLiteralValues(rs, text);
                        assertEquals("varchar", rs.getString(5));
                        assertEquals("varchar", rs.getString(6));
                        assertEquals("varbinary", rs.getString(7));
                        assertFalse(rs.next());
                    }
                    stmt.executeUpdate("DELETE FROM #literal_results");
                    if (createInPreparedSql) {
                        stmt.executeUpdate("DROP TABLE #literal_marker");
                    }
                }
            }
        }
    }

    /** Verifies a negative value bound after '-' is subtracted rather than forming a '--' comment. */
    @ParameterizedTest(name = "mode={0}")
    @MethodSource("prepareModes")
    public void testNegativeValueAfterMinusOperator(String mode) throws SQLException {
        try (Connection conn = openConnection(mode, true);
                PreparedStatement ps = conn.prepareStatement("SELECT 10 -?, 10 -?, 10 -?")) {
            ps.setInt(1, -5);
            ps.setBigDecimal(2, new BigDecimal("-2.5"));
            ps.setDouble(3, -1.5);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(3, rs.getMetaData().getColumnCount());
                assertEquals(15, rs.getInt(1));
                assertEquals(0, new BigDecimal("12.5").compareTo(rs.getBigDecimal(2)));
                assertEquals(11.5, rs.getDouble(3));
                assertFalse(rs.next());
            }
        }
    }

    /** Verifies a negative value after '-' cannot comment out the rest of a WHERE clause and widen the result. */
    @ParameterizedTest(name = "mode={0}")
    @MethodSource("prepareModes")
    public void testNegativeValueCannotCommentOutPredicate(String mode) throws SQLException {
        try (Connection conn = openConnection(mode, true); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE #literal_rows (id int, visible bit)");
            stmt.execute("INSERT INTO #literal_rows VALUES (2, 0)");
            // The predicate is id = 3 AND visible = 1, so no row matches. If the trailing filter were commented out,
            // the hidden row with id = 2 would be returned.
            try (PreparedStatement ps = conn
                    .prepareStatement("SELECT COUNT(*) FROM #literal_rows WHERE id = 2 -? AND visible = 1")) {
                ps.setInt(1, -1);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(0, rs.getInt(1));
                    assertFalse(rs.next());
                }
            }
        }
    }

    /** Verifies setLong values keep bigint semantics in arithmetic instead of overflowing as int literals. */
    @ParameterizedTest(name = "mode={0}")
    @MethodSource("prepareModes")
    public void testBigintArithmeticDoesNotOverflowInt(String mode) throws SQLException {
        try (Connection conn = openConnection(mode, true); PreparedStatement ps = conn.prepareStatement("SELECT ? + ?")) {
            ps.setLong(1, Integer.MAX_VALUE);
            ps.setLong(2, 1L);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(Integer.MAX_VALUE + 1L, rs.getLong(1));
                assertFalse(rs.next());
            }
        }
    }

    /** Verifies a Long bound with an explicit INTEGER target keeps int semantics instead of widening to bigint. */
    @ParameterizedTest(name = "mode={0}")
    @MethodSource("prepareModes")
    public void testLongWithIntegerTargetKeepsIntSemantics(String mode) throws SQLException {
        try (Connection conn = openConnection(mode, true)) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?")) {
                ps.setObject(1, (long) Integer.MAX_VALUE, Types.INTEGER);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(Types.INTEGER, rs.getMetaData().getColumnType(1));
                    assertEquals(Integer.MAX_VALUE, rs.getInt(1));
                    assertFalse(rs.next());
                }
            }
            try (PreparedStatement ps = conn.prepareStatement("SELECT ? + 1")) {
                ps.setObject(1, (long) Integer.MAX_VALUE, Types.INTEGER);
                assertThrows(SQLException.class, () -> {
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                    }
                });
            }
        }
    }

    /** Verifies a Long bound with a DECIMAL or NUMERIC target keeps decimal semantics instead of bigint. */
    @ParameterizedTest(name = "mode={0}")
    @MethodSource("prepareModes")
    public void testLongWithDecimalTargetKeepsDecimalSemantics(String mode) throws SQLException {
        try (Connection conn = openConnection(mode, true);
                PreparedStatement ps = conn.prepareStatement("SELECT ? + 1, ?")) {
            for (int targetType : new int[] {Types.DECIMAL, Types.NUMERIC}) {
                ps.setObject(1, Long.MAX_VALUE, targetType);
                ps.setObject(2, 5L, targetType, 2);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(new BigDecimal("9223372036854775808"), rs.getBigDecimal(1),
                            "targetType=" + targetType);
                    assertEquals(new BigDecimal("5.00"), rs.getBigDecimal(2), "targetType=" + targetType);
                    assertFalse(rs.next());
                }
            }
        }
    }

    /** Verifies a Double bound with an INTEGER target keeps int semantics instead of float. */
    @ParameterizedTest(name = "mode={0}")
    @MethodSource("prepareModes")
    public void testDoubleWithIntegerTargetKeepsIntSemantics(String mode) throws SQLException {
        try (Connection conn = openConnection(mode, true)) {
            try (PreparedStatement ps = conn.prepareStatement("SELECT ?, ? / 2")) {
                ps.setObject(1, 2.7, Types.INTEGER);
                ps.setObject(2, 5.0, Types.INTEGER);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(Types.INTEGER, rs.getMetaData().getColumnType(1));
                    assertEquals(2, rs.getInt(1));
                    assertEquals(Types.INTEGER, rs.getMetaData().getColumnType(2));
                    assertEquals(2, rs.getInt(2));
                    assertFalse(rs.next());
                }
            }
            try (PreparedStatement ps = conn.prepareStatement("SELECT ? + 1")) {
                ps.setObject(1, (double) Integer.MAX_VALUE, Types.INTEGER);
                assertThrows(SQLException.class, () -> {
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                    }
                });
            }
        }
    }

    /**
     * Verifies a Double bound with a DECIMAL or NUMERIC target keeps decimal semantics instead of float, while
     * setDouble keeps float semantics.
     */
    @ParameterizedTest(name = "mode={0}")
    @MethodSource("prepareModes")
    public void testDoubleWithDecimalTargetKeepsDecimalSemantics(String mode) throws SQLException {
        try (Connection conn = openConnection(mode, true);
                PreparedStatement ps = conn.prepareStatement("SELECT ? + 1, ?, ? + 1")) {
            for (int targetType : new int[] {Types.DECIMAL, Types.NUMERIC}) {
                ps.setObject(1, 1.0E20, targetType);
                ps.setObject(2, 1.23456, targetType, 2);
                ps.setDouble(3, 1.0E20);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    ResultSetMetaData metaData = rs.getMetaData();
                    assertEquals(Types.DECIMAL, metaData.getColumnType(1), "targetType=" + targetType);
                    assertEquals(0, new BigDecimal("100000000000000000001").compareTo(rs.getBigDecimal(1)),
                            "targetType=" + targetType);
                    // Typed binding declares a double with a scale of at least 4.
                    assertEquals(new BigDecimal("1.2346"), rs.getBigDecimal(2), "targetType=" + targetType);
                    assertEquals(Types.DOUBLE, metaData.getColumnType(3), "targetType=" + targetType);
                    assertEquals(1.0E20, rs.getDouble(3), "targetType=" + targetType);
                    assertFalse(rs.next());
                }
            }
        }
    }

    /**
     * Verifies literal formatting when the prepared SQL creates a temp table, which also enables direct execution
     * under scopeTempTablesToConnection. In the direct modes the temp table outlives the statement.
     */
    @ParameterizedTest(name = "mode={0}")
    @MethodSource("prepareModes")
    public void testLiteralsInStatementThatCreatesTempTable(String mode) throws SQLException {
        try (Connection conn = openConnection(mode, true); PreparedStatement ps = conn.prepareStatement(
                "SELECT ? + 1 AS a, 10 - ? AS b, ? + 1 AS c, ? AS t INTO #literal_scope; "
                        + "SELECT a, b, c, t FROM #literal_scope")) {
            ps.setLong(1, Integer.MAX_VALUE);
            ps.setInt(2, -5);
            ps.setObject(3, Long.MAX_VALUE, Types.DECIMAL);
            ps.setTime(4, new Time(Time.valueOf("13:14:15").getTime() + 1));
            try (ResultSet rs = firstResultSet(ps)) {
                assertTrue(rs.next());
                assertEquals(2147483648L, rs.getLong(1));
                assertEquals(15, rs.getInt(2));
                assertEquals(new BigDecimal("9223372036854775808"), rs.getBigDecimal(3));
                // With sendTimeAsDatetime, .001 is rounded to .000 as with typed binding.
                assertEquals(Time.valueOf("13:14:15"), rs.getTime(4));
                assertFalse(rs.next());
            }
            assertEquals(isDirectMode(mode), tempTableExists(conn, "#literal_scope"));
        }
    }

    /**
     * Verifies the RPC fallback when the prepared SQL creates a temp table. A temp table created before the statement
     * stays accessible. A temp table created by the RPC call is dropped when the call ends, while the literal batch
     * keeps it on the connection.
     */
    @ParameterizedTest(name = "mode={0}")
    @MethodSource("prepareModes")
    public void testRpcFallbackInStatementThatCreatesTempTable(String mode) throws SQLException {
        try (Connection conn = openConnection(mode, true); Statement stmt = conn.createStatement();
                PreparedStatement ps = conn.prepareStatement("CREATE TABLE #literal_rpc (v varbinary(max)); "
                        + "INSERT INTO #literal_rpc VALUES (?); "
                        + "SELECT (SELECT DATALENGTH(v) FROM #literal_rpc), (SELECT n FROM #literal_existing)")) {
            stmt.execute("CREATE TABLE #literal_existing (n int); INSERT INTO #literal_existing VALUES (7)");
            for (boolean stream : new boolean[] {true, false}) {
                if (stream) {
                    ps.setBinaryStream(1, new ByteArrayInputStream(BINARY_VALUE));
                } else {
                    ps.setBytes(1, BINARY_VALUE);
                }
                try (ResultSet rs = firstResultSet(ps)) {
                    assertTrue(rs.next());
                    assertEquals(BINARY_VALUE.length, rs.getInt(1), "stream=" + stream);
                    assertEquals(7, rs.getInt(2), "stream=" + stream);
                    assertFalse(rs.next());
                }
                assertEquals(!stream && isDirectMode(mode), tempTableExists(conn, "#literal_rpc"),
                        "stream=" + stream);
            }

            // A null stream is a NULL literal, not a stream that requires an RPC. Remove the prior table first.
            if (tempTableExists(conn, "#literal_rpc")) {
                stmt.execute("DROP TABLE #literal_rpc");
            }
            ps.setBinaryStream(1, null);
            try (ResultSet rs = firstResultSet(ps)) {
                assertTrue(rs.next());
                assertNull(rs.getObject(1));
                assertEquals(7, rs.getInt(2));
                assertFalse(rs.next());
            }
            assertEquals(isDirectMode(mode), tempTableExists(conn, "#literal_rpc"), "null stream");
        }
    }

    private static boolean isDirectMode(String mode) {
        return "none".equals(mode) || "scopeTempTablesToConnection".equals(mode);
    }

    private static ResultSet firstResultSet(PreparedStatement ps) throws SQLException {
        boolean isResultSet = ps.execute();
        while (!isResultSet) {
            assertTrue(-1 != ps.getUpdateCount(), "The statement did not return a result set.");
            isResultSet = ps.getMoreResults();
        }
        return ps.getResultSet();
    }

    private static boolean tempTableExists(Connection conn, String name) throws SQLException {
        try (Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT OBJECT_ID('tempdb.." + name + "')")) {
            assertTrue(rs.next());
            return null != rs.getObject(1);
        }
    }

    /**
     * Verifies a REAL setObject target is converted like typed binding, which declares REAL as float, so 16777217 is
     * not rounded to a 32-bit real. The second execution binds a stream, so direct execution falls back to RPC.
     */
    @ParameterizedTest(name = "mode={0}")
    @MethodSource("prepareModes")
    public void testRealTargetMatchesTypedBinding(String mode) throws SQLException {
        try (Connection conn = openConnection(mode, true);
                PreparedStatement ps = conn.prepareStatement("SELECT ?, ?, DATALENGTH(?)")) {
            for (boolean stream : new boolean[] {false, true}) {
                ps.setObject(1, "16777217", Types.REAL);
                ps.setObject(2, 16777217L, Types.REAL);
                if (stream) {
                    ps.setBinaryStream(3, new ByteArrayInputStream(BINARY_VALUE));
                } else {
                    ps.setBytes(3, BINARY_VALUE);
                }
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(16777217.0, rs.getDouble(1), "stream=" + stream);
                    assertEquals(16777217.0, rs.getDouble(2), "stream=" + stream);
                    assertEquals(BINARY_VALUE.length, rs.getInt(3));
                    assertFalse(rs.next());
                }
            }
        }
    }

    /** Verifies setTime keeps milliseconds that are not on a 1/300 second tick when sendTimeAsDatetime=false. */
    @ParameterizedTest(name = "mode={0}, callable={1}")
    @MethodSource("callableConfigurations")
    public void testTimeWithoutSendTimeAsDatetimeKeepsMilliseconds(String mode,
            boolean callable) throws SQLException {
        Time expected = new Time(Time.valueOf("13:14:15").getTime() + 1);
        try (Connection conn = openConnection(mode, true); Statement stmt = conn.createStatement()) {
            conn.unwrap(SQLServerConnection.class).setSendTimeAsDatetime(false);
            String query = "SELECT CAST(? AS time(3))";
            if (callable) {
                stmt.execute("CREATE PROCEDURE #literal_time @value time(3) AS SELECT @value");
                query = "{call #literal_time(?)}";
            }
            assertEquals(expected, readValue(conn, query, ps -> ps.setTime(1, expected), callable));
        }
    }

    /** Verifies setObject's target JDBC type (VARCHAR) drives SQL semantics: string concatenation, not addition. */
    @ParameterizedTest(name = "mode={0}")
    @MethodSource("prepareModes")
    public void testSetObjectTargetTypeIsHonored(String mode) throws SQLException {
        try (Connection conn = openConnection(mode, true);
                PreparedStatement ps = conn.prepareStatement("SELECT ? + 'x'")) {
            ps.setObject(1, 3, Types.VARCHAR);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("3x", rs.getString(1));
                assertFalse(rs.next());
            }
        }
    }

    /** Verifies setNString and NVARCHAR setObject preserve Unicode text when sendStringParametersAsUnicode=false. */
    @ParameterizedTest(name = "mode={0}")
    @MethodSource("prepareModes")
    public void testNationalCharacterSettersWithoutUnicodeDefault(String mode) throws SQLException {
        try (Connection conn = openConnection(mode, false);
                PreparedStatement ps = conn.prepareStatement("SELECT ?, ?")) {
            ps.setNString(1, UNICODE_TEXT);
            ps.setObject(2, UNICODE_TEXT, Types.NVARCHAR);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(UNICODE_TEXT, rs.getString(1));
                assertEquals(UNICODE_TEXT, rs.getString(2));
                assertFalse(rs.next());
            }
        }
    }

    /**
     * Verifies that when direct execution falls back to RPC (here because of stream parameters), character values keep
     * the types of typed binding: setString and setCharacterStream are varchar when
     * sendStringParametersAsUnicode=false, and setNString stays nvarchar. A later execution without streams inlines
     * literals again, and the string parameters are still sent as text.
     */
    @ParameterizedTest(name = "mode={0}, unicode={1}")
    @MethodSource("configurations")
    public void testRpcFallbackKeepsSetterCharacterTypes(String mode, boolean unicode) throws SQLException {
        int expectedLength = unicode ? 6 : 3;
        try (Connection conn = openConnection(mode, unicode); PreparedStatement ps = conn
                .prepareStatement("SELECT DATALENGTH(?), DATALENGTH(?), DATALENGTH(?), ?, ?")) {
            ps.setString(1, "abc");
            ps.setNString(2, "abc");
            ps.setCharacterStream(3, new StringReader("abc"));
            ps.setString(4, "abc");
            ps.setBinaryStream(5, new ByteArrayInputStream(BINARY_VALUE));
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(expectedLength, rs.getInt(1));
                assertEquals(6, rs.getInt(2));
                assertEquals(expectedLength, rs.getInt(3));
                assertEquals("abc", rs.getString(4));
                assertArrayEquals(BINARY_VALUE, rs.getBytes(5));
                assertFalse(rs.next());
            }

            ps.setString(3, "abc");
            ps.setBytes(5, BINARY_VALUE);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(expectedLength, rs.getInt(1));
                assertEquals(6, rs.getInt(2));
                assertEquals(expectedLength, rs.getInt(3));
                assertEquals("abc", rs.getString(4));
                assertArrayEquals(BINARY_VALUE, rs.getBytes(5));
                assertFalse(rs.next());
            }
        }
    }

    /** Verifies numeric, bit, binary and NULL markers followed directly by a keyword (e.g. '?and') stay valid SQL. */
    @ParameterizedTest(name = "{0}, mode={1}")
    @MethodSource("adjacentKeywordCases")
    public void testAdjacentKeywordAfterNonStringParameter(QueryCase testCase, String mode) throws SQLException {
        try (Connection conn = openConnection(mode, true); PreparedStatement ps = conn.prepareStatement(testCase.sql)) {
            testCase.binder.bind(ps);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    /** Verifies a table-valued parameter bound with setStructured can be queried as a table source. */
    @ParameterizedTest(name = "mode={0}")
    @MethodSource("prepareModes")
    public void testTableValuedParameter(String mode) throws SQLException {
        String typeName = "PrepareMethodLiteralTvp_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection conn = openConnection(mode, true); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TYPE " + typeName + " AS TABLE (value int)");
            try {
                SQLServerDataTable table = new SQLServerDataTable();
                table.addColumnMetadata("value", Types.INTEGER);
                table.addRow(1);
                table.addRow(2);
                table.addRow(3);
                try (SQLServerPreparedStatement ps = (SQLServerPreparedStatement) conn
                        .prepareStatement("SELECT SUM(value) FROM ?")) {
                    ps.setStructured(1, typeName, table);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(6, rs.getInt(1));
                        assertFalse(rs.next());
                    }
                }
            } finally {
                stmt.execute("DROP TYPE IF EXISTS " + typeName);
            }
        }
    }

    private static void bindLiteralValues(PreparedStatement ps, String text) throws SQLException {
        ps.setString(1, text);
        ps.setObject(2, text, Types.VARCHAR);
        ps.setBytes(3, BINARY_VALUE);
        ps.setString(4, "2026-10-15 16:17:39.360");
    }

    private static void assertLiteralValues(ResultSet rs, String text) throws SQLException {
        assertEquals(text, rs.getString(1));
        assertEquals(text, rs.getString(2));
        assertArrayEquals(BINARY_VALUE, rs.getBytes(3));
        assertEquals(Timestamp.valueOf("2026-10-15 16:17:39.360"), rs.getTimestamp(4));
    }

    private static Stream<Arguments> literalExecutionCases() {
        return prepareModes().flatMap(mode -> {
            Stream<Boolean> creationCases = "none".equals(mode) || "scopeTempTablesToConnection".equals(mode) ? Stream
                    .of(false, true) : Stream.of(false);
            return creationCases
                    .flatMap(create -> Stream.of(false, true).map(batch -> Arguments.of(mode, create, batch)));
        });
    }

    private static Stream<String> prepareModes() {
        return Arrays.stream(MODES);
    }

    private static Stream<Arguments> configurations() {
        return prepareModes().flatMap(mode -> Stream.of(false, true).map(unicode -> Arguments.of(mode, unicode)));
    }

    private static Stream<Arguments> callableConfigurations() {
        return prepareModes().flatMap(mode -> Stream.of(false, true).map(callable -> Arguments.of(mode, callable)));
    }

    private static Stream<Arguments> predicateCases() {
        PredicateCase[] cases = {
                new PredicateCase("character-string", "char(9)", "'SAMPLE123'", "=",
                        ps -> ps.setString(1, "SAMPLE123")),
                new PredicateCase("integer", "int", "1234567", "=", ps -> ps.setInt(1, 1234567)),
                new PredicateCase("datetime-equality", "datetime", "'" + DATE_TEXT + "'", "=",
                        ps -> ps.setString(1, DATE_TEXT)),
                new PredicateCase("datetime-range", "datetime", "'" + TIMESTAMP_TEXT + "'", "<=",
                        ps -> ps.setString(1, TIMESTAMP_TEXT)),
                new PredicateCase("date-string", "date", "'" + DATE_TEXT + "'", "=", ps -> ps.setString(1, DATE_TEXT)),
                new PredicateCase("unicode-column", "nvarchar(20)", "N'SAMPLE123'", "=",
                        ps -> ps.setString(1, "SAMPLE123")),
                new PredicateCase("integer-string", "int", "42", "=", ps -> ps.setString(1, "42")),
                new PredicateCase("bit-string", "bit", "1", "=", ps -> ps.setString(1, "1")),
                new PredicateCase("decimal-string", "decimal(10,2)", "123.45", "=", ps -> ps.setString(1, "123.45")),
                new PredicateCase("datetime2-string", "datetime2(3)", "'" + TIMESTAMP_TEXT + "'", "=",
                        ps -> ps.setString(1, TIMESTAMP_TEXT)),
                new PredicateCase("time-string", "time(3)", "'12:23:45.047'", "=",
                        ps -> ps.setString(1, "12:23:45.047")),
                new PredicateCase("smalldatetime-string", "smalldatetime", "'" + DATE_TEXT + "'", "=",
                        ps -> ps.setString(1, DATE_TEXT)),
                new PredicateCase("guid-string", "uniqueidentifier", "'" + GUID_TEXT + "'", "=",
                        ps -> ps.setString(1, GUID_TEXT)),
                new PredicateCase("datetimeoffset-string", "datetimeoffset", "'" + TIMESTAMP_TEXT + " +00:00'", "=",
                        ps -> ps.setString(1, TIMESTAMP_TEXT + " +00:00")),
                new PredicateCase("float-string", "float", "123.45", "=", ps -> ps.setString(1, "123.45")),
                new PredicateCase("binary-control", "varbinary(4)", "0x01020304", "=",
                        ps -> ps.setBytes(1, BINARY_VALUE))};
        return Arrays.stream(cases).flatMap(testCase -> prepareModes().flatMap(mode -> Stream.of(false, true).flatMap(
                unicode -> Stream.of(false, true).map(callable -> Arguments.of(testCase, mode, unicode, callable)))));
    }

    private static Stream<Arguments> roundTripCases() {
        ParameterCase[] cases = {new ParameterCase("integer", "int", ps -> ps.setInt(1, 42)),
                new ParameterCase("string", "nvarchar(50)", ps -> ps.setString(1, "sample")),
                new ParameterCase("boolean", "bit", ps -> ps.setBoolean(1, true)),
                new ParameterCase("decimal", "decimal(18,4)", ps -> ps.setBigDecimal(1, new BigDecimal("123.4500"))),
                new ParameterCase("binary", "varbinary(4)", ps -> ps.setBytes(1, BINARY_VALUE)),
                new ParameterCase("date", "date", ps -> ps.setDate(1, Date.valueOf(DATE_TEXT))),
                new ParameterCase("timestamp", "datetime2",
                        ps -> ps.setTimestamp(1, Timestamp.valueOf(TIMESTAMP_TEXT))),
                new ParameterCase("time", "time", ps -> ps.setTime(1, Time.valueOf("13:14:15"))),
                new ParameterCase("offset", "datetimeoffset",
                        ps -> ps.setObject(1, DateTimeOffset.valueOf(Timestamp.valueOf(TIMESTAMP_TEXT), 330),
                                microsoft.sql.Types.DATETIMEOFFSET)),
                new ParameterCase("high-precision-decimal", "decimal(38,10)",
                        ps -> ps.setBigDecimal(1, new BigDecimal("1234567890.1234567890"))),
                new ParameterCase("fractional-seconds", "datetime2(7)",
                        ps -> ps.setTimestamp(1, Timestamp.valueOf("2024-02-13 13:14:15.1234567"))),
                new ParameterCase("quoted-string", "nvarchar(200)", ps -> ps.setString(1, "O'Brien said 'hi'")),
                new ParameterCase("unicode-string", "nvarchar(200)",
                        ps -> ps.setString(1, "caf\u00e9 \u2014 \u540d\u524d \u2603")),
                new ParameterCase("sql-text-as-data", "nvarchar(200)", ps -> ps.setString(1, "x'); SELECT 99; --")),
                new ParameterCase("null-date", "date", ps -> ps.setNull(1, Types.DATE)),
                new ParameterCase("null-timestamp", "datetime2", ps -> ps.setNull(1, Types.TIMESTAMP)),
                new ParameterCase("null-decimal", "decimal(38,10)", ps -> ps.setNull(1, Types.DECIMAL)),
                // Time with milliseconds keeps its fractional seconds.
                new ParameterCase("time-milliseconds", "time(3)",
                        ps -> ps.setTime(1, new Time(Time.valueOf("13:14:15").getTime() + 500))),
                // With sendTimeAsDatetime (the default), a time is rounded to a 1/300 second tick: .001 becomes .000.
                new ParameterCase("time-off-tick-milliseconds", "time(3)",
                        ps -> ps.setTime(1, new Time(Time.valueOf("13:14:15").getTime() + 1))),
                // The same rounding applies to setTime with a Calendar.
                new ParameterCase("time-calendar-off-tick-milliseconds", "time(3)",
                        ps -> ps.setTime(1, new Time(Time.valueOf("13:14:15").getTime() + 1), foreignCalendar())),
                // Timestamp with 9-digit nanoseconds rounds like typed binding instead of failing to convert.
                new ParameterCase("timestamp-nanoseconds", "datetime2(7)",
                        ps -> ps.setTimestamp(1, Timestamp.valueOf("2024-02-13 13:14:15.123456789"))),
                // Timestamp with 7 fractional digits converts to legacy datetime with the same rounding.
                new ParameterCase("datetime-fractional-seconds", "datetime",
                        ps -> ps.setTimestamp(1, Timestamp.valueOf("2024-02-13 13:14:15.1234567"))),
                // setTimestamp with a Calendar applies the Calendar's time zone.
                new ParameterCase("timestamp-calendar", "datetime2(3)",
                        ps -> ps.setTimestamp(1, Timestamp.valueOf(TIMESTAMP_TEXT), foreignCalendar())),
                // setDate with a Calendar applies the Calendar's time zone.
                new ParameterCase("date-calendar", "date",
                        ps -> ps.setDate(1, Date.valueOf(DATE_TEXT), foreignCalendar())),
                // setTime with a Calendar applies the Calendar's time zone.
                new ParameterCase("time-calendar", "time(3)",
                        ps -> ps.setTime(1, Time.valueOf("13:14:15"), foreignCalendar())),
                // LocalDateTime with 9-digit nanoseconds rounds like typed binding.
                new ParameterCase("local-date-time-nanoseconds", "datetime2(7)",
                        ps -> ps.setObject(1, LocalDateTime.of(2024, 2, 13, 13, 14, 15, 123456789))),
                // LocalTime with 9-digit nanoseconds rounds like typed binding.
                new ParameterCase("local-time-nanoseconds", "time(7)",
                        ps -> ps.setObject(1, LocalTime.of(13, 14, 15, 123456789))),
                // Double beyond the decimal(38) range is sent as a float, not an out-of-range numeric literal.
                new ParameterCase("large-double", "float", ps -> ps.setDouble(1, 1.0E300)),
                // Tiny double keeps its value instead of a 300-digit plain decimal literal.
                new ParameterCase("small-double", "float", ps -> ps.setDouble(1, 1.0E-300)),
                // setObject with an INTEGER target sends an int, so the sql_variant base type matches typed binding.
                new ParameterCase("string-with-integer-target", "sql_variant",
                        ps -> ps.setObject(1, "123", Types.INTEGER)),
                // setObject with a scale argument rounds the decimal to that scale.
                new ParameterCase("decimal-with-target-scale", "decimal(10,3)",
                        ps -> ps.setObject(1, new BigDecimal("1.235"), Types.DECIMAL, 2)),
                // setCharacterStream sends the Reader's content, not its toString().
                new ParameterCase("character-stream", "nvarchar(max)",
                        ps -> ps.setCharacterStream(1, new StringReader("stream text"))),
                // setNCharacterStream sends the Reader's Unicode content.
                new ParameterCase("ncharacter-stream", "nvarchar(max)",
                        ps -> ps.setNCharacterStream(1, new StringReader(UNICODE_TEXT))),
                // setAsciiStream sends the stream's content, not its toString().
                new ParameterCase("ascii-stream", "varchar(max)",
                        ps -> ps.setAsciiStream(1,
                                new ByteArrayInputStream("ascii text".getBytes(StandardCharsets.US_ASCII)))),
                // setBinaryStream sends the stream's bytes, not its toString().
                new ParameterCase("binary-stream", "varbinary(max)",
                        ps -> ps.setBinaryStream(1, new ByteArrayInputStream(BINARY_VALUE))),
                // setSQLXML sends the XML document, not the SQLXML object's toString().
                new ParameterCase("sqlxml", "xml", ps -> {
                    SQLXML xml = ps.getConnection().createSQLXML();
                    xml.setString("<root><value>1</value></root>");
                    ps.setSQLXML(1, xml);
                })};
        return Arrays.stream(cases).flatMap(testCase -> prepareModes()
                .flatMap(mode -> Stream.of(false, true).map(callable -> Arguments.of(testCase, mode, callable))));
    }

    private static Stream<Arguments> adjacentKeywordCases() {
        QueryCase[] cases = {
                // Integer literal followed by 'and' must not become '5and'.
                new QueryCase("integer", "SELECT CASE WHEN 5 = ?and 1 = 1 THEN 1 ELSE 0 END", ps -> ps.setInt(1, 5)),
                // Decimal literal followed by 'and' must not become '5.5and'.
                new QueryCase("decimal", "SELECT CASE WHEN 5.5 = ?and 1 = 1 THEN 1 ELSE 0 END",
                        ps -> ps.setBigDecimal(1, new BigDecimal("5.5"))),
                // Bit literal followed by 'and' must not become '1and'.
                new QueryCase("boolean", "SELECT CASE WHEN 1 = ?and 1 = 1 THEN 1 ELSE 0 END",
                        ps -> ps.setBoolean(1, true)),
                // Hex literal followed by 'and' must not become the invalid '0x01020304and'.
                new QueryCase("binary", "SELECT CASE WHEN 0x01020304 = ?and 1 = 1 THEN 1 ELSE 0 END",
                        ps -> ps.setBytes(1, BINARY_VALUE)),
                // NULL literal followed by 'is' must not become the identifier 'NULLis'.
                new QueryCase("null", "SELECT CASE WHEN ?is null THEN 1 ELSE 0 END",
                        ps -> ps.setNull(1, Types.INTEGER))};
        return Arrays.stream(cases).flatMap(testCase -> prepareModes().map(mode -> Arguments.of(testCase, mode)));
    }

    // Uses a zone whose offset differs from the JVM default so that an ignored Calendar is observable.
    private static Calendar foreignCalendar() {
        TimeZone zone = TimeZone.getTimeZone("Pacific/Kiritimati");
        if (zone.getRawOffset() == TimeZone.getDefault().getRawOffset()) {
            zone = TimeZone.getTimeZone("Pacific/Pago_Pago");
        }
        return Calendar.getInstance(zone);
    }

    private static Connection openConnection(String mode, boolean unicode) throws SQLException {
        String url = TestUtils.removeProperty(connectionString, "prepareMethod");
        if (!"default".equals(mode)) {
            url = TestUtils.addOrOverrideProperty(url, "prepareMethod", mode);
        }
        url = TestUtils.addOrOverrideProperty(url, "sendStringParametersAsUnicode", Boolean.toString(unicode));
        return PrepUtil.getConnection(url);
    }

    private static Object readValue(Connection conn, String query, Binder binder,
            boolean callable) throws SQLException {
        try (PreparedStatement ps = callable ? conn.prepareCall(query) : conn.prepareStatement(query)) {
            binder.bind(ps);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                Object value = rs.getObject(1);
                assertFalse(rs.next());
                return value;
            }
        }
    }

    @FunctionalInterface
    private interface Binder {
        void bind(PreparedStatement statement) throws SQLException;
    }

    private static final class ParameterCase {
        final String name;
        final String sqlType;
        final Binder binder;

        ParameterCase(String name, String sqlType, Binder binder) {
            this.name = name;
            this.sqlType = sqlType;
            this.binder = binder;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static final class QueryCase {
        final String name;
        final String sql;
        final Binder binder;

        QueryCase(String name, String sql, Binder binder) {
            this.name = name;
            this.sql = sql;
            this.binder = binder;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    private static final class PredicateCase {
        final String name;
        final String sqlType;
        final String seed;
        final String operator;
        final Binder binder;

        PredicateCase(String name, String sqlType, String seed, String operator, Binder binder) {
            this.name = name;
            this.sqlType = sqlType;
            this.seed = seed;
            this.operator = operator;
            this.binder = binder;
        }

        @Override
        public String toString() {
            return name;
        }
    }
}
