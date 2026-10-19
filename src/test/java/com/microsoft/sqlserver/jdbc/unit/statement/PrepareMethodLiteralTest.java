/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

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
                new ParameterCase("null-decimal", "decimal(38,10)", ps -> ps.setNull(1, Types.DECIMAL))};
        return Arrays.stream(cases).flatMap(testCase -> prepareModes()
                .flatMap(mode -> Stream.of(false, true).map(callable -> Arguments.of(testCase, mode, callable))));
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
