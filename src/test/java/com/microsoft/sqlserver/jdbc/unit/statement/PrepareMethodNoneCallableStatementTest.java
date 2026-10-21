/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;

import microsoft.sql.DateTimeOffset;

/**
 * Regression coverage for CallableStatement execution with prepareMethod=none.
 */
@RunWith(JUnitPlatform.class)
public class PrepareMethodNoneCallableStatementTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    public void testDateInput() throws SQLException {
        Date expected = Date.valueOf("2026-10-07");
        assertInputRoundTrip("date", cs -> cs.setDate(1, expected), rs -> assertEquals(expected, rs.getDate(1)));
    }

    @Test
    public void testTimeInput() throws SQLException {
        Time expected = Time.valueOf("13:14:15");
        assertInputRoundTrip("time(7)", cs -> cs.setTime(1, expected), rs -> assertEquals(expected, rs.getTime(1)));
    }

    @Test
    public void testTimestampInput() throws SQLException {
        Timestamp expected = Timestamp.valueOf("2026-10-07 00:00:00.1234567");
        assertInputRoundTrip("datetime2(7)", cs -> cs.setTimestamp(1, expected),
                rs -> assertEquals(expected, rs.getTimestamp(1)));
    }

    @Test
    public void testDateTimeInput() throws SQLException {
        Timestamp expected = Timestamp.valueOf("2026-10-07 00:00:00.0");
        assertInputRoundTrip("datetime", cs -> cs.setTimestamp(1, expected),
                rs -> assertEquals(expected, rs.getTimestamp(1)));
    }

    @Test
    public void testUtilDateInput() throws SQLException {
        java.util.Date value = new java.util.Date(Timestamp.valueOf("2026-10-07 00:00:00.123").getTime());
        Timestamp expected = new Timestamp(value.getTime());
        assertInputRoundTrip("datetime2(3)", cs -> cs.setObject(1, value),
                rs -> assertEquals(expected, rs.getTimestamp(1)));
    }

    @Test
    public void testHighPrecisionDecimalInput() throws SQLException {
        BigDecimal expected = new BigDecimal("1234567890123.1234567");
        assertInputRoundTrip("decimal(20,7)", cs -> cs.setBigDecimal(1, expected),
                rs -> assertEquals(expected, rs.getBigDecimal(1)));
    }

    @Test
    public void testHighScaleDecimalInput() throws SQLException {
        BigDecimal expected = new BigDecimal("0.1234567");
        assertInputRoundTrip("decimal(8,7)", cs -> cs.setBigDecimal(1, expected),
                rs -> assertEquals(expected, rs.getBigDecimal(1)));
    }

    @Test
    public void testSqlVariantPreservesDecimalTypesAcrossFormattingThreshold() throws SQLException {
        String procedureName = temporaryProcedureName("callable_none_sql_variant");
        String connStrWithNone = connectionString + ";prepareMethod=none";
        BigDecimal lowPrecision = new BigDecimal("12345.123456");
        BigDecimal precisionBoundary = new BigDecimal("12345678901234.12345");
        BigDecimal negativeHighPrecision = new BigDecimal("-12345678901234.12345");
        BigDecimal highScale = new BigDecimal("-0.0000001");
        BigDecimal scientificNotation = new BigDecimal("1E-7");

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrWithNone);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE " + procedureName
                    + " @low sql_variant, @boundary sql_variant, @negative sql_variant,"
                    + " @highScale sql_variant, @scientific sql_variant"
                    + " AS SELECT @low, SQL_VARIANT_PROPERTY(@low, 'BaseType'),"
                    + " SQL_VARIANT_PROPERTY(@low, 'Precision'), SQL_VARIANT_PROPERTY(@low, 'Scale'),"
                    + " @boundary, SQL_VARIANT_PROPERTY(@boundary, 'BaseType'),"
                    + " SQL_VARIANT_PROPERTY(@boundary, 'Precision'), SQL_VARIANT_PROPERTY(@boundary, 'Scale'),"
                    + " @negative, SQL_VARIANT_PROPERTY(@negative, 'BaseType'),"
                    + " SQL_VARIANT_PROPERTY(@negative, 'Precision'), SQL_VARIANT_PROPERTY(@negative, 'Scale'),"
                    + " @highScale, SQL_VARIANT_PROPERTY(@highScale, 'BaseType'),"
                    + " @scientific, SQL_VARIANT_PROPERTY(@scientific, 'BaseType')");

            try (CallableStatement cs = conn.prepareCall("{call " + procedureName + "(?, ?, ?, ?, ?)}")) {
                cs.setBigDecimal(1, lowPrecision);
                cs.setBigDecimal(2, precisionBoundary);
                cs.setBigDecimal(3, negativeHighPrecision);
                cs.setBigDecimal(4, highScale);
                cs.setBigDecimal(5, scientificNotation);

                try (ResultSet rs = cs.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(lowPrecision, rs.getBigDecimal(1));
                    assertDecimalBaseType(rs.getString(2));
                    assertEquals(lowPrecision.precision(), rs.getInt(3));
                    assertEquals(lowPrecision.scale(), rs.getInt(4));
                    assertEquals(precisionBoundary, rs.getBigDecimal(5));
                    assertDecimalBaseType(rs.getString(6));
                    assertEquals(precisionBoundary.precision(), rs.getInt(7));
                    assertEquals(precisionBoundary.scale(), rs.getInt(8));
                    assertEquals(negativeHighPrecision, rs.getBigDecimal(9));
                    assertDecimalBaseType(rs.getString(10));
                    assertEquals(negativeHighPrecision.precision(), rs.getInt(11));
                    assertEquals(negativeHighPrecision.scale(), rs.getInt(12));
                    assertEquals(highScale, rs.getBigDecimal(13));
                    assertDecimalBaseType(rs.getString(14));
                    assertEquals(scientificNotation, rs.getBigDecimal(15));
                    assertDecimalBaseType(rs.getString(16));
                    assertFalse(rs.next());
                }
            } finally {
                TestUtils.dropProcedureIfExists(procedureName, stmt);
            }
        }
    }

    @Test
    public void testTypedNullInput() throws SQLException {
        assertInputRoundTrip("date", cs -> cs.setNull(1, Types.DATE), rs -> assertNull(rs.getDate(1)));
    }

    @Test
    public void testRepeatedTimestampExecution() throws Exception {
        String procedureName = temporaryProcedureName("callable_none_repeated");
        String connStrWithNone = connectionString + ";prepareMethod=none";
        Timestamp expected = Timestamp.valueOf("2026-10-07 00:00:00.1234567");

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrWithNone);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE " + procedureName
                    + " @value datetime2(7) AS SELECT @value");

            try (CallableStatement cs = conn.prepareCall("{call " + procedureName + "(?)}")) {
                cs.setTimestamp(1, expected);
                for (int execution = 0; execution < 2; execution++) {
                    try (ResultSet rs = cs.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(expected, rs.getTimestamp(1));
                        assertFalse(rs.next());
                    }
                }
                assertNoPreparedHandle(cs);
            } finally {
                TestUtils.dropProcedureIfExists(procedureName, stmt);
            }
        }
    }

    @Test
    public void testUnaffectedInputTypes() throws SQLException {
        String procedureName = temporaryProcedureName("callable_none_controls");
        String connStrWithNone = connectionString + ";prepareMethod=none";

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrWithNone);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE " + procedureName
                    + " @number int, @text nvarchar(20), @flag bit"
                    + " AS SELECT @number, @text, @flag");

            try (CallableStatement cs = conn.prepareCall("{call " + procedureName + "(?, ?, ?)}")) {
                cs.setInt(1, 42);
                cs.setString(2, "control");
                cs.setBoolean(3, true);

                try (ResultSet rs = cs.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(42, rs.getInt(1));
                    assertEquals("control", rs.getString(2));
                    assertTrue(rs.getBoolean(3));
                    assertFalse(rs.next());
                }
            } finally {
                TestUtils.dropProcedureIfExists(procedureName, stmt);
            }
        }
    }

    @Test
    public void testCustomerTimestampCall() throws SQLException {
        String procedureName = temporaryProcedureName("callable_none_customer");
        String connStrWithNone = connectionString + ";prepareMethod=none";
        Timestamp firstTimestamp = Timestamp.valueOf("2026-10-07 00:00:00.0");
        Timestamp secondTimestamp = Timestamp.valueOf("2026-10-06 00:00:00.0");

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrWithNone);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE " + procedureName
                    + " @firstValue datetime, @secondValue datetime, @name nvarchar(20)"
                    + " AS SELECT @firstValue, @secondValue, @name");

            try (CallableStatement cs = conn.prepareCall("{call " + procedureName + "(?, ?, ?)}")) {
                cs.setTimestamp(1, firstTimestamp);
                cs.setTimestamp(2, secondTimestamp);
                cs.setString(3, "pxHierarchy");

                try (ResultSet rs = cs.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(firstTimestamp, rs.getTimestamp(1));
                    assertEquals(secondTimestamp, rs.getTimestamp(2));
                    assertEquals("pxHierarchy", rs.getString(3));
                    assertFalse(rs.next());
                }
            } finally {
                TestUtils.dropProcedureIfExists(procedureName, stmt);
            }
        }
    }

    @Test
    public void testParameterOrderingWithMixedInputTypes() throws SQLException {
        String procedureName = temporaryProcedureName("callable_none_ordering");
        String connStrWithNone = connectionString + ";prepareMethod=none";
        Timestamp timestamp = Timestamp.valueOf("2026-10-07 00:00:00.123");
        BigDecimal decimal = new BigDecimal("12345678901234.12345");

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrWithNone);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE " + procedureName
                    + " @text nvarchar(20), @number int, @timestamp datetime2(3), @decimal decimal(19,5),"
                    + " @flag bit, @tail nvarchar(20)"
                    + " AS SELECT @text, @number, @timestamp, @decimal, @flag, @tail");

            try (CallableStatement cs = conn.prepareCall("{call " + procedureName + "(?, ?, ?, ?, ?, ?)}")) {
                cs.setString(1, "first");
                cs.setInt(2, 42);
                cs.setTimestamp(3, timestamp);
                cs.setBigDecimal(4, decimal);
                cs.setBoolean(5, true);
                cs.setString(6, "last");

                try (ResultSet rs = cs.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("first", rs.getString(1));
                    assertEquals(42, rs.getInt(2));
                    assertEquals(timestamp, rs.getTimestamp(3));
                    assertEquals(decimal, rs.getBigDecimal(4));
                    assertTrue(rs.getBoolean(5));
                    assertEquals("last", rs.getString(6));
                    assertFalse(rs.next());
                }
                assertNoPreparedHandle(cs);
            } catch (ReflectiveOperationException e) {
                throw new SQLException(e);
            } finally {
                TestUtils.dropProcedureIfExists(procedureName, stmt);
            }
        }
    }

    @Test
    public void testJavaTimeInputs() throws SQLException {
        String procedureName = temporaryProcedureName("callable_none_java_time");
        String connStrWithNone = connectionString + ";prepareMethod=none";
        LocalDateTime localDateTime = LocalDateTime.of(2026, 10, 7, 1, 2, 3, 123456700);
        LocalDate localDate = LocalDate.of(2026, 10, 7);
        LocalTime localTime = LocalTime.of(23, 59, 59, 999999900);
        OffsetDateTime offsetDateTime = OffsetDateTime.parse("2026-10-07T01:02:03.1234567+05:30");
        OffsetTime offsetTime = OffsetTime.parse("01:02:03.1234567+05:30");

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrWithNone);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE " + procedureName
                    + " @localDateTime datetime2(7), @localDate date, @localTime time(7),"
                    + " @offsetDateTime datetimeoffset(7), @offsetTime datetimeoffset(7)"
                    + " AS SELECT @localDateTime, @localDate, @localTime, @offsetDateTime, @offsetTime");

            try (CallableStatement cs = conn.prepareCall("{call " + procedureName + "(?, ?, ?, ?, ?)}")) {
                cs.setObject(1, localDateTime);
                cs.setObject(2, localDate);
                cs.setObject(3, localTime);
                cs.setObject(4, offsetDateTime);
                cs.setObject(5, offsetTime);

                try (ResultSet rs = cs.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(localDateTime, rs.getObject(1, LocalDateTime.class));
                    assertEquals(localDate, rs.getObject(2, LocalDate.class));
                    assertEquals(localTime, rs.getObject(3, LocalTime.class));
                    assertEquals(offsetDateTime, rs.getObject(4, OffsetDateTime.class));
                    // A time-only value has no date, so the server supplies its own default date when widening it
                    // to datetimeoffset; only the time of day and offset are meaningful.
                    OffsetDateTime offsetTimeResult = rs.getObject(5, OffsetDateTime.class);
                    assertEquals(offsetTime.toLocalTime(), offsetTimeResult.toLocalTime());
                    assertEquals(offsetTime.getOffset(), offsetTimeResult.getOffset());
                    assertFalse(rs.next());
                }
            } finally {
                TestUtils.dropProcedureIfExists(procedureName, stmt);
            }
        }
    }

    @Test
    public void testTemporalBoundariesAndNull() throws SQLException {
        String procedureName = temporaryProcedureName("callable_none_temporal_boundaries");
        String connStrWithNone = connectionString + ";prepareMethod=none";
        LocalDate minimumDate = LocalDate.of(1, 1, 1);
        LocalDate maximumDate = LocalDate.of(9999, 12, 31);
        LocalTime maximumTime = LocalTime.of(23, 59, 59, 999999900);

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrWithNone);
                Statement stmt = conn.createStatement()) {
            stmt.execute("SET LANGUAGE French");
            stmt.execute("SET DATEFORMAT mdy");
            stmt.execute("CREATE PROCEDURE " + procedureName
                    + " @minimumDate date, @maximumDate date, @maximumTime time(7), @nullable datetime2(7)"
                    + " AS SELECT @minimumDate, @maximumDate, @maximumTime, @nullable");

            try (CallableStatement cs = conn.prepareCall("{call " + procedureName + "(?, ?, ?, ?)}")) {
                cs.setObject(1, minimumDate);
                cs.setObject(2, maximumDate);
                cs.setObject(3, maximumTime);
                cs.setNull(4, Types.TIMESTAMP);

                try (ResultSet rs = cs.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(minimumDate, rs.getObject(1, LocalDate.class));
                    assertEquals(maximumDate, rs.getObject(2, LocalDate.class));
                    assertEquals(maximumTime, rs.getObject(3, LocalTime.class));
                    assertNull(rs.getObject(4));
                    assertFalse(rs.next());
                }
            } finally {
                TestUtils.dropProcedureIfExists(procedureName, stmt);
            }
        }
    }

    @Test
    public void testUdfArgumentsRemainValidInExpressionContexts() throws SQLException {
        String suffix = RandomUtil.getIdentifier("callable_none_udf");
        String scalarFunction = "dbo." + AbstractSQLGenerator.escapeIdentifier(suffix + "_scalar");
        String tableFunction = "dbo." + AbstractSQLGenerator.escapeIdentifier(suffix + "_table");
        String tableName = AbstractSQLGenerator.escapeIdentifier(suffix + "_data");
        String connStrWithNone = connectionString + ";prepareMethod=none";
        BigDecimal expected = new BigDecimal("12345678901234.12345");

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrWithNone);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION " + scalarFunction
                    + " (@value decimal(19,5)) RETURNS decimal(19,5) AS BEGIN RETURN @value END");
            stmt.execute("CREATE FUNCTION " + tableFunction
                    + " (@value decimal(19,5)) RETURNS TABLE AS RETURN SELECT @value AS value");
            stmt.execute("CREATE TABLE " + tableName + " (id int PRIMARY KEY, value decimal(19,5))");

            try {
                try (PreparedStatement ps = conn.prepareStatement("SELECT " + scalarFunction + "(?)")) {
                    ps.setBigDecimal(1, expected);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(expected, rs.getBigDecimal(1));
                    }
                }

                try (PreparedStatement ps = conn.prepareStatement("SELECT value FROM " + tableFunction + "(?)")) {
                    ps.setBigDecimal(1, expected);
                    try (ResultSet rs = ps.executeQuery()) {
                        assertTrue(rs.next());
                        assertEquals(expected, rs.getBigDecimal(1));
                    }
                }

                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + tableName + " VALUES (1, "
                        + scalarFunction + "(?))")) {
                    ps.setBigDecimal(1, expected);
                    assertEquals(1, ps.executeUpdate());
                }

                try (PreparedStatement ps = conn.prepareStatement("UPDATE " + tableName + " SET value = "
                        + scalarFunction + "(?) WHERE value = " + scalarFunction + "(?)")) {
                    ps.setBigDecimal(1, expected.negate());
                    ps.setBigDecimal(2, expected);
                    assertEquals(1, ps.executeUpdate());
                }

                try (PreparedStatement ps = conn.prepareStatement("DELETE FROM " + tableName + " WHERE value = "
                        + scalarFunction + "(?)")) {
                    ps.setBigDecimal(1, expected.negate());
                    assertEquals(1, ps.executeUpdate());
                }
            } finally {
                // Test identifiers contain an apostrophe, which TestUtils.dropFunctionWithSchemaIfExists does not escape.
                stmt.execute("DROP TABLE IF EXISTS " + tableName);
                stmt.execute("DROP FUNCTION IF EXISTS " + tableFunction);
                stmt.execute("DROP FUNCTION IF EXISTS " + scalarFunction);
            }
        }
    }

    @Test
    public void testSupplementalUnaffectedInputTypes() throws SQLException {
        String procedureName = temporaryProcedureName("callable_none_supplemental_controls");
        String connStrWithNone = connectionString + ";prepareMethod=none";
        BigDecimal expectedDecimal = new BigDecimal("12345678901234.1234");
        byte[] expectedBytes = {0x01, 0x23, 0x45, 0x67};
        DateTimeOffset expectedDateTimeOffset = DateTimeOffset.valueOf(
                Timestamp.valueOf("2026-10-07 05:00:00"), 0);

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrWithNone);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE " + procedureName
                    + " @decimalValue decimal(18,4), @binaryValue varbinary(4),"
                    + " @dateTimeOffsetValue datetimeoffset"
                    + " AS SELECT @decimalValue, @binaryValue, @dateTimeOffsetValue");

            try (CallableStatement cs = conn.prepareCall("{call " + procedureName + "(?, ?, ?)}")) {
                cs.setBigDecimal(1, expectedDecimal);
                cs.setBytes(2, expectedBytes);
                cs.setObject(3, expectedDateTimeOffset, microsoft.sql.Types.DATETIMEOFFSET);

                try (ResultSet rs = cs.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(expectedDecimal, rs.getBigDecimal(1));
                    assertArrayEquals(expectedBytes, rs.getBytes(2));
                    assertEquals(expectedDateTimeOffset, ((SQLServerResultSet) rs).getDateTimeOffset(3));
                    assertFalse(rs.next());
                }
            } finally {
                TestUtils.dropProcedureIfExists(procedureName, stmt);
            }
        }
    }

    @Test
    public void testAffectedExpressionsWithPrepareMethodNone() throws SQLException {
        String connStrWithNone = connectionString + ";prepareMethod=none";
        Date expectedDate = Date.valueOf("2026-10-07");
        Time expectedTime = Time.valueOf("13:14:15");
        Timestamp expectedTimestamp = Timestamp.valueOf("2026-10-07 00:00:00.1234567");
        BigDecimal expectedHighPrecisionDecimal = new BigDecimal("1234567890123.1234567");
        BigDecimal expectedHighScaleDecimal = new BigDecimal("0.1234567");

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrWithNone);
                PreparedStatement ps = conn.prepareStatement("SELECT ?, ?, ?, ?, ?, ?")) {
            ps.setDate(1, expectedDate);
            ps.setTime(2, expectedTime);
            ps.setTimestamp(3, expectedTimestamp);
            ps.setBigDecimal(4, expectedHighPrecisionDecimal);
            ps.setBigDecimal(5, expectedHighScaleDecimal);
            ps.setNull(6, Types.TIMESTAMP);

            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(expectedDate, rs.getDate(1));
                assertEquals(expectedTime, rs.getTime(2));
                assertEquals(expectedTimestamp, rs.getTimestamp(3));
                assertEquals(expectedHighPrecisionDecimal, rs.getBigDecimal(4));
                assertEquals(expectedHighScaleDecimal, rs.getBigDecimal(5));
                assertNull(rs.getTimestamp(6));
                assertFalse(rs.next());
            }
        }
    }

    @Test
    public void testAffectedInputTypesWithPrepexec() throws SQLException {
        String procedureName = temporaryProcedureName("callable_prepexec_controls");
        Date expectedDate = Date.valueOf("2026-10-07");
        Time expectedTime = Time.valueOf("13:14:15");
        Timestamp expectedTimestamp = Timestamp.valueOf("2026-10-07 00:00:00.1234567");
        BigDecimal expectedDecimal = new BigDecimal("1234567890123.1234567");

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connectionString);
                Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE " + procedureName
                    + " @date date, @time time(7), @timestamp datetime2(7), @decimal decimal(20,7)"
                    + " AS SELECT @date, @time, @timestamp, @decimal");

            try (CallableStatement cs = conn.prepareCall("{call " + procedureName + "(?, ?, ?, ?)}")) {
                cs.setDate(1, expectedDate);
                cs.setTime(2, expectedTime);
                cs.setTimestamp(3, expectedTimestamp);
                cs.setBigDecimal(4, expectedDecimal);

                try (ResultSet rs = cs.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(expectedDate, rs.getDate(1));
                    assertEquals(expectedTime, rs.getTime(2));
                    assertEquals(expectedTimestamp, rs.getTimestamp(3));
                    assertEquals(expectedDecimal, rs.getBigDecimal(4));
                    assertFalse(rs.next());
                }
            } finally {
                TestUtils.dropProcedureIfExists(procedureName, stmt);
            }
        }
    }

    /** Verifies the {? = call fn(?)} return value is populated. */
    @ParameterizedTest(name = "prepareMethod={0}")
    @ValueSource(strings = {"prepexec", "none"})
    public void testCallableFunctionReturnValue(String prepareMethod) throws SQLException {
        String functionName = "dbo." + AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("callable_none_fn"));

        try (SQLServerConnection conn = openConnection(prepareMethod); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE FUNCTION " + functionName + " (@value int) RETURNS int AS BEGIN RETURN @value + 11 END");

            try (CallableStatement cs = conn.prepareCall("{? = call " + functionName + "(?)}")) {
                cs.registerOutParameter(1, Types.INTEGER);
                cs.setInt(2, 31);
                cs.execute();
                assertEquals(42, cs.getInt(1));
            } finally {
                stmt.execute("DROP FUNCTION IF EXISTS " + functionName);
            }
        }
    }

    /** Verifies a registered OUTPUT parameter is returned from a stored procedure. */
    @ParameterizedTest(name = "prepareMethod={0}")
    @ValueSource(strings = {"prepexec", "none"})
    public void testCallableOutputParameter(String prepareMethod) throws SQLException {
        String procedureName = temporaryProcedureName("callable_none_output");

        try (SQLServerConnection conn = openConnection(prepareMethod); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE " + procedureName
                    + " @input int, @output int OUTPUT AS SET @output = @input * 3");

            try (CallableStatement cs = conn.prepareCall("{call " + procedureName + "(?, ?)}")) {
                cs.setInt(1, 9);
                cs.registerOutParameter(2, Types.INTEGER);
                cs.execute();
                assertEquals(27, cs.getInt(2));
            } finally {
                TestUtils.dropProcedureIfExists(procedureName, stmt);
            }
        }
    }

    /** Verifies an INOUT parameter sends its input value and returns the updated value. */
    @ParameterizedTest(name = "prepareMethod={0}")
    @ValueSource(strings = {"prepexec", "none"})
    public void testCallableInOutParameter(String prepareMethod) throws SQLException {
        String procedureName = temporaryProcedureName("callable_none_inout");

        try (SQLServerConnection conn = openConnection(prepareMethod); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE " + procedureName
                    + " @value int OUTPUT AS SET @value = @value + 5");

            try (CallableStatement cs = conn.prepareCall("{call " + procedureName + "(?)}")) {
                cs.setInt(1, 7);
                cs.registerOutParameter(1, Types.INTEGER);
                cs.execute();
                assertEquals(12, cs.getInt(1));
            } finally {
                TestUtils.dropProcedureIfExists(procedureName, stmt);
            }
        }
    }

    /** Verifies a procedure can return both a result set and an OUTPUT parameter. */
    @ParameterizedTest(name = "prepareMethod={0}")
    @ValueSource(strings = {"prepexec", "none"})
    public void testCallableOutputParameterWithResultSet(String prepareMethod) throws SQLException {
        String procedureName = temporaryProcedureName("callable_none_output_rs");

        try (SQLServerConnection conn = openConnection(prepareMethod); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE " + procedureName
                    + " @input nvarchar(20), @length int OUTPUT AS BEGIN SELECT UPPER(@input);"
                    + " SET @length = LEN(@input) END");

            try (CallableStatement cs = conn.prepareCall("{call " + procedureName + "(?, ?)}")) {
                cs.setString(1, "sample");
                cs.registerOutParameter(2, Types.INTEGER);
                try (ResultSet rs = cs.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("SAMPLE", rs.getString(1));
                    assertFalse(rs.next());
                }
                assertEquals(6, cs.getInt(2));
            } finally {
                TestUtils.dropProcedureIfExists(procedureName, stmt);
            }
        }
    }

    /** Verifies parameters bound by name, out of declaration order, reach the matching procedure parameters. */
    @ParameterizedTest(name = "prepareMethod={0}")
    @ValueSource(strings = {"prepexec", "none"})
    public void testCallableNamedParameters(String prepareMethod) throws SQLException {
        String procedureName = "dbo."
                + AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("callable_none_named"));

        try (SQLServerConnection conn = openConnection(prepareMethod); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE PROCEDURE " + procedureName
                    + " @number int, @text nvarchar(20), @stamp datetime2(3) AS SELECT @number, @text, @stamp");

            Timestamp stamp = Timestamp.valueOf("2026-10-07 01:02:03.456");
            try (CallableStatement cs = conn.prepareCall("{call " + procedureName + "(?, ?, ?)}")) {
                // Bind by name in an order that differs from the declaration order.
                cs.setTimestamp("stamp", stamp);
                cs.setString("@text", "named");
                cs.setInt("number", 42);
                try (ResultSet rs = cs.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(42, rs.getInt(1));
                    assertEquals("named", rs.getString(2));
                    assertEquals(stamp, rs.getTimestamp(3));
                    assertFalse(rs.next());
                }
            } finally {
                stmt.execute("DROP PROCEDURE IF EXISTS " + procedureName);
            }
        }
    }

    private static SQLServerConnection openConnection(String prepareMethod) throws SQLException {
        return (SQLServerConnection) PrepUtil
                .getConnection(TestUtils.addOrOverrideProperty(connectionString, "prepareMethod", prepareMethod));
    }

    private void assertInputRoundTrip(String sqlType, CallableParameterSetter setter, ResultSetVerifier verifier)
            throws SQLException {
        String procedureName = temporaryProcedureName("callable_none_input");
        String connStrWithNone = connectionString + ";prepareMethod=none";

        try (SQLServerConnection conn = (SQLServerConnection) PrepUtil.getConnection(connStrWithNone);
                Statement stmt = conn.createStatement()) {
            stmt.execute("SET LANGUAGE British");
            stmt.execute("SET DATEFORMAT dmy");
            stmt.execute("CREATE PROCEDURE " + procedureName
                    + " @value " + sqlType + " AS SELECT @value");

            try (CallableStatement cs = conn.prepareCall("{call " + procedureName + "(?)}")) {
                setter.set(cs);
                try (ResultSet rs = cs.executeQuery()) {
                    assertTrue(rs.next());
                    verifier.verify(rs);
                    assertFalse(rs.next());
                }
            } finally {
                TestUtils.dropProcedureIfExists(procedureName, stmt);
            }
        }
    }

    // SQL Server reports numeric constants as "numeric", a synonym of "decimal"; the type must not be a string type.
    private static void assertDecimalBaseType(String baseType) {
        assertTrue("decimal".equals(baseType) || "numeric".equals(baseType),
                "Expected a decimal/numeric base type but was: " + baseType);
    }

    private static String temporaryProcedureName(String prefix) {
        return AbstractSQLGenerator.escapeIdentifier("#" + RandomUtil.getIdentifier(prefix));
    }

    private static void assertNoPreparedHandle(CallableStatement statement) throws ReflectiveOperationException {
        Field handleField = SQLServerPreparedStatement.class.getDeclaredField("prepStmtHandle");
        handleField.setAccessible(true);
        assertEquals(0, handleField.getInt(statement));

        Field cachedHandleField = SQLServerPreparedStatement.class.getDeclaredField("cachedPreparedStatementHandle");
        cachedHandleField.setAccessible(true);
        assertNull(cachedHandleField.get(statement));
    }

    @FunctionalInterface
    private interface CallableParameterSetter {
        void set(CallableStatement statement) throws SQLException;
    }

    @FunctionalInterface
    private interface ResultSetVerifier {
        void verify(ResultSet resultSet) throws SQLException;
    }
}
