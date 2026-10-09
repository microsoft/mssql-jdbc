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

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
                PreparedStatement ps = conn.prepareStatement("SELECT ?, ?, ?, ?, ?")) {
            ps.setDate(1, expectedDate);
            ps.setTime(2, expectedTime);
            ps.setTimestamp(3, expectedTimestamp);
            ps.setBigDecimal(4, expectedHighPrecisionDecimal);
            ps.setBigDecimal(5, expectedHighScaleDecimal);

            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(expectedDate, rs.getDate(1));
                assertEquals(expectedTime, rs.getTime(2));
                assertEquals(expectedTimestamp, rs.getTimestamp(3));
                assertEquals(expectedHighPrecisionDecimal, rs.getBigDecimal(4));
                assertEquals(expectedHighScaleDecimal, rs.getBigDecimal(5));
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
