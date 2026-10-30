/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ParameterMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


/** Integration coverage for statement reuse and metadata parity without server-side preparation. */
@RunWith(JUnitPlatform.class)
public class PrepareMethodNoneMetadataTest extends AbstractTest {
    private static String noneConnectionString;
    private static String prepexecConnectionString;

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
        String url = TestUtils.addOrOverrideProperty(connectionString, "disableStatementPooling", "true");
        url = TestUtils.addOrOverrideProperty(url, "statementPoolingCacheSize", "0");
        url = TestUtils.addOrOverrideProperty(url, "columnEncryptionSetting", "Disabled");
        url = TestUtils.addOrOverrideProperty(url, "sendStringParametersAsUnicode", "true");
        url = TestUtils.addOrOverrideProperty(url, "useFmtOnly", "false");
        noneConnectionString = TestUtils.addOrOverrideProperty(url, "prepareMethod", "none");
        prepexecConnectionString = TestUtils.addOrOverrideProperty(url, "prepareMethod", "prepexec");
    }

    @ParameterizedTest(name = "mixed bindings, prepareOnFirst={0}")
    @ValueSource(booleans = {true, false})
    public void testRepeatedBindingsAndPreparedHandles(boolean prepareOnFirst) throws SQLException {
        String sql = "SELECT CAST(? AS nvarchar(64)), CAST(? AS bigint), CAST(? AS decimal(19,4)), "
                + "CAST(? AS int), CAST(? AS date), CAST(? AS datetime2(7))";
        String[] texts = {"ordinary", "O'Brien", "caf\u00e9 \u540d\u524d"};
        long[] integers = {4294967296L, -4294967296L, 9223372036854775806L};
        String[] decimals = {"12345.6700", "-23456.7800", "34567.8900"};
        try (SQLServerConnection actual = PrepUtil.getConnection(noneConnectionString);
                SQLServerConnection reference = PrepUtil.getConnection(prepexecConnectionString)) {
            actual.setEnablePrepareOnFirstPreparedStatementCall(prepareOnFirst);
            reference.setEnablePrepareOnFirstPreparedStatementCall(prepareOnFirst);
            assertFalse(actual.isStatementPoolingEnabled());
            assertFalse(reference.isStatementPoolingEnabled());
            try (SQLServerPreparedStatement direct = (SQLServerPreparedStatement) actual.prepareStatement(sql);
                    SQLServerPreparedStatement rpc = (SQLServerPreparedStatement) reference.prepareStatement(sql)) {
                int preparedHandle = 0;
                assertEquals(0, direct.getPreparedStatementHandle());
                assertEquals(0, rpc.getPreparedStatementHandle());
                for (int execution = 0; execution < 3; execution++) {
                    BigDecimal decimal = new BigDecimal(decimals[execution]);
                    Date date = Date.valueOf(LocalDate.of(2024, 2, 13).plusDays(execution));
                    Timestamp timestamp = Timestamp.valueOf("2024-02-13 12:23:45.1234567");
                    timestamp.setNanos(123456700 + execution * 100);
                    for (SQLServerPreparedStatement ps : new SQLServerPreparedStatement[] {direct, rpc}) {
                        ps.setString(1, texts[execution]);
                        ps.setLong(2, integers[execution]);
                        ps.setBigDecimal(3, decimal);
                        ps.setNull(4, Types.INTEGER);
                        ps.setDate(5, date);
                        ps.setTimestamp(6, timestamp);
                    }
                    try (ResultSet expected = rpc.executeQuery(); ResultSet result = direct.executeQuery()) {
                        assertTrue(expected.next());
                        assertTrue(result.next());
                        Object[] values = {texts[execution], integers[execution], decimal, null, date, timestamp};
                        for (int column = 1; column <= values.length; column++) {
                            assertEquals(values[column - 1], expected.getObject(column), "Reference column " + column);
                            assertEquals(expected.getObject(column), result.getObject(column),
                                    "Execution " + execution + ", column " + column);
                        }
                        assertFalse(expected.next());
                        assertFalse(result.next());
                    }
                    assertEquals(0, direct.getPreparedStatementHandle(), "none must never prepare");
                    int handle = rpc.getPreparedStatementHandle();
                    if (!prepareOnFirst && 0 == execution) {
                        assertEquals(0, handle, "The first prepexec execution must remain unprepared");
                    } else {
                        assertTrue(handle > 0, "prepexec must acquire a server handle");
                        if (0 == preparedHandle) {
                            preparedHandle = handle;
                        }
                        assertEquals(preparedHandle, handle, "Rebinding must reuse the prepared handle");
                    }
                }
            }
        }
    }

    @ParameterizedTest(name = "result metadata across stream fallback, prepareOnFirst={0}")
    @ValueSource(booleans = {true, false})
    public void testResultMetadataAcrossDirectRpcTransition(boolean prepareOnFirst) throws SQLException, IOException {
        String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("none_result_metadata"));
        byte[][] values = {{1, 2}, {3, 4}, {5, 6}};
        String sql = "SELECT payload AS binary_label, amount AS decimal_label, label AS text_label FROM " + table
                + " WHERE payload = ?";
        try (SQLServerConnection actual = PrepUtil.getConnection(noneConnectionString);
                SQLServerConnection reference = PrepUtil.getConnection(prepexecConnectionString);
                Statement setup = actual.createStatement()) {
            actual.setEnablePrepareOnFirstPreparedStatementCall(prepareOnFirst);
            reference.setEnablePrepareOnFirstPreparedStatementCall(prepareOnFirst);
            setup.executeUpdate("CREATE TABLE " + table
                    + " (payload varbinary(32) NOT NULL, amount decimal(19,4) NULL, label nvarchar(40) NOT NULL)");
            try {
                setup.executeUpdate("INSERT INTO " + table
                        + " VALUES (0x0102,1.2500,N'first'),(0x0304,2.5000,N'second'),(0x0506,3.7500,N'third')");
                try (SQLServerPreparedStatement direct = (SQLServerPreparedStatement) actual.prepareStatement(sql);
                        SQLServerPreparedStatement rpc = (SQLServerPreparedStatement) reference.prepareStatement(sql)) {
                    List<List<Object>> before = resultMetadata(rpc.getMetaData());
                    assertEquals(before, resultMetadata(direct.getMetaData()), "Metadata before execution");
                    for (int execution = 0; execution < 3; execution++) {
                        // A non-null stream forces an unprepared RPC for none; the final bytes keep that RPC route.
                        try (ByteArrayInputStream directStream = new ByteArrayInputStream(values[execution]);
                                ByteArrayInputStream rpcStream = new ByteArrayInputStream(values[execution])) {
                            if (1 == execution) {
                                direct.setBinaryStream(1, directStream, values[execution].length);
                                rpc.setBinaryStream(1, rpcStream, values[execution].length);
                            } else {
                                direct.setBytes(1, values[execution]);
                                rpc.setBytes(1, values[execution]);
                            }
                            try (ResultSet expected = rpc.executeQuery(); ResultSet result = direct.executeQuery()) {
                                assertEquals(before, resultMetadata(expected.getMetaData()));
                                assertEquals(before, resultMetadata(result.getMetaData()), "Execution " + execution);
                                assertEquals(before, resultMetadata(direct.getMetaData()));
                                assertTrue(expected.next());
                                assertTrue(result.next());
                                assertArrayEquals(values[execution], expected.getBytes(1));
                                assertArrayEquals(expected.getBytes(1), result.getBytes(1));
                                assertEquals(expected.getBigDecimal(2), result.getBigDecimal(2));
                                assertEquals(expected.getString(3), result.getString(3));
                                assertFalse(expected.next());
                                assertFalse(result.next());
                            }
                        }
                        assertEquals(0, direct.getPreparedStatementHandle(), "RPC fallback must not prepare none");
                    }
                }
            } finally {
                TestUtils.dropTableIfExists(table, setup);
            }
        }
    }

    @ParameterizedTest(name = "parameter metadata, prepareOnFirst={0}")
    @ValueSource(booleans = {true, false})
    public void testParameterMetadataBeforeAndAfterExecution(boolean prepareOnFirst) throws SQLException {
        String table = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("none_parameter_metadata"));
        long[] values = {4294967296L, -4294967296L, 9223372036854775806L};
        try (SQLServerConnection actual = PrepUtil.getConnection(noneConnectionString);
                SQLServerConnection reference = PrepUtil.getConnection(prepexecConnectionString);
                Statement setup = actual.createStatement()) {
            actual.setEnablePrepareOnFirstPreparedStatementCall(prepareOnFirst);
            reference.setEnablePrepareOnFirstPreparedStatementCall(prepareOnFirst);
            // Use a real local table: sp_describe_undeclared_parameters cannot reliably describe temp tables.
            setup.executeUpdate("CREATE TABLE " + table + " (id bigint NOT NULL)");
            try {
                setup.executeUpdate("INSERT INTO " + table + " VALUES (4294967296),(-4294967296),(9223372036854775806)");
                String sql = "SELECT id FROM " + table + " WHERE id = ?";
                try (SQLServerPreparedStatement direct = (SQLServerPreparedStatement) actual.prepareStatement(sql);
                        SQLServerPreparedStatement rpc = (SQLServerPreparedStatement) reference.prepareStatement(sql)) {
                    ParameterMetaData before = rpc.getParameterMetaData();
                    assertEquals(1, before.getParameterCount());
                    assertEquals(Types.BIGINT, before.getParameterType(1));
                    assertEquals("bigint", before.getParameterTypeName(1));
                    assertEquals(Long.class.getName(), before.getParameterClassName(1));
                    assertEquals(19, before.getPrecision(1));
                    assertEquals(0, before.getScale(1));
                    assertTrue(before.isSigned(1));
                    assertEquals(ParameterMetaData.parameterModeIn, before.getParameterMode(1));
                    for (int execution = -1; execution < 3; execution++) {
                        for (SQLServerPreparedStatement ps : new SQLServerPreparedStatement[] {direct, rpc}) {
                            if (execution >= 0) {
                                ps.setLong(1, values[execution]);
                                try (ResultSet rs = ps.executeQuery()) {
                                    assertTrue(rs.next());
                                    assertEquals(values[execution], rs.getLong(1));
                                    assertFalse(rs.next());
                                }
                            }
                            // Force refresh so this verifies server metadata, not just the connection's cached object.
                            ParameterMetaData metadata = ps.getParameterMetaData(true);
                            assertEquals(before.getParameterCount(), metadata.getParameterCount());
                            assertEquals(before.getParameterType(1), metadata.getParameterType(1));
                            assertEquals(before.getParameterTypeName(1), metadata.getParameterTypeName(1));
                            assertEquals(before.getParameterClassName(1), metadata.getParameterClassName(1));
                            assertEquals(before.getPrecision(1), metadata.getPrecision(1));
                            assertEquals(before.getScale(1), metadata.getScale(1));
                            assertEquals(before.isNullable(1), metadata.isNullable(1));
                            assertEquals(before.isSigned(1), metadata.isSigned(1));
                            assertEquals(before.getParameterMode(1), metadata.getParameterMode(1));
                        }
                        assertEquals(0, direct.getPreparedStatementHandle());
                    }
                }
            } finally {
                TestUtils.dropTableIfExists(table, setup);
            }
        }
    }

    private static List<List<Object>> resultMetadata(ResultSetMetaData metadata) throws SQLException {
        assertNotNull(metadata);
        List<List<Object>> columns = new ArrayList<>();
        for (int column = 1; column <= metadata.getColumnCount(); column++) {
            columns.add(Arrays.asList(metadata.getColumnType(column), metadata.getColumnTypeName(column),
                    metadata.getColumnClassName(column), metadata.getPrecision(column), metadata.getScale(column),
                    metadata.getColumnDisplaySize(column), metadata.isNullable(column), metadata.getColumnLabel(column)));
        }
        return columns;
    }
}