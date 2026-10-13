/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.StringReader;
import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


public class AdaptiveParameterSizingConnectionTest extends AbstractTest {
    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @ParameterizedTest
    @ValueSource(strings = {"prepare", "prepexec"})
    public void testGrowthReprepareAndStatementCache(String method) throws Exception {
        try (SQLServerConnection connection = openConnection(10, true)) {
            connection.setPrepareMethod(method);
            connection.setEnablePrepareOnFirstPreparedStatementCall(true);
            connection.setDisableStatementPooling(false);
            connection.setStatementPoolingCacheSize(10);
            int initialHandle;
            try (SQLServerPreparedStatement statement = (SQLServerPreparedStatement) connection
                    .prepareStatement("SELECT ?")) {
                int[] lengths = {1, 13, 2, 17, 0};
                int[] expected = {10, 13, 13, 17, 17};
                int previousHandle = 0;
                initialHandle = 0;
                for (int i = 0; i < lengths.length; i++) {
                    String value = repeated(lengths[i]);
                    statement.setString(1, value);
                    try (ResultSet result = statement.executeQuery()) {
                        assertTrue(result.next());
                        assertEquals(value, result.getString(1));
                        assertFalse(result.next());
                    }
                    assertFalse(statement.getMoreResults());
                    assertEquals("nvarchar(" + expected[i] + ")", definition(statement.inOutParam[0]));
                    int handle = statement.getPreparedStatementHandle();
                    assertTrue(handle > 0);
                    if (i == 0) {
                        initialHandle = handle;
                    } else if (expected[i] == expected[i - 1]) {
                        assertEquals(previousHandle, handle);
                    }
                    previousHandle = handle;
                }
            }
            try (SQLServerPreparedStatement fresh = (SQLServerPreparedStatement) connection
                    .prepareStatement("SELECT ?")) {
                fresh.setString(1, "x");
                try (ResultSet result = fresh.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals("x", result.getString(1));
                }
                assertFalse(fresh.getMoreResults());
                assertEquals("nvarchar(10)", definition(fresh.inOutParam[0]));
                assertEquals(initialHandle, fresh.getPreparedStatementHandle());
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"VARCHAR,false", "VARCHAR,true", "NVARCHAR,false", "NVARCHAR,true", "VARBINARY,false",
            "VARBINARY,true"})
    public void testBatchDeclarationsGrowRowByRow(JDBCType type, boolean largeBatch) throws Exception {
        String family = type.toString().toLowerCase(Locale.ENGLISH);
        int longLength = type == JDBCType.NVARCHAR ? 4001 : 8001;
        int[] lengths = {1, 13, 2, 17, 1, longLength, 2};
        String[] expected = {"10", "13", "13", "17", "17", "max", "max"};
        try (SQLServerConnection connection = openConnection(10, false);
                Statement setup = connection.createStatement()) {
            setup.execute("CREATE TABLE #AdaptiveBatch (id int IDENTITY, payload " + family + "(max))");
            try (SQLServerPreparedStatement statement = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO #AdaptiveBatch (payload) VALUES (?)")) {
                for (int length : lengths) {
                    set(statement, type, length);
                    statement.addBatch();
                }
                List<Parameter[]> rows = new ArrayList<>(statement.batchParamValues);
                if (largeBatch) {
                    assertArrayEquals(new long[] {1, 1, 1, 1, 1, 1, 1}, statement.executeLargeBatch());
                } else {
                    assertArrayEquals(new int[] {1, 1, 1, 1, 1, 1, 1}, statement.executeBatch());
                }
                for (int i = 0; i < rows.size(); i++) {
                    assertEquals(family + "(" + expected[i] + ")", definition(rows.get(i)[0]), "row " + i);
                }
                set(statement, type, 1);
                statement.executeUpdate();
                assertEquals(family + "(max)", definition(statement.inOutParam[0]));
            }
            try (ResultSet result = setup.executeQuery("SELECT payload FROM #AdaptiveBatch ORDER BY id")) {
                for (int length : lengths) {
                    assertTrue(result.next());
                    if (type.isBinary()) {
                        assertArrayEquals(new byte[length], result.getBytes(1));
                    } else {
                        assertEquals(repeated(length), result.getString(1));
                    }
                }
                assertTrue(result.next());
                assertFalse(result.next());
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"0,false,8000,8000", "0,true,4000,8000", "1,true,1,1", "5000,false,5000,5000", "5000,true,4000,5000",
            "8001,false,8000,8000", "8001,true,4000,8000"})
    public void testBoundsAndDisabledDefaults(int floor, boolean unicode, int textBound,
            int binaryBound) throws Exception {
        try (SQLServerConnection connection = openConnection(floor, unicode);
                SQLServerPreparedStatement statement = (SQLServerPreparedStatement) connection
                        .prepareStatement("SELECT ?, ?")) {
            statement.setString(1, "x");
            statement.setBytes(2, new byte[] {1});
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals("x", result.getString(1));
                assertArrayEquals(new byte[] {1}, result.getBytes(2));
            }
            assertEquals((unicode ? "nvarchar(" : "varchar(") + textBound + ")", definition(statement.inOutParam[0]));
            assertEquals("varbinary(" + binaryBound + ")", definition(statement.inOutParam[1]));
            assertEquals(floor, connection.getStringParameterDefaultLength());
        }
    }

    @Test
    public void testBatchExplicitHintsBypassDefaultHistory() throws Exception {
        try (SQLServerConnection connection = openConnection(100, true);
                Statement setup = connection.createStatement()) {
            setup.execute("CREATE TABLE #AdaptiveHints (payload nvarchar(max))");
            try (SQLServerPreparedStatement statement = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO #AdaptiveHints VALUES (?)")) {
                statement.setObject(1, "x", Types.NVARCHAR, 5);
                statement.addBatch();
                statement.setObject(1, repeated(213), Types.NVARCHAR, 5);
                statement.addBatch();
                statement.setString(1, "x");
                statement.addBatch();
                List<Parameter[]> rows = new ArrayList<>(statement.batchParamValues);
                assertArrayEquals(new int[] {1, 1, 1}, statement.executeBatch());
                assertEquals("nvarchar(5)", definition(rows.get(0)[0]));
                assertEquals("nvarchar(213)", definition(rows.get(1)[0]));
                assertEquals("nvarchar(100)", definition(rows.get(2)[0]));
                try (ResultSet result = setup.executeQuery("SELECT SUM(LEN(payload)) FROM #AdaptiveHints")) {
                    assertTrue(result.next());
                    assertEquals(215, result.getInt(1));
                }
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testStreamsPreserveDataAndLegacyDeclarations(boolean knownLength) throws Exception {
        byte[] bytes = new byte[31];
        Arrays.fill(bytes, (byte) 7);
        String text = repeated(29);
        try (SQLServerConnection connection = openConnection(10, true);
                SQLServerPreparedStatement statement = (SQLServerPreparedStatement) connection
                        .prepareStatement("SELECT ?, ?")) {
            if (knownLength) {
                statement.setBinaryStream(1, new ByteArrayInputStream(bytes), bytes.length);
                statement.setCharacterStream(2, new StringReader(text), text.length());
            } else {
                statement.setBinaryStream(1, new ByteArrayInputStream(bytes));
                statement.setCharacterStream(2, new StringReader(text));
            }
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertArrayEquals(bytes, result.getBytes(1));
                assertEquals(text, result.getString(2));
            }
            assertEquals("varbinary(max)", definition(statement.inOutParam[0]));
            assertEquals("nvarchar(max)", definition(statement.inOutParam[1]));
            assertEquals(10, statement.inOutParam[0].getLengthHistory().resolveLength(JDBCType.VARBINARY, 10, 0));
            assertEquals(10, statement.inOutParam[1].getLengthHistory().resolveLength(JDBCType.NVARCHAR, 10, 0));
        }
    }

    @ParameterizedTest
    @CsvSource({"false,false", "false,true", "true,false", "true,true"})
    public void testCallableNamedAndOrdinalInputsWithOutput(boolean named, boolean inOut) throws Exception {
        String procedure = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("AdaptiveCallable"));
        try (SQLServerConnection connection = openConnection(10, true);
                Statement setup = connection.createStatement()) {
            try {
                setup.execute("CREATE PROCEDURE " + procedure
                        + " @input nvarchar(max), @output nvarchar(max) OUTPUT AS "
                        + "BEGIN SET @output = REPLICATE(CAST(N'x' AS nvarchar(max)), 200); SELECT @input; END");
                try (SQLServerCallableStatement statement = (SQLServerCallableStatement) connection
                        .prepareCall("{call " + procedure + "(?, ?)}")) {
                    if (named) {
                        statement.registerOutParameter("@output", Types.NVARCHAR);
                    } else {
                        statement.registerOutParameter(2, Types.NVARCHAR);
                    }
                    for (int length : new int[] {1, 31, 2}) {
                        if (named) {
                            statement.setString("@input", repeated(length));
                            if (inOut) {
                                statement.setString("@output", "x");
                            }
                        } else {
                            statement.setString(1, repeated(length));
                            if (inOut) {
                                statement.setString(2, "x");
                            }
                        }
                        try (ResultSet result = statement.executeQuery()) {
                            assertTrue(result.next());
                            assertEquals(repeated(length), result.getString(1));
                        }
                        assertEquals(repeated(200), named ? statement.getString("@output") : statement.getString(2));
                        assertEquals(length == 1 ? "nvarchar(10)" : "nvarchar(31)",
                                definition(statement.inOutParam[0]));
                        assertEquals("nvarchar(4000)", definition(statement.inOutParam[1]));
                    }
                }
            } finally {
                TestUtils.dropProcedureIfExists(procedure, setup);
            }
        }
    }

    @Test
    public void testBulkCopyAndLiteralExecutionPreserveValues() throws Exception {
        try (SQLServerConnection connection = openConnection(10, true);
                Statement setup = connection.createStatement()) {
            setup.execute("CREATE TABLE #AdaptiveSpecial (payload nvarchar(max))");
            connection.setUseBulkCopyForBatchInsert(true);
            try (SQLServerPreparedStatement statement = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO #AdaptiveSpecial (payload) VALUES (?)")) {
                for (int length : new int[] {1, 33, 3}) {
                    statement.setString(1, repeated(length));
                    statement.addBatch();
                }
                assertArrayEquals(new int[] {1, 1, 1}, statement.executeBatch());
                assertEquals(10, statement.inOutParam[0].getLengthHistory().resolveLength(JDBCType.NVARCHAR, 10, 0));
            }
            try (ResultSet result = setup.executeQuery("SELECT SUM(LEN(payload)) FROM #AdaptiveSpecial")) {
                assertTrue(result.next());
                assertEquals(37, result.getInt(1));
            }
            connection.setUseBulkCopyForBatchInsert(false);
            connection.setPrepareMethod("none");
            try (SQLServerPreparedStatement statement = (SQLServerPreparedStatement) connection
                    .prepareStatement("SELECT ?")) {
                statement.setString(1, repeated(47));
                try (ResultSet result = statement.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals(repeated(47), result.getString(1));
                }
                assertEquals(0, statement.getPreparedStatementHandle());
            }
        }
    }

    private static SQLServerConnection openConnection(int floor, boolean unicode) throws Exception {
        SQLServerDataSource source = new SQLServerDataSource();
        source.setURL(connectionString);
        source.setStringParameterDefaultLength(floor);
        source.setSendStringParametersAsUnicode(unicode);
        return (SQLServerConnection) source.getConnection();
    }

    private static void set(SQLServerPreparedStatement statement, JDBCType type, int length) throws Exception {
        if (type.isBinary()) {
            statement.setBytes(1, new byte[length]);
        } else if (type == JDBCType.NVARCHAR) {
            statement.setNString(1, repeated(length));
        } else {
            statement.setString(1, repeated(length));
        }
    }

    private static String definition(Parameter parameter) throws Exception {
        Field field = Parameter.class.getDeclaredField("typeDefinition");
        field.setAccessible(true);
        return (String) field.get(parameter);
    }

    private static String repeated(int length) {
        char[] chars = new char[length];
        Arrays.fill(chars, 'x');
        return new String(chars);
    }
}
