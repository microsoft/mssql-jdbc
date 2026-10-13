/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


public class ParameterLengthHistoryConnectionTest extends AbstractTest {
    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @ParameterizedTest
    @ValueSource(ints = {-1, 0, 10, 5000})
    public void testHistoryInitializationAndStatementIsolation(int floor) throws Exception {
        try (SQLServerConnection connection = openConnection(floor);
                SQLServerPreparedStatement first = (SQLServerPreparedStatement) connection.prepareStatement("SELECT ?");
                SQLServerPreparedStatement second = (SQLServerPreparedStatement) connection
                        .prepareStatement("SELECT ?");
                SQLServerCallableStatement callable = (SQLServerCallableStatement) connection
                        .prepareCall("{call sys.sp_executesql(?)}")) {
            for (SQLServerPreparedStatement statement : new SQLServerPreparedStatement[] {first, second, callable}) {
                Parameter parameter = statement.inOutParam[0];
                if (floor > 0) {
                    assertNotNull(parameter.getLengthHistory());
                } else {
                    assertNull(parameter.getLengthHistory());
                }
                statement.setString(1, "x");
                statement.addBatch();
                assertSame(parameter.getLengthHistory(), statement.batchParamValues.get(0)[0].getLengthHistory());
            }
            if (floor > 0) {
                assertNotSame(first.inOutParam[0].getLengthHistory(), second.inOutParam[0].getLengthHistory());
                assertNotSame(first.inOutParam[0].getLengthHistory(), callable.inOutParam[0].getLengthHistory());
            }
        }
    }

    @Test
    public void testSnapshotAndClearLifecycleWithAdaptiveDeclarations() throws Exception {
        try (SQLServerConnection connection = openConnection(10); Statement setup = connection.createStatement()) {
            setup.execute("CREATE TABLE #ParameterLengthHistory (firstValue nvarchar(max), secondValue nvarchar(max))");
            try (SQLServerPreparedStatement statement = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO #ParameterLengthHistory VALUES (?, ?)")) {
                Parameter.LengthHistory firstHistory = statement.inOutParam[0].getLengthHistory();
                Parameter.LengthHistory secondHistory = statement.inOutParam[1].getLengthHistory();
                assertNotSame(firstHistory, secondHistory);
                statement.setString(1, repeated(1000));
                statement.setString(1, "x");
                statement.setString(2, "y");
                statement.addBatch();
                Parameter[] firstRow = statement.batchParamValues.get(0);
                statement.setObject(1, "z", Types.NVARCHAR, 40);
                statement.addBatch();
                Parameter[] secondRow = statement.batchParamValues.get(1);
                assertSame(firstHistory, firstRow[0].getLengthHistory());
                assertSame(firstHistory, secondRow[0].getLengthHistory());
                assertNotSame(firstRow[0].getInputDTV(), secondRow[0].getInputDTV());
                assertEquals("x", firstRow[0].getInputDTV().getSetterValue());
                assertEquals("z", secondRow[0].getInputDTV().getSetterValue());
                assertNull(firstRow[0].getInputDTV().getScale());
                assertEquals(Integer.valueOf(40), secondRow[0].getInputDTV().getScale());
                assertEquals(10, firstHistory.resolveLength(JDBCType.NVARCHAR, 10, 0));
                assertEquals("nvarchar(10)", firstRow[0].getTypeDefinition(connection, null));
                assertEquals("nvarchar(40)", secondRow[0].getTypeDefinition(connection, null));

                statement.setString(1, repeated(113));
                assertEquals("nvarchar(113)", statement.inOutParam[0].getTypeDefinition(connection, null));
                statement.clearParameters();
                assertNull(statement.inOutParam[0].getInputDTV());
                assertSame(firstHistory, statement.inOutParam[0].getLengthHistory());
                assertEquals(113, secondRow[0].getLengthHistory().resolveLength(JDBCType.NVARCHAR, 10, 0));
                statement.clearBatch();
                assertNull(statement.batchParamValues);
                assertSame(firstHistory, statement.inOutParam[0].getLengthHistory());
                assertEquals(113, firstHistory.resolveLength(JDBCType.NVARCHAR, 10, 0));
                assertEquals(10, secondHistory.resolveLength(JDBCType.NVARCHAR, 10, 0));

                statement.setString(1, repeated(2000));
                statement.setString(2, "y");
                statement.addBatch();
                statement.clearBatch();
                assertEquals(113, firstHistory.resolveLength(JDBCType.NVARCHAR, 10, 0));
                statement.setString(1, "x");
                statement.addBatch();
                statement.executeBatch();
                assertEquals("nvarchar(113)", statement.inOutParam[0].getTypeDefinition(connection, null));
                assertEquals(113, firstHistory.resolveLength(JDBCType.NVARCHAR, 10, 0));
                statement.defineParameterType(1, Types.NVARCHAR, 16);
                statement.setString(1, "x");
                statement.executeUpdate();
                assertEquals("nvarchar(16)", statement.inOutParam[0].getTypeDefinition(connection, null));
                assertEquals(113, firstHistory.resolveLength(JDBCType.NVARCHAR, 10, 0));
            }
            try (SQLServerPreparedStatement fresh = (SQLServerPreparedStatement) connection
                    .prepareStatement("INSERT INTO #ParameterLengthHistory VALUES (?, ?)")) {
                assertEquals(10, fresh.inOutParam[0].getLengthHistory().resolveLength(JDBCType.NVARCHAR, 10, 0));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testCallableOutputRemainsUnchanged(boolean inOut) throws Exception {
        String procedure = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("LengthHistoryOutput"));
        try (SQLServerConnection connection = openConnection(10); Statement setup = connection.createStatement()) {
            try {
                setup.execute(
                        "CREATE PROCEDURE " + procedure + " @input nvarchar(100), @output nvarchar(max) OUTPUT AS "
                                + "SET @output = REPLICATE(CAST(N'x' AS nvarchar(max)), 200)");
                try (SQLServerCallableStatement statement = (SQLServerCallableStatement) connection
                        .prepareCall("{call " + procedure + "(?, ?)}")) {
                    Parameter.LengthHistory inputHistory = statement.inOutParam[0].getLengthHistory();
                    Parameter.LengthHistory outputHistory = statement.inOutParam[1].getLengthHistory();
                    assertNotNull(inputHistory);
                    assertNotNull(outputHistory);
                    assertNotSame(inputHistory, outputHistory);
                    statement.setString("@input", "x");
                    statement.registerOutParameter("@output", Types.NVARCHAR);
                    if (inOut) {
                        statement.setString("@output", "x");
                    }
                    statement.execute();
                    assertEquals("nvarchar(10)", statement.inOutParam[0].getTypeDefinition(connection, null));
                    assertEquals(repeated(200), statement.getString("@output"));
                    assertEquals("nvarchar(4000)", statement.inOutParam[1].getTypeDefinition(connection, null));
                    assertEquals(10, outputHistory.resolveLength(JDBCType.NVARCHAR, 10, 0));
                    statement.clearParameters();
                    assertSame(outputHistory, statement.inOutParam[1].getLengthHistory());
                }
            } finally {
                TestUtils.dropProcedureIfExists(procedure, setup);
            }
        }
    }

    private static SQLServerConnection openConnection(int floor) throws Exception {
        SQLServerDataSource source = new SQLServerDataSource();
        source.setURL(connectionString);
        source.setStringParameterDefaultLength(floor);
        return (SQLServerConnection) source.getConnection();
    }

    private static String repeated(int length) {
        char[] value = new char[length];
        Arrays.fill(value, 'x');
        return new String(value);
    }
}
