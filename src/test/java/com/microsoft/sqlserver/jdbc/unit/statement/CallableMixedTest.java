/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


/**
 * Callable Mix tests using stored procedure with input and output
 *
 */
@RunWith(JUnitPlatform.class)
public class CallableMixedTest extends AbstractTest {
    String tableName = RandomUtil.getIdentifier("TFOO3");
    String procName = RandomUtil.getIdentifier("SPFOO3");

    /**
     * Tests Callable mix
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("Test CallableMix")
    public void datatypesTest() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString);
                Statement statement = connection.createStatement();) {

            statement.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + " (c1_int int primary key, col2 int)");
            statement
                    .executeUpdate("Insert into " + AbstractSQLGenerator.escapeIdentifier(tableName) + " values(0, 1)");

            statement.executeUpdate("CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procName)
                    + " (@p2_int int, @p2_int_out int OUTPUT, @p4_smallint smallint,  @p4_smallint_out smallint OUTPUT) AS begin transaction SELECT * FROM "
                    + AbstractSQLGenerator.escapeIdentifier(tableName)
                    + "  ; SELECT @p2_int_out=@p2_int, @p4_smallint_out=@p4_smallint commit transaction RETURN -2147483648");

            try (CallableStatement callableStatement = connection.prepareCall(
                    "{  ? = CALL " + AbstractSQLGenerator.escapeIdentifier(procName) + " (?, ?, ?, ?) }")) {
                callableStatement.registerOutParameter((int) 1, (int) 4);
                callableStatement.setObject((int) 2, Integer.valueOf("31"), (int) 4);
                callableStatement.registerOutParameter((int) 3, (int) 4);
                callableStatement.registerOutParameter((int) 5, java.sql.Types.BINARY);
                callableStatement.registerOutParameter((int) 5, (int) 5);
                callableStatement.setObject((int) 4, Short.valueOf("-5372"), (int) 5);

                // get results and a value
                try (ResultSet rs = callableStatement.executeQuery()) {
                    rs.next();

                    assertEquals(rs.getInt(1), 0, TestResource.getResource("R_setDataNotEqual"));
                    assertEquals(callableStatement.getInt((int) 5), -5372,
                            TestResource.getResource("R_setDataNotEqual"));
                }

                // do nothing and re-execute
                try (ResultSet rs = callableStatement.executeQuery()) {}

                // get the param without getting the resultset
                try (ResultSet rs = callableStatement.executeQuery()) {
                    assertEquals(callableStatement.getInt((int) 1), -2147483648,
                            TestResource.getResource("R_setDataNotEqual"));
                }

                try (ResultSet rs = callableStatement.executeQuery()) {
                    rs.next();

                    assertEquals(rs.getInt(1), 0, TestResource.getResource("R_setDataNotEqual"));
                    assertEquals(callableStatement.getInt((int) 1), -2147483648,
                            TestResource.getResource("R_setDataNotEqual"));
                    assertEquals(callableStatement.getInt((int) 5), -5372,
                            TestResource.getResource("R_setDataNotEqual"));
                }

                try (ResultSet rs = callableStatement.executeQuery()) {}
            }
            terminateVariation(statement);
        }
    }

    /**
     * Cleanups
     * 
     * @throws SQLException
     */
    private void terminateVariation(Statement statement) throws SQLException {
        TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableName), statement);
        TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(procName), statement);
    }
}
