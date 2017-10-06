/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

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
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;
import com.microsoft.sqlserver.testframework.util.RandomUtil;

/**
 * Callable Mix tests using stored procedure with input and output
 *
 */
@RunWith(JUnitPlatform.class)
public class CallableMixedTest extends AbstractTest {
    Connection connection = null;
    String tableN = RandomUtil.getIdentifier("TFOO3");
    String procN = RandomUtil.getIdentifier("SPFOO3");
    String tableName = AbstractSQLGenerator.escapeIdentifier(tableN);
    String procName = AbstractSQLGenerator.escapeIdentifier(procN);

    /**
     * Tests Callable mix
     * 
     * @throws SQLException
     */
    @Test
    @DisplayName("Test CallableMix")
    public void datatypesTest() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString); Statement statement = connection.createStatement();) {

            statement.executeUpdate("create table " + tableName + " (c1_int int primary key, col2 int)");
            statement.executeUpdate("Insert into " + tableName + " values(0, 1)");

            statement.executeUpdate("CREATE PROCEDURE " + procName
                    + " (@p2_int int, @p2_int_out int OUTPUT, @p4_smallint smallint,  @p4_smallint_out smallint OUTPUT) AS begin transaction SELECT * FROM "
                    + tableName + "  ; SELECT @p2_int_out=@p2_int, @p4_smallint_out=@p4_smallint commit transaction RETURN -2147483648");

            try (CallableStatement callableStatement = connection.prepareCall("{  ? = CALL " + procName + " (?, ?, ?, ?) }")) {
                callableStatement.registerOutParameter((int) 1, (int) 4);
                callableStatement.setObject((int) 2, Integer.valueOf("31"), (int) 4);
                callableStatement.registerOutParameter((int) 3, (int) 4);
                callableStatement.registerOutParameter((int) 5, java.sql.Types.BINARY);
                callableStatement.registerOutParameter((int) 5, (int) 5);
                callableStatement.setObject((int) 4, Short.valueOf("-5372"), (int) 5);

                // get results and a value
                ResultSet rs = callableStatement.executeQuery();
                rs.next();

                assertEquals(rs.getInt(1), 0, "Received data not equal to setdata");
                assertEquals(callableStatement.getInt((int) 5), -5372, "Received data not equal to setdata");

                // do nothing and reexecute
                rs = callableStatement.executeQuery();
                // get the param without getting the resultset
                rs = callableStatement.executeQuery();
                assertEquals(callableStatement.getInt((int) 1), -2147483648, "Received data not equal to setdata");

                rs = callableStatement.executeQuery();
                rs.next();

                assertEquals(rs.getInt(1), 0, "Received data not equal to setdata");
                assertEquals(callableStatement.getInt((int) 1), -2147483648, "Received data not equal to setdata");
                assertEquals(callableStatement.getInt((int) 5), -5372, "Received data not equal to setdata");
                rs = callableStatement.executeQuery();
                rs.close();
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
        Utils.dropTableIfExists(tableName, statement);
        Utils.dropProcedureIfExists(procName, statement);
    }
}
