/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.UnitStatement;

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
import com.microsoft.sqlserver.testframework.util.RandomUtil;

@RunWith(JUnitPlatform.class)
public class CallableMixedTest extends AbstractTest {
    Connection connection1 = null;
    Statement Statement151 = null;
    String tableN = RandomUtil.getIdentifier("TFOO3");
    String procN = RandomUtil.getIdentifier("SPFOO3");
    String tableName = AbstractSQLGenerator.escapeIdentifier(tableN);
    String procName = AbstractSQLGenerator.escapeIdentifier(procN);

    /**
     * Tests Callable mix
     * @throws SQLException
     */
    @Test
    @DisplayName("Test CallableMix")
    public void datatypestest() throws SQLException {
        connection1 = DriverManager.getConnection(connectionString);
        Statement151 = connection1.createStatement();

        try {
            Statement151.executeUpdate("DROP TABLE " + tableName);
            Statement151.executeUpdate(" DROP PROCEDURE " + procName);
        }
        catch (Exception e) {
        }

        Statement151.executeUpdate("create table " + tableName + " (c1_int int primary key, col2 int)");
        Statement151.executeUpdate("Insert into " + tableName + " values(0, 1)");
        Statement151.close();
        Statement Statement153 = connection1.createStatement();
        Statement153.executeUpdate("CREATE PROCEDURE " + procName
                + " (@p2_int int, @p2_int_out int OUTPUT, @p4_smallint smallint,  @p4_smallint_out smallint OUTPUT) AS begin transaction SELECT * FROM "
                + tableName + "  ; SELECT @p2_int_out=@p2_int, @p4_smallint_out=@p4_smallint commit transaction RETURN -2147483648");
        Statement153.close();

        CallableStatement CallableStatement1 = connection1.prepareCall("{  ? = CALL " + procName + " (?, ?, ?, ?) }");
        CallableStatement1.registerOutParameter((int) 1, (int) 4);
        CallableStatement1.setObject((int) 2, Integer.valueOf("31"), (int) 4);
        CallableStatement1.registerOutParameter((int) 3, (int) 4);
        CallableStatement1.registerOutParameter((int) 5, java.sql.Types.BINARY); 
        CallableStatement1.registerOutParameter((int) 5, (int) 5);
        CallableStatement1.setObject((int) 4, Short.valueOf("-5372"), (int) 5);

        // get results and a value
        ResultSet ResultSet1 = CallableStatement1.executeQuery();
        ResultSet1.next();

        assertEquals(ResultSet1.getInt(1), 0, "Received data not equal to setdata");
        assertEquals(CallableStatement1.getInt((int) 5), -5372, "Received data not equal to setdata");

        // do nothing and reexecute
        ResultSet1 = CallableStatement1.executeQuery();
        // get the param without getting the resultset
        ResultSet1 = CallableStatement1.executeQuery();
        assertEquals(CallableStatement1.getInt((int) 1), -2147483648, "Received data not equal to setdata");

        ResultSet1 = CallableStatement1.executeQuery();
        ResultSet1.next();

        assertEquals(ResultSet1.getInt(1), 0, "Received data not equal to setdata");
        assertEquals(CallableStatement1.getInt((int) 1), -2147483648, "Received data not equal to setdata");
        assertEquals(CallableStatement1.getInt((int) 5), -5372, "Received data not equal to setdata");
        ResultSet1 = CallableStatement1.executeQuery();
        CallableStatement1.close();
        ResultSet1.close();

        terminateVariation();
    }

    
    private void terminateVariation() throws SQLException {
        Statement151 = connection1.createStatement();
        Statement151.executeUpdate("DROP TABLE " + tableName);
        Statement151.executeUpdate(" DROP PROCEDURE " + procName);
    }

}
