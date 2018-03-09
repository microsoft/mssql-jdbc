/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;

/**
 * Multipart parameters
 *
 */
@RunWith(JUnitPlatform.class)
public class NamedParamMultiPartTest extends AbstractTest {
    private static final String dataPut = "eminem";
    private static Connection connection = null;
    String procedureName = "mystoredproc";

    /**
     * setup
     * 
     * @throws SQLException
     */
    @BeforeAll
    public static void beforeAll() throws SQLException {
        connection = DriverManager.getConnection(connectionString);
        try (Statement statement = connection.createStatement()) {
            Utils.dropProcedureIfExists("mystoredproc", statement);
            statement.executeUpdate("CREATE PROCEDURE [mystoredproc] (@p_out varchar(255) OUTPUT) AS set @p_out =  '" + dataPut + "'");
        }
    }

    /**
     * Stored procedure call
     * 
     * @throws Exception
     */
    @Test
    public void update1() throws Exception {
        try (CallableStatement cs = connection.prepareCall("{ CALL " + procedureName + " (?) }")) {
            cs.registerOutParameter("p_out", Types.VARCHAR);
            cs.executeUpdate();
            String data = cs.getString("p_out");
            assertEquals(data, dataPut, "Received data not equal to setdata");
        }
    }

    /**
     * Stored procedure call
     * 
     * @throws Exception
     */
    @Test
    public void update2() throws Exception {
        try (CallableStatement cs = connection.prepareCall("{ CALL " + procedureName + " (?) }")) {
            cs.registerOutParameter("p_out", Types.VARCHAR);
            cs.executeUpdate();
            Object data = cs.getObject("p_out");
            assertEquals(data, dataPut, "Received data not equal to setdata");
        }
    }

    /**
     * Stored procedure call
     * 
     * @throws Exception
     */
    @Test
    public void update3() throws Exception {
        String catalog = connection.getCatalog();
        String storedproc = "[" + catalog + "]" + ".[dbo].[mystoredproc]";
        try (CallableStatement cs = connection.prepareCall("{ CALL " + storedproc + " (?) }")) {
            cs.registerOutParameter("p_out", Types.VARCHAR);
            cs.executeUpdate();
            Object data = cs.getObject("p_out");
            assertEquals(data, dataPut, "Received data not equal to setdata");
        }
    }

    /**
     * Stored procedure call
     * 
     * @throws Exception
     */
    @Test
    public void update4() throws Exception {
        try (CallableStatement cs = connection.prepareCall("{ CALL " + procedureName + " (?) }")) {
            cs.registerOutParameter("p_out", Types.VARCHAR);
            cs.executeUpdate();
            Object data = cs.getObject("p_out");
            assertEquals(data, dataPut, "Received data not equal to setdata");
        }
    }

    /**
     * Stored procedure call
     * 
     * @throws Exception
     */
    @Test
    public void update5() throws Exception {
        try (CallableStatement cs = connection.prepareCall("{ CALL " + procedureName + " (?) }")) {
            cs.registerOutParameter("p_out", Types.VARCHAR);
            cs.executeUpdate();
            Object data = cs.getObject("p_out");
            assertEquals(data, dataPut, "Received data not equal to setdata");
        }
    }

    /**
     * 
     * @throws Exception
     */
    @Test
    public void update6() throws Exception {
        String catalog = connection.getCatalog();
        String storedproc = catalog + ".dbo." + procedureName;
        try (CallableStatement cs = connection.prepareCall("{ CALL " + storedproc + " (?) }")) {
            cs.registerOutParameter("p_out", Types.VARCHAR);
            cs.executeUpdate();
            Object data = cs.getObject("p_out");
            assertEquals(data, dataPut, "Received data not equal to setdata");
        }
    }

    /**
     * Clean up
     * 
     * @throws SQLException
     */
    @AfterAll
    public static void afterAll() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            Utils.dropProcedureIfExists("mystoredproc", stmt);
        }
        finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

}
