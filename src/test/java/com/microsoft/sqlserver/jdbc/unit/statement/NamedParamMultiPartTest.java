/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

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

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


/**
 * Multipart parameters
 *
 */
@RunWith(JUnitPlatform.class)
public class NamedParamMultiPartTest extends AbstractTest {
    private static final String dataPut = RandomUtil.getIdentifier("dataPut");
    static String procedureName;

    /**
     * setup
     * 
     * @throws SQLException
     */
    @BeforeAll
    public static void beforeAll() throws SQLException {
        procedureName = RandomUtil.getIdentifier("mystoredproc");

        try (Connection connection = DriverManager.getConnection(connectionString);
                Statement statement = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(procedureName), statement);
            statement.executeUpdate("CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(procedureName)
                    + " (@p_out varchar(255) OUTPUT) AS set @p_out =  '" + TestUtils.escapeSingleQuotes(dataPut) + "'");
        }
    }

    /**
     * Stored procedure call
     * 
     * @throws Exception
     */
    @Test
    public void update1() throws Exception {
        try (Connection connection = DriverManager.getConnection(connectionString); CallableStatement cs = connection
                .prepareCall("{ CALL " + AbstractSQLGenerator.escapeIdentifier(procedureName) + " (?) }")) {
            cs.registerOutParameter("p_out", Types.VARCHAR);
            cs.executeUpdate();
            String data = cs.getString("p_out");
            assertEquals(data, dataPut, TestResource.getResource("R_setDataNotEqual"));
        }
    }

    /**
     * Stored procedure call
     * 
     * @throws Exception
     */
    @Test
    public void update2() throws Exception {
        try (Connection connection = DriverManager.getConnection(connectionString); CallableStatement cs = connection
                .prepareCall("{ CALL " + AbstractSQLGenerator.escapeIdentifier(procedureName) + " (?) }")) {
            cs.registerOutParameter("p_out", Types.VARCHAR);
            cs.executeUpdate();
            Object data = cs.getObject("p_out");
            assertEquals(data, dataPut, TestResource.getResource("R_setDataNotEqual"));
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
        String storedproc = "[" + catalog + "]" + ".[dbo]." + AbstractSQLGenerator.escapeIdentifier(procedureName);
        try (Connection connection = DriverManager.getConnection(connectionString);
                CallableStatement cs = connection.prepareCall("{ CALL " + storedproc + " (?) }")) {
            cs.registerOutParameter("p_out", Types.VARCHAR);
            cs.executeUpdate();
            Object data = cs.getObject("p_out");
            assertEquals(data, dataPut, TestResource.getResource("R_setDataNotEqual"));
        }
    }

    /**
     * Stored procedure call
     * 
     * @throws Exception
     */
    @Test
    public void update4() throws Exception {
        try (Connection connection = DriverManager.getConnection(connectionString); CallableStatement cs = connection
                .prepareCall("{ CALL " + AbstractSQLGenerator.escapeIdentifier(procedureName) + " (?) }")) {
            cs.registerOutParameter("p_out", Types.VARCHAR);
            cs.executeUpdate();
            Object data = cs.getObject("p_out");
            assertEquals(data, dataPut, TestResource.getResource("R_setDataNotEqual"));
        }
    }

    /**
     * Stored procedure call
     * 
     * @throws Exception
     */
    @Test
    public void update5() throws Exception {
        try (Connection connection = DriverManager.getConnection(connectionString); CallableStatement cs = connection
                .prepareCall("{ CALL " + AbstractSQLGenerator.escapeIdentifier(procedureName) + " (?) }")) {
            cs.registerOutParameter("p_out", Types.VARCHAR);
            cs.executeUpdate();
            Object data = cs.getObject("p_out");
            assertEquals(data, dataPut, TestResource.getResource("R_setDataNotEqual"));
        }
    }

    /**
     * 
     * @throws Exception
     */
    @Test
    public void update6() throws Exception {
        String catalog = connection.getCatalog();
        String storedproc = catalog + ".dbo." + AbstractSQLGenerator.escapeIdentifier(procedureName);
        try (Connection connection = DriverManager.getConnection(connectionString);
                CallableStatement cs = connection.prepareCall("{ CALL " + storedproc + " (?) }")) {
            cs.registerOutParameter("p_out", Types.VARCHAR);
            cs.executeUpdate();
            Object data = cs.getObject("p_out");
            assertEquals(data, dataPut, TestResource.getResource("R_setDataNotEqual"));
        }
    }

    /**
     * Clean up
     * 
     * @throws SQLException
     */
    @AfterAll
    public static void afterAll() throws SQLException {
        try (Connection connection = DriverManager.getConnection(connectionString);
                Statement stmt = connection.createStatement()) {
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(procedureName), stmt);
        }
    }
}
