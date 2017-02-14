/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.UnitStatement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;

@RunWith(JUnitPlatform.class)
public class NamedParamMultiPart extends AbstractTest {
    private static final String dataPut = "eminem ";
    private static Connection Connection2 = null;
    private static Driver Driver1 = null;
    private static CallableStatement cs = null;

    @BeforeAll
    public static void beforeAll() throws SQLException {
        Driver1 = DriverManager.getDriver(connectionString);
        Connection2 = DriverManager.getConnection(connectionString);
    }

    @Test
    @DisplayName("Named Param Multi Part Test")
    public void datatypestest() throws Exception {
        Connection Connection1 = DriverManager.getConnection(connectionString);
        Statement Statement1 = Connection1.createStatement();
        Statement1.close();
        Connection1.close();

        DatabaseMetaData DatabaseMetaData1 = Connection2.getMetaData();
        String String1 = DatabaseMetaData1.getDriverVersion();
        // 1.0.505.2
        Connection Connection3 = Driver1.connect(connectionString, null);
        Statement Statement6 = Connection2.createStatement();
        Statement6.executeUpdate(
                "if exists (select * from dbo.sysobjects where id = object_id(N'[dbo].[mystoredproc]') and OBJECTPROPERTY(id, N'IsProcedure') = 1) DROP PROCEDURE [mystoredproc]");
        Statement6.executeUpdate("CREATE PROCEDURE [mystoredproc] (@p_out varchar(255) OUTPUT) AS set @p_out =  '" + dataPut + "'");
        Statement6.close();
    }

    @Test
    public void update1() throws Exception {
        cs = Connection2.prepareCall("{ CALL [mystoredproc] (?) }");
        cs.registerOutParameter("p_out", Types.VARCHAR);
        cs.executeUpdate();
        String data = cs.getString("p_out");
        assertEquals(data, dataPut, "Received data not equal to setdata");
    }

    @Test
    public void update2() throws Exception {
        cs = Connection2.prepareCall("{ CALL [dbo].[mystoredproc] (?) }");
        cs.registerOutParameter("p_out", Types.VARCHAR);
        cs.executeUpdate();
        Object data = cs.getObject("p_out");
        assertEquals(data, dataPut, "Received data not equal to setdata");
    }

    @Test
    public void update3() throws Exception {
        String catalog = Connection2.getCatalog();
        String storedproc = "[" + catalog + "]" + ".[dbo].[mystoredproc]";
        cs = Connection2.prepareCall("{ CALL " + storedproc + " (?) }");
        cs.registerOutParameter("p_out", Types.VARCHAR);
        cs.executeUpdate();
        Object data = cs.getObject("p_out");
        assertEquals(data, dataPut, "Received data not equal to setdata");
    }

    @Test
    public void update4() throws Exception {
        cs = Connection2.prepareCall("{ CALL mystoredproc (?) }");
        cs.registerOutParameter("p_out", Types.VARCHAR);
        cs.executeUpdate();
        Object data = cs.getObject("p_out");
        assertEquals(data, dataPut, "Received data not equal to setdata");
    }

    @Test
    public void update5() throws Exception {
        cs = Connection2.prepareCall("{ CALL dbo.mystoredproc (?) }");
        cs.registerOutParameter("p_out", Types.VARCHAR);
        cs.executeUpdate();
        Object data = cs.getObject("p_out");
        assertEquals(data, dataPut, "Received data not equal to setdata");
    }

    @Test
    public void update6() throws Exception {
        String catalog = Connection2.getCatalog();
        String storedproc = catalog + ".dbo.mystoredproc";
        cs = Connection2.prepareCall("{ CALL " + storedproc + " (?) }");
        cs.registerOutParameter("p_out", Types.VARCHAR);
        cs.executeUpdate();
        Object data = cs.getObject("p_out");
        assertEquals(data, dataPut, "Received data not equal to setdata");
    }

    @AfterAll
    public static void afterAll() {
        try {
            if (null != Connection2) {
                Connection2.close();
            }
            if (null != cs) {
                cs.close();
            }
        }
        catch (SQLException e) {
            fail(e.toString());
        }
    }

}
