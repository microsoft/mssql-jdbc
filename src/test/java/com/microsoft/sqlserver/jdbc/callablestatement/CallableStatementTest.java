package com.microsoft.sqlserver.jdbc.callablestatement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;

/**
 * Test CallableStatement
 */
@RunWith(JUnitPlatform.class)
public class CallableStatementTest extends AbstractTest {
    private static String tableNameGUID = "uniqueidentifier_Table";
    private static String outputProcedureNameGUID = "uniqueidentifier_SP";
    private static String setNullProcedureName = "CallableStatementTest_setNull_SP";
    private static String inputParamsProcedureName = "CallableStatementTest_inputParams_SP";

    private static Connection connection = null;
    private static Statement stmt = null;

    /**
     * Setup before test
     * 
     * @throws SQLException
     */
    @BeforeAll
    public static void setupTest() throws SQLException {
        connection = DriverManager.getConnection(connectionString);
        stmt = connection.createStatement();

        Utils.dropTableIfExists(tableNameGUID, stmt);
        Utils.dropProcedureIfExists(outputProcedureNameGUID, stmt);
        Utils.dropProcedureIfExists(setNullProcedureName, stmt);
        Utils.dropProcedureIfExists(inputParamsProcedureName, stmt);

        createGUIDTable(stmt);
        createGUIDStoredProcedure(stmt);
        createSetNullPreocedure(stmt);
        createInputParamsProcedure(stmt);
    }

    /**
     * Tests CallableStatement.getString() with uniqueidentifier parameter
     * 
     * @throws SQLException
     */
    @Test
    public void getStringGUIDTest() throws SQLException {

        String sql = "{call " + outputProcedureNameGUID + "(?)}";
        
        try (SQLServerCallableStatement callableStatement = (SQLServerCallableStatement) connection.prepareCall(sql)) {

            UUID originalValue = UUID.randomUUID();

            callableStatement.registerOutParameter(1, microsoft.sql.Types.GUID);
            callableStatement.setObject(1, originalValue.toString(), microsoft.sql.Types.GUID);
            callableStatement.execute();

            String retrievedValue = callableStatement.getString(1);

            assertEquals(originalValue.toString().toLowerCase(), retrievedValue.toLowerCase());

        }
    }

    /**
     * test for setNull(index, varchar) to behave as setNull(index, nvarchar) when SendStringParametersAsUnicode is true
     * 
     * @throws SQLException
     */
    @Test
    public void getSetNullWithTypeVarchar() throws SQLException {
        String polishchar = "\u0143";

        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);
        ds.setSendStringParametersAsUnicode(true);
        String sql = "{? = call " + setNullProcedureName + " (?,?)}";
        try (Connection connection = ds.getConnection();
        	 SQLServerCallableStatement cs = (SQLServerCallableStatement) connection.prepareCall(sql);
        	 SQLServerCallableStatement cs2 = (SQLServerCallableStatement) connection.prepareCall(sql)){

            cs.registerOutParameter(1, Types.INTEGER);
            cs.setString(2, polishchar);
            cs.setString(3, null);
            cs.registerOutParameter(3, Types.VARCHAR);
            cs.execute();

            String expected = cs.getString(3);

            cs2.registerOutParameter(1, Types.INTEGER);
            cs2.setString(2, polishchar);
            cs2.setNull(3, Types.VARCHAR);
            cs2.registerOutParameter(3, Types.NVARCHAR);
            cs2.execute();

            String actual = cs2.getString(3);
            
            assertEquals(expected, actual);
        }
    }


    /**
     * recognize parameter names with and without leading '@'
     * 
     * @throws SQLException
     */
    @Test
    public void inputParamsTest() throws SQLException {
        String call = "{CALL " + inputParamsProcedureName + " (?,?)}";
        ResultSet rs = null;
        
        // the historical way: no leading '@', parameter names respected (not positional)
        CallableStatement cs1 = connection.prepareCall(call);
        cs1.setString("p2", "world");
        cs1.setString("p1", "hello");
        rs = cs1.executeQuery();
        rs.next();
        assertEquals("helloworld", rs.getString(1));
        
        // the "new" way: leading '@', parameter names still respected (not positional)
        CallableStatement cs2 = connection.prepareCall(call);
        cs2.setString("@p2", "world!");
        cs2.setString("@p1", "Hello ");
        rs = cs2.executeQuery();
        rs.next();
        assertEquals("Hello world!", rs.getString(1));
        
        // sanity check: unrecognized parameter name
        CallableStatement cs3 = connection.prepareCall(call);
        try {
            cs3.setString("@whatever", "test");
            fail("SQLException should have been thrown");
        } catch (SQLException sse) {
            if (!sse.getMessage().startsWith("Parameter @whatever was not defined")) {
                fail("Unexpected content in exception message");
            }
        }

    }
    
    /**
     * Cleanup after test
     * 
     * @throws SQLException
     */
    @AfterAll
    public static void cleanup() throws SQLException {
        Utils.dropTableIfExists(tableNameGUID, stmt);
        Utils.dropProcedureIfExists(outputProcedureNameGUID, stmt);
        Utils.dropProcedureIfExists(setNullProcedureName, stmt);
        Utils.dropProcedureIfExists(inputParamsProcedureName, stmt);

        if (null != stmt) {
            stmt.close();
        }
        if (null != connection) {
            connection.close();
        }
    }

    private static void createGUIDStoredProcedure(Statement stmt) throws SQLException {
        String sql = "CREATE PROCEDURE " + outputProcedureNameGUID + "(@p1 uniqueidentifier OUTPUT) AS SELECT @p1 = c1 FROM " + tableNameGUID + ";";
        stmt.execute(sql);
    }

    private static void createGUIDTable(Statement stmt) throws SQLException {
        String sql = "CREATE TABLE " + tableNameGUID + " (c1 uniqueidentifier null)";
        stmt.execute(sql);
    }

    private static void createSetNullPreocedure(Statement stmt) throws SQLException {
        stmt.execute("create procedure " + setNullProcedureName + " (@p1 nvarchar(255), @p2 nvarchar(255) output) as select @p2=@p1 return 0");
    }

    private static void createInputParamsProcedure(Statement stmt) throws SQLException {
        String sql = 
                "CREATE PROCEDURE [dbo].[CallableStatementTest_inputParams_SP] " +
                "    @p1 nvarchar(max) = N'parameter1', " +
                "    @p2 nvarchar(max) = N'parameter2' " +
                "AS " +
                "BEGIN " +
                "    SET NOCOUNT ON; " +
                "    SELECT @p1 + @p2 AS result; " +
                "END ";
        stmt.execute(sql);
    }
}
