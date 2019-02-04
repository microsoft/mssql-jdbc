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
import java.text.MessageFormat;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;


/**
 * Test CallableStatement
 */
@RunWith(JUnitPlatform.class)
public class CallableStatementTest extends AbstractTest {
    private static String tableNameGUID = RandomUtil.getIdentifier("uniqueidentifier_Table");
    private static String outputProcedureNameGUID = RandomUtil.getIdentifier("uniqueidentifier_SP");
    private static String setNullProcedureName = RandomUtil.getIdentifier("CallableStatementTest_setNull_SP");
    private static String inputParamsProcedureName = RandomUtil.getIdentifier("CallableStatementTest_inputParams_SP");

    /**
     * Setup before test
     * 
     * @throws SQLException
     */
    @BeforeAll
    public static void setupTest() throws SQLException {

        try (Connection connection = DriverManager.getConnection(connectionString);
                Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableNameGUID), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(outputProcedureNameGUID), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(setNullProcedureName), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(inputParamsProcedureName), stmt);

            createGUIDTable(stmt);
            createGUIDStoredProcedure(stmt);
            createSetNullPreocedure(stmt);
            createInputParamsProcedure(stmt);
        }
    }

    /**
     * Tests CallableStatement.getString() with uniqueidentifier parameter
     * 
     * @throws SQLException
     */
    @Test
    public void getStringGUIDTest() throws SQLException {

        String sql = "{call " + AbstractSQLGenerator.escapeIdentifier(outputProcedureNameGUID) + "(?)}";

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
        String sql = "{? = call " + AbstractSQLGenerator.escapeIdentifier(setNullProcedureName) + " (?,?)}";
        try (Connection connection = ds.getConnection();
                SQLServerCallableStatement cs = (SQLServerCallableStatement) connection.prepareCall(sql);
                SQLServerCallableStatement cs2 = (SQLServerCallableStatement) connection.prepareCall(sql)) {

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
        String call = "{CALL " + AbstractSQLGenerator.escapeIdentifier(inputParamsProcedureName) + " (?,?)}";

        // the historical way: no leading '@', parameter names respected (not positional)
        try (CallableStatement cs = connection.prepareCall(call)) {
            cs.setString("p2", "world");
            cs.setString("p1", "hello");
            try (ResultSet rs = cs.executeQuery()) {
                rs.next();
                assertEquals("helloworld", rs.getString(1));
            }
        }

        // the "new" way: leading '@', parameter names still respected (not positional)
        try (CallableStatement cs = connection.prepareCall(call)) {
            cs.setString("@p2", "world!");
            cs.setString("@p1", "Hello ");
            try (ResultSet rs = cs.executeQuery()) {
                rs.next();
                assertEquals("Hello world!", rs.getString(1));
            }
        }

        // sanity check: unrecognized parameter name
        try (CallableStatement cs = connection.prepareCall(call)) {
            cs.setString("@whatever", "test");
            fail(TestResource.getResource("R_shouldThrowException"));
        } catch (SQLException sse) {

            MessageFormat form = new MessageFormat(TestResource.getResource("R_parameterNotDefined"));
            Object[] msgArgs = {"@whatever"};

            if (!sse.getMessage().startsWith(form.format(msgArgs))) {
                fail(TestResource.getResource("R_unexpectedExceptionContent"));
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
        try (Connection connection = DriverManager.getConnection(connectionString);
                Statement stmt = connection.createStatement()) {

            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(tableNameGUID), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(outputProcedureNameGUID), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(setNullProcedureName), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(inputParamsProcedureName), stmt);
        }
    }

    private static void createGUIDStoredProcedure(Statement stmt) throws SQLException {
        String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(outputProcedureNameGUID)
                + "(@p1 uniqueidentifier OUTPUT) AS SELECT @p1 = c1 FROM "
                + AbstractSQLGenerator.escapeIdentifier(tableNameGUID) + ";";
        stmt.execute(sql);
    }

    private static void createGUIDTable(Statement stmt) throws SQLException {
        String sql = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableNameGUID)
                + " (c1 uniqueidentifier null)";
        stmt.execute(sql);
    }

    private static void createSetNullPreocedure(Statement stmt) throws SQLException {
        stmt.execute("create procedure " + AbstractSQLGenerator.escapeIdentifier(setNullProcedureName)
                + " (@p1 nvarchar(255), @p2 nvarchar(255) output) as select @p2=@p1 return 0");
    }

    private static void createInputParamsProcedure(Statement stmt) throws SQLException {
        String sql = "CREATE PROCEDURE " + AbstractSQLGenerator.escapeIdentifier(inputParamsProcedureName)
                + "    @p1 nvarchar(max) = N'parameter1', " + "    @p2 nvarchar(max) = N'parameter2' " + "AS "
                + "BEGIN " + "    SET NOCOUNT ON; " + "    SELECT @p1 + @p2 AS result; " + "END ";

        stmt.execute(sql);
    }
}
