package com.microsoft.sqlserver.jdbc.callablestatement;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
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

        createGUIDTable();
        createGUIDStoredProcedure();
        createSetNullPreocedure();
    }

    /**
     * Tests CallableStatement.getString() with uniqueidentifier parameter
     * 
     * @throws SQLException
     */
    @Test
    public void getStringGUIDTest() throws SQLException {

        SQLServerCallableStatement callableStatement = null;
        try {
            String sql = "{call " + outputProcedureNameGUID + "(?)}";

            callableStatement = (SQLServerCallableStatement) connection.prepareCall(sql);

            UUID originalValue = UUID.randomUUID();

            callableStatement.registerOutParameter(1, microsoft.sql.Types.GUID);
            callableStatement.setObject(1, originalValue.toString(), microsoft.sql.Types.GUID);

            callableStatement.execute();

            String retrievedValue = callableStatement.getString(1);

            assertEquals(originalValue.toString().toLowerCase(), retrievedValue.toLowerCase());

        }
        finally {
            if (null != callableStatement) {
                callableStatement.close();
            }
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

        SQLServerCallableStatement cs = null;
        SQLServerCallableStatement cs2 = null;
        try {
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setURL(connectionString);
            ds.setSendStringParametersAsUnicode(true);
            connection = ds.getConnection();

            String sql = "{? = call " + setNullProcedureName + " (?,?)}";

            cs = (SQLServerCallableStatement) connection.prepareCall(sql);
            cs.registerOutParameter(1, Types.INTEGER);
            cs.setString(2, polishchar);
            cs.setString(3, null);
            cs.registerOutParameter(3, Types.VARCHAR);
            cs.execute();

            String expected = cs.getString(3);

            cs2 = (SQLServerCallableStatement) connection.prepareCall(sql);
            cs2.registerOutParameter(1, Types.INTEGER);
            cs2.setString(2, polishchar);
            cs2.setNull(3, Types.VARCHAR);
            cs2.registerOutParameter(3, Types.NVARCHAR);
            cs2.execute();

            String actual = cs2.getString(3);

            assertEquals(expected, actual);
        }
        finally {
            if (null != cs) {
                cs.close();
            }
            if (null != cs2) {
                cs2.close();
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

        if (null != stmt) {
            stmt.close();
        }
        if (null != connection) {
            connection.close();
        }
    }

    private static void createGUIDStoredProcedure() throws SQLException {
        String sql = "CREATE PROCEDURE " + outputProcedureNameGUID + "(@p1 uniqueidentifier OUTPUT) AS SELECT @p1 = c1 FROM " + tableNameGUID + ";";
        stmt.execute(sql);
    }

    private static void createGUIDTable() throws SQLException {
        String sql = "CREATE TABLE " + tableNameGUID + " (c1 uniqueidentifier null)";
        stmt.execute(sql);
    }

    private static void createSetNullPreocedure() throws SQLException {
        stmt.execute("create procedure " + setNullProcedureName + " (@p1 nvarchar(255), @p2 nvarchar(255) output) as select @p2=@p1 return 0");
    }
}
