package com.microsoft.sqlserver.jdbc.callablestatement;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;

/**
 * Test CallableStatement
 */
@RunWith(JUnitPlatform.class)
public class CallableStatementTest extends AbstractTest {
    private static String tableNameGUID = "uniqueidentifier_Table";
    private static String outputProcedureNameGUID = "uniqueidentifier_SP";

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

        createGUIDTable();
        createGUIDStoredProcedure();
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
     * Cleanup after test
     * 
     * @throws SQLException
     */
    @AfterAll
    public static void cleanup() throws SQLException {
        Utils.dropTableIfExists(tableNameGUID, stmt);
        Utils.dropProcedureIfExists(outputProcedureNameGUID, stmt);
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
}
