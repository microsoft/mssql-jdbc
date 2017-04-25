/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.CallableStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.StringUtils;
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
    
    protected static Logger logger = Logger.getLogger("CallableMixedTest"); 
    
    static String tableN = RandomUtil.getIdentifier("TFOO3");
    static String procN = RandomUtil.getIdentifier("SPFOO3");
    static String tableName = AbstractSQLGenerator.escapeIdentifier(tableN);
    static String procName = AbstractSQLGenerator.escapeIdentifier(procN);
    
    static String oldMetadataLookupVal="Y";
    static String osVersionDetails = RandomUtil.getIdentifier("OSVersionDetails");
    static String getOSDetailsProc = RandomUtil.getIdentifier("GetOSDetails"); //"[dbo].[GetOSDetails]";
    
    /**
     * Setting up procedure / table before executing test methods in this class.
     * 
     * @throws SQLException
     *             @{@link SQLException}
     */
    @BeforeAll
    public static void createProc() throws SQLException {
        
        osVersionDetails = AbstractSQLGenerator.escapeIdentifier(osVersionDetails);
        getOSDetailsProc = AbstractSQLGenerator.escapeIdentifier(getOSDetailsProc);
        
        oldMetadataLookupVal = Utils.getConfiguredProperty("com.mssql.metadata.lookup","Y");
        Statement statement = connection.createStatement();

        // Drop table & proc before creating.
        Utils.dropTableIfExists(tableName, statement);
        Utils.dropProcedureIfExists(procName, statement);

        statement.executeUpdate("create table " + tableName + " (c1_int int primary key, col2 int)");
        statement.executeUpdate("Insert into " + tableName + " values(0, 1)");

        statement.executeUpdate("CREATE PROCEDURE " + procName
                + " (@p2_int int, @p2_int_out int OUTPUT, @p4_smallint smallint,  @p4_smallint_out smallint OUTPUT) AS begin transaction SELECT * FROM "
                + tableName + "  ; SELECT @p2_int_out=@p2_int, @p4_smallint_out=@p4_smallint commit transaction RETURN -2147483648");

        // Drop table & proc for testing patch for metadata lookup
        Utils.dropTableIfExists(osVersionDetails, statement);
        Utils.dropProcedureIfExists(getOSDetailsProc, statement);

        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE  ");
        sb.append(osVersionDetails);
        sb.append("(");
        sb.append("[id] [int] IDENTITY(1,1) NOT NULL,");
        sb.append("[CodeName] [nvarchar](50) NOT  NULL,");
        sb.append("[OSVariant] [varchar](50)  NULL,");
        sb.append("[Description] [nvarchar](200)  NULL,");
        sb.append("[Email] [nvarchar](100)  NULL");
        sb.append(")");

        statement.executeUpdate(sb.toString());

        statement.executeUpdate("Insert into " + osVersionDetails
                + " (CodeName,OSVariant,Description,Email) values('Barolo','MacOS', 'Mac OS X 10.7 Lion','Lion@apple.com')");
        statement.executeUpdate(
                "Insert into " + osVersionDetails + " (CodeName,OSVariant,Description,Email) values('Whistler','XP', 'Windows XP','windowsxp@windows.com')");
        statement.executeUpdate("Insert into " + osVersionDetails
                + " (CodeName,OSVariant,Description, Email) values('Xenial Xerus','Linux', 'ubuntu 16','XenialXerus@ubuntu.com')");
        statement.executeUpdate("Insert into " + osVersionDetails
                + " (CodeName,OSVariant, Description, Email) values('Threshold','Win 10', 'Windows 10','Win10family@windows.com')");
        statement.executeUpdate("Insert into " + osVersionDetails
                + "(CodeName,OSVariant, Description, Email) values('Redstone','Win 10', 'Windows 10 (2016 builds)','Win10family@windows.com')");

        
        //Create Procedures
        sb = new StringBuilder();
        sb.append("Create  PROCEDURE ");
        sb.append(getOSDetailsProc);
        sb.append("(");
        sb.append("@variant varchar(50), @osid INT");
        sb.append(")");
        sb.append("AS BEGIN SELECT * FROM ");
        sb.append(osVersionDetails);
        sb.append(" WHERE OSVariant=@variant and id > @osid; END");
//        sb.append(" WHERE OSVariant like \'% @variant %\' and id > @osid; END");
        
        if(logger.isLoggable(Level.FINE)) {
            logger.fine(sb.toString()); 
        }
        
        statement.executeUpdate(sb.toString());
        
        statement.close();
    }
    
    
    /**
     * Clean up method mark as {@link AfterAll} as we need to cleanup after executing all test methods in this class.
     * 
     * @throws SQLException
     *             @{@link SQLException}
     */
    @AfterAll
    public static void terminateVariation() throws SQLException {
        // Using global connection so do not close Global COnnection
        Statement statement = connection.createStatement();
        Utils.dropProcedureIfExists(procName, statement);
        Utils.dropTableIfExists(tableName, statement);
        
        Utils.dropTableIfExists(osVersionDetails, statement);
        Utils.dropProcedureIfExists(getOSDetailsProc, statement);
        statement.close();
        
        //Set old metadata lookup...
        setMetaDataLookupParam(oldMetadataLookupVal);
        
        
    }
    
    
    /**
     * Tests Callable mix
     * @throws SQLException
     */
    @Test
    @DisplayName("Test CallableMix")
    public void datatypesTest() throws SQLException {
        CallableStatement callableStatement = connection.prepareCall("{  ? = CALL " + procName + " (?, ?, ?, ?) }");
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
        callableStatement.close();
        rs.close();
    }
    
    
    /**
     * 
     * @throws SQLException
     */
    @DisplayName("parameterWithoutOrder")
    @ParameterizedTest()
    @ValueSource(strings={"Y","N","YES", "NO", "TRUE", "FALSE", "ON", "OFF"})
    public void testParameterWithoutOrder(String paramValue) throws SQLException {
        try {
            CallableStatement cstmt = connection.prepareCall("{CALL " + getOSDetailsProc + " (?,?) }");

            setMetaDataLookupParam(paramValue);

            // Change the order.
            cstmt.setInt("osid", 0);
            cstmt.setString("variant", "Win 10");

            ResultSet rs = cstmt.executeQuery();

            if (paramValue.startsWith("Y") || "TRUE".equalsIgnoreCase(paramValue) || "ON".equalsIgnoreCase(paramValue)) {
                assertTrue(true, "Using metadata lookup you can pass parameters with any order");
            }
            else {
                // This should not be executed for meta-data lookup with is default behavior 
                assertTrue(false, "Without metadata lookup parameters should pass with appropriate order ");
            }

            // Iterate through the data in the result set and display it.
            while (rs.next()) {
                assertTrue(!StringUtils.isEmpty(rs.getString(1)));
                assertTrue(!StringUtils.isEmpty(rs.getString(2)));
                assertTrue(!StringUtils.isEmpty(rs.getString(3)));
                assertTrue(!StringUtils.isEmpty(rs.getString(4)));
            }
        }
        catch (Exception e) {
            if (paramValue.startsWith("Y")) {
                assertTrue(false, "With Metadata lookup Exception should not be thrown.");
            } else {
                assertTrue(true, "Without Metadata lookup Expecting an Exception.");
            }
        }
    }

    /**
     * Setting flag for metadata lookup for procedure's param name  
     * @param value
     */
    private static void setMetaDataLookupParam(String value) {
        System.setProperty("com.mssql.metadata.lookup", value);
    }
    

}
