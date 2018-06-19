/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.databasemetadata;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.text.MessageFormat;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDatabaseMetaData;
import com.microsoft.sqlserver.jdbc.StringUtils;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;

/**
 * Test class for testing DatabaseMetaData.
 */
@RunWith(JUnitPlatform.class)
public class DatabaseMetaDataTest extends AbstractTest {

    /**
     * Verify DatabaseMetaData#isWrapperFor and DatabaseMetaData#unwrap.
     * 
     * @throws SQLException
     */
    @Test
    public void testDatabaseMetaDataWrapper() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString)) {
            DatabaseMetaData databaseMetaData = con.getMetaData();
            assertTrue(databaseMetaData.isWrapperFor(DatabaseMetaData.class));
            assertSame(databaseMetaData, databaseMetaData.unwrap(DatabaseMetaData.class));
        }
    }

    /**
     * Testing if driver version is matching with manifest file or not. Will be useful while releasing preview / RTW release.
     * 
     * //TODO: OSGI: Test for capability 1.7 for JDK 1.7 and 1.8 for 1.8 //Require-Capability: osgi.ee;filter:="(&(osgi.ee=JavaSE)(version=1.8))" //String
     * capability = attributes.getValue("Require-Capability");  
     * 
     * @throws SQLException
     *             SQL Exception
     * @throws IOException
     *             IOExcption
     */
    @Test
    public void testDriverVersion() throws SQLException, IOException {
        String manifestFile = Utils.getCurrentClassPath() + "META-INF/MANIFEST.MF";
        manifestFile = manifestFile.replace("test-classes", "classes");

        File f = new File(manifestFile);

        assumeTrue(f.exists(), TestResource.getResource("R_manifestNotFound"));

        InputStream in = new BufferedInputStream(new FileInputStream(f));
        Manifest manifest = new Manifest(in);
        Attributes attributes = manifest.getMainAttributes();
        String buildVersion = attributes.getValue("Bundle-Version");

        DatabaseMetaData dbmData = connection.getMetaData();

        String driverVersion = dbmData.getDriverVersion();

        boolean isSnapshot = buildVersion.contains("SNAPSHOT");

        // Removing all dots & chars easy for comparing.
        driverVersion = driverVersion.replaceAll("[^0-9]", "");
        buildVersion = buildVersion.replaceAll("[^0-9]", "");

        // Not comparing last build number. We will compare only major.minor.patch
        driverVersion = driverVersion.substring(0, 3);
        buildVersion = buildVersion.substring(0, 3);

        int intBuildVersion = Integer.valueOf(buildVersion);
        int intDriverVersion = Integer.valueOf(driverVersion);

        if (isSnapshot) {
            assertTrue(intDriverVersion < intBuildVersion,
                    TestResource.getResource("R_buildVersionError"));
        }
        else {
            assertTrue(intDriverVersion == intBuildVersion,
                    TestResource.getResource("R_buildVersionError"));
        }

    }

    /**
     * Your password should not be in getURL method.
     * 
     * @throws SQLException
     */
    @Test
    public void testGetURL() throws SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        String url = databaseMetaData.getURL();
        url = url.toLowerCase();
        assertFalse(url.contains("password"), TestResource.getResource("R_getURLContainsPwd"));
    }

    /**
     * Test getUsername.
     * 
     * @throws SQLException
     */
    @Test
    public void testDBUserLogin() throws SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();

        String connectionString = getConfiguredProperty("mssql_jdbc_test_connection_properties");

        connectionString = connectionString.toLowerCase();

        int startIndex = 0;
        int endIndex = 0;

        if (connectionString.contains("username")) {
            startIndex = connectionString.indexOf("username=");
            endIndex = connectionString.indexOf(";", startIndex);
            startIndex = startIndex + "username=".length();
        }
        else if (connectionString.contains("user")) {
            startIndex = connectionString.indexOf("user=");
            endIndex = connectionString.indexOf(";", startIndex);
            startIndex = startIndex + "user=".length();
        }

        String userFromConnectionString = connectionString.substring(startIndex, endIndex);
        String userName = databaseMetaData.getUserName();

        assertNotNull(userName, TestResource.getResource("R_userNameNull"));

        assertTrue(userName.equalsIgnoreCase(userFromConnectionString),
                TestResource.getResource("R_userNameNotMatch"));
    }

    /**
     * Testing of {@link SQLServerDatabaseMetaData#getSchemas()}
     * @throws SQLException
     */
    @Test
    public void testDBSchema() throws SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();

        ResultSet rs = databaseMetaData.getSchemas();

        MessageFormat form = new MessageFormat(TestResource.getResource("R_nameEmpty"));
        Object[] msgArgs = {"Schema"};
        while (rs.next()) {
            assertTrue(!StringUtils.isEmpty(rs.getString(1)), form.format(msgArgs));
        }
    }

    /**
     * Get All Tables.
     * 
     * @throws SQLException
     */
    @Test
    public void testDBTables() throws SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        
        ResultSet rsCatalog = databaseMetaData.getCatalogs();
        
        MessageFormat form1 = new MessageFormat(TestResource.getResource("R_atLeastOneFound"));
        Object[] msgArgs1 = {"catalog"};
        assertTrue(rsCatalog.next(), form1.format(msgArgs1));
        
        String[] types = {"TABLE"};
        ResultSet rs = databaseMetaData.getTables(rsCatalog.getString("TABLE_CAT"), null, "%", types);
        
        MessageFormat form2 = new MessageFormat(TestResource.getResource("R_nameEmpty"));
        Object[] msgArgs2 = {"Table"};
        while (rs.next()) {
            assertTrue(!StringUtils.isEmpty(rs.getString("TABLE_NAME")), form2.format(msgArgs2));
        }
    }

    /**
     * Testing DB Columns.<p>
     * We can Improve this test scenario by following way.
     * <ul>
     *  <li> Create table with appropriate column size, data types,auto increment, NULLABLE etc.  
     *  <li> Then get databasemetatadata.getColumns to see if there is any mismatch.
     * </ul>
     * @throws SQLException
     */
    @Test
    public void testGetDBColumn() throws SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        String[] types = {"TABLE"};
        ResultSet rs = databaseMetaData.getTables(null, null, "%", types);
        
        //Fetch one table
        MessageFormat form1 = new MessageFormat(TestResource.getResource("R_atLeastOneFound"));
        Object[] msgArgs1 = {"table"};
        assertTrue(rs.next(), form1.format(msgArgs1));
        
        //Go through all columns.
        ResultSet rs1 = databaseMetaData.getColumns(null, null, rs.getString("TABLE_NAME"), "%");
        
        MessageFormat form2 = new MessageFormat(TestResource.getResource("R_nameEmpty"));
        Object[][] msgArgs2 = {{"Category"}, {"SCHEMA"}, {"Table"}, {"COLUMN"}, {"Data Type"}, {"Type"}, {"Column Size"}, {"Nullable value"}, {"IS_NULLABLE"}, {"IS_AUTOINCREMENT"}};
        while (rs1.next()) {
            assertTrue(!StringUtils.isEmpty(rs1.getString("TABLE_CAT")), form2.format(msgArgs2[0]));
            assertTrue(!StringUtils.isEmpty(rs1.getString("TABLE_SCHEM")), form2.format(msgArgs2[1]));
            assertTrue(!StringUtils.isEmpty(rs1.getString("TABLE_NAME")), form2.format(msgArgs2[2]));
            assertTrue(!StringUtils.isEmpty(rs1.getString("COLUMN_NAME")), form2.format(msgArgs2[3]));
            assertTrue(!StringUtils.isEmpty(rs1.getString("DATA_TYPE")), form2.format(msgArgs2[4]));
            assertTrue(!StringUtils.isEmpty(rs1.getString("TYPE_NAME")), form2.format(msgArgs2[5]));
            assertTrue(!StringUtils.isEmpty(rs1.getString("COLUMN_SIZE")), form2.format(msgArgs2[6]));
            assertTrue(!StringUtils.isEmpty(rs1.getString("NULLABLE")), form2.format(msgArgs2[7])); // 11
            assertTrue(!StringUtils.isEmpty(rs1.getString("IS_NULLABLE")), form2.format(msgArgs2[8])); // 18
            assertTrue(!StringUtils.isEmpty(rs1.getString("IS_AUTOINCREMENT")), form2.format(msgArgs2[9])); // 22
        }
    }
    
    /**
     * We can improve this test case by following manner: 
     * <ul>
     * <li> We can check if PRIVILEGE is in between CRUD / REFERENCES / SELECT / INSERT etc.
     * <li> IS_GRANTABLE can have only 2 values YES / NO
     * </ul>
     * @throws SQLException
     */
    @Test
    public void testGetColumnPrivileges() throws SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        String[] types = {"TABLE"};
        ResultSet rsTables = databaseMetaData.getTables(null, null, "%", types);
        
        //Fetch one table
        MessageFormat form1 = new MessageFormat(TestResource.getResource("R_atLeastOneFound"));
        Object[] msgArgs1 = {"table"};
        assertTrue(rsTables.next(), form1.format(msgArgs1));
        
        //Go through all columns.
        ResultSet rs1 = databaseMetaData.getColumnPrivileges(null, null, rsTables.getString("TABLE_NAME"), "%");
        
        MessageFormat form2 = new MessageFormat(TestResource.getResource("R_nameEmpty"));
        Object[][] msgArgs2 = {{"Category"}, {"SCHEMA"}, {"Table"}, {"COLUMN"}, {"GRANTOR"}, {"GRANTEE"}, {"PRIVILEGE"}, {"IS_GRANTABLE"}};
        while(rs1.next()) {
            assertTrue(!StringUtils.isEmpty(rs1.getString("TABLE_CAT")), form2.format(msgArgs2[0]));
            assertTrue(!StringUtils.isEmpty(rs1.getString("TABLE_SCHEM")), form2.format(msgArgs2[1]));
            assertTrue(!StringUtils.isEmpty(rs1.getString("TABLE_NAME")), form2.format(msgArgs2[2]));
            assertTrue(!StringUtils.isEmpty(rs1.getString("COLUMN_NAME")), form2.format(msgArgs2[3]));
            assertTrue(!StringUtils.isEmpty(rs1.getString("GRANTOR")), form2.format(msgArgs2[4]));
            assertTrue(!StringUtils.isEmpty(rs1.getString("GRANTEE")), form2.format(msgArgs2[5]));
            assertTrue(!StringUtils.isEmpty(rs1.getString("PRIVILEGE")), form2.format(msgArgs2[6]));
            assertTrue(!StringUtils.isEmpty(rs1.getString("IS_GRANTABLE")), form2.format(msgArgs2[7]));
        }
    }
    
    /**
     * TODO: Check JDBC Specs: Can we have any tables/functions without category? 
     * 
     * Testing {@link SQLServerDatabaseMetaData#getFunctions(String, String, String)} with sending wrong category.
     * @throws SQLException
     */
    @Test
    public void testGetFunctionsWithWrongParams() throws SQLException {
        try {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        databaseMetaData.getFunctions("", null, "xp_%");
        assertTrue(false, TestResource.getResource("R_noSchemaShouldFail"));
        } catch(Exception ae) {
            
        }
    }
    
    /**
     * Test {@link SQLServerDatabaseMetaData#getFunctions(String, String, String)}
     * @throws SQLException
     */
    @Test
    public void testGetFunctions() throws SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet rs = databaseMetaData.getFunctions(null, null, "xp_%");
        
        MessageFormat form = new MessageFormat(TestResource.getResource("R_nameNull"));
        Object[][] msgArgs = {{"FUNCTION_CAT"}, {"FUNCTION_SCHEM"}, {"FUNCTION_NAME"}, {"NUM_INPUT_PARAMS"}, {"NUM_OUPUT_PARAMS"}, {"NUM_RESULT_SETS"}, {"FUNCTION_TYPE"}};
        while(rs.next()) {
            assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_CAT")), form.format(msgArgs[0]));
            assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_SCHEM")), form.format(msgArgs[1]));
            assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_NAME")), form.format(msgArgs[2]));
            assertTrue(!StringUtils.isEmpty(rs.getString("NUM_INPUT_PARAMS")), form.format(msgArgs[3]));
            assertTrue(!StringUtils.isEmpty(rs.getString("NUM_OUTPUT_PARAMS")), form.format(msgArgs[4]));
            assertTrue(!StringUtils.isEmpty(rs.getString("NUM_RESULT_SETS")), form.format(msgArgs[5]));
            assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_TYPE")), form.format(msgArgs[6]));
        }
        rs.close();
    }
    
    /**
     * Te
     * @throws SQLException
     */
    @Test
    public void testGetFunctionColumns()  throws SQLException{
        DatabaseMetaData databaseMetaData = connection.getMetaData();
        ResultSet rsFunctions = databaseMetaData.getFunctions(null, null, "%");
        
        //Fetch one Function
        MessageFormat form1 = new MessageFormat(TestResource.getResource("R_atLeastOneFound"));
        Object[] msgArgs1 = {"function"};
        assertTrue(rsFunctions.next(), form1.format(msgArgs1));
        
        //Go through all columns.
        ResultSet rs = databaseMetaData.getFunctionColumns(null, null, rsFunctions.getString("FUNCTION_NAME"), "%");
        
        MessageFormat form2 = new MessageFormat(TestResource.getResource("R_nameNull"));
        Object[][] msgArgs2 = {{"FUNCTION_CAT"}, {"FUNCTION_SCHEM"}, {"FUNCTION_NAME"}, {"COLUMN_NAME"}, {"COLUMN_TYPE"}, {"DATA_TYPE"}, {"TYPE_NAME"}, {"NULLABLE"}, {"IS_NULLABLE"}};
        while(rs.next()) {
            assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_CAT")), form2.format(msgArgs2[0]));
            assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_SCHEM")), form2.format(msgArgs2[1]));
            assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_NAME")), form2.format(msgArgs2[2]));
            assertTrue(!StringUtils.isEmpty(rs.getString("COLUMN_NAME")), form2.format(msgArgs2[3]));
            assertTrue(!StringUtils.isEmpty(rs.getString("COLUMN_TYPE")), form2.format(msgArgs2[4]));
            assertTrue(!StringUtils.isEmpty(rs.getString("DATA_TYPE")), form2.format(msgArgs2[5]));
            assertTrue(!StringUtils.isEmpty(rs.getString("TYPE_NAME")), form2.format(msgArgs2[6]));
            assertTrue(!StringUtils.isEmpty(rs.getString("NULLABLE")), form2.format(msgArgs2[7])); //12
            assertTrue(!StringUtils.isEmpty(rs.getString("IS_NULLABLE")), form2.format(msgArgs2[8])); //19
        }
        
    }
    
}
