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

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDatabaseMetaData;
import com.microsoft.sqlserver.jdbc.StringUtils;
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

        assumeTrue(f.exists(), "Manifest file does not exist on classpath so ignoring test");

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
                    "In case of SNAPSHOT version, build version should be always greater than BuildVersion");
        }
        else {
            assertTrue(intDriverVersion == intBuildVersion, "For NON SNAPSHOT versions build & driver versions should match.");
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
        assertFalse(url.contains("password"), "Get URL should not have password attribute / property.");
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

        assertNotNull(userName, "databaseMetaData.getUserName() should not be null");

        assertTrue(userName.equalsIgnoreCase(userFromConnectionString),
                "databaseMetaData.getUserName() should match with UserName from Connection String.");
    }

    /**
     * Testing of {@link SQLServerDatabaseMetaData#getSchemas()}
     * @throws SQLException
     */
    @Test
    public void testDBSchema() throws SQLException {
        DatabaseMetaData databaseMetaData = connection.getMetaData();

        ResultSet rs = databaseMetaData.getSchemas();

        while (rs.next()) {
            assertTrue(!StringUtils.isEmpty(rs.getString(1)), "Schema Name should not be Empty");
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
        
        assertTrue(rsCatalog.next(), "We should get atleast one catalog"); 
        
        String[] types = {"TABLE"};
        ResultSet rs = databaseMetaData.getTables(rsCatalog.getString("TABLE_CAT"), null, "%", types);
        
        while (rs.next()) {
            assertTrue(!StringUtils.isEmpty(rs.getString("TABLE_NAME")),"Table Name should not be Empty"); 
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
        assertTrue(rs.next(), "At least one table should be found");
        
        //Go through all columns.
        ResultSet rs1 = databaseMetaData.getColumns(null, null, rs.getString("TABLE_NAME"), "%");
        
        while (rs1.next()) {
            assertTrue(!StringUtils.isEmpty(rs1.getString("TABLE_CAT")), "Category Name should not be Empty"); // 1
            assertTrue(!StringUtils.isEmpty(rs1.getString("TABLE_SCHEM")), "SCHEMA Name should not be Empty");
            assertTrue(!StringUtils.isEmpty(rs1.getString("TABLE_NAME")), "Table Name should not be Empty");
            assertTrue(!StringUtils.isEmpty(rs1.getString("COLUMN_NAME")), "COLUMN NAME should not be Empty");
            assertTrue(!StringUtils.isEmpty(rs1.getString("DATA_TYPE")), "Data Type should not be Empty");
            assertTrue(!StringUtils.isEmpty(rs1.getString("TYPE_NAME")), "Data Type Name should not be Empty"); // 6
            assertTrue(!StringUtils.isEmpty(rs1.getString("COLUMN_SIZE")), "Column Size should not be Empty"); // 7
            assertTrue(!StringUtils.isEmpty(rs1.getString("NULLABLE")), "Nullable value should not be Empty"); // 11
            assertTrue(!StringUtils.isEmpty(rs1.getString("IS_NULLABLE")), "Nullable value should not be Empty"); // 18
            assertTrue(!StringUtils.isEmpty(rs1.getString("IS_AUTOINCREMENT")), "Nullable value should not be Empty"); // 22
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
        assertTrue(rsTables.next(), "At least one table should be found");
        
        //Go through all columns.
        ResultSet rs1 = databaseMetaData.getColumnPrivileges(null, null, rsTables.getString("TABLE_NAME"), "%");
        
        while(rs1.next()) {
            assertTrue(!StringUtils.isEmpty(rs1.getString("TABLE_CAT")),"Category Name should not be Empty"); //1
            assertTrue(!StringUtils.isEmpty(rs1.getString("TABLE_SCHEM")),"SCHEMA Name should not be Empty");
            assertTrue(!StringUtils.isEmpty(rs1.getString("TABLE_NAME")),"Table Name should not be Empty");
            assertTrue(!StringUtils.isEmpty(rs1.getString("COLUMN_NAME")),"COLUMN NAME should not be Empty");
            assertTrue(!StringUtils.isEmpty(rs1.getString("GRANTOR")),"GRANTOR should not be Empty");
            assertTrue(!StringUtils.isEmpty(rs1.getString("GRANTEE")),"GRANTEE should not be Empty");
            assertTrue(!StringUtils.isEmpty(rs1.getString("PRIVILEGE")),"PRIVILEGE should not be Empty");
            assertTrue(!StringUtils.isEmpty(rs1.getString("IS_GRANTABLE")),"IS_GRANTABLE should be YES / NO");

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
        assertTrue(false,"As we are not supplying schema it should fail.");
        }catch(Exception ae) {
            
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
        
        while(rs.next()) {
            assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_CAT")),"FUNCTION_CAT should not be NULL");
            assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_SCHEM")),"FUNCTION_SCHEM should not be NULL");
            assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_NAME")),"FUNCTION_NAME should not be NULL");
            assertTrue(!StringUtils.isEmpty(rs.getString("NUM_INPUT_PARAMS")),"NUM_INPUT_PARAMS should not be NULL");
            assertTrue(!StringUtils.isEmpty(rs.getString("NUM_OUTPUT_PARAMS")),"NUM_OUTPUT_PARAMS should not be NULL");
            assertTrue(!StringUtils.isEmpty(rs.getString("NUM_RESULT_SETS")),"NUM_RESULT_SETS should not be NULL");
            assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_TYPE")),"FUNCTION_TYPE should not be NULL");
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
        assertTrue(rsFunctions.next(), "At least one function should be found");
        
        //Go through all columns.
        ResultSet rs = databaseMetaData.getFunctionColumns(null, null, rsFunctions.getString("FUNCTION_NAME"), "%");
        
        while(rs.next()) {
            assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_CAT")),"FUNCTION_CAT should not be NULL");
            assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_SCHEM")),"FUNCTION_SCHEM should not be NULL");
            assertTrue(!StringUtils.isEmpty(rs.getString("FUNCTION_NAME")),"FUNCTION_NAME should not be NULL");
            assertTrue(!StringUtils.isEmpty(rs.getString("COLUMN_NAME")),"COLUMN_NAME should not be NULL");
            assertTrue(!StringUtils.isEmpty(rs.getString("COLUMN_TYPE")),"COLUMN_TYPE should not be NULL");
            assertTrue(!StringUtils.isEmpty(rs.getString("DATA_TYPE")),"DATA_TYPE should not be NULL");
            assertTrue(!StringUtils.isEmpty(rs.getString("TYPE_NAME")),"TYPE_NAME should not be NULL");
            assertTrue(!StringUtils.isEmpty(rs.getString("NULLABLE")),"NULLABLE should not be NULL"); //12
            assertTrue(!StringUtils.isEmpty(rs.getString("IS_NULLABLE")),"IS_NULLABLE should not be NULL"); //19
        }
        
    }
    
}
